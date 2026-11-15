//! Personal MCP credentials resolve to the owner's live identity, never a copied grant set.
use std::{
    collections::BTreeSet,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{Extension, Json, extract::State, http::StatusCode};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use rand::{TryRngCore, rngs::OsRng};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use uuid::Uuid;

use super::{Directory, commit, snapshot};
use crate::admin::{
    ApiResult, App, Error,
    access::{self, Credential, Principal},
    mcp,
};

#[derive(Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct Token {
    owner: String,
    revision: Uuid,
    digest: [u8; 32],
    created_at_ms: u64,
}

pub(super) fn validate(directory: &Directory) -> ApiResult<()> {
    let mut hashes = directory
        .accounts
        .values()
        .filter_map(|account| account.token_sha256)
        .collect::<BTreeSet<_>>();
    for (name, token) in &directory.ai_tokens {
        let owner_valid = if name == "admin" {
            super::models::versions::valid_revision(&token.owner)
        } else {
            directory.accounts.get(name).is_some_and(|account| {
                account.token_sha256.is_none() && token.owner == account.id.to_string()
            })
        };
        if !owner_valid || token.revision.is_nil() || !hashes.insert(token.digest) {
            return Err(Error::invalid(
                "invalid_ai_token",
                "AI 令牌必须绑定有效用户且不可重复",
            ));
        }
    }
    Ok(())
}

fn owner_key(app: &App, directory: &Directory, principal: &Principal) -> ApiResult<String> {
    match principal {
        Principal::Admin => app
            .auth
            .ai_owner_key()
            .ok_or_else(|| Error::invalid("auth_required", "生成个人 AI 令牌前需要启用登录认证")),
        Principal::User { name, id, version } => directory
            .accounts
            .get(name)
            .filter(|account| {
                account.id == *id
                    && account.version == *version
                    && !account.disabled
                    && account.token_sha256.is_none()
            })
            .map(|account| account.id.to_string())
            .ok_or_else(access::forbidden),
        _ => Err(access::forbidden()),
    }
}

pub(in crate::admin) fn authenticate(app: &App, value: &str) -> Option<Credential> {
    if !value.starts_with("glai_") || !access::valid_token(value) {
        return None;
    }
    let digest: [u8; 32] = Sha256::digest(value.as_bytes()).into();
    let live = app.users.live.lock().unwrap();
    let (name, token) = live
        .directory
        .ai_tokens
        .iter()
        .find(|(_, token)| bool::from(token.digest.ct_eq(&digest)))?;
    let principal = if name == "admin" {
        (app.auth.ai_owner_key().as_ref() == Some(&token.owner)).then_some(Principal::Admin)?
    } else {
        let account = live.directory.accounts.get(name)?;
        if account.disabled
            || account.reset_required
            || account.mcp_disabled
            || account.token_sha256.is_some()
            || token.owner != account.id.to_string()
        {
            return None;
        }
        Principal::User {
            name: name.clone(),
            id: account.id,
            version: account.version,
        }
    };
    Some(Credential {
        principal,
        live: live.ai_watches.get(name)?.subscribe(),
    })
}

fn view(app: &App, principal: &Principal) -> ApiResult<Value> {
    let live = app.users.live.lock().unwrap();
    let owner = owner_key(app, &live.directory, principal)?;
    let token = live
        .directory
        .ai_tokens
        .get(principal.audit_name())
        .filter(|token| token.owner == owner);
    Ok(
        json!({"owner":principal.audit_name(),"active":token.is_some(),"revision":token.map(|token|token.revision),"created_at_ms":token.map(|token|token.created_at_ms)}),
    )
}

pub(in crate::admin) async fn status(
    State(app): State<Arc<App>>,
    Extension(principal): Extension<Principal>,
) -> ApiResult<Json<Value>> {
    Ok(Json(view(&app, &principal)?))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::admin) struct Change {
    revision: Option<Uuid>,
}

pub(super) fn generate(owner: String) -> ApiResult<(Token, String)> {
    let mut random = [0; 32];
    OsRng
        .try_fill_bytes(&mut random)
        .map_err(|_| Error::storage())?;
    let value = format!("glai_{}", URL_SAFE_NO_PAD.encode(random));
    let created_at_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error::storage())?
        .as_millis() as u64;
    let token = Token {
        owner,
        revision: Uuid::new_v4(),
        digest: Sha256::digest(value.as_bytes()).into(),
        created_at_ms,
    };
    Ok((token, value))
}

pub(in crate::admin) async fn issue(
    State(app): State<Arc<App>>,
    Extension(principal): Extension<Principal>,
    Json(change): Json<Change>,
) -> ApiResult<Json<Value>> {
    tokio::spawn(async move {
        let _writes = app.users.writes.lock().await;
        let (mut next, revision) = snapshot(&app);
        let owner = owner_key(&app, &next, &principal)?;
        if !mcp::can_use(&app, &principal) { return Err(access::forbidden()); }
        let previous = next.ai_tokens.get(principal.audit_name()).filter(|token| token.owner == owner);
        if previous.map(|token|token.revision) != change.revision { return Err(conflict()); }
        let (token, value) = generate(owner)?;
        let response = json!({"owner":principal.audit_name(),"active":true,"revision":token.revision,"created_at_ms":token.created_at_ms,"token":value});
        next.ai_tokens.insert(principal.audit_name().into(), token);
        commit(&app, next, revision, BTreeSet::new()).await?;
        Ok(Json(response))
    }).await.map_err(|_| Error::storage())?
}

pub(in crate::admin) async fn revoke(
    State(app): State<Arc<App>>,
    Extension(principal): Extension<Principal>,
    Json(change): Json<Change>,
) -> ApiResult<StatusCode> {
    tokio::spawn(async move {
        let _writes = app.users.writes.lock().await;
        let (mut next, revision) = snapshot(&app);
        let owner = owner_key(&app, &next, &principal)?;
        let current = next
            .ai_tokens
            .get(principal.audit_name())
            .filter(|token| token.owner == owner);
        if current.map(|token| token.revision) != change.revision {
            return Err(conflict());
        }
        next.ai_tokens.remove(principal.audit_name());
        commit(&app, next, revision, BTreeSet::new()).await?;
        Ok(StatusCode::NO_CONTENT)
    })
    .await
    .map_err(|_| Error::storage())?
}

fn conflict() -> Error {
    Error::new(
        StatusCode::CONFLICT,
        "ai_token_conflict",
        "AI 令牌已在其他页面变更，请刷新令牌状态后重试",
    )
}
