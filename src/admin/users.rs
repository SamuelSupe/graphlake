use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result};
use axum::{
    Extension, Json,
    extract::{Path, State},
    http::StatusCode,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tokio::sync::watch;
use uuid::Uuid;

use super::{
    ApiResult, App, Error,
    access::{self, Credential, Principal},
    auth,
    cluster::control,
    models,
};

pub(super) mod delegation;

const LIMIT: usize = 128 * 1024;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub(super) enum Role {
    Query,
    Modeler,
    Publisher,
    Auditor,
    Export,
}

type Grants = BTreeMap<String, BTreeSet<Role>>;

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Account {
    id: Uuid,
    version: Uuid,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    password_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    token_sha256: Option<[u8; 32]>,
    disabled: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    console_disabled: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    mcp_disabled: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    reset_required: bool,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Group {
    version: Uuid,
    grants: Grants,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    partitions: BTreeMap<String, crate::Predicate>,
    members: BTreeSet<String>,
}

#[derive(Clone, Default, Deserialize, Serialize)]
struct Directory {
    accounts: BTreeMap<String, Account>,
    #[serde(default)]
    groups: BTreeMap<String, Group>,
    #[serde(default)]
    mcp: super::mcp::settings::Config,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    ai_tokens: BTreeMap<String, delegation::Token>,
}

struct Live {
    directory: Directory,
    revision: i64,
    watches: BTreeMap<String, watch::Sender<()>>,
    ai_watches: BTreeMap<String, watch::Sender<()>>,
}

pub(super) struct Users {
    path: PathBuf,
    live: Mutex<Live>,
    pub(super) writes: tokio::sync::Mutex<()>,
}

impl Users {
    pub(super) fn mcp_config(&self) -> super::mcp::settings::Config {
        self.live.lock().unwrap().directory.mcp.clone()
    }

    pub(super) fn bearer(&self, token: &str) -> Option<Credential> {
        if !access::valid_token(token) {
            return None;
        }
        let digest: [u8; 32] = Sha256::digest(token.as_bytes()).into();
        let name = {
            let live = self.live.lock().unwrap();
            live.directory
                .accounts
                .iter()
                .find(|(_, account)| {
                    !account.disabled
                        && account
                            .token_sha256
                            .as_ref()
                            .is_some_and(|hash| bool::from(hash.ct_eq(&digest)))
                })
                .map(|(name, _)| name.clone())?
        };
        self.service(&name, token)
    }

    pub(super) async fn load(root: &std::path::Path, ha: Option<&super::ha::Ha>) -> Result<Self> {
        let path = root.join(".graphlake-auth/users.json");
        let (directory, revision) = if let Some(ha) = ha {
            match ha
                .leader
                .get::<Directory>("users")
                .await
                .map_err(|e| anyhow::anyhow!(e.message))?
            {
                Some(data) => data,
                None => {
                    let seed = path.clone();
                    let directory = tokio::time::timeout(
                        std::time::Duration::from_secs(2),
                        tokio::task::spawn_blocking(move || read_directory(&seed)),
                    )
                    .await
                    .context("reading user directory timed out")???;
                    let revision = ha
                        .leader
                        .commit("users", 0, &directory, None::<(Uuid, &())>)
                        .await
                        .map_err(|e| anyhow::anyhow!(e.message))?
                        .context("user directory changed during import")?;
                    (directory, revision)
                }
            }
        } else {
            (read_directory(&path)?, 0)
        };
        check_directory(&directory).map_err(|error| anyhow::anyhow!(error.message))?;
        let watches = directory
            .accounts
            .keys()
            .map(|name| (name.clone(), watch::channel(()).0))
            .collect();
        let ai_watches = directory
            .ai_tokens
            .keys()
            .map(|name| (name.clone(), watch::channel(()).0))
            .collect();
        Ok(Self {
            path,
            live: Mutex::new(Live {
                directory,
                revision,
                watches,
                ai_watches,
            }),
            writes: Default::default(),
        })
    }

    pub(super) fn verifier(&self, name: &str) -> Option<(String, Principal)> {
        let live = self.live.lock().unwrap();
        let account = live.directory.accounts.get(name).filter(|entry| {
            !entry.disabled
                && !entry.console_disabled
                && !entry.reset_required
                && entry.token_sha256.is_none()
        })?;
        Some((
            account.password_hash.clone(),
            Principal::User {
                name: name.into(),
                id: account.id,
                version: account.version,
            },
        ))
    }

    pub(super) fn service(&self, name: &str, token: &str) -> Option<Credential> {
        let live = self.live.lock().unwrap();
        let account = live.directory.accounts.get(name)?;
        let hash = account.token_sha256.as_ref()?;
        (!account.disabled
            && access::valid_token(token)
            && bool::from(hash.ct_eq(&Sha256::digest(token.as_bytes()))))
        .then(|| Credential {
            principal: Principal::ServiceMember {
                name: name.into(),
                id: account.id,
                version: account.version,
            },
            live: live.watches[name].subscribe(),
        })
    }

    pub(super) fn roles(&self, principal: &Principal, model: &str) -> BTreeSet<Role> {
        let live = self.live.lock().unwrap();
        if account(&live, principal).is_none() {
            return BTreeSet::new();
        }
        live.directory
            .groups
            .values()
            .filter(|group| group.members.contains(principal.audit_name()))
            .filter_map(|group| group.grants.get(model))
            .flatten()
            .copied()
            .collect()
    }

    pub(super) fn partition(
        &self,
        principal: &Principal,
        model: &str,
    ) -> ApiResult<Option<crate::Predicate>> {
        let live = self.live.lock().unwrap();
        account(&live, principal).ok_or_else(access::forbidden)?;
        let mut scope = None;
        for term in live
            .directory
            .groups
            .values()
            .filter(|group| group.members.contains(principal.audit_name()))
            .filter_map(|group| group.partitions.get(model))
        {
            if scope.is_some_and(|previous| previous != term) {
                return Err(Error::new(
                    StatusCode::FORBIDDEN,
                    "conflicting_role_scope",
                    "该模型的账号角色配置了互相冲突的租户范围",
                ));
            }
            scope = Some(term);
        }
        Ok(scope.cloned())
    }

    pub(super) fn grants(&self, principal: &Principal) -> Grants {
        let live = self.live.lock().unwrap();
        let mut grants = Grants::new();
        if account(&live, principal).is_some() {
            for group in live
                .directory
                .groups
                .values()
                .filter(|group| group.members.contains(principal.audit_name()))
            {
                for (model, roles) in &group.grants {
                    grants.entry(model.clone()).or_default().extend(roles);
                }
            }
        }
        grants
    }

    pub(super) fn watch(&self, principal: &Principal) -> Option<watch::Receiver<()>> {
        let live = self.live.lock().unwrap();
        if account(&live, principal)?.console_disabled {
            return None;
        }
        live.watches
            .get(principal.audit_name())
            .map(watch::Sender::subscribe)
    }

    pub(super) fn mcp_allowed(&self, principal: &Principal) -> bool {
        match principal {
            Principal::Admin | Principal::Service(_) => true,
            _ => account(&self.live.lock().unwrap(), principal).is_some_and(|a| !a.mcp_disabled),
        }
    }
}

fn read_directory(path: &std::path::Path) -> Result<Directory> {
    if let Ok(meta) = std::fs::symlink_metadata(path) {
        auth::private_permissions(&meta, true)?;
    }
    let text = models::read_file(path, LIMIT).map_err(|e| anyhow::anyhow!(e.message))?;
    text.map(|text| serde_json::from_str(&text))
        .transpose()
        .map(|data| data.unwrap_or_default())
        .map_err(Into::into)
}

pub(super) fn disable_restored_accounts(value: &mut serde_json::Value) -> ApiResult<()> {
    let mut directory: Directory =
        serde_json::from_value(value.clone()).map_err(|_| Error::storage())?;
    for account in directory.accounts.values_mut() {
        account.disabled = true;
        account.reset_required = true;
        account.version = Uuid::new_v4();
    }
    for group in directory.groups.values_mut() {
        group.members.clear();
        group.version = Uuid::new_v4();
    }
    directory.ai_tokens.clear();
    *value = serde_json::to_value(directory).map_err(|_| Error::storage())?;
    Ok(())
}

pub(super) fn verify_backup(value: &serde_json::Value) -> ApiResult<()> {
    let directory = serde_json::from_value(value.clone()).map_err(|_| Error::storage())?;
    check_directory(&directory)
}

fn account<'a>(live: &'a Live, principal: &Principal) -> Option<&'a Account> {
    let (name, id, version, service) = match principal {
        Principal::User { name, id, version } => (name, id, version, false),
        Principal::ServiceMember { name, id, version } => (name, id, version, true),
        _ => return None,
    };
    live.directory.accounts.get(name).filter(|a| {
        a.id == *id && a.version == *version && !a.disabled && a.token_sha256.is_some() == service
    })
}

fn check_directory(directory: &Directory) -> ApiResult<()> {
    directory.mcp.validate()?;
    delegation::validate(directory)?;
    if directory.accounts.len() > 64
        || directory.groups.len() > 64
        || serde_json::to_vec(directory)
            .map_err(|_| Error::storage())?
            .len()
            > LIMIT
    {
        return Err(Error::invalid(
            "users_limit",
            "账号目录超过 64 个账号、64 个角色或 128 KiB",
        ));
    }
    let mut service_hashes = BTreeSet::new();
    for (name, user) in &directory.accounts {
        if !models::valid_id(name)
            || name.eq_ignore_ascii_case("admin")
            || if user.token_sha256.is_some() {
                !user.password_hash.is_empty()
            } else {
                !(user.console_disabled && user.password_hash.is_empty()
                    || auth::supported_hash(&user.password_hash))
            }
        {
            return Err(Error::invalid(
                "invalid_user",
                "账号、模型授权或密码哈希无效",
            ));
        }
        if let Some(hash) = user.token_sha256
            && !service_hashes.insert(hash)
        {
            return Err(Error::invalid("duplicate_token", "服务账号须使用独立令牌"));
        }
    }
    for (name, group) in &directory.groups {
        if !models::valid_id(name)
            || group
                .members
                .iter()
                .any(|name| !directory.accounts.contains_key(name))
            || group
                .grants
                .iter()
                .any(|(id, roles)| !models::valid_id(id) || roles.is_empty())
        {
            return Err(Error::invalid(
                "invalid_role",
                "角色、角色成员或模型权限无效",
            ));
        }
        for (model, predicate) in &group.partitions {
            if !group
                .grants
                .get(model)
                .is_some_and(|roles| roles.contains(&Role::Query))
                || predicate.op != crate::Comparison::Eq
                || predicate.validate().is_err()
            {
                return Err(Error::invalid(
                    "invalid_role_partition",
                    "租户范围须属于已授权查询的模型，并使用单个等值谓词",
                ));
            }
        }
    }
    Ok(())
}

#[derive(Serialize)]
pub(super) struct User {
    username: String,
    version: Uuid,
    disabled: bool,
    service: bool,
    console_access: bool,
    mcp_access: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    mcp_token: Option<String>,
    roles: Vec<String>,
}

pub(super) async fn list(State(app): State<Arc<App>>) -> Json<Vec<User>> {
    let live = app.users.live.lock().unwrap();
    Json(
        live.directory
            .accounts
            .iter()
            .map(|(name, entry)| user_view(&live.directory, name, entry))
            .collect(),
    )
}

fn user_view(directory: &Directory, name: &str, entry: &Account) -> User {
    User {
        username: name.into(),
        version: entry.version,
        disabled: entry.disabled,
        service: entry.token_sha256.is_some(),
        console_access: entry.token_sha256.is_none() && !entry.console_disabled,
        mcp_access: !entry.mcp_disabled,
        mcp_token: None,
        roles: directory
            .groups
            .iter()
            .filter(|(_, group)| group.members.contains(name))
            .map(|(name, _)| name.clone())
            .collect(),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Edit {
    version: Option<Uuid>,
    password: Option<String>,
    #[serde(default)]
    service: bool,
    token: Option<String>,
    disabled: bool,
    console_access: Option<bool>,
    mcp_access: Option<bool>,
    #[serde(default)]
    issue_mcp_token: bool,
}

pub(super) async fn update(
    State(app): State<Arc<App>>,
    Path(name): Path<String>,
    Json(edit): Json<Edit>,
) -> ApiResult<Json<User>> {
    if !models::valid_id(&name) || name.eq_ignore_ascii_case("admin") {
        return Err(Error::invalid("invalid_user", "用户名无效或为内置管理员"));
    }
    if (edit.service && edit.password.is_some())
        || (!edit.service && edit.token.is_some())
        || edit
            .token
            .as_deref()
            .is_some_and(|token| !access::valid_token(token))
    {
        return Err(Error::invalid(
            "invalid_credential",
            "服务令牌须为 32 至 256 个可打印的 ASCII 字符",
        ));
    }
    let token_sha256 = edit
        .token
        .map(|token| Sha256::digest(token.as_bytes()).into());
    let hash = if let Some(password) = edit.password {
        Some(app.auth.hash_new_password(password).await?)
    } else {
        None
    };
    // Finish committing and revoking even when the browser disconnects.
    tokio::spawn(async move {
        let _writes = app.users.writes.lock().await;
        let (mut next, revision) = snapshot(&app);
        let existing = next.accounts.get(&name);
        if existing.map(|entry| entry.version) != edit.version {
            return Err(Error::new(
                StatusCode::CONFLICT,
                "user_conflict",
                "账号已变更，请刷新列表",
            ));
        }
        if existing.is_some_and(|account| account.token_sha256.is_some() != edit.service) {
            return Err(Error::invalid("account_type", "已有成员的账号类型不可更改"));
        }
        let console_access = edit.console_access.unwrap_or_else(|| {
            !edit.service && existing.is_none_or(|account| !account.console_disabled)
        });
        let mcp_access = edit
            .mcp_access
            .unwrap_or_else(|| existing.is_none_or(|account| !account.mcp_disabled));
        if edit.service && console_access {
            return Err(Error::invalid(
                "invalid_console_access",
                "服务账号不能登录控制台",
            ));
        }
        if edit.issue_mcp_token && (edit.service || console_access || !mcp_access || edit.disabled)
        {
            return Err(Error::invalid(
                "invalid_mcp_token_issue",
                "只能为启用的仅 MCP 账号签发令牌",
            ));
        }
        if let Some(account) = existing
            && account.reset_required
            && token_sha256.is_some()
            && token_sha256 == account.token_sha256
        {
            return Err(Error::invalid(
                "token_required",
                "恢复后的服务账号须使用新令牌",
            ));
        }
        let has_new_credential = token_sha256.is_some() || hash.is_some() || edit.issue_mcp_token;
        if !edit.disabled
            && !has_new_credential
            && existing.is_some_and(|account| account.reset_required)
        {
            return Err(Error::invalid(
                "password_required",
                "恢复后的账号须更新凭据才能启用",
            ));
        }
        let reset_required =
            !has_new_credential && existing.is_some_and(|account| account.reset_required);
        let (password_hash, token_sha256) = if edit.service {
            let token = token_sha256
                .or_else(|| existing.and_then(|entry| entry.token_sha256))
                .ok_or_else(|| Error::invalid("token_required", "新服务账号需要初始令牌"))?;
            (String::new(), Some(token))
        } else {
            // A replacement MCP token grants only MCP access. An old
            // restored password must never become usable through a later edit.
            let password = hash
                .or_else(|| {
                    existing
                        .filter(|entry| !entry.reset_required || !edit.issue_mcp_token)
                        .map(|entry| entry.password_hash.clone())
                })
                .unwrap_or_default();
            if console_access && password.is_empty() {
                return Err(Error::invalid(
                    "password_required",
                    "允许登录控制台时需要设置密码",
                ));
            }
            (password, None)
        };
        let entry = Account {
            id: existing.map_or_else(Uuid::new_v4, |entry| entry.id),
            version: Uuid::new_v4(),
            password_hash,
            token_sha256,
            disabled: edit.disabled,
            console_disabled: !edit.service && !console_access,
            mcp_disabled: !mcp_access,
            reset_required,
        };
        let mut response = user_view(&next, &name, &entry);
        if has_new_credential || edit.disabled || !mcp_access {
            next.ai_tokens.remove(&name);
        }
        if edit.issue_mcp_token {
            let (token, value) = delegation::generate(entry.id.to_string())?;
            next.ai_tokens.insert(name.clone(), token);
            response.mcp_token = Some(value);
        }
        next.accounts.insert(name.clone(), entry);
        commit(&app, next, revision, BTreeSet::from([name])).await?;
        Ok(Json(response))
    })
    .await
    .map_err(|_| Error::storage())?
}

pub(super) async fn delete(
    State(app): State<Arc<App>>,
    Path(name): Path<String>,
    Json(version): Json<Uuid>,
) -> ApiResult<StatusCode> {
    tokio::spawn(async move {
        let _writes = app.users.writes.lock().await;
        let (mut next, revision) = snapshot(&app);
        let user = next.accounts.get(&name).ok_or_else(Error::not_found)?;
        if user.version != version {
            return Err(Error::new(
                StatusCode::CONFLICT,
                "user_conflict",
                "账号已变更，请刷新列表",
            ));
        }
        next.accounts.remove(&name);
        next.ai_tokens.remove(&name);
        for group in next.groups.values_mut() {
            if group.members.remove(&name) {
                group.version = Uuid::new_v4();
            }
        }
        commit(&app, next, revision, BTreeSet::from([name])).await?;
        Ok(StatusCode::NO_CONTENT)
    })
    .await
    .map_err(|_| Error::storage())?
}

pub(super) async fn list_groups(State(app): State<Arc<App>>) -> Json<BTreeMap<String, Group>> {
    Json(app.users.live.lock().unwrap().directory.groups.clone())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct GroupEdit {
    version: Option<Uuid>,
    grants: Grants,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    partitions: BTreeMap<String, crate::Predicate>,
    members: BTreeSet<String>,
}

pub(super) async fn update_group(
    State(app): State<Arc<App>>,
    Path(name): Path<String>,
    Json(edit): Json<GroupEdit>,
) -> ApiResult<Json<Group>> {
    tokio::spawn(async move {
        let _writes = app.users.writes.lock().await;
        let (mut next, revision) = snapshot(&app);
        let existing = next.groups.get(&name);
        if existing.map(|entry| entry.version) != edit.version {
            return Err(Error::new(
                StatusCode::CONFLICT,
                "role_conflict",
                "角色已变更，请刷新列表",
            ));
        }
        {
            let models = app.models.lock().unwrap();
            for (model, roles) in &edit.grants {
                if let Err(error) = models.get(model)
                    && !(error.status == StatusCode::NOT_FOUND && roles.contains(&Role::Modeler))
                {
                    return Err(error);
                }
            }
        }
        let before = serde_json::to_value(existing).map_err(|_| Error::storage())?;
        let mut affected = edit.members.clone();
        if let Some(previous) = existing {
            affected.extend(previous.members.iter().cloned());
        }
        let group = Group {
            version: Uuid::new_v4(),
            grants: edit.grants,
            partitions: edit.partitions,
            members: edit.members,
        };
        for name in &affected {
            let account = next
                .accounts
                .get_mut(name)
                .ok_or_else(|| Error::invalid("invalid_member", "角色成员不存在"))?;
            account.version = Uuid::new_v4();
        }
        let after = serde_json::to_value(&group).map_err(|_| Error::storage())?;
        next.groups.insert(name.clone(), group.clone());
        commit(&app, next, revision, affected).await?;
        app.access.record_role_change(&name, before, after);
        Ok(Json(group))
    })
    .await
    .map_err(|_| Error::storage())?
}

pub(super) async fn delete_group(
    State(app): State<Arc<App>>,
    Path(name): Path<String>,
    Json(version): Json<Uuid>,
) -> ApiResult<StatusCode> {
    tokio::spawn(async move {
        let _writes = app.users.writes.lock().await;
        let (mut next, revision) = snapshot(&app);
        let group = next.groups.remove(&name).ok_or_else(Error::not_found)?;
        if group.version != version {
            return Err(Error::new(
                StatusCode::CONFLICT,
                "role_conflict",
                "角色已变更，请刷新列表",
            ));
        }
        for name in &group.members {
            next.accounts
                .get_mut(name)
                .expect("validated membership")
                .version = Uuid::new_v4();
        }
        let before = serde_json::to_value(&group).map_err(|_| Error::storage())?;
        commit(&app, next, revision, group.members).await?;
        app.access
            .record_role_change(&name, before, serde_json::Value::Null);
        Ok(StatusCode::NO_CONTENT)
    })
    .await
    .map_err(|_| Error::storage())?
}

fn snapshot(app: &App) -> (Directory, i64) {
    let live = app.users.live.lock().unwrap();
    (live.directory.clone(), live.revision)
}

pub(super) async fn save_mcp(
    app: Arc<App>,
    config: super::mcp::settings::Config,
    expected: String,
) -> ApiResult<()> {
    tokio::spawn(async move {
        let _writes = app.users.writes.lock().await;
        let (mut next, revision) = snapshot(&app);
        if next.mcp.revision() != expected {
            return Err(Error::new(
                StatusCode::CONFLICT,
                "mcp_conflict",
                "MCP 配置已变更，请重新加载",
            ));
        }
        next.mcp = config;
        commit(&app, next, revision, BTreeSet::new()).await
    })
    .await
    .map_err(|_| Error::storage())?
}

async fn commit(
    app: &App,
    next: Directory,
    revision: i64,
    affected: BTreeSet<String>,
) -> ApiResult<()> {
    check_directory(&next)?;
    let revision = if let Some(ha) = &app.ha {
        match ha
            .leader
            .commit("users", revision, &next, None::<(Uuid, &())>)
            .await
        {
            Ok(Some(revision)) => revision,
            result => {
                let error = result.err().unwrap_or_else(control::lost);
                if matches!(
                    error.code,
                    control::MUTATION_UNKNOWN | "coordinator_not_leader"
                ) {
                    ha.leader.retire();
                }
                return Err(error);
            }
        }
    } else {
        models::atomic_write(
            &app.users.path,
            &serde_json::to_vec(&next).map_err(|_| Error::storage())?,
            false,
        )?;
        revision
    };
    let mut live = app.users.live.lock().unwrap();
    let ai_changed = live
        .directory
        .ai_tokens
        .keys()
        .chain(next.ai_tokens.keys())
        .filter(|name| {
            affected.contains(*name)
                || live.directory.ai_tokens.get(*name) != next.ai_tokens.get(*name)
        })
        .cloned()
        .collect::<BTreeSet<_>>();
    for name in ai_changed {
        if next.ai_tokens.contains_key(&name) {
            live.ai_watches.insert(name, watch::channel(()).0);
        } else {
            live.ai_watches.remove(&name);
        }
    }
    for name in affected {
        if next.accounts.contains_key(&name) {
            live.watches.insert(name, watch::channel(()).0);
        } else {
            live.watches.remove(&name);
        }
    }
    live.directory = next;
    live.revision = revision;
    Ok(())
}

pub(super) async fn current_roles(
    State(app): State<Arc<App>>,
    Extension(principal): Extension<Principal>,
) -> Json<Grants> {
    Json(app.users.grants(&principal))
}

pub(super) fn apply_partition(
    catalog: &crate::Catalog,
    options: &mut crate::GraphOptions,
    term: &crate::Predicate,
) -> ApiResult<()> {
    // The tenant must participate in entity identity. Otherwise keyed joins and
    // retained graph selections can refer to an entity in a different tenant.
    let has_key =
        |unpartitioned: bool, keys: &[String]| !unpartitioned && keys.contains(&term.column);
    if !catalog
        .nodes
        .values()
        .all(|node| has_key(node.unpartitioned, &node.key))
        || !catalog.edges.values().all(|edge| {
            has_key(edge.unpartitioned, &edge.key)
                && [&edge.from, &edge.to].into_iter().all(|endpoint| {
                    catalog.nodes[&endpoint.node]
                        .key
                        .iter()
                        .position(|key| key == &term.column)
                        .and_then(|index| endpoint.columns.get(index))
                        .is_some_and(|column| column == &term.column)
                })
        })
    {
        return Err(Error::new(
            StatusCode::FORBIDDEN,
            "tenant_scope_missing",
            "租户字段须位于所有节点、边及端点映射的业务键中",
        ));
    }
    if !options.logical_partition.contains(term) {
        options.logical_partition.push(term.clone());
    }
    Ok(())
}

pub(super) fn check(app: &App, principal: &Principal, model: &str, role: Role) -> ApiResult<()> {
    if principal == &Principal::Admin || app.users.roles(principal, model).contains(&role) {
        Ok(())
    } else {
        Err(access::forbidden())
    }
}
