use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::Write,
    path::Path,
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, ensure};
use axum::http::{HeaderMap, StatusCode, header};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use super::{ApiResult, Error, auth, models};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Principal {
    Admin,
    Service(String),
}

impl Principal {
    pub(super) fn audit_name(&self) -> &str {
        match self {
            Self::Admin => "admin",
            Self::Service(name) => name,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    token_sha256: String,
    models: BTreeSet<String>,
}

struct Account {
    digest: [u8; 32],
    models: BTreeSet<String>,
    http: bool,
}

pub(super) struct Policy {
    accounts: BTreeMap<String, Account>,
    audit: Mutex<File>,
}

impl Policy {
    pub(super) fn load(root: &Path, legacy: Option<(&str, &str)>) -> Result<Self> {
        let dir = root.join(".graphlake-auth");
        let path = dir.join("service-accounts.json");
        if let Ok(meta) = fs::symlink_metadata(&path) {
            ensure!(
                meta.is_file(),
                "service account configuration must be a regular file"
            );
            auth::private_permissions(&meta, true)?;
        }
        let mut accounts = BTreeMap::new();
        if let Some(config) = models::read_file(&path, 256 * 1024)
            .map_err(|_| anyhow::anyhow!("reading service-accounts.json"))?
        {
            let config: BTreeMap<String, Config> =
                serde_json::from_str(&config).context("invalid service-accounts.json")?;
            for (name, entry) in config {
                ensure!(
                    models::valid_id(&name) && !matches!(name.as_str(), "admin" | "graphlake"),
                    "invalid service account name"
                );
                ensure!(
                    !entry.models.is_empty() && entry.models.iter().all(|id| models::valid_id(id)),
                    "service accounts need explicit, valid model IDs"
                );
                let digest = decode_digest(&entry.token_sha256)
                    .context("invalid service account token hash")?;
                ensure!(
                    accounts
                        .values()
                        .all(|account: &Account| account.digest != digest),
                    "service accounts must use different tokens"
                );
                accounts.insert(
                    name,
                    Account {
                        digest,
                        models: entry.models,
                        http: true,
                    },
                );
            }
        }
        if let Some((model, env)) = legacy {
            // An existing driver-only token stays driver-only. Named accounts are independent.
            if accounts.is_empty() || std::env::var_os(env).is_some() {
                let token = std::env::var(env)
                    .with_context(|| format!("reading driver token from {env}"))?;
                ensure!(
                    valid_token(&token),
                    "driver token must contain 32 to 256 printable ASCII characters"
                );
                let digest = Sha256::digest(token.as_bytes()).into();
                ensure!(
                    accounts.values().all(|account| account.digest != digest),
                    "legacy and named service accounts must use different tokens"
                );
                accounts.insert(
                    "graphlake".into(),
                    Account {
                        digest,
                        models: BTreeSet::from([model.into()]),
                        http: false,
                    },
                );
            }
        }
        let path = dir.join("audit.jsonl");
        if let Ok(meta) = fs::symlink_metadata(&path) {
            ensure!(meta.is_file(), "audit log must be a regular file");
        }
        let mut options = OpenOptions::new();
        options.create(true).append(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
        }
        let audit = options.open(path).context("opening operation audit log")?;
        let meta = audit.metadata()?;
        ensure!(meta.is_file(), "audit log must be a regular file");
        auth::private_permissions(&meta, true)?;
        Ok(Self {
            accounts,
            audit: Mutex::new(audit),
        })
    }

    pub(super) fn authenticate(&self, name: &str, token: &str, http: bool) -> Option<Principal> {
        let account = self.accounts.get(name)?;
        if (http && !account.http)
            || !valid_token(token)
            || !bool::from(account.digest.ct_eq(&Sha256::digest(token.as_bytes())))
        {
            return None;
        }
        Some(Principal::Service(name.into()))
    }

    pub(super) fn http(&self, headers: &HeaderMap) -> Option<Principal> {
        let mut values = headers.get_all(header::AUTHORIZATION).iter();
        let (scheme, encoded) = values.next()?.to_str().ok()?.split_once(' ')?;
        if values.next().is_some() || !scheme.eq_ignore_ascii_case("basic") || encoded.len() > 512 {
            return None;
        }
        let raw = String::from_utf8(STANDARD.decode(encoded).ok()?).ok()?;
        let (name, token) = raw.split_once(':')?;
        self.authenticate(name, token, true)
    }

    pub(super) fn default_model(&self, principal: &Principal, configured: &str) -> Option<String> {
        if self.permits(principal, configured) {
            return Some(configured.into());
        }
        let Principal::Service(name) = principal else {
            return None;
        };
        let account = self.accounts.get(name)?;
        (account.models.len() == 1).then(|| account.models.first().unwrap().clone())
    }

    pub(super) fn permits(&self, principal: &Principal, model: &str) -> bool {
        match principal {
            Principal::Admin => true,
            Principal::Service(name) => self
                .accounts
                .get(name)
                .is_some_and(|a| a.models.contains(model)),
        }
    }

    pub(super) fn check(&self, principal: &Principal, model: &str) -> ApiResult<()> {
        if self.permits(principal, model) {
            Ok(())
        } else {
            Err(forbidden())
        }
    }

    pub(super) fn record(
        &self,
        principal: Option<&Principal>,
        protocol: &str,
        action: &str,
        model: Option<&str>,
        outcome: &str,
        status: u16,
    ) {
        let subject = principal.map_or("anonymous", Principal::audit_name);
        let mut entry = serde_json::to_vec(&json!({
            "at_ms": SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis(),
            "subject": subject, "protocol": protocol, "action": action,
            "model": model.filter(|id| models::valid_id(id)), "outcome": outcome, "status": status,
        }))
        .expect("serializable audit event");
        entry.push(b'\n');
        if let Err(error) = self.audit.lock().unwrap().write_all(&entry) {
            eprintln!("GraphLake audit write failed: {error}");
        }
    }
}

pub(super) fn forbidden() -> Error {
    Error::new(
        StatusCode::FORBIDDEN,
        "model_forbidden",
        "当前身份没有此操作的权限",
    )
}

fn valid_token(token: &str) -> bool {
    (32..=256).contains(&token.len()) && token.bytes().all(|b| b.is_ascii_graphic())
}

fn decode_digest(input: &str) -> Option<[u8; 32]> {
    if input.len() != 64 {
        return None;
    }
    let mut bytes = [0; 32];
    for (byte, chunk) in bytes.iter_mut().zip(input.as_bytes().chunks_exact(2)) {
        *byte = u8::from_str_radix(std::str::from_utf8(chunk).ok()?, 16).ok()?;
    }
    Some(bytes)
}

#[derive(Clone)]
pub(super) struct AuditModel(pub String);

pub(super) struct Operation {
    pub action: &'static str,
    pub model: Option<String>,
    pub service: bool,
}

pub(super) fn operation(method: &axum::http::Method, path: &str) -> Operation {
    use axum::http::Method;
    let parts = path.split('/').collect::<Vec<_>>();
    let (action, model, service) = match (method, parts.as_slice()) {
        (&Method::GET | &Method::HEAD, ["", "api", "v1", "auth", "session"]) => {
            ("session", None, false)
        }
        (&Method::POST, ["", "api", "v1", "auth", "login"]) => ("login", None, false),
        (&Method::POST, ["", "api", "v1", "auth", "logout"]) => ("logout", None, false),
        (&Method::GET | &Method::HEAD, ["", "api", "v1", "models"]) => ("list_models", None, true),
        (&Method::POST, ["", "api", "v1", "models"]) => ("create_model", None, false),
        (&Method::POST, ["", "api", "v1", "models", "validate"]) => ("validate", None, false),
        (&Method::DELETE, ["", "api", "v1", "queries", _]) => ("cancel", None, true),
        (&Method::GET, ["", "api", "v1", "downloads", _]) => ("download", None, true),
        (method, ["", "api", "v1", "models", id, rest @ ..]) if models::valid_id(id) => {
            let (action, service) = match (method, rest) {
                (&Method::GET | &Method::HEAD, []) => ("read_model", false),
                (&Method::PUT, []) => ("save_model", false),
                (&Method::GET | &Method::HEAD, ["schema", _]) => ("schema", true),
                (&Method::GET | &Method::HEAD, ["history"] | ["history", _]) => ("history", false),
                (&Method::GET | &Method::HEAD, ["results", _] | ["results", _, "graph", _]) => {
                    ("result", true)
                }
                (&Method::POST, ["query"]) => ("query", true),
                (&Method::POST, ["explore"]) => ("explore", true),
                (&Method::POST, ["export"]) => ("export", true),
                (&Method::POST, ["browse"]) => ("browse", false),
                (&Method::POST, ["publish"]) => ("publish", false),
                (&Method::POST, ["rollback"]) => ("rollback", false),
                _ => ("api", false),
            };
            (action, Some((*id).into()), service)
        }
        _ => ("api", None, false),
    };
    Operation {
        action,
        model,
        service,
    }
}

pub(super) fn owns(principal: &Principal, owner: &Principal) -> bool {
    principal == &Principal::Admin || principal == owner
}
