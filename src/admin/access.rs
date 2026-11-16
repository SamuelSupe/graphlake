use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, ensure};
use axum::http::{HeaderMap, StatusCode, header};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tokio::sync::watch;

use super::{ApiResult, Error, auth, models};

#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
pub(super) enum Principal {
    #[default]
    Admin,
    Service(String),
    User {
        name: String,
        id: uuid::Uuid,
        version: uuid::Uuid,
    },
    ServiceMember {
        name: String,
        id: uuid::Uuid,
        version: uuid::Uuid,
    },
}

impl Principal {
    pub(super) fn audit_name(&self) -> &str {
        match self {
            Self::Admin => "admin",
            Self::Service(name) | Self::User { name, .. } | Self::ServiceMember { name, .. } => {
                name
            }
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    token_sha256: String,
    models: BTreeSet<String>,
    #[serde(default)]
    export_models: BTreeSet<String>,
}

#[derive(PartialEq, Eq)]
struct Grant {
    digest: [u8; 32],
    models: BTreeSet<String>,
    export_models: BTreeSet<String>,
    http: bool,
}

struct Account {
    grant: Grant,
    // Dropping this sender revokes all connections authenticated with this grant.
    live: watch::Sender<()>,
}

#[derive(Clone)]
pub(super) struct Credential {
    pub(super) principal: Principal,
    pub(super) live: watch::Receiver<()>,
}

#[derive(PartialEq, Eq)]
struct FileVersion {
    modified: SystemTime,
    len: u64,
    #[cfg(unix)]
    identity: (u64, u64, i64, i64),
}

#[derive(Default)]
struct Accounts {
    version: Option<FileVersion>,
    invalid: bool,
    entries: BTreeMap<String, Account>,
}

pub(super) struct Policy {
    path: PathBuf,
    managed: bool,
    accounts: Mutex<Accounts>,
    audit: Option<Mutex<AuditFile>>,
    anonymous_audit: Option<Mutex<AuditFile>>,
    audit_failures: AtomicU64,
}

const AUDIT_ROTATE_BYTES: u64 = 32 * 1024 * 1024;

struct AuditFile {
    path: PathBuf,
    file: File,
    bytes: u64,
}

impl AuditFile {
    fn open(audit_path: PathBuf) -> Result<Self> {
        if let Ok(meta) = fs::symlink_metadata(&audit_path) {
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
        let audit = options
            .open(&audit_path)
            .context("opening operation audit log")?;
        let meta = audit.metadata()?;
        ensure!(meta.is_file(), "audit log must be a regular file");
        auth::private_permissions(&meta, true)?;
        Ok(Self {
            path: audit_path,
            file: audit,
            bytes: meta.len(),
        })
    }

    fn append(&mut self, entry: &[u8]) -> Result<()> {
        if self.bytes.saturating_add(entry.len() as u64) > AUDIT_ROTATE_BYTES {
            match fs::rename(&self.path, self.path.with_extension("jsonl.previous")) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            *self = Self::open(self.path.clone())?;
        }
        self.file.write_all(entry)?;
        self.bytes += entry.len() as u64;
        Ok(())
    }
}

impl Policy {
    pub(super) fn load(root: &Path, legacy: Option<(&str, &str)>, managed: bool) -> Result<Self> {
        let dir = root.join(".graphlake-auth");
        let path = dir.join("service-accounts.json");
        let version = config_version(&path)?;
        let legacy = if let Some((model, env)) = legacy {
            if version.is_none() {
                match std::env::var(env) {
                    Ok(token) => {
                        ensure!(
                            valid_token(&token),
                            "driver token must contain 32 to 256 printable ASCII characters"
                        );
                        Some(Grant {
                            digest: Sha256::digest(token.as_bytes()).into(),
                            models: BTreeSet::from([model.into()]),
                            export_models: BTreeSet::new(),
                            http: false,
                        })
                    }
                    Err(std::env::VarError::NotPresent) => None,
                    Err(error) => {
                        return Err(error)
                            .with_context(|| format!("reading driver token from {env}"));
                    }
                }
            } else {
                None
            }
        } else {
            None
        };
        let mut accounts = Accounts::default();
        replace_accounts(&mut accounts, version, read_grants(&path, legacy.as_ref())?);
        let audit = if managed {
            // The HA catalog is rebuilt on each term. Use the service journal so
            // audit events survive process exit and never block on the shared mount.
            None
        } else {
            let audit_path = dir.join("audit.jsonl");
            Some(Mutex::new(AuditFile::open(audit_path)?))
        };
        let anonymous_audit = if managed {
            None
        } else {
            Some(Mutex::new(AuditFile::open(
                dir.join("audit-anonymous.jsonl"),
            )?))
        };
        Ok(Self {
            path,
            managed,
            accounts: Mutex::new(accounts),
            audit,
            anonymous_audit,
            audit_failures: AtomicU64::new(0),
        })
    }

    pub(super) fn refresh(&self) -> bool {
        if self.managed {
            return true;
        }
        let mut accounts = self.accounts.lock().unwrap();
        let update = (|| -> Result<()> {
            let version = config_version(&self.path)?;
            if accounts.invalid || accounts.version != version {
                let grants = read_grants(&self.path, None)?;
                replace_accounts(&mut accounts, version, grants);
            }
            Ok(())
        })();
        if let Err(error) = update {
            if !accounts.entries.is_empty() {
                eprintln!("GraphLake service accounts disabled: {error}");
            }
            accounts.entries.clear();
            // Retry after failure, including recovery to a removed configuration.
            accounts.invalid = true;
            return false;
        }
        true
    }

    pub(super) fn validate_config(config: &str) -> ApiResult<()> {
        if config.len() > 256 * 1024 {
            return Err(Error::invalid(
                "invalid_service_accounts",
                "账号配置超过 256 KiB",
            ));
        }
        parse_grants(Some(config), None)
            .map(|_| ())
            .map_err(|error| Error::invalid("invalid_service_accounts", error.to_string()))
    }

    pub(super) fn apply_config(&self, config: &str) -> ApiResult<()> {
        let grants = parse_grants(Some(config), None).map_err(|_| Error::storage())?;
        replace_accounts(&mut self.accounts.lock().unwrap(), None, grants);
        Ok(())
    }

    pub(super) async fn watch(app: std::sync::Weak<super::App>) {
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        loop {
            interval.tick().await;
            let Some(app) = app.upgrade() else {
                return;
            };
            if app.shutdown.is_cancelled() {
                return;
            }
            app.access.refresh();
        }
    }

    pub(super) fn authenticate(&self, name: &str, token: &str, http: bool) -> Option<Credential> {
        if !self.refresh() {
            return None;
        }
        let accounts = self.accounts.lock().unwrap();
        let account = accounts.entries.get(name)?;
        if (http && !account.grant.http)
            || !valid_token(token)
            || !bool::from(
                account
                    .grant
                    .digest
                    .ct_eq(&Sha256::digest(token.as_bytes())),
            )
        {
            return None;
        }
        Some(Credential {
            principal: Principal::Service(name.into()),
            live: account.live.subscribe(),
        })
    }

    fn only_model(&self, principal: &Principal) -> Option<String> {
        let Principal::Service(name) = principal else {
            return None;
        };
        let accounts = self.accounts.lock().unwrap();
        let account = accounts.entries.get(name)?;
        (account.grant.models.len() == 1).then(|| account.grant.models.first().unwrap().clone())
    }

    pub(super) fn permits(&self, principal: &Principal, model: &str) -> bool {
        match principal {
            Principal::Admin => true,
            Principal::User { .. } | Principal::ServiceMember { .. } => false,
            Principal::Service(name) => {
                self.refresh()
                    && self
                        .accounts
                        .lock()
                        .unwrap()
                        .entries
                        .get(name)
                        .is_some_and(|a| a.grant.models.contains(model))
            }
        }
    }

    fn permits_export(&self, name: &str, model: &str) -> bool {
        self.refresh()
            && self
                .accounts
                .lock()
                .unwrap()
                .entries
                .get(name)
                .is_some_and(|a| a.grant.export_models.contains(model))
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
        self.record_target(principal, protocol, action, model, outcome, status, None);
    }

    pub(super) fn audit_failures(&self) -> u64 {
        self.audit_failures.load(Ordering::Relaxed)
    }

    pub(super) fn record_target(
        &self,
        principal: Option<&Principal>,
        protocol: &str,
        action: &str,
        model: Option<&str>,
        outcome: &str,
        status: u16,
        target: Option<&str>,
    ) {
        let subject = principal.map_or("anonymous", Principal::audit_name);
        let mut entry = json!({
            "at_ms": SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis(),
            "subject": subject, "protocol": protocol, "action": action,
            "model": model.filter(|id| models::valid_id(id)), "outcome": outcome, "status": status,
        });
        if matches!(
            principal,
            Some(Principal::Service(_) | Principal::ServiceMember { .. })
        ) {
            entry["subject_type"] = json!("service");
        }
        if let Some(target) = target {
            entry["target"] = json!(target);
        }
        self.write_audit(entry, principal.is_none());
    }

    pub(super) fn record_role_change(
        &self,
        name: &str,
        before: serde_json::Value,
        after: serde_json::Value,
    ) {
        self.write_audit(json!({
            "at_ms": SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis(),
            "subject": "admin", "protocol": "http", "action": "role_changed", "target": name,
            "outcome": "ok", "before": before, "after": after,
        }), false);
    }

    fn write_audit(&self, entry: serde_json::Value, anonymous: bool) {
        super::telemetry::audit(&entry);
        let audit = if anonymous {
            &self.anonymous_audit
        } else {
            &self.audit
        };
        if let Some(audit) = audit {
            let mut entry = serde_json::to_vec(&entry).expect("serializable audit event");
            entry.push(b'\n');
            if let Err(error) = audit.lock().unwrap().append(&entry) {
                self.audit_failures.fetch_add(1, Ordering::Relaxed);
                eprintln!("GraphLake audit write failed: {error}");
            }
        } else {
            super::telemetry::audit_to_journal(&entry);
        }
    }
}

pub(super) fn authenticate(
    app: &super::App,
    name: &str,
    token: &str,
    http: bool,
) -> Option<Credential> {
    if let Some(name) = name.strip_prefix("svc/") {
        app.users.service(name, token)
    } else {
        app.access.authenticate(name, token, http)
    }
}

pub(super) fn http(app: &super::App, headers: &HeaderMap) -> Option<Credential> {
    let mut values = headers.get_all(header::AUTHORIZATION).iter();
    let (scheme, encoded) = values.next()?.to_str().ok()?.split_once(' ')?;
    if values.next().is_some() || !scheme.eq_ignore_ascii_case("basic") || encoded.len() > 512 {
        return None;
    }
    let raw = String::from_utf8(STANDARD.decode(encoded).ok()?).ok()?;
    let (name, token) = raw.split_once(':')?;
    authenticate(app, name, token, true)
}

pub(super) fn default_model(
    app: &super::App,
    principal: &Principal,
    configured: &str,
) -> Option<String> {
    if query_allowed(app, principal, configured) {
        return Some(configured.into());
    }
    if matches!(principal, Principal::ServiceMember { .. }) {
        let grants = app.users.grants(principal);
        let mut allowed = grants
            .iter()
            .filter(|(_, roles)| roles.contains(&super::users::Role::Query));
        let (only, _) = allowed.next()?;
        return allowed.next().is_none().then(|| only.clone());
    }
    app.access.only_model(principal)
}

pub(super) fn partition(
    app: &super::App,
    principal: &Principal,
    model: &str,
) -> ApiResult<Option<crate::Predicate>> {
    match principal {
        Principal::User { .. } | Principal::ServiceMember { .. } => {
            app.users.partition(principal, model)
        }
        _ => Ok(None),
    }
}

pub(super) async fn revoked(live: &mut Option<watch::Receiver<()>>) {
    match live {
        Some(live) => {
            let _ = live.changed().await;
        }
        None => std::future::pending().await,
    }
}

fn config_version(path: &Path) -> Result<Option<FileVersion>> {
    let meta = match fs::symlink_metadata(path) {
        Ok(meta) => meta,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).context("reading service account file metadata"),
    };
    ensure!(
        meta.is_file(),
        "service account configuration must be a regular file"
    );
    auth::private_permissions(&meta, true)?;
    #[cfg(unix)]
    use std::os::unix::fs::MetadataExt;
    Ok(Some(FileVersion {
        modified: meta.modified()?,
        len: meta.len(),
        #[cfg(unix)]
        identity: (meta.dev(), meta.ino(), meta.ctime(), meta.ctime_nsec()),
    }))
}

fn read_grants(path: &Path, legacy: Option<&Grant>) -> Result<BTreeMap<String, Grant>> {
    let content = models::read_file(path, 256 * 1024)
        .map_err(|_| anyhow::anyhow!("reading service-accounts.json"))?;
    parse_grants(content.as_deref(), legacy)
}

fn parse_grants(content: Option<&str>, legacy: Option<&Grant>) -> Result<BTreeMap<String, Grant>> {
    let mut grants = BTreeMap::new();
    if let Some(content) = content {
        let config: BTreeMap<String, Config> =
            serde_json::from_str(content).context("invalid service-accounts.json")?;
        for (name, entry) in config {
            ensure!(
                models::valid_id(&name) && name != "admin",
                "invalid service account name"
            );
            ensure!(
                !entry.models.is_empty() && entry.models.iter().all(|id| models::valid_id(id)),
                "service accounts need explicit, valid model IDs"
            );
            ensure!(
                entry.export_models.is_subset(&entry.models),
                "service account export_models must be a subset of models"
            );
            let digest =
                decode_digest(&entry.token_sha256).context("invalid service account token hash")?;
            ensure!(
                grants.values().all(|grant: &Grant| grant.digest != digest)
                    && legacy.is_none_or(|grant| grant.digest != digest),
                "service accounts must use different tokens"
            );
            grants.insert(
                name,
                Grant {
                    digest,
                    models: entry.models,
                    export_models: entry.export_models,
                    http: true,
                },
            );
        }
    } else if let Some(legacy) = legacy {
        grants.insert(
            "graphlake".into(),
            Grant {
                digest: legacy.digest,
                models: legacy.models.clone(),
                export_models: BTreeSet::new(),
                http: false,
            },
        );
    }
    Ok(grants)
}

fn replace_accounts(
    accounts: &mut Accounts,
    version: Option<FileVersion>,
    grants: BTreeMap<String, Grant>,
) {
    accounts.entries = grants
        .into_iter()
        .map(|(name, grant)| {
            let old = accounts.entries.remove(&name);
            let account = old
                .filter(|account| account.grant == grant)
                .unwrap_or_else(|| Account {
                    grant,
                    live: watch::channel(()).0,
                });
            (name, account)
        })
        .collect();
    accounts.version = version;
    accounts.invalid = false;
}

pub(super) fn revocation_error() -> Error {
    Error::new(
        StatusCode::UNAUTHORIZED,
        "credentials_revoked",
        "服务账号凭据已撤销",
    )
}

pub(super) fn forbidden() -> Error {
    Error::new(
        StatusCode::FORBIDDEN,
        "model_forbidden",
        "当前身份没有此操作的权限",
    )
}

pub(super) fn valid_token(token: &str) -> bool {
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
    pub target: Option<String>,
}

pub(super) fn operation(method: &axum::http::Method, path: &str) -> Operation {
    use axum::http::Method;
    let parts = path.split('/').collect::<Vec<_>>();
    let (action, model, service) = match (method, parts.as_slice()) {
        (&Method::GET | &Method::HEAD, ["", "api", "v1", "auth", "session"]) => {
            ("session", None, false)
        }
        (&Method::POST, ["", "api", "v1", "auth", "sso"]) => ("sso_login", None, false),
        (&Method::POST, ["", "api", "v1", "auth", "login"]) => ("login", None, false),
        (&Method::POST, ["", "api", "v1", "auth", "logout"]) => ("logout", None, false),
        (&Method::GET | &Method::HEAD, ["", "api", "v1", "models"]) => ("list_models", None, true),
        (&Method::POST, ["", "api", "v1", "models"]) => ("create_model", None, false),
        (&Method::POST, ["", "api", "v1", "models", "validate"]) => ("validate", None, false),
        (&Method::GET | &Method::HEAD, ["", "api", "v1", "queries", _]) => {
            ("query_result", None, true)
        }
        (&Method::DELETE, ["", "api", "v1", "queries", _]) => ("cancel", None, true),
        (&Method::GET | &Method::HEAD, ["", "api", "v1", "downloads", _]) => {
            ("download", None, true)
        }
        (&Method::GET | &Method::HEAD, ["", "api", "v1", "my-roles"]) => ("my_roles", None, false),
        (&Method::GET | &Method::HEAD, ["", "api", "v1", "options"]) => ("options", None, false),
        (&Method::GET | &Method::HEAD, ["", "api", "v1", "settings"]) => {
            ("read_settings", None, false)
        }
        (&Method::GET | &Method::HEAD, ["", "api", "v1", "backups", "status"]) => {
            ("backup_status", None, false)
        }
        (&Method::POST, ["", "api", "v1", "backups", "export"]) => ("backup_export", None, false),
        (&Method::PUT, ["", "api", "v1", "settings"]) => ("save_settings", None, false),
        (&Method::GET | &Method::HEAD, ["", "api", "v1", "mcp", "settings"]) => {
            ("read_mcp_settings", None, false)
        }
        (&Method::PUT, ["", "api", "v1", "mcp", "settings"]) => ("save_mcp_settings", None, false),
        (&Method::GET | &Method::HEAD, ["", "api", "v1", "mcp", "access"]) => {
            ("mcp_access", None, false)
        }
        (&Method::GET | &Method::HEAD, ["", "api", "v1", "mcp", "token"]) => {
            ("ai_token_status", None, false)
        }
        (&Method::POST, ["", "api", "v1", "mcp", "token"]) => ("ai_token_issue", None, false),
        (&Method::DELETE, ["", "api", "v1", "mcp", "token"]) => ("ai_token_revoke", None, false),
        (&Method::GET | &Method::HEAD, ["", "api", "v1", "users"]) => ("list_users", None, false),
        (&Method::GET | &Method::HEAD, ["", "api", "v1", "roles"]) => ("list_roles", None, false),
        (&Method::PUT, ["", "api", "v1", "roles", _]) => ("update_role", None, false),
        (&Method::DELETE, ["", "api", "v1", "roles", _]) => ("delete_role", None, false),
        (&Method::DELETE, ["", "api", "v1", "users", _]) => ("delete_user", None, false),
        (&Method::PUT, ["", "api", "v1", "users", _]) => ("update_user", None, false),
        (&Method::PUT, ["", "api", "v1", "service-accounts"]) => {
            ("service_accounts_update", None, false)
        }
        (&Method::GET | &Method::HEAD, ["", "api", "v1", "mutations", _]) => {
            ("mutation_status", None, false)
        }
        (method, ["", "api", "v1", "models", id, rest @ ..]) if models::valid_id(id) => {
            let (action, service) = match (method, rest) {
                (&Method::GET | &Method::HEAD, []) => ("read_model", false),
                (&Method::PUT, []) => ("save_model", false),
                (&Method::GET | &Method::HEAD, ["schema", _]) => ("schema", true),
                (&Method::GET | &Method::HEAD, ["history"] | ["history", _]) => ("history", false),
                (&Method::GET | &Method::HEAD, ["results", _]) => ("result_download", true),
                (&Method::GET | &Method::HEAD, ["results", _, "graph", _]) => ("result", true),
                (&Method::POST, ["query"]) => ("query", true),
                (&Method::GET | &Method::HEAD, ["csr"]) => ("read_csr", true),
                (&Method::GET | &Method::HEAD, ["freshness"]) => ("read_freshness", true),
                (&Method::POST, ["csr"]) => ("refresh_csr", false),
                (&Method::POST, ["explore"]) => ("explore", true),
                (&Method::POST, ["export"]) => ("export", true),
                (&Method::POST, ["browse"]) => ("browse", false),
                (&Method::POST, ["publish"]) => ("publish", false),
                (&Method::POST, ["rollback"]) => ("rollback", false),
                (&Method::PUT, ["history", _]) => ("restore_archive", false),
                (&Method::POST, ["unpublish"]) => ("unpublish", false),
                (&Method::GET | &Method::HEAD, ["readiness"]) => ("model_readiness", true),
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
        target: match parts.as_slice() {
            ["", "api", "v1", "roles" | "users", name] if models::valid_id(name) => {
                Some((*name).into())
            }
            _ => None,
        },
    }
}

pub(super) fn owns(principal: &Principal, owner: &Principal) -> bool {
    principal == &Principal::Admin
        || principal == owner
        || matches!((principal, owner),
        (Principal::User { id: current, .. }, Principal::User { id: original, .. }) if current == original)
}

pub(super) fn owns_query(principal: &Principal, owner: &Principal) -> bool {
    principal == &Principal::Admin || principal == owner
}

pub(super) fn query_allowed(app: &super::App, principal: &Principal, model: &str) -> bool {
    match principal {
        Principal::User { .. } | Principal::ServiceMember { .. } => {
            super::users::check(app, principal, model, super::users::Role::Query).is_ok()
        }
        _ => app.access.permits(principal, model),
    }
}

pub(super) fn check_query(app: &super::App, principal: &Principal, model: &str) -> ApiResult<()> {
    query_allowed(app, principal, model)
        .then_some(())
        .ok_or_else(forbidden)
}

pub(super) fn check_export(app: &super::App, principal: &Principal, model: &str) -> ApiResult<()> {
    check_query(app, principal, model)?;
    match principal {
        Principal::Admin => Ok(()),
        Principal::Service(name) if app.access.permits_export(name, model) => Ok(()),
        Principal::Service(_) => Err(forbidden()),
        _ => super::users::check(app, principal, model, super::users::Role::Export),
    }
}

pub(super) fn permits_operation(app: &super::App, principal: &Principal, op: &Operation) -> bool {
    use super::users::Role;
    match principal {
        Principal::Admin => true,
        Principal::Service(_) | Principal::ServiceMember { .. } => {
            op.service
                && op.model.as_deref().is_none_or(|id| match op.action {
                    "export" | "result_download" => check_export(app, principal, id).is_ok(),
                    _ => query_allowed(app, principal, id),
                })
        }
        Principal::User { .. } => match (op.action, op.model.as_deref()) {
            (
                "list_models" | "my_roles" | "options" | "logout" | "validate" | "mutation_status"
                | "query_result" | "cancel" | "download" | "mcp_access" | "ai_token_status"
                | "ai_token_issue" | "ai_token_revoke",
                None,
            ) => true,
            (action, Some(id)) => {
                let roles = app.users.roles(principal, id);
                match action {
                    "read_model" | "history" => [Role::Modeler, Role::Publisher, Role::Auditor]
                        .iter()
                        .any(|role| roles.contains(role)),
                    "save_model" => roles.contains(&Role::Modeler),
                    "export" | "result_download" => {
                        roles.contains(&Role::Query) && roles.contains(&Role::Export)
                    }
                    "publish" | "rollback" | "unpublish" | "restore_archive" | "refresh_csr" => {
                        roles.contains(&Role::Publisher)
                    }
                    "model_readiness" => !roles.is_empty(),
                    _ => op.service && roles.contains(&Role::Query),
                }
            }
            _ => false,
        },
    }
}
