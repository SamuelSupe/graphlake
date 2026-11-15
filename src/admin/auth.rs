use std::{
    collections::{HashMap, VecDeque},
    fs::{self, File, OpenOptions},
    io::{self, IsTerminal},
    net::IpAddr,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime},
};

use anyhow::{Context, Result, ensure};
use argon2::{
    Algorithm, Argon2, Params, PasswordHash, PasswordHasher, PasswordVerifier, Version,
    password_hash::SaltString,
};
use axum::{
    Json,
    extract::{Request, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode, header},
    response::{IntoResponse, Response},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use rand::{TryRngCore, rngs::OsRng};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tokio::sync::Semaphore;

use super::{App, Error, access::Principal, models, users::Users};

const USERNAME: &str = "admin";
const IDLE: Duration = Duration::from_secs(30 * 60);
const LIFETIME: Duration = Duration::from_secs(8 * 60 * 60);
const FAILURE_WINDOW: Duration = Duration::from_secs(60);
const MAX_FAILURES: usize = 5;
const MAX_SESSIONS_PER_USER: usize = 4;
const MAX_FAILURE_BUCKETS: usize = 1024;
const MAX_ADMIN_FAILURE_BUCKETS: usize = 64;
const SOURCE_FAILURES: usize = 30;
pub(super) const LOGIN_BODY_LIMIT: usize = 4096;
type Token = [u8; 32];

struct Store {
    credentials: PathBuf,
    lock: File,
}

impl Store {
    fn open(catalog: &Path) -> Result<Self> {
        ensure!(catalog.is_dir(), "catalog directory is not a directory");
        let dir = catalog.join(".graphlake-auth");
        let mut builder = fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        if let Err(error) = builder.create(&dir)
            && error.kind() != io::ErrorKind::AlreadyExists
        {
            return Err(error).context("creating .graphlake-auth");
        }
        let meta = fs::symlink_metadata(&dir).context("reading .graphlake-auth")?;
        ensure!(meta.is_dir(), ".graphlake-auth must be a real directory");
        private_permissions(&meta, false)?;

        let path = dir.join("lock");
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
        }
        if let Ok(meta) = fs::symlink_metadata(&path) {
            ensure!(meta.is_file(), "authentication lock must be a regular file");
        }
        let lock = options.open(path).context("opening authentication lock")?;
        let meta = lock.metadata()?;
        ensure!(meta.is_file(), "authentication lock must be a regular file");
        private_permissions(&meta, true)?;
        Ok(Self {
            credentials: dir.join("password.phc"),
            lock,
        })
    }

    fn try_acquire(&self) -> Result<bool> {
        match self.lock.try_lock() {
            Ok(()) => Ok(true),
            Err(fs::TryLockError::WouldBlock) => Ok(false),
            Err(error) => Err(error).context("locking the catalog"),
        }
    }

    fn has_credentials(&self) -> Result<bool> {
        match fs::symlink_metadata(&self.credentials) {
            Ok(meta) => {
                ensure!(meta.is_file(), "credentials must be a regular file");
                private_permissions(&meta, true)?;
                Ok(true)
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error).context("checking administrator credentials"),
        }
    }
}

pub(super) fn private_permissions(meta: &fs::Metadata, file: bool) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let allowed = if file { 0o600 } else { 0o700 };
        ensure!(
            meta.mode() & 0o7777 & !allowed == 0 && (!file || meta.nlink() == 1),
            "authentication storage requires private permissions (directory 0700, files 0600) and unlinked copies"
        );
    }
    #[cfg(not(unix))]
    let _ = (meta, file);
    Ok(())
}

fn argon2() -> Argon2<'static> {
    Argon2::new(
        Algorithm::Argon2id,
        Version::V0x13,
        Params::new(19_456, 2, 1, Some(32)).expect("valid Argon2id parameters"),
    )
}

fn valid_password(password: &str) -> bool {
    (15..=128).contains(&password.chars().count())
}

pub(super) fn supported_hash(hash: &str) -> bool {
    PasswordHash::new(hash).is_ok_and(|parsed| {
        parsed.algorithm.as_str() == "argon2id"
            && parsed.version == Some(19)
            && parsed.salt.is_some()
            && parsed.hash.is_some_and(|output| output.len() == 32)
            && Params::try_from(&parsed).is_ok_and(|params| params == *argon2().params())
    })
}

fn hash_password(password: &str) -> Result<String> {
    let salt = SaltString::encode_b64(&random::<16>()?).map_err(anyhow::Error::msg)?;
    Ok(argon2()
        .hash_password(password.as_bytes(), &salt)
        .map_err(anyhow::Error::msg)?
        .to_string())
}

fn random<const N: usize>() -> Result<[u8; N]> {
    let mut value = [0; N];
    OsRng
        .try_fill_bytes(&mut value)
        .context("reading system randomness")?;
    Ok(value)
}

/// Initialize or replace local administrator credentials using the controlling terminal.
pub fn setup(catalog: &Path, reset: bool) -> Result<()> {
    let store = Store::open(catalog)?;
    ensure!(
        store.try_acquire()?,
        "catalog directory is already in use; stop its graphlake-admin server before changing credentials"
    );
    let exists = store.has_credentials()?;
    if reset {
        ensure!(
            exists,
            "administrator credentials are missing; run auth init"
        );
    } else {
        ensure!(
            !exists,
            "administrator credentials already exist; use auth reset-password"
        );
    }
    ensure!(
        io::stdin().is_terminal() && io::stderr().is_terminal(),
        "password input requires an interactive terminal"
    );
    let password = rpassword::prompt_password("Administrator password: ")?;
    ensure!(
        valid_password(&password),
        "password must be 15 to 128 characters"
    );
    let confirm = rpassword::prompt_password("Confirm password: ")?;
    ensure!(password == confirm, "password confirmation does not match");
    let encoded = hash_password(&password)?;
    models::atomic_write(&store.credentials, encoded.as_bytes(), !reset)
        .map_err(|error| anyhow::anyhow!("saving administrator credentials: {}", error.message))?;
    eprintln!("GraphLake administrator credentials saved (username: {USERNAME})");
    Ok(())
}

#[derive(Clone, Serialize, Deserialize)]
struct Session {
    created: SystemTime,
    last_used: SystemTime,
    #[serde(default)]
    principal: Principal,
}

impl Session {
    fn active(&self, now: SystemTime) -> bool {
        now.duration_since(self.created)
            .is_ok_and(|age| age < LIFETIME)
            && now
                .duration_since(self.last_used)
                .is_ok_and(|age| age < IDLE)
    }
}

#[derive(Default)]
struct Sessions {
    tokens: HashMap<Token, Session>,
    failures: HashMap<FailureKey, VecDeque<Instant>>,
    last_failure_prune: Option<Instant>,
}

impl Sessions {
    fn check(&mut self, token: Token, now: SystemTime, touch: bool) -> bool {
        let Some(session) = self.tokens.get_mut(&token) else {
            return false;
        };
        if !session.active(now) {
            self.tokens.remove(&token);
            return false;
        }
        if touch {
            session.last_used = now;
        }
        true
    }

    fn insert(&mut self, token: Token, now: SystemTime, principal: Principal) {
        let mut own = Vec::new();
        self.tokens.retain(|digest, session| {
            if !session.active(now) {
                return false;
            }
            let same_user = match (&session.principal, &principal) {
                (Principal::Admin, Principal::Admin) => true,
                (Principal::User { name: old, .. }, Principal::User { name: current, .. }) => {
                    old == current
                }
                _ => false,
            };
            if same_user {
                if session.principal != principal {
                    return false;
                }
                own.push((session.last_used, *digest));
            }
            true
        });
        own.sort_unstable();
        for (_, token) in own
            .iter()
            .take((own.len() + 1).saturating_sub(MAX_SESSIONS_PER_USER))
        {
            self.tokens.remove(token);
        }
        self.tokens.insert(
            token,
            Session {
                created: now,
                last_used: now,
                principal,
            },
        );
    }
}

#[derive(Clone, Hash, PartialEq, Eq)]
enum FailureKey {
    Account(String),
    Source(Option<IpAddr>),
    Administrator(Option<IpAddr>),
}

fn login_keys(name: &str, source: Option<IpAddr>) -> Vec<(FailureKey, usize)> {
    if name == USERNAME {
        vec![(FailureKey::Administrator(source), MAX_FAILURES)]
    } else {
        let name = if models::valid_id(name) { name } else { "" };
        vec![
            (FailureKey::Account(name.into()), MAX_FAILURES),
            (FailureKey::Source(source), SOURCE_FAILURES),
        ]
    }
}

fn prune_failures(failures: &mut HashMap<FailureKey, VecDeque<Instant>>, now: Instant) {
    failures.retain(|_, times| {
        times.retain(|time| now.duration_since(*time) < FAILURE_WINDOW);
        !times.is_empty()
    });
}

#[derive(Serialize, Deserialize)]
struct SharedSessions {
    password_version: Token,
    // Older Coordinators interpret every entry of tokens as an administrator.
    tokens: Vec<(Token, Session)>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    user_tokens: Vec<(Token, Session)>,
}

struct SharedAuth {
    leader: Arc<super::cluster::control::Leadership>,
    password_version: Token,
    revision: tokio::sync::Mutex<i64>,
}

pub(super) struct Auth {
    _store: Store,
    hash: Option<String>,
    cookie: String,
    secure: bool,
    shared: Option<SharedAuth>,
    sessions: Mutex<Sessions>,
    password_slot: Arc<Semaphore>,
    admin_password_slot: Arc<Semaphore>,
    trust_proxy_client_ip: bool,
    proxy_identity_key: Option<Token>,
}

impl Auth {
    pub(super) async fn load(
        catalog: &Path,
        port: u16,
        insecure_dev: bool,
        secure: bool,
        leader: Option<Arc<super::cluster::control::Leadership>>,
    ) -> Result<Self> {
        let store = Store::open(catalog)?;
        // The local lock protects bootstrap credentials against password resets.
        // In HA it is held in the private cache. The etcd lease arbitrates hosts.
        ensure!(store.try_acquire()?, "catalog directory is already in use");
        let exists = store.has_credentials()?;
        let hash = if insecure_dev {
            ensure!(
                !exists,
                "--insecure-dev requires a catalog directory without administrator credentials"
            );
            None
        } else {
            ensure!(
                exists,
                "administrator credentials are missing; run graphlake-admin --catalog-dir <dir> auth init"
            );
            let hash = models::read_file(&store.credentials, 512)
                .map_err(|error| {
                    anyhow::anyhow!("reading administrator credentials: {}", error.message)
                })?
                .context("administrator credentials disappeared")?;
            ensure!(
                supported_hash(&hash),
                "unsupported administrator credential format; run auth reset-password"
            );
            Some(hash)
        };
        let mut sessions = Sessions::default();
        let shared = if let Some(leader) = leader {
            let password_version =
                Sha256::digest(hash.as_ref().context("failover requires authentication")?).into();
            let mut revision = 0;
            if let Some((saved, version)) = leader
                .get::<SharedSessions>("sessions")
                .await
                .map_err(|e| anyhow::anyhow!(e.message))?
            {
                revision = version;
                if saved.password_version == password_version {
                    let now = SystemTime::now();
                    sessions.tokens = saved
                        .tokens
                        .into_iter()
                        .filter(|(_, session)| session.principal == Principal::Admin)
                        .chain(saved.user_tokens.into_iter().filter(|(_, session)| {
                            matches!(session.principal, Principal::User { .. })
                        }))
                        .filter(|(_, session)| session.active(now))
                        .collect();
                }
            }
            Some(SharedAuth {
                leader,
                password_version,
                revision: tokio::sync::Mutex::new(revision),
            })
        } else {
            None
        };
        let proxy_identity_key = std::env::var_os("GRAPHLAKE_PROXY_IDENTITY_SECRET")
            .map(|value| {
                let value = value
                    .into_string()
                    .map_err(|_| anyhow::anyhow!("invalid proxy identity secret"))?;
                ensure!(
                    secure
                        && !insecure_dev
                        && (32..=256).contains(&value.len())
                        && value.bytes().all(|byte| byte.is_ascii_graphic()),
                    "trusted proxy identity requires HTTPS and a 32–256 character secret"
                );
                Ok::<Token, anyhow::Error>(Sha256::digest(value.as_bytes()).into())
            })
            .transpose()?;
        Ok(Self {
            _store: store,
            hash,
            cookie: format!("graphlake_admin_{port}"),
            secure,
            shared,
            sessions: Mutex::new(sessions),
            password_slot: Arc::new(Semaphore::new(2)),
            admin_password_slot: Arc::new(Semaphore::new(1)),
            trust_proxy_client_ip: secure
                && std::env::var("GRAPHLAKE_TRUST_PROXY_CLIENT_IP").as_deref() == Ok("1"),
            proxy_identity_key,
        })
    }

    fn token(&self, headers: &HeaderMap) -> Option<Token> {
        let mut token = None;
        for value in headers.get_all(header::COOKIE) {
            for pair in value.to_str().ok()?.split(';') {
                if let Some((name, value)) = pair.trim().split_once('=')
                    && name == self.cookie
                {
                    if token.is_some() || value.len() != 43 {
                        return None;
                    }
                    let mut bytes = [0; 32];
                    if URL_SAFE_NO_PAD.decode_slice(value, &mut bytes).ok()? != bytes.len() {
                        return None;
                    }
                    token = Some(Sha256::digest(bytes).into());
                }
            }
        }
        token
    }

    fn cookie_header(&self, token: Option<Token>) -> HeaderValue {
        let age = if token.is_some() {
            LIFETIME.as_secs()
        } else {
            0
        };
        let value = token
            .map(|bytes| URL_SAFE_NO_PAD.encode(bytes))
            .unwrap_or_default();
        HeaderValue::from_str(&format!(
            "{}={value}; Path=/api; HttpOnly; SameSite=Strict; Max-Age={age}{}",
            self.cookie,
            if self.secure { "; Secure" } else { "" }
        ))
        .expect("cookie uses ASCII")
    }

    fn proxy_user<'a>(&self, headers: &'a HeaderMap) -> Option<&'a str> {
        let key = self.proxy_identity_key.as_ref()?;
        let mut secrets = headers.get_all("x-graphlake-proxy-secret").iter();
        let supplied = secrets.next()?.to_str().ok()?;
        if secrets.next().is_some()
            || supplied.len() > 256
            || !bool::from(key.ct_eq(&Sha256::digest(supplied.as_bytes())))
        {
            return None;
        }
        let mut identities = headers.get_all("x-graphlake-proxy-user").iter();
        let user = identities.next()?.to_str().ok()?;
        (identities.next().is_none() && user != USERNAME && models::valid_id(user)).then_some(user)
    }

    pub(super) async fn principal(
        self: &Arc<Self>,
        users: &Users,
        headers: &HeaderMap,
        touch: bool,
    ) -> Option<Principal> {
        if self.hash.is_none() {
            return Some(Principal::Admin);
        }
        let token = self.token(headers)?;
        let now = SystemTime::now();
        let (principal, update) = {
            let mut sessions = self.sessions.lock().unwrap();
            if !sessions.check(token, now, touch && self.shared.is_none()) {
                return None;
            }
            let session = &sessions.tokens[&token];
            let update = self.shared.is_some()
                && touch
                && now.duration_since(session.last_used).unwrap_or_default()
                    >= Duration::from_secs(5);
            (session.principal.clone(), update)
        };
        if let Principal::User { name, .. } = &principal {
            if users.watch(&principal).is_none()
                || (self.proxy_identity_key.is_some() && self.proxy_user(headers) != Some(name))
            {
                return None;
            }
        }
        if update {
            let auth = self.clone();
            tokio::spawn(async move { auth.apply(SessionChange::Touch(token, now)).await })
                .await
                .ok()?
                .ok()?;
        }
        Some(principal)
    }

    pub(super) async fn hash_new_password(&self, password: String) -> super::ApiResult<String> {
        if self.hash.is_none() {
            return Err(Error::not_found());
        }
        if !valid_password(&password) {
            return Err(Error::invalid(
                "invalid_password",
                "密码长度必须为 15 至 128 个字符",
            ));
        }
        let slot = self
            .password_slot
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::busy())?;
        tokio::task::spawn_blocking(move || {
            let _slot = slot;
            hash_password(&password).map_err(|_| Error::storage())
        })
        .await
        .map_err(|_| Error::storage())?
    }

    pub(super) fn public(request: &Request) -> bool {
        matches!(
            (request.method(), request.uri().path()),
            (&Method::GET | &Method::HEAD, "/api/v1/auth/session")
                | (
                    &Method::POST,
                    "/api/v1/auth/login" | "/api/v1/auth/sso" | "/api/v1/auth/logout"
                )
        )
    }

    async fn apply(&self, change: SessionChange) -> super::ApiResult<()> {
        if let Some(shared) = &self.shared {
            let mut revision = shared.revision.lock().await;
            let mut snapshot = Sessions {
                tokens: self.sessions.lock().unwrap().tokens.clone(),
                ..Default::default()
            };
            if !change.apply(&mut snapshot)? {
                return Ok(());
            }
            let (tokens, user_tokens) = snapshot
                .tokens
                .iter()
                .map(|(digest, value)| (*digest, value.clone()))
                .partition(|(_, session)| session.principal == Principal::Admin);
            let data = SharedSessions {
                password_version: shared.password_version,
                tokens,
                user_tokens,
            };
            // The transaction may have committed even if the client disconnects.
            // Persist independently of the HTTP handler and retire the process on an ambiguous response.
            let result = shared
                .leader
                .commit("sessions", *revision, &data, None::<(uuid::Uuid, &())>)
                .await;
            match result {
                Ok(Some(next)) => {
                    self.sessions.lock().unwrap().tokens = snapshot.tokens;
                    *revision = next;
                }
                Err(error) => {
                    if error.code != super::cluster::control::UNCERTAIN {
                        shared.leader.retire();
                    }
                    return Err(error);
                }
                Ok(None) => {
                    shared.leader.retire();
                    return Err(super::cluster::control::lost());
                }
            }
        } else {
            change.apply(&mut self.sessions.lock().unwrap())?;
        }
        Ok(())
    }

    async fn replace(
        &self,
        headers: &HeaderMap,
        new_token: Option<(Token, Principal)>,
    ) -> super::ApiResult<()> {
        self.apply(SessionChange::Replace(self.token(headers), new_token))
            .await
    }
}

enum SessionChange {
    Touch(Token, SystemTime),
    Replace(Option<Token>, Option<(Token, Principal)>),
}

impl SessionChange {
    fn apply(self, sessions: &mut Sessions) -> super::ApiResult<bool> {
        match self {
            Self::Touch(token, now) => {
                if !sessions.check(token, now, true) {
                    return Err(Error::new(
                        StatusCode::UNAUTHORIZED,
                        "auth_required",
                        "请重新登录",
                    ));
                }
                Ok(true)
            }
            Self::Replace(old, new) => {
                let removed = old
                    .and_then(|token| sessions.tokens.remove(&token))
                    .is_some();
                let inserted = new.is_some();
                if let Some((token, principal)) = new {
                    sessions.insert(Sha256::digest(token).into(), SystemTime::now(), principal);
                }
                Ok(removed || inserted)
            }
        }
    }
}

#[derive(Serialize)]
pub(super) struct SessionStatus {
    authenticated: bool,
    enabled: bool,
    username: Option<String>,
    administrator: bool,
    subject: Option<uuid::Uuid>,
    authorization: Option<uuid::Uuid>,
    proxy_sign_in: bool,
}

impl SessionStatus {
    fn new(auth: &Auth, principal: Option<&Principal>) -> Self {
        let enabled = auth.hash.is_some();
        let (subject, authorization) = match principal {
            Some(Principal::User { id, version, .. }) => (Some(*id), Some(*version)),
            _ => (None, None),
        };
        Self {
            authenticated: principal.is_some(),
            enabled,
            administrator: principal == Some(&Principal::Admin),
            username: principal.filter(|_| enabled).map(|p| p.audit_name().into()),
            subject,
            authorization,
            proxy_sign_in: auth.proxy_identity_key.is_some(),
        }
    }
}

pub(super) async fn session(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
) -> Json<SessionStatus> {
    let principal = app.auth.principal(&app.users, &headers, false).await;
    Json(SessionStatus::new(&app.auth, principal.as_ref()))
}

#[derive(Deserialize)]
pub(super) struct Credentials {
    username: String,
    password: String,
}

fn throttled(seconds: u64) -> Response {
    let mut response = Error::new(
        StatusCode::TOO_MANY_REQUESTS,
        "login_throttled",
        "登录尝试过多，请稍后再试",
    )
    .into_response();
    response
        .headers_mut()
        .insert(header::RETRY_AFTER, HeaderValue::from(seconds));
    response
}

pub(super) async fn login(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(input): Json<Credentials>,
) -> Response {
    if app.auth.hash.is_none() {
        return Error::not_found().into_response();
    }
    if app.auth.proxy_identity_key.is_some() && input.username != USERNAME {
        return Error::new(
            StatusCode::UNAUTHORIZED,
            "proxy_identity_required",
            "此账号需要通过企业登录入口认证",
        )
        .into_response();
    }
    let auth = &app.auth;
    let source = auth
        .trust_proxy_client_ip
        .then(|| {
            let mut values = headers.get_all("x-graphlake-client-ip").iter();
            let value = values.next()?.to_str().ok()?.parse::<IpAddr>().ok()?;
            values.next().is_none().then_some(value)
        })
        .flatten()
        .map(|ip| ip.to_canonical());
    let keys = login_keys(&input.username, source);
    let semaphore = if input.username == USERNAME {
        &auth.admin_password_slot
    } else {
        &auth.password_slot
    };
    let Ok(slot) = Arc::clone(semaphore).try_acquire_owned() else {
        return throttled(1);
    };
    let verifier = app.clone();
    let worker = tokio::task::spawn_blocking(move || {
        let _slot = slot;
        let auth = &verifier.auth;
        {
            let now = Instant::now();
            let mut sessions = auth.sessions.lock().unwrap();
            if sessions
                .last_failure_prune
                .is_none_or(|last| now.duration_since(last) >= Duration::from_secs(1))
            {
                prune_failures(&mut sessions.failures, now);
                sessions.last_failure_prune = Some(now);
            }
            if let Some(seconds) = keys
                .iter()
                .filter_map(|(key, limit)| {
                    let times = sessions.failures.get(key)?;
                    let age = now.duration_since(*times.iter().rev().nth(*limit - 1)?);
                    (age < FAILURE_WINDOW).then(|| (FAILURE_WINDOW - age).as_secs() + 1)
                })
                .max()
            {
                return Err(throttled(seconds));
            }
        }
        let named = verifier.users.verifier(&input.username);
        let encoded = named
            .as_ref()
            .map(|(hash, _)| hash.as_str())
            .unwrap_or_else(|| auth.hash.as_deref().unwrap());
        let hash = PasswordHash::new(encoded).expect("validated credentials");
        let valid = valid_password(&input.password)
            && argon2()
                .verify_password(input.password.as_bytes(), &hash)
                .is_ok();
        if !valid || (input.username != USERNAME && named.is_none()) {
            let now = Instant::now();
            let mut sessions = auth.sessions.lock().unwrap();
            for (key, limit) in keys {
                if !sessions.failures.contains_key(&key) {
                    // Keep the administrator buckets reserved even if usernames or source IPs churn.
                    let administrator = matches!(key, FailureKey::Administrator(_));
                    let capacity = if administrator {
                        MAX_ADMIN_FAILURE_BUCKETS
                    } else {
                        MAX_FAILURE_BUCKETS
                    };
                    let mut buckets = sessions.failures.iter().filter(|(key, _)| {
                        matches!(key, FailureKey::Administrator(_)) == administrator
                    });
                    if buckets.clone().count() >= capacity
                        && let Some((oldest, _)) = buckets
                            .by_ref()
                            .min_by_key(|(_, times)| times.back().copied())
                    {
                        let oldest = oldest.clone();
                        sessions.failures.remove(&oldest);
                    }
                }
                let times = sessions.failures.entry(key).or_default();
                while times.len() >= limit {
                    times.pop_front();
                }
                times.push_back(now);
            }
            return Err(Error::new(
                StatusCode::UNAUTHORIZED,
                "invalid_credentials",
                "用户名或密码错误",
            )
            .into_response());
        }
        let principal = named.map_or(Principal::Admin, |(_, principal)| principal);
        random::<32>()
            .map(|token| (token, principal))
            .map_err(|_| Error::storage().into_response())
    });
    let (token, principal) = match worker.await {
        Ok(Ok(token)) => token,
        Ok(Err(response)) => return response,
        Err(_) => return Error::storage().into_response(),
    };
    issue_session(app, headers, token, principal).await
}

pub(super) async fn sso(State(app): State<Arc<App>>, headers: HeaderMap) -> Response {
    let Some((_, principal)) = app
        .auth
        .proxy_user(&headers)
        .and_then(|name| app.users.verifier(name))
    else {
        return Error::new(
            StatusCode::UNAUTHORIZED,
            "proxy_identity_required",
            "企业身份未通过验证或账号不可用",
        )
        .into_response();
    };
    let Ok(token) = random::<32>() else {
        return Error::storage().into_response();
    };
    issue_session(app, headers, token, principal).await
}

async fn issue_session(
    app: Arc<App>,
    headers: HeaderMap,
    token: Token,
    principal: Principal,
) -> Response {
    let task = tokio::spawn(async move {
        if let Err(error) = app
            .auth
            .replace(&headers, Some((token, principal.clone())))
            .await
        {
            return error.into_response();
        }
        (
            axum::Extension(principal.clone()),
            [(header::SET_COOKIE, app.auth.cookie_header(Some(token)))],
            Json(SessionStatus::new(&app.auth, Some(&principal))),
        )
            .into_response()
    });
    task.await
        .unwrap_or_else(|_| Error::storage().into_response())
}

pub(super) async fn logout(State(app): State<Arc<App>>, headers: HeaderMap) -> Response {
    let task = tokio::spawn(async move {
        if let Err(error) = app.auth.replace(&headers, None).await {
            return error.into_response();
        }
        (
            StatusCode::NO_CONTENT,
            [(header::SET_COOKIE, app.auth.cookie_header(None))],
        )
            .into_response()
    });
    task.await
        .unwrap_or_else(|_| Error::storage().into_response())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idle_and_absolute_expiry_are_independent_of_status_checks() {
        let start = SystemTime::now();
        let mut sessions = Sessions::default();
        let token = [1; 32];
        sessions.insert(token, start, Principal::Admin);
        assert!(sessions.check(token, start + IDLE - Duration::from_secs(1), false));
        assert!(!sessions.check(token, start + IDLE, true));
        sessions.insert(token, start, Principal::Admin);
        for minute in (15..480).step_by(15) {
            assert!(sessions.check(token, start + Duration::from_secs(minute * 60), true));
        }
        assert!(!sessions.check(token, start + LIFETIME, true));
    }
}
