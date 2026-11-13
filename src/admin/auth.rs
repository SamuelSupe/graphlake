use std::{
    collections::{HashMap, VecDeque},
    fs::{self, File, OpenOptions},
    io::{self, IsTerminal},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
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
use tokio::sync::Semaphore;

use super::{App, Error, models};

const USERNAME: &str = "admin";
const IDLE: Duration = Duration::from_secs(30 * 60);
const LIFETIME: Duration = Duration::from_secs(8 * 60 * 60);
const FAILURE_WINDOW: Duration = Duration::from_secs(60);
const MAX_FAILURES: usize = 5;
const MAX_SESSIONS: usize = 32;
pub(super) const LOGIN_BODY_LIMIT: usize = 4096;
type Token = [u8; 32];

struct Store {
    credentials: PathBuf,
    _lock: File,
}

impl Store {
    fn lock(catalog: &Path) -> Result<Self> {
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
        lock.try_lock().context(
            "catalog directory is already in use; stop its graphlake-admin server before changing credentials",
        )?;
        Ok(Self {
            credentials: dir.join("password.phc"),
            _lock: lock,
        })
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

fn random<const N: usize>() -> Result<[u8; N]> {
    let mut value = [0; N];
    OsRng
        .try_fill_bytes(&mut value)
        .context("reading system randomness")?;
    Ok(value)
}

/// Initialize or replace local administrator credentials using the controlling terminal.
pub fn setup(catalog: &Path, reset: bool) -> Result<()> {
    let store = Store::lock(catalog)?;
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
    let salt = SaltString::encode_b64(&random::<16>()?).map_err(anyhow::Error::msg)?;
    let encoded = argon2()
        .hash_password(password.as_bytes(), &salt)
        .map_err(anyhow::Error::msg)?
        .to_string();
    models::atomic_write(&store.credentials, encoded.as_bytes(), !reset)
        .map_err(|error| anyhow::anyhow!("saving administrator credentials: {}", error.message))?;
    eprintln!("GraphLake administrator credentials saved (username: {USERNAME})");
    Ok(())
}

struct Session {
    created: Instant,
    last_used: Instant,
}

impl Session {
    fn active(&self, now: Instant) -> bool {
        now.duration_since(self.created) < LIFETIME && now.duration_since(self.last_used) < IDLE
    }
}

#[derive(Default)]
struct Sessions {
    tokens: HashMap<Token, Session>,
    failures: VecDeque<Instant>,
}

impl Sessions {
    fn check(&mut self, token: Token, now: Instant, touch: bool) -> bool {
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

    fn insert(&mut self, token: Token, now: Instant) {
        self.tokens.retain(|_, session| session.active(now));
        if self.tokens.len() >= MAX_SESSIONS
            && let Some(oldest) = self
                .tokens
                .iter()
                .min_by_key(|(_, session)| session.last_used)
                .map(|(token, _)| *token)
        {
            self.tokens.remove(&oldest);
        }
        self.tokens.insert(
            token,
            Session {
                created: now,
                last_used: now,
            },
        );
    }

    fn retry_after(&mut self, now: Instant) -> Option<u64> {
        self.failures
            .retain(|time| now.duration_since(*time) < FAILURE_WINDOW);
        (self.failures.len() >= MAX_FAILURES)
            .then(|| (FAILURE_WINDOW - now.duration_since(self.failures[0])).as_secs() + 1)
    }
}

pub(super) struct Auth {
    _store: Store,
    hash: Option<String>,
    cookie: String,
    sessions: Mutex<Sessions>,
    password_slot: Arc<Semaphore>,
}

impl Auth {
    pub(super) fn load(catalog: &Path, port: u16, insecure_dev: bool) -> Result<Self> {
        let store = Store::lock(catalog)?;
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
            let parsed = PasswordHash::new(&hash).map_err(anyhow::Error::msg)?;
            ensure!(
                parsed.algorithm.as_str() == "argon2id"
                    && parsed.version == Some(19)
                    && parsed.salt.is_some()
                    && parsed.hash.is_some_and(|output| output.len() == 32)
                    && Params::try_from(&parsed).map_err(anyhow::Error::msg)? == *argon2().params(),
                "unsupported administrator credential format; run auth reset-password"
            );
            Some(hash)
        };
        Ok(Self {
            _store: store,
            hash,
            cookie: format!("graphlake_admin_{port}"),
            sessions: Mutex::default(),
            password_slot: Arc::new(Semaphore::new(1)),
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
                    token = Some(bytes);
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
            "{}={value}; Path=/api; HttpOnly; SameSite=Strict; Max-Age={age}",
            self.cookie
        ))
        .expect("cookie uses ASCII")
    }

    pub(super) fn authenticated(&self, headers: &HeaderMap, touch: bool) -> bool {
        self.hash.is_none()
            || self.token(headers).is_some_and(|token| {
                self.sessions
                    .lock()
                    .unwrap()
                    .check(token, Instant::now(), touch)
            })
    }

    pub(super) fn public(request: &Request) -> bool {
        matches!(
            (request.method(), request.uri().path()),
            (&Method::GET | &Method::HEAD, "/api/v1/auth/session")
                | (&Method::POST, "/api/v1/auth/login" | "/api/v1/auth/logout")
        )
    }

    fn revoke(&self, headers: &HeaderMap) {
        if let Some(token) = self.token(headers) {
            self.sessions.lock().unwrap().tokens.remove(&token);
        }
    }
}

#[derive(Serialize)]
pub(super) struct SessionStatus {
    authenticated: bool,
    enabled: bool,
    username: Option<&'static str>,
}

pub(super) async fn session(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
) -> Json<SessionStatus> {
    let enabled = app.auth.hash.is_some();
    let authenticated = app.auth.authenticated(&headers, false);
    Json(SessionStatus {
        authenticated,
        enabled,
        username: (enabled && authenticated).then_some(USERNAME),
    })
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
    let Ok(slot) = Arc::clone(&app.auth.password_slot).try_acquire_owned() else {
        return throttled(1);
    };
    let worker = tokio::task::spawn_blocking(move || {
        // The slot and failed-attempt accounting survive a disconnected request.
        let _slot = slot;
        let auth = &app.auth;
        if let Some(seconds) = auth.sessions.lock().unwrap().retry_after(Instant::now()) {
            return throttled(seconds);
        }
        let hash = PasswordHash::new(auth.hash.as_deref().unwrap()).expect("validated credentials");
        let valid = valid_password(&input.password)
            && argon2()
                .verify_password(input.password.as_bytes(), &hash)
                .is_ok();
        if !valid || input.username != USERNAME {
            auth.sessions
                .lock()
                .unwrap()
                .failures
                .push_back(Instant::now());
            return Error::new(
                StatusCode::UNAUTHORIZED,
                "invalid_credentials",
                "用户名或密码错误",
            )
            .into_response();
        }
        let Ok(token) = random::<32>() else {
            return Error::storage().into_response();
        };
        auth.revoke(&headers);
        auth.sessions.lock().unwrap().insert(token, Instant::now());
        (
            [(header::SET_COOKIE, auth.cookie_header(Some(token)))],
            Json(SessionStatus {
                authenticated: true,
                enabled: true,
                username: Some(USERNAME),
            }),
        )
            .into_response()
    });
    worker
        .await
        .unwrap_or_else(|_| Error::storage().into_response())
}

pub(super) async fn logout(State(app): State<Arc<App>>, headers: HeaderMap) -> Response {
    app.auth.revoke(&headers);
    (
        StatusCode::NO_CONTENT,
        [(header::SET_COOKIE, app.auth.cookie_header(None))],
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idle_and_absolute_expiry_are_independent_of_status_checks() {
        let start = Instant::now();
        let mut sessions = Sessions::default();
        let token = [1; 32];
        sessions.insert(token, start);
        assert!(sessions.check(token, start + IDLE - Duration::from_secs(1), false));
        assert!(!sessions.check(token, start + IDLE, true));
        sessions.insert(token, start);
        for minute in (15..480).step_by(15) {
            assert!(sessions.check(token, start + Duration::from_secs(minute * 60), true));
        }
        assert!(!sessions.check(token, start + LIFETIME, true));
    }
}
