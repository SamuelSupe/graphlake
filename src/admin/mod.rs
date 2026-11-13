//! Local HTTP interface for managing graph catalogs and running read-only queries.
mod access;
mod assets;
mod auth;
mod browse;
mod models;
mod ops;
mod query;
mod wire;

use std::{
    collections::{HashMap, VecDeque},
    net::{Ipv4Addr, SocketAddrV4},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, Request, State},
    http::{HeaderMap, Method, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{delete, get, post},
};
use serde::Serialize;
use tokio::{net::TcpListener, sync::Semaphore};

use crate::{CancellationToken, QueryOptions};
pub use auth::setup as setup_auth;

const CATALOG_LIMIT: usize = 1024 * 1024;

struct App {
    auth: auth::Auth,
    access: access::Policy,
    assets: assets::Assets,
    models: Mutex<models::Models>,
    work: Arc<Semaphore>,
    query_waiters: Semaphore,
    export_waiters: Semaphore,
    explore_work: Semaphore,
    explore_producers: Arc<Semaphore>,
    explore_recovery: Arc<Semaphore>,
    explorations: Mutex<query::Sessions>,
    metadata_work: Semaphore,
    export_work: Arc<Semaphore>,
    download_work: Arc<Semaphore>,
    download_waiters: Semaphore,
    downloads: Mutex<VecDeque<query::Download>>,
    results: Mutex<VecDeque<query::ResultSnapshot>>,
    active: Mutex<HashMap<uuid::Uuid, query::ActiveQuery>>,
    options: QueryOptions,
    authority: String,
    shutdown: CancellationToken,
    metrics: ops::Metrics,
    readiness: Arc<Semaphore>,
    wire: Option<wire::Access>,
}

/// Settings for the loopback service and its optional read-only driver endpoints.
pub struct ServiceOptions {
    pub port: u16,
    pub timeout_ms: u64,
    pub query_slots: usize,
    pub insecure_dev: bool,
    pub bolt_port: Option<u16>,
    pub driver_model: Option<String>,
    pub driver_token_env: String,
}

/// Serve the built UI and the catalog API on an IPv4 loopback address.
///
/// Both directories must exist. The assets directory must contain index.html.
pub async fn serve(
    catalog_dir: PathBuf,
    assets_dir: PathBuf,
    port: u16,
    timeout_ms: u64,
) -> Result<()> {
    serve_configured(
        catalog_dir,
        assets_dir,
        ServiceOptions::local(port, timeout_ms, false),
    )
    .await
}

/// Serve a credential-free test catalog on an IPv4 loopback address.
pub async fn serve_insecure_dev(
    catalog_dir: PathBuf,
    assets_dir: PathBuf,
    port: u16,
    timeout_ms: u64,
) -> Result<()> {
    serve_configured(
        catalog_dir,
        assets_dir,
        ServiceOptions::local(port, timeout_ms, true),
    )
    .await
}

impl ServiceOptions {
    fn local(port: u16, timeout_ms: u64, insecure_dev: bool) -> Self {
        Self {
            port,
            timeout_ms,
            insecure_dev,
            query_slots: 1,
            bolt_port: None,
            driver_model: None,
            driver_token_env: String::new(),
        }
    }
}

/// Start the local administration service and, when configured, driver endpoints.
pub async fn serve_configured(
    catalog_dir: PathBuf,
    assets_dir: PathBuf,
    settings: ServiceOptions,
) -> Result<()> {
    let ServiceOptions {
        port,
        timeout_ms,
        insecure_dev,
        query_slots,
        bolt_port,
        driver_model,
        driver_token_env,
    } = settings;
    ensure!((1..=60_000).contains(&timeout_ms), "invalid admin timeout");
    ensure!(
        (1..=8).contains(&query_slots),
        "query slots must be between 1 and 8"
    );
    ensure!(
        bolt_port.is_none() || driver_model.is_some(),
        "--bolt-port requires --driver-model"
    );
    let catalog_dir = catalog_dir
        .canonicalize()
        .context("opening catalog directory")?;
    ensure!(catalog_dir.is_dir(), "catalog directory is not a directory");
    let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port)).await?;
    let port = listener.local_addr()?.port();
    let auth = auth::Auth::load(&catalog_dir, port, insecure_dev)?;
    let assets_dir = assets_dir
        .canonicalize()
        .context("opening assets directory")?;
    let assets = assets::Assets::load(&assets_dir, &catalog_dir)?;
    let authority = if port == 80 {
        Ipv4Addr::LOCALHOST.to_string()
    } else {
        SocketAddrV4::new(Ipv4Addr::LOCALHOST, port).to_string()
    };
    let access = access::Policy::load(
        &catalog_dir,
        driver_model
            .as_deref()
            .map(|model| (model, driver_token_env.as_str())),
    )?;
    let models = models::Models::new(catalog_dir);
    models
        .adopt_existing()
        .map_err(|error| anyhow::anyhow!("adopting existing models: {}", error.message))?;
    models
        .check_published()
        .map_err(|error| anyhow::anyhow!("checking published models: {}", error.message))?;
    let wire = driver_model
        .map(|id| wire::Access::load(&models, id))
        .transpose()?;
    let bolt = match bolt_port {
        Some(port) => Some(TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port)).await?),
        None => None,
    };
    let shutdown = CancellationToken::new();
    let app = Arc::new(App {
        auth,
        access,
        assets,
        models: Mutex::new(models),
        work: Arc::new(Semaphore::new(query_slots)),
        query_waiters: Semaphore::new(16),
        export_waiters: Semaphore::new(4),
        explore_work: Semaphore::new(8),
        explore_producers: Arc::new(Semaphore::new(2)),
        explore_recovery: Arc::new(Semaphore::new(2)),
        explorations: Mutex::default(),
        metadata_work: Semaphore::new(4),
        export_work: Arc::new(Semaphore::new(1)),
        download_work: Arc::new(Semaphore::new(1)),
        download_waiters: Semaphore::new(4),
        downloads: Mutex::default(),
        results: Mutex::default(),
        active: Mutex::default(),
        options: QueryOptions {
            timeout: Duration::from_millis(timeout_ms),
            cancellation: shutdown.clone(),
            ..Default::default()
        },
        authority: authority.clone(),
        shutdown: shutdown.clone(),
        metrics: ops::Metrics::new(query_slots),
        readiness: Arc::new(Semaphore::new(1)),
        wire,
    });
    let api = Router::new()
        .route("/v1/models", get(models::list).post(models::create))
        .route("/v1/models/validate", post(models::validate))
        .route("/v1/models/{id}", get(models::read).put(models::update))
        .route("/v1/models/{id}/schema/{revision}", get(models::schema))
        .route("/v1/models/{id}/query", post(query::run))
        .route("/v1/models/{id}/explore", post(query::explore))
        .route("/v1/models/{id}/export", post(query::export))
        .route("/v1/models/{id}/results/{result}", get(query::snapshot))
        .route(
            "/v1/models/{id}/results/{result}/graph/{page}",
            get(query::graph_page),
        )
        .route(
            "/v1/downloads/{ticket}",
            get(query::download).head(|| async { StatusCode::METHOD_NOT_ALLOWED }),
        )
        .route("/v1/models/{id}/history", get(models::history))
        .route(
            "/v1/models/{id}/history/{revision}",
            get(models::archived_yaml),
        )
        .route("/v1/models/{id}/publish", post(models::publish))
        .route("/v1/models/{id}/rollback", post(models::rollback))
        .route("/v1/models/{id}/browse", post(browse::browse))
        .route(
            "/v1/options",
            get(|State(app): State<Arc<App>>| async move {
                Json(serde_json::json!({ "timeout_ms": app.options.timeout.as_millis() as u64 }))
            }),
        )
        .route("/v1/queries/{id}", delete(query::cancel))
        .fallback(|| async { Error::not_found() })
        // JSON string escaping can expand a valid YAML document.
        .layer(DefaultBodyLimit::max(6 * CATALOG_LIMIT + 4096))
        .route("/v1/auth/session", get(auth::session))
        .route(
            "/v1/auth/login",
            post(auth::login).layer(DefaultBodyLimit::max(auth::LOGIN_BODY_LIMIT)),
        )
        .route("/v1/auth/logout", post(auth::logout));
    let router = Router::new()
        .nest("/api", api)
        .route("/healthz", get(ops::health))
        .route("/readyz", get(ops::ready))
        .route("/metrics", get(ops::metrics))
        .route("/gremlin", get(wire::gremlin::upgrade))
        .route(
            "/assets/{name}",
            get(
                |State(app): State<Arc<App>>, Path(name): Path<String>| async move {
                    app.assets.file(&name)
                },
            ),
        )
        .fallback(get(
            |State(app): State<Arc<App>>, uri: axum::http::Uri| async move {
                app.assets.page(uri.path())
            },
        ))
        .layer(middleware::from_fn_with_state(Arc::clone(&app), guard))
        .with_state(app.clone());
    let bolt = if let Some(listener) = bolt {
        eprintln!("GraphLake Bolt bolt://{}", listener.local_addr()?);
        Some(tokio::spawn(wire::bolt::serve(app.clone(), listener)))
    } else {
        None
    };
    eprintln!("GraphLake admin http://{authority}");
    if insecure_dev {
        eprintln!(
            "Warning: administrator authentication disabled for this test catalog (--insecure-dev)"
        );
    }
    let signal = tokio::spawn(ops::shutdown_signal(shutdown.clone()));
    let server =
        axum::serve(listener, router).with_graceful_shutdown(shutdown.clone().cancelled_owned());
    let result = tokio::select! {
        result = server => result,
        _ = async { shutdown.cancelled().await; tokio::time::sleep(Duration::from_secs(10)).await } => Ok(()),
    };
    shutdown.cancel();
    signal.abort();
    if let Some(bolt) = bolt {
        bolt.await??;
    }
    result?;
    Ok(())
}

async fn guard(State(app): State<Arc<App>>, mut request: Request, next: Next) -> Response {
    use access::Principal;
    let headers = request.headers();
    let path = request.uri().path();
    let is_api = path == "/api" || path.starts_with("/api/");
    let is_probe = matches!(path, "/healthz" | "/readyz" | "/metrics");
    let is_gremlin = path == "/gremlin";
    let started = std::time::Instant::now();
    let operation = is_api.then(|| access::operation(request.method(), path));
    let credential = headers.contains_key(header::AUTHORIZATION);
    let origin = headers.contains_key(header::ORIGIN);
    let public = auth::Auth::public(&request);
    let principal = if is_api && !public {
        if credential {
            app.access.http(headers)
        } else if app.auth.authenticated(headers, true) {
            Some(Principal::Admin)
        } else {
            None
        }
    } else if is_api && path == "/api/v1/auth/logout" && app.auth.authenticated(headers, false) {
        Some(Principal::Admin)
    } else {
        None
    };
    let mut response = if !exact_header(headers, header::HOST, &app.authority) {
        Error::new(
            StatusCode::FORBIDDEN,
            "invalid_host",
            "使用服务启动时显示的本机地址",
        )
        .into_response()
    } else if app.shutdown.is_cancelled() && !is_probe {
        Error::new(StatusCode::SERVICE_UNAVAILABLE, "draining", "服务正在停止").into_response()
    } else if is_api
        && !matches!(*request.method(), Method::GET | Method::HEAD)
        && (origin || !matches!(principal, Some(Principal::Service(_))))
        && !exact_header(
            headers,
            header::ORIGIN,
            &format!("http://{}", app.authority),
        )
    {
        Error::new(
            StatusCode::FORBIDDEN,
            "invalid_origin",
            "管理操作需要来自同一站点",
        )
        .into_response()
    } else if is_api && !public && principal.is_none() {
        Error::new(
            StatusCode::UNAUTHORIZED,
            "auth_required",
            "请登录 GraphLake 管理后台",
        )
        .into_response()
    } else if let (Some(principal @ Principal::Service(_)), Some(operation)) =
        (&principal, &operation)
        && (!operation.service
            || operation
                .model
                .as_deref()
                .is_some_and(|id| !app.access.permits(principal, id)))
    {
        access::forbidden().into_response()
    } else {
        if let Some(principal) = &principal {
            request.extensions_mut().insert(principal.clone());
        }
        next.run(request).await
    };
    if let Some(operation) = operation
        && operation.action != "session"
    {
        let status = response.status();
        let principal = if operation.action == "login" && status.is_success() {
            Some(&Principal::Admin)
        } else {
            principal.as_ref()
        };
        let model = response
            .extensions()
            .get::<access::AuditModel>()
            .map(|m| m.0.as_str())
            .or(operation.model.as_deref());
        app.access.record(
            principal,
            "http",
            operation.action,
            model,
            if status.is_success() { "ok" } else { "error" },
            status.as_u16(),
        );
    }
    if is_api {
        app.metrics.http(response.status(), started.elapsed());
    }
    if is_api || is_probe || is_gremlin {
        response
            .headers_mut()
            .insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
    }
    response
        .headers_mut()
        .insert(header::X_CONTENT_TYPE_OPTIONS, "nosniff".parse().unwrap());
    response
        .headers_mut()
        .insert(header::X_FRAME_OPTIONS, "DENY".parse().unwrap());
    response
}

fn exact_header(headers: &HeaderMap, key: header::HeaderName, expected: &str) -> bool {
    let mut values = headers.get_all(key).iter();
    values.next().is_some_and(|value| value == expected) && values.next().is_none()
}

type ApiResult<T> = std::result::Result<T, Error>;

#[derive(Clone)]
struct Error {
    status: StatusCode,
    code: &'static str,
    message: String,
    location: Option<crate::catalog::ValidationSite>,
}

impl Error {
    fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
            location: None,
        }
    }

    fn invalid(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(StatusCode::UNPROCESSABLE_ENTITY, code, message)
    }

    fn not_found() -> Self {
        Self::new(StatusCode::NOT_FOUND, "not_found", "未找到请求的资源")
    }

    fn busy() -> Self {
        Self::new(
            StatusCode::TOO_MANY_REQUESTS,
            "busy",
            "当前操作的执行槽已满，请稍后重试",
        )
    }

    fn storage() -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "storage",
            "读取或写入模型目录失败",
        )
    }
}

#[derive(Serialize)]
struct ErrorBody {
    code: &'static str,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    location: Option<crate::catalog::ValidationSite>,
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ErrorBody {
                code: self.code,
                message: self.message,
                location: self.location,
            }),
        )
            .into_response()
    }
}
