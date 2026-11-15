//! Local HTTP interface for managing graph catalogs and running read-only queries.
mod access;
mod assets;
mod auth;
mod backup;
mod browse;
pub(crate) mod cluster;
mod ha;
mod mcp;
mod models;
mod ops;
mod query;
mod recovery;
mod telemetry;
mod users;
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
    http::{HeaderMap, HeaderValue, Method, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::Serialize;
use tokio::{net::TcpListener, sync::Semaphore};

use crate::{CancellationToken, QueryOptions};
pub use auth::setup as setup_auth;
pub use backup::{check as check_backup, ingress as check_ingress, run as cluster_backup};
pub use backup::{
    check_checkpoint as verify_backup_checkpoint, record_status as record_backup_status,
};
pub use cluster::serve_worker;
pub use recovery::fingerprint_postgres as recovery_fingerprint_postgres;
pub use telemetry::{
    DEFAULT_FILE as DEFAULT_TELEMETRY_FILE, init as init_telemetry,
    report_exit as report_telemetry_exit, shutdown as shutdown_telemetry,
};

const CATALOG_LIMIT: usize = 1024 * 1024;

struct App {
    auth: Arc<auth::Auth>,
    users: users::Users,
    access: access::Policy,
    assets: Arc<assets::Assets>,
    models: Mutex<models::Models>,
    work: Arc<Semaphore>,
    query_waiters: Semaphore,
    export_waiters: Semaphore,
    explore_work: Semaphore,
    explore_producers: Arc<Semaphore>,
    explore_recovery: Arc<Semaphore>,
    explorations: Mutex<query::Sessions>,
    metadata_work: Semaphore,
    model_probes: Semaphore,
    export_work: Arc<Semaphore>,
    download_work: Arc<Semaphore>,
    download_waiters: Semaphore,
    downloads: Mutex<VecDeque<query::Download>>,
    results: Mutex<VecDeque<query::ResultSnapshot>>,
    active: Mutex<HashMap<uuid::Uuid, query::ActiveQuery>>,
    options: QueryOptions,
    authority: String,
    origin: String,
    shutdown: CancellationToken,
    metrics: ops::Metrics,
    readiness: Arc<Semaphore>,
    manual_backup_dir: Option<PathBuf>,
    backup_work: Arc<Semaphore>,
    wire: Option<wire::Access>,
    cluster: Option<cluster::Coordinator>,
    ha: Option<ha::Ha>,
    _ha_bootstrap: Option<Arc<tempfile::TempDir>>,
    ha_ready: std::sync::atomic::AtomicBool,
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
    pub public_url: Option<String>,
    pub coordinator_failover: bool,
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
            public_url: None,
            coordinator_failover: false,
        }
    }
}

/// Start the local administration service and, when configured, driver endpoints.
pub async fn serve_configured(
    catalog_dir: PathBuf,
    assets_dir: PathBuf,
    settings: ServiceOptions,
) -> Result<()> {
    serve_clustered(catalog_dir, assets_dir, settings, None).await
}

/// Serve the administration API with an optional multi-worker configuration.
pub async fn serve_clustered(
    catalog_dir: PathBuf,
    assets_dir: PathBuf,
    settings: ServiceOptions,
    cluster_config: Option<PathBuf>,
) -> Result<()> {
    serve_clustered_with_status(catalog_dir, assets_dir, settings, cluster_config, None).await
}

/// Start the service with an optional loopback status listener that also covers HA standby.
pub async fn serve_clustered_with_status(
    catalog_dir: PathBuf,
    assets_dir: PathBuf,
    settings: ServiceOptions,
    cluster_config: Option<PathBuf>,
    status_port: Option<u16>,
) -> Result<()> {
    let ServiceOptions {
        port,
        timeout_ms,
        insecure_dev,
        query_slots,
        bolt_port,
        driver_model,
        driver_token_env,
        public_url,
        coordinator_failover,
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
    ensure!(
        !coordinator_failover || (cluster_config.is_some() && public_url.is_some()),
        "--coordinator-failover requires --cluster-config and --public-url"
    );
    ensure!(
        !insecure_dev || (cluster_config.is_none() && public_url.is_none()),
        "cluster and HTTPS proxy modes require administrator authentication"
    );
    let public_url = public_url.as_deref().map(parse_public_url).transpose()?;
    let shutdown = CancellationToken::new();
    let _shutdown_on_exit = shutdown.clone().drop_guard();
    let signal = tokio::spawn(ops::shutdown_signal(shutdown.clone()));
    let elections = coordinator_failover
        .then(|| {
            cluster::control::ElectionMetrics::load(
                std::env::var_os("GRAPHLAKE_HA_METRICS_FILE").map(PathBuf::from),
            )
            .map(Arc::new)
        })
        .transpose()?;
    let status = if let Some(port) = status_port {
        Some(
            ops::serve_node_status(
                port,
                coordinator_failover,
                elections.clone(),
                shutdown.clone(),
            )
            .await?,
        )
    } else {
        None
    };
    let cluster = cluster_config
        .as_deref()
        .map(cluster::Coordinator::load)
        .transpose()?;
    let ha_cache = coordinator_failover
        .then(tempfile::tempdir)
        .transpose()?
        .map(Arc::new);
    let metadata_dir = ha_cache
        .as_ref()
        .map_or(catalog_dir.as_path(), |dir| dir.path());
    let assets_dir = assets_dir
        .canonicalize()
        .context("opening assets directory")?;
    let assets = Arc::new(assets::Assets::load(&assets_dir, &catalog_dir)?);
    let mut listener = None;
    let ha = if coordinator_failover {
        let control = cluster.as_ref().unwrap().control()?;
        // Wait until this host can access the shared source before contending.
        loop {
            ensure!(
                !shutdown.is_cancelled(),
                "Coordinator stopped before activation"
            );
            if ha::source_available(catalog_dir.clone()).await {
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        let leader = control.acquire_with_metrics(&shutdown, elections).await?;
        ha::watch_source(catalog_dir.clone(), shutdown.clone());
        if let Some(public_url) = &public_url
            && std::env::var_os("GRAPHLAKE_RESTORE_INBOX").is_some()
            && leader
                .get::<String>("catalog")
                .await
                .map_err(|e| anyhow::anyhow!(e.message))?
                .is_none()
        {
            let stage_listener =
                TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port)).await?;
            let bound = stage_listener.local_addr()?;
            if let Err(error) = recovery::import::serve(
                stage_listener,
                assets.clone(),
                leader.clone(),
                public_url.origin().ascii_serialization(),
                catalog_dir.clone(),
                shutdown.clone(),
            )
            .await
            {
                leader.release().await;
                return Err(error);
            }
            listener = Some(TcpListener::bind(bound).await?);
        }
        let ha = ha::Ha::open(leader, &catalog_dir, metadata_dir).await?;
        ha::credentials(&ha.leader, &catalog_dir, metadata_dir).await?;
        Some(ha)
    } else {
        None
    };
    // A standby must not expose a listener before obtaining etcd leadership.
    let listener = match listener {
        Some(listener) => listener,
        None => TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port)).await?,
    };
    let cookie_port = match &public_url {
        Some(url) => url.port_or_known_default().unwrap_or(443),
        None => listener.local_addr()?.port(),
    };
    let auth = Arc::new(
        auth::Auth::load(
            metadata_dir,
            cookie_port,
            insecure_dev,
            public_url.is_some(),
            ha.as_ref().map(|ha| ha.leader.clone()),
        )
        .await?,
    );
    let users = users::Users::load(&catalog_dir, ha.as_ref()).await?;
    let origin = match public_url {
        Some(url) => url.origin().ascii_serialization(),
        None => {
            let addr = listener.local_addr()?;
            if addr.port() == 80 {
                format!("http://{}", addr.ip())
            } else {
                format!("http://{addr}")
            }
        }
    };
    let authority = origin.split_once("://").unwrap().1.to_owned();
    let access = access::Policy::load(
        metadata_dir,
        driver_model
            .as_deref()
            .map(|model| (model, driver_token_env.as_str())),
        coordinator_failover,
    )?;
    let models = match &ha_cache {
        Some(snapshot) => models::Models::in_snapshot(Arc::clone(snapshot), catalog_dir),
        None => models::Models::new(catalog_dir),
    };
    if ha.is_none() {
        models
            .adopt_existing()
            .map_err(|error| anyhow::anyhow!("adopting existing models: {}", error.message))?;
    }
    let (_, failed_models) = models
        .check_published()
        .map_err(|error| anyhow::anyhow!("checking published models: {}", error.message))?;
    for (id, error) in failed_models {
        eprintln!("GraphLake model {id} unavailable: {error}");
    }
    let wire = driver_model
        .map(|id| wire::Access::load(&models, id))
        .transpose()?;
    let bolt = match bolt_port {
        Some(port) => Some(TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port)).await?),
        None => None,
    };
    if let Some(cluster) = &cluster {
        cluster.start_cleanup(shutdown.clone());
    }
    let explorations = cluster
        .as_ref()
        .map_or_else(query::Sessions::default, |cluster| {
            query::Sessions::with_cursor_key(cluster.cursor_key())
        });
    let app = Arc::new(App {
        auth,
        users,
        access,
        assets,
        models: Mutex::new(models),
        work: Arc::new(Semaphore::new(query_slots)),
        query_waiters: Semaphore::new(16),
        export_waiters: Semaphore::new(4),
        explore_work: Semaphore::new(8),
        explore_producers: Arc::new(Semaphore::new(2)),
        explore_recovery: Arc::new(Semaphore::new(2)),
        explorations: Mutex::new(explorations),
        metadata_work: Semaphore::new(4),
        model_probes: Semaphore::new(2),
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
        authority,
        origin: origin.clone(),
        shutdown: shutdown.clone(),
        metrics: ops::Metrics::new(query_slots),
        readiness: Arc::new(Semaphore::new(1)),
        manual_backup_dir: ha
            .as_ref()
            .and_then(|_| std::env::var_os("GRAPHLAKE_MANUAL_BACKUP_DIR").map(PathBuf::from)),
        backup_work: Arc::new(Semaphore::new(1)),
        wire,
        cluster,
        ha,
        _ha_bootstrap: ha_cache,
        ha_ready: std::sync::atomic::AtomicBool::new(false),
    });
    if let Some(status) = status {
        *status.lock().unwrap() = Arc::downgrade(&app);
    }
    if app.ha.is_some() {
        tokio::spawn(ha::watch_backend(Arc::downgrade(&app)));
    }
    tokio::spawn(access::Policy::watch(Arc::downgrade(&app)));
    let api = Router::new()
        .route("/v1/models", get(models::list).post(models::create))
        .route("/v1/models/validate", post(models::validate))
        .route("/v1/models/{id}", get(models::read).put(models::update))
        .route("/v1/models/{id}/schema/{revision}", get(models::schema))
        .route("/v1/models/{id}/query", post(query::run))
        .route(
            "/v1/models/{id}/csr",
            get(cluster::csr::current).post(cluster::csr::refresh),
        )
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
            get(models::archived_yaml).put(models::restore_archive),
        )
        .route("/v1/models/{id}/publish", post(models::publish))
        .route("/v1/models/{id}/rollback", post(models::rollback))
        .route("/v1/models/{id}/unpublish", post(models::unpublish))
        .route("/v1/models/{id}/readiness", get(ops::model_ready))
        .route("/v1/models/{id}/browse", post(browse::browse))
        .route("/v1/backups/status", get(backup::overview))
        .route("/v1/backups/export", post(backup::export))
        .route("/v1/mcp/access", get(mcp::settings::access))
        .route("/v1/mcp/token", get(users::delegation::status).post(users::delegation::issue).delete(users::delegation::revoke)
            .layer(DefaultBodyLimit::max(4096)))
        .route("/v1/mcp/settings", get(mcp::settings::read).put(mcp::settings::save)
            .layer(DefaultBodyLimit::max(4096)))
        .route("/v1/settings", get(telemetry::settings::read).put(telemetry::settings::save)
            .layer(DefaultBodyLimit::max(32 * 1024)))
        .route(
            "/v1/options",
            get(|State(app): State<Arc<App>>| async move {
                Json(serde_json::json!({ "timeout_ms": app.options.timeout.as_millis() as u64, "query_recovery": app.cluster.is_some(), "mutation_receipts": app.ha.is_some() }))
            }),
        )
        .route(
            "/v1/queries/{id}",
            get(cluster::resume).delete(query::cancel),
        )
        .fallback(|| async { Error::not_found() })
        // JSON string escaping can expand a valid YAML document.
        .layer(DefaultBodyLimit::max(6 * CATALOG_LIMIT + 4096))
        .route("/v1/auth/session", get(auth::session))
        .route(
            "/v1/auth/login",
            post(auth::login).layer(DefaultBodyLimit::max(auth::LOGIN_BODY_LIMIT)),
        )
        .route("/v1/auth/logout", post(auth::logout))
        .route("/v1/auth/sso", post(auth::sso))
        .route("/v1/mutations/{id}", get(ha::receipt))
        .route("/v1/my-roles", get(users::current_roles))
        .route("/v1/users", get(users::list))
        .route("/v1/roles", get(users::list_groups))
        .route("/v1/roles/{role}", axum::routing::put(users::update_group).delete(users::delete_group)
            .layer(DefaultBodyLimit::max(128 * 1024)))
        .route("/v1/users/{username}", axum::routing::put(users::update).delete(users::delete)
            .layer(DefaultBodyLimit::max(128 * 1024)))
        .route("/v1/service-accounts", axum::routing::put(ha::change_policy)
            .layer(DefaultBodyLimit::max(256 * 1024)));
    let mcp_route = post(mcp::handle)
        .get(mcp::no_stream)
        .delete(mcp::no_stream)
        .layer(DefaultBodyLimit::max(6 * CATALOG_LIMIT + 4096));
    let router = Router::new()
        .nest("/api", api)
        .route("/mcp", mcp_route.clone())
        .route("/mcp/query", mcp_route.clone())
        .route("/mcp/modeling", mcp_route)
        .route("/healthz", get(ops::health))
        .route("/readyz", get(ops::ready))
        .route("/coordinator/active", get(ha::active))
        .route("/coordinator/recovery", get(recovery::probe))
        .route("/recovery/status", get(recovery::status))
        .route("/recovery/check", post(recovery::preflight))
        .route("/recovery/verify", post(recovery::verify))
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
    eprintln!("GraphLake admin {origin}");
    ops::register_coordinator_metrics(&app);
    telemetry::event(
        opentelemetry::logs::Severity::Info,
        "service_started",
        serde_json::json!({
            "distributed": app.cluster.is_some(), "failover": app.ha.is_some(), "query_slots": query_slots
        }),
    );
    if insecure_dev {
        eprintln!(
            "Warning: administrator authentication disabled for this test catalog (--insecure-dev)"
        );
    }
    let server =
        axum::serve(listener, router).with_graceful_shutdown(shutdown.clone().cancelled_owned());
    let result = tokio::select! {
        result = server => result,
        _ = async { shutdown.cancelled().await; tokio::time::sleep(Duration::from_secs(10)).await } => Ok(()),
    };
    shutdown.cancel();
    signal.abort();
    if let Some(ha) = &app.ha {
        ha.leader.release().await;
    }
    if let Some(bolt) = bolt {
        bolt.await??;
    }
    result?;
    ensure!(!coordinator_failover, "Coordinator relinquished leadership");
    Ok(())
}

async fn guard(State(app): State<Arc<App>>, request: Request, next: Next) -> Response {
    use opentelemetry::trace::FutureExt as _;
    let path = request.uri().path();
    let api = (path == "/api" || path.starts_with("/api/"))
        .then(|| access::operation(request.method(), path));
    let operation = api.as_ref().map(|op| op.action).or_else(|| match path {
        "/readyz" => Some("readiness"),
        "/coordinator/active" => Some("coordinator_active"),
        "/coordinator/recovery" => Some("recovery_probe"),
        "/recovery/status" => Some("recovery_status"),
        "/recovery/check" => Some("recovery_check"),
        "/recovery/verify" => Some("recovery_verify"),
        "/gremlin" => Some("gremlin_upgrade"),
        "/mcp" | "/mcp/query" | "/mcp/modeling" => Some("mcp"),
        _ => None,
    });
    let started = std::time::Instant::now();
    let span = operation
        .filter(|op| !matches!(*op, "readiness" | "coordinator_active" | "session"))
        .and_then(|op| telemetry::start_span(op, "http", Some(request.headers())));
    let future = guard_request(&app, request, next, api.as_ref());
    let response = if let Some(span) = &span {
        let response = future.with_context(span.clone()).await;
        telemetry::finish_span(span, response.status().as_u16());
        response
    } else {
        future.await
    };
    if let Some(operation) = operation {
        let elapsed = started.elapsed();
        app.metrics
            .observe("http", operation, response.status().as_u16(), elapsed);
        if api.is_some() {
            app.metrics.http(response.status(), elapsed);
        }
        if matches!(operation, "query" | "export")
            && let Some(code) = response.extensions().get::<ErrorCode>()
        {
            app.metrics.query_error(code.0);
        }
    }
    response
}

async fn await_leadership_pause(token: Option<&CancellationToken>) {
    match token {
        Some(token) => token.cancelled().await,
        None => std::future::pending().await,
    }
}

async fn guard_request(
    app: &Arc<App>,
    mut request: Request,
    next: Next,
    operation: Option<&access::Operation>,
) -> Response {
    use access::Principal;
    let headers = request.headers();
    let path = request.uri().path();
    let is_api = operation.is_some();
    let is_probe = matches!(
        path,
        "/healthz" | "/readyz" | "/metrics" | "/coordinator/active" | "/coordinator/recovery"
    );
    let is_gremlin = path == "/gremlin";
    let is_mcp = mcp::Service::for_path(path).is_some();
    let recovery_rpc = matches!(
        (request.method(), path),
        (&Method::GET | &Method::HEAD, "/recovery/status")
            | (&Method::POST, "/recovery/check" | "/recovery/verify")
    );
    let recovery_assets = matches!(*request.method(), Method::GET | Method::HEAD)
        && (matches!(path, "/recovery" | "/recovery/") || path.starts_with("/assets/"));
    let origin = headers.contains_key(header::ORIGIN);
    let valid_host = exact_header(headers, header::HOST, &app.authority);
    let durable_mutation = operation.as_ref().is_some_and(|value| {
        matches!(
            value.action,
            "create_model"
                | "save_model"
                | "publish"
                | "rollback"
                | "unpublish"
                | "restore_archive"
        )
    });
    let public = auth::Auth::public(&request);
    let basic = !public && headers.contains_key(header::AUTHORIZATION);
    if valid_host
        && !is_probe
        && !recovery_rpc
        && !recovery_assets
        && app.ha.as_ref().is_some_and(ha::Ha::recovery_pending)
    {
        let mut response = recovery::blocked().into_response();
        response
            .headers_mut()
            .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
        return response;
    }
    let credential = (valid_host && (is_api || is_mcp) && basic)
        .then(|| {
            if is_mcp {
                mcp::authenticate(app, headers)
            } else {
                access::http(&app, headers)
            }
        })
        .flatten();
    let mut live = credential
        .as_ref()
        .map(|credential| credential.live.clone());
    let same_origin = exact_header(headers, header::ORIGIN, &app.origin);
    let read = matches!(*request.method(), Method::GET | Method::HEAD);
    let cookie_auth = valid_host
        && is_api
        && (read || same_origin)
        && !headers.contains_key(header::AUTHORIZATION)
        && (!public || path == "/api/v1/auth/logout");
    let principal = if let Some(credential) = credential {
        Some(credential.principal)
    } else if cookie_auth {
        app.auth
            .principal(&app.users, headers, !public && app.ha.is_none())
            .await
    } else {
        None
    };
    let mut principal = principal;
    if let Some(user @ Principal::User { .. }) = &principal
        && live.is_none()
    {
        live = app.users.watch(user);
        if live.is_none() {
            principal = None;
        }
    }
    let mut response = if !valid_host {
        Error::new(
            StatusCode::FORBIDDEN,
            "invalid_host",
            "使用服务启动时显示的地址",
        )
        .into_response()
    } else if app.shutdown.is_cancelled() && !is_probe {
        Error::new(StatusCode::SERVICE_UNAVAILABLE, "draining", "服务正在停止").into_response()
    } else if !same_origin
        && (is_api && !read && (origin || !basic) || (recovery_rpc || is_mcp) && origin)
    {
        Error::new(
            StatusCode::FORBIDDEN,
            "invalid_origin",
            "管理操作需要来自同一站点",
        )
        .into_response()
    } else if (is_api && !public || is_mcp) && principal.is_none() {
        Error::new(
            StatusCode::UNAUTHORIZED,
            "auth_required",
            "请登录 GraphLake 管理后台",
        )
        .into_response()
    } else if let (Some(principal), Some(operation)) = (&principal, &operation)
        && !access::permits_operation(&app, principal, operation)
    {
        access::forbidden().into_response()
    } else {
        let leadership = if !is_probe && let Some(ha) = &app.ha {
            match ha.leader.admission() {
                Ok(token) => Some(token),
                Err(error) => {
                    let mut response = error.into_response();
                    response
                        .headers_mut()
                        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
                    return response;
                }
            }
        } else {
            None
        };
        if leadership.is_some()
            && cookie_auth
            && !public
            && app
                .auth
                .principal(&app.users, request.headers(), true)
                .await
                != principal
        {
            return Error::new(
                StatusCode::UNAUTHORIZED,
                "auth_required",
                "请登录 GraphLake 管理后台",
            )
            .into_response();
        }
        if let Some(principal) = &principal {
            request.extensions_mut().insert(principal.clone());
        }
        let result = tokio::select! {
            biased;
            () = await_leadership_pause(leadership.as_ref()), if !durable_mutation => cluster::control::interrupted().into_response(),
            () = app.shutdown.cancelled(), if app.ha.is_some() && !is_probe && !durable_mutation => cluster::control::lost().into_response(),
            () = access::revoked(&mut live) => access::revocation_error().into_response(),
            result = next.run(request) => result,
        };
        if (live.is_some() || leadership.is_some())
            && result.status().is_success()
            && result.status() != StatusCode::NO_CONTENT
        {
            use futures::StreamExt;
            let (parts, body) = result.into_parts();
            let body = axum::body::Body::from_stream(async_stream::stream! {
                let mut chunks = body.into_data_stream();
                loop {
                    let chunk = tokio::select! {
                        biased;
                        _ = access::revoked(&mut live) => Some(Err(axum::Error::new(std::io::Error::other("credentials revoked")))),
                        _ = await_leadership_pause(leadership.as_ref()), if !durable_mutation => Some(Err(axum::Error::new(std::io::Error::other("leadership interrupted")))),
                        chunk = chunks.next() => chunk,
                    };
                    let Some(chunk) = chunk else { break };
                    let failed = chunk.is_err();
                    yield chunk;
                    if failed { break; }
                }
            });
            Response::from_parts(parts, body)
        } else {
            result
        }
    };
    if let Some(operation) = operation
        && operation.action != "session"
    {
        let status = response.status();
        let principal = if matches!(operation.action, "login" | "sso_login") && status.is_success()
        {
            response.extensions().get::<Principal>()
        } else {
            principal.as_ref()
        };
        let model = response
            .extensions()
            .get::<access::AuditModel>()
            .map(|m| m.0.as_str())
            .or(operation.model.as_deref());
        app.access.record_target(
            principal,
            "http",
            operation.action,
            model,
            if status.is_success() { "ok" } else { "error" },
            status.as_u16(),
            operation.target.as_deref(),
        );
    }
    if is_api || is_probe || is_gremlin || is_mcp || recovery_rpc {
        response
            .headers_mut()
            .insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
    }
    if is_mcp && !response.status().is_success() {
        app.access.record(
            principal.as_ref(),
            "mcp",
            "request",
            None,
            "error",
            response.status().as_u16(),
        );
        if response.status() == StatusCode::UNAUTHORIZED {
            response.headers_mut().insert(
                header::WWW_AUTHENTICATE,
                HeaderValue::from_static("Bearer realm=\"GraphLake MCP\""),
            );
        }
    }
    response
        .headers_mut()
        .insert(header::X_CONTENT_TYPE_OPTIONS, "nosniff".parse().unwrap());
    response
        .headers_mut()
        .insert(header::X_FRAME_OPTIONS, "DENY".parse().unwrap());
    response
}

fn parse_public_url(value: &str) -> Result<url::Url> {
    let url = url::Url::parse(value).context("invalid --public-url")?;
    ensure!(
        url.scheme() == "https"
            && url.host().is_some()
            && url.username().is_empty()
            && url.password().is_none()
            && url.path() == "/"
            && url.query().is_none()
            && url.fragment().is_none(),
        "--public-url must be an HTTPS origin, for example https://graphlake.example"
    );
    Ok(url)
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

#[derive(Clone)]
struct ErrorCode(&'static str);

impl ErrorCode {
    fn class(code: &str) -> Self {
        Self(match code {
            "source_unavailable" => "source_unavailable",
            "query_failed" | "invalid_query" => "configuration",
            _ => "other",
        })
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        (
            self.status,
            axum::Extension(ErrorCode::class(self.code)),
            Json(ErrorBody {
                code: self.code,
                message: self.message,
                location: self.location,
            }),
        )
            .into_response()
    }
}
