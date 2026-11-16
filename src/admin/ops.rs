use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
    time::{Duration, Instant},
};

use axum::{
    Json,
    extract::{Extension, Path, State},
    http::{StatusCode, header},
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::{ApiResult, App, Error, access::Principal, models::versions, telemetry};
use crate::CancellationToken;

#[derive(Deserialize, Serialize)]
pub(super) struct SourceProbe {
    sources: std::collections::BTreeMap<String, bool>,
    status: ProbeStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    location: Option<serde_json::Value>,
}

#[derive(Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ProbeStatus {
    Ready,
    SourceUnavailable,
    MappingInvalid,
}

impl SourceProbe {
    pub(super) fn ready(&self) -> bool {
        self.status == ProbeStatus::Ready
    }

    pub(super) async fn run(
        engine: crate::Engine,
        mut options: crate::QueryOptions,
        admission: &Arc<crate::engine::Admission>,
    ) -> ApiResult<Self> {
        options.timeout = options.timeout.min(Duration::from_secs(5));
        limit_model_probe(&mut options);
        let (sources, mapping) = Box::pin(engine.probe_model_sources(options, admission))
            .await
            .map_err(|_| {
                Error::new(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "model_probe_incomplete",
                    "未能在探针期限内完成来源检查",
                )
            })?;
        let status = if !sources.values().all(|available| *available) {
            ProbeStatus::SourceUnavailable
        } else if mapping.is_err() {
            ProbeStatus::MappingInvalid
        } else {
            ProbeStatus::Ready
        };
        let location = mapping.err().and_then(|error| {
            serde_json::to_value(error.downcast_ref::<crate::catalog::ValidationSite>()?).ok()
        });
        Ok(Self {
            sources,
            status,
            location,
        })
    }
}

pub(super) const MODEL_PROBE_MEMORY_BYTES: usize = 32 * 1024 * 1024;

pub(super) fn limit_model_probe(options: &mut crate::QueryOptions) {
    options.memory_bytes = options.memory_bytes.min(MODEL_PROBE_MEMORY_BYTES);
    options.spill_bytes = 0;
}

pub(super) struct Metrics {
    started: Instant,
    slots: usize,
    http: [AtomicU64; 5],
    http_micros: AtomicU64,
    driver: [[AtomicU64; 2]; 2],
    query_error: [AtomicU64; 3],
    latency: Mutex<BTreeMap<(&'static str, &'static str, &'static str), Latency>>,
    gc_failures: AtomicU64,
    gc_deleted: AtomicU64,
    gc_last_success_ms: AtomicU64,
}

#[derive(Default)]
struct Latency {
    buckets: [u64; telemetry::BUCKETS.len() + 1],
    sum: f64,
}

// Query contexts only expose I/O consumed by this process. A distributed query
// can add observations on several Workers and must not be counted as one API call here.
static QUERY_IO: [AtomicU64; 4] = [const { AtomicU64::new(0) }; 4];
const QUERY_IO_METRICS: [(&str, &str, &str); 4] = [
    (
        "graphlake_query_output_rows_total",
        "graphlake.query.output_rows",
        "{row}",
    ),
    (
        "graphlake_query_source_requests_total",
        "graphlake.query.source_requests",
        "{request}",
    ),
    (
        "graphlake_query_object_bytes_total",
        "graphlake.query.object_bytes",
        "By",
    ),
    (
        "graphlake_query_spilled_bytes_total",
        "graphlake.query.spilled_bytes",
        "By",
    ),
];

pub(super) fn observe_query_io(result: &crate::QueryResult) {
    let stats = result.metrics();
    let values = [
        stats.output_rows,
        stats.reads.requests,
        stats.reads.object_bytes,
        stats.spilled_bytes,
    ];
    for (counter, value) in QUERY_IO.iter().zip(values) {
        counter.fetch_add(value, Relaxed);
    }
    telemetry::query_io_span(values, stats.peak_reserved_bytes);
}

pub(super) fn register_query_io(meter: &opentelemetry::metrics::Meter) {
    for ((_, name, unit), counter) in QUERY_IO_METRICS.into_iter().zip(&QUERY_IO) {
        meter
            .u64_observable_counter(name)
            .with_unit(unit)
            .with_callback(move |observer| observer.observe(counter.load(Relaxed), &[]))
            .build();
    }
}

impl Metrics {
    pub(super) fn new(slots: usize) -> Self {
        Self {
            started: Instant::now(),
            slots,
            http: Default::default(),
            http_micros: 0.into(),
            driver: Default::default(),
            query_error: Default::default(),
            latency: Default::default(),
            gc_failures: 0.into(),
            gc_deleted: 0.into(),
            gc_last_success_ms: 0.into(),
        }
    }

    pub(super) fn gc_deleted(&self) {
        self.gc_deleted.fetch_add(1, Relaxed);
    }

    pub(super) fn gc_finished(&self, at_ms: Option<u64>) {
        if let Some(at_ms) = at_ms {
            self.gc_last_success_ms.store(at_ms, Relaxed);
        } else {
            self.gc_failures.fetch_add(1, Relaxed);
        }
    }

    pub(super) fn slots(&self) -> usize {
        self.slots
    }

    pub(super) fn http(&self, status: StatusCode, elapsed: Duration) {
        if let Some(count) = self.http.get(usize::from(status.as_u16() / 100 - 1)) {
            count.fetch_add(1, Relaxed);
        }
        self.http_micros
            .fetch_add(elapsed.as_micros() as u64, Relaxed);
    }

    pub(super) fn driver(&self, language: crate::Language, ok: bool) {
        let index = usize::from(matches!(language, crate::Language::Gremlin));
        self.driver[index][usize::from(!ok)].fetch_add(1, Relaxed);
    }

    pub(super) fn query_error(&self, code: &str) {
        let index = match code {
            "source_unavailable" => 0,
            "configuration" => 1,
            _ => 2,
        };
        self.query_error[index].fetch_add(1, Relaxed);
    }

    pub(super) fn observe(
        &self,
        protocol: &'static str,
        operation: &'static str,
        status: u16,
        elapsed: Duration,
    ) {
        let outcome = match status {
            100..=399 => "ok",
            401 | 403 => "denied",
            409 => "conflict",
            429 => "busy",
            499 => "abandoned",
            504 => "timeout",
            400..=499 => "invalid",
            _ => "error",
        };
        let seconds = elapsed.as_secs_f64();
        let index = telemetry::BUCKETS.partition_point(|upper| *upper < seconds);
        {
            let mut latency = self.latency.lock().unwrap();
            let histogram = latency.entry((protocol, operation, outcome)).or_default();
            histogram.buckets[index] += 1;
            histogram.sum += seconds;
        }
        telemetry::duration(protocol, operation, outcome, seconds);
    }
}

pub(super) fn register_coordinator_metrics(app: &Arc<App>) {
    let Some(meter) = telemetry::meter() else {
        return;
    };
    register_query_io(&meter);
    let weak = Arc::downgrade(app);
    meter
        .u64_observable_gauge("graphlake.query.slots")
        .with_callback(move |observer| {
            if let Some(app) = weak.upgrade() {
                observer.observe(app.metrics.slots as u64, &[]);
            }
        })
        .build();
    let weak = Arc::downgrade(app);
    meter
        .u64_observable_gauge("graphlake.queries")
        .with_callback(move |observer| {
            if let Some(app) = weak.upgrade() {
                let (active, waiting) = query_counts(&app);
                for (state, count) in [("active", active), ("waiting", waiting)] {
                    observer.observe(
                        count as u64,
                        &[opentelemetry::KeyValue::new("state", state)],
                    );
                }
            }
        })
        .build();
    let weak = Arc::downgrade(app);
    meter
        .u64_observable_counter("graphlake.audit.write_failures")
        .with_callback(move |observer| {
            if let Some(app) = weak.upgrade() {
                observer.observe(app.access.audit_failures(), &[]);
            }
        })
        .build();
    if app.ha.is_some() {
        let weak = Arc::downgrade(app);
        meter
            .u64_observable_gauge("graphlake.coordinator.ready")
            .with_callback(move |observer| {
                if let Some(app) = weak.upgrade() {
                    observer.observe(u64::from(coordinator_ready(&app)), &[]);
                }
            })
            .build();
    }
}

fn coordinator_ready(app: &App) -> bool {
    app.ha.as_ref().is_some_and(|ha| {
        ha.leader.eligible()
            && !ha.recovery_pending()
            && app.ha_ready.load(std::sync::atomic::Ordering::Acquire)
    })
}

pub(super) async fn serve_node_status(
    port: u16,
    ha: bool,
    elections: Option<Arc<super::cluster::control::ElectionMetrics>>,
    shutdown: CancellationToken,
) -> anyhow::Result<Arc<Mutex<Weak<App>>>> {
    let state = Arc::new(Mutex::new(Weak::<App>::new()));
    let state_for_route = state.clone();
    let router = axum::Router::new()
        .route(
            "/status",
            axum::routing::get(move || {
                let state = state_for_route.clone();
                async move {
                    let app = state.lock().unwrap().upgrade();
                    let (role, ready) = match app {
                        Some(app) if app.shutdown.is_cancelled() => ("draining", false),
                        Some(app) if app.ha.as_ref().is_some_and(|h| h.leader.eligible()) => {
                            ("leader", coordinator_ready(&app))
                        }
                        Some(app) if app.ha.is_some() => ("leader_uncertain", false),
                        Some(_) => ("standalone", true),
                        None if ha => ("candidate", false),
                        None => ("starting", false),
                    };
                    Json(json!({ "role": role, "ready": ready }))
                }
            }),
        )
        .route(
            "/metrics",
            axum::routing::get(move || {
                let elections = elections.clone();
                async move {
                    (
                        [(
                            axum::http::header::CONTENT_TYPE,
                            "text/plain; version=0.0.4; charset=utf-8",
                        )],
                        elections.map_or_else(String::new, |metrics| metrics.render()),
                    )
                }
            }),
        );
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port)).await?;
    tokio::spawn(async move {
        let _ = axum::serve(listener, router)
            .with_graceful_shutdown(shutdown.cancelled_owned())
            .await;
    });
    Ok(state)
}

fn query_counts(app: &App) -> (usize, usize) {
    let queries = app.active.lock().unwrap();
    let active = queries.values().filter(|query| query.running).count();
    (active, queries.len() - active)
}

pub(super) async fn health() -> Json<serde_json::Value> {
    Json(json!({
        "status": "alive",
        "version": env!("CARGO_PKG_VERSION"),
        "build": option_env!("GRAPHLAKE_BUILD_VERSION"),
    }))
}

pub(super) async fn ready(State(app): State<Arc<App>>) -> impl IntoResponse {
    if app.shutdown.is_cancelled() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"status":"draining"})),
        );
    }
    if let Some(ha) = &app.ha {
        let reason = if ha.recovery_pending() {
            Some("recovery_pending")
        } else if !ha.leader.eligible() {
            Some("leadership_uncertain")
        } else {
            None
        };
        if let Some(reason) = reason {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"status":reason})),
            );
        }
    }
    let Ok(Ok(slot)) = tokio::time::timeout(
        Duration::from_millis(850),
        app.readiness.clone().acquire_owned(),
    )
    .await
    else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"status":"probe_in_progress"})),
        );
    };
    if let Some(cluster) = &app.cluster {
        if let Some(status) = cluster.readiness_failure().await {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"status":status})),
            );
        }
    }
    let dir = app.models.lock().unwrap().directory().to_path_buf();
    let checked = tokio::task::spawn_blocking(move || {
        let _slot = slot;
        let models = super::models::Models::new(dir);
        models.check_published()
    })
    .await;
    match checked {
        Ok(Ok((count, failures))) if failures.is_empty() => (
            StatusCode::OK,
            Json(json!({"status":"ready","published_models":count})),
        ),
        Ok(Ok((count, failures))) => (
            if count > 0 {
                StatusCode::OK
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            },
            Json(
                json!({"status":"degraded","published_models":count,"unavailable_models":failures.len()}),
            ),
        ),
        _ => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"status":"catalog_unavailable"})),
        ),
    }
}

pub(super) async fn model_ready(
    State(app): State<Arc<App>>,
    Path(id): Path<String>,
    Extension(principal): Extension<Principal>,
) -> ApiResult<impl IntoResponse> {
    let _permit = app.model_probes.try_acquire().map_err(|_| Error::busy())?;
    if matches!(
        principal,
        Principal::Service(_) | Principal::ServiceMember { .. }
    ) {
        super::access::check_query(&app, &principal, &id)?;
    }
    let files = app.models.lock().unwrap().read_files();
    let admission = app.admission.clone();
    let model = id.clone();
    let (revision, yaml, mut engine) = tokio::task::spawn_blocking(move || {
        let revision = files.published_revision(&model)?;
        let (yaml, engine) = files.cached_engine(&model, &revision, &admission)?;
        Ok::<_, Error>((revision, yaml, engine))
    })
    .await
    .map_err(|_| Error::storage())??;
    let deadline = app.options.timeout.min(Duration::from_secs(5));
    let _selection = super::local_tables::managed::resolve(&app, &mut engine, &id, 30).await?;
    let (coordinator, workers) = tokio::join!(
        SourceProbe::run(engine, app.options.clone(), &app.probe_admission),
        async {
            match &app.cluster {
                Some(cluster) => cluster
                    .probe_workers(&id, &revision, yaml, deadline)
                    .await
                    .map(Some),
                None => Ok(None),
            }
        }
    );
    let coordinator = coordinator?;
    let workers = workers?;
    if app.published_model_revision(&id).await? != revision {
        return Err(versions::active_conflict());
    }
    let workers_ready = workers
        .as_ref()
        .is_none_or(|workers| workers.iter().all(|worker| worker.ready()));
    let available = coordinator.ready() && workers_ready;
    let status = if coordinator.ready() && !workers_ready {
        json!("worker_unavailable")
    } else {
        json!(coordinator.status)
    };
    Ok((
        if available {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        Json(json!({
            "status": status,
            "model_revision": revision,
            "sources": coordinator.sources,
            "location": coordinator.location,
            "workers": workers,
        })),
    ))
}

pub(super) async fn metrics(State(app): State<Arc<App>>) -> impl IntoResponse {
    let (active, waiting) = query_counts(&app);
    app.metrics.render(
        active,
        waiting,
        app.access.audit_failures(),
        app.ha.as_ref().map(|_| coordinator_ready(&app)),
        app.ha.as_ref().map(|ha| ha.leader.metrics().render()),
        super::local_tables::managed::backend(&app).is_some(),
    )
}

impl Metrics {
    pub(super) fn render(
        &self,
        active: usize,
        waiting: usize,
        audit_failures: u64,
        coordinator: Option<bool>,
        elections: Option<String>,
        managed_tables: bool,
    ) -> impl IntoResponse + use<> {
        use std::fmt::Write as _;
        let metrics = self;
        let mut body = format!(
            "# TYPE graphlake_process_uptime_seconds gauge\ngraphlake_process_uptime_seconds {}\n\
         # TYPE graphlake_query_slots gauge\ngraphlake_query_slots {}\n\
         # TYPE graphlake_active_queries gauge\ngraphlake_active_queries {}\n\
         # TYPE graphlake_waiting_queries gauge\ngraphlake_waiting_queries {}\n\
         # TYPE graphlake_http_request_duration_seconds_total counter\ngraphlake_http_request_duration_seconds_total {}\n\
         # TYPE graphlake_http_requests_total counter\n",
            metrics.started.elapsed().as_secs(),
            metrics.slots,
            active,
            waiting,
            metrics.http_micros.load(Relaxed) as f64 / 1_000_000.0,
        );
        for (index, count) in metrics.http.iter().enumerate() {
            let _ = writeln!(
                body,
                "graphlake_http_requests_total{{status_class=\"{}xx\"}} {}",
                index + 1,
                count.load(Relaxed)
            );
        }
        let _ = writeln!(
            body,
            "# TYPE graphlake_audit_write_failures_total counter\ngraphlake_audit_write_failures_total {}",
            audit_failures
        );
        body.push_str("# TYPE graphlake_driver_queries_total counter\n");
        for (protocol, counts) in ["bolt", "gremlin"].into_iter().zip(&metrics.driver) {
            for (outcome, count) in ["ok", "error"].into_iter().zip(counts) {
                let _ = writeln!(
                    body,
                    "graphlake_driver_queries_total{{protocol=\"{protocol}\",outcome=\"{outcome}\"}} {}",
                    count.load(Relaxed)
                );
            }
        }
        body.push_str("# TYPE graphlake_query_errors_total counter\n");
        for (category, count) in ["source_unavailable", "configuration", "other"]
            .into_iter()
            .zip(&metrics.query_error)
        {
            let _ = writeln!(
                body,
                "graphlake_query_errors_total{{category=\"{category}\"}} {}",
                count.load(Relaxed)
            );
        }
        if let Some(ready) = coordinator {
            let _ = writeln!(
                body,
                "# TYPE graphlake_coordinator_ready gauge\ngraphlake_coordinator_ready {}",
                u8::from(ready)
            );
        }
        body.push_str("# TYPE graphlake_otlp_export_failures_total counter\n");
        for (signal, failures) in telemetry::failures() {
            let _ = writeln!(
                body,
                "graphlake_otlp_export_failures_total{{signal=\"{signal}\"}} {failures}"
            );
        }
        body.push_str("# TYPE graphlake_otlp_rejected_total counter\n# TYPE graphlake_otlp_last_rejected gauge\n# TYPE graphlake_otlp_last_success_timestamp_seconds gauge\n");
        for (signal, success, rejected, last_rejected) in telemetry::export_delivery() {
            let _ = writeln!(
                body,
                "graphlake_otlp_rejected_total{{signal=\"{signal}\"}} {rejected}\ngraphlake_otlp_last_rejected{{signal=\"{signal}\"}} {last_rejected}\ngraphlake_otlp_last_success_timestamp_seconds{{signal=\"{signal}\"}} {}",
                success as f64 / 1000.0
            );
        }
        for ((name, _, _), counter) in QUERY_IO_METRICS.into_iter().zip(&QUERY_IO) {
            let _ = writeln!(
                body,
                "# TYPE {name} counter\n{name} {}",
                counter.load(Relaxed)
            );
        }
        body.push_str("# TYPE graphlake_request_duration_seconds histogram\n");
        let latency = metrics.latency.lock().unwrap();
        for ((protocol, operation, outcome), histogram) in latency.iter() {
            let labels =
                format!("protocol=\"{protocol}\",operation=\"{operation}\",outcome=\"{outcome}\"");
            let mut count = 0;
            for (upper, bucket) in telemetry::BUCKETS.into_iter().zip(histogram.buckets) {
                count += bucket;
                let _ = writeln!(
                    body,
                    "graphlake_request_duration_seconds_bucket{{{labels},le=\"{upper}\"}} {count}"
                );
            }
            count += histogram.buckets[telemetry::BUCKETS.len()];
            let _ = writeln!(
                body,
                "graphlake_request_duration_seconds_bucket{{{labels},le=\"+Inf\"}} {count}\ngraphlake_request_duration_seconds_count{{{labels}}} {count}\ngraphlake_request_duration_seconds_sum{{{labels}}} {}",
                histogram.sum
            );
        }
        if let Some(elections) = elections {
            body.push_str(&elections);
        }
        if managed_tables {
            let _ = writeln!(
                body,
                "# TYPE graphlake_managed_gc_failures_total counter\ngraphlake_managed_gc_failures_total {}\n\
                 # TYPE graphlake_managed_gc_deleted_objects_total counter\ngraphlake_managed_gc_deleted_objects_total {}\n\
                 # TYPE graphlake_managed_gc_last_success_timestamp_seconds gauge\ngraphlake_managed_gc_last_success_timestamp_seconds {}",
                metrics.gc_failures.load(Relaxed),
                metrics.gc_deleted.load(Relaxed),
                metrics.gc_last_success_ms.load(Relaxed) as f64 / 1000.0,
            );
        }
        (
            [(
                header::CONTENT_TYPE,
                "text/plain; version=0.0.4; charset=utf-8",
            )],
            body,
        )
    }
}

pub(super) async fn shutdown_signal(shutdown: CancellationToken) {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut terminate) => {
                tokio::select! { _ = terminate.recv() => {}, _ = tokio::signal::ctrl_c() => {} }
            }
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
    shutdown.cancel();
}
