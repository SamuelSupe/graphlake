use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
    time::{Duration, Instant},
};

use axum::{
    Json,
    extract::State,
    http::{StatusCode, header},
    response::IntoResponse,
};
use serde_json::json;

use super::App;
use crate::CancellationToken;

pub(super) struct Metrics {
    started: Instant,
    slots: usize,
    http: [AtomicU64; 5],
    http_micros: AtomicU64,
    driver: [[AtomicU64; 2]; 2],
}

impl Metrics {
    pub(super) fn new(slots: usize) -> Self {
        Self {
            started: Instant::now(),
            slots,
            http: Default::default(),
            http_micros: 0.into(),
            driver: Default::default(),
        }
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
}

pub(super) async fn health() -> Json<serde_json::Value> {
    Json(json!({"status":"alive","version":env!("CARGO_PKG_VERSION")}))
}

pub(super) async fn ready(State(app): State<Arc<App>>) -> impl IntoResponse {
    if app.shutdown.is_cancelled() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"status":"draining"})),
        );
    }
    let Ok(slot) = app.readiness.clone().try_acquire_owned() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"status":"probe_in_progress"})),
        );
    };
    let dir = app.models.lock().unwrap().directory().to_path_buf();
    let driver_model = app.wire.as_ref().map(|driver| driver.model.clone());
    let checked = tokio::task::spawn_blocking(move || {
        let _slot = slot;
        let models = super::models::Models::new(dir);
        let count = models.check_published()?;
        if let Some(model) = driver_model
            && models.active_revision(&model)?.is_none()
        {
            return Err(super::Error::not_found());
        }
        Ok(count)
    })
    .await;
    match checked {
        Ok(Ok(count)) => (
            StatusCode::OK,
            Json(json!({"status":"ready","published_models":count})),
        ),
        _ => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"status":"catalog_unavailable"})),
        ),
    }
}

pub(super) async fn metrics(State(app): State<Arc<App>>) -> impl IntoResponse {
    use std::fmt::Write as _;
    let metrics = &app.metrics;
    let (active, waiting) = {
        let queries = app.active.lock().unwrap();
        let active = queries.values().filter(|query| query.running).count();
        (active, queries.len() - active)
    };
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
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        body,
    )
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
