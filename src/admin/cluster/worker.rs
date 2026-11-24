use std::{collections::VecDeque, path::PathBuf, sync::Arc};

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, Request, State},
    http::header,
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::post,
};
use opentelemetry::trace::FutureExt as _;
use subtle::ConstantTimeEq;
use tokio::sync::Semaphore;

use super::*;
use crate::{
    CancellationToken, Engine, QueryOptions,
    admin::{ops::Metrics, telemetry},
};

#[derive(Debug, PartialEq, Eq)]
enum Publication {
    Committed,
    Rejected,
    Unknown,
}

pub(super) struct Worker {
    pub(super) id: Uuid,
    pub(super) shared: Shared,
    pub(super) workers: Vec<url::Url>,
    partitions: usize,
    pub(super) distributed_min_rows: usize,
    pub(super) memory_bytes: usize,
    source_dir: PathBuf,
    models: Mutex<VecDeque<(String, String, Engine)>>,
    token_hash: [u8; 32],
    slots: Arc<Semaphore>,
    probes: Semaphore,
    pub(super) stage_slots: Arc<Semaphore>,
    pub(super) admission: Arc<crate::engine::Admission>,
    pub(super) shutdown: CancellationToken,
    pub(super) arc_cache: super::csr::ArcBlocks,
    active_jobs: Arc<AtomicUsize>,
    drained: Arc<tokio::sync::Notify>,
    metrics: Metrics,
}

pub(super) struct WorkerDrain {
    active_jobs: Arc<AtomicUsize>,
    drained: Arc<tokio::sync::Notify>,
}

impl WorkerDrain {
    pub(super) async fn wait(self, timeout: Duration) {
        let _ = tokio::time::timeout(timeout, async {
            loop {
                let notified = self.drained.notified();
                if self.active_jobs.load(Ordering::Acquire) == 0 {
                    break;
                }
                notified.await;
            }
        })
        .await;
    }
}

struct ActiveJobGuard {
    active_jobs: Arc<AtomicUsize>,
    drained: Arc<tokio::sync::Notify>,
}

impl Drop for ActiveJobGuard {
    fn drop(&mut self) {
        if self.active_jobs.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.drained.notify_waiters();
        }
    }
}

pub(super) fn router(
    shared: Shared,
    config: Config,
    source_dir: PathBuf,
    slots: usize,
) -> (Router, CancellationToken, WorkerDrain) {
    let shutdown = CancellationToken::new();
    let active_jobs = Arc::new(AtomicUsize::new(0));
    let drained = Arc::new(tokio::sync::Notify::new());
    let capacity = config.process_memory_mib / config.memory_mib;
    let reserve = usize::from(config.distributed_partitions > 0);
    let slots = slots.min(capacity - reserve);
    let remote = if reserve > 0 {
        (capacity - slots).clamp(1, 4)
    } else {
        0
    };
    let worker = Arc::new(Worker {
        id: Uuid::new_v4(),
        token_hash: Sha256::digest(shared.token.as_bytes()).into(),
        shared,
        workers: config.workers,
        partitions: config.distributed_partitions,
        distributed_min_rows: config.distributed_min_rows,
        memory_bytes: config.memory_mib * 1024 * 1024,
        source_dir,
        models: Default::default(),
        slots: Arc::new(Semaphore::new(slots)),
        probes: Semaphore::new(2),
        stage_slots: Arc::new(Semaphore::new(remote)),
        admission: Arc::new(
            crate::engine::Admission::new(crate::EngineLimits {
                max_queries: slots + remote,
                memory_bytes: config.process_memory_mib * 1024 * 1024,
                spill_bytes: (slots + remote) as u64 * QueryOptions::default().spill_bytes,
            })
            .expect("validated worker capacity"),
        ),
        shutdown: shutdown.clone(),
        arc_cache: Default::default(),
        active_jobs: Arc::clone(&active_jobs),
        drained: Arc::clone(&drained),
        metrics: Metrics::new(slots),
    });
    if let Some(meter) = telemetry::meter() {
        super::super::ops::register_query_io(&meter);
        let weak = Arc::downgrade(&worker);
        meter
            .u64_observable_gauge("graphlake.queries")
            .with_callback(move |observer| {
                if let Some(worker) = weak.upgrade() {
                    observer.observe(
                        (slots - worker.slots.available_permits()) as u64,
                        &[opentelemetry::KeyValue::new("state", "active")],
                    );
                }
            })
            .build();
        meter
            .u64_observable_gauge("graphlake.query.slots")
            .with_callback(move |observer| {
                observer.observe(slots as u64, &[]);
            })
            .build();
    }
    let router = Router::new()
        .route("/metrics", axum::routing::get(metrics))
        .route("/internal/v1/jobs", post(accept))
        .route("/internal/v1/stages", post(distributed::run_task))
        .route("/internal/v1/csr", post(csr::run_task))
        .route("/internal/v1/identity", axum::routing::get(identity))
        .route(
            "/internal/v1/readiness/{model}/{revision}",
            axum::routing::get(readiness),
        )
        .layer(DefaultBodyLimit::max(DISPATCH_LIMIT as usize))
        .layer(middleware::from_fn_with_state(worker.clone(), authenticate))
        .with_state(worker);
    (
        router,
        shutdown,
        WorkerDrain {
            active_jobs,
            drained,
        },
    )
}

async fn metrics(State(worker): State<Arc<Worker>>) -> impl IntoResponse {
    worker.metrics.render(
        worker.metrics.slots() - worker.slots.available_permits(),
        0,
        0,
        None,
        None,
        false,
    )
}

async fn identity(State(worker): State<Arc<Worker>>) -> ApiResult<Json<Uuid>> {
    if worker.shutdown.is_cancelled() {
        return Err(unavailable());
    }
    Ok(Json(worker.id))
}

async fn readiness(
    State(worker): State<Arc<Worker>>,
    Path((model, revision)): Path<(String, String)>,
) -> ApiResult<Json<SourceProbe>> {
    if !models::valid_id(&model) || !models::versions::valid_revision(&revision) {
        return Err(Error::not_found());
    }
    if worker.shutdown.is_cancelled() {
        return Err(unavailable());
    }
    let _permit = worker.probes.try_acquire().map_err(|_| Error::busy())?;
    let mut options = QueryOptions::default();
    options.memory_bytes = worker.memory_bytes;
    let probe = tokio::time::timeout(Duration::from_secs(5), async {
        worker.shared.metadata.ping().await.map_err(storage)?;
        let mut engine = worker.engine(&model, &revision).await?;
        let ids = crate::admin::local_tables::managed::referenced(&engine.catalog);
        let managed = crate::admin::local_tables::managed::Managed::reader(
            worker.shared.metadata.clone(),
            worker.shared.objects.clone(),
        );
        let selection = managed.pin_model(ids, &model, 30).await?;
        use crate::admin::local_tables::managed::error;
        engine.managed_parquet = Arc::new(
            managed
                .snapshots(&selection.pins().map_err(error)?)
                .await
                .map_err(error)?,
        );
        SourceProbe::run(engine, options, &worker.admission).await
    })
    .await
    .map_err(|_| unavailable())??;
    Ok(Json(probe))
}

async fn authenticate(State(worker): State<Arc<Worker>>, request: Request, next: Next) -> Response {
    let operation = match request.uri().path() {
        "/internal/v1/jobs" => Some("dispatch"),
        "/internal/v1/stages" => Some("stage"),
        "/internal/v1/csr" => Some("csr_task"),
        path if path.starts_with("/internal/v1/readiness/") => Some("model_readiness"),
        _ => None,
    };
    let started = std::time::Instant::now();
    let mut values = request.headers().get_all(header::AUTHORIZATION).iter();
    let bearer = values
        .next()
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .filter(|value| value.len() <= 256);
    let authorized = values.next().is_none()
        && bearer.is_some_and(|token| {
            bool::from(worker.token_hash.ct_eq(&Sha256::digest(token.as_bytes())))
        });
    let span = operation
        .and_then(|op| telemetry::start_span(op, "worker", authorized.then(|| request.headers())));
    let response = if !authorized {
        StatusCode::UNAUTHORIZED.into_response()
    } else if let Some(span) = &span {
        next.run(request).with_context(span.clone()).await
    } else {
        next.run(request).await
    };
    if let Some(span) = &span {
        telemetry::finish_span(span, response.status().as_u16());
    }
    if let Some(operation) = operation {
        worker.metrics.observe(
            "worker",
            operation,
            response.status().as_u16(),
            started.elapsed(),
        );
    }
    response
}

async fn accept(
    State(worker): State<Arc<Worker>>,
    headers: HeaderMap,
    Json(dispatch): Json<Dispatch>,
) -> ApiResult<StatusCode> {
    let started = Instant::now();
    if worker.shutdown.is_cancelled() {
        return Err(unavailable());
    }
    let (id, _, _) = query::validate_request(&dispatch.request, QueryOptions::default())?;
    if id != dispatch.id {
        return Err(Error::not_found());
    }
    let slot = worker
        .slots
        .clone()
        .try_acquire_owned()
        .map_err(|_| Error::busy())?;
    let metadata = &worker.shared.metadata;
    let saved = metadata
        .get(id)
        .await
        .map_err(storage)?
        .ok_or_else(Error::not_found)?;
    let mut job = saved.job;
    if job.attempt != dispatch.attempt
        || job.fingerprint != fingerprint(&dispatch)?
        || job.revision != dispatch.request.model_revision
        || !models::valid_id(&job.model)
        || job.worker.is_some()
        || !matches!(job.state, JobState::Running)
    {
        return Err(Error::not_found());
    }
    let deadline = work_deadline(&headers, started, job.deadline_ms)?;
    if Instant::now() >= deadline {
        return Err(timeout());
    }
    job.worker = Some(worker.id);
    job.worker_claim = Some(Uuid::new_v4());
    job.worker_seen_ms = now_ms();
    let claimed = match metadata.replace(id, saved.revision, &job).await {
        Ok(claimed) => claimed,
        Err(error) => {
            // An etcd transaction can commit even if its response is lost.
            // Confirm this exact claim before starting any work.
            let current = tokio::time::timeout_at(deadline, metadata.get(id))
                .await
                .map_err(|_| timeout())?
                .map_err(|_| storage(error))?;
            current.is_some_and(|current| {
                current.job.attempt == job.attempt
                    && current.job.worker == job.worker
                    && current.job.worker_claim == job.worker_claim
                    && matches!(current.job.state, JobState::Running)
            })
        }
    };
    if !claimed {
        return Err(Error::not_found());
    }
    let span = telemetry::start_span("query", "worker", None);
    worker.active_jobs.fetch_add(1, Ordering::AcqRel);
    let active_guard = ActiveJobGuard {
        active_jobs: Arc::clone(&worker.active_jobs),
        drained: Arc::clone(&worker.drained),
    };
    tokio::spawn(async move {
        let _active_guard = active_guard;
        if let Some(span) = span {
            worker
                .run(dispatch, job, slot, deadline, Some(&span))
                .with_context(span.clone())
                .await;
        } else {
            worker.run(dispatch, job, slot, deadline, None).await;
        }
    });
    Ok(StatusCode::ACCEPTED)
}

impl Worker {
    async fn engine(&self, model: &str, revision: &str) -> ApiResult<Engine> {
        {
            let mut models = self.models.lock().unwrap();
            if let Some(index) = models
                .iter()
                .position(|(id, active, _)| id == model && active == revision)
            {
                let entry = models.remove(index).expect("cached model");
                let engine = entry.2.clone();
                models.push_front(entry);
                return Ok(engine);
            }
        }
        let yaml = self
            .shared
            .objects
            .read_model(model, revision)
            .await
            .map_err(storage)?;
        if models::revision(&yaml) != revision {
            return Err(Error::storage());
        }
        let mut catalog = models::parse(&yaml, &self.source_dir.join(format!("{model}.yaml")))?;
        catalog.export_locations.clear();
        let engine =
            Engine::with_admission(catalog, self.admission.clone()).map_err(query::query_error)?;
        let mut models = self.models.lock().unwrap();
        if let Some((_, _, cached)) = models
            .iter()
            .find(|(id, active, _)| id == model && active == revision)
        {
            return Ok(cached.clone());
        }
        if models.len() >= 16 {
            models.pop_back();
        }
        models.push_front((model.into(), revision.into(), engine.clone()));
        Ok(engine)
    }

    async fn run(
        self: &Arc<Self>,
        dispatch: Dispatch,
        job: Job,
        slot: tokio::sync::OwnedSemaphorePermit,
        deadline: Instant,
        span: Option<&opentelemetry::Context>,
    ) {
        let started = std::time::Instant::now();
        let token = self.shutdown.child_token();
        let id = dispatch.id;
        let result = {
            let worker = self.clone();
            let lease_job = job.clone();
            let lease_token = token.clone();
            let lease = tokio::spawn(async move {
                worker.maintain_lease(id, &lease_job, &lease_token).await;
            });
            let operation = self.compute(id, &job, &dispatch, token.clone(), deadline);
            let result = tokio::select! {
                biased;
                _ = token.cancelled() => Err(cancelled()),
                _ = tokio::time::sleep_until(deadline) => Err(timeout()),
                result = operation => result,
            };
            token.cancel();
            // Finish any in-flight heartbeat before attempting the terminal CAS.
            let _ = lease.await;
            result
        };
        let status = result
            .as_ref()
            .map_or_else(|error| error.status.as_u16(), |_| 200);
        self.metrics
            .observe("worker", "query", status, started.elapsed());
        if let Some(span) = span {
            telemetry::finish_span(span, status);
        }
        if status >= 500 {
            telemetry::event(
                opentelemetry::logs::Severity::Warn,
                "worker_query_failed",
                serde_json::json!({ "status": status, "code": result.as_ref().err().map(|e| e.code) }),
            );
        }
        let (state, written) = match result {
            Ok(state) => (state, true),
            Err(error) => (failure(&error), false),
        };
        self.finish_result(id, &job, &state, written, slot).await;
    }

    async fn finish_result(
        &self,
        id: Uuid,
        job: &Job,
        state: &JobState,
        written: bool,
        slot: tokio::sync::OwnedSemaphorePermit,
    ) {
        let mut publication = self.publish_result(id, job, state).await;
        drop(slot);
        while publication == Publication::Unknown {
            // The Coordinator may need the original request to retry this attempt.
            // Leave both artifacts available until etcd resolves the outcome.
            tokio::select! {
                _ = self.shutdown.cancelled() => return,
                _ = tokio::time::sleep(Duration::from_secs(1)) => {}
            }
            publication = self.publish_result(id, job, state).await;
        }
        if publication == Publication::Rejected || !written {
            self.shared.objects.clean_results(id, job.attempt).await;
        } else {
            if self.partitions > 0 {
                self.shared.objects.clean_shuffle(id, job.attempt).await;
            }
            let request = self
                .shared
                .objects
                .artifact(id, job.attempt, "request.json");
            self.shared.objects.delete(&request).await;
        }
    }

    async fn publish_result(&self, id: Uuid, job: &Job, state: &JobState) -> Publication {
        let metadata = &self.shared.metadata;
        let deadline = Instant::now() + Duration::from_secs(5);
        tokio::time::timeout_at(deadline, async {
            loop {
                match metadata.get(id).await {
                    Ok(Some(saved))
                        if saved.job.attempt == job.attempt
                            && saved.job.worker == Some(self.id) =>
                    {
                        if !matches!(saved.job.state, JobState::Running) {
                            return if matches!(
                                saved.job.state,
                                JobState::Done { .. } | JobState::Downloaded
                            ) && matches!(state, JobState::Done { .. })
                            {
                                Publication::Committed
                            } else {
                                Publication::Rejected
                            };
                        }
                        let mut current = saved.job;
                        current.state = state.clone();
                        if let Ok(true) = metadata.replace(id, saved.revision, &current).await {
                            return Publication::Committed;
                        }
                    }
                    Ok(_) => return Publication::Rejected,
                    Err(_) => {}
                }
                tokio::time::sleep(WAIT).await;
            }
        })
        .await
        .unwrap_or(Publication::Unknown)
    }

    async fn maintain_lease(&self, id: Uuid, job: &Job, token: &CancellationToken) {
        const HEARTBEAT: Duration = Duration::from_secs(2);
        const OWNERSHIP_POLL: Duration = Duration::from_millis(500);
        let mut next_heartbeat = Instant::now() + HEARTBEAT;
        loop {
            tokio::select! {
                _ = token.cancelled() => return,
                _ = tokio::time::sleep(OWNERSHIP_POLL) => {}
            }
            match self.shared.metadata.get(id).await {
                Ok(Some(saved))
                    if saved.job.attempt == job.attempt
                        && saved.job.worker == Some(self.id)
                        && matches!(saved.job.state, JobState::Running) =>
                {
                    if Instant::now() >= next_heartbeat {
                        let mut current = saved.job;
                        current.worker_seen_ms = now_ms();
                        let _ = self
                            .shared
                            .metadata
                            .replace(id, saved.revision, &current)
                            .await;
                        next_heartbeat = Instant::now() + HEARTBEAT;
                    }
                }
                Ok(_) => {
                    token.cancel();
                    return;
                }
                // A failed metadata read cannot establish loss of ownership. The
                // deadline and the final compare-and-swap still fence this attempt.
                Err(_) => {}
            }
        }
    }

    async fn compute(
        self: &Arc<Self>,
        id: Uuid,
        job: &Job,
        dispatch: &Dispatch,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> ApiResult<JobState> {
        let mut engine = self.engine(&job.model, &job.revision).await?;
        let request = &dispatch.request;
        if let Some(pin) = dispatch.managed.first() {
            let managed = crate::admin::local_tables::managed::Managed::reader(
                self.shared.metadata.clone(),
                self.shared.objects.clone(),
            );
            engine.managed_parquet = Arc::new(
                managed
                    .snapshots(&dispatch.managed)
                    .await
                    .map_err(crate::admin::local_tables::managed::error)?,
            );
            let remaining = managed
                .remaining(pin)
                .await
                .map_err(crate::admin::local_tables::managed::error)?;
            if remaining
                <= deadline.saturating_duration_since(Instant::now()) + Duration::from_secs(5)
            {
                return Err(timeout());
            }
        }
        let (_, language, mut options) = query::validate_request(request, QueryOptions::default())?;
        if let Some(term) = &request.tenant_partition {
            crate::admin::users::apply_partition(&engine.catalog, &mut options.graph, term)?;
        }
        options.memory_bytes = self.memory_bytes;
        options.timeout = options
            .timeout
            .min(deadline.saturating_duration_since(Instant::now()));
        if options.timeout.is_zero() {
            return Err(timeout());
        }
        options.cancellation = cancellation;
        let distributed = (self.partitions > 0).then(|| {
            distributed::Distributed::new(
                self.shared.clone(),
                self.workers.clone(),
                id,
                job.attempt,
                self.id,
                deadline,
                self.partitions,
                self.distributed_min_rows,
            )
        });
        let cypher_csr = if let Some(generation) = request.csr_generation {
            Some(csr::CypherCsr::open(self.clone(), job, id, generation).await?)
        } else {
            None
        };
        let (result, csr_stats) = if let Some(csr_request) = &request.csr {
            let (result, stats) =
                csr::query_csr(self, &engine, job, id, csr_request, options.clone()).await?;
            (result, Some(stats))
        } else {
            let result = if let Some(distributed) = &distributed {
                Box::pin(engine.cluster_query(
                    language,
                    &request.query,
                    options.clone(),
                    !job.export,
                    distributed,
                    cypher_csr.as_ref(),
                ))
                .await
            } else if job.export {
                Box::pin(engine.query(language, &request.query, options.clone())).await
            } else {
                Box::pin(engine.preview_query(language, &request.query, options.clone())).await
            }
            .map_err(query::query_error)?;
            (result, None)
        };
        if let Some(proof) = &job.freshness {
            proof.verify(&result.versions)?;
        }
        let (arrow, graph_pages) = if job.export {
            let arrow = query::download::capture_all(result, options.spill_bytes).await?;
            (Some(arrow), 0)
        } else {
            let preview = query::preview(
                result,
                &engine.catalog,
                request,
                &options,
                id,
                Some(self.id),
                distributed
                    .as_ref()
                    .and_then(distributed::Distributed::stats),
                csr_stats,
                cypher_csr.as_ref(),
            )
            .await?;
            let key = self
                .shared
                .objects
                .artifact(id, job.attempt, "preview.json");
            self.shared
                .objects
                .put_json(&key, &preview.page)
                .await
                .map_err(storage)?;
            let mut count = 0;
            if let Some((pages, incomplete)) = preview.graph {
                count = pages.len();
                for (index, graph) in pages.into_iter().enumerate() {
                    let key = self.shared.objects.artifact(
                        id,
                        job.attempt,
                        &format!("graph-{index}.json"),
                    );
                    self.shared
                        .objects
                        .put_json(
                            &key,
                            &json!({
                                "graph":graph, "next":(index+1<count).then_some(index+1),
                                "incomplete":incomplete, "model_revision":job.revision,
                            }),
                        )
                        .await
                        .map_err(storage)?;
                }
            }
            (preview.arrow, count)
        };
        let has_arrow = arrow.is_some();
        if let Some(arrow) = arrow {
            let key = self
                .shared
                .objects
                .artifact(id, job.attempt, "result.arrow");
            self.shared
                .objects
                .put_arrow(&key, arrow)
                .await
                .map_err(storage)?;
        }
        Ok(JobState::Done {
            arrow: has_arrow,
            graph_pages,
            completed_ms: now_ms(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::post;
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    use std::sync::atomic::{AtomicBool, Ordering};

    #[tokio::test]
    async fn terminal_result_keeps_recovery_artifacts_until_metadata_returns() -> anyhow::Result<()>
    {
        let worker_id = Uuid::new_v4();
        let job_id = Uuid::new_v4();
        let job = Job {
            model: "test".into(),
            revision: "revision".into(),
            owner: super::super::Principal::Admin,
            attempt: Uuid::new_v4(),
            worker: Some(worker_id),
            worker_claim: None,
            worker_seen_ms: now_ms(),
            retried: false,
            managed_only: false,
            fingerprint: [0; 32],
            export: false,
            csr_generation: None,
            freshness: None,
            deadline_ms: now_ms() + 1000,
            state: JobState::Running,
        };
        let unavailable = Arc::new(AtomicBool::new(true));
        let saved = Arc::new(std::sync::Mutex::new(None));
        let router = Router::new()
            .route("/v3/kv/range", post({
                let unavailable = unavailable.clone();
                let record = STANDARD.encode(serde_json::to_vec(&job)?);
                move || async move {
                    if unavailable.load(Ordering::Acquire) {
                        StatusCode::SERVICE_UNAVAILABLE.into_response()
                    } else {
                        Json(serde_json::json!({ "kvs": [{ "value": record, "mod_revision": "7" }] })).into_response()
                    }
                }
            }))
            .route("/v3/kv/txn", post({
                let saved = saved.clone();
                move |Json(body): Json<serde_json::Value>| async move {
                    assert_eq!(body["compare"][0]["mod_revision"], "7");
                    let value = body["success"][0]["request_put"]["value"].as_str().unwrap();
                    let job: Job = serde_json::from_slice(&STANDARD.decode(value).unwrap()).unwrap();
                    *saved.lock().unwrap() = Some(job);
                    Json(serde_json::json!({ "succeeded": true, "header": { "revision": "8" } }))
                }
            }));
        let address = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", address.local_addr()?).parse()?;
        let server = tokio::spawn(async move { axum::serve(address, router).await });
        let dir = tempfile::tempdir()?;
        let client = reqwest::Client::new();
        let worker = Worker {
            id: worker_id,
            shared: Shared {
                client: client.clone(),
                metadata: Metadata::new(client, vec![endpoint], "worker-test"),
                objects: Objects::open(dir.path().to_str().unwrap(), "worker-test")?,
                token: String::new(),
            },
            workers: Vec::new(),
            partitions: 0,
            memory_bytes: 256 * 1024 * 1024,
            distributed_min_rows: 0,
            source_dir: dir.path().to_owned(),
            models: Default::default(),
            token_hash: [0; 32],
            slots: Arc::new(Semaphore::new(1)),
            probes: Semaphore::new(0),
            stage_slots: Arc::new(Semaphore::new(0)),
            admission: Arc::new(crate::engine::Admission::new(
                crate::EngineLimits::default(),
            )?),
            shutdown: CancellationToken::new(),
            arc_cache: Default::default(),
            active_jobs: Default::default(),
            drained: Default::default(),
            metrics: Metrics::new(1),
        };
        let terminal = JobState::Done {
            arrow: true,
            graph_pages: 0,
            completed_ms: now_ms(),
        };
        let worker = Arc::new(worker);
        let objects = &worker.shared.objects;
        let request = objects.artifact(job_id, job.attempt, "request.json");
        let result = objects.artifact(job_id, job.attempt, "result.arrow");
        for key in [&request, &result] {
            objects.put_bytes(key, b"fixture".to_vec()).await?;
        }
        let attempt = job.attempt;
        let slot = worker.slots.clone().acquire_owned().await?;
        let publish = {
            let worker = worker.clone();
            tokio::spawn(async move {
                worker
                    .finish_result(job_id, &job, &terminal, true, slot)
                    .await;
            })
        };
        let _slot = tokio::time::timeout(Duration::from_secs(10), worker.slots.acquire()).await??;
        assert!(saved.lock().unwrap().is_none());
        assert!(objects.exists(&request).await?);
        assert!(objects.exists(&result).await?);
        unavailable.store(false, Ordering::Release);
        tokio::time::timeout(Duration::from_secs(10), publish).await??;
        assert!(!objects.exists(&request).await?);
        assert!(objects.exists(&result).await?);
        server.abort();
        let persisted = saved.lock().unwrap().take().expect("terminal CAS");
        assert_eq!(persisted.attempt, attempt);
        assert!(matches!(
            persisted.state,
            JobState::Done { arrow: true, .. }
        ));
        Ok(())
    }
}
