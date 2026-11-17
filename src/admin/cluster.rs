//! Optional multi-worker administration service.
mod config;
pub(super) mod control;
pub(crate) mod csr;
pub(crate) mod distributed;
mod results;
mod retention;
mod s3;
pub(in crate::admin) mod store;
mod worker;

use std::{
    collections::HashMap,
    net::{Ipv4Addr, SocketAddrV4},
    path::{Path as FsPath, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use axum::{
    http::{HeaderMap, StatusCode},
    response::Response,
};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::{net::TcpListener, time::Instant};
use uuid::Uuid;

use super::{
    ApiResult, App, Error,
    access::{self, Principal},
    freshness, models,
    ops::SourceProbe,
    query,
};
use config::{Config, Shared};
pub(super) use results::resume;
use store::{Job, JobState, Metadata, Objects, VersionedJob, now_ms};

const WORK_BUDGET_HEADER: &str = "x-graphlake-work-budget-ms";

fn work_deadline(
    headers: &HeaderMap,
    started: Instant,
    legacy_deadline: u64,
) -> ApiResult<Instant> {
    let budget = match headers.get(WORK_BUDGET_HEADER) {
        Some(value) => value
            .to_str()
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .ok_or_else(|| Error::invalid("invalid_work_budget", "集群请求时间预算无效"))?,
        // Older senders omit the relative budget during a rolling upgrade.
        None => legacy_deadline.saturating_sub(now_ms()),
    };
    if budget == 0 {
        return Err(timeout());
    }
    Ok(started + Duration::from_millis(budget.min(86_400_000)))
}

const WAIT: Duration = Duration::from_millis(150);
const RESULT_TTL: u64 = 30 * 60;
const JOB_LIMIT: u64 = 1024 * 1024;
const GRAPH_PAGE_LIMIT: u64 = 256 * 1024;
const WORKER_HEARTBEAT_TIMEOUT_MS: u64 = 6000;
const DISPATCH_LIMIT: u64 = 768 * 1024;

pub(super) struct Coordinator {
    shared: Shared,
    pub(super) managed: Option<super::local_tables::managed::Managed>,
    name: String,
    workers: Vec<url::Url>,
    next: AtomicUsize,
    graph_partitions: usize,
    memory_bytes: usize,
    heartbeats: Mutex<Heartbeats>,
    csr_work: tokio::sync::Semaphore,
    csr_refresh: Mutex<std::collections::BTreeMap<String, csr::RefreshStatus>>,
}

#[derive(Default)]
struct Heartbeats {
    jobs: HashMap<Uuid, (i64, Instant, Instant)>,
    last_prune: Option<Instant>,
}

impl Heartbeats {
    fn age(&mut self, id: Uuid, revision: i64, now: Instant) -> Duration {
        let interval = Duration::from_millis(WORKER_HEARTBEAT_TIMEOUT_MS);
        if self
            .last_prune
            .is_none_or(|last| now.duration_since(last) >= interval)
        {
            self.jobs
                .retain(|_, (_, _, seen)| now.duration_since(*seen) <= 4 * interval);
            self.last_prune = Some(now);
        }
        let (current, since, seen) = self.jobs.entry(id).or_insert((revision, now, now));
        if revision < *current {
            return Duration::ZERO;
        }
        if revision > *current {
            *current = revision;
            *since = now;
        }
        *seen = now;
        now.duration_since(*since)
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Dispatch {
    id: Uuid,
    attempt: Uuid,
    request: query::Request,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    managed: Vec<super::local_tables::managed::Pin>,
}

#[derive(Serialize)]
pub(super) struct WorkerProbe {
    worker: usize,
    probe: Option<SourceProbe>,
}

impl WorkerProbe {
    pub(super) fn ready(&self) -> bool {
        self.probe.as_ref().is_some_and(SourceProbe::ready)
    }
}

impl Coordinator {
    pub(super) fn max_coordinator_query_bytes(&self) -> usize {
        self.memory_bytes
            .max(crate::QueryOptions::default().memory_bytes)
    }

    pub fn load(path: &FsPath) -> Result<Self> {
        let (config, shared) = Config::load(path)?;
        ensure!(
            (1..=32).contains(&config.workers.len()),
            "configure 1–32 cluster workers"
        );
        ensure!(
            config
                .workers
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len()
                == config.workers.len(),
            "duplicate cluster worker"
        );
        let managed = config.managed_local_tables.then(|| {
            super::local_tables::managed::Managed::new(
                shared.metadata.clone(),
                shared.objects.clone(),
                config.local_snapshot_retention_seconds,
                config.local_snapshot_gc_grace_seconds,
            )
        });
        Ok(Self {
            shared,
            managed,
            name: config.name,
            workers: config.workers,
            next: AtomicUsize::new(0),
            graph_partitions: config.distributed_partitions,
            memory_bytes: config.memory_mib * 1024 * 1024,
            heartbeats: Default::default(),
            csr_work: tokio::sync::Semaphore::new(1),
            csr_refresh: Default::default(),
        })
    }

    pub(super) async fn check_ingress(path: &FsPath, node: &str) -> Result<bool> {
        control::Control::check_ingress(Config::ingress(path)?, node).await
    }

    pub(super) fn control(&self) -> Result<control::Control> {
        ensure!(
            self.shared.objects.remote(),
            "Coordinator failover requires S3-compatible catalog object storage"
        );
        Ok(control::Control::new(
            self.shared.metadata.clone(),
            self.shared.objects.clone(),
        ))
    }

    pub fn cursor_key(&self) -> [u8; 32] {
        use hmac::{Hmac, KeyInit, Mac};
        let mut mac =
            Hmac::<sha2::Sha256>::new_from_slice(self.shared.token.as_bytes()).expect("HMAC key");
        mac.update(b"graphlake cluster neighbor cursors v1");
        mac.update(self.name.as_bytes());
        mac.finalize().into_bytes().into()
    }

    pub async fn readiness_failure(&self) -> Option<&'static str> {
        if self.shared.metadata.ping().await.is_err() {
            return Some("cluster_metadata_unavailable");
        }
        let mut probes = futures::stream::FuturesUnordered::new();
        for endpoint in &self.workers {
            probes.push(self.worker_identity(endpoint));
        }
        while let Some(identity) = probes.next().await {
            if identity.is_some() {
                return None;
            }
        }
        Some("worker_unavailable")
    }

    pub(super) async fn probe_workers(
        &self,
        model: &str,
        revision: &str,
        yaml: String,
        timeout: Duration,
    ) -> ApiResult<Vec<WorkerProbe>> {
        let deadline = Instant::now() + timeout;
        self.shared.metadata.ping().await.map_err(|_| {
            Error::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "cluster_metadata_unavailable",
                "集群元数据不可用",
            )
        })?;
        tokio::time::timeout_at(deadline, self.shared.objects.model(model, revision, yaml))
            .await
            .map_err(|_| unavailable())?
            .map_err(|_| {
                Error::new(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "cluster_storage_unavailable",
                    "集群模型存储不可用",
                )
            })?;
        let timeout = deadline.saturating_duration_since(Instant::now());
        if timeout.is_zero() {
            return Err(unavailable());
        }
        Ok(
            futures::stream::iter(self.workers.clone().into_iter().enumerate())
                .map(|(index, endpoint)| async move {
                    let url = endpoint
                        .join(&format!("/internal/v1/readiness/{model}/{revision}"))
                        .expect("validated cluster URL and model revision");
                    let probe = async {
                        Some(
                            self.shared
                                .client
                                .get(url)
                                .bearer_auth(&self.shared.token)
                                .timeout(timeout)
                                .send()
                                .await
                                .ok()?
                                .error_for_status()
                                .ok()?
                                .json()
                                .await
                                .ok()?,
                        )
                    }
                    .await;
                    WorkerProbe {
                        worker: index + 1,
                        probe,
                    }
                })
                .buffered(self.workers.len())
                .collect()
                .await,
        )
    }

    pub(super) fn start_cleanup(
        &self,
        shutdown: crate::CancellationToken,
        app: std::sync::Weak<App>,
    ) {
        let shared = self.shared.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(300));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    _ = interval.tick() => {
                        let Some(app) = app.upgrade() else { break };
                        let leader = app.ha.as_ref().map(|ha| ha.leader.as_ref());
                        let timeout = Duration::from_secs(120);
                        let (results, generations) = tokio::select! {
                            _ = shutdown.cancelled() => break,
                            sweeps = async {
                                tokio::join!(
                                    tokio::time::timeout(timeout, shared.objects.expire_results()),
                                    tokio::time::timeout(timeout, retention::sweep(
                                        &shared.metadata, &shared.objects, chrono::Utc::now(), leader, Some(&app)
                                    ))
                                )
                            } => sweeps,
                        };
                        for (area, result) in [("results", results), ("generations", generations)] {
                            if !matches!(result, Ok(Ok(()))) {
                                super::telemetry::event(opentelemetry::logs::Severity::Warn,
                                    "retention_sweep_failed", json!({"area":area}));
                            }
                        }
                    }
                }
            }
        });
    }

    pub async fn execute(
        &self,
        app: &Arc<App>,
        model: String,
        owner: Principal,
        mut request: query::Request,
        export: bool,
    ) -> ApiResult<Response> {
        let started = Instant::now();
        crate::admin::access::check_query(&app, &owner, &model)?;
        request.tenant_partition = crate::admin::access::partition(app, &owner, &model)?;
        if request.tenant_partition.is_some()
            && (request.csr.is_some() || request.csr_generation.is_some())
        {
            return Err(Error::new(
                StatusCode::FORBIDDEN,
                "tenant_csr",
                "此角色的租户查询需要读取实时来源",
            ));
        }
        let (id, _, mut options) = query::validate_request(&request, app.options.clone())?;
        let _export_slot = if export {
            Some(
                query::acquire(
                    app,
                    &app.export_work,
                    &app.export_waiters,
                    options.timeout,
                    &app.shutdown,
                )
                .await?,
            )
        } else {
            None
        };
        options.timeout = query::remaining(options.timeout, started)?;
        let active = query::admit(app, id, &model, &owner, &mut options).await?;
        let (_, yaml, engine) = app
            .query_model(&model, &request.model_revision, &owner)
            .await?;
        let deadline = started
            + request
                .timeout_ms
                .map_or(app.options.timeout, Duration::from_millis);
        let operation = async {
            let generation = request
                .csr
                .as_ref()
                .map(|csr| csr.generation)
                .or(request.csr_generation);
            let policy = freshness::policy(&engine.catalog, request.freshness.as_ref());
            let inputs = if policy == crate::FreshnessPolicy::default() {
                Vec::new()
            } else {
                freshness::observe(app, &engine.catalog).await
            };
            freshness::check(&policy, &inputs, generation.is_some())?;
            let proof = freshness::Proof::new(policy, &inputs);
            if let Some(proof) = &proof {
                if app.published_model_revision(&model).await? != request.model_revision {
                    return Err(freshness::conflict("严格查询需要当前已发布的模型"));
                }
                if generation.is_none() && proof.policy.require_consistent_snapshot {
                    request.require_consistent_snapshot(options.graph.clone());
                }
            }
            if let Some(generation) = generation {
                if self.graph_partitions == 0 {
                    return Err(Error::not_found());
                }
                let current = self
                    .shared
                    .metadata
                    .graph(&model, &request.model_revision)
                    .await
                    .map_err(storage)?;
                if current.map(|(current, _)| current) != Some(generation) {
                    return Err(Error::new(
                        StatusCode::CONFLICT,
                        "csr_generation_conflict",
                        "CSR 代次已变化，请获取已发布代次",
                    ));
                }
                if let Some(proof) = &proof {
                    csr::verify_freshness(
                        self,
                        &model,
                        &request.model_revision,
                        generation,
                        &proof.policy,
                        &inputs,
                    )
                    .await?;
                }
            }
            self.shared
                .objects
                .model(&model, &request.model_revision, yaml)
                .await
                .map_err(storage)?;
            request.timeout_ms = Some(options.timeout.as_millis().max(1) as u64);
            let managed_ids = super::local_tables::managed::referenced(&engine.catalog);
            let managed_only = !managed_ids.is_empty()
                && models::physical_tables(&engine.catalog)
                    .iter()
                    .all(|reference| {
                        engine
                            .catalog
                            .source(reference)
                            .is_ok_and(|(_, relation, source)| match source {
                                crate::Source::Parquet { tables } => {
                                    tables[relation].iter().all(|p| {
                                        crate::source::parquet_snapshot::logical_id(p).is_some()
                                    })
                                }
                                _ => false,
                            })
                    });
            let selection = if managed_ids.is_empty() {
                None
            } else {
                let backend = self
                    .managed
                    .as_ref()
                    .ok_or_else(super::local_tables::managed::unavailable)?;
                Some(
                    backend
                        .pin_model(
                            managed_ids,
                            &model,
                            deadline.saturating_duration_since(Instant::now()).as_secs() + 120,
                        )
                        .await?,
                )
            };
            let dispatch = Dispatch {
                id,
                attempt: Uuid::new_v4(),
                request,
                managed: selection
                    .map(|s| s.into_dispatch())
                    .transpose()
                    .map_err(super::local_tables::managed::error)?
                    .unwrap_or_default(),
            };
            let job = Job {
                model,
                revision: dispatch.request.model_revision.clone(),
                owner,
                export,
                csr_generation: generation,
                freshness: proof,
                attempt: dispatch.attempt,
                worker: None,
                worker_seen_ms: now_ms(),
                retried: false,
                managed_only,
                fingerprint: fingerprint(&dispatch)?,
                state: JobState::Running,
                deadline_ms: now_ms()
                    + deadline
                        .saturating_duration_since(Instant::now())
                        .as_millis() as u64,
            };
            let payload = self
                .shared
                .objects
                .artifact(id, job.attempt, "request.json");
            self.shared
                .objects
                .put_json(&payload, &dispatch)
                .await
                .map_err(storage)?;
            if !self
                .shared
                .metadata
                .create(
                    id,
                    &job,
                    RESULT_TTL + 60,
                    app.ha.as_ref().map(|ha| ha.leader.as_ref()),
                )
                .await
                .map_err(storage)?
            {
                self.shared.objects.clean_results(id, job.attempt).await;
                if let Some(ha) = &app.ha {
                    ha.leader.confirm().await?;
                }
                return Err(Error::new(
                    StatusCode::CONFLICT,
                    "duplicate_request_id",
                    "查询请求标识已在使用",
                ));
            }
            if active.token.is_cancelled() {
                self.cancel(app, &job.owner, id).await?;
                return Err(cancelled());
            }
            self.dispatch(&dispatch, deadline).await?;
            let mut polls = 0;
            loop {
                let saved = self
                    .shared
                    .metadata
                    .get(id)
                    .await
                    .map_err(storage)?
                    .ok_or_else(expired)?;
                if !matches!(saved.job.state, JobState::Running) {
                    self.heartbeats.lock().unwrap().jobs.remove(&id);
                    return self.response_with_freshness(app, id, &saved.job).await;
                }
                if self.mark_worker_lost(id, &saved).await? {
                    return Err(worker_lost());
                }
                polls += 1;
                tokio::time::sleep(if polls <= 8 {
                    Duration::from_millis(25)
                } else {
                    WAIT
                })
                .await;
            }
        };
        tokio::select! {
            biased;
            _ = app.shutdown.cancelled() => Err(Error::new(StatusCode::SERVICE_UNAVAILABLE, "draining", "服务正在停止，可按请求标识继续读取")),
            _ = tokio::time::sleep_until(deadline) => Err(timeout()),
            result = operation => result,
        }
    }

    async fn mark_worker_lost(&self, id: Uuid, saved: &VersionedJob) -> ApiResult<bool> {
        let Some(worker) = saved.job.worker else {
            return Ok(false);
        };
        if !matches!(saved.job.state, JobState::Running) {
            self.heartbeats.lock().unwrap().jobs.remove(&id);
            return Ok(false);
        }
        // etcd changes the revision on every heartbeat. Time its last change
        // locally so a worker's wall clock cannot speed up or delay expiry.
        let heartbeat_age = self
            .heartbeats
            .lock()
            .unwrap()
            .age(id, saved.revision, Instant::now());
        let heartbeat_timeout = Duration::from_millis(WORKER_HEARTBEAT_TIMEOUT_MS);
        if heartbeat_age <= heartbeat_timeout {
            return Ok(false);
        }
        // A long query can delay lease writes while the worker process still owns
        // the attempt. A live process gets one grace window, but it cannot
        // extend the lifetime of a query indefinitely without heartbeats.
        if heartbeat_age <= 2 * heartbeat_timeout && self.worker_identity_is_live(worker).await {
            return Ok(false);
        }
        let mut job = saved.job.clone();
        if (job.csr_generation.is_some() || job.managed_only)
            && !job.retried
            && now_ms() < job.deadline_ms
        {
            let old_attempt = job.attempt;
            let old_key = self
                .shared
                .objects
                .artifact(id, old_attempt, "request.json");
            if let Ok(mut dispatch) = self
                .shared
                .objects
                .get_json::<Dispatch>(&old_key, DISPATCH_LIMIT)
                .await
                && dispatch.id == id
                && dispatch.attempt == old_attempt
                && fingerprint(&dispatch)? == job.fingerprint
            {
                dispatch.attempt = Uuid::new_v4();
                job.attempt = dispatch.attempt;
                job.fingerprint = fingerprint(&dispatch)?;
                job.worker = None;
                job.worker_seen_ms = now_ms();
                job.retried = true;
                let key = self
                    .shared
                    .objects
                    .artifact(id, job.attempt, "request.json");
                self.shared
                    .objects
                    .put_json(&key, &dispatch)
                    .await
                    .map_err(storage)?;
                if self
                    .shared
                    .metadata
                    .replace(id, saved.revision, &job)
                    .await
                    .map_err(storage)?
                {
                    let objects = self.shared.objects.clone();
                    tokio::spawn(async move {
                        objects.clean_results(id, old_attempt).await;
                    });
                    // A subsequent GET can also dispatch this attempt if the connection drops.
                    self.dispatch(
                        &dispatch,
                        Instant::now()
                            + Duration::from_millis(job.deadline_ms.saturating_sub(now_ms())),
                    )
                    .await?;
                } else {
                    self.shared.objects.clean_results(id, job.attempt).await;
                }
                return Ok(false);
            }
        }
        job.state = failure(&worker_lost());
        if !self
            .shared
            .metadata
            .replace(id, saved.revision, &job)
            .await
            .map_err(storage)?
        {
            return Ok(false);
        }
        let objects = self.shared.objects.clone();
        tokio::spawn(async move {
            objects.clean_results(id, job.attempt).await;
        });
        Ok(true)
    }

    async fn worker_identity_is_live(&self, id: Uuid) -> bool {
        let mut probes = futures::stream::FuturesUnordered::new();
        for endpoint in &self.workers {
            probes.push(self.worker_identity(endpoint));
        }
        while let Some(identity) = probes.next().await {
            if identity == Some(id) {
                return true;
            }
        }
        false
    }

    async fn worker_identity(&self, endpoint: &url::Url) -> Option<Uuid> {
        self.shared
            .client
            .get(endpoint.join("/internal/v1/identity").ok()?)
            .bearer_auth(&self.shared.token)
            .timeout(Duration::from_secs(1))
            .send()
            .await
            .ok()?
            .error_for_status()
            .ok()?
            .json()
            .await
            .ok()
    }

    async fn dispatch(&self, dispatch: &Dispatch, deadline: Instant) -> ApiResult<()> {
        let start = self.next.fetch_add(1, Ordering::Relaxed) % self.workers.len();
        let mut busy = false;
        for url in self
            .workers
            .iter()
            .cycle()
            .skip(start)
            .take(self.workers.len())
        {
            let url = url.join("/internal/v1/jobs").expect("validated worker URL");
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(timeout());
            }
            let response = self
                .shared
                .client
                .post(url)
                .header(WORK_BUDGET_HEADER, remaining.as_millis().max(1).to_string())
                .headers(super::telemetry::trace_headers())
                .bearer_auth(&self.shared.token)
                .timeout(remaining.min(Duration::from_secs(2)))
                .json(dispatch)
                .send()
                .await;
            if let Ok(response) = response {
                if response.status() == StatusCode::ACCEPTED {
                    return Ok(());
                }
                busy |= response.status() == StatusCode::TOO_MANY_REQUESTS;
            }
            let saved = self
                .shared
                .metadata
                .get(dispatch.id)
                .await
                .map_err(storage)?
                .ok_or_else(expired)?;
            if saved.job.worker.is_some() || !matches!(saved.job.state, JobState::Running) {
                return Ok(());
            }
        }
        let error = if busy { Error::busy() } else { unavailable() };
        if let Some(saved) = self
            .shared
            .metadata
            .get(dispatch.id)
            .await
            .map_err(storage)?
        {
            if saved.job.worker.is_some() || !matches!(saved.job.state, JobState::Running) {
                return Ok(());
            }
            let mut job = saved.job;
            job.state = failure(&error);
            if !self
                .shared
                .metadata
                .replace(dispatch.id, saved.revision, &job)
                .await
                .map_err(storage)?
            {
                return Ok(());
            }
            self.shared
                .objects
                .clean_results(dispatch.id, dispatch.attempt)
                .await;
        }
        Err(error)
    }

    async fn recover_dispatch(&self, id: Uuid, job: &Job) -> ApiResult<()> {
        if !matches!(job.state, JobState::Running)
            || job.worker.is_some()
            || now_ms() >= job.deadline_ms
        {
            return Ok(());
        }
        let key = self
            .shared
            .objects
            .artifact(id, job.attempt, "request.json");
        let dispatch: Dispatch = match self.shared.objects.get_json(&key, DISPATCH_LIMIT).await {
            Ok(dispatch) => dispatch,
            Err(error) => {
                if self
                    .shared
                    .metadata
                    .get(id)
                    .await
                    .map_err(storage)?
                    .is_some_and(|saved| saved.job.worker.is_some())
                {
                    return Ok(());
                }
                return Err(storage(error));
            }
        };
        if dispatch.id != id
            || dispatch.attempt != job.attempt
            || fingerprint(&dispatch)? != job.fingerprint
        {
            return Err(storage("invalid persisted request"));
        }
        self.dispatch(
            &dispatch,
            Instant::now() + Duration::from_millis(job.deadline_ms.saturating_sub(now_ms())),
        )
        .await
    }

    async fn owned(
        &self,
        app: &App,
        principal: &Principal,
        id: Uuid,
    ) -> ApiResult<Option<VersionedJob>> {
        let Some(saved) = self.shared.metadata.get(id).await.map_err(storage)? else {
            return Ok(None);
        };
        if !access::owns_query(principal, &saved.job.owner) {
            return Ok(None);
        }
        crate::admin::access::check_query(&app, principal, &saved.job.model)?;
        Ok(Some(saved))
    }

    pub async fn cancel(
        &self,
        app: &App,
        principal: &Principal,
        id: Uuid,
    ) -> ApiResult<Option<String>> {
        loop {
            let Some(saved) = self.owned(app, principal, id).await? else {
                return Ok(None);
            };
            let mut job = saved.job;
            match job.state {
                JobState::Running => job.state = JobState::Cancelled,
                JobState::Cancelled => return Ok(Some(job.model)),
                _ => return Err(Error::not_found()),
            }
            if self
                .shared
                .metadata
                .replace(id, saved.revision, &job)
                .await
                .map_err(storage)?
            {
                return Ok(Some(job.model));
            }
        }
    }
}

fn failure(error: &Error) -> JobState {
    JobState::Failed {
        code: error.code.into(),
        message: error.message.clone(),
        status: error.status.as_u16(),
    }
}

fn fingerprint(dispatch: &Dispatch) -> ApiResult<[u8; 32]> {
    let bytes = serde_json::to_vec(dispatch).map_err(storage)?;
    if bytes.len() > DISPATCH_LIMIT as usize {
        return Err(Error::invalid(
            "cluster_request_limit",
            "集群查询请求超过大小上限",
        ));
    }
    Ok(Sha256::digest(bytes).into())
}

fn storage(_: impl std::fmt::Display) -> Error {
    Error::new(
        StatusCode::SERVICE_UNAVAILABLE,
        "cluster_storage",
        "集群元数据或结果存储不可用",
    )
}

fn unavailable() -> Error {
    Error::new(
        StatusCode::SERVICE_UNAVAILABLE,
        "worker_unavailable",
        "没有可用的查询节点",
    )
}

fn worker_lost() -> Error {
    Error::new(
        StatusCode::SERVICE_UNAVAILABLE,
        "worker_lost",
        "查询节点已断开，请重新运行查询",
    )
}

fn timeout() -> Error {
    Error::new(StatusCode::GATEWAY_TIMEOUT, "timeout", "查询或校验超时")
}

fn cancelled() -> Error {
    Error::new(StatusCode::CONFLICT, "cancelled", "查询已取消")
}

fn expired() -> Error {
    Error::new(
        StatusCode::NOT_FOUND,
        "result_expired",
        "本次结果已过期，请重新运行查询",
    )
}

/// Run a loopback worker. Cross-host deployment terminates mTLS at a private proxy.
pub async fn serve_worker(
    config: PathBuf,
    source_dir: PathBuf,
    port: u16,
    slots: usize,
) -> Result<()> {
    let (config, shared) = Config::load(&config)?;
    let source_dir = source_dir
        .canonicalize()
        .context("opening worker source directory")?;
    ensure!(
        source_dir.is_dir() && (1..=8).contains(&slots),
        "invalid worker source directory or slots"
    );
    let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port)).await?;
    let addr = listener.local_addr()?;
    let (router, shutdown) = worker::router(shared, config, source_dir, slots);
    eprintln!("GraphLake worker http://{addr}");
    super::telemetry::event(
        opentelemetry::logs::Severity::Info,
        "worker_started",
        json!({"query_slots": slots}),
    );
    let signal = tokio::spawn(super::ops::shutdown_signal(shutdown.clone()));
    let result = axum::serve(listener, router)
        .with_graceful_shutdown(shutdown.clone().cancelled_owned())
        .await;
    shutdown.cancel();
    signal.abort();
    Ok(result?)
}

#[cfg(test)]
mod heartbeat_tests {
    use super::*;

    #[test]
    fn relative_budget_ignores_sender_wall_clock_and_keeps_local_queue_time() {
        let received = Instant::now();
        let mut headers = HeaderMap::new();
        headers.insert(WORK_BUDGET_HEADER, "500".parse().unwrap());
        assert_eq!(
            work_deadline(&headers, received, 1).ok(),
            Some(received + Duration::from_millis(500))
        );
        assert_eq!(
            work_deadline(&headers, received, u64::MAX).ok(),
            Some(received + Duration::from_millis(500))
        );
    }

    #[test]
    fn observations_follow_revision_progress_on_the_coordinator_clock() {
        let id = Uuid::new_v4();
        let start = Instant::now();
        let mut heartbeats = Heartbeats::default();
        assert_eq!(heartbeats.age(id, 17, start), Duration::ZERO);
        assert_eq!(
            heartbeats.age(id, 17, start + Duration::from_secs(13)),
            Duration::from_secs(13)
        );
        assert_eq!(
            heartbeats.age(id, 18, start + Duration::from_secs(14)),
            Duration::ZERO
        );
        assert_eq!(
            heartbeats.age(id, 17, start + Duration::from_secs(15)),
            Duration::ZERO
        );
        assert_eq!(
            heartbeats.age(id, 18, start + Duration::from_secs(16)),
            Duration::from_secs(2)
        );
    }
}
