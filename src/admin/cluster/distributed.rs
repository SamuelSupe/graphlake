//! Execute a bounded DataFusion stage graph using committed Arrow shuffle files.
mod plan;
mod shuffle;

use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Result, ensure};
use axum::{
    Json,
    extract::State,
    http::{HeaderMap, StatusCode},
};
use datafusion::{
    execution::{SessionState, TaskContext},
    physical_plan::{ExecutionPlan, ExecutionPlanProperties},
};
use futures::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{JobState, Shared, WORK_BUDGET_HEADER, work_deadline, worker::Worker};
use crate::{QueryOptions, engine::QueryScope, source::parquet::ScanObject};
use tokio::time::Instant;

const MAX_PLAN: u64 = 768 * 1024;
const MAX_STAGES: usize = 48;

#[derive(Clone)]
pub(crate) struct Distributed {
    shared: Shared,
    workers: Vec<url::Url>,
    id: Uuid,
    attempt: Uuid,
    leader: Uuid,
    deadline: Instant,
    pub(crate) partitions: usize,
    min_rows: usize,
    stats: Arc<Mutex<Stats>>,
    unavailable: Arc<Mutex<BTreeSet<url::Url>>>,
}

#[derive(Default, Clone, Serialize)]
pub(crate) struct Stats {
    pub workers: BTreeSet<Uuid>,
    pub stages: usize,
    pub partitions: usize,
    pub partition_retries: usize,
    pub shuffle_bytes: u64,
    pub operator_spill_bytes: u64,
    pub parquet_partitions: usize,
    pub join_partitions: usize,
    pub partial_aggregate_partitions: usize,
    pub remote_partitions: usize,
    pub remote_join_partitions: usize,
    pub remote_partial_aggregate_partitions: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Output {
    task: Uuid,
    buckets: Vec<u64>,
    worker: Uuid,
    operator_spill: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct Task {
    plan: String,
    inputs: Vec<plan::Input>,
    parquet: Option<Vec<ScanObject>>,
    exchange: Option<String>,
    partition: usize,
    graph: bool,
    memory_bytes: usize,
    spill_bytes: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct StageRequest<T> {
    id: Uuid,
    attempt: Uuid,
    task: Uuid,
    plan: T,
}

impl Distributed {
    pub(super) fn new(
        shared: Shared,
        workers: Vec<url::Url>,
        id: Uuid,
        attempt: Uuid,
        leader: Uuid,
        deadline: Instant,
        partitions: usize,
        min_rows: usize,
    ) -> Self {
        Self {
            shared,
            workers,
            id,
            attempt,
            leader,
            deadline,
            partitions,
            min_rows,
            stats: Default::default(),
            unavailable: Default::default(),
        }
    }

    pub(crate) fn stats(&self) -> Option<Stats> {
        let stats = self.stats.lock().unwrap();
        (stats.stages > 0).then(|| stats.clone())
    }

    pub(crate) fn configure(state: &mut SessionState, memory_bytes: usize) {
        let config = state.config_mut().options_mut();
        // Staged readers consume committed data rather than live upstream operators.
        config.optimizer.enable_join_dynamic_filter_pushdown = false;
        config.optimizer.enable_topk_dynamic_filter_pushdown = false;
        config.optimizer.enable_aggregate_dynamic_filter_pushdown = false;
        config.optimizer.repartition_file_scans = false;
        config.execution.enable_file_stream_work_stealing = false;
        // Separate stage tasks cannot share a CollectLeft build-side bitmap.
        config.optimizer.hash_join_single_partition_threshold = 0;
        config.optimizer.hash_join_single_partition_threshold_rows = 0;
        if memory_bytes <= 64 * 1024 * 1024 {
            config.optimizer.prefer_hash_join = false;
            config.execution.sort_spill_reservation_bytes = 256 * 1024;
        }
    }

    pub(crate) async fn execute(
        &self,
        root: Arc<dyn ExecutionPlan>,
        scope: &QueryScope,
        ctx: Arc<TaskContext>,
        graph: bool,
        allow_materialized_inputs: bool,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let has_staged_input =
            allow_materialized_inputs && self.stats.lock().unwrap().parquet_partitions > 0;
        if self.min_rows > 0
            && !has_staged_input
            && input_rows(root.as_ref()).is_some_and(|rows| rows < self.min_rows)
        {
            return Ok(root);
        }
        let Some((root, stages)) =
            plan::build(Arc::clone(&root), self.partitions, has_staged_input)?
        else {
            return Ok(root);
        };
        let mut committed = Vec::new();
        let mut bytes = self.stats.lock().unwrap().shuffle_bytes;
        let local_slot = tokio::sync::Semaphore::new(1);
        for stage in stages {
            scope.check()?;
            let spill_bytes = scope.options.spill_bytes.saturating_sub(bytes);
            let partitions = stage.input.output_partitioning().partition_count();
            let mut tasks = futures::stream::iter(0..partitions)
                .map(|partition| {
                    let ctx = Arc::clone(&ctx);
                    let stage = &stage;
                    let committed = &committed;
                    let local_slot = &local_slot;
                    async move {
                        let local = || async {
                            let _slot = local_slot.acquire().await?;
                            let input =
                                plan::bind(Arc::clone(&stage.input), committed, self, scope)?;
                            shuffle::write(
                                input,
                                stage
                                    .exchange
                                    .as_ref()
                                    .map(|exchange| exchange.partitioning()),
                                partition,
                                &ctx,
                                scope,
                                self,
                                Uuid::new_v4(),
                                self.leader,
                            )
                            .await
                        };
                        if let Some(task) =
                            stage.task(partition, committed, graph, scope, spill_bytes)?
                        {
                            if let Some(output) = self.dispatch(task, stage.id + partition).await? {
                                Ok(output)
                            } else {
                                local().await
                            }
                        } else {
                            local().await
                        }
                    }
                })
                .buffer_unordered(if stage.remote() {
                    self.workers.len().saturating_sub(1).max(1)
                } else {
                    1
                });
            let mut outputs = Vec::with_capacity(partitions);
            while let Some(output) = tasks.try_next().await? {
                bytes = bytes.saturating_add(output.buckets.iter().sum::<u64>());
                ensure!(
                    bytes <= scope.options.spill_bytes,
                    "distributed shuffle exceeds query spill budget"
                );
                outputs.push(output);
            }
            drop(tasks);
            ensure!(outputs.len() == partitions, "missing distributed outputs");
            let mut stats = self.stats.lock().unwrap();
            stats
                .workers
                .extend(outputs.iter().map(|output| output.worker));
            stats.stages += 1;
            stats.partitions += outputs.len();
            stats.parquet_partitions += usize::from(stage.parquet.is_some()) * outputs.len();
            stats.join_partitions += usize::from(stage.join) * outputs.len();
            stats.partial_aggregate_partitions += usize::from(stage.aggregate) * outputs.len();
            let remote = outputs
                .iter()
                .filter(|output| output.worker != self.leader)
                .count();
            stats.remote_partitions += remote;
            stats.remote_join_partitions += usize::from(stage.join) * remote;
            stats.remote_partial_aggregate_partitions += usize::from(stage.aggregate) * remote;
            stats.operator_spill_bytes += outputs
                .iter()
                .map(|output| output.operator_spill)
                .sum::<u64>();
            stats.shuffle_bytes = bytes;
            committed.push(outputs);
        }
        self.stats.lock().unwrap().workers.insert(self.leader);
        plan::bind(root, &committed, self, scope)
    }

    async fn dispatch(&self, task: Task, start: usize) -> Result<Option<Output>> {
        if serde_json::to_vec(&task)?.len() as u64 > MAX_PLAN - 256 {
            return Ok(None);
        }
        let mut retries = 0;
        // Never replay a live database read. Remote stages only consume pinned objects
        // and committed shuffle files, so a lost attempt can move to another worker.
        for round in 0..2 {
            for worker in self
                .workers
                .iter()
                .cycle()
                .skip(start % self.workers.len())
                .take(self.workers.len())
            {
                if self.unavailable.lock().unwrap().contains(worker) {
                    continue;
                }
                let attempt = Uuid::new_v4();
                let request = StageRequest {
                    id: self.id,
                    attempt: self.attempt,
                    task: attempt,
                    plan: &task,
                };
                let remaining = self.deadline.saturating_duration_since(Instant::now());
                let remaining = remaining.as_millis() as u64;
                ensure!(remaining > 0, "query deadline exceeded");
                let request = self
                    .shared
                    .client
                    .post(worker.join("/internal/v1/stages")?)
                    .headers(crate::admin::telemetry::trace_headers())
                    .bearer_auth(&self.shared.token)
                    .timeout(Duration::from_millis(remaining))
                    .header(WORK_BUDGET_HEADER, remaining.to_string())
                    .json(&request)
                    .send();
                let disconnected = async {
                    let identity = worker.join("/internal/v1/identity")?;
                    let mut misses = 0;
                    loop {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        let response = self
                            .shared
                            .client
                            .get(identity.clone())
                            .bearer_auth(&self.shared.token)
                            .timeout(Duration::from_millis(750))
                            .send()
                            .await;
                        if response.is_ok_and(|response| response.status().is_success()) {
                            misses = 0;
                        } else {
                            misses += 1;
                            if misses == 2 {
                                return anyhow::Ok(());
                            }
                        }
                    }
                };
                let response = tokio::select! {
                    response = request => Some(response),
                    result = disconnected => { result?; None }
                };
                match response {
                    Some(Ok(response)) if response.status().is_success() => {
                        match response.json::<Output>().await {
                            Ok(output) => {
                                ensure!(output.task == attempt, "invalid stage attempt");
                                self.stats.lock().unwrap().partition_retries += retries;
                                return Ok(Some(output));
                            }
                            Err(_) => retries += 1,
                        }
                    }
                    Some(Ok(response))
                        if response.status() == StatusCode::TOO_MANY_REQUESTS
                            || response.status() == StatusCode::CONFLICT =>
                    {
                        if response.status() == StatusCode::CONFLICT {
                            self.unavailable.lock().unwrap().insert(worker.clone());
                        }
                    }
                    Some(Ok(response)) if response.status() == StatusCode::PRECONDITION_FAILED => {
                        anyhow::bail!("distributed Parquet snapshot changed")
                    }
                    Some(Ok(response)) if response.status() == StatusCode::NOT_IMPLEMENTED => {
                        return Ok(None);
                    }
                    Some(Ok(response)) if response.status() == StatusCode::BAD_GATEWAY => {
                        self.unavailable.lock().unwrap().insert(worker.clone());
                        retries += 1;
                    }
                    Some(Ok(response)) if response.status().is_server_error() => retries += 1,
                    Some(Ok(response)) => {
                        let status = response.status();
                        let error = response
                            .json::<serde_json::Value>()
                            .await
                            .unwrap_or_default();
                        anyhow::bail!(
                            "distributed partition failed ({status}): {}",
                            error["message"]
                                .as_str()
                                .unwrap_or("stage execution failed")
                        );
                    }
                    Some(Err(error)) => {
                        if error.is_connect() {
                            self.unavailable.lock().unwrap().insert(worker.clone());
                        }
                        retries += 1;
                    }
                    None => {
                        self.unavailable.lock().unwrap().insert(worker.clone());
                        retries += 1;
                    }
                }
                self.shared
                    .objects
                    .clean_stage(self.id, self.attempt, attempt)
                    .await;
            }
            if round == 0 {
                if self.unavailable.lock().unwrap().len() == self.workers.len() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(60)).await;
            }
        }
        self.stats.lock().unwrap().partition_retries += retries;
        Ok(None)
    }

    fn key(&self, task: Uuid, name: &str) -> object_store::path::Path {
        self.shared.objects.stage(self.id, self.attempt, task, name)
    }
}

fn input_rows(plan: &dyn ExecutionPlan) -> Option<usize> {
    use datafusion::physical_plan::statistics::{StatisticsArgs, StatisticsContext};
    fn count(plan: &dyn ExecutionPlan, stats: &StatisticsContext) -> Option<usize> {
        let children = plan.children();
        if children.is_empty() {
            return stats
                .compute(plan, &StatisticsArgs::default())
                .ok()?
                .num_rows
                .get_value()
                .copied();
        }
        children.into_iter().try_fold(0_usize, |sum, child| {
            sum.checked_add(count(child.as_ref(), stats)?)
        })
    }
    count(plan, &StatisticsContext::new())
}

pub(super) async fn run_task(
    State(worker): State<Arc<Worker>>,
    headers: HeaderMap,
    Json(request): Json<StageRequest<Task>>,
) -> super::super::ApiResult<Json<Output>> {
    let started = Instant::now();
    let job = worker
        .shared
        .metadata
        .get(request.id)
        .await
        .map_err(super::storage)?
        .filter(|saved| {
            saved.job.attempt == request.attempt && matches!(saved.job.state, JobState::Running)
        })
        .ok_or_else(super::cancelled)?
        .job;
    let leader = job.worker.ok_or_else(super::cancelled)?;
    if leader == worker.id {
        return Err(super::super::Error::new(
            StatusCode::CONFLICT,
            "stage_leader",
            "分区交给其他节点",
        ));
    }
    let _slot = worker
        .stage_slots
        .clone()
        .try_acquire_owned()
        .map_err(|_| super::super::Error::busy())?;
    let deadline = work_deadline(&headers, started, job.deadline_ms)?;
    let cluster = Distributed::new(
        worker.shared.clone(),
        Vec::new(),
        request.id,
        request.attempt,
        leader,
        deadline,
        1,
        0,
    );
    let task = request.plan;
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(super::timeout());
    }
    let cancellation = worker.shutdown.child_token();
    let options = QueryOptions {
        timeout: remaining,
        cancellation: cancellation.clone(),
        memory_bytes: task.memory_bytes.min(worker.memory_bytes),
        spill_bytes: task.spill_bytes,
        partitions: 1,
        ..Default::default()
    };
    let scope = Arc::new(
        crate::engine::QueryScope::new(options, &worker.admission)
            .await
            .map_err(super::query::query_error)?,
    );
    let context = if task.graph {
        scope.graph_session()
    } else {
        scope.session()
    };
    let ctx = context.task_ctx();
    let operation = async {
        let partition = task.partition;
        let (input, exchange) = plan::decode(task, &ctx, &scope, &cluster).await?;
        shuffle::write(
            input,
            exchange.as_ref().map(|plan| plan.partitioning()),
            partition,
            &ctx,
            &scope,
            &cluster,
            request.task,
            worker.id,
        )
        .await
    };
    let monitor = async {
        loop {
            tokio::time::sleep(super::WAIT).await;
            if !worker
                .shared
                .metadata
                .get(request.id)
                .await
                .is_ok_and(|saved| {
                    saved.is_some_and(|saved| {
                        saved.job.attempt == request.attempt
                            && matches!(saved.job.state, JobState::Running)
                    })
                })
            {
                cancellation.cancel();
                break;
            }
        }
    };
    let result = tokio::select! {
        result = scope.run(operation) => result,
        _ = monitor => Err(anyhow::anyhow!("query cancelled")),
    };
    match result {
        Ok(output) => Ok(Json(output)),
        Err(error) if snapshot_changed(&error) => Err(super::super::Error::new(
            StatusCode::PRECONDITION_FAILED,
            "graph_sources_changed",
            "Parquet 来源版本已变化",
        )),
        Err(error)
            if error.chain().any(|cause| {
                cause
                    .downcast_ref::<datafusion::common::DataFusionError>()
                    .is_some_and(|error| {
                        matches!(
                            error,
                            datafusion::common::DataFusionError::NotImplemented(_)
                        )
                    })
            }) =>
        {
            Err(super::super::Error::new(
                StatusCode::NOT_IMPLEMENTED,
                "stage_operator",
                "当前节点无法执行此分区计划",
            ))
        }
        Err(error) => Err(super::query::query_error(error)),
    }
}

pub(crate) fn snapshot_changed(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        let message = cause.to_string();
        message.contains("distributed Parquet snapshot changed")
            || message.contains("Parquet object changed during query")
            || message.contains("local Parquet file changed during range read")
            || matches!(
                cause.downcast_ref::<object_store::Error>(),
                Some(object_store::Error::Precondition { .. })
            )
    })
}
