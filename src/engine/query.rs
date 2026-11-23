pub(in crate::engine) mod allocation;

use std::{
    collections::BTreeMap,
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    task::{Poll, Waker},
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use datafusion::{
    common::DataFusionError,
    dataframe::DataFrame,
    execution::{
        SendableRecordBatchStream,
        context::SessionState,
        memory_pool::{FairSpillPool, PeakRecordingPool},
        runtime_env::{RuntimeEnv, RuntimeEnvBuilder},
    },
    logical_expr::LogicalPlan,
    physical_plan::{ExecutionPlan, displayable, execute_stream, stream::RecordBatchStreamAdapter},
    prelude::{SessionConfig, SessionContext},
    sql::parser::Statement,
};
use futures::{TryStreamExt, stream::poll_fn};
use serde::Serialize;
use tokio::{sync::Notify, time::Instant};
use tokio_util::sync::CancellationToken;

use crate::{GraphOptions, ReadMetrics, SourceVersion, source::ReadStats};

use super::Prepared;

pub(crate) fn graph_statement(
    state: &SessionState,
    sql: &str,
) -> datafusion::common::Result<Statement> {
    // A generated traversal can contain several nested SELECTs per step.
    // The SQL parser's debug stack frames can exceed an embedding thread's stack.
    stacker::maybe_grow(3 * 1024 * 1024, 8 * 1024 * 1024, || {
        state.sql_to_statement(sql, &state.config_options().sql_parser.dialect)
    })
}

pub(super) fn gremlin_statement(
    state: &SessionState,
    sql: &str,
) -> datafusion::common::Result<Statement> {
    use datafusion::sql::sqlparser::parser::ParserError;
    let statement = match graph_statement(state, sql) {
        Err(DataFusionError::SQL(error, _))
            if matches!(*error, ParserError::RecursionLimitExceeded) =>
        {
            return datafusion::common::plan_err!(
                "Gremlin traversal exceeds the correlated plan size limit"
            );
        }
        result => result?,
    };
    crate::gremlin::cost::check(&statement)?;
    Ok(statement)
}

pub(crate) async fn graph_frame(
    context: &SessionContext,
    sql: &str,
) -> datafusion::common::Result<DataFrame> {
    generated_frame(context, sql, false).await
}

pub(crate) async fn gremlin_frame(
    context: &SessionContext,
    sql: &str,
) -> datafusion::common::Result<DataFrame> {
    generated_frame(context, sql, true).await
}

async fn generated_frame(
    context: &SessionContext,
    sql: &str,
    gremlin: bool,
) -> datafusion::common::Result<DataFrame> {
    let state = context.state();
    let statement = if gremlin {
        gremlin_statement(&state, sql)?
    } else {
        graph_statement(&state, sql)?
    };
    context
        .execute_logical_plan(graph_logical_plan(&state, statement).await?)
        .await
}

pub(super) async fn graph_logical_plan(
    state: &SessionState,
    statement: Statement,
) -> datafusion::common::Result<LogicalPlan> {
    planner_stack(state.statement_to_plan(statement)).await
}

pub(super) async fn planner_stack<T>(plan: impl Future<Output = T>) -> T {
    // Logical and physical planners can run beneath several nested CALLs.
    // An alternate stack is only needed for the duration of each poll.
    let mut plan = std::pin::pin!(plan);
    std::future::poll_fn(|cx| {
        stacker::maybe_grow(2 * 1024 * 1024, 8 * 1024 * 1024, || plan.as_mut().poll(cx))
    })
    .await
}

#[derive(Debug, Clone)]
pub struct QueryOptions {
    pub timeout: Duration,
    pub memory_bytes: usize,
    pub spill_bytes: u64,
    pub partitions: usize,
    pub batch_size: usize,
    pub cancellation: CancellationToken,
    pub graph: GraphOptions,
    /// Named or positional Cypher parameters represented as JSON values.
    pub parameters: BTreeMap<String, serde_json::Value>,
}

impl Default for QueryOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(60),
            memory_bytes: 256 * 1024 * 1024,
            spill_bytes: 1024 * 1024 * 1024,
            partitions: 2,
            batch_size: 1024,
            cancellation: CancellationToken::new(),
            graph: GraphOptions::default(),
            parameters: BTreeMap::new(),
        }
    }
}

impl QueryOptions {
    /// Bind a JSON object of Cypher parameters.
    pub fn with_parameters_json(mut self, parameters: &str) -> Result<Self> {
        self.parameters =
            serde_json::from_str(parameters).context("invalid Cypher parameter object")?;
        Ok(self)
    }
}

pub struct QueryResult {
    pub graph: String,
    pub versions: Vec<SourceVersion>,
    pub physical_plan: String,
    pub batches: SendableRecordBatchStream,
    pub(super) scope: Arc<QueryScope>,
    #[cfg(feature = "admin")]
    pub(crate) graph_output: super::query_graph::GraphOutput,
    #[cfg(feature = "admin")]
    pub(crate) graph_context: Option<SessionContext>,
    #[cfg(feature = "admin")]
    pub(crate) graph_csr: Option<crate::admin::cluster::csr::CypherCsr>,
    #[cfg(feature = "admin")]
    resources: std::sync::Weak<ResultResources>,
    #[cfg(feature = "admin")]
    graph_resources: Option<Arc<ResultResources>>,
}

struct ResultResources {
    _sources: Vec<datafusion::execution::memory_pool::MemoryReservation>,
    _lease: Lease,
}

/// Aggregate limits for active queries, including their result streams.
#[derive(Debug, Clone, Copy)]
pub struct EngineLimits {
    pub max_queries: usize,
    pub memory_bytes: usize,
    pub spill_bytes: u64,
}

impl Default for EngineLimits {
    fn default() -> Self {
        Self {
            max_queries: 8,
            memory_bytes: 2 * 1024 * 1024 * 1024,
            spill_bytes: 8 * 1024 * 1024 * 1024,
        }
    }
}

#[derive(Default)]
struct Usage {
    queries: usize,
    memory: usize,
    spill: u64,
}

pub(crate) struct Admission {
    limits: EngineLimits,
    usage: Mutex<Usage>,
    changed: Notify,
    pub(super) delta_snapshots: crate::source::delta::SnapshotCache,
}

impl Admission {
    pub(crate) fn new(limits: EngineLimits) -> Result<Self> {
        ensure!(
            limits.max_queries > 0 && limits.memory_bytes > 0,
            "engine query and memory limits must be positive"
        );
        Ok(Self {
            limits,
            usage: Mutex::default(),
            changed: Notify::new(),
            delta_snapshots: crate::source::delta::SnapshotCache::new(limits.memory_bytes),
        })
    }

    async fn acquire(self: &Arc<Self>, options: &QueryOptions, deadline: Instant) -> Result<Lease> {
        let limits = self.limits;
        let memory = options.memory_bytes;
        let spill = options.spill_bytes;
        ensure!(
            memory <= limits.memory_bytes && spill <= limits.spill_bytes,
            "query resource budget exceeds the engine's aggregate limit"
        );
        loop {
            let changed = self.changed.notified();
            ensure!(!options.cancellation.is_cancelled(), "query cancelled");
            ensure!(
                Instant::now() < deadline,
                "query deadline exceeded waiting for engine resources"
            );
            {
                let mut usage = self.usage.lock().expect("engine resource accounting");
                if usage.queries < limits.max_queries
                    && memory <= limits.memory_bytes - usage.memory
                    && spill <= limits.spill_bytes - usage.spill
                {
                    usage.queries += 1;
                    usage.memory += memory;
                    usage.spill += spill;
                    return Ok(Lease {
                        admission: Arc::clone(self),
                        memory,
                        spill,
                    });
                }
            }
            tokio::select! {
                _ = options.cancellation.cancelled() => bail!("query cancelled"),
                _ = tokio::time::sleep_until(deadline) => bail!("query deadline exceeded waiting for engine resources"),
                _ = changed => {},
            }
        }
    }
}

struct Lease {
    admission: Arc<Admission>,
    memory: usize,
    spill: u64,
}

impl Drop for Lease {
    fn drop(&mut self) {
        let mut usage = self
            .admission
            .usage
            .lock()
            .expect("engine resource accounting");
        usage.queries -= 1;
        usage.memory -= self.memory;
        usage.spill -= self.spill;
        drop(usage);
        self.admission.changed.notify_waiters();
    }
}

#[derive(Debug, Serialize)]
pub struct QueryMetrics {
    pub elapsed_ms: u64,
    pub first_batch_ms: Option<u64>,
    pub output_rows: u64,
    pub peak_reserved_bytes: usize,
    pub active_spill_bytes: u64,
    pub spilled_bytes: u64,
    pub reads: ReadMetrics,
    pub visits_by_depth: Vec<u64>,
    pub csr_generation: Option<String>,
    pub algorithm: Option<crate::GraphAlgorithmMetrics>,
}

impl QueryResult {
    #[cfg(feature = "admin")]
    pub(super) fn retain_graph_resources(&mut self) {
        self.graph_resources = self.resources.upgrade();
    }

    #[cfg(feature = "admin")]
    pub(crate) async fn graph_read<T>(
        &mut self,
        future: impl Future<Output = Result<T>>,
    ) -> Result<T> {
        self.scope.run(future).await
    }

    #[cfg(feature = "admin")]
    pub(crate) fn graph_options(&self) -> &GraphOptions {
        &self.scope.options.graph
    }

    #[cfg(feature = "admin")]
    pub(crate) fn graph_scope(&self) -> Arc<QueryScope> {
        self.scope.clone()
    }

    /// Return the metrics accumulated so far. Consume the stream to completion for final counts.
    pub fn metrics(&self) -> QueryMetrics {
        QueryMetrics {
            first_batch_ms: match self.scope.first_batch_ms.load(Ordering::Relaxed) {
                u64::MAX => None,
                value => Some(value),
            },
            elapsed_ms: self
                .scope
                .finished_ms
                .lock()
                .expect("elapsed time")
                .unwrap_or_else(|| self.scope.start.elapsed().as_millis() as u64),
            output_rows: self.scope.output_rows.load(Ordering::Relaxed),
            peak_reserved_bytes: self.scope.pool.peak_reserved(),
            active_spill_bytes: self
                .scope
                .runtime
                .disk_manager
                .spilling_progress()
                .current_bytes,
            spilled_bytes: self
                .scope
                .spilled_bytes
                .load(Ordering::Relaxed)
                .saturating_add(
                    self.scope
                        .intermediate_spilled_bytes
                        .load(Ordering::Relaxed),
                ),
            reads: self.scope.reads.metrics(),
            algorithm: self
                .scope
                .algorithm
                .lock()
                .expect("algorithm metrics")
                .clone(),
            csr_generation: self
                .scope
                .csr_generation
                .lock()
                .expect("CSR metrics")
                .clone(),
            visits_by_depth: self
                .scope
                .visits_by_depth
                .lock()
                .expect("depth metrics")
                .clone(),
        }
    }
}

pub(crate) struct QueryScope {
    pub(crate) options: QueryOptions,
    start: Instant,
    pub(crate) wall_time: chrono::DateTime<chrono::Utc>,
    pub(crate) deadline: Instant,
    lease: Mutex<Option<Lease>>,
    pub(crate) runtime: Arc<RuntimeEnv>,
    pool: Arc<PeakRecordingPool>,
    pub(crate) reads: Arc<ReadStats>,
    output_rows: AtomicU64,
    first_batch_ms: AtomicU64,
    finished_ms: Mutex<Option<u64>>,
    spilled_bytes: AtomicU64,
    intermediate_spilled_bytes: Arc<AtomicU64>,
    pub(crate) visits_by_depth: Arc<Mutex<Vec<u64>>>,
    pub(crate) csr_generation: Mutex<Option<String>>,
    #[cfg(feature = "admin")]
    pub(crate) clock_labels: Mutex<std::collections::BTreeSet<String>>,
    pub(crate) algorithm: Arc<Mutex<Option<crate::GraphAlgorithmMetrics>>>,
}

impl QueryScope {
    #[cfg(feature = "admin")]
    pub(crate) fn remaining(&self) -> Result<Duration> {
        self.check()?;
        Ok(self.deadline.saturating_duration_since(Instant::now()))
    }

    pub(crate) async fn new(options: QueryOptions, admission: &Arc<Admission>) -> Result<Self> {
        ensure!(!options.timeout.is_zero(), "query timeout must be positive");
        ensure!(
            options.memory_bytes > 0 && options.partitions > 0 && options.batch_size > 0,
            "memory, partitions and batch size must be positive"
        );
        options.graph.validate()?;
        let start = Instant::now();
        let wall_time = chrono::Utc::now();
        let deadline = start
            .checked_add(options.timeout)
            .context("query timeout is too large")?;
        let lease = admission.acquire(&options, deadline).await?;
        let pool = Arc::new(PeakRecordingPool::new(Arc::new(FairSpillPool::new(
            options.memory_bytes,
        ))));
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(pool.clone())
            .with_max_temp_directory_size(options.spill_bytes)
            .build_arc()?;
        Ok(Self {
            options,
            start,
            wall_time,
            deadline,
            lease: Mutex::new(Some(lease)),
            runtime,
            pool,
            reads: Arc::default(),
            output_rows: AtomicU64::new(0),
            first_batch_ms: AtomicU64::new(u64::MAX),
            finished_ms: Mutex::new(None),
            spilled_bytes: AtomicU64::new(0),
            intermediate_spilled_bytes: Arc::default(),
            visits_by_depth: Arc::default(),
            csr_generation: Mutex::default(),
            #[cfg(feature = "admin")]
            clock_labels: Mutex::default(),
            algorithm: Arc::default(),
        })
    }

    pub(crate) fn check(&self) -> Result<()> {
        ensure!(!self.options.cancellation.is_cancelled(), "query cancelled");
        ensure!(Instant::now() < self.deadline, "query deadline exceeded");
        Ok(())
    }

    pub(crate) fn elapsed_ms(&self) -> u64 {
        self.start.elapsed().as_millis() as u64
    }

    pub(crate) fn peak_reserved_bytes(&self) -> usize {
        self.pool.peak_reserved()
    }

    pub(crate) fn session(&self) -> SessionContext {
        let config = SessionConfig::new()
            .with_target_partitions(self.options.partitions)
            .with_batch_size(self.options.batch_size);
        let context = SessionContext::new_with_config_rt(config, Arc::clone(&self.runtime));
        allocation::register(&context, self, allocation::BUILT_INS);
        context
    }

    #[cfg(feature = "admin")]
    pub(crate) fn exploration_session(&self) -> SessionContext {
        let mut state = self.session().state();
        state
            .config_mut()
            .options_mut()
            .execution
            .parquet
            .pushdown_filters = true;
        SessionContext::new_with_state(state)
    }

    pub(crate) fn graph_session(&self) -> SessionContext {
        let mut state = self.session().state();
        // The generated list comprehensions use SQL lambdas.
        state.config_mut().options_mut().sql_parser.dialect = datafusion::config::Dialect::DuckDB;
        if self.options.memory_bytes <= 32 * 1024 * 1024 && self.options.spill_bytes > 0 {
            let optimizer = &mut state.config_mut().options_mut().optimizer;
            optimizer.enable_distinct_aggregation_soft_limit = false;
        }
        // Several graph branches can hold sort reservations concurrently, even
        // on small inputs. Size each reservation against the shared query budget.
        let execution = &mut state.config_mut().options_mut().execution;
        // Concurrent wide aggregation and external merge each need space
        // for several input batches, even when their state is spillable.
        let workers = (self.options.memory_bytes / (16 * 1024 * 1024)).max(1);
        execution.target_partitions = execution.target_partitions.min(workers);
        let per_sort = self.options.memory_bytes / execution.target_partitions.saturating_mul(32);
        execution.sort_spill_reservation_bytes = execution
            .sort_spill_reservation_bytes
            .min(per_sort.max(64 * 1024));
        // DataFusion 55 can extract the same nested field twice across subquery aliases.
        let rules = state
            .optimizer()
            .rules
            .iter()
            .filter(|rule| rule.name() != "push_down_leaf_projections")
            .cloned()
            .collect();
        let context = SessionContext::new_with_state(
            datafusion::execution::SessionStateBuilder::new_from_existing(state)
                .with_optimizer_rules(rules)
                .build(),
        );
        super::query_functions::register_scoped(&context, self);
        allocation::register(&context, self, &["__graphlake_cypher_range"]);
        super::query_functions::cypher::reduce::register(&context, self);
        super::query_functions::cypher::collect::register(
            &context,
            Arc::clone(&self.runtime),
            self.options.memory_bytes,
            Arc::clone(&self.intermediate_spilled_bytes),
            self.options.cancellation.clone(),
            self.deadline,
        );
        context
    }

    pub(crate) fn record_spill(&self, plan: &dyn ExecutionPlan) {
        self.intermediate_spilled_bytes
            .fetch_add(spilled_bytes(plan), Ordering::Relaxed);
    }

    pub(crate) async fn run<T>(&self, future: impl Future<Output = Result<T>>) -> Result<T> {
        tokio::select! {
            biased;
            _ = self.options.cancellation.cancelled() => bail!("query cancelled"),
            _ = tokio::time::sleep_until(self.deadline) => bail!("query deadline exceeded"),
            result = future => result,
        }
    }

    pub(crate) async fn result(
        self: &Arc<Self>,
        graph: &str,
        context: &SessionContext,
        frame: DataFrame,
        prepared: Prepared,
        export: Option<super::export::Destination>,
    ) -> Result<QueryResult> {
        let plan = frame.create_physical_plan().await?;
        self.execute(graph, context, plan, prepared, export)
    }

    pub(crate) fn execute(
        self: &Arc<Self>,
        graph: &str,
        context: &SessionContext,
        plan: Arc<dyn ExecutionPlan>,
        prepared: Prepared,
        export: Option<super::export::Destination>,
    ) -> Result<QueryResult> {
        let physical_plan = displayable(plan.as_ref()).indent(false).to_string();
        let batches = execute_stream(Arc::clone(&plan), context.task_ctx())?;
        let batches = match export {
            Some(destination) => destination.write(batches, Arc::clone(self)),
            None => batches,
        };
        let schema = batches.schema();
        let lease = self
            .lease
            .lock()
            .expect("query resource lease")
            .take()
            .context("query already has a result stream")?;
        let resources = Arc::new(ResultResources {
            _lease: lease,
            _sources: prepared.reservations,
        });
        #[cfg(feature = "admin")]
        let weak_resources = Arc::downgrade(&resources);
        let scope = Arc::clone(self);
        let stream = async_stream::try_stream! {
            let mut batches = batches;
            let completion = loop {
                match scope.run(async { Ok(batches.try_next().await?) }).await {
                    Ok(Some(batch)) => {
                        if batch.num_rows() != 0 {
                            let _ = scope.first_batch_ms.compare_exchange(
                                u64::MAX, scope.start.elapsed().as_millis() as u64,
                                Ordering::Relaxed, Ordering::Relaxed,
                            );
                        }
                        scope.output_rows.fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
                        yield batch;
                    }
                    Ok(None) => break Ok(()),
                    Err(error) => break Err(DataFusionError::External(error.into())),
                }
            };
            drop(batches);
            scope.spilled_bytes.store(spilled_bytes(plan.as_ref()), Ordering::Relaxed);
            *scope.finished_ms.lock().expect("elapsed time") = Some(scope.start.elapsed().as_millis() as u64);
            drop(plan);
            drop(resources);
            completion?;
        };
        Ok(QueryResult {
            graph: graph.to_owned(),
            versions: prepared.versions,
            physical_plan,
            #[cfg(feature = "admin")]
            graph_output: Default::default(),
            #[cfg(feature = "admin")]
            graph_context: None,
            #[cfg(feature = "admin")]
            graph_csr: None,
            #[cfg(feature = "admin")]
            resources: weak_resources,
            #[cfg(feature = "admin")]
            graph_resources: None,
            batches: self.expiring_stream(Box::pin(RecordBatchStreamAdapter::new(schema, stream))),
            scope: Arc::clone(self),
        })
    }

    fn expiring_stream(
        self: &Arc<Self>,
        stream: SendableRecordBatchStream,
    ) -> SendableRecordBatchStream {
        let schema = stream.schema();
        let state = Arc::new(Mutex::new(Output {
            stream: Some(stream),
            error: None,
            polling: false,
            waker: None,
        }));
        let weak = Arc::downgrade(&state);
        let scope = Arc::downgrade(self);
        let deadline = self.deadline;
        let cancelled = self.options.cancellation.clone();
        let done = CancellationToken::new();
        let guard = done.clone().drop_guard();
        let stop = done.clone();
        tokio::spawn(async move {
            let error = tokio::select! {
                biased;
                _ = stop.cancelled() => return,
                _ = cancelled.cancelled() => "query cancelled",
                _ = tokio::time::sleep_until(deadline) => "query deadline exceeded",
            };
            let Some(state) = weak.upgrade() else { return };
            let (stream, waker) = {
                let mut state = state.lock().expect("query output");
                let stream = state.stream.take();
                if stream.is_none() && !state.polling {
                    return;
                }
                state.error = Some(error);
                (stream, state.waker.take())
            };
            // The consumer may have stopped polling under wire backpressure.
            // Dropping the stream releases its leases and reservations here.
            drop(stream);
            if let Some(scope) = scope.upgrade() {
                scope
                    .finished_ms
                    .lock()
                    .expect("elapsed time")
                    .get_or_insert_with(|| scope.start.elapsed().as_millis() as u64);
            }
            if let Some(waker) = waker {
                waker.wake();
            }
        });
        Box::pin(RecordBatchStreamAdapter::new(
            schema,
            poll_fn(move |cx| {
                let _guard = &guard;
                let mut stream = {
                    let mut state = state.lock().expect("query output");
                    if let Some(error) = state.error.take() {
                        return Poll::Ready(Some(Err(DataFusionError::Execution(error.into()))));
                    }
                    let Some(stream) = state.stream.take() else {
                        return Poll::Ready(None);
                    };
                    state.polling = true;
                    stream
                };
                // DataFusion may do substantial work in poll_next. The deadline
                // task can record cancellation without blocking on that work.
                let polled = stream.as_mut().poll_next(cx);
                let mut state = state.lock().expect("query output");
                state.polling = false;
                if let Some(error) = state.error.take() {
                    return Poll::Ready(Some(Err(DataFusionError::Execution(error.into()))));
                }
                match &polled {
                    Poll::Ready(None | Some(Err(_))) => {
                        state.waker.take();
                        done.cancel();
                    }
                    Poll::Pending => {
                        state.waker = Some(cx.waker().clone());
                        state.stream = Some(stream);
                    }
                    Poll::Ready(Some(Ok(_))) => {
                        state.waker.take();
                        state.stream = Some(stream);
                    }
                }
                polled
            }),
        ))
    }
}

struct Output {
    stream: Option<SendableRecordBatchStream>,
    error: Option<&'static str>,
    polling: bool,
    waker: Option<Waker>,
}

pub(crate) fn spilled_bytes(plan: &dyn ExecutionPlan) -> u64 {
    let own = plan
        .metrics()
        .and_then(|metrics| metrics.spilled_bytes())
        .unwrap_or_default() as u64;
    plan.children().iter().fold(own, |total, child| {
        total.saturating_add(spilled_bytes(child.as_ref()))
    })
}
