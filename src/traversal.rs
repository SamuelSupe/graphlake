mod algorithms;
mod cache;
mod expand;
mod heterogeneous;
mod projected;
mod remote;
mod topology;
mod walk;

use std::{
    collections::BTreeSet,
    fmt,
    mem::size_of,
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result, ensure};
use datafusion::{
    arrow::{
        array::RecordBatch,
        datatypes::{DataType, Field, Schema, SchemaRef},
    },
    common::{DataFusionError, ScalarValue, tree_node::TreeNodeRecursion},
    execution::{
        SendableRecordBatchStream, TaskContext,
        memory_pool::{MemoryConsumer, MemoryReservation},
    },
    physical_expr::{EquivalenceProperties, PhysicalExpr},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, execute_stream,
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
    },
};
use futures::TryStreamExt;

use crate::{
    Catalog, Engine, QueryOptions, QueryResult,
    catalog::ident,
    engine::{Prepared, QueryScope},
};
use datafusion::prelude::SessionContext;

pub use algorithms::{
    ALGORITHM_CAPABILITIES, AlgorithmCapability, GraphAlgorithm, GraphAlgorithmMetrics,
};
pub(crate) use cache::Cache;
pub use cache::RefreshReport;
pub(crate) use expand::{Expansion, MixedExpansion, StartSeed, text_key};
pub use projected::{
    CommunityOptions, GraphProjection, Node2VecOptions, ProjectionAlgorithm, Route, RouteStrategy,
    VertexKey,
};
pub(crate) use projected::{projection_table, route_table};
use remote::Remote;
use topology::{Neighbor, Topology, path_item, row_converter};
#[cfg(feature = "admin")]
pub(crate) use topology::{encode_key as graph_encode_key, row_converter as graph_row_converter};
use walk::{Hit, Step, Target, Walk};

pub(crate) const HOPS: &str = "__graphlake_hops";
pub(crate) const PATH: &str = "__graphlake_path";
pub(crate) const PATH_NODES: &str = "__graphlake_path_nodes";
const COOPERATIVE_INTERVAL: usize = 2048;

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    Outgoing,
    Incoming,
    Undirected,
}

#[derive(Clone, Debug)]
pub enum TraversalKind {
    /// Return each reachable vertex once at its minimum distance, including the start.
    Reachable,
    /// Return one shortest path to the target, or an empty stream if it is unreachable.
    Shortest { target: Vec<ScalarValue> },
    /// Enumerate edge-unique paths. Vertices may repeat.
    Paths { min_hops: usize },
}

#[derive(Clone, Debug)]
pub struct Traversal {
    pub edge: String,
    pub direction: Direction,
    /// Business key in catalog order. UTF-8 inputs can be parsed as the declared column type.
    pub start: Vec<ScalarValue>,
    pub max_hops: usize,
    pub kind: TraversalKind,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TraversalStrategy {
    #[default]
    Auto,
    Scan,
    Batch,
    Cached,
}

impl Engine {
    /// Traverse one directed edge type whose endpoints belong to the same node type.
    pub async fn traverse(&self, request: Traversal, options: QueryOptions) -> Result<QueryResult> {
        self.traverse_with_strategy(request, options, TraversalStrategy::Auto)
            .await
    }

    pub async fn traverse_with_strategy(
        &self,
        request: Traversal,
        options: QueryOptions,
        strategy: TraversalStrategy,
    ) -> Result<QueryResult> {
        ensure!(request.max_hops <= 256, "max_hops must be at most 256");
        if let TraversalKind::Paths { min_hops } = &request.kind {
            ensure!(
                *min_hops <= request.max_hops,
                "min_hops must be at most max_hops"
            );
        }
        let edge = self
            .catalog
            .edges
            .get(&request.edge)
            .with_context(|| format!("unknown edge type {}", request.edge))?;
        ensure!(
            edge.from.node == edge.to.node,
            "traversal requires a single endpoint node type"
        );
        let scope = self.scope(options).await?;
        scope
            .run(async {
                let context = scope.session();
                let (input, prepared) = self.graph_input(&request.edge, request.direction, strategy, &scope, &context, false).await?;
                let remote = if matches!(input, Input::Live(_)) && matches!(strategy, TraversalStrategy::Auto | TraversalStrategy::Batch) {
                    Remote::open(&self.catalog, &prepared, &scope, &request.edge, request.direction, strategy == TraversalStrategy::Batch).await?
                } else { None };
                ensure!(strategy != TraversalStrategy::Batch || remote.is_some(),
                    "batch traversal requires indexed PostgreSQL node and edge lookups with stable edge keys");
                let mut plan = TraversalExec::new(input, Mode::Single(request), Arc::clone(&scope.visits_by_depth));
                plan.remote = remote;
                scope.execute(&self.catalog.graph, &context, Arc::new(plan), prepared, None)
            })
            .await
    }

    async fn graph_input(
        &self,
        edge: &str,
        direction: Direction,
        strategy: TraversalStrategy,
        scope: &Arc<QueryScope>,
        context: &SessionContext,
        full_scan: bool,
    ) -> Result<(Input, Prepared)> {
        if matches!(
            strategy,
            TraversalStrategy::Auto | TraversalStrategy::Cached
        ) && let Some(cache) = &self.cache
        {
            let mapping = &self.catalog.edges[edge];
            let time_sensitive = scope
                .options
                .graph
                .snapshot_time
                .as_deref()
                .is_none_or(|time| time.eq_ignore_ascii_case("current"))
                && (mapping.scd2.is_some()
                    || self.catalog.nodes[&mapping.from.node].scd2.is_some()
                    || self.catalog.nodes[&mapping.to.node].scd2.is_some());
            let loaded = if strategy == TraversalStrategy::Auto && time_sensitive {
                Ok(None)
            } else {
                cache.load(edge, direction, Arc::clone(scope)).await
            };
            match loaded {
                Ok(Some(generation))
                    if strategy == TraversalStrategy::Cached
                        || (!time_sensitive
                            && self
                                .pinned_versions_match(scope, &generation.graph.versions)
                                .await
                                .unwrap_or(false)) =>
                {
                    *scope.csr_generation.lock().expect("CSR metrics") =
                        Some(generation.graph.generation.clone());
                    let prepared = Prepared {
                        aggregate: None,
                        versions: generation.graph.versions.clone(),
                        reservations: Vec::new(),
                        postgres: Default::default(),
                    };
                    return Ok((Input::Cached(generation), prepared));
                }
                Err(error) if strategy == TraversalStrategy::Cached => return Err(error),
                _ => scope.check()?,
            }
        }
        ensure!(
            strategy != TraversalStrategy::Cached,
            "no published CSR generation"
        );
        let mapping = &self.catalog.edges[edge];
        let labels = BTreeSet::from([edge.to_owned(), mapping.from.node.clone()]);
        let prepared = if full_scan {
            self.prepare_full_topology(scope, context, labels, None)
                .await?
        } else {
            self.prepare(scope, context, labels).await?
        };
        let inputs = scan_plans(context, &self.catalog, edge, direction, &[]).await?;
        Ok((Input::Live(inputs), prepared))
    }
}

async fn scan_plans(
    context: &SessionContext,
    catalog: &Catalog,
    edge_name: &str,
    direction: Direction,
    filters: &[String],
) -> Result<Inputs> {
    let edge = &catalog.edges[edge_name];
    if edge.from.node != edge.to.node {
        return heterogeneous::scan(context, catalog, edge_name, direction, filters).await;
    }
    let node = &catalog.nodes[&edge.from.node];
    let nodes = node_plan(context, &edge.from.node, &node.key, &[]).await?;
    let (from, to) = match direction {
        Direction::Outgoing | Direction::Undirected => (&edge.from.columns, &edge.to.columns),
        Direction::Incoming => (&edge.to.columns, &edge.from.columns),
    };
    let select = from
        .iter()
        .chain(to)
        .chain(&edge.identity())
        .enumerate()
        .map(|(i, c)| format!("{} AS c{i}", ident(c)))
        .collect::<Vec<_>>()
        .join(", ");
    let edges = context
        .sql(&format!(
            "SELECT {select} FROM {} AS src{}",
            ident(edge_name),
            where_clause(filters)
        ))
        .await?
        .create_physical_plan()
        .await?;
    let identity = edge
        .identity()
        .into_iter()
        .zip(edges.schema().fields().iter().skip(2 * node.key.len()))
        .map(|(name, field)| Arc::new(Field::new(name, field.data_type().clone(), false)))
        .collect::<Vec<_>>();
    Ok(Inputs {
        nodes,
        edges,
        identity: Arc::new(Schema::new(identity)),
        undirected: matches!(direction, Direction::Undirected),
        starts: None,
    })
}

async fn node_plan(
    context: &SessionContext,
    node: &str,
    key: &[String],
    filters: &[String],
) -> Result<Arc<dyn ExecutionPlan>> {
    let selected = key.iter().map(|c| ident(c)).collect::<Vec<_>>().join(", ");
    Ok(context
        .sql(&format!(
            "SELECT {selected} FROM {} AS src{}",
            ident(node),
            where_clause(filters)
        ))
        .await?
        .create_physical_plan()
        .await?)
}

fn where_clause(terms: &[String]) -> String {
    if terms.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", terms.join(" AND "))
    }
}

#[derive(Debug, Clone)]
struct Inputs {
    nodes: Arc<dyn ExecutionPlan>,
    edges: Arc<dyn ExecutionPlan>,
    identity: SchemaRef,
    undirected: bool,
    starts: Option<Arc<dyn ExecutionPlan>>,
}

impl Inputs {
    async fn load(
        &self,
        context: Arc<TaskContext>,
        budget: &MemoryReservation,
    ) -> Result<Topology> {
        let nodes = execute_stream(Arc::clone(&self.nodes), Arc::clone(&context))?;
        let edges = execute_stream(Arc::clone(&self.edges), context)?;
        Topology::load(
            nodes,
            edges,
            Arc::clone(&self.identity),
            self.undirected,
            budget,
        )
        .await
    }
}

#[derive(Debug, Clone)]
enum Input {
    Live(Inputs),
    Cached(Arc<cache::Loaded>),
}

#[derive(Debug, Clone)]
enum Mode {
    Single(Traversal),
    Expand(Expansion),
    Algorithm {
        edge: String,
        algorithm: GraphAlgorithm,
        metrics: Arc<Mutex<Option<GraphAlgorithmMetrics>>>,
    },
}

#[derive(Debug)]
struct TraversalExec {
    input: Input,
    mode: Mode,
    properties: Arc<PlanProperties>,
    visits: Arc<Mutex<Vec<u64>>>,
    remote: Option<Remote>,
    shared_topology: Option<Arc<tokio::sync::OnceCell<(Arc<Topology>, MemoryReservation)>>>,
}

impl TraversalExec {
    fn new(input: Input, mode: Mode, visits: Arc<Mutex<Vec<u64>>>) -> Self {
        let (node_schema, identity) = match &input {
            Input::Live(inputs) => (inputs.nodes.schema(), Arc::clone(&inputs.identity)),
            Input::Cached(generation) => (
                generation.graph.topology.keys.schema(),
                generation.graph.topology.identities.schema(),
            ),
        };
        let mut fields = match &mode {
            Mode::Single(_) | Mode::Algorithm { .. } => node_schema.fields().to_vec(),
            Mode::Expand(_) => ["from_", "to_"]
                .into_iter()
                .flat_map(|prefix| {
                    node_schema
                        .fields()
                        .iter()
                        .enumerate()
                        .map(move |(i, field)| {
                            Arc::new(Field::new(
                                format!("{prefix}{i}"),
                                field.data_type().clone(),
                                field.is_nullable(),
                            ))
                        })
                })
                .collect(),
        };
        if let Mode::Algorithm { algorithm, .. } = &mode {
            fields.push(Arc::new(algorithm.field(&node_schema)));
        } else {
            fields.push(Arc::new(Field::new(HOPS, DataType::UInt32, false)));
            if !matches!(
                &mode,
                Mode::Single(Traversal {
                    kind: TraversalKind::Reachable,
                    ..
                })
            ) {
                fields.push(Arc::new(Field::new(
                    PATH,
                    DataType::List(path_item(&identity)),
                    false,
                )));
            }
        }
        if matches!(&mode, Mode::Expand(expansion) if expansion.track_vertices) {
            fields.push(Arc::new(Field::new(
                PATH_NODES,
                DataType::List(path_item(&node_schema)),
                false,
            )));
        }
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::new(Schema::new(fields))),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            // Start selection can depend on the replay seed. The node and
            // edge inputs are bound to the same snapshot for the whole query.
            shared_topology: matches!((&input, &mode), (Input::Live(_), Mode::Expand(_)))
                .then(|| Arc::new(tokio::sync::OnceCell::new())),
            input,
            mode,
            properties,
            visits,
            remote: None,
        }
    }
}

impl DisplayAs for TraversalExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.mode {
            Mode::Algorithm {
                edge, algorithm, ..
            } => write!(
                f,
                "GraphAlgorithmExec: edge={edge}, algorithm={algorithm:?}, access={}",
                if matches!(self.input, Input::Cached(_)) {
                    "published CSR"
                } else {
                    "scan"
                }
            ),
            Mode::Expand(expansion) => write!(
                f,
                "GraphExpandExec: edge={}, min_hops={}, max_hops={}, direction={:?}",
                if expansion.mixed.is_some() {
                    "*"
                } else {
                    &expansion.edge
                },
                expansion.min,
                expansion.max,
                expansion.direction
            ),
            Mode::Single(request) => {
                let mode = match request.kind {
                    TraversalKind::Reachable => "reachability",
                    TraversalKind::Shortest { .. } => "shortest",
                    TraversalKind::Paths { .. } => "edge-unique paths",
                };
                let access = match &self.input {
                    Input::Cached(_) => "published CSR",
                    Input::Live(_) if self.remote.is_some() => "PostgreSQL frontier",
                    Input::Live(_) => "scan",
                };
                write!(
                    f,
                    "TraversalExec: {mode}, edge={}, direction={:?}, max_hops={}, access={access}",
                    request.edge, request.direction, request.max_hops
                )
            }
        }
    }
}

impl ExecutionPlan for TraversalExec {
    fn name(&self) -> &str {
        match &self.mode {
            Mode::Expand(_) => "GraphExpandExec",
            Mode::Single(_) => "TraversalExec",
            Mode::Algorithm { .. } => "GraphAlgorithmExec",
        }
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        match &self.input {
            Input::Live(inputs) => [&inputs.nodes, &inputs.edges]
                .into_iter()
                .chain(inputs.starts.as_ref())
                .collect(),
            Input::Cached(_) => Vec::new(),
        }
    }

    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> datafusion::common::Result<TreeNodeRecursion>,
    ) -> datafusion::common::Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let input = match &self.input {
            Input::Live(inputs) if children.len() == 2 + usize::from(inputs.starts.is_some()) => {
                let mut children = children.into_iter();
                Input::Live(Inputs {
                    nodes: children.next().unwrap(),
                    edges: children.next().unwrap(),
                    identity: Arc::clone(&inputs.identity),
                    undirected: inputs.undirected,
                    starts: children.next(),
                })
            }
            Input::Cached(generation) if children.is_empty() => {
                Input::Cached(Arc::clone(generation))
            }
            _ => {
                return Err(DataFusionError::Internal(
                    "traversal input count mismatch".into(),
                ));
            }
        };
        let mut plan = Self::new(input, self.mode.clone(), Arc::clone(&self.visits));
        plan.remote = self.remote.clone();
        plan.shared_topology = self.shared_topology.clone();
        Ok(Arc::new(plan))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(
                "traversal has one partition".into(),
            ));
        }
        let schema = self.schema();
        let output_schema = Arc::clone(&schema);
        let mode = self.mode.clone();
        let input = self.input.clone();
        let remote = self.remote.clone();
        let shared_topology = self
            .shared_topology
            .as_ref()
            .filter(|_| crate::cypher::apply::replay::has_seed(&context))
            .cloned();
        let visits = Arc::clone(&self.visits);
        let stream = async_stream::try_stream! {
            let reservation = MemoryConsumer::new("GraphLake traversal").register(&context.runtime_env().memory_pool);
            let batch_size = context.session_config().batch_size();
            let mut bound_start = None;
            let (topology, starts) = match &input {
                Input::Cached(generation) => (Arc::clone(&generation.graph.topology), None),
                Input::Live(inputs) => {
                    let topology = match (&mode, remote) {
                        (Mode::Single(request), Some(remote)) => remote.load(request, inputs, Arc::clone(&context), &reservation)
                            .await.map_err(external)?,
                        (Mode::Expand(expansion), Some(remote)) => {
                            let starts = inputs.starts.as_ref().expect("anchored expansion");
                            let Some(start) = single_start(execute_stream(Arc::clone(starts), Arc::clone(&context))?)
                                .await.map_err(external)? else { return; };
                            let request = Traversal {
                                edge: expansion.edge.clone(), start: start.clone(), max_hops: expansion.max,
                                direction: expansion.direction,
                                kind: TraversalKind::Paths { min_hops: expansion.min },
                            };
                            bound_start = Some(start);
                            remote.load(&request, inputs, Arc::clone(&context), &reservation).await.map_err(external)?
                        }
                        _ => None,
                    };
                    let topology = match topology {
                        Some(graph) => Arc::new(graph),
                        None => if let Some(shared) = &shared_topology {
                            shared.get_or_try_init(|| async {
                                let budget = reservation.new_empty();
                                let topology = inputs.load(context.clone(), &budget).await?;
                                Ok::<_, anyhow::Error>((Arc::new(topology), budget))
                            }).await.map_err(external)?.0.clone()
                        } else {
                            Arc::new(inputs.load(context.clone(), &reservation).await.map_err(external)?)
                        },
                    };
                    (topology, inputs.starts.clone())
                }
            };
            let expand = matches!(mode, Mode::Expand(_));
            let track_vertices = matches!(&mode, Mode::Expand(expansion) if expansion.track_vertices);
            let shortest = match &mode {
                Mode::Expand(expansion) => expansion.shortest.map(|kind| (kind, expansion.min)),
                _ => None,
            };
            let (starts, target, kind, max_hops): (Box<dyn Iterator<Item = usize> + Send>, _, _, _) = match mode {
                Mode::Single(request) => {
                    let start = topology.node_id(&request.start).map_err(external)?
                        .ok_or_else(|| DataFusionError::Execution("start vertex does not exist".into()))?;
                    let target = match &request.kind {
                        TraversalKind::Shortest { target } => Target::Vertex(topology.node_id(target).map_err(external)?),
                        _ => Target::All,
                    };
                    (Box::new(std::iter::once(start)), target, request.kind, request.max_hops)
                }
                Mode::Algorithm { algorithm, metrics, .. } => {
                    let (scores, summary) = algorithm.run(&topology, &reservation).await.map_err(external)?;
                    *metrics.lock().expect("algorithm metrics") = Some(summary);
                    for offset in (0..topology.keys.num_rows()).step_by(batch_size) {
                        let len = batch_size.min(topology.keys.num_rows() - offset);
                        yield scores.batch(&topology, Arc::clone(&output_schema), offset, len)?;
                        tokio::task::yield_now().await;
                    }
                    return;
                }
                Mode::Expand(expansion) => {
                    let starts = if let Some(seed) = expansion.imported_keys(&context, expansion.mixed.is_some()).map_err(external)? {
                        let tag = expansion.start_seed.as_ref().and_then(|start| start.tag);
                        Box::new(imported_starts(&topology, &seed, tag, &reservation)
                            .map_err(external)?.into_iter()) as Box<dyn Iterator<Item = usize> + Send>
                    } else { match (bound_start, starts) {
                        (Some(key), _) => Box::new(topology.node_id(&key).map_err(external)?.into_iter())
                            as Box<dyn Iterator<Item = usize> + Send>,
                        (None, Some(plan)) => Box::new(select_starts(&topology, execute_stream(plan, context)?, &reservation)
                            .await.map_err(external)?.into_iter()) as Box<dyn Iterator<Item = usize> + Send>,
                        (None, None) => Box::new(0..topology.keys.num_rows()),
                    }};
                    (starts, Target::All, TraversalKind::Paths { min_hops: expansion.min }, expansion.max)
                }
            };
            let mut batches = walk_batches(&topology, starts, target, kind, max_hops,
                WalkOutput { batch_size, schema: output_schema, expand, visits, shortest, track_vertices }, &reservation);
            while let Some(batch) = batches.try_next().await? {
                yield batch;
            }
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

fn imported_starts(
    topology: &Topology,
    seed: &RecordBatch,
    tag: Option<(bool, usize)>,
    reservation: &MemoryReservation,
) -> Result<Vec<usize>> {
    use datafusion::arrow::array::{ArrayRef, Int32Array, new_null_array};
    let schema = topology.keys.schema();
    let keys = if let Some((second, offset)) = tag {
        let tag = Arc::new(Int32Array::from_value(i32::from(second), seed.num_rows())) as ArrayRef;
        let mut columns = vec![tag];
        for (index, field) in schema.fields().iter().enumerate().skip(1) {
            let key = index
                .checked_sub(offset)
                .filter(|&index| index < seed.num_columns());
            columns.push(match key {
                Some(index) => seed.column(index).clone(),
                None => new_null_array(field.data_type(), seed.num_rows()),
            });
        }
        columns
    } else {
        seed.columns().to_vec()
    };
    reservation.try_grow(seed.num_rows().saturating_mul(2 * size_of::<usize>()))?;
    let converter = row_converter(&schema)?;
    let keys = converter.convert_columns(&keys)?;
    let mut starts = keys
        .iter()
        .filter_map(|key| topology.index.get(key.data()).copied())
        .collect::<Vec<_>>();
    // Preserve traversal order and visit each imported vertex once.
    starts.sort_unstable();
    starts.dedup();
    Ok(starts)
}

async fn single_start(mut stream: SendableRecordBatchStream) -> Result<Option<Vec<ScalarValue>>> {
    let mut start = None;
    while let Some(batch) = stream.try_next().await? {
        if batch.num_rows() > 0 {
            ensure!(
                start.is_none() && batch.num_rows() == 1,
                "duplicate bound node business key"
            );
            start = Some(
                batch
                    .columns()
                    .iter()
                    .map(|column| ScalarValue::try_from_array(column.as_ref(), 0))
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }
    }
    Ok(start)
}

async fn select_starts(
    topology: &Topology,
    mut batches: SendableRecordBatchStream,
    reservation: &MemoryReservation,
) -> Result<Vec<usize>> {
    let converter = row_converter(&topology.keys.schema())?;
    let mut ids = Vec::new();
    while let Some(batch) = batches.try_next().await? {
        reservation.try_grow(batch.num_rows().saturating_mul(2 * size_of::<usize>()))?;
        for row in converter.convert_columns(batch.columns())?.iter() {
            if let Some(&id) = topology.index.get(row.data()) {
                ids.push(id);
            }
        }
        tokio::task::yield_now().await;
    }
    Ok(ids)
}

struct WalkOutput {
    batch_size: usize,
    schema: SchemaRef,
    expand: bool,
    visits: Arc<Mutex<Vec<u64>>>,
    shortest: Option<(crate::cypher::paths::Shortest, usize)>,
    track_vertices: bool,
}

fn walk_batches<'a>(
    topology: &'a Topology,
    starts: Box<dyn Iterator<Item = usize> + Send>,
    target: Target,
    kind: TraversalKind,
    max_hops: usize,
    output: WalkOutput,
    reservation: &'a MemoryReservation,
) -> futures::stream::BoxStream<'a, datafusion::common::Result<datafusion::arrow::array::RecordBatch>>
{
    Box::pin(async_stream::try_stream! {
        let max_hops = if output.shortest.is_some() {
            max_hops.min(topology.keys.num_rows().saturating_sub(1))
        } else { max_hops };
        let path = !matches!(kind, TraversalKind::Reachable);
        let entry_bytes = size_of::<(usize, Hit)>()
            + if output.shortest.is_none() && path { (max_hops + 1) * 4 * size_of::<usize>() } else { 0 };
        if let Some((kind, _)) = output.shortest {
            let per_vertex = 128;
            reservation.try_grow(topology.keys.num_rows().saturating_mul(per_vertex))?;
            if matches!(kind, crate::cypher::paths::Shortest::All) {
                reservation.try_grow(topology.neighbors.len().saturating_mul(4 * size_of::<Neighbor>()))?;
            }
        }
        reservation.try_grow(output.batch_size.saturating_mul(entry_bytes))?;
        let mut depth_counts = vec![0_u64; max_hops + 1];
        let mut hits = Vec::new();
        let mut work = 0;
        let mut path_bytes = 0;
        let mut state: Option<Walk> = None;
        for start in starts {
            let walk = match &mut state {
                Some(walk) => {
                    walk.restart(topology, start, target);
                    walk
                }
                slot => slot.insert(Walk::new(topology, start, target, kind.clone(), output.shortest, output.track_vertices)),
            };
            loop {
                match walk.step(topology, max_hops) {
                    Step::Done => break,
                    Step::Visit { depth, hit } => {
                        depth_counts[depth] += 1;
                        if let Some(hit) = hit {
                            if output.shortest.is_some() {
                                let bytes = (hit.edges.capacity() + hit.vertices.capacity()) * 2 * size_of::<usize>();
                                reservation.try_grow(bytes)?;
                                path_bytes += bytes;
                            }
                            hits.push((start, hit));
                            if hits.len() == output.batch_size {
                                yield topology.batch(&hits, Arc::clone(&output.schema), output.expand)?;
                                hits.clear();
                                reservation.shrink(path_bytes);
                                path_bytes = 0;
                            }
                        }
                    }
                    Step::Continue => {}
                }
                work += 1;
                if work == COOPERATIVE_INTERVAL {
                    work = 0;
                    tokio::task::yield_now().await;
                }
            }
        }
        if !hits.is_empty() {
            yield topology.batch(&hits, output.schema, output.expand)?;
        }
        let mut visits = output.visits.lock().expect("depth metrics");
        if visits.len() < depth_counts.len() { visits.resize(depth_counts.len(), 0); }
        for (total, count) in visits.iter_mut().zip(depth_counts) { *total = total.saturating_add(count); }
    })
}

fn external(error: anyhow::Error) -> DataFusionError {
    DataFusionError::External(error.into())
}
