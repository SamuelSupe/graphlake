mod embedding;
mod graph;
mod route_strategies;
mod routes;

use std::{collections::BTreeMap, mem::size_of, sync::Arc};

use anyhow::{Result, ensure};
use bigdecimal::ToPrimitive;
use datafusion::{
    arrow::{
        array::{
            ArrayRef, Float32Array, Float64Array, ListArray, ListBuilder, RecordBatch, StringArray,
            StringBuilder, StructArray, UInt64Array,
        },
        buffer::{OffsetBuffer, ScalarBuffer},
        datatypes::{DataType, Field, Schema},
    },
    catalog::TableProvider,
    execution::memory_pool::{MemoryConsumer, MemoryReservation},
    prelude::SessionContext,
};
use leiden_rs::{GraphDataBuilder, Leiden, LeidenConfig, QualityType};

use super::{
    COOPERATIVE_INTERVAL, Direction, GraphAlgorithm, GraphAlgorithmMetrics, algorithms::root,
};
use crate::{
    Engine, QueryOptions, QueryResult, ScalarValue, engine::QueryScope, source::snapshot::Buffer,
};
use graph::{Graph, GraphRead, Identity, Labels};

/// The union of selected edge types, their endpoint labels, and any explicit isolated labels.
#[derive(Clone, Debug)]
pub struct GraphProjection {
    pub nodes: Vec<String>,
    pub edges: Vec<String>,
    /// Numeric weight property shared by the selected edge types. Omit for unit weights.
    pub weight: Option<String>,
    pub direction: Direction,
}

impl Default for GraphProjection {
    fn default() -> Self {
        Self {
            nodes: Vec::new(),
            edges: Vec::new(),
            weight: None,
            direction: Direction::Outgoing,
        }
    }
}

#[derive(Clone, Debug)]
pub struct VertexKey {
    pub label: String,
    pub key: Vec<ScalarValue>,
}

#[derive(Clone, Debug)]
pub struct Route {
    pub start: VertexKey,
    pub target: VertexKey,
    /// Maximum number of loopless routes in nondecreasing cost order.
    pub k: usize,
}

#[derive(Clone, Copy, Debug, Default)]
pub enum RouteStrategy {
    #[default]
    Auto,
    BellmanFord,
    DeltaStepping,
    Johnson,
    DagLongest,
}

impl RouteStrategy {
    fn signed(self) -> bool {
        matches!(self, Self::BellmanFord | Self::Johnson | Self::DagLongest)
    }
}

impl std::str::FromStr for RouteStrategy {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        Ok(match value.replace('-', "_").as_str() {
            "auto" | "dijkstra" | "yen" => Self::Auto,
            "bellman_ford" => Self::BellmanFord,
            "delta_stepping" => Self::DeltaStepping,
            "johnson" => Self::Johnson,
            "dag_longest" => Self::DagLongest,
            _ => anyhow::bail!("unknown route strategy {value}"),
        })
    }
}

#[derive(Clone, Copy, Debug)]
pub enum ProjectionAlgorithm {
    PageRank {
        damping: f64,
        tolerance: f64,
        max_iterations: usize,
    },
    WeaklyConnectedComponents,
    Louvain(CommunityOptions),
    Leiden(CommunityOptions),
    LabelPropagation {
        max_iterations: usize,
    },
    Node2Vec(Node2VecOptions),
}

#[derive(Clone, Copy, Debug, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CommunityOptions {
    pub resolution: f64,
    pub max_iterations: usize,
    pub seed: u64,
}

impl Default for CommunityOptions {
    fn default() -> Self {
        Self {
            resolution: 1.0,
            max_iterations: 20,
            seed: 42,
        }
    }
}

#[derive(Clone, Copy, Debug, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Node2VecOptions {
    pub dimensions: usize,
    pub walk_length: usize,
    pub walks_per_node: usize,
    pub window: usize,
    pub epochs: usize,
    pub negative_samples: usize,
    pub return_bias: f64,
    pub explore_bias: f64,
    pub learning_rate: f64,
    pub seed: u64,
}

impl Default for Node2VecOptions {
    fn default() -> Self {
        Self {
            dimensions: 32,
            walk_length: 20,
            walks_per_node: 4,
            window: 4,
            epochs: 2,
            negative_samples: 5,
            return_bias: 1.0,
            explore_bias: 1.0,
            learning_rate: 0.025,
            seed: 42,
        }
    }
}

impl Engine {
    /// Whether a label can use the single-node-type traversal and cache APIs.
    pub fn has_homogeneous_edge(&self, edge: &str) -> bool {
        self.catalog
            .edges
            .get(edge)
            .is_some_and(|edge| edge.from.node == edge.to.node)
    }

    /// Analyze a graph with multiple node and edge types under one query scope.
    pub async fn analyze_projection(
        &self,
        projection: GraphProjection,
        algorithm: ProjectionAlgorithm,
        options: QueryOptions,
    ) -> Result<QueryResult> {
        algorithm.validate()?;
        let labels = Labels::new(&self.catalog, &projection)?;
        let scope = self.scope(options).await?;
        scope
            .run(async {
                let context = scope.session();
                let mut prepared = self
                    .prepare_full_topology(
                        &scope,
                        &context,
                        labels.all(),
                        projection.weight.as_deref(),
                    )
                    .await?;
                let (table, output) = projection_table(
                    &self.catalog,
                    &projection,
                    algorithm,
                    &context,
                    &scope,
                    false,
                )
                .await?;
                prepared.reservations.push(output);
                let frame = context.read_table(table)?;
                scope
                    .result(&self.catalog.graph, &context, frame, prepared, None)
                    .await
            })
            .await
    }

    /// Find up to k loopless shortest routes across a projection, using Dijkstra and Yen.
    pub async fn route(
        &self,
        projection: GraphProjection,
        request: Route,
        options: QueryOptions,
    ) -> Result<QueryResult> {
        self.route_with_strategy(projection, request, RouteStrategy::Auto, options)
            .await
    }

    /// Find a path using a selected strategy. Alternative strategies return at most one path.
    pub async fn route_with_strategy(
        &self,
        projection: GraphProjection,
        request: Route,
        strategy: RouteStrategy,
        options: QueryOptions,
    ) -> Result<QueryResult> {
        validate_route_count(request.k, strategy)?;
        let labels = Labels::new(&self.catalog, &projection)?;
        let scope = self.scope(options).await?;
        scope
            .run(async {
                let context = scope.session();
                let mut prepared = self
                    .prepare_full_topology(
                        &scope,
                        &context,
                        labels.all(),
                        projection.weight.as_deref(),
                    )
                    .await?;
                let (table, output) = route_table(
                    &self.catalog,
                    &projection,
                    &request,
                    strategy,
                    &context,
                    &scope,
                    false,
                )
                .await?;
                prepared.reservations.push(output);
                let frame = context.read_table(table)?;
                scope
                    .result(&self.catalog.graph, &context, frame, prepared, None)
                    .await
            })
            .await
    }
}

pub(crate) async fn projection_table(
    catalog: &crate::Catalog,
    projection: &GraphProjection,
    algorithm: ProjectionAlgorithm,
    context: &SessionContext,
    scope: &Arc<QueryScope>,
    explain_only: bool,
) -> Result<(Arc<dyn TableProvider>, MemoryReservation)> {
    algorithm.validate()?;
    let labels = Labels::new(catalog, projection)?;
    let schema = Arc::new(Schema::new(vec![
        identity_field("vertex"),
        algorithm.field(),
    ]));
    if explain_only {
        return Buffer::new(&scope.runtime.memory_pool, None).finish_schema(schema);
    }
    let batch_size = scope.options.batch_size;
    let graph_budget = Arc::new(
        MemoryConsumer::new("GraphLake projected topology").register(&scope.runtime.memory_pool),
    );
    let mut graph = Graph::read(
        catalog,
        projection,
        &labels,
        context,
        scope,
        &graph_budget,
        match algorithm {
            ProjectionAlgorithm::PageRank { .. }
            | ProjectionAlgorithm::Node2Vec(_)
            | ProjectionAlgorithm::LabelPropagation { .. } => GraphRead::Adjacency,
            _ => GraphRead::Edges,
        },
    )
    .await?;
    let working = Arc::new(graph_budget.new_empty());
    let projection_ready_ms = scope.elapsed_ms();
    let (scores, mut metrics) = algorithm
        .run(&mut graph, scope, &working, &graph_budget)
        .await?;
    metrics.projection_ready_ms = Some(projection_ready_ms);
    metrics.kernel_ms = Some(scope.elapsed_ms().saturating_sub(projection_ready_ms));
    let mut buffer = Buffer::new(
        &scope.runtime.memory_pool,
        Some(&scope.runtime.disk_manager),
    );
    for start in (0..graph.vertices.len()).step_by(batch_size) {
        scope.check()?;
        let end = (start + batch_size).min(graph.vertices.len());
        let vertices = identities(graph.vertices[start..end].iter())?;
        let values = scores.array(&graph, start, end)?;
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(vertices), values])?;
        buffer.push(batch)?;
        tokio::task::yield_now().await;
    }
    *scope.algorithm.lock().expect("algorithm metrics") = Some(metrics);
    drop(scores);
    drop(working);
    drop(graph);
    drop(graph_budget);
    buffer.finish(schema, &scope.reads)
}

fn validate_route_count(k: usize, strategy: RouteStrategy) -> Result<()> {
    ensure!((1..=100).contains(&k), "route k must be between 1 and 100");
    ensure!(
        matches!(strategy, RouteStrategy::Auto) || k == 1,
        "selected route strategy requires k=1"
    );
    Ok(())
}

pub(crate) async fn route_table(
    catalog: &crate::Catalog,
    projection: &GraphProjection,
    request: &Route,
    strategy: RouteStrategy,
    context: &SessionContext,
    scope: &Arc<QueryScope>,
    explain_only: bool,
) -> Result<(Arc<dyn TableProvider>, MemoryReservation)> {
    validate_route_count(request.k, strategy)?;
    let labels = Labels::new(catalog, projection)?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("rank", DataType::UInt64, false),
        Field::new("cost", DataType::Float64, true),
        Field::new("hops", DataType::UInt64, false),
        Field::new(
            "vertices",
            DataType::List(Arc::new(identity_field("item"))),
            false,
        ),
        Field::new(
            "edges",
            DataType::List(Arc::new(identity_field("item"))),
            false,
        ),
        Field::new("cost_exact", DataType::Utf8, false),
    ]));
    if explain_only {
        return Buffer::new(&scope.runtime.memory_pool, None).finish_schema(schema);
    }
    let batch_size = scope.options.batch_size;
    let graph_budget =
        MemoryConsumer::new("GraphLake projected topology").register(&scope.runtime.memory_pool);
    let graph = Graph::read(
        catalog,
        projection,
        &labels,
        context,
        scope,
        &graph_budget,
        GraphRead::Routes {
            signed: strategy.signed(),
        },
    )
    .await?;
    let start = graph.vertex(&request.start)?;
    let target = graph.vertex(&request.target)?;
    let working = graph_budget.new_empty();
    let (paths, name) =
        route_strategies::find(&graph, start, target, request.k, strategy, scope, &working).await?;
    let mut buffer = Buffer::new(
        &scope.runtime.memory_pool,
        Some(&scope.runtime.disk_manager),
    );
    for (chunk, paths) in paths.chunks(batch_size).enumerate() {
        scope.check()?;
        let mut chunk_bytes = paths.len().saturating_mul(64);
        for path in paths {
            for &id in &path.vertices {
                let entity = &graph.vertices[id];
                chunk_bytes = chunk_bytes
                    .saturating_add(entity.label.len())
                    .saturating_add(entity.key.iter().map(String::len).sum::<usize>())
                    .saturating_add(entity.key.len().saturating_mul(4))
                    .saturating_add(16);
            }
            for &id in &path.edges {
                let entity = &graph.route_edges[id];
                chunk_bytes = chunk_bytes
                    .saturating_add(entity.label.len())
                    .saturating_add(entity.key.iter().map(String::len).sum::<usize>())
                    .saturating_add(entity.key.len().saturating_mul(4))
                    .saturating_add(16);
            }
        }
        let chunk_budget = working.new_empty();
        chunk_budget.try_grow(chunk_bytes)?;
        let vertices = identity_lists(
            paths
                .iter()
                .map(|path| path.vertices.iter().map(|&id| &graph.vertices[id])),
        )?;
        let edges = identity_lists(
            paths
                .iter()
                .map(|path| path.edges.iter().map(|&id| &graph.route_edges[id])),
        )?;
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(UInt64Array::from_iter_values(
                    (1..=paths.len()).map(|i| (chunk * batch_size + i) as u64),
                )),
                Arc::new(Float64Array::from_iter(paths.iter().map(|path| {
                    path.cost.to_f64().filter(|value| value.is_finite())
                }))),
                Arc::new(UInt64Array::from_iter_values(
                    paths.iter().map(|path| path.edges.len() as u64),
                )),
                Arc::new(vertices),
                Arc::new(edges),
                Arc::new(StringArray::from_iter_values(
                    paths
                        .iter()
                        .map(|path| path.cost.normalized().to_plain_string()),
                )),
            ],
        )?;
        drop(chunk_budget);
        buffer.push(batch)?;
        tokio::task::yield_now().await;
    }
    *scope.algorithm.lock().expect("algorithm metrics") = Some(metrics(name, &graph));
    drop(paths);
    drop(working);
    drop(graph);
    drop(graph_budget);
    buffer.finish(schema, &scope.reads)
}

fn metrics(name: &'static str, graph: &Graph) -> GraphAlgorithmMetrics {
    GraphAlgorithmMetrics {
        name,
        vertices: graph.vertices.len(),
        edges: graph.links.len(),
        iterations: None,
        converged: None,
        components: None,
        execution: "local_projection",
        workspace_bytes: None,
        projection_ready_ms: None,
        kernel_ms: None,
    }
}

impl ProjectionAlgorithm {
    fn validate(self) -> Result<()> {
        match self {
            Self::PageRank {
                damping,
                tolerance,
                max_iterations,
            } => GraphAlgorithm::PageRank {
                damping,
                tolerance,
                max_iterations,
            }
            .validate(),
            Self::Louvain(options) | Self::Leiden(options) => {
                ensure!(
                    options.resolution.is_finite()
                        && options.resolution > 0.0
                        && options.max_iterations > 0,
                    "community resolution and max_iterations must be positive"
                );
                Ok(())
            }
            Self::Node2Vec(options) => options.validate(),
            Self::LabelPropagation { max_iterations } => {
                ensure!(
                    max_iterations > 0,
                    "label propagation requires positive max_iterations"
                );
                Ok(())
            }
            Self::WeaklyConnectedComponents => Ok(()),
        }
    }

    async fn run(
        self,
        graph: &mut Graph,
        scope: &Arc<QueryScope>,
        budget: &Arc<MemoryReservation>,
        projected: &Arc<MemoryReservation>,
    ) -> Result<(Scores, GraphAlgorithmMetrics)> {
        let name = match self {
            Self::PageRank { .. } => "pagerank",
            Self::WeaklyConnectedComponents => "weak_components",
            Self::Louvain(_) => "louvain",
            Self::Leiden(_) => "leiden",
            Self::LabelPropagation { .. } => "label_propagation",
            Self::Node2Vec(_) => "node2vec",
        };
        let mut stats = metrics(name, graph);
        let scores = match self {
            Self::PageRank {
                damping,
                tolerance,
                max_iterations,
            } => {
                budget.try_grow(stats.vertices.saturating_mul(3 * size_of::<f64>()))?;
                let (ranks, iterations, converged) =
                    pagerank(graph, scope, damping, tolerance, max_iterations).await?;
                stats.iterations = Some(iterations);
                stats.converged = Some(converged);
                Scores::Rank(ranks)
            }
            Self::WeaklyConnectedComponents => {
                let n = stats.vertices;
                budget.try_grow(n.saturating_mul(3 * size_of::<usize>()))?;
                let mut parents = (0..n).collect::<Vec<_>>();
                let mut sizes = vec![1; n];
                for (i, link) in graph.links.iter().enumerate() {
                    let mut a = root(&mut parents, link.from);
                    let mut b = root(&mut parents, link.to);
                    if a != b {
                        if sizes[a] < sizes[b] {
                            std::mem::swap(&mut a, &mut b);
                        }
                        parents[b] = a;
                        sizes[a] += sizes[b];
                    }
                    cooperate(scope, i).await?;
                }
                let mut best = vec![usize::MAX; n];
                let mut count = 0;
                for (vertex, label) in sizes.iter_mut().enumerate() {
                    let component = root(&mut parents, vertex);
                    if best[component] == usize::MAX {
                        best[component] = vertex;
                        count += 1;
                    }
                    *label = best[component];
                    cooperate(scope, vertex).await?;
                }
                stats.components = Some(count);
                Scores::Components(sizes)
            }
            Self::Louvain(options) | Self::Leiden(options) => {
                let working = if graph.links.iter().any(|link| link.weight > 0.0) {
                    stats
                        .vertices
                        .saturating_mul(512)
                        .saturating_add(stats.edges.saturating_mul(256))
                } else {
                    0
                };
                stats.workspace_bytes = Some(
                    working.saturating_add(stats.vertices.saturating_mul(4 * size_of::<usize>())),
                );
                let (labels, count) = communities(
                    graph,
                    scope,
                    budget,
                    options,
                    matches!(self, Self::Louvain(_)),
                    projected,
                    working,
                    &stats,
                )
                .await?;
                stats.components = Some(count);
                Scores::Communities(labels)
            }
            Self::LabelPropagation { max_iterations } => {
                let (labels, iterations, converged) =
                    label_propagation(graph, scope, budget, max_iterations).await?;
                stats.iterations = Some(iterations);
                stats.converged = Some(converged);
                stats.components = Some(
                    labels
                        .iter()
                        .copied()
                        .collect::<std::collections::BTreeSet<_>>()
                        .len(),
                );
                Scores::Communities(labels)
            }
            Self::Node2Vec(options) => {
                stats.workspace_bytes = Some(embedding::workspace(stats.vertices, options));
                reserve_workspace(budget, scope, stats.workspace_bytes.unwrap(), &stats)?;
                let input = std::mem::take(graph);
                let worker_scope = scope.clone();
                let worker_memory = (projected.clone(), budget.clone());
                let stop = crate::CancellationToken::new();
                let _cancel_worker = stop.clone().drop_guard();
                let (input, values) = tokio::task::spawn_blocking(move || {
                    let _memory = worker_memory;
                    let values = embedding::train(&input, options, &worker_scope, &stop)?;
                    Ok::<_, anyhow::Error>((input, values))
                })
                .await??;
                *graph = input;
                // embedding::train dropped the `output` matrix; release its half of the reservation
                // before materializing Arrow output batches.
                let retained = stats
                    .vertices
                    .saturating_mul(options.dimensions)
                    .saturating_mul(std::mem::size_of::<f32>());
                if budget.size() > retained {
                    budget.shrink(budget.size() - retained);
                }
                stats.iterations = Some(options.epochs);
                Scores::Embedding {
                    values,
                    dimensions: options.dimensions,
                }
            }
        };
        Ok((scores, stats))
    }
}

async fn communities(
    graph: &mut Graph,
    scope: &Arc<QueryScope>,
    reservation: &MemoryReservation,
    options: CommunityOptions,
    louvain: bool,
    projected: &MemoryReservation,
    workspace: usize,
    stats: &GraphAlgorithmMetrics,
) -> Result<(Vec<u64>, usize)> {
    let n = graph.vertices.len();
    reserve_workspace(
        reservation,
        scope,
        n.saturating_mul(4 * size_of::<usize>()),
        stats,
    )?;
    if workspace == 0 {
        return Ok(((0..n as u64).collect(), n));
    }
    let worker = reservation.new_empty();
    reserve_workspace(&worker, scope, workspace, stats)?;
    let mut builder = GraphDataBuilder::new(n);
    let mut total = 0.0;
    for (i, link) in graph.links.iter().enumerate() {
        if link.weight > 0.0 {
            total += link.weight;
            ensure!(
                total.is_finite() && total <= f64::MAX / 2.0,
                "total community edge weight exceeds Float64"
            );
            builder.add_edge(link.from.min(link.to), link.from.max(link.to), link.weight)?;
        }
        cooperate(scope, i).await?;
    }
    // The community solver owns its edge list. Only projected vertex identities
    // are needed to render the resulting labels.
    drop(std::mem::take(&mut graph.links));
    projected.shrink(stats.edges.saturating_mul(2 * size_of::<graph::Link>()));
    let config = LeidenConfig {
        max_iterations: options.max_iterations,
        resolution: options.resolution,
        seed: Some(options.seed),
        quality: QualityType::Modularity,
        skip_refinement: louvain,
        ..Default::default()
    };
    let stop = crate::CancellationToken::new();
    let _stop_on_drop = stop.clone().drop_guard();
    let worker_scope = Arc::clone(scope);
    let (result, _worker) = tokio::task::spawn_blocking(move || -> Result<_> {
        worker_scope.check()?;
        anyhow::ensure!(!stop.is_cancelled(), "query cancelled");
        let graph = builder.build()?;
        let result = Leiden::new(config).run_interruptible(&graph, &|| {
            stop.is_cancelled() || worker_scope.check().is_err()
        });
        worker_scope.check()?;
        Ok((result?, worker))
    })
    .await??;
    let mut identifiers = BTreeMap::new();
    let labels = result
        .partition
        .as_slice()
        .iter()
        .enumerate()
        .map(|(index, label)| *identifiers.entry(*label).or_insert(index as u64))
        .collect();
    Ok((labels, identifiers.len()))
}

fn reserve_workspace(
    reservation: &MemoryReservation,
    scope: &QueryScope,
    bytes: usize,
    stats: &GraphAlgorithmMetrics,
) -> Result<()> {
    reservation.try_grow(bytes).map_err(|error| anyhow::anyhow!(error).context(format!(
        "algorithm memory budget for {} ({} projected vertices, {} internal links, {} bytes working space, {} byte query budget)",
        stats.name, stats.vertices, stats.edges, stats.workspace_bytes.unwrap_or(bytes), scope.options.memory_bytes,
    )))
}

async fn pagerank(
    graph: &Graph,
    scope: &QueryScope,
    damping: f64,
    tolerance: f64,
    max: usize,
) -> Result<(Vec<f64>, usize, bool)> {
    let n = graph.vertices.len();
    if n == 0 {
        return Ok((Vec::new(), 0, true));
    }
    let mut strength = Vec::with_capacity(n);
    for (index, row) in graph.adjacency.iter().enumerate() {
        let sum = row
            .iter()
            .map(|neighbor| graph.links[neighbor.edge].weight)
            .sum::<f64>();
        ensure!(sum.is_finite(), "PageRank outgoing weight exceeds Float64");
        strength.push(sum);
        cooperate(scope, index).await?;
    }
    let mut rank = vec![1.0 / n as f64; n];
    let mut next = vec![0.0; n];
    for i in 1..=max {
        let dangling: f64 = rank
            .iter()
            .zip(&strength)
            .filter(|(_, strength)| **strength == 0.0)
            .map(|(rank, _)| *rank)
            .sum();
        next.fill((1.0 - damping + damping * dangling) / n as f64);
        let mut work = 0_usize;
        for (from, row) in graph.adjacency.iter().enumerate() {
            if strength[from] > 0.0 {
                for neighbor in row {
                    next[neighbor.vertex] +=
                        damping * rank[from] * (graph.links[neighbor.edge].weight / strength[from]);
                    work += 1;
                    if work.is_multiple_of(COOPERATIVE_INTERVAL) {
                        cooperate(scope, work).await?;
                    }
                }
            }
            cooperate(scope, from).await?;
        }
        let delta = rank
            .iter()
            .zip(&next)
            .map(|(a, b)| (a - b).abs())
            .sum::<f64>();
        std::mem::swap(&mut rank, &mut next);
        if delta <= tolerance {
            return Ok((rank, i, true));
        }
    }
    Ok((rank, max, false))
}

async fn label_propagation(
    graph: &Graph,
    scope: &QueryScope,
    budget: &MemoryReservation,
    max_iterations: usize,
) -> Result<(Vec<u64>, usize, bool)> {
    let max_degree = graph.adjacency.iter().map(Vec::len).max().unwrap_or(0);
    budget.try_grow(
        graph
            .vertices
            .len()
            .saturating_mul(32)
            .saturating_add(max_degree.saturating_mul(96)),
    )?;
    let mut labels = (0..graph.vertices.len() as u64).collect::<Vec<_>>();
    let mut votes = std::collections::HashMap::<u64, f64>::with_capacity(max_degree.min(1024));
    let mut work = 0;
    for iteration in 1..=max_iterations {
        let mut changed = false;
        for (vertex, neighbors) in graph.adjacency.iter().enumerate() {
            work += 1;
            cooperate(scope, work).await?;
            votes.clear();
            for neighbor in neighbors {
                work += 1;
                cooperate(scope, work).await?;
                let weight = graph.links[neighbor.edge].weight;
                if weight > 0.0 {
                    let total = votes.entry(labels[neighbor.vertex]).or_default();
                    *total += weight;
                    ensure!(
                        total.is_finite(),
                        "label propagation weights exceed Float64"
                    );
                }
            }
            let current = labels[vertex];
            let next = votes
                .iter()
                .max_by(|(left, a), (right, b)| {
                    a.total_cmp(b)
                        .then_with(|| (**left == current).cmp(&(**right == current)))
                        .then_with(|| right.cmp(left))
                })
                .map_or(current, |(&label, _)| label);
            changed |= next != current;
            labels[vertex] = next;
        }
        if !changed {
            return Ok((labels, iteration, true));
        }
    }
    Ok((labels, max_iterations, false))
}

async fn cooperate(scope: &QueryScope, index: usize) -> Result<()> {
    if index.is_multiple_of(COOPERATIVE_INTERVAL) {
        scope.check()?;
        tokio::task::yield_now().await;
    }
    Ok(())
}

enum Scores {
    Rank(Vec<f64>),
    Components(Vec<usize>),
    Communities(Vec<u64>),
    Embedding { values: Vec<f32>, dimensions: usize },
}

impl ProjectionAlgorithm {
    fn field(&self) -> Field {
        match self {
            Self::PageRank { .. } => Field::new("__graphlake_rank", DataType::Float64, false),
            Self::WeaklyConnectedComponents => identity_field("__graphlake_component"),
            Self::Louvain(_) | Self::Leiden(_) | Self::LabelPropagation { .. } => {
                Field::new("__graphlake_community", DataType::UInt64, false)
            }
            Self::Node2Vec(_) => Field::new(
                "__graphlake_embedding",
                DataType::List(Arc::new(Field::new("item", DataType::Float32, false))),
                false,
            ),
        }
    }
}

impl Scores {
    fn array(&self, graph: &Graph, start: usize, end: usize) -> Result<ArrayRef> {
        Ok(match self {
            Self::Rank(values) => Arc::new(Float64Array::from_iter_values(
                values[start..end].iter().copied(),
            )),
            Self::Components(labels) => Arc::new(identities(
                labels[start..end].iter().map(|&id| &graph.vertices[id]),
            )?),
            Self::Communities(values) => Arc::new(UInt64Array::from_iter_values(
                values[start..end].iter().copied(),
            )),
            Self::Embedding { values, dimensions } => {
                let offsets = (0..=end - start)
                    .map(|row| i32::try_from(row * dimensions))
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                let array = Float32Array::from_iter_values(
                    values[start * dimensions..end * dimensions].iter().copied(),
                );
                Arc::new(ListArray::try_new(
                    Arc::new(Field::new("item", DataType::Float32, false)),
                    OffsetBuffer::new(ScalarBuffer::from(offsets)),
                    Arc::new(array),
                    None,
                )?)
            }
        })
    }
}

fn identity_field(name: &str) -> Field {
    Field::new(
        name,
        DataType::Struct(
            vec![
                Field::new("label", DataType::Utf8, false),
                Field::new(
                    "key",
                    DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
                    false,
                ),
            ]
            .into(),
        ),
        false,
    )
}

fn identities<'a>(ids: impl Iterator<Item = &'a Identity>) -> Result<StructArray> {
    let mut labels = StringBuilder::new();
    let mut keys = ListBuilder::new(StringBuilder::new());
    let mut label_bytes = 0usize;
    let mut key_bytes = 0usize;
    for id in ids {
        label_bytes = label_bytes.saturating_add(id.label.len());
        ensure!(
            i32::try_from(label_bytes).is_ok(),
            "projected route output exceeds Arrow string capacity"
        );
        labels.append_value(id.label.as_ref());
        for part in &id.key {
            key_bytes = key_bytes.saturating_add(part.len());
            ensure!(
                i32::try_from(key_bytes).is_ok(),
                "projected route output exceeds Arrow string capacity"
            );
            keys.values().append_value(part);
        }
        keys.append(true);
    }
    let fields = match identity_field("item").data_type() {
        DataType::Struct(fields) => fields.clone(),
        _ => unreachable!("identity field is a struct"),
    };
    Ok(StructArray::try_new(
        fields,
        vec![Arc::new(labels.finish()), Arc::new(keys.finish())],
        None,
    )?)
}

fn identity_lists<'a, I: Iterator<Item = &'a Identity>>(
    rows: impl Iterator<Item = I>,
) -> Result<ListArray> {
    let mut offsets = vec![0_i32];
    let mut values = Vec::new();
    for row in rows {
        values.extend(row);
        offsets.push(i32::try_from(values.len())?);
    }
    Ok(ListArray::try_new(
        Arc::new(identity_field("item")),
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        Arc::new(identities(values.into_iter())?),
        None,
    )?)
}
