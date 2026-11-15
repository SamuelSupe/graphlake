use std::{mem::size_of, sync::Arc};

use anyhow::{Context, Result, ensure};
use datafusion::{
    arrow::{
        array::{ArrayRef, Float64Array, RecordBatch, StructArray, UInt64Array},
        compute::take,
        datatypes::{DataType, Field, Schema, SchemaRef},
    },
    execution::memory_pool::MemoryReservation,
};
use serde::Serialize;

use crate::{Engine, QueryOptions, QueryResult};

use super::{COOPERATIVE_INTERVAL, Direction, Mode, Topology, TraversalExec, TraversalStrategy};

#[derive(Clone, Copy, Debug)]
pub enum GraphAlgorithm {
    /// Directed, unweighted PageRank, with parallel relationships counted individually.
    PageRank {
        damping: f64,
        tolerance: f64,
        max_iterations: usize,
    },
    /// Ignore relationship direction and label each component with its lowest business key.
    WeaklyConnectedComponents,
}

#[derive(Clone, Debug, Serialize)]
pub struct GraphAlgorithmMetrics {
    pub name: &'static str,
    pub vertices: usize,
    pub edges: usize,
    pub iterations: Option<usize>,
    pub converged: Option<bool>,
    pub components: Option<usize>,
}

impl Engine {
    /// Run an algorithm over one edge type on a single node type, including isolated vertices.
    pub async fn analyze_graph(
        &self,
        edge: &str,
        algorithm: GraphAlgorithm,
        options: QueryOptions,
        strategy: TraversalStrategy,
    ) -> Result<QueryResult> {
        algorithm.validate()?;
        ensure!(
            strategy != TraversalStrategy::Batch,
            "whole-graph algorithms require a scan or a published CSR"
        );
        let mapping = self
            .catalog
            .edges
            .get(edge)
            .with_context(|| format!("unknown edge type {edge}"))?;
        ensure!(
            mapping.from.node == mapping.to.node,
            "whole-graph algorithms require one endpoint node type"
        );
        let scope = self.scope(options).await?;
        scope
            .run(async {
                let context = scope.session();
                let (input, prepared) = self
                    .graph_input(edge, Direction::Outgoing, strategy, &scope, &context, true)
                    .await?;
                let plan = TraversalExec::new(
                    input,
                    Mode::Algorithm {
                        edge: edge.into(),
                        algorithm,
                        metrics: Arc::clone(&scope.algorithm),
                    },
                    Arc::clone(&scope.visits_by_depth),
                );
                scope.execute(
                    &self.catalog.graph,
                    &context,
                    Arc::new(plan),
                    prepared,
                    None,
                )
            })
            .await
    }
}

impl GraphAlgorithm {
    pub(crate) fn validate(self) -> Result<()> {
        if let Self::PageRank {
            damping,
            tolerance,
            max_iterations,
        } = self
        {
            ensure!(
                damping.is_finite() && damping > 0.0 && damping < 1.0,
                "PageRank damping must be between 0 and 1"
            );
            ensure!(
                tolerance.is_finite() && tolerance > 0.0,
                "PageRank tolerance must be positive and finite"
            );
            ensure!(
                max_iterations > 0,
                "PageRank max_iterations must be positive"
            );
        }
        Ok(())
    }

    pub(super) fn field(self, keys: &Schema) -> Field {
        match self {
            Self::PageRank { .. } => Field::new("__graphlake_rank", DataType::Float64, false),
            Self::WeaklyConnectedComponents => Field::new(
                "__graphlake_component",
                DataType::Struct(keys.fields().clone()),
                false,
            ),
        }
    }

    pub(super) async fn run(
        self,
        graph: &Topology,
        budget: &MemoryReservation,
    ) -> Result<(Scores, GraphAlgorithmMetrics)> {
        let mut metrics = GraphAlgorithmMetrics {
            name: match self {
                Self::PageRank { .. } => "pagerank",
                Self::WeaklyConnectedComponents => "weak_components",
            },
            vertices: graph.keys.num_rows(),
            edges: graph.neighbors.len(),
            iterations: None,
            converged: None,
            components: None,
        };
        let scores = match self {
            Self::PageRank {
                damping,
                tolerance,
                max_iterations,
            } => {
                budget.try_grow(metrics.vertices.saturating_mul(2 * size_of::<f64>()))?;
                let (ranks, iterations, converged) =
                    pagerank(graph, damping, tolerance, max_iterations).await;
                metrics.iterations = Some(iterations);
                metrics.converged = Some(converged);
                Scores::PageRank(ranks)
            }
            Self::WeaklyConnectedComponents => {
                budget.try_grow(
                    metrics
                        .vertices
                        .saturating_mul(3 * size_of::<usize>() + size_of::<&[u8]>()),
                )?;
                let (components, count) = weak_components(graph).await;
                metrics.components = Some(count);
                Scores::Components(components)
            }
        };
        Ok((scores, metrics))
    }
}

pub(super) enum Scores {
    PageRank(Vec<f64>),
    Components(Vec<usize>),
}

impl Scores {
    pub(super) fn batch(
        &self,
        graph: &Topology,
        schema: SchemaRef,
        offset: usize,
        len: usize,
    ) -> datafusion::common::Result<RecordBatch> {
        let keys = graph.keys.slice(offset, len);
        let output: ArrayRef = match self {
            Self::PageRank(ranks) => Arc::new(Float64Array::from_iter_values(
                ranks[offset..offset + len].iter().copied(),
            )),
            Self::Components(labels) => {
                let ids = UInt64Array::from_iter_values(
                    labels[offset..offset + len].iter().map(|&id| id as u64),
                );
                let values = graph
                    .keys
                    .columns()
                    .iter()
                    .map(|column| take(column, &ids, None))
                    .collect::<Result<Vec<_>, _>>()?;
                let representatives = RecordBatch::try_new(graph.keys.schema(), values)?;
                Arc::new(StructArray::from(representatives))
            }
        };
        let mut columns = keys.columns().to_vec();
        columns.push(output);
        Ok(RecordBatch::try_new(schema, columns)?)
    }
}

async fn pagerank(
    graph: &Topology,
    damping: f64,
    tolerance: f64,
    max_iterations: usize,
) -> (Vec<f64>, usize, bool) {
    let n = graph.keys.num_rows();
    if n == 0 {
        return (Vec::new(), 0, true);
    }
    let mut ranks = vec![1.0 / n as f64; n];
    let mut next = vec![0.0; n];
    for iteration in 1..=max_iterations {
        let mut dangling = 0.0;
        for (vertex, range) in graph.offsets.windows(2).enumerate() {
            if range[0] == range[1] {
                dangling += ranks[vertex];
            }
            if vertex % COOPERATIVE_INTERVAL == 0 {
                tokio::task::yield_now().await;
            }
        }
        let base = (1.0 - damping + damping * dangling) / n as f64;
        for chunk in next.chunks_mut(COOPERATIVE_INTERVAL) {
            chunk.fill(base);
            tokio::task::yield_now().await;
        }
        for (vertex, range) in graph.offsets.windows(2).enumerate() {
            let neighbors = &graph.neighbors[range[0]..range[1]];
            if !neighbors.is_empty() {
                let share = damping * ranks[vertex] / neighbors.len() as f64;
                for (chunk_index, chunk) in neighbors.chunks(COOPERATIVE_INTERVAL).enumerate() {
                    for neighbor in chunk {
                        next[neighbor.vertex] += share;
                    }
                    if chunk_index > 0 || chunk.len() == COOPERATIVE_INTERVAL {
                        tokio::task::yield_now().await;
                    }
                }
            }
            if vertex % COOPERATIVE_INTERVAL == 0 {
                tokio::task::yield_now().await;
            }
        }
        let mut delta = 0.0;
        for (before, after) in ranks
            .chunks(COOPERATIVE_INTERVAL)
            .zip(next.chunks(COOPERATIVE_INTERVAL))
        {
            delta += before
                .iter()
                .zip(after)
                .map(|(a, b)| (a - b).abs())
                .sum::<f64>();
            tokio::task::yield_now().await;
        }
        std::mem::swap(&mut ranks, &mut next);
        if delta <= tolerance {
            return (ranks, iteration, true);
        }
    }
    (ranks, max_iterations, false)
}

async fn weak_components(graph: &Topology) -> (Vec<usize>, usize) {
    let n = graph.keys.num_rows();
    let mut parents = (0..n).collect::<Vec<_>>();
    let mut sizes = vec![1_usize; n];
    for (from, range) in graph.offsets.windows(2).enumerate() {
        for (chunk_index, chunk) in graph.neighbors[range[0]..range[1]]
            .chunks(COOPERATIVE_INTERVAL)
            .enumerate()
        {
            for neighbor in chunk {
                let mut a = root(&mut parents, from);
                let mut b = root(&mut parents, neighbor.vertex);
                if a == b {
                    continue;
                }
                if sizes[a] < sizes[b] {
                    std::mem::swap(&mut a, &mut b);
                }
                parents[b] = a;
                sizes[a] += sizes[b];
            }
            if chunk_index > 0 || chunk.len() == COOPERATIVE_INTERVAL {
                tokio::task::yield_now().await;
            }
        }
        if from % COOPERATIVE_INTERVAL == 0 {
            tokio::task::yield_now().await;
        }
    }
    let mut keys = vec![&[][..]; n];
    for (i, (key, &vertex)) in graph.index.iter().enumerate() {
        keys[vertex] = key;
        if i % COOPERATIVE_INTERVAL == 0 {
            tokio::task::yield_now().await;
        }
    }
    let mut best = vec![usize::MAX; n];
    let mut count = 0;
    for vertex in 0..n {
        let component = root(&mut parents, vertex);
        let current = &mut best[component];
        if *current == usize::MAX {
            count += 1;
            *current = vertex;
        } else if keys[vertex] < keys[*current] {
            *current = vertex;
        }
        if vertex % COOPERATIVE_INTERVAL == 0 {
            tokio::task::yield_now().await;
        }
    }
    for (vertex, label) in sizes.iter_mut().enumerate() {
        let component = root(&mut parents, vertex);
        *label = best[component];
        if vertex % COOPERATIVE_INTERVAL == 0 {
            tokio::task::yield_now().await;
        }
    }
    (sizes, count)
}

pub(super) fn root(parents: &mut [usize], mut vertex: usize) -> usize {
    while parents[vertex] != vertex {
        parents[vertex] = parents[parents[vertex]];
        vertex = parents[vertex];
    }
    vertex
}
