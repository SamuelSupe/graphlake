use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

use anyhow::Result;
use datafusion::{
    arrow::{
        array::{Array, RecordBatch, UInt64Array},
        compute::take_record_batch,
        datatypes::SchemaRef,
    },
    execution::{
        TaskContext,
        memory_pool::{MemoryConsumer, MemoryReservation},
    },
    physical_plan::stream::RecordBatchStreamAdapter,
};
use futures::TryStreamExt;

use crate::{
    Catalog, ScalarValue,
    engine::{Prepared, QueryScope},
    source::{ReadStats, postgres::PgTable},
};

use super::{
    Direction, Inputs, Topology, Traversal,
    topology::{coerce_key, encode_key, row_converter},
};

const KEYS_PER_REQUEST: usize = 128;

#[derive(Clone, Debug)]
pub(super) struct Remote {
    nodes: Arc<PgTable>,
    edges: Arc<PgTable>,
    key: Vec<String>,
    from: Vec<String>,
    selected_edges: Vec<String>,
    scan_after: Option<usize>,
    stats: Arc<ReadStats>,
}

impl Remote {
    pub(super) async fn open(
        catalog: &Catalog,
        prepared: &Prepared,
        scope: &QueryScope,
        edge_name: &str,
        direction: Direction,
        force: bool,
    ) -> Result<Option<Self>> {
        let edge = &catalog.edges[edge_name];
        let node = &catalog.nodes[&edge.from.node];
        if !edge.inputs.is_empty()
            || !node.inputs.is_empty()
            || edge.scd2.is_some()
            || node.scd2.is_some()
            || !scope.options.graph.is_empty()
        {
            return Ok(None);
        }
        let (Some(nodes), Some(edges)) = (
            prepared.postgres.get(&node.source),
            prepared.postgres.get(&edge.source),
        ) else {
            return Ok(None);
        };
        if edge.key.is_empty() {
            return Ok(None);
        }
        let (from, to) = match direction {
            Direction::Outgoing => (&edge.from.columns, &edge.to.columns),
            Direction::Incoming => (&edge.to.columns, &edge.from.columns),
            Direction::Undirected => return Ok(None),
        };
        let (Some(_), Some(edge_rows)) =
            tokio::try_join!(nodes.index_estimate(&node.key), edges.index_estimate(from),)?
        else {
            return Ok(None);
        };
        if !force && edge_rows > 0 && edge_rows < (KEYS_PER_REQUEST * 2) as u64 {
            return Ok(None);
        }
        let scan_after =
            (!force).then(|| (edge_rows / 8).clamp(KEYS_PER_REQUEST as u64, 4096) as usize);
        Ok(Some(Self {
            nodes: Arc::clone(nodes),
            edges: Arc::clone(edges),
            key: node.key.clone(),
            from: from.clone(),
            selected_edges: from.iter().chain(to).chain(&edge.key).cloned().collect(),
            scan_after,
            stats: Arc::clone(&scope.reads),
        }))
    }

    /// Every edge on a bounded walk leaves a vertex with a shortest distance
    /// below the bound. Fetch each such vertex once, then reuse the trail walker.
    pub(super) async fn load(
        &self,
        request: &Traversal,
        inputs: &Inputs,
        context: Arc<TaskContext>,
        topology_budget: &MemoryReservation,
    ) -> Result<Option<Topology>> {
        let budget = MemoryConsumer::new("GraphLake remote frontier")
            .register(&context.runtime_env().memory_pool);
        let node_schema = inputs.nodes.schema();
        let edge_schema = inputs.edges.schema();
        let converter = row_converter(&node_schema)?;
        let start = coerce_key(&request.start, &node_schema)?;
        let encoded = encode_key(&start, &node_schema, &converter)?;
        let mut seen = HashSet::from([encoded.clone()]);
        let mut candidates = BTreeMap::from([(encoded, start)]);
        let mut node_batches = Vec::new();
        let mut edge_batches = Vec::new();
        let mut edge_rows = 0usize;
        for depth in 0..=request.max_hops {
            let mut frontier = Vec::new();
            let entries = candidates.into_iter().collect::<Vec<_>>();
            for chunk in entries.chunks(batch_size(self.key.len())) {
                let keys = chunk.iter().map(|(_, key)| key.clone()).collect::<Vec<_>>();
                let expected = chunk
                    .iter()
                    .map(|(encoded, _)| encoded.as_slice())
                    .collect::<HashSet<_>>();
                let mut rows = self.nodes.lookup(
                    &self.key,
                    &keys,
                    &self.key,
                    Arc::clone(&node_schema),
                    Arc::clone(&context),
                )?;
                while let Some(batch) = rows.try_next().await? {
                    let encoded = converter.convert_columns(batch.columns())?;
                    let mut selected = Vec::new();
                    for (index, row) in encoded.iter().enumerate() {
                        if expected.contains(row.data()) {
                            selected.push(index as u64);
                            if depth < request.max_hops {
                                budget.try_grow(
                                    row.data().len().saturating_add(self.key.len() * 64),
                                )?;
                                frontier.push((
                                    row.data().to_vec(),
                                    scalars(&batch, 0, self.key.len(), index)?,
                                ));
                            }
                        }
                    }
                    retain(batch, selected, &budget, &mut node_batches)?;
                }
            }
            if self
                .scan_after
                .is_some_and(|limit| seen.len() >= limit || edge_rows >= limit)
            {
                self.stats.frontier_scan_fallback();
                return Ok(None);
            }
            if frontier.is_empty() {
                break;
            }
            candidates = BTreeMap::new();
            for chunk in frontier.chunks(batch_size(self.key.len())) {
                let keys = chunk.iter().map(|(_, key)| key.clone()).collect::<Vec<_>>();
                let expected = chunk
                    .iter()
                    .map(|(encoded, _)| encoded.as_slice())
                    .collect::<HashSet<_>>();
                let mut rows = self.edges.lookup(
                    &self.from,
                    &keys,
                    &self.selected_edges,
                    Arc::clone(&edge_schema),
                    Arc::clone(&context),
                )?;
                while let Some(batch) = rows.try_next().await? {
                    edge_rows = edge_rows.saturating_add(batch.num_rows());
                    if self.scan_after.is_some_and(|limit| edge_rows >= limit) {
                        self.stats.frontier_scan_fallback();
                        return Ok(None);
                    }
                    let width = self.key.len();
                    let selected = discover(
                        &batch,
                        &converter,
                        &expected,
                        width,
                        &mut candidates,
                        &mut seen,
                        &budget,
                    )?;
                    retain(batch, selected, &budget, &mut edge_batches)?;
                }
            }
            if candidates.is_empty() {
                break;
            }
        }
        let nodes = buffered(node_schema, node_batches);
        let edges = buffered(edge_schema, edge_batches);
        Ok(Some(
            Topology::load(
                nodes,
                edges,
                Arc::clone(&inputs.identity),
                false,
                topology_budget,
            )
            .await?,
        ))
    }
}

fn discover(
    batch: &RecordBatch,
    converter: &datafusion::arrow::row::RowConverter,
    expected: &HashSet<&[u8]>,
    width: usize,
    candidates: &mut BTreeMap<Vec<u8>, Vec<ScalarValue>>,
    seen: &mut HashSet<Vec<u8>>,
    budget: &MemoryReservation,
) -> Result<Vec<u64>> {
    let from = converter.convert_columns(&batch.columns()[..width])?;
    let to = converter.convert_columns(&batch.columns()[width..2 * width])?;
    let mut selected = Vec::new();
    for (index, (from, to)) in from.iter().zip(to.iter()).enumerate() {
        if !expected.contains(from.data())
            || batch.columns()[width..]
                .iter()
                .any(|column| column.is_null(index))
        {
            continue;
        }
        selected.push(index as u64);
        if !seen.contains(to.data()) {
            budget.try_grow(
                to.data()
                    .len()
                    .saturating_mul(3)
                    .saturating_add(width * 64 + 64),
            )?;
            let key = to.data().to_vec();
            seen.insert(key.clone());
            candidates.insert(key, scalars(batch, width, width, index)?);
        }
    }
    Ok(selected)
}

fn retain(
    batch: RecordBatch,
    indices: Vec<u64>,
    budget: &MemoryReservation,
    batches: &mut Vec<RecordBatch>,
) -> Result<()> {
    if indices.is_empty() {
        return Ok(());
    }
    budget.try_grow(batch.get_array_memory_size())?;
    let batch = if indices.len() == batch.num_rows() {
        batch
    } else {
        take_record_batch(&batch, &UInt64Array::from(indices))?
    };
    batches.push(batch);
    Ok(())
}

fn buffered(
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
) -> datafusion::execution::SendableRecordBatchStream {
    Box::pin(RecordBatchStreamAdapter::new(
        schema,
        futures::stream::iter(batches.into_iter().map(Ok)),
    ))
}

fn scalars(
    batch: &RecordBatch,
    offset: usize,
    width: usize,
    row: usize,
) -> Result<Vec<ScalarValue>> {
    Ok(batch.columns()[offset..offset + width]
        .iter()
        .map(|column| ScalarValue::try_from_array(column.as_ref(), row))
        .collect::<Result<Vec<_>, _>>()?)
}

fn batch_size(width: usize) -> usize {
    KEYS_PER_REQUEST.min((32_000 / width).max(1))
}
