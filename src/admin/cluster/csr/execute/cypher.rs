use super::*;

use crate::{
    Catalog,
    cypher::paths::Shortest,
    traversal::{Expansion, HOPS, PATH, PATH_NODES},
};
use datafusion::{
    arrow::{
        array::{ListArray, RecordBatch, StructArray, UInt32Array},
        buffer::OffsetBuffer,
        datatypes::SchemaRef,
        ipc::reader::StreamReader,
        row::RowConverter,
    },
    catalog::{Session, TableProvider, streaming::StreamingTable},
    common::{DataFusionError, TableReference},
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::{Expr, TableType},
    physical_plan::streaming::{PartitionStream, StreamingTableExec},
    physical_plan::{ExecutionPlan, execute_stream, stream::RecordBatchStreamAdapter},
    prelude::SessionContext,
};

#[derive(Clone)]
pub(crate) struct CypherCsr {
    worker: Arc<Worker>,
    job: Job,
    id: Uuid,
    generation: Arc<Manifest>,
    stats: Arc<std::sync::Mutex<Stats>>,
    cache: Arc<BlockCache>,
}

impl CypherCsr {
    pub(super) fn property_source<'a>(
        &'a self,
        scope: &'a QueryScope,
    ) -> super::properties::Read<'a> {
        let objects = &self.worker.shared.objects;
        super::properties::Read {
            objects,
            prefix: objects.graph(
                &self.job.model,
                &self.job.revision,
                self.generation.generation,
            ),
            manifest: &self.generation,
            scope,
            cache: Some(&self.cache),
        }
    }

    pub(in crate::admin) async fn visit_properties(
        &self,
        scope: &QueryScope,
        ids: BTreeSet<String>,
        visit: impl FnMut(&str, &str, &RecordBatch, usize) -> Result<()>,
    ) -> Result<()> {
        self.property_source(scope)
            .identities(&ids, None, false, |_| {}, visit)
            .await
    }
}

impl CypherCsr {
    pub(in crate::admin::cluster) async fn open(
        worker: Arc<Worker>,
        job: &Job,
        id: Uuid,
        generation: Uuid,
    ) -> ApiResult<Self> {
        let generation = manifest(
            &worker.shared.objects,
            &job.model,
            &job.revision,
            generation,
        )
        .await
        .map_err(storage)?;
        if generation.format < 3 {
            return Err(Error::new(
                StatusCode::CONFLICT,
                "csr_refresh_required",
                "请刷新 CSR 以生成 Cypher 所需的属性快照",
            ));
        }
        Ok(Self {
            worker,
            job: job.clone(),
            id,
            generation: Arc::new(generation),
            stats: Default::default(),
            cache: Default::default(),
        })
    }

    pub(crate) fn options(&self) -> crate::GraphOptions {
        self.generation.graph_options.clone()
    }

    pub(crate) async fn prepare(
        &self,
        context: &SessionContext,
        scope: &Arc<QueryScope>,
        catalog: &Catalog,
        labels: BTreeSet<String>,
    ) -> Result<Prepared> {
        let objects = &self.worker.shared.objects;
        let prefix = objects.graph(
            &self.job.model,
            &self.job.revision,
            self.generation.generation,
        );
        let prepared = register_tables(
            objects,
            prefix,
            &self.generation,
            context,
            scope,
            catalog,
            labels,
        )
        .await?;
        *scope.csr_generation.lock().unwrap() = Some(self.generation.generation.to_string());
        if self.indexed() {
            super::properties::register(context, self, scope);
        }
        Ok(prepared)
    }

    pub(crate) fn indexed(&self) -> bool {
        self.generation.format >= 4
    }
}

pub(in crate::admin::cluster::csr) async fn register_tables(
    objects: &Objects,
    prefix: Key,
    generation: &Arc<Manifest>,
    context: &SessionContext,
    scope: &Arc<QueryScope>,
    catalog: &Catalog,
    mut labels: BTreeSet<String>,
) -> Result<Prepared> {
    for label in labels.clone() {
        if let Some(edge) = catalog.edges.get(&label) {
            labels.extend([edge.from.node.clone(), edge.to.node.clone()]);
        }
    }
    for label in labels {
        let blocks = generation
            .tables
            .get(&label)
            .context("label missing from published CSR")?;
        let schema = if let Some(schema) = generation.table_schemas.get(&label) {
            decode_schema(schema)?
        } else {
            let path = prefix.clone().join("tables").join(label.as_str());
            let first = blocks.first().context("missing schema in CSR table")?;
            let bytes = read_block(objects, &path.clone().join("0"), first, scope).await?;
            StreamReader::try_new(std::io::Cursor::new(bytes), None)?.schema()
        };
        let table = Snapshot {
            objects: objects.clone(),
            generation: generation.clone(),
            path: prefix.clone().join("tables").join(label.as_str()),
            label: label.clone(),
            schema: schema.clone(),
            scope: scope.clone(),
        };
        context.register_table(
            TableReference::bare(label),
            Arc::new(StreamingTable::try_new(schema, vec![Arc::new(table)])?),
        )?;
    }
    Ok(Prepared {
        versions: generation.source_versions(),
        reservations: Vec::new(),
        postgres: BTreeMap::new(),
    })
}

impl CypherCsr {
    pub(in crate::admin) fn stats(&self, metrics: &crate::engine::QueryMetrics) -> Stats {
        let mut stats = self.stats.lock().unwrap().clone();
        stats.workers.insert(self.worker.id);
        stats.object_bytes += metrics.reads.object_bytes;
        stats.peak_leader_bytes = stats.peak_leader_bytes.max(metrics.peak_reserved_bytes);
        stats.lookup_spilled_bytes = metrics.reads.snapshot_spilled_bytes;
        stats
    }

    pub(super) fn property_column_bytes(&self, bytes: u64, order_only: bool) {
        if bytes != 0 {
            let mut stats = self.stats.lock().unwrap();
            if order_only {
                stats.sort_column_bytes += bytes;
            } else {
                stats.hydration_column_bytes += bytes;
            }
        }
    }

    pub(crate) fn expansion(
        &self,
        scope: &Arc<QueryScope>,
        catalog: &Catalog,
        expansion: Expansion,
        schema: SchemaRef,
        starts: Arc<dyn ExecutionPlan>,
        typed_starts: BTreeMap<String, Arc<dyn ExecutionPlan>>,
        finishes: BTreeMap<String, Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn TableProvider>> {
        let names = if let Some(mixed) = &expansion.mixed {
            mixed.edges.keys().map(String::as_str).collect::<Vec<_>>()
        } else {
            vec![expansion.edge.as_str()]
        };
        let types = names
            .iter()
            .map(|name| {
                self.generation
                    .edge_types
                    .binary_search_by(|x| x.as_str().cmp(name))
                    .map(|i| i as u32)
                    .map_err(|_| anyhow::anyhow!("edge missing from published CSR"))
            })
            .collect::<Result<BTreeSet<_>>>()?;
        let filters = types
            .iter()
            .zip(&names)
            .filter_map(|(&kind, name)| {
                let filters = if let Some(mixed) = &expansion.mixed {
                    &mixed.edges[*name]
                } else {
                    &expansion.filters
                };
                (!filters.is_empty()).then(|| (kind, filters.clone()))
            })
            .collect();
        let layout = if expansion.mixed.is_some() {
            Layout::Mixed
        } else {
            let edge = &catalog.edges[&expansion.edge];
            if edge.from.node == edge.to.node {
                Layout::Single(edge.from.node.clone())
            } else {
                Layout::Heterogeneous(edge.from.node.clone(), edge.to.node.clone())
            }
        };
        let max = end_hops(&expansion, names.iter().map(|name| &catalog.edges[*name]));
        let part = Paths {
            csr: self.clone(),
            scope: scope.clone(),
            layout,
            types,
            filters,
            expansion,
            schema: schema.clone(),
            starts,
            typed_starts,
            finishes,
            max,
        };
        Ok(Arc::new(part))
    }
}

struct Snapshot {
    objects: Objects,
    generation: Arc<Manifest>,
    path: Key,
    label: String,
    schema: SchemaRef,
    scope: Arc<QueryScope>,
}

impl std::fmt::Debug for Snapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("PublishedCsrTable")
            .field(&self.label)
            .finish()
    }
}

impl PartitionStream for Snapshot {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
    fn execute(&self, _: Arc<TaskContext>) -> SendableRecordBatchStream {
        let objects = self.objects.clone();
        let generation = self.generation.clone();
        let path = self.path.clone();
        let label = self.label.clone();
        let scope = self.scope.clone();
        let schema = self.schema.clone();
        let stream = async_stream::try_stream! {
            let objects = &objects;
            let prefetch = (scope.options.memory_bytes / (8 * 1024 * 1024)).clamp(1, 4);
            let mut reads = futures::stream::iter(0..generation.tables[&label].len())
                .map(|i| {
                    let key = path.clone().join(i.to_string());
                    let scope = &scope;
                    let block = &generation.tables[&label][i];
                    async move {
                        let reservation = MemoryConsumer::new("CSR compressed property block")
                            .register(&scope.runtime.memory_pool);
                        reservation.try_grow(block.bytes as usize)?;
                        let bytes = read_block(objects, &key, block, scope).await.map_err(external)?;
                        Ok::<_, DataFusionError>((bytes, reservation))
                    }
                })
                .buffered(prefetch);
            while let Some((bytes, _compressed)) = reads.try_next().await? {
                let budget = MemoryConsumer::new("CSR property block").register(&scope.runtime.memory_pool);
                let projection = (generation.format >= 4).then(|| (1..=schema.fields().len()).collect());
                for batch in StreamReader::try_new(std::io::Cursor::new(bytes), projection)? {
                    let batch = batch?;
                    budget.try_resize(batch.get_array_memory_size())?;
                    yield batch;
                }
            }
        };
        Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), stream))
    }
}

#[derive(Clone)]
enum Layout {
    Single(String),
    Heterogeneous(String, String),
    Mixed,
}

#[derive(Clone)]
struct Paths {
    csr: CypherCsr,
    scope: Arc<QueryScope>,
    layout: Layout,
    types: BTreeSet<u32>,
    filters: Vec<(u32, Vec<String>)>,
    expansion: Expansion,
    schema: SchemaRef,
    starts: Arc<dyn ExecutionPlan>,
    typed_starts: BTreeMap<String, Arc<dyn ExecutionPlan>>,
    finishes: BTreeMap<String, Arc<dyn ExecutionPlan>>,
    max: usize,
}

impl std::fmt::Debug for Paths {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("PartitionedCsrExpand")
            .field(&self.expansion.table)
            .finish()
    }
}

#[async_trait::async_trait]
impl TableProvider for Paths {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        _: &dyn Session,
        projection: Option<&Vec<usize>>,
        _: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let mut scan = self.clone();
        if let Some(indices) = projection {
            scan.schema = Arc::new(self.schema.project(indices)?);
        }
        Ok(Arc::new(StreamingTableExec::try_new(
            scan.schema.clone(),
            vec![Arc::new(scan)],
            None,
            None,
            false,
            limit,
        )?))
    }
}

impl PartitionStream for Paths {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
    fn execute(&self, ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let csr = self.csr.clone();
        let scope = self.scope.clone();
        let expansion = self.expansion.clone();
        let types = self.types.clone();
        let filters = self.filters.clone();
        let layout = self.layout.clone();
        let schema = self.schema.clone();
        let finishes = self.finishes.clone();
        let starts = if self.typed_starts.is_empty() {
            vec![(None, self.starts.clone())]
        } else {
            self.typed_starts
                .iter()
                .map(|(label, plan)| (Some(label.clone()), plan.clone()))
                .collect()
        };
        let max = self.max;
        let output_schema = schema.clone();
        let stream = async_stream::try_stream! {
            if types.is_empty() && expansion.min > 0 { return; }
            let execution = Execution::new(&csr.worker, &csr.job, csr.id, csr.generation.clone(),
                &scope, types, expansion.direction, csr.stats.clone());
            let keys = Keys::new(&execution.graph, &csr.cache).map_err(external)?;
            let mut emitter = Batches::new(output_schema.clone(), layout, &scope);
            let target_budget = execution.graph.budget.new_empty();
            let mut targets = BTreeSet::new();
            let restricted = !expansion.finishes.is_empty() && expansion.finishes.len() == finishes.len();
            if restricted {
                for (label, plan) in finishes {
                    let plan = datafusion::physical_plan::execution_plan::reset_plan_states(plan)?;
                    let mut rows = execute_stream(plan, ctx.clone())?;
                    let (_, converter) = &keys.nodes[&label];
                    while let Some(batch) = rows.try_next().await? {
                        for row in converter.convert_columns(batch.columns())?.iter() {
                            if targets.insert(vertex_id(&label,row.data())) { target_budget.try_grow(48)?; }
                        }
                    }
                }
                if targets.is_empty() { return; }
            }
            for (label, plan) in starts {
                let plan = datafusion::physical_plan::execution_plan::reset_plan_states(plan)?;
                let mut rows = execute_stream(plan, ctx.clone())?;
                while let Some(batch) = rows.try_next().await? {
                    let layout = label.as_ref().map(|label| Layout::Single(label.clone()));
                    for row in 0..batch.num_rows() {
                        let start = layout.as_ref().unwrap_or(&emitter.layout).start(&batch, row, &keys).map_err(external)?;
                        let mut paths = walks(&execution, start, &expansion, max, restricted.then_some(&targets), &filters);
                        while let Some(path) = paths.try_next().await.map_err(external)? {
                            if let Some(batch) = emitter.push(&keys, path).await.map_err(external)? { yield batch; }
                        }
                    }
                }
            }
            if let Some(batch) = emitter.flush(&keys).await.map_err(external)? { yield batch; }
        };
        Box::pin(RecordBatchStreamAdapter::new(schema, stream))
    }
}

fn external(error: anyhow::Error) -> DataFusionError {
    DataFusionError::External(error.into())
}

impl Layout {
    fn start(&self, batch: &RecordBatch, row: usize, keys: &Keys<'_>) -> Result<Vertex> {
        let at = |index| ScalarValue::try_from_array(batch.column(index), row);
        match self {
            Self::Mixed => anyhow::bail!("mixed CSR starts require a typed plan per label"),
            Self::Single(label) => keys.id(
                label,
                &(0..batch.num_columns())
                    .map(at)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            Self::Heterogeneous(left, right) => {
                let from = keys.nodes[left].0.fields().len();
                let second = matches!(at(0)?, ScalarValue::Int32(Some(1)));
                let (label, offset, count) = if second {
                    (right, 1 + from, keys.nodes[right].0.fields().len())
                } else {
                    (left, 1, from)
                };
                keys.id(
                    label,
                    &(offset..offset + count)
                        .map(at)
                        .collect::<Result<Vec<_>, _>>()?,
                )
            }
        }
    }

    fn vertex(
        &self,
        identity: &Identity,
        values: Vec<ScalarValue>,
        keys: &Keys<'_>,
    ) -> Result<Vec<ScalarValue>> {
        match self {
            Self::Mixed => tagged(&identity.label, &values),
            Self::Single(_) => Ok(values),
            Self::Heterogeneous(left, right) => {
                let second = identity.label == *right;
                let mut output = vec![ScalarValue::Int32(Some(i32::from(second)))];
                for (is_right, label) in [(false, left), (true, right)] {
                    if is_right == second {
                        output.extend(values.iter().cloned());
                    } else {
                        for field in keys.nodes[label].0.fields() {
                            output.push(ScalarValue::try_from(field.data_type())?);
                        }
                    }
                }
                Ok(output)
            }
        }
    }
}

fn tagged(label: &str, values: &[ScalarValue]) -> Result<Vec<ScalarValue>> {
    let values = values
        .iter()
        .map(|value| value.cast_to(&DataType::Utf8))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(vec![
        ScalarValue::Utf8(Some(label.to_owned())),
        ScalarValue::List(ScalarValue::new_list(&values, &DataType::Utf8, true)),
    ])
}

struct Keys<'a> {
    graph: &'a Graph<'a>,
    cache: &'a BlockCache,
    nodes: BTreeMap<String, (SchemaRef, RowConverter)>,
    edges: BTreeMap<String, RowConverter>,
    edge_ranges: Vec<(u64, &'a str, &'a [Block])>,
}

impl<'a> Keys<'a> {
    fn new(graph: &'a Graph<'a>, cache: &'a BlockCache) -> Result<Self> {
        let nodes = graph
            .manifest
            .schemas
            .iter()
            .map(|(label, encoded)| {
                let schema = decode_schema(encoded)?;
                let converter = graph_row_converter(&schema)?;
                Ok((label.clone(), (schema, converter)))
            })
            .collect::<Result<_>>()?;
        let edges = graph
            .manifest
            .edge_schemas
            .iter()
            .map(|(label, schema)| {
                Ok((
                    label.clone(),
                    graph_row_converter(decode_schema(schema)?.as_ref())?,
                ))
            })
            .collect::<Result<_>>()?;
        let mut edge_ranges = graph
            .manifest
            .edge_identities
            .iter()
            .filter_map(|(label, blocks)| {
                blocks
                    .last()
                    .map(|last| (last.last, label.as_str(), blocks.as_slice()))
            })
            .collect::<Vec<_>>();
        edge_ranges.sort_unstable_by_key(|(last, _, _)| *last);
        Ok(Self {
            graph,
            cache,
            nodes,
            edges,
            edge_ranges,
        })
    }
    fn id(&self, label: &str, key: &[ScalarValue]) -> Result<Vertex> {
        let (schema, converter) = self.nodes.get(label).context("unknown CSR vertex label")?;
        Ok(vertex_id(label, &graph_encode_key(key, schema, converter)?))
    }
    fn node(&self, data: &[u8], layout: &Layout) -> Result<Vec<ScalarValue>> {
        let (text, raw) = vertex_record(data)?;
        let identity: Identity = serde_json::from_slice(text)?;
        let values = decode_row(
            &self.nodes[&identity.label].1,
            &raw[..raw
                .len()
                .checked_sub(8)
                .context("invalid CSR vertex order")?],
        )?;
        layout.vertex(&identity, values, self)
    }
    fn edge(&self, label: Option<&str>, raw: &[u8], layout: &Layout) -> Result<Vec<ScalarValue>> {
        #[derive(Deserialize)]
        struct EdgeIdentity {
            identity: Identity,
        }
        let legacy;
        let (label, raw) = if let Some(label) = label {
            (label, raw)
        } else {
            let (text, raw) = vertex_record(raw)?;
            legacy = serde_json::from_slice::<EdgeIdentity>(text)?;
            (legacy.identity.label.as_str(), raw)
        };
        let values = decode_row(&self.edges[label], raw)?;
        if matches!(layout, Layout::Mixed) {
            tagged(label, &values)
        } else {
            Ok(values)
        }
    }

    async fn resolve(
        &self,
        ids: BTreeSet<u64>,
        layout: &Layout,
        edge: bool,
        budget: &MemoryReservation,
    ) -> Result<BTreeMap<u64, Vec<ScalarValue>>> {
        let mut blocks = BTreeMap::<Key, (&Block, Option<&str>, Vec<u64>)>::new();
        let prefix = &self.graph.prefix;
        for id in ids {
            let (directory, index, label) = if edge && !self.edge_ranges.is_empty() {
                let (_, label, blocks) = self
                    .edge_ranges
                    .get(self.edge_ranges.partition_point(|&(last, _, _)| last < id))
                    .context("missing CSR relationship label")?;
                (
                    prefix.clone().join("edge-identities").join(*label),
                    *blocks,
                    Some(*label),
                )
            } else if edge {
                (
                    prefix.clone().join("edges"),
                    self.graph.manifest.edge_keys.as_slice(),
                    None,
                )
            } else {
                let shard = partition(id, self.graph.manifest.partitions.len());
                (
                    prefix.clone().join(format!("v-{shard}")),
                    self.graph.manifest.partitions[shard].vertices.as_slice(),
                    None,
                )
            };
            let i = index.partition_point(|block| block.last < id);
            let block = index
                .get(i)
                .filter(|block| block.first <= id)
                .context("missing CSR identity block")?;
            blocks
                .entry(directory.join(i.to_string()))
                .or_insert_with(|| (block, label, Vec::new()))
                .2
                .push(id);
        }
        let mut output = BTreeMap::new();
        // Keep only the requested records. Prefetching whole blocks into the
        // cache first can evict them before the original row order uses them.
        let prefetch = (self.graph.scope.options.memory_bytes / (2 * BLOCK_BYTES)).clamp(1, 8);
        let mut groups = blocks.into_iter();
        loop {
            let mut reads = futures::stream::FuturesUnordered::new();
            for (key, (block, label, ids)) in groups.by_ref().take(prefetch) {
                reads.push(async move {
                    let data = self
                        .cache
                        .read(self.graph.objects, key, block, self.graph.scope)
                        .await?;
                    Ok::<_, anyhow::Error>((data, label, ids))
                });
            }
            if reads.is_empty() {
                break;
            }
            while let Some((data, label, ids)) = reads.try_next().await? {
                let mut records = records(&data.bytes);
                for id in ids {
                    let (_, raw) = records
                        .find(|(key, _)| *key >= id)
                        .filter(|(key, _)| *key == id)
                        .context("missing CSR identity")?;
                    let value = if edge {
                        self.edge(label, raw, layout)?
                    } else {
                        self.node(raw, layout)?
                    };
                    budget.try_grow(48 + value.iter().map(ScalarValue::size).sum::<usize>())?;
                    output.insert(id, value);
                }
            }
        }
        Ok(output)
    }
}

fn decode_row(converter: &RowConverter, data: &[u8]) -> Result<Vec<ScalarValue>> {
    Ok(converter
        .convert_rows([converter.parser().parse(data)])?
        .iter()
        .map(|array| ScalarValue::try_from_array(array, 0))
        .collect::<Result<_, _>>()?)
}

type Path = (Vec<Vertex>, Vec<u64>);

fn end_hops<'a>(
    expansion: &Expansion,
    mut edges: impl Iterator<Item = &'a crate::catalog::Edge>,
) -> usize {
    let Some(edge) = edges.next() else {
        return expansion.max;
    };
    let (left, right) = (&edge.from.node, &edge.to.node);
    if left == right {
        return expansion.max;
    }
    if expansion.direction != Direction::Undirected {
        if edges.all(|edge| edge.from.node == *left && edge.to.node == *right) {
            return expansion.max.min(1);
        }
    } else if edges.all(|edge| {
        (edge.from.node == *left && edge.to.node == *right)
            || (edge.from.node == *right && edge.to.node == *left)
    }) && expansion.finishes.len() == 1
        && let Some(finish) = expansion.finishes.keys().next()
        && [left, right].contains(&finish)
    {
        let start = match &expansion.mixed {
            Some(mixed) if mixed.starts.len() == 1 => mixed.starts.keys().next(),
            Some(_) => None,
            None => Some(&expansion.start_node),
        };
        if let Some(start) = start
            && [left, right].contains(&start)
            && expansion.max > 0
            && expansion.max % 2 != usize::from(start != finish)
        {
            return expansion.max - 1;
        }
    }
    expansion.max
}

struct Batches<'a> {
    schema: SchemaRef,
    layout: Layout,
    budget: MemoryReservation,
    scope: &'a QueryScope,
    paths: Vec<Path>,
    batch_size: usize,
    start: bool,
    finish: bool,
    track_nodes: bool,
    track_edges: bool,
}
impl<'a> Batches<'a> {
    fn new(schema: SchemaRef, layout: Layout, scope: &'a QueryScope) -> Self {
        let has = |name| schema.field_with_name(name).is_ok();
        let track_nodes = has(PATH_NODES);
        let track_edges = has(PATH);
        let start = schema
            .fields()
            .iter()
            .any(|field| field.name().starts_with("from_"));
        let finish = schema
            .fields()
            .iter()
            .any(|field| field.name().starts_with("to_"));
        Self {
            schema,
            layout,
            paths: Vec::new(),
            batch_size: scope.options.batch_size.min(128),
            start,
            finish,
            track_nodes,
            track_edges,
            scope,
            budget: MemoryConsumer::new("CSR Cypher path batch")
                .register(&scope.runtime.memory_pool),
        }
    }
    async fn push(&mut self, keys: &Keys<'_>, path: Path) -> Result<Option<RecordBatch>> {
        self.budget
            .try_grow((path.0.capacity() + path.1.capacity()) * size_of::<u64>())?;
        self.paths.push(path);
        if self.paths.len() >= self.batch_size || self.budget.size() >= 16 * 1024 {
            self.flush(keys).await
        } else {
            Ok(None)
        }
    }

    async fn flush(&mut self, keys: &Keys<'_>) -> Result<Option<RecordBatch>> {
        if self.paths.is_empty() {
            return Ok(None);
        }
        let budget = self.budget.new_empty();
        let mut vertices = BTreeSet::new();
        for (nodes, _) in &self.paths {
            if self.track_nodes {
                vertices.extend(nodes.iter().copied());
            } else {
                if self.start {
                    vertices.insert(nodes[0]);
                }
                if self.finish {
                    vertices.insert(*nodes.last().unwrap());
                }
            }
        }
        let vertices = keys.resolve(vertices, &self.layout, false, &budget).await?;
        let relationships = if self.track_edges {
            keys.resolve(
                self.paths
                    .iter()
                    .flat_map(|(_, edges)| edges.iter().copied())
                    .collect(),
                &self.layout,
                true,
                &budget,
            )
            .await?
        } else {
            BTreeMap::new()
        };
        let paths = std::mem::take(&mut self.paths);
        let arrays = self
            .schema
            .fields()
            .iter()
            .map(|field| -> Result<ArrayRef> {
                let ty = field.data_type();
                match field.name().as_str() {
                    HOPS => Ok(Arc::new(UInt32Array::from_iter_values(
                        paths.iter().map(|(_, edges)| edges.len() as u32),
                    ))),
                    PATH => path_array(
                        ty,
                        paths.iter().map(|(_, edges)| edges.len()),
                        paths.iter().flat_map(|(_, edges)| edges.iter()),
                        &relationships,
                    ),
                    PATH_NODES => path_array(
                        ty,
                        paths.iter().map(|(nodes, _)| nodes.len()),
                        paths.iter().flat_map(|(nodes, _)| nodes.iter()),
                        &vertices,
                    ),
                    name => {
                        let (side, index) =
                            name.split_once('_').context("invalid CSR endpoint field")?;
                        let index = index.parse::<usize>()?;
                        let values = paths.iter().map(|(nodes, _)| {
                            let id = if side == "from" {
                                &nodes[0]
                            } else {
                                nodes.last().unwrap()
                            };
                            vertices[id][index].clone()
                        });
                        Ok(identity_array(ty, values)?)
                    }
                }
            })
            .collect::<Result<Vec<_>>>()?;
        self.budget.try_grow(
            arrays
                .iter()
                .map(|array| array.get_array_memory_size())
                .sum(),
        )?;
        let rows =
            datafusion::arrow::array::RecordBatchOptions::new().with_row_count(Some(paths.len()));
        let batch = RecordBatch::try_new_with_options(self.schema.clone(), arrays, &rows)?;
        self.budget.free();
        self.batch_size = self.scope.options.batch_size.min(512);
        Ok(Some(batch))
    }
}

fn identity_array(
    ty: &DataType,
    values: impl Iterator<Item = ScalarValue>,
) -> datafusion::common::Result<ArrayRef> {
    let mut values = values.peekable();
    if values.peek().is_none() {
        return Ok(datafusion::arrow::array::new_empty_array(ty));
    }
    let array = ScalarValue::iter_to_array(values)?;
    if array.data_type() == ty {
        Ok(array)
    } else {
        Ok(datafusion::arrow::compute::cast(&array, ty)?)
    }
}

fn path_array<'a>(
    ty: &DataType,
    lengths: impl Iterator<Item = usize>,
    ids: impl Iterator<Item = &'a u64> + Clone,
    rows: &BTreeMap<u64, Vec<ScalarValue>>,
) -> Result<ArrayRef> {
    let DataType::List(item) = ty else {
        anyhow::bail!("invalid CSR path type");
    };
    let DataType::Struct(fields) = item.data_type() else {
        anyhow::bail!("invalid CSR entity type");
    };
    let columns = fields
        .iter()
        .enumerate()
        .map(|(i, field)| {
            identity_array(field.data_type(), ids.clone().map(|id| rows[id][i].clone()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let offsets = OffsetBuffer::from_lengths(lengths);
    let values = StructArray::try_new_with_length(
        fields.clone(),
        columns,
        None,
        offsets.last().copied().unwrap_or_default() as usize,
    )?;
    Ok(Arc::new(ListArray::try_new(
        item.clone(),
        offsets,
        Arc::new(values),
        None,
    )?))
}

fn walks<'a>(
    execution: &'a Execution<'a>,
    start: Vertex,
    expansion: &'a Expansion,
    max: usize,
    targets: Option<&'a BTreeSet<Vertex>>,
    filters: &'a [(u32, Vec<String>)],
) -> futures::stream::BoxStream<'a, Result<Path>> {
    Box::pin(async_stream::try_stream! {
        let graph = &execution.graph;
        if let Some(kind) = expansion.shortest {
            let mut depths = BTreeMap::from([(start, 0usize)]);
            let mut parents = BTreeMap::<Vertex, Vec<(Vertex,u64)>>::new();
            let budget = graph.budget.new_empty();
            let mut frontier = vec![start];
            let mut remaining = targets.map(BTreeSet::len);
            for depth in 0..=max {
                if frontier.is_empty() { break; }
                if depth >= expansion.min {
                    for &target in &frontier {
                        if targets.is_some_and(|targets| !targets.contains(&target)) { continue; }
                        if let Some(remaining) = &mut remaining { *remaining -= 1; }
                        let mut stack = vec![(target, 0)];
                        let mut edges = Vec::new();
                        while let Some((vertex, choice)) = stack.last_mut() {
                            graph.scope.check()?;
                            if *vertex == start {
                                yield (stack.iter().rev().map(|(v,_)| *v).collect(), edges.iter().rev().copied().collect());
                            } else if let Some(&(parent, edge)) = parents.get(vertex).and_then(|p| p.get(*choice)) {
                                *choice += 1;
                                stack.push((parent,0));
                                edges.push(edge);
                                continue;
                            }
                            stack.pop();
                            edges.pop();
                        }
                    }
                }
                if depth == max || remaining == Some(0) { break; }
                let mut next = Vec::new();
                let final_targets = if depth+1 == max { targets } else { None };
                for mut batch in frontier.chunks(128).map(|chunk| adjacency(execution, chunk, final_targets, filters)) {
                    while let Some((to,from,edge)) = batch.try_next().await? {
                        if depth+1 == max && targets.is_some_and(|targets| !targets.contains(&to)) {
                            continue;
                        }
                        if let std::collections::btree_map::Entry::Vacant(entry) = depths.entry(to) {
                            budget.try_grow(144)?;
                            entry.insert(depth+1);
                            next.push(to);
                        }
                        if depths[&to] == depth+1 {
                            let choices = parents.entry(to).or_default();
                            if choices.is_empty() || matches!(kind, Shortest::All) {
                                budget.try_grow(40)?;
                                choices.push((from,edge));
                            }
                        }
                    }
                }
                frontier = next;
            }
        } else {
            let root = [start];
            let mut nodes = vec![start];
            let mut edges = Vec::new();
            let mut stack = vec![adjacency(execution, &root, if max == 1 { targets } else { None }, filters)];
            if expansion.min == 0 && targets.is_none_or(|targets| targets.contains(&start)) { yield (nodes.clone(), Vec::new()); }
            if max == 0 { return; }
            while let Some(next) = stack.last_mut() {
                if let Some((to, _, edge)) = next.try_next().await? {
                    if edges.contains(&edge) { continue; }
                    nodes.push(to);
                    edges.push(edge);
                    if edges.len() >= expansion.min && targets.is_none_or(|targets| targets.contains(&to)) { yield (nodes.clone(), edges.clone()); }
                    if edges.len() < max {
                        let terminal = if edges.len()+1 == max { targets } else { None };
                        stack.push(adjacency(execution, &nodes[nodes.len()-1..], terminal, filters));
                    } else {
                        nodes.pop();
                        edges.pop();
                    }
                } else {
                    stack.pop();
                    nodes.pop();
                    edges.pop();
                }
            }
        }
    })
}

fn adjacency<'a>(
    execution: &'a Execution<'a>,
    vertices: &[Vertex],
    targets: Option<&'a BTreeSet<Vertex>>,
    filters: &'a [(u32, Vec<String>)],
) -> futures::stream::BoxStream<'a, Result<(Vertex, Vertex, u64)>> {
    let mut groups = BTreeMap::<usize, BTreeSet<Vertex>>::new();
    for &vertex in vertices {
        groups
            .entry(partition(vertex, execution.graph.manifest.partitions.len()))
            .or_default()
            .insert(vertex);
    }
    Box::pin(async_stream::try_stream! {
        for (shard, ids) in groups {
            if execution.graph.types.is_empty() { continue; }
            let blocks = &execution.graph.manifest.partitions[shard].arcs;
            let candidates = blocks.iter().enumerate().filter(|(_,block)| ids.range(block.first..=block.last).next().is_some())
                .map(|(i,_)|i).collect::<Vec<_>>();
            for group in candidates.chunk_by(|a,b| *a+1==*b) {
                for chunk in group.chunks(2) {
                    let remote_targets = targets.filter(|ids| ids.len() <= 256).cloned();
                    let round = execution.round(vec![(shard, Step::Arcs { frontier: ids.clone(), start_block: chunk[0],
                        end_block: chunk.last().unwrap()+1, targets: remote_targets, filters: filters.to_vec() })]).await?;
                    for output in round.outputs {
                        for link in output.links { yield link; }
                    }
                }
            }
        }
    })
}
