use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use datafusion::{
    arrow::{
        array::{Array, ArrayRef, RecordBatch, UInt64Array},
        datatypes::{DataType, Field, Schema},
        util::display::{ArrayFormatter, FormatOptions},
    },
    logical_expr::col,
};
use futures::TryStreamExt;

use super::*;
use crate::{
    Engine, ScalarValue,
    catalog::{ident, qualified},
    source::snapshot::Buffer,
    traversal::graph_row_converter,
};

struct Records {
    pending: Vec<u8>,
    blocks: Vec<Block>,
    first: u64,
    last: u64,
}

// Arc rows have a fixed width. Spill sorted runs and keep one current row per run
// during the merge, even when one source vertex owns most of a shard's edges.
struct ArcRuns {
    rows: Vec<ArcEntry>,
    runs: Vec<Arc<dyn datafusion::execution::spill_file::SpillFile>>,
    budget: datafusion::execution::memory_pool::MemoryReservation,
}

impl ArcRuns {
    fn new(scope: &QueryScope) -> Self {
        use datafusion::execution::memory_pool::MemoryConsumer;
        Self {
            rows: Vec::new(),
            runs: Vec::new(),
            budget: MemoryConsumer::new("CSR adjacency run").register(&scope.runtime.memory_pool),
        }
    }

    fn push(&mut self, arc: ArcEntry, scope: &QueryScope) -> Result<()> {
        self.budget.try_grow(std::mem::size_of::<ArcEntry>() * 2)?;
        self.rows.push(arc);
        if self.rows.len() == 8192 {
            self.spill(scope)?;
        }
        Ok(())
    }

    fn spill(&mut self, scope: &QueryScope) -> Result<()> {
        use std::io::Write;
        if self.rows.is_empty() {
            return Ok(());
        }
        scope.check()?;
        self.rows.sort_unstable_by_key(arc_order);
        let file = scope
            .runtime
            .disk_manager
            .create_tmp_file("CSR adjacency run")?;
        let mut writer = std::io::BufWriter::new(file.open_writer()?);
        for run in self.rows.chunk_by(|left, right| left.from == right.from) {
            writer.write_all(&run[0].from.to_le_bytes())?;
            writer.write_all(&(run.len() as u16).to_le_bytes())?;
            for arc in run {
                writer.write_all(&arc.to.to_le_bytes())?;
                writer.write_all(&arc.edge.to_le_bytes())?;
                writer.write_all(&arc.kind.to_le_bytes())?;
                writer.write_all(&[u8::from(arc.reverse)])?;
            }
        }
        writer
            .into_inner()
            .map_err(|err| err.into_error())?
            .finish()?;
        scope.reads.snapshot_spill(file.size().unwrap_or_default());
        self.runs.push(file);
        self.rows = Vec::new();
        self.budget.free();
        Ok(())
    }

    async fn publish(
        mut self,
        scope: &QueryScope,
        objects: &Objects,
        prefix: &Key,
        shard: usize,
        partition: &mut Partition,
    ) -> Result<()> {
        if self.runs.is_empty() {
            self.rows.sort_unstable_by_key(arc_order);
            for batch in self.rows.chunks((BLOCK_BYTES - 4) / 36) {
                arc_block(objects, prefix, shard, partition, batch).await?;
            }
            return Ok(());
        }
        self.spill(scope)?;
        let budget = self.budget.new_empty();
        budget.try_grow(self.runs.len() * (4096 + 128) + BLOCK_BYTES)?;
        let mut readers = self
            .runs
            .iter()
            .map(|file| {
                Ok(ArcRunReader {
                    input: std::io::BufReader::with_capacity(
                        4096,
                        std::fs::File::open(file.path().context("CSR adjacency spill path")?)?,
                    ),
                    from: 0,
                    remaining: 0,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut queue = std::collections::BinaryHeap::new();
        let insert = |queue: &mut std::collections::BinaryHeap<_>, index, arc: ArcEntry| {
            queue.push(std::cmp::Reverse((
                arc_order(&arc),
                index,
                arc.kind,
                arc.reverse,
            )));
        };
        for (index, reader) in readers.iter_mut().enumerate() {
            if let Some(arc) = reader.next()? {
                insert(&mut queue, index, arc);
            }
        }
        let mut batch = Vec::with_capacity((BLOCK_BYTES - 4) / 36);
        while let Some(std::cmp::Reverse(((from, to, edge), index, kind, reverse))) = queue.pop() {
            batch.push(ArcEntry {
                from,
                to,
                edge,
                kind,
                reverse,
            });
            if batch.len() == (BLOCK_BYTES - 4) / 36 {
                scope.check()?;
                arc_block(objects, prefix, shard, partition, &batch).await?;
                batch.clear();
            }
            if let Some(arc) = readers[index].next()? {
                insert(&mut queue, index, arc);
            }
        }
        arc_block(objects, prefix, shard, partition, &batch).await
    }
}

fn arc_order(arc: &ArcEntry) -> (u64, u64, u64) {
    (arc.from, arc.to, arc.edge)
}

struct ArcRunReader {
    input: std::io::BufReader<std::fs::File>,
    from: u64,
    remaining: u16,
}

impl ArcRunReader {
    fn next(&mut self) -> Result<Option<ArcEntry>> {
        use std::io::{BufRead, Read};
        if self.remaining == 0 {
            if self.input.fill_buf()?.is_empty() {
                return Ok(None);
            }
            let mut header = [0; 10];
            self.input.read_exact(&mut header)?;
            self.from = u64::from_le_bytes(header[..8].try_into()?);
            self.remaining = u16::from_le_bytes(header[8..].try_into()?);
            ensure!(self.remaining > 0, "empty CSR adjacency run");
        }
        let mut bytes = [0; 21];
        self.input.read_exact(&mut bytes)?;
        self.remaining -= 1;
        Ok(Some(ArcEntry {
            from: self.from,
            to: u64::from_le_bytes(bytes[0..8].try_into()?),
            edge: u64::from_le_bytes(bytes[8..16].try_into()?),
            kind: u32::from_le_bytes(bytes[16..20].try_into()?),
            reverse: bytes[20] != 0,
        }))
    }
}

impl Records {
    fn new() -> Self {
        Self {
            pending: Vec::new(),
            blocks: Vec::new(),
            first: 0,
            last: 0,
        }
    }

    async fn record(
        &mut self,
        objects: &Objects,
        prefix: &Key,
        id: u64,
        value: &[u8],
    ) -> Result<()> {
        if self.should_flush(value.len(), BLOCK_BYTES) {
            self.flush(objects, prefix).await?;
        }
        self.append(id, value)
    }

    fn should_flush(&self, size: usize, limit: usize) -> bool {
        !self.pending.is_empty() && self.pending.len() + size + 12 > limit
    }

    fn append(&mut self, id: u64, value: &[u8]) -> Result<()> {
        ensure!(
            value.len() + 12 <= BLOCK_BYTES,
            "CSR identity or edge properties exceed block size"
        );
        if self.pending.is_empty() {
            self.first = id;
        }
        self.last = id;
        self.pending.extend_from_slice(&id.to_le_bytes());
        self.pending
            .extend_from_slice(&(value.len() as u32).to_le_bytes());
        self.pending.extend_from_slice(value);
        Ok(())
    }

    fn queue(
        &mut self,
        id: u64,
        value: &[u8],
        limit: usize,
    ) -> Result<Option<(u64, u64, Vec<u8>)>> {
        let ready = self
            .should_flush(value.len(), limit)
            .then(|| (self.first, self.last, std::mem::take(&mut self.pending)));
        self.append(id, value)?;
        Ok(ready)
    }

    async fn write(
        &mut self,
        objects: &Objects,
        prefix: &Key,
        first: u64,
        last: u64,
        bytes: Vec<u8>,
    ) -> Result<()> {
        self.blocks.push(
            write_block_data(
                objects,
                &prefix.clone().join(self.blocks.len().to_string()),
                first,
                last,
                bytes,
                true,
            )
            .await?,
        );
        Ok(())
    }

    async fn flush(&mut self, objects: &Objects, prefix: &Key) -> Result<()> {
        if !self.pending.is_empty() {
            let pending = std::mem::take(&mut self.pending);
            self.write(objects, prefix, self.first, self.last, pending)
                .await?;
        }
        Ok(())
    }
}

// Each block is a CSR slice. A high-degree vertex can occupy several slices.
pub(super) fn encode_arcs(arcs: &[ArcEntry]) -> Vec<u8> {
    let mut rows = Vec::new();
    let mut adjacency = Vec::with_capacity(arcs.len() * 24);
    for (index, run) in arcs.chunk_by(|a, b| a.from == b.from).enumerate() {
        rows.extend_from_slice(&run[0].from.to_le_bytes());
        rows.extend_from_slice(&((adjacency.len() / 24) as u32).to_le_bytes());
        debug_assert_eq!(rows.len(), (index + 1) * 12);
        for entry in run {
            adjacency.extend_from_slice(&entry.to.to_le_bytes());
            adjacency.extend_from_slice(&entry.edge.to_le_bytes());
            adjacency.extend_from_slice(&entry.kind.to_le_bytes());
            adjacency.extend_from_slice(&u32::from(entry.reverse).to_le_bytes());
        }
    }
    let mut bytes = Vec::with_capacity(4 + rows.len() + adjacency.len());
    bytes.extend_from_slice(&((rows.len() / 12) as u32).to_le_bytes());
    bytes.extend_from_slice(&rows);
    bytes.extend_from_slice(&adjacency);
    bytes
}

fn text(columns: &[ArrayRef], row: usize) -> Result<Vec<String>> {
    columns
        .iter()
        .map(|array| {
            let value = ScalarValue::try_from_array(array, row)?;
            ensure!(!value.is_null(), "CSR business key cannot be null");
            Ok(value.to_string())
        })
        .collect()
}

fn edge_record(
    identity: &Identity,
    properties: &BTreeMap<&str, Option<String>>,
    buffer: &mut Vec<u8>,
) -> Result<()> {
    #[derive(Serialize)]
    struct EdgeRecord<'a> {
        identity: &'a Identity,
        properties: &'a BTreeMap<&'a str, Option<String>>,
    }
    buffer.clear();
    serde_json::to_writer(
        &mut *buffer,
        &EdgeRecord {
            identity,
            properties,
        },
    )?;
    buffer.push(b'\n');
    Ok(())
}

pub(super) async fn build(
    engine: &Engine,
    scope: &QueryScope,
    objects: &Objects,
    prefix: &Key,
    generation: Generation<'_>,
    partitions: usize,
) -> Result<Manifest> {
    let mut state = scope.session().state();
    super::super::distributed::Distributed::configure(&mut state, scope.options.memory_bytes);
    if scope.options.memory_bytes <= 64 * 1024 * 1024 {
        state.config_mut().options_mut().execution.batch_size =
            datafusion::config::ConfigNonZeroUsize::try_new(256)?;
        scope.runtime.disk_manager.set_max_spill_merge_fan_in(4);
    }
    let context = datafusion::prelude::SessionContext::new_with_state(state);
    crate::engine::register_graph_functions(&context);
    let prepared = engine.prepare_partitioned_topology(scope, &context).await?;
    let mut manifest = Manifest {
        format: 5,
        generation: generation.id,
        model_revision: generation.revision.into(),
        graph_options: scope.options.graph.clone(),
        sources: prepared
            .versions
            .iter()
            .map(|source| Version {
                source: source.source.clone(),
                version: source.version.clone(),
                consistency: source.consistency.to_owned(),
            })
            .collect(),
        schemas: BTreeMap::new(),
        edge_types: engine.catalog.edges.keys().cloned().collect(),
        partitions: (0..partitions).map(|_| Partition::default()).collect(),
        edge_keys: Vec::new(),
        edge_identities: BTreeMap::new(),
        edge_schemas: BTreeMap::new(),
        tables: BTreeMap::new(),
        table_schemas: BTreeMap::new(),
        filter_columns: BTreeMap::new(),
        vertices: 0,
        edges: 0,
        bytes: 0,
        build_ms: 0,
        reads: serde_json::Value::Null,
    };
    // Sort compact identity records instead of retaining a dictionary of all vertices.
    let mut vertex_files = (0..partitions).map(|_| Records::new()).collect::<Vec<_>>();
    let vertex_schema = Arc::new(Schema::new(vec![
        Field::new("vid", DataType::UInt64, false),
        Field::new("identity", DataType::Binary, false),
    ]));
    let mut vertex_buffer = Buffer::new(
        &scope.runtime.memory_pool,
        Some(&scope.runtime.disk_manager),
    );
    let mut ordinal = 0_u64;
    for (name, node) in &engine.catalog.nodes {
        let keys = node
            .key
            .iter()
            .map(|key| qualified("n", key))
            .collect::<Vec<_>>()
            .join(", ");
        let frame = context
            .sql(&format!(
                "SELECT {keys} FROM {} n ORDER BY {keys}",
                ident(name)
            ))
            .await?;
        write_snapshot(
            objects,
            prefix,
            name,
            &node.key,
            0,
            &[],
            &context,
            scope,
            &mut manifest,
        )
        .await?;
        let schema = frame.schema().inner();
        let converter = graph_row_converter(schema)?;
        manifest
            .schemas
            .insert(name.clone(), encode_schema(schema)?);
        let mut rows = frame.execute_stream().await?;
        while let Some(batch) = rows.try_next().await? {
            scope.check()?;
            use datafusion::arrow::array::BinaryBuilder;
            let keys = batch.columns();
            let mut identities = BinaryBuilder::new();
            let encoded = converter.convert_columns(keys)?;
            for row in 0..batch.num_rows() {
                let identity = Identity {
                    label: name.clone(),
                    key: text(keys, row)?,
                };
                let mut record = serde_json::to_vec(&identity)?;
                record.push(b'\n');
                record.extend_from_slice(encoded.row(row).data());
                record.extend_from_slice(&ordinal.to_le_bytes());
                ordinal += 1;
                identities.append_value(record);
            }
            vertex_buffer.push(RecordBatch::try_new(
                vertex_schema.clone(),
                vec![
                    Arc::new(UInt64Array::from_iter_values(
                        encoded.iter().map(|row| vertex_id(name, row.data())),
                    )),
                    Arc::new(identities.finish()),
                ],
            )?)?;
        }
    }
    let (table, reservation) = vertex_buffer.finish(vertex_schema, &scope.reads)?;
    let mut rows = context
        .read_table(table)?
        .sort(vec![col("vid").sort(true, false)])?
        .execute_stream()
        .await?;
    let mut previous = None;
    while let Some(batch) = rows.try_next().await? {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .context("invalid CSR vertex ID")?;
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::BinaryArray>()
            .context("invalid CSR vertex values")?;
        for (row, id) in ids.values().iter().copied().enumerate() {
            ensure!(
                !ids.is_null(row) && previous != Some(id),
                "duplicate node identity or CSR ID collision"
            );
            previous = Some(id);
            let shard = partition(id, partitions);
            vertex_files[shard]
                .record(
                    objects,
                    &prefix.clone().join(format!("v-{shard}")),
                    id,
                    values.value(row),
                )
                .await?;
            manifest.vertices += 1;
        }
    }
    drop(rows);
    drop(reservation);
    for (id, mut file) in vertex_files.into_iter().enumerate() {
        file.flush(objects, &prefix.clone().join(format!("v-{id}")))
            .await?;
        manifest.partitions[id].vertices = file.blocks;
    }

    let mut adjacency = (0..partitions)
        .map(|_| ArcRuns::new(scope))
        .collect::<Vec<_>>();
    let mut edges = Records::new();
    let edge_path = prefix.clone().join("edges");
    let edge_types = manifest.edge_types.clone();
    for (kind, name) in edge_types.iter().enumerate() {
        let edge = &engine.catalog.edges[name];
        let mut identities = Records::new();
        let identity_path = prefix.clone().join("edge-identities").join(name.as_str());
        let first_edge = manifest.edges;
        let keys = edge.identity();
        let (fields, mut rows) = snapshot_rows(name, &keys, &context, scope).await?;
        manifest
            .table_schemas
            .insert(name.clone(), encode_schema(&fields)?);
        let prop_fields = edge.properties.iter().filter(|key| !keys.contains(key));
        let projection = edge
            .from
            .columns
            .iter()
            .chain(&edge.to.columns)
            .chain(&keys)
            .chain(prop_fields)
            .map(|column| fields.index_of(column))
            .collect::<Result<Vec<_>, _>>()?;
        let from_width = edge.from.columns.len();
        let to_width = from_width + edge.to.columns.len();
        let key_width = to_width + keys.len();
        let from_encoder =
            graph_row_converter(decode_schema(&manifest.schemas[&edge.from.node])?.as_ref())?;
        let to_encoder =
            graph_row_converter(decode_schema(&manifest.schemas[&edge.to.node])?.as_ref())?;
        let key_schema = fields.project(&projection[to_width..key_width])?;
        let key_encoder = graph_row_converter(&key_schema)?;
        manifest
            .edge_schemas
            .insert(name.clone(), encode_schema(&key_schema)?);
        let projection = projection.iter().map(|i| i + 1).collect::<Vec<_>>();
        let mut prior = Vec::new();
        let mut record = Vec::new();
        let mut properties = BTreeMap::new();
        let prefix = prefix.clone().join("tables").join(name.as_str());
        let mut pages = crate::catalog::columns([&edge.properties, &keys])
            .into_iter()
            .filter(|name| {
                fields.field_with_name(name).is_ok_and(|field| {
                    let ty = field.data_type();
                    ty.is_numeric()
                        || ty.is_string()
                        || ty.is_temporal()
                        || *ty == DataType::Boolean
                })
            })
            .collect::<Vec<_>>();
        if !pages.is_empty() {
            pages.push(LOOKUP_ID.to_owned());
            manifest.filter_columns.insert(name.clone(), pages.clone());
        }
        let mut snapshot = SnapshotWriter::new(prefix, rows.schema(), first_edge, &pages, scope)?;
        while let Some(batch) = rows
            .try_next()
            .await
            .with_context(|| format!("sorting CSR properties for {name}"))?
        {
            scope.check()?;
            snapshot.push(objects, &batch).await?;
            let batch = batch.project(&projection)?;
            let columns = batch.columns();
            let from = from_encoder.convert_columns(&columns[..from_width])?;
            let to = to_encoder.convert_columns(&columns[from_width..to_width])?;
            let ids = key_encoder.convert_columns(&columns[to_width..key_width])?;
            let (ready, identity_blocks) = {
                let format = FormatOptions::default();
                let formatters = edge
                    .properties
                    .iter()
                    .filter(|prop| !keys.contains(prop))
                    .zip(&columns[key_width..])
                    .map(|(name, col)| {
                        Ok((
                            name.as_str(),
                            col,
                            ArrayFormatter::try_new(col.as_ref(), &format)?,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let mut ready = Vec::new();
                let mut identity_blocks = Vec::new();
                for (row, ((from, to), key)) in
                    from.iter().zip(to.iter()).zip(ids.iter()).enumerate()
                {
                    ensure!(
                        prior.as_slice() != key.data(),
                        "duplicate relationship identity in {name}"
                    );
                    prior.clear();
                    prior.extend_from_slice(key.data());
                    let from = vertex_id(&edge.from.node, from.data());
                    let to = vertex_id(&edge.to.node, to.data());
                    let id = manifest.edges;
                    manifest.edges += 1;
                    let identity = Identity {
                        label: name.clone(),
                        key: if edge.key.is_empty() {
                            // Preserve the explicit CSR API's original numeric key order.
                            let row_ids = columns[to_width]
                                .as_any()
                                .downcast_ref::<UInt64Array>()
                                .context("invalid CSR edge row IDs")?;
                            vec![(first_edge + row_ids.value(row) + 1).to_string()]
                        } else {
                            text(&columns[to_width..key_width], row)?
                        },
                    };
                    for (name, col, formatter) in &formatters {
                        properties.insert(
                            *name,
                            (!col.is_null(row)).then(|| formatter.value(row).to_string()),
                        );
                    }
                    edge_record(&identity, &properties, &mut record)?;
                    record.extend_from_slice(key.data());
                    if let Some(block) = edges.queue(id, &record, BLOCK_BYTES)? {
                        ready.push(block);
                    }
                    if let Some(block) = identities.queue(id, key.data(), 32 * 1024)? {
                        identity_blocks.push(block);
                    }
                    adjacency[partition(from, partitions)].push(
                        ArcEntry {
                            from,
                            to,
                            edge: id,
                            kind: kind as u32,
                            reverse: false,
                        },
                        scope,
                    )?;
                    adjacency[partition(to, partitions)].push(
                        ArcEntry {
                            from: to,
                            to: from,
                            edge: id,
                            kind: kind as u32,
                            reverse: true,
                        },
                        scope,
                    )?;
                }
                (ready, identity_blocks)
            };
            for (first, last, bytes) in ready {
                edges.write(objects, &edge_path, first, last, bytes).await?;
            }
            for (first, last, bytes) in identity_blocks {
                identities
                    .write(objects, &identity_path, first, last, bytes)
                    .await?;
            }
        }
        identities.flush(objects, &identity_path).await?;
        manifest
            .edge_identities
            .insert(name.clone(), identities.blocks);
        manifest
            .tables
            .insert(name.clone(), snapshot.finish(objects, scope).await?);
    }
    edges.flush(objects, &edge_path).await?;
    manifest.edge_keys = edges.blocks;
    for (shard, runs) in adjacency.into_iter().enumerate() {
        runs.publish(
            scope,
            objects,
            prefix,
            shard,
            &mut manifest.partitions[shard],
        )
        .await?;
    }
    manifest.bytes = manifest
        .edge_keys
        .iter()
        .chain(manifest.edge_identities.values().flatten())
        .chain(manifest.tables.values().flatten())
        .chain(
            manifest
                .partitions
                .iter()
                .flat_map(|p| p.vertices.iter().chain(&p.arcs)),
        )
        .map(|b| {
            b.bytes
                + b.columns
                    .iter()
                    .map(|col| u64::from(col.bytes))
                    .sum::<u64>()
        })
        .sum();
    manifest.build_ms = scope.elapsed_ms();
    manifest.reads = serde_json::to_value(scope.reads.metrics())?;
    let bytes = serde_json::to_vec(&manifest)?;
    ensure!(
        bytes.len() as u64 <= MANIFEST_LIMIT,
        "CSR manifest exceeds size limit"
    );
    objects
        .put_bytes(&prefix.clone().join("manifest.json"), bytes)
        .await?;
    Ok(manifest)
}

async fn arc_block(
    objects: &Objects,
    prefix: &Key,
    shard: usize,
    partition: &mut Partition,
    items: &[ArcEntry],
) -> Result<()> {
    if let (Some(first), Some(last)) = (items.first(), items.last()) {
        let key = prefix
            .clone()
            .join(format!("a-{shard}"))
            .join(partition.arcs.len().to_string());
        partition
            .arcs
            .push(write_block(objects, &key, first.from, last.from, encode_arcs(items)).await?);
    }
    Ok(())
}

fn lookup_id(label: &str, alias: &str, key: &[String]) -> String {
    crate::cypher::element_id(
        &crate::cypher::string(label),
        &crate::traversal::text_key(alias, key),
    )
}

async fn snapshot_rows(
    label: &str,
    keys: &[String],
    context: &datafusion::prelude::SessionContext,
    scope: &QueryScope,
) -> Result<(
    Arc<Schema>,
    datafusion::execution::SendableRecordBatchStream,
)> {
    let visible = context
        .table(datafusion::common::TableReference::bare(label))
        .await?;
    let schema = visible.schema().inner().clone();
    let mut source = context
        .sql(&format!(
            "SELECT {} AS {LOOKUP_ID}, src.* FROM {} AS src",
            lookup_id(label, "src", keys),
            ident(label),
        ))
        .await?
        .execute_stream()
        .await?;
    let sorted_schema = source.schema();
    // The data written to the sorter can share large source allocations. Stage
    // once, with compression, so joins and the merge never hold memory together.
    let mut input = Buffer::new_compressed(&scope.runtime.memory_pool, &scope.runtime.disk_manager);
    while let Some(batch) = source
        .try_next()
        .await
        .with_context(|| format!("reading CSR properties for {label}"))?
    {
        scope.check()?;
        input.push(batch)?;
    }
    drop(source);
    let (table, reservation) = input.finish(sorted_schema, &scope.reads)?;
    let mut state = context.state();
    state.config_mut().options_mut().execution.target_partitions = 1;
    let sorter = datafusion::prelude::SessionContext::new_with_state(state);
    let mut rows = sorter
        .read_table(table)?
        .sort(vec![col(LOOKUP_ID).sort(true, false)])?
        .execute_stream()
        .await?;
    let schema_ref = rows.schema();
    let stream = async_stream::try_stream! {
        let _reservation = reservation;
        while let Some(batch) = rows.try_next().await? { yield batch; }
    };
    let rows = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(schema_ref, stream);
    Ok((schema, Box::pin(rows)))
}

async fn write_snapshot(
    objects: &Objects,
    prefix: &Key,
    label: &str,
    keys: &[String],
    first: u64,
    statistics: &[String],
    context: &datafusion::prelude::SessionContext,
    scope: &QueryScope,
    manifest: &mut Manifest,
) -> Result<()> {
    let (schema, mut rows) = snapshot_rows(label, keys, context, scope).await?;
    manifest
        .table_schemas
        .insert(label.to_owned(), encode_schema(&schema)?);
    let mut snapshot = SnapshotWriter::new(
        prefix.clone().join("tables").join(label),
        rows.schema(),
        first,
        statistics,
        scope,
    )?;
    while let Some(batch) = rows.try_next().await? {
        scope.check()?;
        snapshot.push(objects, &batch).await?;
    }
    manifest
        .tables
        .insert(label.to_owned(), snapshot.finish(objects, scope).await?);
    Ok(())
}

struct SnapshotWriter {
    prefix: Key,
    schema: Arc<Schema>,
    writer: StreamWriter<Vec<u8>>,
    blocks: Vec<Block>,
    first: u64,
    end: u64,
    ids: Option<[String; 2]>,
    numeric: Vec<usize>,
    numbers: bounds::Ranges,
    filter_columns: Vec<usize>,
    column_batches: Vec<RecordBatch>,
    column_memory: datafusion::execution::memory_pool::MemoryReservation,
    filter_prefix: Key,
    filter_file: Option<(
        Arc<dyn datafusion::execution::spill_file::SpillFile>,
        Box<dyn datafusion::execution::spill_file::SpillWriter>,
    )>,
}

impl SnapshotWriter {
    fn new(
        prefix: Key,
        schema: Arc<Schema>,
        first: u64,
        statistics: &[String],
        scope: &QueryScope,
    ) -> Result<Self> {
        let numeric = statistics
            .iter()
            .filter_map(|name| schema.index_of(name).ok())
            .filter(|&i| bounds::supported(schema.field(i).data_type()))
            .collect();
        let filter_columns = statistics
            .iter()
            .filter_map(|name| schema.index_of(name).ok())
            .collect::<Vec<_>>();
        let filter_file = if filter_columns.is_empty() {
            None
        } else {
            let file = scope
                .runtime
                .disk_manager
                .create_tmp_file("CSR filter columns")?;
            let writer = file.open_writer()?;
            Some((file, writer))
        };
        Ok(Self {
            filter_prefix: prefix.clone().join("filter-columns"),
            prefix,
            writer: Self::writer(&schema)?,
            schema,
            blocks: Vec::new(),
            first,
            end: first,
            ids: None,
            numeric,
            numbers: BTreeMap::new(),
            filter_columns,
            column_batches: Vec::new(),
            column_memory: datafusion::execution::memory_pool::MemoryConsumer::new(
                "CSR sort column encoding",
            )
            .register(&scope.runtime.memory_pool),
            filter_file,
        })
    }

    fn writer(schema: &Schema) -> Result<StreamWriter<Vec<u8>>> {
        use datafusion::arrow::ipc::{CompressionType, writer::IpcWriteOptions};
        let options =
            IpcWriteOptions::default().try_with_compression(Some(CompressionType::ZSTD))?;
        Ok(StreamWriter::try_new_with_options(
            Vec::new(),
            schema,
            options,
        )?)
    }

    async fn push(&mut self, objects: &Objects, batch: &RecordBatch) -> Result<()> {
        use datafusion::arrow::array::StringArray;
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .context("invalid CSR lookup IDs")?;
        for offset in (0..batch.num_rows()).step_by(128) {
            let count = (batch.num_rows() - offset).min(128);
            let slice = batch.slice(offset, count);
            let range = self
                .ids
                .get_or_insert_with(|| [ids.value(offset).to_owned(), String::new()]);
            range[1] = ids.value(offset + count - 1).to_owned();
            self.end += count as u64;
            bounds::merge(&mut self.numbers, &slice, &self.numeric)?;
            self.writer.write(&slice)?;
            if !self.filter_columns.is_empty() {
                // Slices can retain the entire sorted input batch, especially for string views.
                let indices =
                    datafusion::arrow::array::UInt32Array::from_iter_values(0..count as u32);
                let projection = slice.project(&self.filter_columns)?;
                let arrays = projection
                    .columns()
                    .iter()
                    .map(|array| {
                        Ok(super::compact_view(datafusion::arrow::compute::take(
                            array, &indices, None,
                        )?))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let compact = RecordBatch::try_new(projection.schema(), arrays)?;
                self.column_memory
                    .try_grow(compact.get_array_memory_size())?;
                self.column_batches.push(compact);
            }
            if self.writer.get_ref().len() >= BLOCK_BYTES || self.column_memory.size() >= 768 * 1024
            {
                self.flush(objects).await?;
            }
        }
        Ok(())
    }

    async fn flush(&mut self, objects: &Objects) -> Result<()> {
        if let Some(ids) = self.ids.take() {
            let bytes =
                std::mem::replace(&mut self.writer, Self::writer(&self.schema)?).into_inner()?;
            ensure!(
                bytes.len() <= 2 * BLOCK_BYTES,
                "CSR property batch exceeds block size"
            );
            let mut block = write_block(
                objects,
                &self.prefix.clone().join(self.blocks.len().to_string()),
                self.first,
                self.end - 1,
                bytes,
            )
            .await?;
            block.ids = Some(ids);
            block.numbers = bounds::finish(&mut self.numbers);
            if let Some((_, file)) = &mut self.filter_file {
                let first = self
                    .column_batches
                    .first()
                    .context("missing CSR filter rows")?;
                let combined = datafusion::arrow::compute::concat_batches(
                    &first.schema(),
                    &self.column_batches,
                )?;
                self.column_memory
                    .try_grow(combined.get_array_memory_size())?;
                for index in 0..self.filter_columns.len() {
                    let column = combined.project(&[index])?;
                    let mut writer = Self::writer(&column.schema())?;
                    writer.write(&column)?;
                    let bytes = writer.into_inner()?;
                    use std::io::Write;
                    file.write_all(&bytes)?;
                    block.columns.push(ColumnPage {
                        bytes: bytes.len().try_into()?,
                        sha256: Sha256::digest(&bytes).into(),
                    });
                }
                self.column_batches.clear();
            }
            self.column_memory.free();
            self.blocks.push(block);
            self.first = self.end;
        }
        Ok(())
    }

    async fn finish(mut self, objects: &Objects, scope: &QueryScope) -> Result<Vec<Block>> {
        self.flush(objects).await?;
        if let Some((file, mut writer)) = self.filter_file {
            writer.finish()?;
            let size = file.size().context("CSR filter column file size")?;
            if size > 0 {
                let budget = datafusion::execution::memory_pool::MemoryConsumer::new(
                    "CSR filter column upload",
                )
                .register(&scope.runtime.memory_pool);
                budget.try_grow(size.min(5 * 1024 * 1024) as usize)?;
                objects
                    .put_file(
                        &self.filter_prefix,
                        file.path().context("CSR filter column file")?,
                        size,
                    )
                    .await?;
                scope.reads.snapshot_spill(size);
            }
        }
        Ok(self.blocks)
    }
}
