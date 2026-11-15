mod sort_columns;
use sort_columns::OrderProjection;

use super::*;
use datafusion::{
    arrow::{
        array::{Array, BooleanArray, ListArray, StructArray, UInt32Array},
        buffer::OffsetBuffer,
        compute::{cast, take},
        ipc::reader::StreamReader,
    },
    common::{DFSchema, DataFusionError},
    logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
        async_udf::{AsyncScalarUDF, AsyncScalarUDFImpl},
    },
    prelude::SessionContext,
};

pub(super) struct Read<'a> {
    pub(super) objects: &'a Objects,
    pub(super) prefix: Key,
    pub(super) manifest: &'a Manifest,
    pub(super) scope: &'a QueryScope,
    pub(super) cache: Option<&'a BlockCache>,
}

type PropertyReader = (
    Box<dyn Iterator<Item = Result<RecordBatch, datafusion::arrow::error::ArrowError>> + Send>,
    MemoryReservation,
    Vec<Arc<CachedBlock>>,
);

#[derive(Default)]
pub(super) struct Filtered {
    pub(super) accepted: BTreeSet<u64>,
    pub(super) skipped: u64,
    pub(super) column_pages: u64,
}

impl Read<'_> {
    pub(super) async fn filter_edges(
        &self,
        candidates: &[ArcEntry],
        filters: &[(u32, Vec<String>)],
        budget: &MemoryReservation,
    ) -> Result<Filtered> {
        ensure!(
            self.manifest.format >= 4,
            "CSR edge filtering requires indexed properties"
        );
        let state = self.scope.graph_session().state();
        let mut result = Filtered::default();
        for (kind, terms) in filters {
            let kind = *kind;
            let label = self
                .manifest
                .edge_types
                .get(kind as usize)
                .context("unknown CSR edge type")?;
            let blocks = self
                .manifest
                .tables
                .get(label)
                .context("missing CSR edge property index")?;
            let mut ids = candidates
                .iter()
                .filter(|arc| arc.kind == kind)
                .map(|arc| arc.edge)
                .collect::<Vec<_>>();
            ids.sort_unstable();
            ids.dedup();
            if ids.is_empty() {
                continue;
            }
            let schema = decode_schema(
                self.manifest
                    .table_schemas
                    .get(label)
                    .context("missing CSR edge property schema")?,
            )?;
            let sql = terms.join(" AND ");
            let df_schema = DFSchema::try_from_qualified_schema("src", &schema)?;
            let logical = state.create_logical_expr(&sql, &df_schema)?;
            if bounds::impossible(&logical, &df_schema) {
                continue;
            }
            let mut equalities = Vec::new();
            bounds::equalities(&logical, &mut equalities);
            let references = logical.column_refs();
            if references.is_empty() {
                let empty = Arc::new(Schema::empty());
                let df_schema = DFSchema::try_from_qualified_schema("src", &empty)?;
                let expr = state.create_physical_expr(logical, &df_schema)?;
                let one_row =
                    datafusion::arrow::array::RecordBatchOptions::new().with_row_count(Some(1));
                let batch = RecordBatch::try_new_with_options(empty, vec![], &one_row)?;
                let value = expr.evaluate(&batch)?.into_array(1)?;
                let matches = value
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .context("CSR edge filter must return boolean")?;
                if matches.is_valid(0) && matches.value(0) {
                    budget.try_grow(ids.len().saturating_mul(32))?;
                    result.accepted.extend(ids);
                }
                continue;
            }
            let projection = schema
                .fields()
                .iter()
                .enumerate()
                .filter_map(|(i, field)| {
                    references
                        .iter()
                        .any(|column| column.name == *field.name())
                        .then_some(i)
                })
                .collect::<Vec<_>>();
            let projected = Arc::new(schema.project(&projection)?);
            let df_schema = DFSchema::try_from_qualified_schema("src", &projected)?;
            let expr = state.create_logical_expr(&sql, &df_schema)?;
            let expr = state.create_physical_expr(expr, &df_schema)?;
            let projection = projection
                .into_iter()
                .map(|index| index + 1)
                .collect::<Vec<_>>();
            let pages = self.manifest.filter_columns.get(label).and_then(|columns| {
                projected
                    .fields()
                    .iter()
                    .map(|field| columns.iter().position(|name| name == field.name()))
                    .collect::<Option<Vec<_>>>()
            });
            if let Some(pages) =
                pages.filter(|_| blocks.iter().all(|block| !block.columns.is_empty()))
            {
                let filtered = self
                    .filter_pages(
                        label,
                        blocks,
                        &ids,
                        &equalities,
                        &pages,
                        projected,
                        &expr,
                        budget,
                    )
                    .await?;
                result.accepted.extend(filtered.accepted);
                result.skipped += filtered.skipped;
                result.column_pages += filtered.column_pages;
                continue;
            }
            let mut cursor = 0;
            while cursor < ids.len() {
                let id = ids[cursor];
                let index = blocks.partition_point(|block| block.last < id);
                let block = blocks
                    .get(index)
                    .filter(|block| block.first <= id)
                    .context("missing CSR edge properties")?;
                if bounds::excludes(block, &equalities)? {
                    result.skipped += 1;
                    cursor += ids[cursor..].partition_point(|&id| id <= block.last);
                    continue;
                }
                let (reader, reservation, _data) = self
                    .block(label, index, block, Some(projection.clone()))
                    .await?;
                let mut first = block.first;
                for batch in reader {
                    let batch = batch?;
                    reservation.try_resize(batch.get_array_memory_size())?;
                    let next = first + batch.num_rows() as u64;
                    let end = cursor + ids[cursor..].partition_point(|&id| id < next);
                    if end > cursor {
                        let mut input = batch;
                        if end - cursor < input.num_rows() {
                            let offsets = UInt32Array::from_iter_values(
                                ids[cursor..end].iter().map(|id| (id - first) as u32),
                            );
                            let columns = input
                                .columns()
                                .iter()
                                .map(|column| take(column, &offsets, None))
                                .collect::<Result<Vec<_>, _>>()?;
                            input = RecordBatch::try_new(input.schema(), columns)?;
                        }
                        let evaluation = reservation.new_empty();
                        evaluation.try_grow(input.get_array_memory_size())?;
                        let value = expr.evaluate(&input)?.into_array(input.num_rows())?;
                        let matches = value
                            .as_any()
                            .downcast_ref::<BooleanArray>()
                            .context("CSR edge filter must return boolean")?;
                        for (row, &id) in ids[cursor..end].iter().enumerate() {
                            if matches.is_valid(row) && matches.value(row) {
                                budget.try_grow(32)?;
                                result.accepted.insert(id);
                            }
                        }
                        cursor = end;
                    }
                    first = next;
                    if cursor == ids.len() || ids[cursor] > block.last {
                        break;
                    }
                }
                ensure!(
                    cursor == ids.len() || ids[cursor] > block.last,
                    "incomplete CSR edge property block"
                );
            }
        }
        Ok(result)
    }

    async fn filter_pages(
        &self,
        label: &str,
        blocks: &[Block],
        ids: &[u64],
        equalities: &[(&str, bigdecimal::BigDecimal)],
        columns: &[usize],
        schema: Arc<Schema>,
        expr: &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
        budget: &MemoryReservation,
    ) -> Result<Filtered> {
        let key = self
            .prefix
            .clone()
            .join("tables")
            .join(label)
            .join("filter-columns");
        let mut result = Filtered::default();
        let mut cursor = 0;
        let mut offset = 0;
        let mut groups = Vec::new();
        let mut ranges = Vec::new();
        let mut bytes = 0;
        for block in blocks {
            if cursor == ids.len() {
                break;
            }
            let start = offset;
            offset += block
                .columns
                .iter()
                .map(|page| u64::from(page.bytes))
                .sum::<u64>();
            if ids[cursor] > block.last {
                continue;
            }
            ensure!(ids[cursor] >= block.first, "missing CSR edge properties");
            let end = cursor + ids[cursor..].partition_point(|&id| id <= block.last);
            if bounds::excludes(block, equalities)? {
                result.skipped += 1;
            } else {
                let mut offsets = vec![start];
                offsets.extend(block.columns.iter().scan(start, |offset, page| {
                    *offset += u64::from(page.bytes);
                    Some(*offset)
                }));
                let pages = columns
                    .iter()
                    .map(|&i| {
                        let page = block.columns.get(i).context("missing CSR filter column")?;
                        Ok((offsets[i]..offsets[i + 1], page))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let next_bytes: usize = pages.iter().map(|(_, page)| page.bytes as usize).sum();
                if !ranges.is_empty()
                    && (groups.len() >= 16
                        || bytes + next_bytes > self.scope.options.memory_bytes / 16)
                {
                    self.evaluate_pages(&key, &ranges, &groups, &schema, expr, budget, &mut result)
                        .await?;
                    groups.clear();
                    ranges.clear();
                    bytes = 0;
                }
                ranges.extend(pages.iter().map(|(range, _)| range.clone()));
                groups.push((
                    block.first,
                    &ids[cursor..end],
                    pages.into_iter().map(|(_, page)| page).collect::<Vec<_>>(),
                ));
                bytes += next_bytes;
            }
            cursor = end;
        }
        ensure!(cursor == ids.len(), "incomplete CSR filter index");
        if !ranges.is_empty() {
            self.evaluate_pages(&key, &ranges, &groups, &schema, expr, budget, &mut result)
                .await?;
        }
        Ok(result)
    }

    async fn evaluate_pages(
        &self,
        key: &Key,
        ranges: &[std::ops::Range<u64>],
        groups: &[(u64, &[u64], Vec<&ColumnPage>)],
        schema: &Arc<Schema>,
        expr: &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
        budget: &MemoryReservation,
        result: &mut Filtered,
    ) -> Result<()> {
        self.scope.check()?;
        let reads = budget.new_empty();
        let bytes = ranges.iter().map(|r| r.end - r.start).sum::<u64>();
        reads.try_grow(bytes as usize)?;
        let mut buffers = self.objects.get_ranges(key, ranges).await?.into_iter();
        self.scope.reads.object_bytes(bytes as usize);
        for _ in ranges {
            self.scope.reads.request();
        }
        for &(first, ids, ref pages) in groups {
            let mut columns = Vec::new();
            let evaluation = budget.new_empty();
            for page in pages {
                let bytes = buffers
                    .next()
                    .context("missing CSR filter column response")?;
                ensure!(
                    bytes.len() == page.bytes as usize
                        && Sha256::digest(&bytes).as_slice() == page.sha256,
                    "CSR filter column checksum mismatch"
                );
                columns.push(select_column(bytes, first, ids, &evaluation)?);
            }
            let batch = RecordBatch::try_new(schema.clone(), columns)?;
            let matched = expr.evaluate(&batch)?.into_array(batch.num_rows())?;
            let matched = matched
                .as_any()
                .downcast_ref::<BooleanArray>()
                .context("CSR edge filter must return boolean")?;
            for (&id, matched) in ids.iter().zip(matched.iter()) {
                if matched == Some(true) {
                    budget.try_grow(32)?;
                    result.accepted.insert(id);
                }
            }
            result.column_pages += pages.len() as u64;
        }
        Ok(())
    }

    async fn block(
        &self,
        label: &str,
        index: usize,
        block: &Block,
        projection: Option<Vec<usize>>,
    ) -> Result<(
        StreamReader<std::io::Cursor<bytes::Bytes>>,
        MemoryReservation,
        Arc<CachedBlock>,
    )> {
        let budget = MemoryConsumer::new("CSR indexed property block")
            .register(&self.scope.runtime.memory_pool);
        let key = self
            .prefix
            .clone()
            .join("tables")
            .join(label)
            .join(index.to_string());
        let data = if let Some(cache) = self.cache {
            cache.read(self.objects, key, block, self.scope).await?
        } else {
            let compressed = budget.new_empty();
            compressed.try_grow(block.bytes as usize)?;
            let data = read_block(self.objects, &key, block, self.scope).await?;
            Arc::new(CachedBlock {
                bytes: data,
                _memory: compressed,
                compress_spill: false,
            })
        };
        Ok((
            StreamReader::try_new(std::io::Cursor::new(data.bytes.clone()), projection)?,
            budget,
            data,
        ))
    }

    // Select by the same escaped identity used by Cypher, including heterogeneous keys.
    pub(super) async fn identities(
        &self,
        ids: &BTreeSet<String>,
        projection: Option<&DataType>,
        order_only: bool,
        mut record_column_read: impl FnMut(u64),
        mut visit: impl FnMut(&str, &str, &RecordBatch, usize) -> Result<()>,
    ) -> Result<()> {
        let ids = ids.iter().collect::<Vec<_>>();
        for ids in ids.chunk_by(|left, right| id_label(left) == id_label(right)) {
            let label = unescape(id_label(ids[0]));
            let label = label.as_str();
            let Some(blocks) = self.manifest.tables.get(label) else {
                continue;
            };
            let selected = projection
                .map(|ty| self.order_projection(label, ty))
                .transpose()?;
            let divisor = if order_only { 2 } else { 4 } * 1024 * 1024;
            let prefetch = (self.scope.options.memory_bytes / divisor).clamp(1, 16);
            let mut remaining = ids;
            while !remaining.is_empty() {
                let mut reads = Vec::new();
                while let Some(id) = remaining.first() {
                    let index = blocks.partition_point(|block| {
                        block.ids.as_ref().is_some_and(|range| *id > &range[1])
                    });
                    let Some(block) = blocks.get(index) else {
                        remaining = &[];
                        break;
                    };
                    let [first, last] = block.ids.as_ref().context("missing CSR index range")?;
                    let start = remaining.partition_point(|id| *id < first);
                    let end = remaining.partition_point(|id| *id <= last);
                    let group = &remaining[start..end];
                    remaining = &remaining[end..];
                    if !group.is_empty() {
                        reads.push(self.read_group(
                            label,
                            index,
                            block,
                            group,
                            selected.as_ref(),
                            order_only,
                        ));
                        if reads.len() == prefetch {
                            break;
                        }
                    }
                }
                let mut reads: futures::stream::FuturesOrdered<_> = reads.into_iter().collect();
                while let Some((read, ids, sort_bytes)) = reads.try_next().await? {
                    record_column_read(sort_bytes);
                    let mut cursor = 0;
                    let (mut reader, reservation, _blocks) = read;
                    for batch in &mut reader {
                        let batch = batch?;
                        reservation.try_resize(batch.get_array_memory_size())?;
                        let column = batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .context("missing CSR lookup IDs")?;
                        if column.is_empty() {
                            continue;
                        }
                        let end = cursor
                            + ids[cursor..].partition_point(|id| {
                                id.as_str() <= column.value(column.len() - 1)
                            });
                        for id in &ids[cursor..end] {
                            let row = lower_bound(column.len(), |i| column.value(i) < id.as_str());
                            if row < column.len() && column.value(row) == id.as_str() {
                                visit(label, id, &batch, row)?;
                            }
                        }
                        cursor = end;
                        if cursor == ids.len() {
                            break;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn read_group<'a>(
        &self,
        label: &str,
        index: usize,
        block: &Block,
        ids: &'a [&'a String],
        selected: Option<&OrderProjection>,
        order_only: bool,
    ) -> Result<(PropertyReader, &'a [&'a String], u64)> {
        if let Some(selected) = selected {
            if let Some((reader, bytes)) = self
                .order_pages(label, index, block, ids, selected, order_only)
                .await?
            {
                return Ok((reader, ids, bytes));
            }
        }
        let (reader, reservation, data) = self
            .block(label, index, block, selected.map(|p| p.full_block.clone()))
            .await?;
        Ok(((Box::new(reader), reservation, vec![data]), ids, 0))
    }
}

// Read a column page generated by either layout, preserving the requested edge order.
fn select_column(
    bytes: bytes::Bytes,
    first: u64,
    ids: &[u64],
    budget: &MemoryReservation,
) -> Result<ArrayRef> {
    let mut cursor = 0;
    let mut row = first;
    let mut arrays = Vec::new();
    let staging = budget.new_empty();
    let decoded = budget.new_empty();
    for batch in StreamReader::try_new(std::io::Cursor::new(bytes), None)? {
        let batch = batch?;
        decoded.try_resize(batch.get_array_memory_size())?;
        let next = row + batch.num_rows() as u64;
        let end = cursor + ids[cursor..].partition_point(|&id| id < next);
        if end > cursor {
            let indices =
                UInt32Array::from_iter_values(ids[cursor..end].iter().map(|id| (id - row) as u32));
            let array = compact_view(take(batch.column(0), &indices, None)?);
            staging.try_grow(array.get_array_memory_size())?;
            arrays.push(array);
            cursor = end;
        }
        row = next;
        if cursor == ids.len() {
            break;
        }
    }
    ensure!(cursor == ids.len(), "incomplete CSR filter column");
    let arrays = arrays
        .iter()
        .map(|array| array.as_ref())
        .collect::<Vec<_>>();
    let result = datafusion::arrow::compute::concat(&arrays)?;
    budget.try_grow(result.get_array_memory_size())?;
    Ok(result)
}

fn id_label(id: &str) -> &str {
    id.split_once('[').map_or("", |(label, _)| label)
}

fn unescape(text: &str) -> String {
    text.replace("%2C", ",")
        .replace("%5B", "[")
        .replace("%5D", "]")
        .replace("%25", "%")
}

fn lower_bound(mut end: usize, mut before: impl FnMut(usize) -> bool) -> usize {
    let mut start = 0;
    while start < end {
        let middle = start + (end - start) / 2;
        if before(middle) {
            start = middle + 1;
        } else {
            end = middle;
        }
    }
    start
}

fn entity(
    label: &str,
    id: &str,
    kind: &str,
    batch: &RecordBatch,
    row: usize,
    ty: &DataType,
) -> Result<ScalarValue> {
    let DataType::Struct(fields) = ty else {
        anyhow::bail!("invalid CSR entity layout")
    };
    let text = |index: usize, text: &str| {
        ScalarValue::Utf8(Some(text.to_owned())).cast_to(fields[index].data_type())
    };
    let key = id
        .strip_suffix(']')
        .and_then(|id| id.split_once('['))
        .context("malformed CSR identity")?
        .1;
    let keys = key
        .split(',')
        .map(|part| ScalarValue::Utf8(Some(unescape(part))))
        .collect::<Vec<_>>();
    let offsets = UInt32Array::from(vec![u32::try_from(row)?]);
    let props = |ty: &DataType| -> Result<ScalarValue> {
        let DataType::Struct(fields) = ty else {
            anyhow::bail!("invalid CSR property layout")
        };
        let columns = fields
            .iter()
            .map(|field| -> Result<ArrayRef> {
                if let Some(array) = batch.column_by_name(field.name()) {
                    let array = compact_view(take(array, &offsets, None)?);
                    if array.data_type() == field.data_type() {
                        Ok(array)
                    } else {
                        Ok(cast(&array, field.data_type())?)
                    }
                } else {
                    Ok(datafusion::arrow::array::new_null_array(
                        field.data_type(),
                        1,
                    ))
                }
            })
            .collect::<Result<_>>()?;
        Ok(ScalarValue::Struct(Arc::new(StructArray::try_new(
            fields.clone(),
            columns,
            None,
        )?)))
    };
    let payload = if fields[3].name() == "lanes" {
        let DataType::Struct(lanes) = fields[3].data_type() else {
            anyhow::bail!("invalid CSR lanes")
        };
        let columns = lanes
            .iter()
            .map(|lane| -> Result<ArrayRef> {
                if lane.name() == label {
                    Ok(props(lane.data_type())?.to_array()?)
                } else {
                    Ok(datafusion::arrow::array::new_null_array(
                        lane.data_type(),
                        1,
                    ))
                }
            })
            .collect::<Result<_>>()?;
        ScalarValue::Struct(Arc::new(StructArray::try_new(
            lanes.clone(),
            columns,
            None,
        )?))
    } else {
        props(fields[3].data_type())?
    };
    let value = [
        text(0, kind)?,
        text(1, label)?,
        ScalarValue::List(ScalarValue::new_list(&keys, &DataType::Utf8, true))
            .cast_to(fields[2].data_type())?,
        payload,
    ];
    let columns = value
        .iter()
        .map(ScalarValue::to_array)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ScalarValue::Struct(Arc::new(StructArray::try_new(
        fields.clone(),
        columns,
        None,
    )?)))
}

pub(super) fn register(context: &SessionContext, csr: &CypherCsr, scope: &Arc<QueryScope>) {
    for order_only in [false, true] {
        context.register_udf(
            AsyncScalarUDF::new(Arc::new(Hydrate {
                csr: csr.clone(),
                scope: scope.clone(),
                order_only,
                signature: Signature::any(3, Volatility::Volatile),
            }))
            .into_scalar_udf(),
        );
    }
}

struct Hydrate {
    csr: CypherCsr,
    scope: Arc<QueryScope>,
    signature: Signature,
    order_only: bool,
}

impl PartialEq for Hydrate {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.scope, &other.scope) && self.order_only == other.order_only
    }
}

impl Eq for Hydrate {}

impl std::hash::Hash for Hydrate {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(&Arc::as_ptr(&self.scope), state);
        std::hash::Hash::hash(&self.order_only, state);
    }
}

impl std::fmt::Debug for Hydrate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

impl ScalarUDFImpl for Hydrate {
    fn name(&self) -> &str {
        if self.order_only {
            "__graphlake_csr_order_columns"
        } else {
            "__graphlake_csr_hydrate"
        }
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, args: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(DataType::new_list(args[1].clone(), true))
    }
    fn invoke_with_args(&self, _: ScalarFunctionArgs) -> datafusion::common::Result<ColumnarValue> {
        datafusion::common::internal_err!("CSR hydration requires async evaluation")
    }
}

#[async_trait::async_trait]
impl AsyncScalarUDFImpl for Hydrate {
    fn ideal_batch_size(&self) -> Option<usize> {
        Some(if self.order_only { 512 } else { 128 })
    }
    async fn invoke_async_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        self.invoke(args)
            .await
            .map_err(|err| DataFusionError::External(err.into()))
    }
}

impl Hydrate {
    async fn invoke(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ids = args.args[0].to_array(args.number_rows)?;
        let ids = ids
            .as_any()
            .downcast_ref::<ListArray>()
            .context("invalid CSR identity list")?;
        // Arrow list slices retain their original values and absolute offsets.
        // Restrict each hydration batch to its own IDs before reading blocks.
        let offsets = ids.value_offsets();
        let start = offsets[0];
        let end = offsets[offsets.len() - 1];
        let keys = cast(
            &ids.values().slice(start as usize, (end - start) as usize),
            &DataType::Utf8,
        )?;
        let keys = keys
            .as_any()
            .downcast_ref::<StringArray>()
            .context("invalid CSR identity keys")?;
        let source = self.csr.property_source(&self.scope);
        let budget =
            MemoryConsumer::new("CSR hydration batch").register(&self.scope.runtime.memory_pool);
        let mut unique = BTreeSet::new();
        for id in keys.iter().flatten() {
            if !unique.contains(id) {
                budget.try_grow(id.len() + 48)?;
                unique.insert(id.to_owned());
            }
        }
        let DataType::List(item) = args.return_field.data_type() else {
            anyhow::bail!("invalid CSR hydration result")
        };
        let ColumnarValue::Scalar(kind) = &args.args[2] else {
            anyhow::bail!("CSR hydration requires a constant entity kind");
        };
        let kind = kind
            .try_as_str()
            .flatten()
            .context("invalid CSR entity kind")?;
        let mut indices = BTreeMap::new();
        let mut payloads = Vec::new();
        source
            .identities(
                &unique,
                Some(item.data_type()),
                self.order_only,
                |bytes| self.csr.property_column_bytes(bytes, self.order_only),
                |label, id, batch, row| {
                    let value = entity(label, id, kind, batch, row, item.data_type())?;
                    budget.try_grow(id.len() + value.size() + 64)?;
                    indices.insert(id.to_owned(), payloads.len() as u32);
                    payloads.push(value);
                    Ok(())
                },
            )
            .await?;
        // Reuse the Arrow payloads for repeated elements while preserving list order and nulls.
        let raw = if payloads.is_empty() {
            datafusion::arrow::array::new_empty_array(item.data_type())
        } else {
            ScalarValue::iter_to_array(payloads.into_iter())?
        };
        let take_ids = UInt32Array::from_iter(
            keys.iter()
                .map(|id| id.and_then(|id| indices.get(id).copied())),
        );
        let values = take(&raw, &take_ids, None)?;
        budget.try_grow(values.get_array_memory_size())?;
        let result = ListArray::try_new(
            item.clone(),
            OffsetBuffer::new(
                offsets
                    .iter()
                    .map(|offset| offset - start)
                    .collect::<Vec<_>>()
                    .into(),
            ),
            values,
            ids.nulls().cloned(),
        )?;
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}
