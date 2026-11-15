use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result, ensure};
use datafusion::{
    arrow::{
        array::{Array, Int64Array},
        datatypes::DataType,
    },
    common::{Column, DataFusionError},
    dataframe::DataFrame,
    execution::memory_pool::MemoryReservation,
    logical_expr::{ColumnarValue, Expr, ExprSchemable, Volatility, cast, create_udf, lit},
    prelude::SessionContext,
};
use futures::TryStreamExt;
use serde_json::Value;

use crate::{
    Catalog, Comparison, GraphOptions, Predicate, ScalarValue, Scd2,
    catalog::{ConflictPolicy, Mapping, ValidationSite, columns, equal_keys, ident},
};

use super::{QueryScope, Raw, check_key_type, materialize_edges, select_columns};
use crate::source::{postgres::PgTable, snapshot::Buffer};

const PRIORITY: &str = "__graphlake_input_priority";
const FIRST: &str = "__graphlake_first_priority";
const COUNT: &str = "__graphlake_key_count";

struct Label<'a> {
    mapping: Mapping<'a>,
    fields: Vec<String>,
    keys: &'a [String],
}

#[derive(Clone, Copy)]
pub(super) struct TopologyFields<'a>(pub Option<&'a str>);

pub(super) type KeyChecks = Mutex<BTreeMap<String, Vec<(String, String)>>>;

pub(super) struct Tables<'a> {
    pub raw: &'a BTreeMap<&'a str, Raw>,
    pub postgres: &'a BTreeMap<String, Arc<PgTable>>,
    pub proven_key_label: Option<&'a str>,
    pub parquet_keys: &'a KeyChecks,
}

#[derive(Clone, Copy, Default)]
pub(super) struct ReadKind<'a> {
    pub validate_endpoints: bool,
    pub topology: Option<TopologyFields<'a>>,
    pub explain_only: bool,
    pub lazy_validation: bool,
    pub check_data: bool,
    pub sql: Option<&'a super::sql_read::Read>,
    #[cfg(feature = "admin")]
    pub probe_source: Option<&'a str>,
    #[cfg(feature = "admin")]
    pub graph_versions: bool,
    #[cfg(feature = "admin")]
    pub transaction: Option<&'a super::ReadTransaction>,
    #[cfg(feature = "admin")]
    pub distributed: Option<&'a crate::admin::cluster::distributed::Distributed>,
    #[cfg(feature = "admin")]
    pub partitioned_topology: bool,
    #[cfg(feature = "admin")]
    pub exploration: Option<&'a Exploration<'a>>,
}

#[cfg(feature = "admin")]
pub(crate) struct Exploration<'a> {
    pub(crate) endpoints: BTreeMap<String, Vec<&'a [String]>>,
    pub(crate) key: &'a [String],
    pub(crate) start: Option<(&'a str, &'a [String])>,
}

fn label<'a>(
    catalog: &'a Catalog,
    name: &str,
    options: &GraphOptions,
    topology: Option<TopologyFields<'_>>,
    sql: Option<&super::sql_read::Read>,
) -> Label<'a> {
    let mapping = catalog.mapping(name);
    let (mut fields, keys) = if let Some(node) = catalog.nodes.get(name) {
        (columns([&node.key, &node.properties]), node.key.as_slice())
    } else {
        let edge = &catalog.edges[name];
        (edge.source_columns(), edge.key.as_slice())
    };
    if let Some(TopologyFields(weight)) = topology
        && mapping.inputs.is_empty()
        && mapping.scd2.is_none()
    {
        let edge = catalog.edges.get(name);
        fields.retain(|field| {
            keys.contains(field)
                || (!mapping.unpartitioned
                    && options.logical_partition.iter().any(|p| p.column == *field))
                || edge.is_some_and(|edge| {
                    edge.from.columns.contains(field)
                        || edge.to.columns.contains(field)
                        || weight == Some(field.as_str())
                })
        });
    }
    if let Some(used) = sql.and_then(|sql| sql.columns.as_ref())
        && !keys.is_empty()
        && mapping.inputs.is_empty()
        && mapping.scd2.is_none()
    {
        fields.retain(|field| {
            used.contains(field)
                || keys.contains(field)
                || (!mapping.unpartitioned
                    && options.logical_partition.iter().any(|p| p.column == *field))
                || catalog.edges.get(name).is_some_and(|edge| {
                    edge.from.columns.contains(field) || edge.to.columns.contains(field)
                })
        });
    }
    Label {
        mapping,
        fields,
        keys,
    }
}

impl Label<'_> {
    fn read_fields(&self) -> Vec<String> {
        let mut fields = self.fields.clone();
        for name in self.mapping.scd2.into_iter().flat_map(Scd2::columns) {
            if !fields.contains(name) {
                fields.push(name.clone());
            }
        }
        fields
    }
}

pub(super) fn required<'a>(
    catalog: &'a Catalog,
    labels: &mut BTreeSet<String>,
    options: &GraphOptions,
    topology: Option<TopologyFields<'_>>,
    sql: Option<&super::sql_read::Read>,
) -> Result<BTreeMap<&'a str, BTreeSet<String>>> {
    for name in labels.clone() {
        if let Some(edge) = catalog.edges.get(&name) {
            labels.insert(edge.from.node.clone());
            labels.insert(edge.to.node.clone());
        } else {
            ensure!(
                catalog.nodes.contains_key(&name),
                "unknown graph table {name:?}"
            );
        }
    }
    let mut required = BTreeMap::<&str, BTreeSet<String>>::new();
    for name in labels.iter() {
        let label = label(catalog, name, options, topology, sql);
        let fields = label.read_fields();
        if !label.mapping.unpartitioned {
            for predicate in &options.logical_partition {
                ensure!(
                    fields.contains(&predicate.column),
                    "logical partition field {} is missing from {name}",
                    predicate.column
                );
            }
        }
        if label.mapping.inputs.is_empty() {
            required
                .entry(label.mapping.source)
                .or_default()
                .extend(fields);
        } else {
            for input in label.mapping.inputs {
                required.entry(&input.table).or_default().extend(
                    fields
                        .iter()
                        .map(|field| input.columns.get(field).unwrap_or(field).clone())
                        .chain(
                            input
                                .filters
                                .iter()
                                .map(|predicate| predicate.column.clone()),
                        ),
                );
            }
        }
    }
    Ok(required)
}

pub(super) async fn register(
    catalog: &Catalog,
    context: &SessionContext,
    private: &SessionContext,
    labels: BTreeSet<String>,
    tables: Tables<'_>,
    scope: &QueryScope,
    read: ReadKind<'_>,
) -> Result<Vec<MemoryReservation>> {
    let mut reservations = Vec::new();
    let mut modeled = BTreeMap::<&str, Raw>::new();
    for (index, name) in labels.iter().enumerate() {
        let label = label(catalog, name, &scope.options.graph, read.topology, read.sql);
        let alias = format!("model_{index}");
        let (frame, budget) = bind_label(private, &tables, scope, name, &label, read)
            .await
            .map_err(|error| ValidationSite::for_mapping(error, name))?;
        reservations.extend(budget);
        private.register_table(alias.as_str(), frame.into_view())?;
        let frame = private.table(alias.as_str()).await?;
        let mut table = Raw {
            schema: Arc::clone(frame.schema().inner()),
            alias,
            parquet_versions: None,
        };
        #[cfg(feature = "admin")]
        let neighbor_read = read.exploration.is_some();
        #[cfg(not(feature = "admin"))]
        let neighbor_read = false;
        if let Some(edge) = catalog.edges.get(name)
            && edge.key.is_empty()
            && !neighbor_read
        {
            let alias = format!("synthetic_{index}");
            reservations.push(
                materialize_edges(
                    private,
                    &table,
                    &label.fields,
                    &alias,
                    scope,
                    read.explain_only,
                )
                .await?,
            );
            table.schema = Arc::clone(private.table(alias.as_str()).await?.schema().inner());
            table.alias = alias;
        }
        modeled.insert(name, table);
    }
    for name in &labels {
        let table = &modeled[name.as_str()];
        let mut frame = private.table(table.alias.as_str()).await?;
        if let Some(edge) = catalog.edges.get(name) {
            let mut joins = String::new();
            for (side, alias, endpoint) in [
                ("from", "src_node", &edge.from),
                ("to", "dst_node", &edge.to),
            ] {
                let node = &catalog.nodes[&endpoint.node];
                let node_table = &modeled[endpoint.node.as_str()];
                for (index, (node_key, edge_key)) in
                    node.key.iter().zip(&endpoint.columns).enumerate()
                {
                    let expected = node_table.schema.field_with_name(node_key)?.data_type();
                    let actual = table.schema.field_with_name(edge_key)?.data_type();
                    if actual != expected {
                        return Err(anyhow::anyhow!("edge {name}.{edge_key} has type {actual}, expected {expected} for {}.{node_key}", endpoint.node)
                            .context(ValidationSite::field(name, side, Some(index))));
                    }
                }
                if read.check_data {
                    let missing = format!(
                        "SELECT 1 FROM {} AS src LEFT ANTI JOIN {} AS {alias} ON {} LIMIT 1",
                        ident(&table.alias),
                        ident(&node_table.alias),
                        equal_keys("src", &endpoint.columns, alias, &node.key)
                    );
                    if has_rows(private.sql(&missing).await?).await? {
                        return Err(anyhow::anyhow!(
                            "边 {name} 的{}端点无法匹配节点 {}（{}）",
                            if side == "from" { "起始" } else { "目标" },
                            endpoint.node,
                            endpoint.columns.join(", ")
                        )
                        .context(ValidationSite::field(name, side, None)));
                    }
                }
                if read.validate_endpoints {
                    joins.push_str(&format!(
                        " LEFT SEMI JOIN {} AS {alias} ON {}",
                        ident(&node_table.alias),
                        equal_keys("src", &endpoint.columns, alias, &node.key)
                    ));
                }
            }
            if read.validate_endpoints {
                let mut fields = columns([&edge.identity(), &edge.source_columns()]);
                fields.retain(|field| table.schema.field_with_name(field).is_ok());
                frame = private
                    .sql(&format!(
                        "SELECT {} FROM {} AS src{joins}",
                        select_columns("src", &fields),
                        ident(&table.alias)
                    ))
                    .await?;
            }
        }
        context.register_table(
            datafusion::common::TableReference::bare(name.as_str()),
            frame.into_view(),
        )?;
    }
    Ok(reservations)
}

async fn bind_label(
    context: &SessionContext,
    tables: &Tables<'_>,
    scope: &QueryScope,
    name: &str,
    label: &Label<'_>,
    read: ReadKind<'_>,
) -> Result<(DataFrame, Option<MemoryReservation>)> {
    let mapping = &label.mapping;
    let fields = label.read_fields();
    let prefer_first = mapping.on_conflict == ConflictPolicy::PreferFirst;
    let mut relation = None::<DataFrame>;
    let mut first_types = Vec::new();
    let mut branches = 0;
    for (index, input) in std::iter::once(None)
        .filter(|_| mapping.inputs.is_empty())
        .chain(mapping.inputs.iter().map(Some))
        .enumerate()
    {
        let table = &tables.raw[input.map_or(mapping.source, |input| &input.table)];
        let mut frame = context.table(table.alias.as_str()).await?;
        if let Some(input) = input {
            for predicate in &input.filters {
                frame = filter(frame, predicate)?;
            }
        }
        let mut projection = fields
            .iter()
            .map(|field| {
                let physical = input
                    .and_then(|input| input.columns.get(field))
                    .unwrap_or(field);
                let expr = column(physical);
                if let Some(data_type) = input.and_then(|input| input.casts.get(field)) {
                    Ok(cast(expr, data_type.parse()?).alias(field))
                } else {
                    Ok(if physical == field {
                        expr
                    } else {
                        expr.alias(field)
                    })
                }
            })
            .collect::<Result<Vec<_>>>()?;
        if prefer_first {
            projection.push(lit(index as u64).alias(PRIORITY));
        }
        frame = frame.select(projection)?;
        let types = frame
            .schema()
            .fields()
            .iter()
            .map(|field| field.data_type().clone())
            .collect::<Vec<_>>();
        if index == 0 {
            first_types = types;
        } else {
            for ((field, actual), expected) in fields.iter().zip(types).zip(&first_types) {
                if &actual != expected {
                    return Err(anyhow::anyhow!(
                        "column {field} has type {actual}, expected {expected}"
                    )
                    .context(ValidationSite::InputColumn {
                        label: name.into(),
                        input: index,
                        column: field.clone(),
                    }));
                }
            }
        }
        for (key_index, key) in label.keys.iter().enumerate() {
            check_key_type(&data_type(&frame, key)?, name, key)
                .with_context(|| ValidationSite::field(name, "key", Some(key_index)))?;
        }
        if !mapping.unpartitioned {
            for predicate in &scope.options.graph.logical_partition {
                frame = filter(frame, predicate)?;
            }
        }
        if let Some(scd2) = mapping.scd2 {
            frame = temporal(frame, scd2, scope.options.graph.snapshot_time.as_deref())?;
        }
        if read.check_data
            && let Some(null_key) = label
                .keys
                .iter()
                .map(|key| column(key).is_null())
                .reduce(Expr::or)
        {
            if has_rows(frame.clone().filter(null_key)?.limit(0, Some(1))?).await? {
                return Err(anyhow::anyhow!(
                    "标签 {name} 存在空业务键（{}），来源 {}",
                    label.keys.join(", "),
                    input.map_or(mapping.source, |input| &input.table)
                )
                .context(ValidationSite::field(name, "key", None)));
            }
        }
        for key in label.keys {
            frame = frame.filter(column(key).is_not_null())?;
        }
        // A filtered copy of an earlier unfiltered projection cannot win for
        // any key under prefer_first. Still bind it to validate its configuration.
        if prefer_first
            && !label.keys.is_empty()
            && input.is_some_and(|input| {
                mapping.inputs[..index].iter().any(|earlier| {
                    earlier.table == input.table
                        && earlier.filters.is_empty()
                        && fields.iter().all(|field| {
                            earlier.columns.get(field).unwrap_or(field)
                                == input.columns.get(field).unwrap_or(field)
                                && earlier.casts.get(field) == input.casts.get(field)
                        })
                })
            })
        {
            continue;
        }
        branches += 1;
        relation = Some(match relation {
            Some(previous) => previous.union(frame)?,
            None => frame,
        });
    }
    let mut frame = relation.expect("validated label inputs");
    #[cfg(feature = "admin")]
    let exploration = read.exploration.and_then(|exploration| {
        exploration
            .endpoints
            .get(name)
            .map(|endpoints| (endpoints, exploration.key))
    });
    #[cfg(feature = "admin")]
    if let Some((start_label, values)) = read.exploration.and_then(|explore| explore.start)
        && name == start_label
        && !label.keys.is_empty()
        && label.keys.len() == values.len()
    {
        // Restrict both sides of candidate selection before conflict handling.
        // No winner for a retained business key can be lost this way.
        let mut suffix = None;
        for (key, value) in label.keys.iter().zip(values).rev() {
            let ty = data_type(&frame, key)?;
            if !exploration_seek_type(&ty) {
                suffix = None;
                break;
            }
            let key = column(key);
            let value = lit(ScalarValue::try_from_string(value.clone(), &ty)?);
            suffix = Some(match suffix {
                None => key.gt_eq(value),
                Some(rest) => key.clone().gt(value.clone()).or(key.eq(value).and(rest)),
            });
        }
        if let Some(filter) = suffix {
            frame = frame.filter(filter)?;
        }
    }
    #[cfg(feature = "admin")]
    if let Some((endpoints, anchor)) = exploration
        && !label.keys.is_empty()
        && (!mapping.inputs.is_empty() || mapping.scd2.is_some())
    {
        frame = exploration_candidates(frame, label.keys, endpoints, anchor)?;
    }
    let mut retained = None;
    let visible = label
        .fields
        .iter()
        .map(|name| column(name))
        .collect::<Vec<_>>();
    if !read.explain_only
        && read.topology.is_none()
        && !label.keys.is_empty()
        && mapping.inputs.is_empty()
        && mapping.scd2.is_none()
    {
        // Only complete scans may establish a proof for the pinned file set.
        let full_scan = scope.options.graph.logical_partition.is_empty();
        #[cfg(feature = "admin")]
        let full_scan = full_scan && read.exploration.is_none() && read.distributed.is_none();
        let parquet = tables.raw[mapping.source]
            .parquet_versions
            .as_ref()
            .filter(|_| full_scan);
        let cached = parquet
            .is_some_and(|objects| tables.parquet_keys.lock().unwrap().get(name) == Some(objects));
        let unique = if cached || tables.proven_key_label == Some(name) {
            true
        } else {
            match tables.postgres.get(mapping.source) {
                Some(table) => table.guarantees_unique_key(label.keys).await?,
                None => false,
            }
        };
        if !unique {
            if read.lazy_validation {
                #[cfg(feature = "admin")]
                if let Some((endpoints, anchor)) = exploration {
                    frame = exploration_candidates(frame, label.keys, endpoints, anchor)?;
                }
                let error = format!("duplicate business key in {name} from {}", mapping.source);
                let frame = check_keys(context, frame, &fields, label.keys).await?;
                return Ok((
                    verify_keys(frame, label.keys, error)?.select(visible)?,
                    None,
                ));
            }
            let keys = label
                .keys
                .iter()
                .map(|name| column(name))
                .collect::<Vec<_>>();
            let counts = frame.clone().aggregate(
                keys,
                vec![datafusion::functions_aggregate::expr_fn::count(lit(1_i64)).alias(COUNT)],
            )?;
            let duplicate_keys = counts
                .filter(column(COUNT).gt(lit(1_i64)))?
                .limit(0, Some(1))?;
            let mut duplicates = validation_stream(duplicate_keys, scope, read).await?;
            while let Some(batch) = duplicates.try_next().await? {
                scope.check()?;
                if batch.num_rows() != 0 {
                    return Err(anyhow::anyhow!(
                        "duplicate business key in {name} from {}",
                        mapping.source
                    )
                    .context(ValidationSite::field(name, "key", None)));
                }
            }
            if let Some(objects) = parquet {
                tables
                    .parquet_keys
                    .lock()
                    .unwrap()
                    .insert(name.into(), objects.clone());
            }
        }
    }
    if !label.keys.is_empty() && (!mapping.inputs.is_empty() || mapping.scd2.is_some()) {
        let alias = format!("dedup_{}", uuid::Uuid::new_v4().simple());
        let key = label
            .keys
            .iter()
            .map(|key| ident(key))
            .collect::<Vec<_>>()
            .join(", ");
        let base = if prefer_first && branches > 1 {
            context.register_table(alias.as_str(), frame.into_view())?;
            frame = context.sql(&format!(
                "SELECT {} FROM (SELECT *, min({PRIORITY}) OVER (PARTITION BY {key}) AS {FIRST} FROM {alias}) AS src WHERE {PRIORITY} = {FIRST}",
                select_columns("src", &fields))).await?;
            context.deregister_table(alias.as_str())?;
            frame
        } else {
            frame.select(fields.iter().map(|field| column(field)).collect::<Vec<_>>())?
        };
        #[cfg(feature = "admin")]
        if read.partitioned_topology {
            return materialize_partitioned_mapping(context, base, scope, name, label, visible)
                .await;
        }
        let distinct = base.distinct()?;
        if read.explain_only {
            return Ok((distinct.select(visible)?, None));
        }
        let check = check_keys(context, distinct, &fields, label.keys).await?;
        if read.lazy_validation {
            let frame = verify_keys(check, label.keys, format!("conflicting records for {name}"))?;
            return Ok((frame.select(visible)?, None));
        }
        let mut stream = validation_stream(check, scope, read).await?;
        let mut buffer = Buffer::new(
            &scope.runtime.memory_pool,
            Some(&scope.runtime.disk_manager),
        );
        let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(
            stream.schema().fields()[..fields.len()].to_vec(),
        ));
        while let Some(batch) = stream.try_next().await? {
            scope.check()?;
            let counts = batch
                .column(fields.len())
                .as_any()
                .downcast_ref::<Int64Array>()
                .context("invalid key count")?;
            if let Some(row) = counts.values().iter().position(|count| *count != 1) {
                let key = label
                    .keys
                    .iter()
                    .map(|field| {
                        let index = schema.index_of(field)?;
                        Ok(format!(
                            "{field}={}",
                            ScalarValue::try_from_array(batch.column(index), row)?
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?
                    .join(", ");
                let sources = if mapping.inputs.is_empty() {
                    mapping.source.to_owned()
                } else {
                    mapping
                        .inputs
                        .iter()
                        .map(|input| input.table.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                };
                anyhow::bail!("conflicting records for {name} ({key}) in {sources}");
            }
            let batch = datafusion::arrow::array::RecordBatch::try_new(
                Arc::clone(&schema),
                batch.columns()[..fields.len()].to_vec(),
            )?;
            buffer.push(batch)?;
        }
        let (table, budget) = buffer.finish(schema, &scope.reads)?;
        frame = context.read_table(table)?;
        retained = Some(budget);
    }
    Ok((frame.select(visible)?, retained))
}

#[cfg(feature = "admin")]
async fn materialize_partitioned_mapping(
    context: &SessionContext,
    frame: DataFrame,
    scope: &QueryScope,
    name: &str,
    label: &Label<'_>,
    visible: Vec<Expr>,
) -> Result<(DataFrame, Option<MemoryReservation>)> {
    use datafusion::arrow::{
        array::RecordBatch,
        row::{RowConverter, SortField},
    };
    // Ordered keys make full-row deduplication streaming, including conflict
    // detection across the boundary between two Arrow batches.
    let schema = Arc::clone(frame.schema().inner());
    let keys = label
        .keys
        .iter()
        .map(|key| schema.index_of(key))
        .collect::<Result<Vec<_>, _>>()?;
    let converter = RowConverter::new(
        keys.iter()
            .map(|&index| SortField::new(schema.field(index).data_type().clone()))
            .collect(),
    )?;
    let mut rows = frame
        .sort(
            label
                .keys
                .iter()
                .map(|key| column(key).sort(true, false))
                .collect(),
        )?
        .execute_stream()
        .await?;
    let mut buffer = Buffer::new(
        &scope.runtime.memory_pool,
        Some(&scope.runtime.disk_manager),
    );
    let mut prior_key = Vec::new();
    let mut prior_values = Vec::<ScalarValue>::new();
    while let Some(batch) = rows.try_next().await? {
        scope.check()?;
        let encoded = converter.convert_columns(
            &keys
                .iter()
                .map(|&i| batch.column(i).clone())
                .collect::<Vec<_>>(),
        )?;
        let mut retained = Vec::new();
        for (i, key) in encoded.iter().enumerate() {
            let values = || {
                batch
                    .columns()
                    .iter()
                    .map(|column| ScalarValue::try_from_array(column, i))
                    .collect::<Result<Vec<_>, _>>()
            };
            if prior_key.as_slice() == key.data() {
                ensure!(prior_values == values()?, "conflicting records for {name}");
            } else {
                prior_key.clear();
                prior_key.extend_from_slice(key.data());
                prior_values = values()?;
                retained.push(i as u32);
            }
        }
        if !retained.is_empty() {
            let rows = datafusion::arrow::array::UInt32Array::from(retained);
            let columns = batch
                .columns()
                .iter()
                .map(|column| datafusion::arrow::compute::take(column.as_ref(), &rows, None))
                .collect::<Result<Vec<_>, _>>()?;
            buffer.push(RecordBatch::try_new(schema.clone(), columns)?)?;
        }
    }
    let (table, reservation) = buffer.finish(schema, &scope.reads)?;
    let frame = context.read_table(table)?;
    Ok((frame.select(visible)?, Some(reservation)))
}

async fn validation_stream(
    frame: DataFrame,
    _scope: &QueryScope,
    _read: ReadKind<'_>,
) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
    #[cfg(feature = "admin")]
    if let Some(distributed) = _read.distributed {
        let ctx = Arc::new(frame.task_ctx());
        let plan = frame.create_physical_plan().await?;
        let plan = distributed
            .execute(plan, _scope, ctx.clone(), false, false)
            .await?;
        return Ok(datafusion::physical_plan::execute_stream(plan, ctx)?);
    }
    Ok(frame.execute_stream().await?)
}

async fn has_rows(frame: DataFrame) -> Result<bool> {
    let mut stream = frame.execute_stream().await?;
    while let Some(batch) = stream.try_next().await? {
        if batch.num_rows() > 0 {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(feature = "admin")]
pub(crate) fn exploration_seek_type(ty: &DataType) -> bool {
    ty.is_integer()
        || matches!(
            ty,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        )
}

#[cfg(feature = "admin")]
fn exploration_candidates(
    frame: DataFrame,
    keys: &[String],
    endpoints: &[&[String]],
    values: &[String],
) -> Result<DataFrame> {
    let anchor = endpoints
        .iter()
        .map(|fields| {
            fields
                .iter()
                .zip(values)
                .map(|(field, value)| {
                    let col = column(field);
                    let ty = data_type(&frame, field)?;
                    let test = match ty {
                        DataType::Int8
                        | DataType::Int16
                        | DataType::Int32
                        | DataType::Int64
                        | DataType::UInt8
                        | DataType::UInt16
                        | DataType::UInt32
                        | DataType::UInt64 => {
                            match ScalarValue::try_from_string(value.clone(), &ty) {
                                Ok(value) => col.eq(lit(value)),
                                Err(_) => lit(false),
                            }
                        }
                        _ => cast(col, DataType::Utf8).eq(lit(value.clone())),
                    };
                    Ok(test)
                })
                .collect::<Result<Vec<_>>>()
                .map(|tests| tests.into_iter().reduce(Expr::and))
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .reduce(Expr::or);
    let Some(anchor) = anchor else {
        return Ok(frame);
    };
    // The first scan finds keys, not winners. Restrict each key as a whole so
    // another mapping can still win or expose a conflicting row for that key.
    let candidates = frame
        .clone()
        .filter(anchor)?
        .select(keys.iter().map(|key| column(key)).collect::<Vec<_>>())?
        .distinct()?;
    let names: Vec<_> = keys.iter().map(String::as_str).collect();
    // The candidate keys are the build side. DataFusion can push their
    // dynamic filter into the wider input scan.
    Ok(candidates.alias("explore_keys")?.join(
        frame.alias("explore_input")?,
        datafusion::logical_expr::JoinType::RightSemi,
        &names,
        &names,
        None,
    )?)
}

async fn check_keys(
    context: &SessionContext,
    frame: DataFrame,
    fields: &[String],
    keys: &[String],
) -> Result<DataFrame> {
    let alias = format!("dedup_{}", uuid::Uuid::new_v4().simple());
    let key = keys
        .iter()
        .map(|name| ident(name))
        .collect::<Vec<_>>()
        .join(", ");
    context.register_table(alias.as_str(), frame.into_view())?;
    let check = context
        .sql(&format!(
            "SELECT {}, count(*) OVER (PARTITION BY {key}) AS {COUNT} FROM {alias} AS src",
            select_columns("src", fields)
        ))
        .await;
    context.deregister_table(alias.as_str())?;
    Ok(check?)
}

fn verify_keys(frame: DataFrame, keys: &[String], error: String) -> Result<DataFrame> {
    // Filters on the partition key can reach each input. The count prevents
    // property filters or projection from hiding a conflicting record.
    let fields = std::iter::once(COUNT).chain(keys.iter().map(String::as_str));
    let types = fields
        .clone()
        .map(|field| data_type(&frame, field))
        .collect::<Result<Vec<_>>>()?;
    let check = key_validator(types, error);
    Ok(frame.filter(check.call(fields.map(column).collect()))?)
}

pub(crate) const KEY_VALIDATOR: &str = "__graphlake_verify_keys_";

pub(crate) fn key_validator(
    types: Vec<DataType>,
    error: String,
) -> datafusion::logical_expr::ScalarUDF {
    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
    let name = format!(
        "{KEY_VALIDATOR}{}",
        URL_SAFE_NO_PAD.encode(error.as_bytes())
    );
    create_udf(
        &name,
        types,
        DataType::Boolean,
        Volatility::Volatile,
        Arc::new(move |args| {
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let count = arrays[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("key count");
            if let Some(row) = count.iter().position(|count| count != Some(1)) {
                let keys = arrays[1..]
                    .iter()
                    .map(|array| ScalarValue::try_from_array(array, row))
                    .collect::<datafusion::common::Result<Vec<_>>>()?;
                return Err(DataFusionError::Execution(format!("{error} ({keys:?})")));
            }
            Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))))
        }),
    )
}

fn column(name: &str) -> Expr {
    Expr::Column(Column::from_name(name))
}

fn data_type(frame: &DataFrame, field: &str) -> Result<DataType> {
    Ok(column(field).get_type(frame.schema())?)
}

fn typed(value: &Value, ty: &DataType) -> Result<Expr> {
    let text = value
        .as_str()
        .map_or_else(|| value.to_string(), str::to_owned);
    let scalar = ScalarValue::try_from_string(text, ty)?;
    ensure!(!scalar.is_null(), "invalid literal for {ty}");
    Ok(lit(scalar))
}

fn filter(frame: DataFrame, predicate: &Predicate) -> Result<DataFrame> {
    let col = column(&predicate.column);
    let ty = col.get_type(frame.schema())?;
    let expr = match predicate.op {
        Comparison::IsNull => col.is_null(),
        Comparison::IsNotNull => col.is_not_null(),
        op => {
            let value = typed(&predicate.value, &ty)?;
            match op {
                Comparison::Eq => col.eq(value),
                Comparison::Ne => col.not_eq(value),
                Comparison::Lt => col.lt(value),
                Comparison::Le => col.lt_eq(value),
                Comparison::Gt => col.gt(value),
                Comparison::Ge => col.gt_eq(value),
                _ => unreachable!(),
            }
        }
    };
    Ok(frame.filter(expr)?)
}

fn temporal(frame: DataFrame, scd2: &Scd2, time: Option<&str>) -> Result<DataFrame> {
    let interval = if let (Some(from), Some(to)) = (&scd2.valid_from, &scd2.valid_to) {
        let ty = data_type(&frame, from)?;
        ensure!(
            ty == data_type(&frame, to)?
                && matches!(
                    ty,
                    DataType::Date32
                        | DataType::Date64
                        | DataType::Timestamp(..)
                        | DataType::Int32
                        | DataType::Int64
                ),
            "SCD2 validity columns must have one matching date, timestamp or integer type"
        );
        Some((column(from), column(to), ty))
    } else {
        None
    };
    let expr = if let Some(time) = time.filter(|time| !time.eq_ignore_ascii_case("current")) {
        let (from, to, ty) =
            interval.context("historical SCD2 queries require a validity interval")?;
        let point = typed(&Value::String(time.to_owned()), &ty)?;
        let open = match &scd2.open_end {
            Some(value) => to.clone().eq(typed(value, &ty)?),
            None => to.clone().is_null(),
        };
        from.clone()
            .is_null()
            .or(from.lt_eq(point.clone()))
            .and(open.or(to.gt(point)))
    } else if let Some(flag) = &scd2.current_flag {
        let ty = data_type(&frame, flag)?;
        column(flag).eq(typed(
            scd2.current_value.as_ref().unwrap_or(&Value::Bool(true)),
            &ty,
        )?)
    } else {
        let (_, to, ty) = interval.expect("validated SCD2 mapping");
        match &scd2.open_end {
            Some(value) => to.eq(typed(value, &ty)?),
            None => to.is_null(),
        }
    };
    Ok(frame.filter(expr)?)
}
