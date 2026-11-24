use crate::source::SourceConsistency;

use std::{
    collections::{HashMap, VecDeque},
    fmt,
    io::Cursor,
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant},
};

use sha2::{Digest, Sha256};

use anyhow::{Context, Result, ensure};
use arrow_array_58::RecordBatch as IcebergBatch;
use arrow_ipc_58::writer::StreamWriter;
use arrow_schema_58::Schema as IcebergSchema;
use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{RecordBatch, RecordBatchOptions},
        datatypes::{DataType, Schema, SchemaRef, TimeUnit},
        ipc::reader::StreamReader,
    },
    catalog::{Session, TableProvider},
    common::{DataFusionError, ScalarValue},
    execution::{SendableRecordBatchStream, TaskContext, memory_pool::MemoryConsumer},
    logical_expr::{Expr, Operator, TableProviderFilterPushDown, TableType},
    physical_plan::{
        ExecutionPlan,
        stream::RecordBatchStreamAdapter,
        streaming::{PartitionStream, StreamingTableExec},
    },
};
use futures::TryStreamExt;
use iceberg::{
    Catalog, CatalogBuilder, TableIdent,
    arrow::schema_to_arrow_schema,
    expr::{Predicate, Reference},
    spec::{Datum, FormatVersion},
    table::Table,
};
use iceberg_catalog_rest::{RestCatalog, RestCatalogBuilder};
use iceberg_storage_opendal::OpenDalResolvingStorageFactory;

use super::{ReadStats, SourceVersion};
use crate::{Comparison, Predicate as ReadPredicate};

type CachedCatalog = (
    [u8; 32],
    Option<tokio::runtime::Id>,
    Instant,
    Arc<RestCatalog>,
);

pub(crate) async fn catalog(
    name: &str,
    uri: &str,
    warehouse: Option<&str>,
    token_env: Option<&str>,
) -> Result<Arc<RestCatalog>> {
    static CATALOGS: OnceLock<Mutex<VecDeque<CachedCatalog>>> = OnceLock::new();
    const CACHE_TTL: Duration = Duration::from_secs(300);
    const CACHE_LIMIT: usize = 32;

    let mut props: HashMap<String, String> = HashMap::from([("uri".into(), uri.into())]);
    if let Some(warehouse) = warehouse {
        props.insert("warehouse".into(), warehouse.into());
    }
    if let Some(name) = token_env {
        props.insert(
            "token".into(),
            std::env::var(name).with_context(|| format!("missing Iceberg token in {name}"))?,
        );
    }
    for (env, property) in [
        ("AWS_ENDPOINT", "s3.endpoint"),
        ("AWS_REGION", "s3.region"),
        ("AWS_ACCESS_KEY_ID", "s3.access-key-id"),
        ("AWS_SECRET_ACCESS_KEY", "s3.secret-access-key"),
        ("AWS_SESSION_TOKEN", "s3.session-token"),
    ] {
        if let Ok(value) = std::env::var(env) {
            if env == "AWS_ENDPOINT" {
                props.insert("s3.path-style-access".into(), "true".into());
            }
            props.insert(property.into(), value);
        }
    }
    let mut entries = props
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect::<Vec<_>>();
    entries.sort_unstable();
    let mut hasher = Sha256::new();
    hasher.update(name.len().to_le_bytes());
    hasher.update(name.as_bytes());
    for (k, v) in entries {
        for part in [k.as_bytes(), v.as_bytes()] {
            hasher.update(part.len().to_le_bytes());
            hasher.update(part);
        }
    }
    let fingerprint: [u8; 32] = hasher.finalize().into();
    let runtime = tokio::runtime::Handle::try_current().ok().map(|h| h.id());
    {
        let mut cache = CATALOGS.get_or_init(Mutex::default).lock().unwrap();
        cache.retain(|(_, _, opened, _)| opened.elapsed() < CACHE_TTL);
        if let Some(index) = cache
            .iter()
            .position(|(key, rt, _, _)| key == &fingerprint && *rt == runtime)
        {
            let entry = cache.remove(index).expect("located Iceberg catalog");
            let client = Arc::clone(&entry.3);
            cache.push_back(entry);
            return Ok(client);
        }
    }
    let built = Arc::new(
        RestCatalogBuilder::default()
            .with_storage_factory(Arc::new(OpenDalResolvingStorageFactory::new()))
            .load(name, props)
            .await?,
    );
    let mut cache = CATALOGS.get_or_init(Mutex::default).lock().unwrap();
    cache.retain(|(_, _, opened, _)| opened.elapsed() < CACHE_TTL);
    if let Some(index) = cache
        .iter()
        .position(|(key, rt, _, _)| key == &fingerprint && *rt == runtime)
    {
        let entry = cache.remove(index).expect("located Iceberg catalog");
        let existing = Arc::clone(&entry.3);
        cache.push_back(entry);
        return Ok(existing);
    }
    if cache.len() >= CACHE_LIMIT {
        cache.pop_front();
    }
    cache.push_back((fingerprint, runtime, Instant::now(), Arc::clone(&built)));
    Ok(built)
}

#[derive(Clone)]
pub(crate) struct IcebergTable {
    table: Table,
    snapshot: Option<i64>,
    schema: SchemaRef,
    filter: Option<Predicate>,
    stats: Arc<ReadStats>,
}

impl IcebergTable {
    pub(crate) async fn open(
        catalog: &RestCatalog,
        reference: &str,
        relation: &str,
        columns: &[String],
        filters: &[ReadPredicate],
        requested: Option<i64>,
        stats: Arc<ReadStats>,
    ) -> Result<(Self, SourceVersion)> {
        stats.request();
        let table = catalog
            .load_table(&TableIdent::from_strs(relation.split('.'))?)
            .await
            .with_context(|| format!("loading Iceberg table {reference}"))?;
        let metadata = table.metadata();
        ensure!(
            matches!(
                metadata.format_version(),
                FormatVersion::V1 | FormatVersion::V2
            ),
            "Iceberg table {reference} requires format version 1 or 2"
        );
        let snapshot = requested.or_else(|| metadata.current_snapshot_id());
        let schema = match snapshot {
            Some(id) => metadata
                .snapshot_by_id(id)
                .with_context(|| format!("Iceberg table {reference} has no snapshot {id}"))?
                .schema(metadata)?,
            None => Arc::clone(metadata.current_schema()),
        };
        let schema = schema_to_arrow_schema(&schema)?;
        let schema = if columns.is_empty() {
            schema
        } else {
            let projection = columns
                .iter()
                .map(|column| schema.index_of(column))
                .collect::<Result<Vec<_>, _>>()?;
            schema.project(&projection)?
        };
        let schema = bridge(&schema, None)?.schema();
        let filter = filters
            .iter()
            .filter_map(|filter| iceberg_predicate(&schema, filter))
            .reduce(Predicate::and);
        let version = SourceVersion {
            source: reference.into(),
            version: snapshot
                .map_or_else(|| format!("empty:{}", metadata.uuid()), |id| id.to_string()),
            consistency: SourceConsistency::Iceberg.label(),
        };
        Ok((
            Self {
                table,
                snapshot,
                schema,
                filter,
                stats,
            },
            version,
        ))
    }

    pub(crate) fn proof_token(&self, version: &SourceVersion) -> Option<String> {
        // Predicate-filtered scans only see a subset of keys and must not cache a
        // full-table uniqueness proof; include the table UUID so recreated tables
        // reusing a numeric snapshot ID cannot reuse a prior proof.
        self.filter
            .is_none()
            .then(|| format!("{}:{}", self.table.metadata().uuid(), version.version))
    }
}

impl fmt::Debug for IcebergTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IcebergScan")
            .field("table", self.table.identifier())
            .field("snapshot", &self.snapshot)
            .finish()
    }
}

#[async_trait]
impl TableProvider for IcebergTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::common::Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|expr| {
                if scan_predicate(&self.schema, expr).is_some() {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        _: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let mut table = self.clone();
        table.filter = table
            .filter
            .take()
            .into_iter()
            .chain(
                filters
                    .iter()
                    .filter_map(|expr| scan_predicate(&self.schema, expr)),
            )
            .reduce(Predicate::and);
        if let Some(columns) = projection {
            table.schema = Arc::new(self.schema.project(columns)?);
        }
        let schema = Arc::clone(&table.schema);
        Ok(Arc::new(StreamingTableExec::try_new(
            schema,
            vec![Arc::new(table)],
            None,
            [],
            false,
            limit,
        )?))
    }
}

impl PartitionStream for IcebergTable {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, context: Arc<TaskContext>) -> SendableRecordBatchStream {
        let Self {
            table,
            snapshot,
            schema,
            filter,
            stats,
        } = self.clone();
        let output = Arc::clone(&schema);
        let stream = async_stream::try_stream! {
            if let Some(snapshot) = snapshot {
                let columns = schema.fields().iter().map(|field| field.name());
                let mut scan = table.scan().snapshot_id(snapshot).select(columns)
                    .with_batch_size(Some(context.session_config().batch_size()))
                    .with_concurrency_limit(context.session_config().target_partitions());
                if let Some(filter) = filter {
                    scan = scan.with_filter(filter);
                }
                let scan = scan.build().map_err(external)?;
                stats.request();
                let mut batches = scan.to_arrow().await.map_err(external)?;
                while let Some(batch) = batches.try_next().await.map_err(external)? {
                    let reservation = MemoryConsumer::new("GraphLake Iceberg batch")
                        .register(&context.runtime_env().memory_pool);
                    reservation.try_grow(batch.get_array_memory_size().saturating_mul(3))?;
                    let batch = bridge(&batch.schema(), Some(&batch)).map_err(external)?
                        .next().ok_or_else(|| DataFusionError::Execution("missing Iceberg Arrow batch".into()))??;
                    let arrays = schema.fields().iter().map(|field| {
                        let index = batch.schema().index_of(field.name())?;
                        Ok(Arc::clone(batch.column(index)))
                    }).collect::<datafusion::common::Result<Vec<_>>>()?;
                    let batch = RecordBatch::try_new_with_options(Arc::clone(&schema), arrays,
                        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())))?;
                    stats.iceberg_batch(&batch);
                    yield batch;
                    drop(reservation);
                }
            }
        };
        Box::pin(RecordBatchStreamAdapter::new(output, stream))
    }
}

fn scan_predicate(schema: &Schema, expr: &Expr) -> Option<Predicate> {
    match expr {
        Expr::IsNull(expr) => {
            let Expr::Column(column) = expr.as_ref() else {
                return None;
            };
            if column.name.contains('.') {
                return None;
            }
            let field = schema.field_with_name(&column.name).ok()?;
            if field.data_type().is_nested() {
                return None;
            }
            Some(Reference::new(&column.name).is_null())
        }
        Expr::IsNotNull(expr) => {
            let Expr::Column(column) = expr.as_ref() else {
                return None;
            };
            if column.name.contains('.') {
                return None;
            }
            let field = schema.field_with_name(&column.name).ok()?;
            if field.data_type().is_nested() {
                return None;
            }
            Some(Reference::new(&column.name).is_not_null())
        }
        Expr::BinaryExpr(expr) if expr.op == Operator::And => {
            let left = scan_predicate(schema, &expr.left)?;
            let right = scan_predicate(schema, &expr.right)?;
            Some(left.and(right))
        }
        Expr::BinaryExpr(expr) => {
            let (column, op, literal) = match (expr.left.as_ref(), expr.right.as_ref()) {
                (Expr::Column(column), Expr::Literal(literal, _)) => (column, expr.op, literal),
                (Expr::Literal(literal, _), Expr::Column(column)) => {
                    let flipped = match expr.op {
                        Operator::Eq => Operator::Eq,
                        Operator::NotEq => Operator::NotEq,
                        Operator::Lt => Operator::Gt,
                        Operator::LtEq => Operator::GtEq,
                        Operator::Gt => Operator::Lt,
                        Operator::GtEq => Operator::LtEq,
                        _ => return None,
                    };
                    (column, flipped, literal)
                }
                _ => return None,
            };
            if column.name.contains('.') {
                return None;
            }
            let kind = schema.field_with_name(&column.name).ok()?.data_type();
            let value = match (kind, literal) {
                (DataType::Int32, ScalarValue::Int32(Some(v))) => Datum::int(*v),
                (DataType::Int64, ScalarValue::Int64(Some(v))) => Datum::long(*v),
                (DataType::Int64, ScalarValue::Int32(Some(v))) => Datum::long(i64::from(*v)),
                (DataType::Boolean, ScalarValue::Boolean(Some(v))) => Datum::bool(*v),
                (
                    DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
                    ScalarValue::Utf8(Some(v))
                    | ScalarValue::LargeUtf8(Some(v))
                    | ScalarValue::Utf8View(Some(v)),
                ) => Datum::string(v.clone()),
                (DataType::Date32, ScalarValue::Date32(Some(days))) => Datum::date(*days),
                (
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    ScalarValue::TimestampMicrosecond(Some(us), None),
                ) => Datum::timestamp_micros(*us),
                (
                    DataType::Timestamp(TimeUnit::Microsecond, Some(_)),
                    ScalarValue::TimestampMicrosecond(Some(us), Some(_)),
                ) => Datum::timestamptz_micros(*us),
                _ => return None,
            };
            let reference = Reference::new(&column.name);
            Some(match op {
                Operator::Eq => reference.equal_to(value),
                Operator::NotEq => reference.not_equal_to(value),
                Operator::Lt => reference.less_than(value),
                Operator::LtEq => reference.less_than_or_equal_to(value),
                Operator::Gt => reference.greater_than(value),
                Operator::GtEq => reference.greater_than_or_equal_to(value),
                _ => return None,
            })
        }
        _ => None,
    }
}

fn iceberg_predicate(schema: &Schema, filter: &ReadPredicate) -> Option<Predicate> {
    if filter.column.contains('.') {
        return None;
    }
    let kind = schema.field_with_name(&filter.column).ok()?.data_type();
    let reference = Reference::new(&filter.column);
    let value = match (kind, filter.op) {
        (ty, Comparison::IsNull) if !ty.is_nested() => {
            return Some(reference.is_null());
        }
        (ty, Comparison::IsNotNull) if !ty.is_nested() => {
            return Some(reference.is_not_null());
        }
        (DataType::Int32, _) => Datum::int(i32::try_from(filter.value.as_i64()?).ok()?),
        (DataType::Int64, _) => Datum::long(filter.value.as_i64()?),
        (DataType::Boolean, _) => Datum::bool(filter.value.as_bool()?),
        (DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View, _) => {
            Datum::string(filter.value.as_str()?.to_owned())
        }
        _ => return None,
    };
    Some(match filter.op {
        Comparison::Eq => reference.equal_to(value),
        Comparison::Ne => reference.not_equal_to(value),
        Comparison::Lt => reference.less_than(value),
        Comparison::Le => reference.less_than_or_equal_to(value),
        Comparison::Gt => reference.greater_than(value),
        Comparison::Ge => reference.greater_than_or_equal_to(value),
        Comparison::IsNull | Comparison::IsNotNull => unreachable!(),
    })
}

// Iceberg currently uses Arrow 58. IPC preserves field IDs, decimals and nested
// types across that boundary without relying on Rust struct layout compatibility.
fn bridge(
    schema: &IcebergSchema,
    batch: Option<&IcebergBatch>,
) -> Result<StreamReader<Cursor<Vec<u8>>>> {
    let mut writer = StreamWriter::try_new(Vec::new(), schema)?;
    if let Some(batch) = batch {
        writer.write(batch)?;
    }
    Ok(StreamReader::try_new(
        Cursor::new(writer.into_inner()?),
        None,
    )?)
}

fn external(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> DataFusionError {
    DataFusionError::External(error.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::{
        arrow::datatypes::Field,
        prelude::{col, lit},
    };

    #[test]
    fn scan_predicate_pushes_down_strings_booleans_dates_timestamps_and_conjunctions() {
        let schema = Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("active", DataType::Boolean, true),
            Field::new("d", DataType::Date32, true),
            Field::new(
                "tz",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
        ]);
        assert!(scan_predicate(&schema, &col("name").eq(lit("alice"))).is_some());
        assert!(scan_predicate(&schema, &col("active").not_eq(lit(false))).is_some());
        assert!(
            scan_predicate(&schema, &lit(ScalarValue::Date32(Some(19723))).lt(col("d"))).is_some()
        );
        assert!(
            scan_predicate(
                &schema,
                &col("name").eq(lit("alice")).and(col("tz").gt_eq(lit(
                    ScalarValue::TimestampMicrosecond(
                        Some(1_704_067_200_000_000),
                        Some("UTC".into()),
                    )
                )))
            )
            .is_some()
        );
    }

    #[test]
    fn nested_is_null_predicates_are_not_pushed_down() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("tags", DataType::new_list(DataType::Utf8, true), true),
            Field::new(
                "meta",
                DataType::Struct(vec![Field::new("k", DataType::Utf8, true)].into()),
                true,
            ),
        ]);
        assert!(scan_predicate(&schema, &col("id").is_null()).is_some());
        assert!(scan_predicate(&schema, &col("id").is_not_null()).is_some());
        assert!(scan_predicate(&schema, &col("tags").is_null()).is_none());
        assert!(scan_predicate(&schema, &col("tags").is_not_null()).is_none());
        assert!(scan_predicate(&schema, &col("meta").is_null()).is_none());
        assert!(scan_predicate(&schema, &col("meta").is_not_null()).is_none());

        let nested_read = ReadPredicate {
            column: "tags".into(),
            op: Comparison::IsNotNull,
            value: serde_json::Value::Null,
        };
        assert!(iceberg_predicate(&schema, &nested_read).is_none());
        let primitive_read = ReadPredicate {
            column: "id".into(),
            op: Comparison::IsNotNull,
            value: serde_json::Value::Null,
        };
        assert!(iceberg_predicate(&schema, &primitive_read).is_some());
    }
}
