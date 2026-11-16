use std::{collections::HashMap, fmt, io::Cursor, sync::Arc};

use anyhow::{Context, Result, ensure};
use arrow_array_58::RecordBatch as IcebergBatch;
use arrow_ipc_58::writer::StreamWriter;
use arrow_schema_58::Schema as IcebergSchema;
use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{RecordBatch, RecordBatchOptions},
        datatypes::{DataType, Schema, SchemaRef},
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

pub(crate) async fn catalog(
    name: &str,
    uri: &str,
    warehouse: Option<&str>,
    token_env: Option<&str>,
) -> Result<RestCatalog> {
    let mut props = HashMap::from([("uri".into(), uri.into())]);
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
    Ok(RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalResolvingStorageFactory::new()))
        .load(name, props)
        .await?)
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
            consistency: "Iceberg snapshot",
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
    let (column, op, value) = match expr {
        Expr::IsNull(expr) => (expr.as_ref(), Comparison::IsNull, serde_json::Value::Null),
        Expr::IsNotNull(expr) => (
            expr.as_ref(),
            Comparison::IsNotNull,
            serde_json::Value::Null,
        ),
        Expr::BinaryExpr(expr) => {
            let Expr::Literal(literal, _) = expr.right.as_ref() else {
                return None;
            };
            let value = match literal {
                ScalarValue::Int32(Some(value)) => i64::from(*value),
                ScalarValue::Int64(Some(value)) => *value,
                _ => return None,
            };
            let comparison = match expr.op {
                Operator::Eq => Comparison::Eq,
                Operator::Lt => Comparison::Lt,
                Operator::LtEq => Comparison::Le,
                Operator::Gt => Comparison::Gt,
                Operator::GtEq => Comparison::Ge,
                _ => return None,
            };
            (expr.left.as_ref(), comparison, value.into())
        }
        _ => return None,
    };
    let Expr::Column(column) = column else {
        return None;
    };
    iceberg_predicate(
        schema,
        &ReadPredicate {
            column: column.name.clone(),
            op,
            value,
        },
    )
}

fn iceberg_predicate(schema: &Schema, filter: &ReadPredicate) -> Option<Predicate> {
    if filter.column.contains('.') {
        return None;
    }
    let kind = schema.field_with_name(&filter.column).ok()?.data_type();
    let reference = Reference::new(&filter.column);
    let value = match (kind, filter.op) {
        (DataType::Int32 | DataType::Int64, Comparison::IsNull) => {
            return Some(reference.is_null());
        }
        (DataType::Int32 | DataType::Int64, Comparison::IsNotNull) => {
            return Some(reference.is_not_null());
        }
        (DataType::Int32, _) => Datum::int(i32::try_from(filter.value.as_i64()?).ok()?),
        (DataType::Int64, _) => Datum::long(filter.value.as_i64()?),
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
