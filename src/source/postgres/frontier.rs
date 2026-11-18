use std::sync::Arc;

use anyhow::{Result, bail, ensure};
use datafusion::{
    arrow::datatypes::{DataType, SchemaRef},
    common::ScalarValue,
    execution::{SendableRecordBatchStream, TaskContext, memory_pool::MemoryConsumer},
    physical_plan::stream::RecordBatchStreamAdapter,
};
use futures::TryStreamExt;
use tokio_postgres::types::{ToSql, Type};

use super::{PgTable, Row, external, ident, to_batch};

impl PgTable {
    /// Return estimated table rows if the lookup has a usable index prefix.
    pub(crate) async fn index_estimate(&self, columns: &[String]) -> Result<Option<u64>> {
        if !columns.iter().all(|column| {
            self.schema.index_of(column).is_ok_and(|i| {
                matches!(
                    self.types[i],
                    Type::BOOL
                        | Type::INT2
                        | Type::INT4
                        | Type::INT8
                        | Type::TEXT
                        | Type::VARCHAR
                        | Type::UUID
                        | Type::BYTEA
                )
            })
        }) {
            return Ok(None);
        }
        let width = i64::try_from(columns.len())?;
        self.snapshot.stats.request();
        let row = self
            .snapshot
            .request(self.snapshot.client.query_opt(
                "SELECT t.reltuples::float8 FROM pg_class t
             WHERE t.oid = to_regclass($1) AND EXISTS (
               SELECT FROM pg_index i JOIN pg_class ix ON ix.oid = i.indexrelid
               JOIN pg_am am ON am.oid = ix.relam
               WHERE i.indrelid = t.oid AND i.indisvalid AND i.indisready
                 AND i.indpred IS NULL AND i.indnkeyatts >= $2::bigint AND am.amname = 'btree'
                 AND ARRAY(
                   SELECT a.attname::text
                   FROM unnest(i.indkey) WITH ORDINALITY k(attnum, ordinal)
                   JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum
                   WHERE k.ordinal <= $2 ORDER BY a.attname::text COLLATE \"C\"
                 ) = ARRAY(SELECT key FROM unnest($3::text[]) AS key ORDER BY key COLLATE \"C\")
             )",
                &[&self.relation, &width, &columns],
            ))
            .await?;
        Ok(row
            .map(|row| row.try_get::<_, f64>(0))
            .transpose()?
            .map(|rows| rows.max(0.0) as u64))
    }

    pub(crate) fn lookup(
        &self,
        key_columns: &[String],
        keys: &[Vec<ScalarValue>],
        selected: &[String],
        schema: SchemaRef,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        ensure!(
            !keys.is_empty() && !key_columns.is_empty(),
            "empty frontier lookup"
        );
        ensure!(
            key_columns.len().saturating_mul(keys.len()) <= 32_000,
            "too many frontier lookup parameters"
        );
        let types = key_columns
            .iter()
            .map(|key| self.schema.index_of(key).map(|i| &self.types[i]))
            .collect::<Result<Vec<_>, _>>()?;
        let mut args = Vec::new();
        let mut tuples = Vec::new();
        for key in keys {
            ensure!(key.len() == types.len(), "frontier key arity mismatch");
            let mut fields = Vec::new();
            for (value, ty) in key.iter().zip(&types) {
                args.push(parameter(value, ty)?);
                fields.push(format!("${}", args.len()));
            }
            tuples.push(format!("({})", fields.join(", ")));
        }
        let selected = selected
            .iter()
            .zip(schema.fields())
            .map(|(name, field)| {
                let name = ident(name);
                if matches!(field.data_type(), DataType::Decimal128(..)) {
                    format!("{name}::text")
                } else {
                    name
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
        let key_columns = key_columns
            .iter()
            .map(|col| ident(col))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT {selected} FROM {} WHERE ({key_columns}) IN ({})",
            self.relation,
            tuples.join(", ")
        );
        let snapshot = Arc::clone(&self.snapshot);
        let output = Arc::clone(&schema);
        let stream = async_stream::try_stream! {
            snapshot.stats.frontier_request();
            let mut request = snapshot.active_request();
            let result = snapshot.client.query_raw(sql.as_str(), args.iter().map(|arg| arg.as_ref() as &(dyn ToSql + Sync)))
                .await.map_err(external)?;
            futures::pin_mut!(result);
            let batch_size = context.session_config().batch_size();
            let pool = &context.runtime_env().memory_pool;
            let raw = MemoryConsumer::new("PostgreSQL frontier rows").register(pool);
            let output = MemoryConsumer::new("PostgreSQL frontier batch").register(pool);
            let mut rows = Vec::new();
            loop {
                let row = result.try_next().await.map_err(external)?;
                let done = row.is_none();
                if done { request.finish(); }
                if let Some(row) = row {
                    raw.try_grow(row.raw_size_bytes().saturating_add(std::mem::size_of::<Row>()))?;
                    rows.push(row);
                }
                if !rows.is_empty() && (done || rows.len() == batch_size) {
                    let batch = to_batch(Arc::clone(&schema), &rows).map_err(external)?;
                    output.try_grow(batch.get_array_memory_size())?;
                    snapshot.stats.batch(&batch);
                    rows.clear();
                    raw.free();
                    yield batch;
                    output.free();
                }
                if done { break; }
            }
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(output, stream)))
    }
}

fn parameter(value: &ScalarValue, ty: &Type) -> Result<Box<dyn ToSql + Send + Sync>> {
    Ok(match (value, ty) {
        (ScalarValue::Boolean(Some(value)), &Type::BOOL) => Box::new(*value),
        (ScalarValue::Int16(Some(value)), &Type::INT2) => Box::new(*value),
        (ScalarValue::Int32(Some(value)), &Type::INT4) => Box::new(*value),
        (ScalarValue::Int64(Some(value)), &Type::INT8) => Box::new(*value),
        (ScalarValue::Utf8(Some(value)), &Type::TEXT | &Type::VARCHAR) => Box::new(value.clone()),
        (ScalarValue::Utf8(Some(value)), &Type::UUID) => Box::new(uuid::Uuid::parse_str(value)?),
        (ScalarValue::Binary(Some(value)), &Type::BYTEA) => Box::new(value.clone()),
        _ => bail!(
            "unsupported PostgreSQL frontier parameter type {ty} for {}",
            value.data_type()
        ),
    })
}
