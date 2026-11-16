use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use datafusion::{
    arrow::{
        array::RecordBatch,
        datatypes::{DataType, Field, Schema, SchemaRef},
    },
    catalog::TableProvider,
    common::{DataFusionError, ScalarValue},
    execution::{
        memory_pool::{MemoryConsumer, MemoryReservation},
        runtime_env::RuntimeEnv,
    },
    physical_plan::stream::RecordBatchStreamAdapter,
};
use rusqlite::{
    Connection, OpenFlags, OptionalExtension, StatementStatus, params_from_iter,
    types::{Value, ValueRef},
};
use tokio_util::sync::CancellationToken;

use super::{ReadStats, snapshot::Buffer};
use crate::{
    Comparison, Predicate,
    catalog::ident,
    engine::{
        read_filters,
        sql_read::{Native, Pushdown},
    },
};

pub(crate) struct Snapshot {
    connection: Arc<Mutex<Connection>>,
    cancelled: CancellationToken,
    stats: Arc<ReadStats>,
    pub(crate) version: String,
}

impl Drop for Snapshot {
    fn drop(&mut self) {
        self.cancelled.cancel();
    }
}

impl Snapshot {
    pub(crate) async fn open(path: &str, timeout: Duration, stats: Arc<ReadStats>) -> Result<Self> {
        let cancelled = CancellationToken::new();
        let guard = cancelled.clone().drop_guard();
        let stop = cancelled.clone();
        let path = path.to_owned();
        let started = std::time::Instant::now();
        stats.request();
        let connection = tokio::task::spawn_blocking(move || -> Result<Connection> {
            ensure!(!stop.is_cancelled(), "SQLite read cancelled");
            let mut connection = Connection::open_with_flags(
                path,
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )?;
            let busy = stop.clone();
            connection.busy_handler(Some(move |_| {
                let left = timeout.saturating_sub(started.elapsed());
                if left.is_zero() || busy.is_cancelled() {
                    return false;
                }
                std::thread::sleep(left.min(Duration::from_millis(10)));
                !busy.is_cancelled() && started.elapsed() < timeout
            }))?;
            connection.progress_handler(1000, Some(move || stop.is_cancelled()))?;
            connection.execute_batch("PRAGMA query_only=ON; PRAGMA trusted_schema=OFF; BEGIN")?;
            // BEGIN is deferred: reading sqlite_schema pins the snapshot before
            // any mapped table is described or scanned.
            connection.query_row("SELECT count(*) FROM sqlite_schema", [], |_| Ok(()))?;
            Ok(connection)
        })
        .await??;
        guard.disarm();
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
            cancelled,
            stats,
            version: uuid::Uuid::new_v4().to_string(),
        })
    }

    async fn run<T: Send + 'static>(
        &self,
        work: impl FnOnce(&Connection) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let connection = Arc::clone(&self.connection);
        let stop = self.cancelled.clone();
        let guard = stop.clone().drop_guard();
        self.stats.request();
        let result = tokio::task::spawn_blocking(move || {
            ensure!(!stop.is_cancelled(), "SQLite read cancelled");
            let connection = connection
                .lock()
                .map_err(|_| anyhow::anyhow!("SQLite connection poisoned"))?;
            work(&connection)
        })
        .await?;
        guard.disarm();
        result
    }

    #[cfg(feature = "admin")]
    pub(crate) async fn browse(&self) -> Result<Vec<String>> {
        self.run(|connection| {
            Ok(connection.prepare("SELECT name FROM sqlite_schema WHERE type IN ('table','view') AND name NOT LIKE 'sqlite_%' ORDER BY name LIMIT 2001")?
                .query_map([], |row| row.get(0))?.collect::<rusqlite::Result<Vec<_>>>()?)
        }).await
    }

    pub(crate) async fn table(
        self: &Arc<Self>,
        relation: &str,
        columns: &[String],
        filters: &[Predicate],
        native: Option<(&[String], &Pushdown)>,
        stream: bool,
        raw_table: bool,
        runtime: &Arc<RuntimeEnv>,
        batch_size: usize,
        explain_only: bool,
    ) -> Result<(Arc<dyn TableProvider>, MemoryReservation, bool)> {
        let name = relation.to_owned();
        let columns = columns.to_vec();
        let filters = filters.to_vec();
        let native = native.map(|(keys, plan)| (keys.to_vec(), plan.clone()));
        let snapshot = self.clone();
        let runtime = runtime.clone();
        self.run(move |connection| {
            let schema = schema(connection, &name, &columns)?;
            let mut buffer = Buffer::new(
                &runtime.memory_pool,
                (!explain_only).then_some(&runtime.disk_manager),
            );
            if explain_only {
                let (table, budget) = buffer.finish_schema(schema)?;
                return Ok((table, budget, false));
            }
            let pushed = native
                .as_ref()
                .map(|(keys, plan)| native_scan(connection, &name, &schema, keys, plan))
                .transpose()?
                .flatten();
            let proven = pushed.is_some();
            let (suffix, params) = pushed.unwrap_or_else(|| {
                (
                    read_filters::where_clause(&filters, &schema, ident),
                    Vec::new(),
                )
            });
            let selected = schema
                .fields()
                .iter()
                .map(|field| ident(field.name()))
                .collect::<Vec<_>>()
                .join(", ");
            let scan = Scan {
                schema: schema.clone(),
                sql: format!("SELECT {selected} FROM {}{suffix}", ident(&name)),
                params,
                batch_size,
            };
            if stream && (proven || raw_table) {
                let table = super::snapshot::stream_once(schema, move |context| {
                    let schema = scan.schema.clone();
                    let cancellation = snapshot.cancelled.clone().drop_guard();
                    let pool = context.runtime_env().memory_pool.clone();
                    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
                    let task = tokio::task::spawn_blocking(move || {
                        let connection = snapshot
                            .connection
                            .lock()
                            .map_err(|_| anyhow::anyhow!("SQLite connection poisoned"))?;
                        snapshot.stats.request();
                        scan.read(&connection, &snapshot, &pool, |batch| {
                            let memory =
                                MemoryConsumer::new("GraphLake SQLite output").register(&pool);
                            memory.try_grow(batch.get_array_memory_size())?;
                            Ok(sender.blocking_send((batch, memory)).is_ok())
                        })
                    });
                    Box::pin(RecordBatchStreamAdapter::new(
                        schema,
                        async_stream::try_stream! {
                            let _cancel_on_drop = cancellation;
                            while let Some((batch, _memory)) = receiver.recv().await {
                                yield batch;
                            }
                            task.await.map_err(|error| DataFusionError::External(error.into()))?
                                .map_err(|error| DataFusionError::External(error.into()))?;
                        },
                    ))
                })?;
                let budget =
                    MemoryConsumer::new("GraphLake SQLite stream").register(&runtime.memory_pool);
                return Ok((table, budget, proven));
            }
            scan.read(connection, &snapshot, &runtime.memory_pool, |batch| {
                buffer.push(batch)?;
                Ok(true)
            })?;
            let (table, budget) = buffer.finish(schema, &snapshot.stats)?;
            Ok((table, budget, proven))
        })
        .await
        .with_context(|| format!("reading SQLite table {relation}"))
    }
}

struct Scan {
    schema: SchemaRef,
    sql: String,
    params: Vec<Value>,
    batch_size: usize,
}

impl Scan {
    fn read(
        self,
        connection: &Connection,
        snapshot: &Snapshot,
        pool: &Arc<dyn datafusion::execution::memory_pool::MemoryPool>,
        mut emit: impl FnMut(RecordBatch) -> Result<bool>,
    ) -> Result<()> {
        let mut statement = connection.prepare(&self.sql)?;
        let mut rows = statement.query(params_from_iter(self.params))?;
        let raw = MemoryConsumer::new("GraphLake SQLite batch").register(pool);
        let mut columns = vec![Vec::new(); self.schema.fields().len()];
        let mut count = 0;
        loop {
            ensure!(!snapshot.cancelled.is_cancelled(), "SQLite read cancelled");
            let row = rows.next()?;
            let done = row.is_none();
            if let Some(row) = row {
                for (index, field) in self.schema.fields().iter().enumerate() {
                    let value = row.get_ref(index)?;
                    let bytes = match value {
                        ValueRef::Text(bytes) | ValueRef::Blob(bytes) => bytes.len(),
                        _ => 0,
                    };
                    raw.try_grow(bytes.saturating_add(2 * size_of::<ScalarValue>()))?;
                    columns[index].push(scalar(value, field)?);
                }
                count += 1;
            }
            if count > 0 && (done || count == self.batch_size) {
                let arrays = columns
                    .iter_mut()
                    .map(|values| ScalarValue::iter_to_array(values.drain(..)))
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                let batch = RecordBatch::try_new(self.schema.clone(), arrays)?;
                snapshot.stats.sqlite_batch(&batch);
                if let Some(statement) = rows.as_ref() {
                    snapshot
                        .stats
                        .sqlite_fullscan(statement.reset_status(StatementStatus::FullscanStep));
                }
                raw.free();
                if !emit(batch)? {
                    break;
                }
                count = 0;
            }
            if done {
                break;
            }
        }
        drop(rows);
        snapshot
            .stats
            .sqlite_fullscan(statement.get_status(StatementStatus::FullscanStep));
        Ok(())
    }
}

fn native_scan(
    connection: &Connection,
    relation: &str,
    schema: &Schema,
    keys: &[String],
    plan: &Pushdown,
) -> Result<Option<(String, Vec<Value>)>> {
    let suffix = match &plan.operation {
        Native::Filter => String::new(),
        Native::Top { order, limit } => {
            if !order.iter().all(|(name, _)| {
                schema
                    .field_with_name(name)
                    .is_ok_and(|field| field.data_type().is_integer())
            }) {
                return Ok(None);
            }
            format!("{} LIMIT {limit}", super::order_by(order, ident))
        }
        Native::Aggregate { .. } => return Ok(None),
    };
    let Some((strict, without_rowid)) = connection
        .query_row(
            "SELECT strict, wr FROM pragma_table_list(?1) WHERE schema='main' AND type='table'",
            [relation],
            |row| Ok((row.get::<_, bool>(0)?, row.get::<_, bool>(1)?)),
        )
        .optional()?
    else {
        return Ok(None);
    };
    let mut primary = connection
        .prepare("SELECT name, type FROM pragma_table_xinfo(?1) WHERE pk > 0 ORDER BY pk")?;
    let primary = primary
        .query_map([relation], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if primary.len() != keys.len() || !primary.iter().all(|(name, _)| keys.contains(name)) {
        return Ok(None);
    }
    if !strict {
        // Only a rowid alias guarantees integer storage in a legacy rowid table.
        // INTEGER PRIMARY KEY DESC has a separate PK index and is not an alias.
        let rowid_alias = !without_rowid
            && primary.len() == 1
            && primary[0].1.eq_ignore_ascii_case("INTEGER")
            && schema
                .fields()
                .iter()
                .all(|field| field.name() == &primary[0].0)
            && connection.query_row(
                "SELECT NOT EXISTS(SELECT 1 FROM pragma_index_list(?1) WHERE origin='pk')",
                [relation],
                |row| row.get::<_, bool>(0),
            )?;
        if !rowid_alias {
            return Ok(None);
        }
    }
    // Bypass the decoder only for columns whose storage class is fixed.
    if !schema.fields().iter().all(|field| {
        matches!(
            field.data_type(),
            DataType::Int64 | DataType::Utf8 | DataType::Binary
        )
    }) {
        return Ok(None);
    }
    let mut terms = keys
        .iter()
        .map(|key| format!("{} IS NOT NULL", ident(key)))
        .collect::<Vec<_>>();
    let mut params = Vec::new();
    for predicate in &plan.filters {
        if let Some(term) = read_filters::sql_term(predicate, schema, ident) {
            terms.push(term);
        } else if predicate.op == Comparison::Eq
            && schema
                .field_with_name(&predicate.column)
                .is_ok_and(|field| field.data_type() == &DataType::Utf8)
            && let Some(value) = predicate.value.as_str()
        {
            terms.push(format!("{} COLLATE BINARY = ?", ident(&predicate.column)));
            params.push(Value::Text(value.to_owned()));
        } else {
            return Ok(None);
        }
    }
    Ok(Some((
        format!(" WHERE {}{suffix}", terms.join(" AND ")),
        params,
    )))
}

fn schema(connection: &Connection, relation: &str, columns: &[String]) -> Result<SchemaRef> {
    let mut statement = connection.prepare(
        "SELECT name, type, \"notnull\" FROM pragma_table_xinfo(?1) WHERE hidden <> 1 ORDER BY cid",
    )?;
    let metadata = statement
        .query_map([relation], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, bool>(2)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    ensure!(!metadata.is_empty(), "unknown SQLite table {relation}");
    let requested = if columns.is_empty() {
        metadata.iter().map(|(name, _, _)| name.clone()).collect()
    } else {
        columns.to_vec()
    };
    let fields = requested.iter().map(|name| {
        let (_, kind, required) = metadata.iter().find(|(column, _, _)| column == name)
            .with_context(|| format!("missing SQLite column {relation}.{name}"))?;
        let declared = kind.to_ascii_uppercase();
        let kind = if declared == "BOOLEAN" || declared == "BOOL" { DataType::Boolean }
        else if declared.contains("INT") { DataType::Int64 }
        else if ["CHAR", "CLOB", "TEXT"].iter().any(|part| declared.contains(part)) { DataType::Utf8 }
        else if ["REAL", "FLOA", "DOUB"].iter().any(|part| declared.contains(part)) { DataType::Float64 }
        else if declared == "BLOB" { DataType::Binary }
        else { bail!("SQLite column {relation}.{name} needs a supported declared type, got {declared:?}"); };
        Ok(Field::new(name, kind, !required))
    }).collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

fn scalar(value: ValueRef<'_>, field: &Field) -> Result<ScalarValue> {
    Ok(match (value, field.data_type()) {
        (ValueRef::Null, kind) => {
            ensure!(
                field.is_nullable(),
                "NULL in required SQLite column {}",
                field.name()
            );
            ScalarValue::try_from(kind)?
        }
        (ValueRef::Integer(value), DataType::Int64) => ScalarValue::Int64(Some(value)),
        (ValueRef::Integer(value @ (0 | 1)), DataType::Boolean) => {
            ScalarValue::Boolean(Some(value == 1))
        }
        (ValueRef::Real(value), DataType::Float64) => ScalarValue::Float64(Some(value)),
        (ValueRef::Integer(value), DataType::Float64) => ScalarValue::Float64(Some(value as f64)),
        (ValueRef::Text(value), DataType::Utf8) => {
            ScalarValue::Utf8(Some(std::str::from_utf8(value)?.to_owned()))
        }
        (ValueRef::Blob(value), DataType::Binary) => ScalarValue::Binary(Some(value.to_vec())),
        _ => bail!(
            "SQLite value does not match declared type for column {}",
            field.name()
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn read_transaction_keeps_snapshot_and_cannot_write() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("snapshot.db");
        let writer = Connection::open(&path)?;
        writer.execute_batch("PRAGMA journal_mode=WAL; CREATE TABLE a (id INTEGER); CREATE TABLE b (id INTEGER); INSERT INTO a VALUES (1); INSERT INTO b VALUES (2);")?;
        let snapshot = Snapshot::open(
            path.to_str().unwrap(),
            Duration::from_secs(1),
            Arc::new(ReadStats::default()),
        )
        .await?;
        writer.execute_batch("BEGIN; UPDATE a SET id=3; UPDATE b SET id=4; COMMIT;")?;
        let read = |connection: &Connection| -> Result<(i64, i64)> {
            Ok((
                connection.query_row("SELECT id FROM a", [], |r| r.get(0))?,
                connection.query_row("SELECT id FROM b", [], |r| r.get(0))?,
            ))
        };
        assert_eq!(snapshot.run(read).await?, (1, 2));
        assert!(
            snapshot
                .run(|connection| {
                    connection.execute("DELETE FROM a", [])?;
                    Ok(())
                })
                .await
                .is_err()
        );
        drop(snapshot);
        let fresh = Snapshot::open(
            path.to_str().unwrap(),
            Duration::from_secs(1),
            Arc::new(ReadStats::default()),
        )
        .await?;
        assert_eq!(fresh.run(read).await?, (3, 4));
        Ok(())
    }

    #[tokio::test]
    async fn cancelling_a_read_interrupts_sqlite_work() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("cancel.db");
        let writer = Connection::open(&path)?;
        writer.execute_batch("CREATE TABLE n (id INTEGER); WITH RECURSIVE x(id) AS (VALUES(1) UNION ALL SELECT id+1 FROM x WHERE id<1000) INSERT INTO n SELECT id FROM x;")?;
        let snapshot = Snapshot::open(
            path.to_str().unwrap(),
            Duration::from_secs(1),
            Arc::new(ReadStats::default()),
        )
        .await?;
        let (finished, done) = tokio::sync::oneshot::channel();
        let result = tokio::time::timeout(
            Duration::from_millis(50),
            snapshot.run(move |connection| {
                let result =
                    connection.query_row("SELECT sum(a.id) FROM n a, n b, n c", [], |row| {
                        row.get::<_, i64>(0)
                    });
                let _ = finished.send(result.is_err());
                Ok(())
            }),
        )
        .await;
        assert!(result.is_err());
        assert!(tokio::time::timeout(Duration::from_secs(2), done).await??);
        Ok(())
    }
}
