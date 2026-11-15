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
    common::ScalarValue,
    execution::{
        memory_pool::{MemoryConsumer, MemoryReservation},
        runtime_env::RuntimeEnv,
    },
};
use rusqlite::{Connection, OpenFlags, types::ValueRef};
use tokio_util::sync::CancellationToken;

use super::{ReadStats, snapshot::Buffer};
use crate::{Predicate, catalog::ident};

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
        stats.request();
        let connection = tokio::task::spawn_blocking(move || -> Result<Connection> {
            ensure!(!stop.is_cancelled(), "SQLite read cancelled");
            let mut connection = Connection::open_with_flags(
                path,
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )?;
            connection.busy_timeout(timeout.min(Duration::from_millis(100)))?;
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
        &self,
        relation: &str,
        columns: &[String],
        filters: &[Predicate],
        runtime: &Arc<RuntimeEnv>,
        batch_size: usize,
        explain_only: bool,
    ) -> Result<(Arc<dyn TableProvider>, MemoryReservation)> {
        let relation = relation.to_owned();
        let table_name = relation.clone();
        let columns = columns.to_vec();
        let filters = filters.to_vec();
        let runtime = Arc::clone(runtime);
        let stats = Arc::clone(&self.stats);
        let stop = self.cancelled.clone();
        self.run(move |connection| {
            let schema = schema(connection, &relation, &columns)?;
            let mut buffer = Buffer::new(
                &runtime.memory_pool,
                (!explain_only).then_some(&runtime.disk_manager),
            );
            if explain_only {
                return buffer.finish_schema(schema);
            }
            let sql = format!(
                "SELECT {} FROM {}{}",
                schema
                    .fields()
                    .iter()
                    .map(|field| ident(field.name()))
                    .collect::<Vec<_>>()
                    .join(", "),
                ident(&relation),
                crate::engine::read_filters::where_clause(&filters, &schema, ident)
            );
            let mut statement = connection.prepare(&sql)?;
            let mut rows = statement.query([])?;
            let raw = MemoryConsumer::new("GraphLake SQLite batch").register(&runtime.memory_pool);
            let mut values = vec![Vec::new(); schema.fields().len()];
            let mut count = 0;
            loop {
                ensure!(!stop.is_cancelled(), "SQLite read cancelled");
                let row = rows.next()?;
                let done = row.is_none();
                if let Some(row) = row {
                    for (index, field) in schema.fields().iter().enumerate() {
                        let value = row.get_ref(index)?;
                        let bytes = match value {
                            ValueRef::Text(bytes) | ValueRef::Blob(bytes) => bytes.len(),
                            _ => 0,
                        };
                        raw.try_grow(bytes.saturating_add(2 * size_of::<ScalarValue>()))?;
                        values[index].push(scalar(value, field)?);
                    }
                    count += 1;
                }
                if count > 0 && (done || count == batch_size) {
                    let arrays = values
                        .iter_mut()
                        .map(|values| ScalarValue::iter_to_array(values.drain(..)))
                        .collect::<std::result::Result<Vec<_>, _>>()?;
                    let batch = RecordBatch::try_new(Arc::clone(&schema), arrays)?;
                    stats.sqlite_batch(&batch);
                    buffer.push(batch)?;
                    raw.free();
                    count = 0;
                }
                if done {
                    break;
                }
            }
            buffer.finish(schema, &stats)
        })
        .await
        .with_context(|| format!("reading SQLite table {table_name}"))
    }
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
