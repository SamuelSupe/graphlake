use crate::{
    Predicate,
    catalog::ident,
    engine::{
        read_filters::{exact_terms, needs_sql_types, where_clause},
        sql_read::{Aggregate, Native, Pushdown},
    },
    source::{ReadStats, snapshot::Buffer},
};
use anyhow::{Context, Result, ensure};
use datafusion::{
    arrow::{array::RecordBatch, datatypes::SchemaRef, ipc::reader::StreamReader},
    catalog::TableProvider,
    execution::{
        memory_pool::{MemoryConsumer, MemoryLimit, MemoryReservation},
        runtime_env::RuntimeEnv,
    },
};
use duckdb::{AccessMode, Config, Connection, InterruptHandle};
use std::{
    io::Cursor,
    sync::{Arc, Mutex},
};
use tokio_util::sync::CancellationToken;

pub(crate) struct Driver {
    connection: Arc<Mutex<Connection>>,
    interrupt: Arc<InterruptHandle>,
    cancelled: CancellationToken,
    runtime: Arc<RuntimeEnv>,
    stats: Arc<ReadStats>,
    _database_memory: MemoryReservation,
}

struct Cancel {
    interrupt: Arc<InterruptHandle>,
    token: CancellationToken,
    armed: bool,
}
impl Drop for Cancel {
    fn drop(&mut self) {
        if self.armed {
            self.token.cancel();
            self.interrupt.interrupt();
        }
    }
}

impl Driver {
    pub(super) async fn open(
        path: &str,
        runtime: Arc<RuntimeEnv>,
        stats: Arc<ReadStats>,
        source_count: usize,
    ) -> Result<Self> {
        let path = path.to_owned();
        let memory = database_budget(&runtime) / source_count.max(1);
        ensure!(
            memory > 0,
            "query memory budget is too small for DuckDB sources"
        );
        let database_memory =
            MemoryConsumer::new("GraphLake DuckDB engine").register(&runtime.memory_pool);
        database_memory.try_grow(memory)?;
        let connection = tokio::task::spawn_blocking(move || -> Result<Connection> {
            ensure!(
                std::path::Path::new(&path).is_file(),
                "DuckDB file does not exist"
            );
            let config = Config::default()
                .with("temp_directory", "")?
                .enable_autoload_extension(false)?
                .access_mode(AccessMode::ReadOnly)?
                .enable_external_access(false)?
                .max_memory(&format!("{memory}B"))?
                .threads(1)?;
            let connection = Connection::open_with_flags(path, config)?;
            connection.execute_batch("BEGIN TRANSACTION")?;
            Ok(connection)
        })
        .await??;
        Ok(Self {
            interrupt: connection.interrupt_handle(),
            connection: Arc::new(Mutex::new(connection)),
            cancelled: CancellationToken::new(),
            runtime,
            stats,
            _database_memory: database_memory,
        })
    }

    async fn run<T: Send + 'static>(
        &self,
        work: impl FnOnce(&Connection) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let connection = self.connection.clone();
        let token = self.cancelled.clone();
        let mut guard = Cancel {
            interrupt: self.interrupt.clone(),
            token: token.clone(),
            armed: true,
        };
        self.stats.request();
        let result = tokio::task::spawn_blocking(move || {
            ensure!(!token.is_cancelled(), "DuckDB read cancelled");
            let connection = connection
                .lock()
                .map_err(|_| anyhow::anyhow!("DuckDB connection poisoned"))?;
            work(&connection)
        })
        .await?;
        guard.armed = false;
        result
    }

    #[cfg(feature = "admin")]
    pub(super) async fn browse(&self, namespace: Option<&str>) -> Result<Vec<String>> {
        let namespace = namespace.map(str::to_owned);
        self.run(move |connection| {
            let mut statement = connection.prepare(if namespace.is_some() {
                "SELECT table_name FROM information_schema.tables WHERE table_schema=? ORDER BY table_name LIMIT 2001"
            } else { "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name LIMIT 2001" })?;
            let params = namespace.iter().map(String::as_str).collect::<Vec<_>>();
            Ok(statement.query_map(duckdb::params_from_iter(params), |row| row.get(0))?.collect::<duckdb::Result<_>>()?)
        }).await
    }

    pub(super) async fn pushdown(
        &self,
        table: &str,
        columns: &[String],
        keys: &[String],
        operation: &Pushdown,
        batch_size: usize,
    ) -> Result<Option<super::PushedRead>> {
        let relation = super::relation(table, ident);
        let select = format!(
            "SELECT {} FROM {relation}",
            super::projection(columns, ident)
        );
        let operation = operation.clone();
        let keys = keys.to_vec();
        let runtime = self.runtime.clone();
        let stats = self.stats.clone();
        let stop = self.cancelled.clone();
        self.run(move |connection| {
            // The constraints table function scans the catalog before filtering.
            // Use the mapped table's enforced primary key directly.
            let mut constraint =
                connection.prepare("SELECT name FROM pragma_table_info(?) WHERE pk")?;
            let primary = constraint
                .query_map([&relation], |row| row.get::<_, String>(0))?
                .collect::<duckdb::Result<Vec<_>>>()?;
            drop(constraint);
            if primary.len() != keys.len() || !keys.iter().all(|key| primary.contains(key)) {
                return Ok(None);
            }

            stats.request();
            let mut metadata = connection.prepare(&format!("{select} LIMIT 0"))?;
            let columns = metadata.stream_arrow([])?;
            let schema = arrow_schema(&columns.get_schema())?;
            let integer = |name: &str| {
                schema
                    .field_with_name(name)
                    .is_ok_and(|field| field.data_type().is_integer())
            };
            if !keys.iter().all(|key| integer(key)) {
                return Ok(None);
            }
            drop(columns);
            drop(metadata);
            let Some(terms) = exact_terms(&operation.filters, &schema, ident) else {
                return Ok(None);
            };
            let condition = if terms.is_empty() {
                String::new()
            } else {
                format!(" WHERE {}", terms.join(" AND "))
            };
            stats.request();
            let pushed = match operation.operation {
                Native::Top { order, limit } => {
                    // DuckDB tries a second row-ID scan for Top-N up to 50 rows.
                    // For unfiltered, narrow integer reads one pass avoids that
                    // scan. Filters and wide columns can favor late materialization.
                    if limit <= 50
                        && order.len() == 1
                        && operation.filters.is_empty()
                        && schema.fields().len() == 2
                        && schema
                            .fields()
                            .iter()
                            .all(|field| field.data_type().is_integer())
                    {
                        stats.request();
                        connection.execute_batch("SET late_materialization_max_rows = 0")?;
                    }
                    let order = order
                        .iter()
                        .map(|(name, asc)| {
                            format!("{} {}", ident(name), if *asc { "ASC" } else { "DESC" })
                        })
                        .collect::<Vec<_>>()
                        .join(", ");
                    let sql = format!("{select}{condition} ORDER BY {order} LIMIT {limit}");
                    let (table, budget) =
                        read_table(connection, &sql, &runtime, &stats, &stop, batch_size, false)?;
                    super::PushedRead {
                        table,
                        budget,
                        aggregate: None,
                    }
                }
                Native::Aggregate { function, column } => {
                    let function = match function {
                        Aggregate::Count => "COUNT",
                        Aggregate::Min if column.as_deref().is_some_and(integer) => "MIN",
                        Aggregate::Max if column.as_deref().is_some_and(integer) => "MAX",
                        _ => return Ok(None),
                    };
                    let input = column.as_deref().map(ident).unwrap_or_else(|| "*".into());
                    let mut statement = connection.prepare(&format!(
                        "SELECT {function}({input}) FROM {relation}{condition}"
                    ))?;
                    let mut stream = statement.stream_arrow([])?;
                    let batch = stream.next().context("missing DuckDB aggregate result")??;
                    ensure!(
                        batch.num_rows() == 1,
                        "DuckDB aggregate did not return a singleton"
                    );
                    let aggregate = bridge_batch(&batch)?;
                    stats.warehouse_batch(&aggregate);
                    let (table, budget) =
                        Buffer::new(&runtime.memory_pool, None).finish(schema, &stats)?;
                    super::PushedRead {
                        table,
                        budget,
                        aggregate: Some(aggregate),
                    }
                }
            };
            Ok(Some(pushed))
        })
        .await
        .context("pushing SQL into DuckDB")
    }

    pub(super) async fn table(
        &self,
        table: &str,
        columns: &[String],
        filters: &[Predicate],
        batch_size: usize,
        explain: bool,
    ) -> Result<(Arc<dyn TableProvider>, MemoryReservation)> {
        let mut sql = format!(
            "SELECT {} FROM {}{}",
            super::projection(columns, ident),
            super::relation(table, ident),
            if explain { " LIMIT 0" } else { "" }
        );
        let runtime = self.runtime.clone();
        let stats = self.stats.clone();
        let stop = self.cancelled.clone();
        let filters = filters.to_vec();
        self.run(move |connection| {
            if !explain && needs_sql_types(&filters) {
                let mut metadata = connection.prepare(&format!("{sql} LIMIT 0"))?;
                let stream = metadata.stream_arrow([])?;
                let schema = arrow_schema(&stream.get_schema())?;
                sql.push_str(&where_clause(&filters, &schema, ident));
            }
            read_table(
                connection, &sql, &runtime, &stats, &stop, batch_size, explain,
            )
        })
        .await
        .context("reading DuckDB table")
    }
}

fn database_budget(runtime: &RuntimeEnv) -> usize {
    match runtime.memory_pool.memory_limit() {
        MemoryLimit::Finite(bytes) => bytes / 2,
        _ => 128 * 1024 * 1024,
    }
}

fn arrow_schema(schema: &duckdb::arrow::datatypes::Schema) -> Result<SchemaRef> {
    let mut writer = arrow_ipc_60::writer::StreamWriter::try_new(Vec::new(), schema)?;
    writer.finish()?;
    Ok(StreamReader::try_new(Cursor::new(writer.into_inner()?), None)?.schema())
}

fn bridge_batch(batch: &duckdb::arrow::array::RecordBatch) -> Result<RecordBatch> {
    let mut writer = arrow_ipc_60::writer::StreamWriter::try_new(Vec::new(), &batch.schema())?;
    writer.write(batch)?;
    writer.finish()?;
    let mut reader = StreamReader::try_new(Cursor::new(writer.into_inner()?), None)?;
    Ok(reader.next().context("missing DuckDB Arrow batch")??)
}

fn read_table(
    connection: &Connection,
    sql: &str,
    runtime: &RuntimeEnv,
    stats: &ReadStats,
    stop: &CancellationToken,
    batch_size: usize,
    explain: bool,
) -> Result<(Arc<dyn TableProvider>, MemoryReservation)> {
    let mut statement = connection.prepare(sql)?;
    let stream = statement.stream_arrow([])?;
    let schema = arrow_schema(&stream.get_schema())?;
    let mut buffer = Buffer::new(
        &runtime.memory_pool,
        (!explain).then_some(&runtime.disk_manager),
    )
    .with_source_reservation(database_budget(runtime));
    if explain {
        return buffer.finish_schema(schema);
    }
    let raw = MemoryConsumer::new("GraphLake DuckDB batch").register(&runtime.memory_pool);
    for batch in stream {
        ensure!(!stop.is_cancelled(), "DuckDB read cancelled");
        let batch = batch?;
        raw.try_grow(batch.get_array_memory_size().saturating_mul(3))?;
        let batch = bridge_batch(&batch)?;
        stats.warehouse_batch(&batch);
        buffer.push_chunks(batch, batch_size)?;
        raw.free();
    }
    buffer.finish(schema, stats)
}
