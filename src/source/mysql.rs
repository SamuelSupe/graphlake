use std::{collections::BTreeMap, mem::size_of, path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime};
use datafusion::{
    arrow::{
        array::{
            ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
            Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, RecordBatch, StringArray,
            TimestampMicrosecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
        },
        datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
    },
    catalog::TableProvider,
    common::{DataFusionError, ScalarValue},
    execution::{
        memory_pool::{MemoryConsumer, MemoryReservation},
        runtime_env::RuntimeEnv,
    },
    physical_plan::stream::RecordBatchStreamAdapter,
};
use futures::{TryStreamExt, stream::BoxStream};
use mysql_async::{
    Conn, Opts, OptsBuilder, Row, Value,
    prelude::{FromValue, Queryable},
};

use super::{ReadStats, decimal128};
use crate::{
    Comparison,
    engine::{
        read_filters,
        sql_read::{Native, Pushdown},
    },
};

pub(crate) struct Snapshot {
    conn: Option<Conn>,
    stats: Arc<ReadStats>,
    pub(crate) version: String,
    transactional: bool,
    server_uuid: Option<String>,
}

impl Snapshot {
    #[cfg(feature = "admin")]
    pub(crate) async fn browse(&mut self, database: Option<&str>) -> Result<Vec<String>> {
        self.stats.request();
        Ok(if let Some(database) = database {
            self.conn.as_mut().expect("open connection").query(format!(
                "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA={}{} ORDER BY TABLE_NAME LIMIT 2001",
                literal(database), if self.transactional { " AND ENGINE='InnoDB'" } else { "" },
            )).await?
        } else {
            self.conn.as_mut().expect("open connection").query("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA ORDER BY SCHEMA_NAME LIMIT 2001").await?
        })
    }

    #[cfg(feature = "admin")]
    pub(crate) async fn describe(&mut self, relation: &str) -> Result<Vec<(String, String, bool)>> {
        let (database, table) = relation
            .split_once('.')
            .context("expected MySQL database.table")?;
        self.stats.request();
        let columns = self.conn.as_mut().expect("open connection").query_map(
            format!("SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA={} AND TABLE_NAME={} ORDER BY ORDINAL_POSITION", literal(database), literal(table)),
            |(name, kind, nullable): (String, String, String)| (name, kind, nullable == "YES"),
        ).await?;
        ensure!(!columns.is_empty(), "MySQL relation has no visible columns");
        Ok(columns)
    }
    pub(crate) async fn open_source(
        source: &crate::Source,
        timeout: Duration,
        stats: Arc<ReadStats>,
    ) -> Result<Self> {
        use crate::Source;
        let (dsn_env, tls, ca_file, dialect) = match source {
            Source::Mysql {
                dsn_env,
                tls,
                ca_file,
            } => (dsn_env, *tls, ca_file.as_deref(), "mysql"),
            Source::Doris(c) => (&c.dsn_env, c.tls, c.ca_file.as_deref(), "doris"),
            Source::Starrocks(c) => (&c.dsn_env, c.tls, c.ca_file.as_deref(), "starrocks"),
            Source::Tidb(c) => (&c.dsn_env, c.tls, c.ca_file.as_deref(), "tidb"),
            Source::Oceanbase(c) => (&c.dsn_env, c.tls, c.ca_file.as_deref(), "oceanbase"),
            Source::Mariadb(c) => (&c.dsn_env, c.tls, c.ca_file.as_deref(), "mariadb"),
            _ => bail!("expected MySQL protocol source"),
        };
        let transactional = matches!(dialect, "mysql" | "mariadb");
        let dsn =
            std::env::var(dsn_env).with_context(|| format!("missing MySQL DSN in {dsn_env}"))?;
        let opts =
            Opts::from_url(&dsn).map_err(|_| anyhow::anyhow!("invalid MySQL DSN in {dsn_env}"))?;
        ensure!(tls || ca_file.is_none(), "MySQL ca_file requires TLS");
        let ssl = tls.then(|| {
            let ssl = opts.ssl_opts().cloned().unwrap_or_default();
            match ca_file {
                Some(path) => ssl.with_root_certs(vec![PathBuf::from(path).into()]),
                None => ssl,
            }
        });
        ensure!(
            ssl.as_ref()
                .is_none_or(|ssl| !ssl.accept_invalid_certs() && !ssl.skip_domain_validation()),
            "MySQL TLS requires certificate and hostname verification"
        );
        ensure!(
            !tls || opts.socket().is_none(),
            "MySQL TLS requires a TCP connection"
        );
        // mysql_async uses the process default. Both providers are enabled by dependencies.
        if tls && rustls::crypto::CryptoProvider::get_default().is_none() {
            let _ = rustls::crypto::ring::default_provider().install_default();
        }
        let builder = OptsBuilder::from_opts(opts)
            .prefer_socket(false)
            .ssl_opts(ssl);
        let mut conn = tokio::time::timeout(timeout, Conn::new(builder))
            .await
            .context("MySQL connection timed out")?
            .context("opening MySQL connection")?;
        let timeout_ms = timeout.as_millis().clamp(1, u32::MAX as u128);
        let mut setup = vec![
            "SET NAMES utf8mb4".to_owned(),
            "SET SESSION time_zone = '+00:00'".to_owned(),
        ];
        setup.push(match dialect {
            "mariadb" => format!(
                "SET SESSION max_statement_time = {}",
                timeout.as_secs_f64().max(0.001)
            ),
            "doris" | "starrocks" => format!("SET query_timeout = {}", timeout.as_secs().max(1)),
            "oceanbase" => format!("SET ob_query_timeout = {}", timeout.as_micros().max(1)),
            _ => format!("SET SESSION max_execution_time = {timeout_ms}"),
        });
        if transactional {
            setup.extend([
                "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ".to_owned(),
                "START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY".to_owned(),
            ]);
        }
        for sql in setup {
            stats.request();
            conn.query_drop(sql)
                .await
                .context("initializing read connection")?;
        }
        let server_uuid = conn
            .query_first::<Option<String>, _>("SELECT @@server_uuid")
            .await
            .ok()
            .flatten()
            .flatten()
            .filter(|id| !id.is_empty());
        Ok(Self {
            conn: Some(conn),
            stats,
            transactional,
            server_uuid,
            version: format!("session-{}", uuid::Uuid::new_v4()),
        })
    }

    pub(crate) async fn table(
        &mut self,
        reference: &str,
        columns: &[String],
        filters: &[crate::Predicate],
        native: Option<(&[String], &Pushdown)>,
        watermark: Option<(&str, &ScalarValue)>,
        stream: bool,
        raw_table: bool,
        runtime: &RuntimeEnv,
        batch_size: usize,
        explain_only: bool,
    ) -> Result<(Arc<dyn TableProvider>, MemoryReservation, bool)> {
        let conn = self.conn.as_mut().context("MySQL read session closed")?;
        let (database, table) = reference
            .split_once('.')
            .context("expected MySQL database.table")?;
        let relation = format!("{}.{}", ident(database), ident(table));
        let selected = columns
            .iter()
            .map(|column| ident(column))
            .collect::<Vec<_>>()
            .join(", ");
        let scan = format!("SELECT {selected} FROM {relation}");
        self.stats.request();
        // Transactional MySQL/MariaDB reads retain this metadata lock until rollback.
        conn.query_drop(format!("{scan} LIMIT 0"))
            .await
            .with_context(|| format!("binding MySQL relation {relation}"))?;
        self.stats.request();
        if self.transactional {
            let engine: Option<Option<String>> = conn.query_first(format!(
                "SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA={} AND TABLE_NAME={}", literal(database), literal(table),
            )).await?;
            ensure!(
                engine
                    .flatten()
                    .is_some_and(|engine| engine.eq_ignore_ascii_case("InnoDB")),
                "MySQL relation {relation} must be an InnoDB table for a consistent snapshot"
            );
        }
        self.stats.request();
        let metadata = conn
            .query_map(
                format!(
                "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE, CHARACTER_SET_NAME
             FROM information_schema.COLUMNS WHERE TABLE_SCHEMA={} AND TABLE_NAME={}",
                    literal(database),
                    literal(table)
                ),
                |(name, kind, declared, precision, scale, charset): (
                    String,
                    String,
                    String,
                    Option<u64>,
                    Option<u64>,
                    Option<String>,
                )| { (name, (kind, declared, precision, scale, charset)) },
            )
            .await?
            .into_iter()
            .collect::<BTreeMap<_, _>>();
        let fields = columns
            .iter()
            .map(|name| {
                let (kind, declared, precision, scale, _) = metadata
                    .get(name)
                    .with_context(|| format!("missing MySQL column {relation}.{}", ident(name)))?;
                field(name, kind, declared, *precision, *scale)
            })
            .collect::<Result<Vec<_>>>()?;
        let schema = Arc::new(Schema::new(fields));
        if explain_only {
            let (table, budget) =
                super::snapshot::Buffer::new(&runtime.memory_pool, None).finish_schema(schema)?;
            return Ok((table, budget, false));
        }
        let mut pushed = None;
        if self.transactional
            && let Some((keys, plan)) = native
        {
            let exact_text = |name: &str| {
                metadata.get(name).is_some_and(|(kind, _, _, _, charset)| {
                    matches!(
                        kind.as_str(),
                        "varchar" | "tinytext" | "text" | "mediumtext" | "longtext"
                    ) && charset.as_deref() == Some("utf8mb4")
                })
            };
            if schema.fields().iter().all(|field| {
                field.data_type().is_integer()
                    || field.data_type() == &DataType::Binary
                    || exact_text(field.name())
            }) && let Some(statement) = native_scan(&schema, keys, plan, exact_text)
            {
                self.stats.request();
                let primary = conn
                    .exec_map(
                        "SELECT COLUMN_NAME FROM information_schema.STATISTICS
                     WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND INDEX_NAME='PRIMARY' AND SUB_PART IS NULL ORDER BY SEQ_IN_INDEX",
                        (database, table),
                        |column: String| column,
                    )
                    .await?;
                if primary.len() == keys.len() && primary.iter().all(|key| keys.contains(key)) {
                    pushed = Some(statement);
                }
            }
        }
        let proven = pushed.is_some();
        let (suffix, mut params) = pushed.unwrap_or_else(|| {
            (
                read_filters::where_clause(filters, &schema, ident),
                Vec::new(),
            )
        });
        let mut scan = format!("{scan}{suffix}");
        if self.transactional
            && let Some((column, checkpoint)) = watermark
            && let Some(value) = watermark_param(&schema, column, checkpoint)
        {
            debug_assert!(
                !proven,
                "local-table sync does not provide a native query plan"
            );
            let conjunction = if suffix.is_empty() {
                " WHERE "
            } else {
                " AND "
            };
            let column = ident(column);
            // Keep legacy zero dates visible to the decoder even below the watermark.
            let minimum = if matches!(checkpoint, ScalarValue::TimestampMicrosecond(_, Some(_))) {
                "1970-01-01 00:00:01"
            } else {
                "1000-01-01"
            };
            scan.push_str(&format!(
                "{conjunction}({column} >= ? OR {column} < '{minimum}')"
            ));
            params.push(value);
        }
        let query = Scan {
            sql: scan,
            params,
            schema: schema.clone(),
            batch_size,
            stats: self.stats.clone(),
        };
        if self.transactional && stream && (proven || raw_table) {
            let conn = OwnedConnection {
                conn: self.conn.take(),
                transactional: self.transactional,
                server_uuid: self.server_uuid.clone(),
            };
            let provider = super::snapshot::stream_once(schema, move |context| {
                let schema = query.schema.clone();
                Box::pin(RecordBatchStreamAdapter::new(
                    schema,
                    async_stream::try_stream! {
                        let mut conn = conn;
                        let mut batches = query.batches(
                            conn.conn.as_mut().expect("owned read connection"),
                            context.runtime_env().memory_pool.clone(),
                        );
                        while let Some(batch) = batches.try_next().await.map_err(external)? {
                            yield batch;
                        }
                        drop(batches);
                        conn.conn.take().expect("owned read connection").disconnect().await
                            .map_err(|error| DataFusionError::External(error.into()))?;
                    },
                ))
            })?;
            let budget =
                MemoryConsumer::new("GraphLake MySQL stream").register(&runtime.memory_pool);
            return Ok((provider, budget, proven));
        }
        let mut snapshot =
            super::snapshot::Buffer::new(&runtime.memory_pool, Some(&runtime.disk_manager));
        // Dropping an active result normally drains it before disconnecting.
        // Own the connection for the scan so conversion errors and cancellation kill it.
        let mut owned = OwnedConnection {
            conn: self.conn.take(),
            transactional: self.transactional,
            server_uuid: self.server_uuid.clone(),
        };
        let mut batches = query.batches(
            owned.conn.as_mut().expect("owned read connection"),
            runtime.memory_pool.clone(),
        );
        while let Some(batch) = batches.try_next().await? {
            snapshot.push(batch)?;
        }
        drop(batches);
        self.conn = owned.conn.take();
        let (table, budget) = snapshot.finish(schema, &self.stats)?;
        Ok((table, budget, proven))
    }

    pub(crate) async fn close(mut self) -> Result<()> {
        if let Some(mut conn) = self.conn.take() {
            self.stats.request();
            if self.transactional {
                conn.query_drop("ROLLBACK").await?;
            }
            conn.disconnect().await?;
        }
        Ok(())
    }
}

struct Scan {
    sql: String,
    params: Vec<Value>,
    schema: SchemaRef,
    batch_size: usize,
    stats: Arc<ReadStats>,
}

impl Scan {
    fn batches(
        self,
        conn: &mut Conn,
        pool: Arc<dyn datafusion::execution::memory_pool::MemoryPool>,
    ) -> BoxStream<'_, Result<RecordBatch>> {
        Box::pin(async_stream::try_stream! {
            let raw = MemoryConsumer::new("GraphLake MySQL batch rows").register(&pool);
            let output = MemoryConsumer::new("GraphLake MySQL batch output").register(&pool);
            let mut rows = Vec::new();
            self.stats.request();
            let mut stream: BoxStream<'_, mysql_async::Result<Row>> = if self.params.is_empty() {
                Box::pin(conn.query_stream::<Row, _>(self.sql).await?)
            } else {
                Box::pin(conn.exec_stream::<Row, _, _>(self.sql, self.params).await?)
            };
            loop {
                let row = stream.try_next().await?;
                let done = row.is_none();
                if let Some(row) = row {
                    let payload = (0..row.len())
                        .map(|i| match row.as_ref(i) {
                            Some(Value::Bytes(bytes)) => bytes.len(),
                            _ => 0,
                        })
                        .sum::<usize>();
                    raw.try_grow(payload.saturating_add(size_of::<Row>() + row.len() * size_of::<Value>()))?;
                    rows.push(row);
                }
                if !rows.is_empty() && (done || rows.len() >= self.batch_size) {
                    let batch = to_batch(self.schema.clone(), &mut rows)?;
                    self.stats.mysql_batch(&batch);
                    rows.clear();
                    raw.free();
                    output.try_resize(batch.get_array_memory_size())?;
                    yield batch;
                    output.free();
                }
                if done { break; }
            }
        })
    }
}

struct OwnedConnection {
    conn: Option<Conn>,
    transactional: bool,
    server_uuid: Option<String>,
}

impl Drop for OwnedConnection {
    fn drop(&mut self) {
        let Some(conn) = self.conn.take() else { return };
        let transactional = self.transactional;
        let expected_uuid = self.server_uuid.take();
        // mysql_async normally drains unread rows on drop. Terminate this
        // dedicated connection through the same account before closing it,
        // after confirming server identity when available so proxies or multi-FE
        // clusters do not kill an unrelated session on another backend.
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                if expected_uuid.is_some() || transactional {
                    let id = conn.id();
                    let options = conn.opts().clone();
                    let _ = tokio::time::timeout(Duration::from_secs(2), async {
                        let mut cancel = Conn::new(options).await?;
                        let same_server = match &expected_uuid {
                            Some(expected) => {
                                let actual = cancel
                                    .query_first::<Option<String>, _>("SELECT @@server_uuid")
                                    .await
                                    .ok()
                                    .flatten()
                                    .flatten();
                                actual.as_deref() == Some(expected.as_str())
                            }
                            None => true,
                        };
                        if same_server {
                            cancel.query_drop(format!("KILL CONNECTION {id}")).await?;
                        }
                        cancel.disconnect().await
                    })
                    .await;
                }
                let _ = tokio::time::timeout(Duration::from_secs(1), conn.disconnect()).await;
            });
        }
    }
}

fn external(error: anyhow::Error) -> DataFusionError {
    DataFusionError::External(error.into())
}

fn native_scan(
    schema: &Schema,
    keys: &[String],
    plan: &Pushdown,
    exact_text: impl Fn(&str) -> bool,
) -> Option<(String, Vec<Value>)> {
    let suffix = match &plan.operation {
        Native::Filter => String::new(),
        Native::Top { order, limit }
            if order.iter().all(|(name, _)| {
                schema
                    .field_with_name(name)
                    .is_ok_and(|field| field.data_type().is_integer())
            }) =>
        {
            format!("{} LIMIT {limit}", super::order_by(order, ident))
        }
        _ => return None,
    };
    let mut terms = keys
        .iter()
        .map(|key| format!("{} IS NOT NULL", ident(key)))
        .collect::<Vec<_>>();
    let mut params = Vec::new();
    for filter in &plan.filters {
        if let Some(term) = read_filters::sql_term(filter, schema, ident) {
            terms.push(term);
        } else if filter.op == Comparison::Eq && exact_text(&filter.column) {
            let value = filter.value.as_str()?;
            let column = ident(&filter.column);
            terms.push(format!("({column} = ? AND CAST({column} AS BINARY) = ?)"));
            params.extend([
                Value::Bytes(value.as_bytes().to_vec()),
                Value::Bytes(value.as_bytes().to_vec()),
            ]);
        } else {
            return None;
        }
    }
    Some((format!(" WHERE {}{suffix}", terms.join(" AND ")), params))
}

fn watermark_param(schema: &Schema, column: &str, checkpoint: &ScalarValue) -> Option<Value> {
    if schema.field_with_name(column).ok()?.data_type() != &checkpoint.data_type() {
        return None;
    }
    match checkpoint {
        ScalarValue::Date32(Some(days)) => {
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?;
            let date = epoch.checked_add_signed(chrono::Duration::days(i64::from(*days)))?;
            (1000..=9999)
                .contains(&date.year())
                .then(|| Value::from(date))
        }
        ScalarValue::TimestampMicrosecond(Some(value), _) => {
            let date = DateTime::from_timestamp_micros(*value)?.naive_utc();
            (1000..=9999)
                .contains(&date.year())
                .then(|| Value::from(date))
        }
        _ => None,
    }
}

fn literal(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
}

fn ident(name: &str) -> String {
    let quote = char::from(96);
    format!(
        "{quote}{}{quote}",
        name.replace(quote, &quote.to_string().repeat(2))
    )
}

fn field(
    name: &str,
    kind: &str,
    declared: &str,
    precision: Option<u64>,
    scale: Option<u64>,
) -> Result<Field> {
    let unsigned = declared
        .split_ascii_whitespace()
        .any(|part| part.eq_ignore_ascii_case("unsigned") || part.eq_ignore_ascii_case("zerofill"));
    let normalized = kind.to_ascii_lowercase();
    let kind = normalized.split('(').next().unwrap_or(&normalized);
    let data_type = match (kind, unsigned) {
        ("boolean" | "bool", _) => DataType::Boolean,
        ("largeint", _) => DataType::Decimal128(38, 0),
        ("tinyint", false) => DataType::Int8,
        ("tinyint", true) => DataType::UInt8,
        ("smallint", false) => DataType::Int16,
        ("smallint" | "year", _) => DataType::UInt16,
        ("int" | "mediumint", false) => DataType::Int32,
        ("int" | "mediumint", true) => DataType::UInt32,
        ("bigint", false) => DataType::Int64,
        ("bigint", true) => DataType::UInt64,
        ("float", _) => DataType::Float32,
        ("double", _) => DataType::Float64,
        (
            "char" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext" | "enum" | "set"
            | "string" | "json",
            _,
        ) => DataType::Utf8,
        ("binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob", _) => {
            DataType::Binary
        }
        ("date" | "datev2", _) => DataType::Date32,
        ("datetime" | "datetimev2", _) => DataType::Timestamp(TimeUnit::Microsecond, None),
        ("timestamp", _) => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        ("decimal" | "decimalv2" | "decimalv3" | "decimal32" | "decimal64" | "decimal128", _) => {
            let (precision, scale) = precision
                .zip(scale)
                .context("missing MySQL DECIMAL precision or scale")?;
            ensure!(
                (1..=38).contains(&precision) && scale <= precision,
                "MySQL column {name} requires DECIMAL precision at most 38"
            );
            DataType::Decimal128(precision as u8, scale as i8)
        }
        _ => bail!("unsupported MySQL type {kind} for column {name}"),
    };
    Ok(Field::new(name, data_type, true))
}

fn values<T: FromValue>(rows: &mut [Row], column: usize, field: &Field) -> Result<Vec<Option<T>>> {
    rows.iter_mut()
        .map(|row| {
            row.take_opt::<Option<T>, _>(column)
                .context("missing MySQL value")?
                .map_err(|error| {
                    let name = field.name();
                    let temporal = matches!(field.data_type(), DataType::Date32 | DataType::Timestamp(_, _));
                    let zero_date = match error.0 {
                        Value::Date(year, month, day, ..) => year == 0 || month == 0 || day == 0,
                        Value::Bytes(text) if temporal && text.len() >= 10 => {
                            text[4] == b'-' && text[7] == b'-'
                                && (&text[..4] == b"0000" || &text[5..7] == b"00" || &text[8..10] == b"00")
                        }
                        _ => false,
                    };
                    if zero_date && temporal {
                        anyhow::anyhow!("MySQL column {name} contains a zero date; expose it as a text column in the source table")
                    } else {
                        anyhow::anyhow!("cannot decode MySQL column {name}")
                    }
                })
        })
        .collect()
}

fn to_batch(schema: SchemaRef, rows: &mut [Row]) -> Result<RecordBatch> {
    let arrays = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| -> Result<ArrayRef> {
            macro_rules! collect {
                ($ty:ty, $array:ty) => {
                    Arc::new(<$array>::from(values::<$ty>(rows, i, field)?))
                };
            }
            let array: ArrayRef = match field.data_type() {
                DataType::Boolean => collect!(bool, BooleanArray),
                DataType::Int8 => collect!(i8, Int8Array),
                DataType::Int16 => collect!(i16, Int16Array),
                DataType::Int32 => collect!(i32, Int32Array),
                DataType::Int64 => collect!(i64, Int64Array),
                DataType::UInt8 => collect!(u8, UInt8Array),
                DataType::UInt16 => collect!(u16, UInt16Array),
                DataType::UInt32 => collect!(u32, UInt32Array),
                DataType::UInt64 => collect!(u64, UInt64Array),
                DataType::Float32 => collect!(f32, Float32Array),
                DataType::Float64 => collect!(f64, Float64Array),
                DataType::Utf8 => collect!(String, StringArray),
                DataType::Binary => {
                    let cells = values::<Vec<u8>>(rows, i, field)?;
                    Arc::new(BinaryArray::from_iter(cells.iter().map(Option::as_deref)))
                }
                DataType::Date32 => {
                    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("Unix epoch");
                    Arc::new(Date32Array::from_iter(
                        values::<NaiveDate>(rows, i, field)?
                            .into_iter()
                            .map(|date| date.map(|date| (date - epoch).num_days() as i32)),
                    ))
                }
                DataType::Timestamp(_, timezone) => {
                    let timestamps = values::<NaiveDateTime>(rows, i, field)?
                        .into_iter()
                        .map(|value| value.map(|value| value.and_utc().timestamp_micros()));
                    Arc::new(
                        TimestampMicrosecondArray::from_iter(timestamps)
                            .with_timezone_opt(timezone.clone()),
                    )
                }
                DataType::Decimal128(precision, scale) => {
                    let decimals = values::<String>(rows, i, field)?
                        .into_iter()
                        .map(|value| value.map(|value| decimal128(&value, *scale)).transpose())
                        .collect::<Result<Vec<_>>>()?;
                    let array = Decimal128Array::from(decimals)
                        .with_precision_and_scale(*precision, *scale)?;
                    array.validate_decimal_precision(*precision)?;
                    Arc::new(array)
                }
                ty => bail!("unsupported MySQL Arrow type {ty}"),
            };
            Ok(array)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(schema, arrays)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn field_detects_uppercase_unsigned_and_zerofill() -> Result<()> {
        assert_eq!(
            field("a", "BIGINT", "BIGINT(20) UNSIGNED", None, None)?.data_type(),
            &DataType::UInt64
        );
        assert_eq!(
            field("b", "INT", "INT(11) UNSIGNED ZEROFILL", None, None)?.data_type(),
            &DataType::UInt32
        );
        assert_eq!(
            field("c", "TINYINT", "TINYINT(3) ZEROFILL", None, None)?.data_type(),
            &DataType::UInt8
        );
        assert_eq!(
            field("d", "BIGINT", "BIGINT(20)", None, None)?.data_type(),
            &DataType::Int64
        );
        Ok(())
    }
}
