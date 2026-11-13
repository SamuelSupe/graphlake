use std::{collections::BTreeMap, mem::size_of, path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use chrono::{NaiveDate, NaiveDateTime};
use datafusion::{
    arrow::{
        array::{
            ArrayRef, BinaryArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
            Int8Array, Int16Array, Int32Array, Int64Array, RecordBatch, StringArray,
            TimestampMicrosecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
        },
        datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
    },
    catalog::TableProvider,
    execution::{
        memory_pool::{MemoryConsumer, MemoryReservation},
        runtime_env::RuntimeEnv,
    },
};
use futures::TryStreamExt;
use mysql_async::{
    Conn, Opts, OptsBuilder, Row, Value,
    prelude::{FromValue, Queryable},
};

use super::{ReadStats, decimal128};

pub(crate) struct Snapshot {
    conn: Conn,
    stats: Arc<ReadStats>,
    pub(crate) version: String,
}

impl Snapshot {
    #[cfg(feature = "admin")]
    pub(crate) async fn browse(&mut self, database: Option<&str>) -> Result<Vec<String>> {
        self.stats.request();
        Ok(if let Some(database) = database {
            self.conn.exec(
                "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND ENGINE='InnoDB' ORDER BY TABLE_NAME LIMIT 2001",
                (database,),
            ).await?
        } else {
            self.conn.query("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA ORDER BY SCHEMA_NAME LIMIT 2001").await?
        })
    }

    #[cfg(feature = "admin")]
    pub(crate) async fn describe(&mut self, relation: &str) -> Result<Vec<(String, String, bool)>> {
        let (database, table) = relation
            .split_once('.')
            .context("expected MySQL database.table")?;
        self.stats.request();
        let columns = self.conn.exec_map(
            "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? ORDER BY ORDINAL_POSITION",
            (database, table),
            |(name, kind, nullable): (String, String, String)| (name, kind, nullable == "YES"),
        ).await?;
        ensure!(!columns.is_empty(), "MySQL relation has no visible columns");
        Ok(columns)
    }
    pub(crate) async fn open(
        dsn_env: &str,
        tls: bool,
        ca_file: Option<&str>,
        timeout: Duration,
        stats: Arc<ReadStats>,
    ) -> Result<Self> {
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
        let mut conn = Conn::new(builder)
            .await
            .context("opening MySQL connection")?;
        let timeout_ms = timeout.as_millis().clamp(1, u32::MAX as u128);
        for sql in [
            "SET NAMES utf8mb4".to_owned(),
            "SET SESSION time_zone = '+00:00'".to_owned(),
            format!("SET SESSION max_execution_time = {timeout_ms}"),
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ".to_owned(),
            "START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY".to_owned(),
        ] {
            stats.request();
            conn.query_drop(sql)
                .await
                .context("starting MySQL read-only snapshot")?;
        }
        Ok(Self {
            conn,
            stats,
            version: format!("session-{}", uuid::Uuid::new_v4()),
        })
    }

    pub(crate) async fn table(
        &mut self,
        reference: &str,
        columns: &[String],
        filters: &[crate::Predicate],
        runtime: &RuntimeEnv,
        batch_size: usize,
        explain_only: bool,
    ) -> Result<(Arc<dyn TableProvider>, MemoryReservation)> {
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
        // Keep a metadata lock on this relation until the snapshot closes.
        self.conn
            .query_drop(format!("{scan} LIMIT 0"))
            .await
            .with_context(|| format!("binding MySQL relation {relation}"))?;
        self.stats.request();
        let engine: Option<Option<String>> = self.conn.exec_first(
            "SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?",
            (database, table),
        ).await?;
        ensure!(
            engine
                .flatten()
                .is_some_and(|engine| engine.eq_ignore_ascii_case("InnoDB")),
            "MySQL relation {relation} must be an InnoDB table for a consistent snapshot"
        );
        self.stats.request();
        let metadata = self
            .conn
            .exec_map(
                "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE
             FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=?",
                (database, table),
                |(name, kind, declared, precision, scale): (
                    String,
                    String,
                    String,
                    Option<u64>,
                    Option<u64>,
                )| { (name, (kind, declared, precision, scale)) },
            )
            .await?
            .into_iter()
            .collect::<BTreeMap<_, _>>();
        let fields = columns
            .iter()
            .map(|name| {
                let (kind, declared, precision, scale) = metadata
                    .get(name)
                    .with_context(|| format!("missing MySQL column {relation}.{}", ident(name)))?;
                field(name, kind, declared, *precision, *scale)
            })
            .collect::<Result<Vec<_>>>()?;
        let schema = Arc::new(Schema::new(fields));
        if explain_only {
            return super::snapshot::Buffer::new(&runtime.memory_pool, None).finish_schema(schema);
        }
        let scan = format!(
            "{scan}{}",
            crate::engine::read_filters::where_clause(filters, &schema, ident)
        );
        let raw = MemoryConsumer::new("GraphLake MySQL batch rows").register(&runtime.memory_pool);
        let mut snapshot =
            super::snapshot::Buffer::new(&runtime.memory_pool, Some(&runtime.disk_manager));
        let mut rows = Vec::new();
        self.stats.request();
        let mut stream = self
            .conn
            .exec_stream::<Row, _, _>(scan, ())
            .await
            .with_context(|| format!("reading MySQL relation {relation}"))?;
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
                raw.try_grow(
                    payload.saturating_add(size_of::<Row>() + row.len() * size_of::<Value>()),
                )?;
                rows.push(row);
            }
            if !rows.is_empty() && (done || rows.len() >= batch_size) {
                let batch = to_batch(Arc::clone(&schema), &mut rows)?;
                self.stats.mysql_batch(&batch);
                snapshot.push(batch)?;
                rows.clear();
                raw.free();
                tokio::task::yield_now().await;
            }
            if done {
                break;
            }
        }
        snapshot.finish(schema, &self.stats)
    }

    pub(crate) async fn close(mut self) -> Result<()> {
        self.stats.request();
        self.conn.query_drop("ROLLBACK").await?;
        self.conn.disconnect().await?;
        Ok(())
    }
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
        .any(|part| part == "unsigned" || part == "zerofill");
    let data_type = match (kind, unsigned) {
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
            | "json",
            _,
        ) => DataType::Utf8,
        ("binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob", _) => {
            DataType::Binary
        }
        ("date", _) => DataType::Date32,
        ("datetime", _) => DataType::Timestamp(TimeUnit::Microsecond, None),
        ("timestamp", _) => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        ("decimal", _) => {
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

fn values<T: FromValue>(rows: &mut [Row], column: usize) -> Result<Vec<Option<T>>> {
    rows.iter_mut()
        .map(|row| {
            row.take_opt::<Option<T>, _>(column)
                .context("missing MySQL value")?
                .map_err(|_| anyhow::anyhow!("cannot decode MySQL column at index {column}"))
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
                    Arc::new(<$array>::from(values::<$ty>(rows, i)?))
                };
            }
            let array: ArrayRef = match field.data_type() {
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
                    let cells = values::<Vec<u8>>(rows, i)?;
                    Arc::new(BinaryArray::from_iter(cells.iter().map(Option::as_deref)))
                }
                DataType::Date32 => {
                    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("Unix epoch");
                    Arc::new(Date32Array::from_iter(
                        values::<NaiveDate>(rows, i)?
                            .into_iter()
                            .map(|date| date.map(|date| (date - epoch).num_days() as i32)),
                    ))
                }
                DataType::Timestamp(_, timezone) => {
                    let timestamps = values::<NaiveDateTime>(rows, i)?
                        .into_iter()
                        .map(|value| value.map(|value| value.and_utc().timestamp_micros()));
                    Arc::new(
                        TimestampMicrosecondArray::from_iter(timestamps)
                            .with_timezone_opt(timezone.clone()),
                    )
                }
                DataType::Decimal128(precision, scale) => {
                    let decimals = values::<String>(rows, i)?
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
