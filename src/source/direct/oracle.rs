use crate::{
    Predicate, Source,
    catalog::ident,
    engine::read_filters::{needs_sql_types, where_clause},
    source::{
        ReadStats,
        snapshot::Buffer,
        warehouse::types::{self, Encoding},
    },
};
use anyhow::{Context, Result, bail, ensure};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use datafusion::{
    arrow::datatypes::{DataType, Field, Schema, TimeUnit},
    catalog::TableProvider,
    execution::{
        memory_pool::{MemoryConsumer, MemoryReservation},
        runtime_env::RuntimeEnv,
    },
};
use oracle::{ColumnInfo, Connection, sql_type::OracleType};
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

pub(crate) struct Driver {
    connection: Arc<Connection>,
    cancelled: CancellationToken,
    runtime: Arc<RuntimeEnv>,
    stats: Arc<ReadStats>,
}
struct Cancel {
    connection: Arc<Connection>,
    token: CancellationToken,
    armed: bool,
}
impl Drop for Cancel {
    fn drop(&mut self) {
        if self.armed {
            self.token.cancel();
            let connection = Arc::clone(&self.connection);
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                runtime.spawn_blocking(move || {
                    let _ = connection.break_execution();
                });
            } else {
                let _ = connection.break_execution();
            }
        }
    }
}

static CONNECT_SLOTS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(16);

fn host_connect_slots(connect_string: &str) -> Arc<tokio::sync::Semaphore> {
    use std::collections::VecDeque;
    use std::sync::{Mutex, OnceLock};
    static PER_HOST: OnceLock<Mutex<VecDeque<(String, Arc<tokio::sync::Semaphore>)>>> =
        OnceLock::new();
    let mut hosts = PER_HOST.get_or_init(Mutex::default).lock().unwrap();
    if let Some(pos) = hosts.iter().position(|(key, _)| key == connect_string) {
        let entry = hosts.remove(pos).expect("located Oracle host semaphore");
        let sem = Arc::clone(&entry.1);
        hosts.push_front(entry);
        return sem;
    }
    let sem = Arc::new(tokio::sync::Semaphore::new(4));
    if hosts.len() >= 64 {
        hosts.pop_back();
    }
    hosts.push_front((connect_string.to_owned(), Arc::clone(&sem)));
    sem
}

impl Driver {
    pub(super) async fn open(
        source: &Source,
        timeout: Duration,
        runtime: Arc<RuntimeEnv>,
        stats: Arc<ReadStats>,
    ) -> Result<Self> {
        let Source::OracleNative {
            connect_string,
            user,
            password_env,
        } = source
        else {
            bail!("expected Oracle source")
        };
        let password = std::env::var(password_env)
            .with_context(|| format!("missing credential in {password_env}"))?;
        let (connect_string, user) = (connect_string.clone(), user.clone());
        let host_slots = host_connect_slots(&connect_string);
        let connection = tokio::time::timeout(timeout, async move {
            let host_permit = host_slots.acquire_owned().await?;
            let permit = CONNECT_SLOTS.acquire().await?;
            tokio::task::spawn_blocking(move || -> Result<_> {
                // An in-flight OCI connect cannot be cancelled by dropping its waiter.
                let (_host_permit, _permit) = (host_permit, permit);
                let connection = Connection::connect(user, password, connect_string)
                    .context("connecting to Oracle")?;
                connection.set_call_timeout(Some(timeout))?;
                connection.execute("ALTER SESSION SET TIME_ZONE='+00:00'", &[])?;
                connection.execute("ALTER SESSION SET NLS_NUMERIC_CHARACTERS='.,'", &[])?;
                connection.execute("SET TRANSACTION READ ONLY", &[])?;
                Ok(connection)
            })
            .await?
        })
        .await
        .context("Oracle connection timed out")??;
        Ok(Self {
            connection: Arc::new(connection),
            cancelled: CancellationToken::new(),
            runtime,
            stats,
        })
    }

    async fn run<T: Send + 'static>(
        &self,
        work: impl FnOnce(&Connection, &CancellationToken) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let connection = self.connection.clone();
        let token = self.cancelled.clone();
        let mut guard = Cancel {
            connection: connection.clone(),
            token: token.clone(),
            armed: true,
        };
        self.stats.request();
        let result = tokio::task::spawn_blocking(move || {
            ensure!(!token.is_cancelled(), "Oracle read cancelled");
            work(&connection, &token)
        })
        .await?;
        guard.armed = false;
        result
    }

    #[cfg(feature = "admin")]
    pub(super) async fn browse(&self, namespace: Option<&str>) -> Result<Vec<String>> {
        let namespace = namespace.map(str::to_owned);
        self.run(move |connection, _| {
            let rows = match &namespace {
                Some(schema) => connection.query("SELECT object_name FROM all_objects WHERE owner=:1 AND object_type IN ('TABLE','VIEW','MATERIALIZED VIEW') ORDER BY object_name FETCH FIRST 2001 ROWS ONLY", &[schema])?,
                None => connection.query("SELECT DISTINCT owner FROM all_objects WHERE object_type IN ('TABLE','VIEW','MATERIALIZED VIEW') ORDER BY owner FETCH FIRST 2001 ROWS ONLY", &[])?,
            };
            rows.map(|row| Ok(row?.get(0)?)).collect()
        }).await
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
            if explain { " WHERE 1=0" } else { "" }
        );
        let runtime = self.runtime.clone();
        let stats = self.stats.clone();
        let filters = filters.to_vec();
        self.run(move |connection, stop| {
            if !explain && needs_sql_types(&filters) {
                let mut metadata = connection.statement(&format!("{sql} WHERE 1=0")).build()?;
                let result = metadata.query(&[])?;
                let schema = arrow_schema(result.column_info())?;
                sql.push_str(&where_clause(&filters, &schema, ident));
            }
            let mut statement = connection
                .statement(&sql)
                .fetch_array_size(batch_size.min(1024) as u32)
                .build()?;
            let mut result = statement.query(&[])?;
            let schema = arrow_schema(result.column_info())?;
            let mut buffer = Buffer::new(
                &runtime.memory_pool,
                (!explain).then_some(&runtime.disk_manager),
            );
            if explain {
                return buffer.finish_schema(schema);
            }
            let raw = MemoryConsumer::new("GraphLake Oracle rows").register(&runtime.memory_pool);
            let mut rows = Vec::new();
            loop {
                ensure!(!stop.is_cancelled(), "Oracle read cancelled");
                let row = result.next().transpose()?;
                let done = row.is_none();
                if let Some(row) = row {
                    let values = schema
                        .fields()
                        .iter()
                        .enumerate()
                        .map(|(i, field)| -> Result<Option<String>> {
                            Ok(match field.data_type() {
                                DataType::Binary => row
                                    .get::<_, Option<Vec<u8>>>(i)?
                                    .map(|bytes| STANDARD.encode(bytes)),
                                DataType::Timestamp(_, Some(_)) => row
                                    .get::<_, Option<chrono::DateTime<chrono::FixedOffset>>>(i)?
                                    .map(|v| v.to_rfc3339()),
                                DataType::Timestamp(_, None) => row
                                    .get::<_, Option<chrono::NaiveDateTime>>(i)?
                                    .map(|v| v.format("%Y-%m-%dT%H:%M:%S%.f").to_string()),
                                _ => row.get::<_, Option<String>>(i)?,
                            })
                        })
                        .collect::<Result<Vec<_>>>()?;
                    raw.try_grow(
                        values
                            .iter()
                            .map(|v| v.as_ref().map_or(0, String::len) + 32)
                            .sum::<usize>()
                            .saturating_mul(2),
                    )?;
                    rows.push(values);
                }
                if !rows.is_empty() && (done || rows.len() >= batch_size) {
                    let batch = types::batch(schema.clone(), &rows, Encoding::Rfc3339)?;
                    stats.warehouse_batch(&batch);
                    buffer.push(batch)?;
                    rows.clear();
                    raw.free();
                }
                if done {
                    return buffer.finish(schema, &stats);
                }
            }
        })
        .await
        .context("reading Oracle table")
    }
}

fn arrow_schema(columns: &[ColumnInfo]) -> Result<datafusion::arrow::datatypes::SchemaRef> {
    Ok(Arc::new(Schema::new(
        columns
            .iter()
            .map(|column| {
                let kind = arrow_type(column.oracle_type())?;
                Ok(Field::new(column.name(), kind, column.nullable()))
            })
            .collect::<Result<Vec<_>>>()?,
    )))
}

fn arrow_type(oracle_type: &OracleType) -> Result<DataType> {
    Ok(match oracle_type {
        OracleType::Number(p, 0) if *p > 0 && *p <= 18 => DataType::Int64,
        OracleType::Number(p, s) if *p > 0 && *s >= 0 && (*s as u8) <= *p => {
            types::numeric(*p, *s)?
        }
        OracleType::Number(p, s) if *p > 0 && *s > 0 && *s <= 38 => {
            types::numeric((*p).max(*s as u8), *s)?
        }
        OracleType::Number(p, s) if *p > 0 && *s < 0 && i16::from(*p) - i16::from(*s) <= 38 => {
            let precision = (i16::from(*p) - i16::from(*s)) as u8;
            if precision <= 18 {
                DataType::Int64
            } else {
                types::numeric(precision, 0)?
            }
        }
        // Oracle FLOAT is a NUMBER subtype. Text preserves digits
        // when the declared type cannot be mapped to fixed point.
        OracleType::Number(_, _) | OracleType::Float(_) => DataType::Utf8,
        OracleType::BinaryFloat => DataType::Float32,
        OracleType::BinaryDouble => DataType::Float64,
        OracleType::Date => DataType::Timestamp(TimeUnit::Microsecond, None),
        OracleType::Timestamp(_) => DataType::Timestamp(TimeUnit::Microsecond, None),
        OracleType::TimestampTZ(_) | OracleType::TimestampLTZ(_) => {
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        }
        OracleType::Raw(_) | OracleType::BLOB => DataType::Binary,
        OracleType::Varchar2(_)
        | OracleType::NVarchar2(_)
        | OracleType::Char(_)
        | OracleType::NChar(_)
        | OracleType::Rowid
        | OracleType::CLOB
        | OracleType::NCLOB
        | OracleType::Long => DataType::Utf8,
        other => {
            bail!("unsupported Oracle type {other}; expose it as a scalar view column")
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unconstrained_numbers_preserve_business_keys_as_text() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ID", arrow_type(&OracleType::Number(0, -127))?, false),
            Field::new("AMOUNT", arrow_type(&OracleType::Float(126))?, false),
        ]));
        let rows = [
            vec![
                Some("9007199254740992".into()),
                Some("0.10000000000000000001".into()),
            ],
            vec![
                Some("9007199254740993".into()),
                Some("1.00000000000000000002".into()),
            ],
        ];
        let batch = types::batch(schema, &rows, Encoding::Rfc3339)?;
        for (index, field) in batch.columns().iter().enumerate() {
            let values = field
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .unwrap();
            assert_eq!(values.value(0), rows[0][index].as_deref().unwrap());
            assert_eq!(values.value(1), rows[1][index].as_deref().unwrap());
        }
        Ok(())
    }
}
