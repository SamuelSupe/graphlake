use crate::{
    Predicate, Source,
    engine::read_filters::where_clause,
    source::{
        ReadStats,
        snapshot::Buffer,
        warehouse::types::{self, Encoding},
    },
};
use anyhow::{Context, Result, bail, ensure};
use datafusion::{
    arrow::datatypes::{DataType, Field, Schema, TimeUnit},
    catalog::TableProvider,
    execution::{
        memory_pool::{MemoryConsumer, MemoryReservation},
        runtime_env::RuntimeEnv,
    },
};
use futures::TryStreamExt;
use std::{sync::Arc, time::Duration};
use tiberius::{AuthMethod, Client, Config, EncryptionLevel};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

pub(crate) struct Driver {
    client: Client<Compat<TcpStream>>,
    runtime: Arc<RuntimeEnv>,
    stats: Arc<ReadStats>,
}

fn ident(name: &str) -> String {
    format!("[{}]", name.replace(']', "]]"))
}
fn literal(value: &str) -> String {
    format!("N'{}'", value.replace('\'', "''"))
}

fn native_scalar(kind: &DataType) -> bool {
    matches!(
        kind,
        DataType::Boolean | DataType::UInt8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    )
}

impl Driver {
    pub(super) async fn open(
        source: &Source,
        timeout: Duration,
        runtime: Arc<RuntimeEnv>,
        stats: Arc<ReadStats>,
    ) -> Result<Self> {
        let Source::SqlserverNative {
            host,
            port,
            database,
            user,
            password_env,
            tls,
            ca_file,
        } = source
        else {
            bail!("expected SQL Server source")
        };
        let password = std::env::var(password_env)
            .with_context(|| format!("missing credential in {password_env}"))?;
        let mut config = Config::new();
        config.host(host);
        config.port(*port);
        config.database(database);
        config.application_name("GraphLake");
        config.authentication(AuthMethod::sql_server(user, password));
        config.encryption(if *tls {
            EncryptionLevel::Required
        } else {
            EncryptionLevel::NotSupported
        });
        if let Some(path) = ca_file {
            config.trust_cert_ca(path);
        }
        let mut client = tokio::time::timeout(timeout, async {
            let tcp = TcpStream::connect(config.get_addr()).await?;
            tcp.set_nodelay(true)?;
            stats.request();
            Ok::<_, anyhow::Error>(Client::connect(config, tcp.compat_write()).await?)
        })
        .await
        .context("SQL Server connection timed out")?
        .context("connecting to SQL Server")?;
        client
            .simple_query(format!(
                "SET LOCK_TIMEOUT {}",
                timeout.as_millis().clamp(1, i32::MAX as u128)
            ))
            .await?
            .into_results()
            .await?;
        Ok(Self {
            client,
            runtime,
            stats,
        })
    }

    #[cfg(feature = "admin")]
    pub(super) async fn browse(&mut self, schema: Option<&str>) -> Result<Vec<String>> {
        let sql = match schema {
            None => "SELECT TOP (2001) SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA ORDER BY SCHEMA_NAME".into(),
            Some(schema) => format!("SELECT TOP (2001) TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA={} ORDER BY TABLE_NAME", literal(schema)),
        };
        self.stats.request();
        self.client
            .simple_query(sql)
            .await?
            .into_first_result()
            .await?
            .iter()
            .map(|row| {
                Ok(row
                    .try_get::<&str, _>(0)?
                    .context("NULL SQL Server table name")?
                    .to_owned())
            })
            .collect()
    }

    pub(super) async fn table(
        &mut self,
        table: &str,
        columns: &[String],
        filters: &[Predicate],
        batch_size: usize,
        explain: bool,
    ) -> Result<(Arc<dyn TableProvider>, MemoryReservation)> {
        let (namespace, name) = table.split_once('.').context("expected schema.table")?;
        self.stats.request();
        let metadata = self.client.simple_query(format!("SELECT COLUMN_NAME, DATA_TYPE, CAST(NUMERIC_PRECISION AS INT), CAST(NUMERIC_SCALE AS INT), IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA={} AND TABLE_NAME={} ORDER BY ORDINAL_POSITION", literal(namespace), literal(name))).await?.into_first_result().await?;
        ensure!(
            !metadata.is_empty(),
            "SQL Server table has no visible columns"
        );
        let mut fields = Vec::new();
        for row in &metadata {
            let name = row.try_get::<&str, _>(0)?.context("missing column name")?;
            if !columns.is_empty() && !columns.iter().any(|c| c == name) {
                continue;
            }
            let kind = row.try_get::<&str, _>(1)?.context("missing column type")?;
            let precision = row.try_get::<i32, _>(2)?.unwrap_or(0);
            let scale = row.try_get::<i32, _>(3)?.unwrap_or(0);
            let ty = arrow_type(kind, precision, scale)?;
            fields.push(Field::new(
                name,
                ty,
                row.try_get::<&str, _>(4)? != Some("NO"),
            ));
        }
        let fields = if columns.is_empty() {
            fields
        } else {
            columns
                .iter()
                .map(|name| {
                    fields
                        .iter()
                        .find(|field| field.name() == name)
                        .cloned()
                        .with_context(|| format!("missing SQL Server column {name}"))
                })
                .collect::<Result<_>>()?
        };
        let schema = Arc::new(Schema::new(fields));
        let mut buffer = Buffer::new(
            &self.runtime.memory_pool,
            (!explain).then_some(&self.runtime.disk_manager),
        );
        if explain {
            return buffer.finish_schema(schema);
        }
        let projection = schema
            .fields()
            .iter()
            .map(|field| {
                let name = ident(field.name());
                if native_scalar(field.data_type()) {
                    return name;
                }
                let style = match field.data_type() {
                    DataType::Binary | DataType::Decimal128(_, _) => 2,
                    DataType::Float32 | DataType::Float64 => 3,
                    _ => 127,
                };
                format!("CONVERT(nvarchar(max), {name}, {style}) AS {name}")
            })
            .collect::<Vec<_>>()
            .join(", ");
        self.stats.request();
        let mut stream = self
            .client
            .simple_query(format!(
                "SELECT {projection} FROM {}{}",
                super::relation(table, ident),
                where_clause(filters, &schema, ident),
            ))
            .await?
            .into_row_stream();
        let raw =
            MemoryConsumer::new("GraphLake SQL Server rows").register(&self.runtime.memory_pool);
        let mut rows = Vec::new();
        loop {
            let row = stream.try_next().await?;
            let done = row.is_none();
            if let Some(row) = row {
                let values = schema
                    .fields()
                    .iter()
                    .enumerate()
                    .map(|(i, field)| -> Result<_> {
                        Ok(match field.data_type() {
                            DataType::Boolean => row.try_get::<bool, _>(i)?.map(|v| v.to_string()),
                            DataType::UInt8 => row.try_get::<u8, _>(i)?.map(|v| v.to_string()),
                            DataType::Int16 => row.try_get::<i16, _>(i)?.map(|v| v.to_string()),
                            DataType::Int32 => row.try_get::<i32, _>(i)?.map(|v| v.to_string()),
                            DataType::Int64 => row.try_get::<i64, _>(i)?.map(|v| v.to_string()),
                            _ => row.try_get::<&str, _>(i)?.map(str::to_owned),
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
                let batch = types::batch(schema.clone(), &rows, Encoding::Snowflake)?;
                self.stats.warehouse_batch(&batch);
                buffer.push(batch)?;
                rows.clear();
                raw.free();
            }
            if done {
                return buffer.finish(schema, &self.stats);
            }
        }
    }
}

fn arrow_type(kind: &str, precision: i32, scale: i32) -> Result<DataType> {
    Ok(match kind {
        "bit" => DataType::Boolean,
        "tinyint" => DataType::UInt8,
        "smallint" => DataType::Int16,
        "int" => DataType::Int32,
        "bigint" => DataType::Int64,
        "real" => DataType::Float32,
        "float" => DataType::Float64,
        "decimal" | "numeric" => types::numeric(precision as u8, scale as i8)?,
        "money" => DataType::Decimal128(19, 4),
        "smallmoney" => DataType::Decimal128(10, 4),
        "date" => DataType::Date32,
        "time" => DataType::Time64(TimeUnit::Nanosecond),
        "datetime" | "smalldatetime" | "datetime2" => {
            DataType::Timestamp(TimeUnit::Microsecond, None)
        }
        "datetimeoffset" => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        "binary" | "varbinary" | "image" | "timestamp" | "rowversion" => DataType::Binary,
        "char" | "varchar" | "nchar" | "nvarchar" | "text" | "ntext" | "uniqueidentifier"
        | "xml" => DataType::Utf8,
        _ => bail!("unsupported SQL Server type {kind}; expose it as a scalar view column"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamps_keep_scd2_sentinels_and_reject_fractional_truncation() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("start", arrow_type("datetime2", 0, 0)?, false),
            Field::new("end", arrow_type("datetimeoffset", 0, 0)?, false),
        ]));
        let mut rows = vec![vec![
            Some("0001-01-01T00:00:00.0000000".into()),
            Some("9999-12-31T23:59:59.9999990+00:00".into()),
        ]];
        let batch = types::batch(Arc::clone(&schema), &rows, Encoding::Snowflake)?;
        for (index, expected) in [-62_135_596_800_000_000, 253_402_300_799_999_999]
            .into_iter()
            .enumerate()
        {
            let values = batch
                .column(index)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::TimestampMicrosecondArray>()
                .unwrap();
            assert_eq!(values.value(0), expected);
        }
        rows[0][0] = Some("2026-01-01T00:00:00.0000001".into());
        assert!(types::batch(schema, &rows, Encoding::Snowflake).is_err());
        Ok(())
    }
}
