use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use datafusion::{
    arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
    catalog::TableProvider,
    execution::{
        disk_manager::DiskManager,
        memory_pool::{MemoryPool, MemoryReservation},
    },
};
use google_cloud_spanner::{
    client::{DatabaseClient, Spanner},
    result::ResultSet,
    statement::Statement,
    transaction::{BeginTransactionOption, MultiUseReadOnlyTransaction},
    value::{Kind, TypeCode, Value},
};

use super::{ReadStats, collect_rows};
use crate::{
    Predicate, Source,
    engine::read_filters::{needs_sql_types, where_clause},
};

#[derive(Default)]
pub(crate) struct Client {
    database: tokio::sync::Mutex<Option<(tokio::runtime::Id, DatabaseClient)>>,
}

pub(crate) struct Snapshot {
    tx: MultiUseReadOnlyTransaction,
    timeout: Duration,
    pool: Arc<dyn MemoryPool>,
    stats: Arc<ReadStats>,
    pub(crate) version: String,
}

impl Client {
    pub(crate) async fn open(
        &self,
        source: &Source,
        timeout: Duration,
        pool: Arc<dyn MemoryPool>,
        stats: Arc<ReadStats>,
    ) -> Result<Snapshot> {
        let Source::Spanner { database, uri } = source else {
            bail!("expected Spanner source");
        };
        let database = tokio::time::timeout(timeout, async {
            // The SDK runs channel and session maintenance on the creating runtime.
            let runtime = tokio::runtime::Handle::current().id();
            let mut cached = self.database.lock().await;
            if let Some((_, client)) = cached.as_ref().filter(|(id, _)| *id == runtime) {
                Ok::<_, anyhow::Error>(client.clone())
            } else {
                let _ = rustls::crypto::ring::default_provider().install_default();
                let mut builder = Spanner::builder();
                if let Some(uri) = uri {
                    builder = builder.with_endpoint(uri);
                }
                let client = builder.build().await?;
                stats.request();
                let database = client.database_client(database).build().await?;
                *cached = Some((runtime, database.clone()));
                Ok(database)
            }
        })
        .await
        .context("Spanner connection timed out")??;
        stats.request();
        let tx = database
            .read_only_transaction()
            .set_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
            .with_begin_attempt_timeout(timeout)
            .build()
            .await?;
        let version = tx
            .read_timestamp()
            .context("Spanner snapshot has no read timestamp")?;
        let version =
            chrono::DateTime::from_timestamp(version.seconds(), version.nanos().try_into()?)
                .context("invalid Spanner read timestamp")?
                .to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true);
        Ok(Snapshot {
            tx,
            timeout,
            pool,
            stats,
            version,
        })
    }
}

impl Snapshot {
    #[cfg(feature = "admin")]
    pub(crate) async fn browse(&self) -> Result<Vec<String>> {
        use futures::TryStreamExt;
        let (_, result) = self.query("SELECT TABLE_SCHEMA AS schema_name, TABLE_NAME AS table_name FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = '' AND TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_SCHEMA, TABLE_NAME LIMIT 2001".into(), &[]).await?;
        let mut rows = std::pin::pin!(rows(result));
        let mut names = Vec::new();
        while let Some(row) = rows.try_next().await? {
            if let [Some(schema), Some(table)] = row.as_slice() {
                names.push(if schema.is_empty() {
                    table.clone()
                } else {
                    format!("{schema}.{table}")
                });
            }
        }
        Ok(names)
    }

    pub(crate) async fn table(
        &self,
        relation: &str,
        columns: &[String],
        filters: &[Predicate],
        batch_size: usize,
        disk: &Arc<DiskManager>,
        explain_only: bool,
    ) -> Result<(Arc<dyn TableProvider>, MemoryReservation)> {
        let relation = relation.split('.').map(ident).collect::<Vec<_>>().join(".");
        let selected = columns
            .iter()
            .map(|name| ident(name))
            .collect::<Vec<_>>()
            .join(", ");
        let selected = if explain_only && columns.is_empty() {
            "*".into()
        } else {
            selected
        };
        let mut sql = format!("SELECT {selected} FROM {relation}");
        let expected = if explain_only || needs_sql_types(filters) {
            let (schema, result) = self.query(format!("{sql} WHERE FALSE"), columns).await?;
            let metadata = collect_rows(
                rows(result),
                Arc::clone(&schema),
                batch_size,
                &self.pool,
                &self.stats,
                None,
            )
            .await?;
            if explain_only {
                return Ok(metadata);
            }
            sql.push_str(&where_clause(filters, &schema, ident));
            Some(schema)
        } else {
            None
        };
        let (schema, result) = self.query(sql, columns).await?;
        ensure!(
            expected.is_none_or(|expected| expected == schema),
            "Spanner result schema changed"
        );
        collect_rows(
            rows(result),
            schema,
            batch_size,
            &self.pool,
            &self.stats,
            Some(disk),
        )
        .await
    }

    async fn query(&self, sql: String, columns: &[String]) -> Result<(SchemaRef, ResultSet)> {
        self.stats.request();
        let result = self
            .tx
            .execute_query(
                Statement::builder(sql)
                    .with_attempt_timeout(self.timeout)
                    .build(),
            )
            .await?;
        let metadata = result.metadata();
        ensure!(
            (columns.is_empty() || metadata.column_names() == columns)
                && metadata.column_types().len() == metadata.column_names().len(),
            "Spanner result columns differ from projection"
        );
        let fields = metadata
            .column_names()
            .iter()
            .zip(metadata.column_types())
            .map(|(name, ty)| {
                let data_type = match ty.code() {
                    TypeCode::Bool => DataType::Boolean,
                    TypeCode::Int64 => DataType::Int64,
                    TypeCode::Float32 => DataType::Float32,
                    TypeCode::Float64 => DataType::Float64,
                    TypeCode::String | TypeCode::Json | TypeCode::Uuid => DataType::Utf8,
                    TypeCode::Bytes => DataType::Binary,
                    TypeCode::Date => DataType::Date32,
                    TypeCode::Timestamp => {
                        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
                    }
                    TypeCode::Numeric => DataType::Decimal128(38, 9),
                    kind => bail!("unsupported Spanner type {kind:?} for {name}"),
                };
                Ok(Field::new(name, data_type, true))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok((Arc::new(Schema::new(fields)), result))
    }
}

fn rows(mut result: ResultSet) -> impl futures::Stream<Item = Result<Vec<Option<String>>>> {
    async_stream::try_stream! {
        while let Some(row) = result.next().await {
            yield row?.raw_values().iter().map(cell).collect::<Result<Vec<_>>>()?;
        }
    }
}

fn cell(value: &Value) -> Result<Option<String>> {
    Ok(match value.kind() {
        Kind::Null => None,
        Kind::String => value.as_str().map(str::to_owned),
        Kind::Bool => value.as_bool().map(|v| v.to_string()),
        Kind::Number => value.as_f64().map(|v| v.to_string()),
        kind => bail!("unexpected Spanner value kind {kind:?}"),
    })
}

fn ident(name: &str) -> String {
    format!("`{}`", name.replace('\\', "\\\\").replace('`', "\\`"))
}
