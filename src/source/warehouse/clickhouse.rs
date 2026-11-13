use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use datafusion::{
    arrow::{buffer::Buffer, ipc::reader::StreamDecoder},
    catalog::TableProvider,
    execution::{
        disk_manager::DiskManager,
        memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation},
    },
};
use reqwest::header::ACCEPT;

use super::{Http, credential};
use crate::{Source, catalog::ident, source::ReadStats};

pub(crate) struct ClickHouse {
    http: Http,
    user: String,
    password: Option<String>,
    timeout: Duration,
    server_settings: bool,
}

impl ClickHouse {
    #[cfg(feature = "admin")]
    pub(crate) async fn browse(
        &mut self,
        database: Option<&str>,
        context: &datafusion::prelude::SessionContext,
        disk: &Arc<DiskManager>,
    ) -> Result<Vec<String>> {
        let sql = match database {
            Some(database) => format!(
                "SELECT name FROM system.tables WHERE database = '{}' ORDER BY name LIMIT 2001 FORMAT ArrowStream",
                database.replace('\\', "\\\\").replace('\'', "''")
            ),
            None => "SELECT name FROM system.databases ORDER BY name LIMIT 2001 FORMAT ArrowStream"
                .to_owned(),
        };
        let (table, _budget, _) = self
            .read(sql, &["name".to_owned()], 256, Some(disk))
            .await?;
        let mut names = Vec::new();
        for batch in context.read_table(table)?.collect().await? {
            use datafusion::arrow::{array::Array, compute::cast, datatypes::DataType};
            let column = cast(batch.column(0), &DataType::Utf8)?;
            let values = column
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .context("invalid table metadata")?;
            names.extend(values.iter().flatten().map(str::to_owned));
        }
        Ok(names)
    }

    pub(crate) fn new(
        source: &Source,
        timeout: Duration,
        pool: Arc<dyn MemoryPool>,
        stats: Arc<ReadStats>,
    ) -> Result<Self> {
        let Source::Clickhouse {
            uri,
            user,
            password_env,
        } = source
        else {
            bail!("expected ClickHouse source");
        };
        Ok(Self {
            http: Http::new(uri, None, timeout, pool, stats)?,
            user: user.clone(),
            password: password_env.as_deref().map(credential).transpose()?,
            timeout,
            server_settings: false,
        })
    }

    pub(crate) async fn table(
        &mut self,
        relation: &str,
        columns: &[String],
        filters: &[crate::Predicate],
        batch_size: usize,
        disk: &Arc<DiskManager>,
        explain_only: bool,
    ) -> Result<(Arc<dyn TableProvider>, MemoryReservation, String)> {
        let relation = relation.split('.').map(ident).collect::<Vec<_>>().join(".");
        let selected = columns
            .iter()
            .map(|column| ident(column))
            .collect::<Vec<_>>()
            .join(", ");
        let selected = if explain_only && columns.is_empty() {
            "*".into()
        } else {
            selected
        };
        let mut sql = format!("SELECT {selected} FROM {relation}");
        let expected = if filters.is_empty() && !explain_only {
            None
        } else {
            let metadata = self
                .read(
                    format!("{sql} WHERE 0 FORMAT ArrowStream"),
                    columns,
                    batch_size,
                    None,
                )
                .await?;
            if explain_only {
                return Ok(metadata);
            }
            let schema = metadata.0.schema();
            sql.push_str(&crate::engine::read_filters::where_clause(
                filters, &schema, ident,
            ));
            Some(schema)
        };
        let result = self
            .read(
                format!("{sql} FORMAT ArrowStream"),
                columns,
                batch_size,
                Some(disk),
            )
            .await?;
        ensure!(
            expected.is_none_or(|expected| expected.fields() == result.0.schema().fields()),
            "ClickHouse result schema changed after describing the table"
        );
        Ok(result)
    }

    async fn read(
        &mut self,
        sql: String,
        columns: &[String],
        batch_size: usize,
        disk: Option<&Arc<DiskManager>>,
    ) -> Result<(Arc<dyn TableProvider>, MemoryReservation, String)> {
        let id = uuid::Uuid::new_v4().to_string();
        let timeout = self.timeout.as_secs_f64().to_string();
        let batch = batch_size.to_string();
        let request = |server_settings| {
            let request = self
                .http
                .client
                .post(self.http.base.clone())
                .header(ACCEPT, "application/vnd.apache.arrow.stream")
                .basic_auth(&self.user, Some(self.password.as_deref().unwrap_or("")))
                .query(&[
                    ("query_id", id.as_str()),
                    ("readonly", "1"),
                    ("wait_end_of_query", "1"),
                ]);
            let request = if server_settings {
                request
            } else {
                request.query(&[
                    ("max_execution_time", timeout.as_str()),
                    ("max_block_size", batch.as_str()),
                    ("cancel_http_readonly_queries_on_client_close", "1"),
                    ("output_format_arrow_compression_method", "none"),
                    ("output_format_arrow_string_as_string", "1"),
                    ("output_format_arrow_low_cardinality_as_dictionary", "0"),
                ])
            };
            self.http.stats.request();
            request.body(sql.clone()).send()
        };
        const EXCEPTION_CODE: &str = "X-ClickHouse-Exception-Code";
        let mut response = request(self.server_settings).await?;
        if !self.server_settings
            && !response.status().is_success()
            && response
                .headers()
                .get(EXCEPTION_CODE)
                .is_some_and(|code| code == "164")
        {
            // A readonly profile can disallow per-statement settings before the SELECT starts.
            drop(response);
            response = request(true).await?;
            self.server_settings = true;
        }
        ensure!(
            response.status().is_success(),
            "ClickHouse returned HTTP {} (server code {})",
            response.status(),
            response
                .headers()
                .get(EXCEPTION_CODE)
                .and_then(|code| code.to_str().ok())
                .unwrap_or("unavailable")
        );
        ensure!(
            response.headers().get(EXCEPTION_CODE).is_none(),
            "ClickHouse query failed"
        );
        let raw = MemoryConsumer::new("GraphLake ClickHouse IPC input").register(&self.http.pool);
        let mut decoder = StreamDecoder::new();
        let mut snapshot = crate::source::snapshot::Buffer::new(&self.http.pool, disk);
        let mut tail = [0u8; 8];
        while let Some(chunk) = response.chunk().await? {
            raw.try_grow(chunk.len().saturating_mul(2))?;
            if chunk.len() >= tail.len() {
                let offset = chunk.len() - tail.len();
                tail.copy_from_slice(&chunk[offset..]);
            } else {
                let n = chunk.len();
                tail.rotate_left(n);
                tail[8 - n..].copy_from_slice(&chunk);
            }
            let mut buffer = Buffer::from(chunk);
            while !buffer.is_empty() {
                if let Some(batch) = decoder.decode(&mut buffer)? {
                    self.http.stats.warehouse_batch(&batch);
                    snapshot.push_chunks(batch, batch_size)?;
                    raw.free();
                    raw.try_grow(buffer.len().saturating_mul(2))?;
                    tokio::task::yield_now().await;
                }
            }
        }
        decoder.finish()?;
        ensure!(
            tail == [255, 255, 255, 255, 0, 0, 0, 0],
            "ClickHouse Arrow stream has no end marker"
        );
        let schema = decoder
            .schema()
            .context("ClickHouse Arrow stream has no schema")?;
        ensure!(
            columns.is_empty() || schema.fields().iter().map(|field| field.name()).eq(columns),
            "ClickHouse result columns differ from projection"
        );
        let (table, budget) = if disk.is_none() {
            snapshot.finish_schema(schema)?
        } else {
            snapshot.finish(schema, &self.http.stats)?
        };
        Ok((table, budget, id))
    }
}
