use std::{
    collections::{BTreeSet, VecDeque},
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail, ensure};
use datafusion::{
    arrow::{array::RecordBatch, buffer::Buffer, datatypes::SchemaRef, ipc::reader::StreamDecoder},
    catalog::TableProvider,
    common::DataFusionError,
    execution::{
        disk_manager::DiskManager,
        memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation},
    },
    physical_plan::stream::RecordBatchStreamAdapter,
};
use serde::Deserialize;

use super::{Http, credential};
use crate::{Comparison, Source, source::ReadStats};

fn ident(name: &str) -> String {
    format!("\"{}\"", name.replace('\\', "\\\\").replace('"', "\\\""))
}

fn readonly_profile_key(uri: &str, user: &str) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut hash = Sha256::new();
    hash.update(uri.len().to_le_bytes());
    hash.update(uri.as_bytes());
    hash.update(user.len().to_le_bytes());
    hash.update(user.as_bytes());
    hash.finalize().into()
}

fn readonly_profiles() -> &'static Mutex<VecDeque<([u8; 32], Instant)>> {
    static CACHE: OnceLock<Mutex<VecDeque<([u8; 32], Instant)>>> = OnceLock::new();
    CACHE.get_or_init(Mutex::default)
}

fn is_readonly_profile(key: &[u8; 32]) -> bool {
    let mut cache = readonly_profiles().lock().unwrap();
    cache.retain(|(_, seen)| seen.elapsed() < Duration::from_secs(300));
    cache.iter().any(|(cached, _)| cached == key)
}

fn remember_readonly_profile(key: [u8; 32]) {
    let mut cache = readonly_profiles().lock().unwrap();
    cache.retain(|(cached, seen)| cached != &key && seen.elapsed() < Duration::from_secs(300));
    if cache.len() >= 64 {
        cache.pop_front();
    }
    cache.push_back((key, Instant::now()));
}

#[derive(Clone)]
pub(crate) struct ClickHouse {
    http: Http,
    user: String,
    password: Option<String>,
    timeout: Duration,
    profile_key: [u8; 32],
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
        let http = Http::new(uri, None, timeout, pool, stats)?;
        let profile_key = readonly_profile_key(http.base.as_str(), user);
        let server_settings = is_readonly_profile(&profile_key);
        Ok(Self {
            http,
            user: user.clone(),
            password: password_env.as_deref().map(credential).transpose()?,
            timeout,
            profile_key,
            server_settings,
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
        stream: bool,
    ) -> Result<(Arc<dyn TableProvider>, MemoryReservation, String)> {
        let relation = relation.split('.').map(ident).collect::<Vec<_>>().join(".");
        let metadata_columns = self.describe(&relation, batch_size).await?;
        let dates: BTreeSet<_> = metadata_columns
            .iter()
            .filter(|column| {
                let ty = column.kind.trim_end_matches(')');
                matches!(
                    ty,
                    "Date"
                        | "Nullable(Date"
                        | "LowCardinality(Date"
                        | "LowCardinality(Nullable(Date"
                )
            })
            .map(|column| column.name.as_str())
            .collect();
        let datetimes = metadata_columns
            .iter()
            .filter(|column| {
                let kind = column
                    .kind
                    .replace("Nullable(", "")
                    .replace("LowCardinality(", "");
                let kind = kind.trim_end_matches(')');
                kind == "DateTime" || kind.starts_with("DateTime(")
            })
            .map(|column| column.name.as_str())
            .collect::<BTreeSet<_>>();
        let selected = if columns.is_empty() {
            metadata_columns
                .iter()
                .filter(|column| column.default_type != "EPHEMERAL")
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>()
        } else {
            columns.iter().map(String::as_str).collect()
        };
        let selected = selected
            .into_iter()
            .map(|name| {
                let column = ident(name);
                if dates.contains(name) {
                    format!("toDate32({column}) AS {column}")
                } else if datetimes.contains(name) {
                    format!("toDateTime64({column}, 0, 'UTC') AS {column}")
                } else {
                    column
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
        let mut sql = format!("SELECT {selected} FROM {relation}");
        let expected = if filters.is_empty() && !explain_only && !stream {
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
            let terms = filters
                .iter()
                .filter_map(|filter| {
                    crate::engine::read_filters::sql_term(filter, &schema, ident).or_else(|| {
                        // ClickHouse String uses byte equality. FixedString and Enum
                        // have different comparison semantics.
                        if filter.op != Comparison::Eq
                            || !metadata_columns.iter().any(|col| {
                                col.name == filter.column
                                    && matches!(
                                        col.kind.as_str(),
                                        "String"
                                            | "Nullable(String)"
                                            | "LowCardinality(String)"
                                            | "LowCardinality(Nullable(String))"
                                    )
                            })
                        {
                            return None;
                        }
                        let value = filter.value.as_str()?;
                        let mut hex = String::with_capacity(value.len() * 2);
                        for byte in value.as_bytes() {
                            use std::fmt::Write;
                            write!(hex, "{byte:02x}").expect("writing to String");
                        }
                        Some(format!("{} = unhex('{hex}')", ident(&filter.column)))
                    })
                })
                .collect::<Vec<_>>();
            if !terms.is_empty() {
                sql.push_str(&format!(" WHERE {}", terms.join(" AND ")));
            }
            Some(schema)
        };
        let sql = format!("{sql} FORMAT ArrowStream");
        if stream {
            let schema = expected.context("missing ClickHouse scan schema")?;
            let id = uuid::Uuid::new_v4().to_string();
            let query_id = id.clone();
            let mut driver = self.clone();
            let table = crate::source::snapshot::stream_once(schema.clone(), move |context| {
                let pool = context.runtime_env().memory_pool.clone();
                let expected = schema.clone();
                Box::pin(RecordBatchStreamAdapter::new(
                    schema,
                    async_stream::try_stream! {
                        let response = driver.request(sql, batch_size, &query_id).await.map_err(external)?;
                        let mut reader = IpcRead::new(response, &pool, driver.http.stats.clone());
                        let budget = MemoryConsumer::new("GraphLake ClickHouse IPC output").register(&pool);
                        while let Some(batch) = reader.next().await.map_err(external)? {
                            check_schema(&expected, &batch.schema()).map_err(external)?;
                            budget.try_resize(batch.get_array_memory_size())?;
                            for offset in (0..batch.num_rows()).step_by(batch_size) {
                                yield batch.slice(offset, batch_size.min(batch.num_rows() - offset));
                            }
                            budget.free();
                        }
                        check_schema(&expected, &reader.schema().map_err(external)?).map_err(external)?;
                    },
                ))
            })?;
            let budget =
                MemoryConsumer::new("GraphLake ClickHouse stream").register(&self.http.pool);
            return Ok((table, budget, id));
        }
        let result = self.read(sql, columns, batch_size, Some(disk)).await?;
        ensure!(
            expected.is_none_or(|expected| expected.fields() == result.0.schema().fields()),
            "ClickHouse result schema changed after describing the table"
        );
        Ok(result)
    }

    async fn describe(&mut self, relation: &str, batch_size: usize) -> Result<Vec<Column>> {
        let mut response = self
            .request(
                format!("DESCRIBE TABLE {relation} FORMAT JSONEachRow"),
                batch_size,
                &uuid::Uuid::new_v4().to_string(),
            )
            .await?;
        let mut bytes = Vec::new();
        let budget = MemoryConsumer::new("GraphLake ClickHouse schema").register(&self.http.pool);
        while let Some(chunk) = response.chunk().await? {
            ensure!(
                bytes.len() + chunk.len() <= 1024 * 1024,
                "ClickHouse schema exceeds 1 MiB"
            );
            budget.try_grow(chunk.len())?;
            bytes.extend_from_slice(&chunk);
        }
        let columns = serde_json::Deserializer::from_slice(&bytes)
            .into_iter()
            .collect::<std::result::Result<Vec<Column>, _>>()?;
        ensure!(!columns.is_empty(), "ClickHouse table has no columns");
        Ok(columns)
    }

    async fn request(
        &mut self,
        sql: String,
        batch_size: usize,
        id: &str,
    ) -> Result<reqwest::Response> {
        let timeout = self.timeout.as_secs_f64().to_string();
        let batch = batch_size.to_string();
        let request = |server_settings| {
            let request = self
                .http
                .client
                .post(self.http.base.clone())
                .basic_auth(&self.user, Some(self.password.as_deref().unwrap_or("")))
                .query(&[("query_id", id), ("readonly", "1")]);
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
            remember_readonly_profile(self.profile_key);
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
        Ok(response)
    }

    async fn read(
        &mut self,
        sql: String,
        columns: &[String],
        batch_size: usize,
        disk: Option<&Arc<DiskManager>>,
    ) -> Result<(Arc<dyn TableProvider>, MemoryReservation, String)> {
        let id = uuid::Uuid::new_v4().to_string();
        let response = self.request(sql, batch_size, &id).await?;
        let mut reader = IpcRead::new(response, &self.http.pool, self.http.stats.clone());
        let mut snapshot = crate::source::snapshot::Buffer::new(&self.http.pool, disk);
        while let Some(batch) = reader.next().await? {
            snapshot.push_chunks(batch, batch_size)?;
            tokio::task::yield_now().await;
        }
        let schema = reader.schema()?;
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

struct IpcRead {
    response: reqwest::Response,
    decoder: StreamDecoder,
    input: Buffer,
    tail: [u8; 8],
    raw: MemoryReservation,
    stats: Arc<ReadStats>,
}

impl IpcRead {
    fn new(response: reqwest::Response, pool: &Arc<dyn MemoryPool>, stats: Arc<ReadStats>) -> Self {
        Self {
            response,
            decoder: StreamDecoder::new(),
            input: Buffer::from(Vec::<u8>::new()),
            tail: [0; 8],
            raw: MemoryConsumer::new("GraphLake ClickHouse IPC input").register(pool),
            stats,
        }
    }

    async fn next(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            while !self.input.is_empty() {
                if let Some(batch) = self.decoder.decode(&mut self.input)? {
                    self.stats.warehouse_batch(&batch);
                    self.raw.try_resize(self.input.len().saturating_mul(2))?;
                    return Ok(Some(batch));
                }
            }
            let Some(chunk) = self.response.chunk().await? else {
                self.decoder.finish()?;
                ensure!(
                    self.tail == [255, 255, 255, 255, 0, 0, 0, 0],
                    "ClickHouse Arrow stream has no end marker"
                );
                return Ok(None);
            };
            self.stats.clickhouse_bytes(chunk.len());
            self.raw.try_grow(chunk.len().saturating_mul(2))?;
            if chunk.len() >= self.tail.len() {
                self.tail.copy_from_slice(&chunk[chunk.len() - 8..]);
            } else {
                let n = chunk.len();
                self.tail.rotate_left(n);
                self.tail[8 - n..].copy_from_slice(&chunk);
            }
            self.input = Buffer::from(chunk);
        }
    }

    fn schema(&self) -> Result<SchemaRef> {
        self.decoder
            .schema()
            .context("ClickHouse Arrow stream has no schema")
    }
}

fn check_schema(expected: &SchemaRef, actual: &SchemaRef) -> Result<()> {
    ensure!(
        expected.fields() == actual.fields(),
        "ClickHouse result schema changed after describing the table"
    );
    Ok(())
}

fn external(error: anyhow::Error) -> DataFusionError {
    DataFusionError::External(error.into())
}

#[derive(Deserialize)]
struct Column {
    name: String,
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    default_type: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    #[ignore = "requires the ClickHouse production stream fixture"]
    async fn single_scan_yields_before_eof_and_preserves_arrow() -> Result<()> {
        use datafusion::arrow::array::{Int64Array, StringArray};
        use futures::TryStreamExt;
        use std::time::Instant;

        let source = Source::Clickhouse {
            uri: std::env::var("GRAPHLAKE_TEST_CLICKHOUSE_URI")?.parse()?,
            user: "readonly_reader".into(),
            password_env: Some("GRAPHLAKE_TEST_CLICKHOUSE_PASSWORD".into()),
        };
        let context = SessionContext::new();
        let runtime = context.runtime_env();
        let mut control = Vec::new();
        for (eager, cancel) in [(true, false), (false, true), (false, false)] {
            let stats = Arc::new(ReadStats::default());
            let mut connector = ClickHouse::new(
                &source,
                Duration::from_secs(30),
                runtime.memory_pool.clone(),
                stats.clone(),
            )?;
            let start = Instant::now();
            let (table, _budget, _) = connector
                .table(
                    "graphlake.stream_nodes",
                    &["id".into(), "name".into()],
                    &[],
                    512,
                    &runtime.disk_manager,
                    false,
                    !eager,
                )
                .await?;
            if !eager {
                assert_eq!(stats.metrics().warehouse_rows, 0);
            }
            let mut stream = context.read_table(table)?.execute_stream().await?;
            let first = stream
                .try_next()
                .await?
                .context("missing ClickHouse first batch")?;
            let first_ms = start.elapsed().as_millis();
            let at_first = stats.metrics();
            if !eager {
                assert!(at_first.warehouse_rows < 200000);
            }
            if cancel {
                drop(stream);
                eprintln!(
                    "ClickHouse dropped first_ms={first_ms} rows={} http_bytes={}",
                    at_first.warehouse_rows, at_first.clickhouse_http_bytes
                );
                continue;
            }
            let mut batches = vec![first];
            batches.extend(stream.try_collect::<Vec<_>>().await?);
            let mut ids = Vec::new();
            for batch in batches {
                let batch_ids = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let names = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                for (id, name) in batch_ids.values().iter().zip(names.iter()) {
                    let expected = format!("{id}{}", "x".repeat(128));
                    assert_eq!(name, Some(expected.as_str()));
                    ids.push(*id);
                }
            }
            ids.sort_unstable();
            assert_eq!(ids.len(), 200000);
            if eager {
                control = ids;
            } else {
                assert_eq!(control, ids);
            }
            let final_reads = stats.metrics();
            assert_eq!(final_reads.warehouse_rows, 200000);
            eprintln!(
                "ClickHouse eager={eager} first_ms={first_ms} first_rows={} first_http_bytes={} total_http_bytes={}",
                at_first.warehouse_rows,
                at_first.clickhouse_http_bytes,
                final_reads.clickhouse_http_bytes
            );
        }
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires graphlake.nodes with an EPHEMERAL column and the ClickHouse readonly fixture"]
    async fn preview_schema_excludes_ephemeral_columns() -> Result<()> {
        let uri = std::env::var("GRAPHLAKE_TEST_CLICKHOUSE_URI")?.parse()?;
        let source = Source::Clickhouse {
            uri,
            user: "readonly_reader".into(),
            password_env: Some("GRAPHLAKE_TEST_CLICKHOUSE_PASSWORD".into()),
        };
        let context = SessionContext::new();
        let runtime = context.runtime_env();
        let mut connector = ClickHouse::new(
            &source,
            Duration::from_secs(10),
            Arc::clone(&runtime.memory_pool),
            Arc::new(ReadStats::default()),
        )?;
        let (table, _, _) = connector
            .table(
                "graphlake.nodes",
                &[],
                &[],
                32,
                &runtime.disk_manager,
                true,
                false,
            )
            .await?;
        let schema = table.schema();
        assert!(schema.field_with_name("id").is_ok());
        assert!(schema.field_with_name("only_insert").is_err());
        Ok(())
    }
}
