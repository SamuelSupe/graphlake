mod bigquery;
pub(crate) mod clickhouse;
pub(crate) mod hana;
mod snowflake;
pub(crate) mod spanner;
pub(crate) mod trino;
mod types;

use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use chrono::{DateTime, SecondsFormat, Utc};
use datafusion::{
    arrow::{
        array::{Array, StringArray},
        datatypes::{Field, Schema, SchemaRef},
    },
    catalog::TableProvider,
    execution::{
        disk_manager::DiskManager,
        memory_pool::{MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation},
    },
};
use futures::{Stream, TryStreamExt};
use reqwest::{
    Client, RequestBuilder, StatusCode,
    header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue},
};
use serde::de::DeserializeOwned;
use url::Url;

use super::{ReadStats, snapshot::Buffer};
use crate::{Source, catalog::warehouse_endpoint};

const MAX_PAGE_BYTES: usize = 32 * 1024 * 1024;

pub(crate) struct Snapshot {
    driver: Driver,
    pub(crate) version: String,
    pub(crate) consistency: &'static str,
}

enum Driver {
    Snowflake(snowflake::Warehouse),
    BigQuery(bigquery::Warehouse),
}

impl Snapshot {
    #[cfg(feature = "admin")]
    pub(crate) async fn browse(&self, namespace: &str) -> Result<Vec<String>> {
        let name = "table_name";
        let sql = match &self.driver {
            Driver::Snowflake(_) => {
                let (database, schema) = namespace
                    .split_once('.')
                    .context("expected database.schema")?;
                format!(
                    "SELECT TABLE_NAME AS \"{name}\" FROM {}.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{}' ORDER BY TABLE_NAME LIMIT 2001",
                    crate::catalog::ident(database),
                    schema.replace('\\', "\\\\").replace('\'', "''")
                )
            }
            Driver::BigQuery(_) => {
                let (project, dataset) = namespace
                    .split_once('.')
                    .context("expected project.dataset")?;
                ensure!(
                    [project, dataset]
                        .into_iter()
                        .all(crate::catalog::warehouse_identifier),
                    "invalid BigQuery namespace"
                );
                format!(
                    "SELECT table_name FROM `{project}.{dataset}.INFORMATION_SCHEMA.TABLES` ORDER BY table_name LIMIT 2001"
                )
            }
        };
        let (_, buffer) = self.read(&sql, &[name.into()], 256, false, None).await?;
        let mut names = Vec::new();
        for batch in buffer.memory() {
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .context("invalid table metadata")?;
            names.extend(values.iter().flatten().map(str::to_owned));
        }
        Ok(names)
    }

    pub(crate) async fn open(
        source: &Source,
        timeout: Duration,
        pool: Arc<dyn MemoryPool>,
        stats: Arc<ReadStats>,
    ) -> Result<Self> {
        let (driver, consistency, as_of) = match source {
            Source::Snowflake {
                uri,
                token_env,
                warehouse,
                role,
                as_of,
            } => {
                let http = Http::new(uri, Some(token_env), timeout, pool, stats)?;
                (
                    Driver::Snowflake(snowflake::Warehouse {
                        http,
                        warehouse: warehouse.clone(),
                        role: role.clone(),
                        timeout,
                    }),
                    "Snowflake time travel",
                    as_of.as_deref(),
                )
            }
            Source::Bigquery {
                uri,
                token_env,
                billing_project,
                location,
                maximum_bytes_billed,
                as_of,
            } => {
                let http = Http::new(
                    uri.as_deref().unwrap_or("https://bigquery.googleapis.com"),
                    Some(token_env),
                    timeout,
                    pool,
                    stats,
                )?;
                (
                    Driver::BigQuery(bigquery::Warehouse {
                        http,
                        project: billing_project.clone(),
                        location: location.clone(),
                        maximum_bytes_billed: *maximum_bytes_billed,
                        timeout,
                    }),
                    "BigQuery time travel",
                    as_of.as_deref(),
                )
            }
            _ => bail!("expected a warehouse source"),
        };
        let mut snapshot = Self {
            driver,
            version: String::new(),
            consistency,
        };
        let time = match as_of {
            Some(time) => time.to_owned(),
            None => {
                let sql = match snapshot.driver {
                    Driver::Snowflake(_) => snowflake::CLOCK,
                    Driver::BigQuery(_) => bigquery::CLOCK,
                };
                let (_, buffer) = snapshot
                    .read(sql, &["snapshot_time".into()], 2, false, None)
                    .await?;
                let batches = buffer.memory();
                ensure!(
                    batches.len() == 1 && batches[0].num_rows() == 1,
                    "invalid warehouse clock result"
                );
                let values = batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .context("warehouse clock did not return a string")?;
                ensure!(!values.is_null(0), "warehouse clock returned NULL");
                values.value(0).to_owned()
            }
        };
        let time = DateTime::parse_from_rfc3339(&time)?.with_timezone(&Utc);
        if matches!(snapshot.driver, Driver::BigQuery(_)) {
            ensure!(
                time.timestamp_subsec_nanos().is_multiple_of(1_000),
                "BigQuery as_of requires microsecond precision"
            );
        }
        snapshot.version = time.to_rfc3339_opts(SecondsFormat::AutoSi, true);
        Ok(snapshot)
    }

    pub(crate) async fn table(
        &self,
        relation: &str,
        columns: &[String],
        batch_size: usize,
        disk: &Arc<DiskManager>,
        explain_only: bool,
    ) -> Result<(Arc<dyn TableProvider>, MemoryReservation)> {
        let mut sql = match &self.driver {
            Driver::Snowflake(_) => snowflake::select(relation, columns, &self.version),
            Driver::BigQuery(_) => bigquery::select(relation, columns, &self.version)?,
        };
        if explain_only {
            sql.push_str(" WHERE FALSE");
        }
        let (schema, buffer) = self
            .read(
                &sql,
                columns,
                batch_size,
                true,
                (!explain_only).then_some(disk),
            )
            .await?;
        if explain_only {
            return buffer.finish_schema(schema);
        }
        let stats = match &self.driver {
            Driver::Snowflake(source) => &source.http.stats,
            Driver::BigQuery(source) => &source.http.stats,
        };
        buffer.finish(schema, stats)
    }

    async fn read(
        &self,
        sql: &str,
        columns: &[String],
        batch_size: usize,
        count_rows: bool,
        disk: Option<&Arc<DiskManager>>,
    ) -> Result<(SchemaRef, Buffer)> {
        let (http, mut pages) = match &self.driver {
            Driver::Snowflake(source) => (&source.http, source.pages(sql)),
            Driver::BigQuery(source) => (&source.http, source.pages(sql, batch_size)),
        };
        collect_pages(
            http,
            &mut pages,
            columns,
            batch_size,
            count_rows,
            match self.driver {
                Driver::Snowflake(_) => types::Encoding::Snowflake,
                Driver::BigQuery(_) => types::Encoding::BigQuery,
            },
            disk,
        )
        .await
    }
}

async fn collect_pages(
    http: &Http,
    pages: &mut futures::stream::BoxStream<'_, Result<Page>>,
    columns: &[String],
    batch_size: usize,
    count_rows: bool,
    encoding: types::Encoding,
    disk: Option<&Arc<DiskManager>>,
) -> Result<(SchemaRef, Buffer)> {
    let mut schema = None;
    let mut buffer = Buffer::new(&http.pool, disk);
    while let Some(page) = pages.try_next().await? {
        if let Some(fields) = page.fields {
            ensure!(
                columns.is_empty() || fields.iter().map(|field| field.name()).eq(columns),
                "warehouse response columns do not match the requested projection"
            );
            let fields = Arc::new(Schema::new(fields));
            if let Some(previous) = &schema {
                ensure!(
                    previous == &fields,
                    "warehouse result schema changed during pagination"
                );
            }
            schema = Some(fields);
        }
        let schema = schema.as_ref().context("warehouse result has no schema")?;
        for chunk in page.rows.chunks(batch_size) {
            let batch = types::batch(Arc::clone(schema), chunk, encoding)?;
            if count_rows {
                http.stats.warehouse_batch(&batch);
            }
            buffer.push(batch)?;
            tokio::task::yield_now().await;
        }
    }
    Ok((schema.context("warehouse result has no schema")?, buffer))
}

async fn collect_rows(
    rows: impl Stream<Item = Result<Vec<Option<String>>>>,
    schema: SchemaRef,
    batch_size: usize,
    pool: &Arc<dyn MemoryPool>,
    stats: &ReadStats,
    disk: Option<&Arc<DiskManager>>,
) -> Result<(Arc<dyn TableProvider>, MemoryReservation)> {
    let mut rows = std::pin::pin!(rows);
    let mut buffer = Buffer::new(pool, disk);
    if disk.is_none() {
        ensure!(
            rows.try_next().await?.is_none(),
            "schema-only query returned rows"
        );
        return buffer.finish_schema(schema);
    }
    let input = MemoryConsumer::new("GraphLake warehouse row batch").register(pool);
    let batch_bytes = match pool.memory_limit() {
        MemoryLimit::Finite(limit) => (limit / 8).max(1),
        _ => usize::MAX,
    };
    let mut cells = Vec::new();
    loop {
        let end = match rows.try_next().await? {
            Some(row) => {
                let bytes = row.len() * std::mem::size_of::<Option<String>>()
                    + row.iter().flatten().map(String::len).sum::<usize>();
                input.try_grow(bytes)?;
                cells.push(row);
                false
            }
            None => true,
        };
        if !cells.is_empty() && (end || cells.len() == batch_size || input.size() >= batch_bytes) {
            let batch = types::batch(Arc::clone(&schema), &cells, types::Encoding::Rfc3339)?;
            cells.clear();
            input.free();
            stats.warehouse_batch(&batch);
            buffer.push(batch)?;
            tokio::task::yield_now().await;
        }
        if end {
            return buffer.finish(schema, stats);
        }
    }
}

fn credential(env: &str) -> Result<String> {
    let value = std::env::var(env).with_context(|| format!("missing credential in {env}"))?;
    ensure!(!value.is_empty(), "empty credential in {env}");
    Ok(value)
}

struct Page {
    fields: Option<Vec<Field>>,
    rows: Vec<Vec<Option<String>>>,
    _budget: MemoryReservation,
}

struct Http {
    client: Client,
    base: Url,
    pool: Arc<dyn MemoryPool>,
    stats: Arc<ReadStats>,
}

impl Http {
    fn new(
        uri: &str,
        token_env: Option<&str>,
        timeout: Duration,
        pool: Arc<dyn MemoryPool>,
        stats: Arc<ReadStats>,
    ) -> Result<Self> {
        let base = warehouse_endpoint(uri)?;
        let mut headers = HeaderMap::from_iter([
            (ACCEPT, HeaderValue::from_static("application/json")),
            (CONTENT_TYPE, HeaderValue::from_static("application/json")),
        ]);
        if let Some(env) = token_env {
            let mut token = HeaderValue::from_str(&format!("Bearer {}", credential(env)?))
                .context("invalid warehouse bearer token")?;
            token.set_sensitive(true);
            headers.insert(AUTHORIZATION, token);
        }
        let client = Client::builder()
            .default_headers(headers)
            .user_agent(concat!("GraphLake/", env!("CARGO_PKG_VERSION")))
            .redirect(reqwest::redirect::Policy::none())
            .timeout(timeout)
            .build()?;
        Ok(Self {
            client,
            base,
            pool,
            stats,
        })
    }

    fn endpoint(&self, parts: &[&str]) -> Url {
        let mut url = self.base.clone();
        url.path_segments_mut()
            .expect("HTTP origin")
            .pop_if_empty()
            .extend(parts);
        url
    }

    async fn receive<T: DeserializeOwned>(
        &self,
        request: RequestBuilder,
    ) -> Result<(StatusCode, T, MemoryReservation)> {
        self.stats.request();
        let mut response = request.send().await?;
        let status = response.status();
        ensure!(status.is_success(), "warehouse API returned HTTP {status}");
        let budget = MemoryConsumer::new("GraphLake warehouse HTTP page").register(&self.pool);
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            ensure!(
                body.len().saturating_add(chunk.len()) <= MAX_PAGE_BYTES,
                "warehouse response exceeds {MAX_PAGE_BYTES} decoded bytes"
            );
            budget.try_grow(chunk.len().saturating_mul(4))?;
            body.extend_from_slice(&chunk);
        }
        let decoded = serde_json::from_slice(&body).context("invalid warehouse response JSON")?;
        Ok((status, decoded, budget))
    }
}

fn verify(condition: bool, message: impl std::fmt::Display) -> Result<()> {
    ensure!(condition, "{message}");
    Ok(())
}

struct Pending(Option<RequestBuilder>);

impl Drop for Pending {
    fn drop(&mut self) {
        if let Some(request) = self.0.take()
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            runtime.spawn(async move {
                let _ = request.timeout(Duration::from_secs(2)).send().await;
            });
        }
    }
}
