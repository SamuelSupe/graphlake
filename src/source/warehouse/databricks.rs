use super::{
    Http, Page, Pending, collect_pages,
    types::{self, Encoding},
    verify,
};
use crate::{
    Predicate, Source,
    engine::read_filters::{needs_sql_types, where_clause},
    source::ReadStats,
};
use anyhow::{Context, Result, bail, ensure};
#[cfg(feature = "admin")]
use datafusion::arrow::array::StringArray;
use datafusion::{
    arrow::datatypes::{DataType, Field, TimeUnit},
    catalog::TableProvider,
    execution::{
        disk_manager::DiskManager,
        memory_pool::{MemoryPool, MemoryReservation},
    },
};
use futures::stream::BoxStream;
use serde::Deserialize;
use serde_json::json;
use std::{sync::Arc, time::Duration};

pub(crate) struct Databricks {
    http: Http,
    warehouse: String,
    catalog: String,
}
fn ident(value: &str) -> String {
    format!("`{}`", value.replace('`', "``"))
}
#[cfg(feature = "admin")]
fn literal(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
}

impl Databricks {
    pub(crate) fn new(
        source: &Source,
        timeout: Duration,
        pool: Arc<dyn MemoryPool>,
        stats: Arc<ReadStats>,
    ) -> Result<Self> {
        let Source::Databricks {
            uri,
            token_env,
            warehouse,
            catalog,
        } = source
        else {
            bail!("expected Databricks source")
        };
        Ok(Self {
            http: Http::new(uri, Some(token_env), timeout, pool, stats)?,
            warehouse: warehouse.clone(),
            catalog: catalog.clone(),
        })
    }

    #[cfg(feature = "admin")]
    pub(crate) async fn browse(&self, namespace: Option<&str>) -> Result<Vec<String>> {
        let sql = match namespace {
            None => format!(
                "SELECT schema_name AS name FROM {}.information_schema.schemata ORDER BY schema_name LIMIT 2001",
                ident(&self.catalog)
            ),
            Some(namespace) => format!(
                "SELECT table_name AS name FROM {}.information_schema.tables WHERE table_schema={} ORDER BY table_name LIMIT 2001",
                ident(&self.catalog),
                literal(namespace)
            ),
        };
        let (_, buffer) = collect_pages(
            &self.http,
            &mut self.pages(&sql),
            &[],
            256,
            false,
            Encoding::Rfc3339,
            None,
        )
        .await?;
        let mut names = Vec::new();
        for batch in buffer.memory() {
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .context("invalid Databricks metadata")?;
            names.extend(values.iter().flatten().map(str::to_owned));
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
        explain: bool,
    ) -> Result<(Arc<dyn TableProvider>, MemoryReservation)> {
        let fields = if columns.is_empty() {
            "*".into()
        } else {
            columns
                .iter()
                .map(|c| ident(c))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let table = relation.split('.').map(ident).collect::<Vec<_>>().join(".");
        let mut sql = format!("SELECT {fields} FROM {}.{table}", ident(&self.catalog));
        let mut expected = None;
        if explain || needs_sql_types(filters) {
            let (schema, buffer) = collect_pages(
                &self.http,
                &mut self.pages(&format!("{sql} LIMIT 0")),
                columns,
                batch_size,
                false,
                Encoding::Rfc3339,
                None,
            )
            .await?;
            ensure!(
                buffer.memory().is_empty(),
                "Databricks schema query returned rows"
            );
            if explain {
                return buffer.finish_schema(schema);
            }
            sql.push_str(&where_clause(filters, &schema, ident));
            expected = Some(schema);
        }
        let (schema, buffer) = collect_pages(
            &self.http,
            &mut self.pages(&sql),
            columns,
            batch_size,
            true,
            Encoding::Rfc3339,
            Some(disk),
        )
        .await?;
        ensure!(
            expected.is_none_or(|expected| expected.fields() == schema.fields()),
            "Databricks result schema changed after describing the table"
        );
        buffer.finish(schema, &self.http.stats)
    }

    fn pages<'a>(&'a self, sql: &'a str) -> BoxStream<'a, Result<Page>> {
        Box::pin(async_stream::try_stream! {
            let http = &self.http;
            let (_, mut response, mut budget) = http.receive::<Response>(http.client.post(http.endpoint(&["api", "2.0", "sql", "statements"])).json(&json!({
                "statement": sql, "warehouse_id": self.warehouse, "catalog": self.catalog,
                "wait_timeout": "0s", "on_wait_timeout": "CONTINUE", "format": "JSON_ARRAY", "disposition": "INLINE",
            }))).await?;
            let id = response.statement_id.take().context("missing Databricks statement ID")?;
            uuid::Uuid::parse_str(&id).context("invalid Databricks statement ID")?;
            let mut pending = Pending(Some(http.client.post(http.endpoint(&["api", "2.0", "sql", "statements", &id, "cancel"]))));
            while matches!(response.status.state.as_str(), "PENDING" | "RUNNING") {
                drop(budget); tokio::time::sleep(Duration::from_millis(200)).await;
                (_, response, budget) = http.receive(http.client.get(http.endpoint(&["api", "2.0", "sql", "statements", &id]))).await?;
            }
            verify(response.status.state == "SUCCEEDED", format!("Databricks statement ended with {}", response.status.state))?;
            let manifest = response.manifest.context("missing Databricks manifest")?;
            verify(!manifest.truncated, "Databricks result was truncated; reduce the mapped view result size")?;
            let fields = manifest.schema.columns.into_iter().map(Column::field).collect::<Result<Vec<_>>>()?;
            let mut chunk = response.result.unwrap_or_default();
            let mut offset = 0; let mut index = 0; let mut first = Some(fields);
            loop {
                verify(chunk.chunk_index == index && chunk.row_offset == offset && chunk.row_count == chunk.data_array.len(), "incomplete or out-of-order Databricks chunk")?;
                offset += chunk.row_count;
                let next = chunk.next_chunk_internal_link.take();
                yield Page { fields: first.take(), rows: chunk.data_array, _budget: budget };
                let Some(next) = next else { break };
                index += 1;
                let url = http.base.join(&next)?;
                let expected = http.endpoint(&["api", "2.0", "sql", "statements", &id, "result", "chunks", &index.to_string()]);
                verify(url.origin() == http.base.origin() && url.path() == expected.path() && url.fragment().is_none() && url.username().is_empty() && url.password().is_none(), "Databricks cursor escaped the statement result")?;
                (_, chunk, budget) = http.receive(http.client.get(url)).await?;
            }
            verify(offset == manifest.total_row_count, "incomplete Databricks result")?;
            pending.0 = None;
        })
    }
}

#[derive(Deserialize)]
struct Response {
    statement_id: Option<String>,
    status: Status,
    manifest: Option<Manifest>,
    result: Option<Chunk>,
}
#[derive(Deserialize)]
struct Status {
    state: String,
}
#[derive(Deserialize)]
struct Manifest {
    schema: Columns,
    total_row_count: usize,
    #[serde(default)]
    truncated: bool,
}
#[derive(Deserialize)]
struct Columns {
    columns: Vec<Column>,
}
#[derive(Deserialize)]
struct Column {
    name: String,
    type_name: String,
    type_text: Option<String>,
    type_precision: Option<u8>,
    type_scale: Option<i8>,
}
#[derive(Default, Deserialize)]
struct Chunk {
    #[serde(default)]
    chunk_index: usize,
    #[serde(default)]
    row_offset: usize,
    #[serde(default)]
    row_count: usize,
    #[serde(default)]
    data_array: Vec<Vec<Option<String>>>,
    next_chunk_internal_link: Option<String>,
}
impl Column {
    fn field(self) -> Result<Field> {
        let kind = match self.type_name.as_str() {
            "BOOLEAN" => DataType::Boolean,
            "BYTE" | "TINYINT" => DataType::Int8,
            "SHORT" | "SMALLINT" => DataType::Int16,
            "INT" | "INTEGER" => DataType::Int32,
            "LONG" | "BIGINT" => DataType::Int64,
            "FLOAT" => DataType::Float32,
            "DOUBLE" => DataType::Float64,
            "STRING" | "CHAR" | "VARCHAR" => DataType::Utf8,
            "BINARY" => DataType::Binary,
            "DATE" => DataType::Date32,
            "TIMESTAMP" => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            "TIMESTAMP_NTZ" => DataType::Timestamp(TimeUnit::Microsecond, None),
            "DECIMAL" => {
                let text = self.type_text.as_deref().unwrap_or("");
                let parts = text
                    .strip_prefix("DECIMAL(")
                    .and_then(|v| v.strip_suffix(')'))
                    .and_then(|v| v.split_once(','));
                let precision = self
                    .type_precision
                    .or_else(|| parts.and_then(|(p, _)| p.trim().parse().ok()))
                    .context("missing Databricks decimal precision")?;
                let scale = self
                    .type_scale
                    .or_else(|| parts.and_then(|(_, s)| s.trim().parse().ok()))
                    .context("missing Databricks decimal scale")?;
                types::numeric(precision, scale)?
            }
            other => bail!("unsupported Databricks type {other}; expose scalar columns in a view"),
        };
        Ok(Field::new(self.name, kind, true))
    }
}
