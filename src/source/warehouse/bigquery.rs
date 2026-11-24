use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
use futures::stream::BoxStream;
use serde::Deserialize;
use serde_json::{Value, json};

use super::{Http, Page, Pending, types::numeric, verify};
use crate::catalog::warehouse_identifier;

pub(super) const CLOCK: &str =
    "SELECT FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E6SZ', CURRENT_TIMESTAMP(), 'UTC') AS snapshot_time";

pub(super) struct Warehouse {
    pub(super) http: Http,
    pub(super) project: String,
    pub(super) location: String,
    pub(super) maximum_bytes_billed: u64,
    pub(super) timeout: Duration,
}

pub(super) fn select(table: &str, columns: &[String], version: &str) -> Result<String> {
    let quote = char::from(96);
    ensure!(
        columns.iter().all(|column| warehouse_identifier(column)),
        "BigQuery mapped columns must use simple identifiers"
    );
    let columns = columns
        .iter()
        .map(|column| format!("{quote}{column}{quote}"))
        .collect::<Vec<_>>()
        .join(", ");
    let columns = if columns.is_empty() {
        "*".into()
    } else {
        columns
    };
    Ok(format!(
        "SELECT {columns} FROM {quote}{table}{quote} FOR SYSTEM_TIME AS OF TIMESTAMP '{version}'"
    ))
}

impl Warehouse {
    pub(super) fn pages<'a>(
        &'a self,
        sql: &'a str,
        batch_size: usize,
    ) -> BoxStream<'a, Result<Page>> {
        Box::pin(async_stream::try_stream! {
            let http = &self.http;
            let job = format!("graphlake_{}", uuid::Uuid::new_v4().simple());
            let jobs = ["bigquery", "v2", "projects", &self.project, "jobs"];
            let cancel = http.endpoint(&[&jobs[..], &[job.as_str(), "cancel"]].concat());
            let mut pending = Pending(Some(http.client.post(cancel).query(&[("location", &self.location)])));
            let timeout_ms = self.timeout.as_millis().clamp(1, i64::MAX as u128).to_string();
            let (.., inserted, submit_budget) = http.receive::<Job>(http.client.post(http.endpoint(&jobs)).json(&json!({
                "jobReference": {"projectId": self.project, "jobId": job, "location": self.location},
                "configuration": {
                    "jobTimeoutMs": timeout_ms,
                    "query": {"query": sql, "useLegacySql": false, "maximumBytesBilled": self.maximum_bytes_billed.to_string()},
                },
            }))).await?;
            verify(inserted.job_reference.job_id == job
                && inserted.job_reference.project_id == self.project
                && inserted.job_reference.location.as_deref().is_some_and(|location| location.eq_ignore_ascii_case(&self.location)), "BigQuery returned an unexpected job reference")?;
            if let Some(error) = inserted.status.and_then(|status| status.error_result) {
                pending.0 = None;
                Err(anyhow::anyhow!("BigQuery job failed ({})", error.reason))?;
            }
            drop(submit_budget);

            let deadline = tokio::time::Instant::now() + self.timeout;
            let results = http.endpoint(&["bigquery", "v2", "projects", &self.project, "queries", &job]);
            let max_results = batch_size.min(10_000).to_string();
            let mut token: Option<String> = None;
            let mut total = None;
            let mut seen = 0_u64;
            loop {
                let remaining = deadline
                    .checked_duration_since(tokio::time::Instant::now())
                    .filter(|d| !d.is_zero())
                    .context("BigQuery job timed out")?;
                let mut request = http.client.get(results.clone()).query(&[
                    ("location", self.location.as_str()), ("timeoutMs", "500"),
                    ("maxResults", max_results.as_str()), ("formatOptions.useInt64Timestamp", "true"),
                ]);
                if let Some(token) = &token { request = request.query(&[("pageToken", token)]); }
                let (_, result, budget) = tokio::time::timeout(
                    remaining,
                    http.receive::<Response>(request),
                )
                .await
                .context("BigQuery job timed out")??;
                if !result.job_complete {
                    verify(token.is_none(), "BigQuery completed job became incomplete during pagination")?;
                    drop(budget);
                    let remaining = deadline
                        .checked_duration_since(tokio::time::Instant::now())
                        .filter(|d| !d.is_zero())
                        .context("BigQuery job timed out")?;
                    tokio::time::sleep(Duration::from_millis(200).min(remaining)).await;
                    continue;
                }
                pending.0 = None;
                if total.is_none() {
                    if result.schema.is_none() && let Some(error) = result.errors.first() {
                        Err(anyhow::anyhow!("BigQuery job failed ({})", error.reason))?;
                    }
                    total = Some(result.total_rows.context("missing BigQuery totalRows")?.parse::<u64>()?);
                }
                let fields = result.schema.map(|schema| schema.fields.into_iter().map(Column::field).collect::<Result<Vec<_>>>()).transpose()?;
                let rows = result.rows.into_iter().map(|row| row.f.into_iter().map(|cell| cell_string(cell.v))
                    .collect::<Result<Vec<_>>>()).collect::<Result<Vec<_>>>()?;
                seen = seen.checked_add(rows.len() as u64).context("BigQuery row count overflow")?;
                let next = result.page_token.filter(|token| !token.is_empty());
                let total = total.expect("BigQuery result row count");
                verify(seen <= total && (next.is_some() || seen == total), "BigQuery result row count mismatch")?;
                verify(next.is_none() || (next != token && !rows.is_empty() && seen < total), "BigQuery pagination did not advance")?;
                yield Page { fields, rows, _budget: budget };
                if next.is_none() { break; }
                token = next;
            }
        })
    }
}

fn cell_string(value: Value) -> Result<Option<String>> {
    Ok(match value {
        Value::Null => None,
        Value::String(value) => Some(value),
        Value::Bool(value) => Some(value.to_string()),
        Value::Number(value) => Some(value.to_string()),
        _ => bail!("unsupported structured BigQuery result cell"),
    })
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Job {
    job_reference: JobRef,
    status: Option<JobStatus>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct JobRef {
    job_id: String,
    project_id: String,
    location: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct JobStatus {
    error_result: Option<ErrorInfo>,
}

#[derive(Deserialize)]
struct ErrorInfo {
    reason: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Response {
    job_complete: bool,
    schema: Option<Schema>,
    total_rows: Option<String>,
    page_token: Option<String>,
    #[serde(default)]
    rows: Vec<Row>,
    #[serde(default)]
    errors: Vec<ErrorInfo>,
}

#[derive(Deserialize)]
struct Row {
    f: Vec<Cell>,
}

#[derive(Deserialize)]
struct Cell {
    v: Value,
}

#[derive(Deserialize)]
struct Schema {
    fields: Vec<Column>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Column {
    name: String,
    #[serde(rename = "type")]
    kind: String,
    mode: Option<String>,
    precision: Option<String>,
    scale: Option<String>,
    timestamp_precision: Option<String>,
}

impl Column {
    fn field(self) -> Result<Field> {
        let nullable = match self.mode.as_deref() {
            Some("REQUIRED") => false,
            Some("NULLABLE") | None => true,
            Some(mode) => bail!("unsupported BigQuery field mode {mode} for {}", self.name),
        };
        let data_type = match self.kind.as_str() {
            "INTEGER" | "INT64" => DataType::Int64,
            "FLOAT" | "FLOAT64" => DataType::Float64,
            "BOOLEAN" | "BOOL" => DataType::Boolean,
            "STRING" | "JSON" | "GEOGRAPHY" => DataType::Utf8,
            "BYTES" => DataType::Binary,
            "DATE" => DataType::Date32,
            "TIME" => DataType::Time64(TimeUnit::Microsecond),
            "DATETIME" => DataType::Timestamp(TimeUnit::Microsecond, None),
            "TIMESTAMP" => {
                ensure!(
                    self.timestamp_precision
                        .as_deref()
                        .is_none_or(|p| p == "6" || p == "TIMESTAMP_PRECISION_MICROS"),
                    "unsupported BigQuery timestamp precision"
                );
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
            }
            "NUMERIC" | "DECIMAL" => {
                let precision = self
                    .precision
                    .as_deref()
                    .map(str::parse)
                    .transpose()?
                    .unwrap_or(38);
                let scale = self
                    .scale
                    .as_deref()
                    .map(str::parse)
                    .transpose()?
                    .unwrap_or(if self.precision.is_some() { 0 } else { 9 });
                numeric(precision, scale)?
            }
            _ => bail!(
                "unsupported BigQuery column type {} for {}",
                self.kind,
                self.name
            ),
        };
        Ok(Field::new(self.name, data_type, nullable))
    }
}
