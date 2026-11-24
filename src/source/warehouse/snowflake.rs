use std::time::Duration;

use anyhow::{Context, Result, bail};
use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
use futures::stream::BoxStream;
use reqwest::StatusCode;
use serde::Deserialize;
use serde_json::json;

use super::{Http, Page, Pending, types::numeric, verify};
use crate::catalog::ident;

pub(super) const CLOCK: &str = r#"SELECT TO_CHAR(CURRENT_TIMESTAMP()::TIMESTAMP_TZ,
 'YYYY-MM-DD"T"HH24:MI:SS.FF6TZH:TZM') AS "snapshot_time""#;

pub(super) struct Warehouse {
    pub(super) http: Http,
    pub(super) warehouse: String,
    pub(super) role: Option<String>,
    pub(super) timeout: Duration,
}

pub(super) fn select(table: &str, columns: &[String], version: &str) -> String {
    let fields = columns
        .iter()
        .map(|field| ident(field))
        .collect::<Vec<_>>()
        .join(", ");
    let fields = if fields.is_empty() {
        "*".into()
    } else {
        fields
    };
    let table = table.split('.').map(ident).collect::<Vec<_>>().join(".");
    format!("SELECT {fields} FROM {table} AT (TIMESTAMP => '{version}'::TIMESTAMP_TZ)")
}

impl Warehouse {
    pub(super) fn pages<'a>(&'a self, sql: &'a str) -> BoxStream<'a, Result<Page>> {
        Box::pin(async_stream::try_stream! {
            let http = &self.http;
            let endpoint = http.endpoint(&["api", "v2", "statements"]);
            let mut body = json!({
                "statement": sql, "warehouse": self.warehouse,
                "timeout": self.timeout.as_secs().clamp(1, 86_400),
                "parameters": {
                    "multi_statement_count": "1", "timezone": "UTC", "binary_output_format": "HEX",
                    "date_output_format": "YYYY-MM-DD", "time_output_format": "HH24:MI:SS.FF9",
                    "timestamp_ntz_output_format": "YYYY-MM-DD\"T\"HH24:MI:SS.FF9",
                    "timestamp_ltz_output_format": "YYYY-MM-DD\"T\"HH24:MI:SS.FF9TZH:TZM",
                    "timestamp_tz_output_format": "YYYY-MM-DD\"T\"HH24:MI:SS.FF9TZH:TZM",
                    "client_result_chunk_size": "16",
                },
            });
            if let Some(role) = &self.role { body["role"] = json!(role); }
            let request_id = uuid::Uuid::new_v4().to_string();
            let request = http.client.post(endpoint)
                .query(&[("async", "true"), ("nullable", "true"), ("requestId", &request_id)]).json(&body);
            let (mut status, mut result, mut budget) = http.receive::<Response>(request).await?;
            let handle = result.statement_handle.take().context("missing Snowflake statement handle")?;
            uuid::Uuid::parse_str(&handle).context("invalid Snowflake statement handle")?;
            let mut pending = Pending(Some(http.client.post(http.endpoint(&["api", "v2", "statements", &handle, "cancel"]))));
            let deadline = tokio::time::Instant::now() + self.timeout;
            let status_url = http.endpoint(&["api", "v2", "statements", &handle]);
            while status == StatusCode::ACCEPTED {
                drop(budget);
                let remaining = deadline
                    .checked_duration_since(tokio::time::Instant::now())
                    .filter(|d| !d.is_zero())
                    .context("Snowflake statement timed out")?;
                tokio::time::sleep(Duration::from_millis(200).min(remaining)).await;
                let remaining = deadline
                    .checked_duration_since(tokio::time::Instant::now())
                    .filter(|d| !d.is_zero())
                    .context("Snowflake statement timed out")?;
                (status, result, budget) = tokio::time::timeout(
                    remaining,
                    http.receive(http.client.get(status_url.clone())),
                )
                .await
                .context("Snowflake statement timed out")??;
            }
            verify(status == StatusCode::OK, format!("unexpected Snowflake status {status}"))?;
            pending.0 = None;
            let metadata = result.result_set_meta_data.context("missing Snowflake result metadata")?;
            let fields = metadata.row_type.into_iter().map(Column::field).collect::<Result<Vec<_>>>()?;
            let parts = metadata.partition_info;
            let mut rows = result.data.unwrap_or_default();
            verify(rows.len() == parts.first().map_or(0, |part| part.row_count), "incomplete Snowflake partition 0")?;
            let mut received = rows.len();
            yield Page { fields: Some(fields), rows, _budget: budget };
            for (index, part) in parts.iter().enumerate().skip(1) {
                let remaining = deadline
                    .checked_duration_since(tokio::time::Instant::now())
                    .filter(|d| !d.is_zero())
                    .context("Snowflake statement timed out")?;
                let (status, response, budget) = tokio::time::timeout(
                    remaining,
                    http.receive::<Response>(
                        http.client.get(status_url.clone()).query(&[("partition", index)])
                    ),
                )
                .await
                .context("Snowflake statement timed out")??;
                verify(status == StatusCode::OK, format!("unexpected Snowflake partition status {status}"))?;
                rows = response.data.context("missing Snowflake partition data")?;
                verify(rows.len() == part.row_count, format!("incomplete Snowflake partition {index}"))?;
                received += rows.len();
                yield Page { fields: None, rows, _budget: budget };
            }
            verify(received == metadata.num_rows, "Snowflake result row count mismatch")?;
        })
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Response {
    statement_handle: Option<String>,
    result_set_meta_data: Option<Metadata>,
    data: Option<Vec<Vec<Option<String>>>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Metadata {
    num_rows: usize,
    row_type: Vec<Column>,
    partition_info: Vec<Partition>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Partition {
    row_count: usize,
}

#[derive(Deserialize)]
struct Column {
    name: String,
    #[serde(rename = "type")]
    kind: String,
    precision: Option<u8>,
    scale: Option<i8>,
    nullable: bool,
}

impl Column {
    fn field(self) -> Result<Field> {
        let data_type = match self.kind.to_ascii_lowercase().as_str() {
            "fixed" => match (self.precision, self.scale) {
                (Some(1..=4), Some(0)) => DataType::Int16,
                (Some(5..=9), Some(0)) => DataType::Int32,
                (Some(10..=18), Some(0)) => DataType::Int64,
                (Some(precision), Some(scale)) => numeric(precision, scale)?,
                _ => bail!(
                    "Snowflake fixed-point column {} has no precision or scale",
                    self.name
                ),
            },
            "real" => DataType::Float64,
            "boolean" => DataType::Boolean,
            "text" | "variant" | "object" | "array" => DataType::Utf8,
            "binary" => DataType::Binary,
            "date" => DataType::Date32,
            "time" => DataType::Time64(if self.scale.unwrap_or(9) <= 6 {
                TimeUnit::Microsecond
            } else {
                TimeUnit::Nanosecond
            }),
            "timestamp_ntz" => DataType::Timestamp(TimeUnit::Microsecond, None),
            "timestamp_ltz" | "timestamp_tz" => {
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
            }
            _ => bail!(
                "unsupported Snowflake column type {} for {}",
                self.kind,
                self.name
            ),
        };
        Ok(Field::new(self.name, data_type, self.nullable))
    }
}
