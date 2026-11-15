use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use datafusion::{
    arrow::datatypes::{DataType, Field, TimeUnit},
    catalog::TableProvider,
    execution::{
        disk_manager::DiskManager,
        memory_pool::{MemoryPool, MemoryReservation},
    },
};
use futures::stream::BoxStream;
use reqwest::{RequestBuilder, header::CONTENT_TYPE};
use serde::Deserialize;
use serde_json::Value;
use url::Url;

use super::{
    Http, Page, Pending, collect_pages, credential,
    types::{Encoding, numeric},
    verify,
};
use crate::{Source, catalog::ident, source::ReadStats};

pub(crate) struct Trino {
    http: Http,
    catalog: Option<String>,
    user: String,
    password: Option<String>,
    timeout: Duration,
}

impl Trino {
    fn qualified_relation(&self, relation: &str) -> Result<String> {
        Ok(if let Some(catalog) = self.catalog.as_deref() {
            let (schema, table) = relation.split_once('.').context("expected schema.table")?;
            [catalog, schema, table].map(ident).join(".")
        } else {
            relation.split('.').map(ident).collect::<Vec<_>>().join(".")
        })
    }

    #[cfg(feature = "admin")]
    pub(crate) async fn describe(&self, relation: &str) -> Result<Vec<(String, String)>> {
        use futures::TryStreamExt;
        let mut id = None;
        let mut pages = self.pages(
            format!("SHOW COLUMNS FROM {}", self.qualified_relation(relation)?),
            &mut id,
        );
        let mut columns = Vec::new();
        while let Some(page) = pages.try_next().await? {
            for row in page.rows {
                if let [Some(name), Some(kind), ..] = row.as_slice() {
                    columns.push((name.clone(), kind.clone()));
                }
            }
        }
        Ok(columns)
    }

    #[cfg(feature = "admin")]
    pub(crate) async fn browse(&self, namespace: Option<&str>) -> Result<Vec<String>> {
        let (sql, prefix) = match (self.catalog.as_deref(), namespace) {
            (None, None) => ("SHOW CATALOGS".to_owned(), None),
            (Some(catalog), None) => (format!("SHOW SCHEMAS FROM {}", ident(catalog)), None),
            (None, Some(catalog)) if !catalog.contains('.') => (
                format!("SHOW SCHEMAS FROM {}", ident(catalog)),
                Some(catalog),
            ),
            (None, Some(path)) => {
                let (catalog, schema) = path.split_once('.').context("expected catalog.schema")?;
                (
                    format!("SHOW TABLES FROM {}.{}", ident(catalog), ident(schema)),
                    None,
                )
            }
            (Some(catalog), Some(schema)) => (
                format!("SHOW TABLES FROM {}.{}", ident(catalog), ident(schema)),
                None,
            ),
        };
        let mut id = None;
        let mut pages = self.pages(sql, &mut id);
        use futures::TryStreamExt;
        let mut names = Vec::new();
        while let Some(page) = pages.try_next().await? {
            for row in page.rows {
                if let Some(Some(name)) = row.into_iter().next() {
                    names.push(if let Some(catalog) = prefix {
                        format!("{catalog}.{name}")
                    } else {
                        name
                    });
                    if names.len() == 2001 {
                        return Ok(names);
                    }
                }
            }
        }
        Ok(names)
    }
    pub(crate) fn new(
        source: &Source,
        timeout: Duration,
        pool: Arc<dyn MemoryPool>,
        stats: Arc<ReadStats>,
    ) -> Result<Self> {
        let (uri, user, token, password, catalog) = match source {
            Source::Trino {
                uri,
                user,
                token_env,
                password_env,
            } => (uri, user, token_env, password_env, None),
            Source::Mongodb(config)
            | Source::Elasticsearch(config)
            | Source::Redshift(config)
            | Source::Sqlserver(config)
            | Source::Oracle(config) => (
                &config.uri,
                &config.user,
                &config.token_env,
                &config.password_env,
                Some(config.catalog.clone()),
            ),
            _ => bail!("expected Trino source"),
        };
        Ok(Self {
            http: Http::new(uri, token.as_deref(), timeout, pool, stats)?,
            catalog,
            user: user.clone(),
            password: password.as_deref().map(credential).transpose()?,
            timeout,
        })
    }

    pub(crate) async fn table(
        &self,
        relation: &str,
        columns: &[String],
        filters: &[crate::Predicate],
        batch_size: usize,
        disk: &Arc<DiskManager>,
        explain_only: bool,
    ) -> Result<(Arc<dyn TableProvider>, MemoryReservation, String)> {
        let relation = self.qualified_relation(relation)?;
        let selected = columns
            .iter()
            .map(|column| ident(column))
            .collect::<Vec<_>>()
            .join(", ");
        let mut sql = format!("SELECT {selected} FROM {relation}");
        let expected = if !crate::engine::read_filters::needs_sql_types(filters) && !explain_only {
            None
        } else {
            let mut metadata_id = None;
            let (schema, buffer) = {
                let mut pages = self.pages(format!("{sql} WHERE FALSE"), &mut metadata_id);
                collect_pages(
                    &self.http,
                    &mut pages,
                    columns,
                    batch_size,
                    false,
                    Encoding::Trino,
                    None,
                )
                .await?
            };
            if explain_only {
                let (table, budget) = buffer.finish_schema(schema)?;
                return Ok((
                    table,
                    budget,
                    metadata_id.context("Trino response has no query ID")?,
                ));
            }
            ensure!(
                buffer.memory().is_empty(),
                "Trino schema query returned rows"
            );
            sql.push_str(&crate::engine::read_filters::where_clause(
                filters, &schema, ident,
            ));
            Some(schema)
        };
        let mut id = None;
        let (schema, buffer) = {
            let mut pages = self.pages(sql, &mut id);
            collect_pages(
                &self.http,
                &mut pages,
                columns,
                batch_size,
                true,
                Encoding::Trino,
                Some(disk),
            )
            .await?
        };
        ensure!(
            expected.is_none_or(|expected| expected.fields() == schema.fields()),
            "Trino result schema changed after describing the table"
        );
        let (table, budget) = buffer.finish(schema, &self.http.stats)?;
        Ok((table, budget, id.context("Trino response has no query ID")?))
    }

    fn auth(&self, request: RequestBuilder) -> RequestBuilder {
        match &self.password {
            Some(password) => request.basic_auth(&self.user, Some(password)),
            None => request,
        }
    }

    fn pages<'a>(&'a self, sql: String, id: &'a mut Option<String>) -> BoxStream<'a, Result<Page>> {
        Box::pin(async_stream::try_stream! {
            let mut request = self.auth(self.http.client.post(self.http.endpoint(&["v1", "statement"])))
                .header(CONTENT_TYPE, "text/plain; charset=utf-8")
                .header("X-Trino-User", &self.user)
                .header("X-Trino-Source", "GraphLake")
                .header("X-Trino-Client-Capabilities", "PARAMETRIC_DATETIME")
                .header("X-Trino-Time-Zone", "UTC")
                .header("X-Trino-Session", format!("query_max_run_time={}ms", self.timeout.as_millis().max(1)))
                .body(sql);
            let mut pending = Pending(None);
            let mut previous = None;
            loop {
                let (_, result, budget) = self.http.receive::<Results>(request).await?;
                verify(!result.id.is_empty() && id.as_ref().is_none_or(|expected| expected == &result.id),
                    "Trino returned a different query ID")?;
                if id.is_none() { *id = Some(result.id); }
                if let Some(error) = result.error {
                    pending.0 = None;
                    Err(anyhow::anyhow!("Trino query failed ({})", error.error_name))?;
                }
                let next = result.next_uri.as_deref().map(|uri| self.cursor(uri)).transpose()?;
                verify(next.is_none() || next != previous, "Trino result cursor did not advance")?;
                pending.0 = next.clone().map(|url| self.auth(self.http.client.delete(url)));
                let fields = result.columns.map(|columns| columns.into_iter().map(Column::field)
                    .collect::<Result<Vec<_>>>()).transpose()?;
                let rows = result.data.unwrap_or_default().into_iter().map(|row| row.into_iter().map(cell).collect())
                    .collect::<Result<Vec<_>>>()?;
                if fields.is_some() || !rows.is_empty() {
                    yield Page { fields, rows, _budget: budget };
                }
                let Some(next) = next else { break; };
                request = self.auth(self.http.client.get(next.clone()));
                previous = Some(next);
            }
        })
    }

    fn cursor(&self, uri: &str) -> Result<Url> {
        let url = self.http.base.join(uri)?;
        ensure!(
            url.origin() == self.http.base.origin()
                && url.username().is_empty()
                && url.password().is_none()
                && url.fragment().is_none()
                && url.path().starts_with("/v1/statement/"),
            "invalid Trino result cursor"
        );
        Ok(url)
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Results {
    id: String,
    next_uri: Option<String>,
    columns: Option<Vec<Column>>,
    data: Option<Vec<Vec<Value>>>,
    error: Option<TrinoError>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TrinoError {
    error_name: String,
}

#[derive(Deserialize)]
struct Column {
    name: String,
    #[serde(rename = "type")]
    kind: String,
}

impl Column {
    fn field(self) -> Result<Field> {
        let (base, precision) = if let Some((base, rest)) = self.kind.split_once('(') {
            let (parameters, suffix) = rest
                .split_once(')')
                .context("invalid Trino type signature")?;
            (format!("{base}{suffix}"), Some(parameters))
        } else {
            (self.kind.clone(), None)
        };
        let units = || -> Result<TimeUnit> {
            Ok(match precision.unwrap_or("3").parse::<u8>()? {
                0..=6 => TimeUnit::Microsecond,
                7..=9 => TimeUnit::Nanosecond,
                _ => bail!("Trino temporal precision exceeds Arrow nanoseconds"),
            })
        };
        let ty = match base.as_str() {
            "tinyint" => DataType::Int8,
            "smallint" => DataType::Int16,
            "integer" => DataType::Int32,
            "bigint" => DataType::Int64,
            "real" => DataType::Float32,
            "double" => DataType::Float64,
            "boolean" => DataType::Boolean,
            "char" | "varchar" | "uuid" | "ipaddress" | "json" => DataType::Utf8,
            "varbinary" | "ObjectId" | "objectid" => DataType::Binary,
            "date" => DataType::Date32,
            "time" => DataType::Time64(units()?),
            "timestamp" => {
                units()?;
                DataType::Timestamp(TimeUnit::Microsecond, None)
            }
            "timestamp with time zone" => {
                units()?;
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
            }
            "decimal" => {
                let (p, s) = precision
                    .and_then(|value| value.split_once(','))
                    .context("invalid Trino decimal")?;
                numeric(p.trim().parse()?, s.trim().parse()?)?
            }
            _ => bail!("unsupported Trino type {} for {}", self.kind, self.name),
        };
        Ok(Field::new(self.name, ty, true))
    }
}

fn cell(value: Value) -> Result<Option<String>> {
    Ok(match value {
        Value::Null => None,
        Value::String(value) => Some(value),
        Value::Bool(value) => Some(value.to_string()),
        Value::Number(value) => Some(value.to_string()),
        _ => bail!("structured Trino result values require explicit scalar columns"),
    })
}
