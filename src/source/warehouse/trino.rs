use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail, ensure};
use datafusion::{
    arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
    catalog::TableProvider,
    execution::{
        disk_manager::DiskManager,
        memory_pool::{MemoryPool, MemoryReservation},
    },
};
use futures::{StreamExt, TryStreamExt, stream::BoxStream};
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

type SchemaKey = (String, String, Vec<String>);
type CachedSchema = (SchemaKey, Instant, SchemaRef);

#[derive(Default)]
pub(crate) struct SchemaCache(Mutex<VecDeque<CachedSchema>>);

impl SchemaCache {
    fn get(&self, key: &SchemaKey) -> Option<SchemaRef> {
        let mut entries = self.0.lock().unwrap();
        entries.retain(|(_, at, _)| at.elapsed() < Duration::from_secs(60));
        let index = entries.iter().position(|(entry, _, _)| entry == key)?;
        let entry = entries.remove(index).expect("located schema");
        let schema = entry.2.clone();
        entries.push_back(entry);
        Some(schema)
    }

    fn put(&self, key: SchemaKey, schema: SchemaRef) {
        let mut entries = self.0.lock().unwrap();
        entries.retain(|(entry, _, _)| entry != &key);
        if entries.len() >= 128 {
            entries.pop_front();
        }
        entries.push_back((key, Instant::now(), schema));
    }

    fn remove(&self, key: &SchemaKey) {
        self.0.lock().unwrap().retain(|(entry, _, _)| entry != key);
    }
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
        cache: &SchemaCache,
        source: &str,
    ) -> Result<(Arc<dyn TableProvider>, MemoryReservation, String)> {
        let relation = self.qualified_relation(relation)?;
        let key = (source.to_owned(), relation.clone(), columns.to_vec());
        let selected = columns
            .iter()
            .map(|column| ident(column))
            .collect::<Vec<_>>()
            .join(", ");
        let mut metadata_id = None;
        // A fresh schema is needed before pushing predicates based on its types.
        let use_cache = !explain_only && !crate::engine::read_filters::needs_sql_types(filters);
        let cached = use_cache.then(|| cache.get(&key)).flatten();
        let expected = if let Some(schema) = cached.as_ref() {
            schema.clone()
        } else if use_cache {
            let mut id = None;
            let mut pages = self.pages(format!("SELECT {selected} FROM {relation}"), &mut id);
            let first = pages
                .try_next()
                .await?
                .context("warehouse result has no schema")?;
            let fields = first
                .fields
                .as_ref()
                .context("warehouse result has no schema")?;
            if !fields
                .iter()
                .any(|field| matches!(field.data_type(), DataType::Timestamp(_, Some(_))))
            {
                let mut pages = futures::stream::iter([Ok(first)]).chain(pages).boxed();
                let (schema, buffer) = collect_pages(
                    &self.http,
                    &mut pages,
                    columns,
                    batch_size,
                    true,
                    Encoding::Trino,
                    Some(disk),
                )
                .await?;
                drop(pages);
                let (table, budget) = buffer.finish(schema, &self.http.stats)?;
                return Ok((table, budget, id.context("Trino response has no query ID")?));
            }
            // Dropping the unnormalized read cancels its remaining pages.
            Arc::new(Schema::new(fields.clone()))
        } else {
            let mut pages = self.pages(
                format!("SELECT {selected} FROM {relation} WHERE FALSE"),
                &mut metadata_id,
            );
            let (expected, buffer) = collect_pages(
                &self.http,
                &mut pages,
                columns,
                batch_size,
                false,
                Encoding::Trino,
                None,
            )
            .await?;
            if explain_only {
                let (table, budget) = buffer.finish_schema(expected)?;
                drop(pages);
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
            expected
        };
        // A zone name plus a wall time is ambiguous at a DST fold. Convert
        // before the Trino protocol formats the timestamp as text.
        let projection = expected
            .fields()
            .iter()
            .map(|field| {
                let column = ident(field.name());
                if matches!(field.data_type(), DataType::Timestamp(_, Some(_))) {
                    format!("at_timezone({column}, 'UTC') AS {column}")
                } else {
                    column
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
        let where_clause = crate::engine::read_filters::where_clause(filters, &expected, ident);
        let sql = format!("SELECT {projection} FROM {relation}{where_clause}");
        let mut id = None;
        let result = {
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
            .await
        };
        if cached.is_some()
            && !result
                .as_ref()
                .is_ok_and(|(schema, _)| schema.fields() == expected.fields())
        {
            cache.remove(&key);
        }
        let (schema, buffer) = result?;
        ensure!(
            expected.fields() == schema.fields(),
            "Trino result schema changed after describing the table"
        );
        let (table, budget) = buffer.finish(schema, &self.http.stats)?;
        if cached.is_none()
            && expected
                .fields()
                .iter()
                .any(|field| matches!(field.data_type(), DataType::Timestamp(_, Some(_))))
        {
            cache.put(key, expected);
        }
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
            let limit_ms = self.timeout.as_millis().max(1);
            let mut request = self.auth(self.http.client.post(self.http.endpoint(&["v1", "statement"])))
                .header(CONTENT_TYPE, "text/plain; charset=utf-8")
                .header("X-Trino-User", &self.user)
                .header("X-Trino-Source", "GraphLake")
                .header("X-Trino-Client-Capabilities", "PARAMETRIC_DATETIME")
                .header("X-Trino-Time-Zone", "UTC")
                .header(
                    "X-Trino-Session",
                    format!("query_max_run_time={limit_ms}ms,query_max_execution_time={limit_ms}ms"),
                )
                .body(sql);
            let mut pending = Pending(None);
            let mut previous = None;
            let deadline = tokio::time::Instant::now() + self.timeout;
            loop {
                let remaining = deadline
                    .checked_duration_since(tokio::time::Instant::now())
                    .filter(|d| !d.is_zero())
                    .context("Trino query timed out")?;
                let (_, result, budget) = tokio::time::timeout(
                    remaining,
                    self.http.receive::<Results>(request),
                )
                .await
                .context("Trino query timed out")??;
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
                let empty_page = fields.is_none() && rows.is_empty();
                if !empty_page {
                    yield Page { fields, rows, _budget: budget };
                } else {
                    drop(budget);
                }
                let Some(next) = next else { break; };
                if empty_page {
                    let remaining = deadline
                        .checked_duration_since(tokio::time::Instant::now())
                        .filter(|d| !d.is_zero())
                        .context("Trino query timed out")?;
                    tokio::time::sleep(Duration::from_millis(100).min(remaining)).await;
                }
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
            "timestamp" => DataType::Timestamp(units()?, None),
            "timestamp with time zone" => DataType::Timestamp(units()?, Some("UTC".into())),
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
