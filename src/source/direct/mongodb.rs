use crate::{
    Source,
    source::{
        ReadStats,
        snapshot::Buffer,
        warehouse::types::{self, Encoding},
    },
};
use anyhow::{Context, Result, bail, ensure};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use datafusion::{
    arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
    catalog::TableProvider,
    execution::{
        memory_pool::{MemoryConsumer, MemoryReservation},
        runtime_env::RuntimeEnv,
    },
};
use futures::TryStreamExt;
use mongodb::{
    Client,
    bson::{Bson, Document, doc},
    options::ClientOptions,
};
use std::{collections::BTreeMap, sync::Arc, time::Duration};

pub(crate) struct Driver {
    client: Client,
    sample_size: u32,
    timeout: Duration,
    runtime: Arc<RuntimeEnv>,
    stats: Arc<ReadStats>,
}

fn flatten<'a>(
    document: &'a Document,
    prefix: &str,
    output: &mut BTreeMap<String, &'a Bson>,
    depth: usize,
) -> Result<()> {
    ensure!(depth < 64, "MongoDB document nesting exceeds 64 levels");
    for (name, value) in document {
        ensure!(
            !name.contains('.') && !name.starts_with('$'),
            "MongoDB field names containing dots or starting with $ require a normalized view"
        );
        let name = if prefix.is_empty() {
            name.clone()
        } else {
            format!("{prefix}.{name}")
        };
        if let Bson::Document(child) = value {
            flatten(child, &name, output, depth + 1)?;
        } else {
            output.insert(name, value);
        }
    }
    Ok(())
}

fn kind(value: &Bson) -> Result<Option<DataType>> {
    Ok(Some(match value {
        Bson::Null => return Ok(None),
        Bson::Int32(_) | Bson::Int64(_) => DataType::Int64,
        Bson::Double(_) => DataType::Float64,
        Bson::Boolean(_) => DataType::Boolean,
        Bson::String(_) | Bson::ObjectId(_) | Bson::Decimal128(_) | Bson::Array(_) => {
            DataType::Utf8
        }
        Bson::Binary(_) => DataType::Binary,
        Bson::DateTime(_) => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        other => bail!(
            "unsupported MongoDB BSON value type {:?}",
            other.element_type()
        ),
    }))
}

fn cell(value: Option<&&Bson>, data_type: &DataType) -> Result<Option<String>> {
    let Some(value) = value.copied() else {
        return Ok(None);
    };
    let Some(actual) = kind(value)? else {
        return Ok(None);
    };
    ensure!(
        &actual == data_type,
        "MongoDB field type changed after schema sampling; normalize the collection or view"
    );
    Ok(Some(match value {
        Bson::Int32(v) => v.to_string(),
        Bson::Int64(v) => v.to_string(),
        Bson::Double(v) => v.to_string(),
        Bson::Boolean(v) => v.to_string(),
        Bson::String(v) => v.clone(),
        Bson::ObjectId(v) => v.to_hex(),
        Bson::Decimal128(v) => v.to_string(),
        Bson::Binary(v) => STANDARD.encode(&v.bytes),
        Bson::DateTime(v) => chrono::DateTime::from_timestamp_millis(v.timestamp_millis())
            .context("MongoDB datetime outside supported range")?
            .to_rfc3339(),
        Bson::Array(_) => serde_json::to_string(&value.clone().into_canonical_extjson())?,
        _ => unreachable!(),
    }))
}

fn projection<'a>(columns: impl Iterator<Item = &'a str>) -> Document {
    let mut projection = doc! { "_id": 0 };
    for column in columns {
        projection.insert(column, 1);
    }
    projection
}

impl Driver {
    pub(super) async fn open(
        source: &Source,
        timeout: Duration,
        runtime: Arc<RuntimeEnv>,
        stats: Arc<ReadStats>,
    ) -> Result<Self> {
        let Source::MongodbNative {
            dsn_env,
            schema_sample_size,
        } = source
        else {
            bail!("expected MongoDB source")
        };
        let uri = std::env::var(dsn_env)
            .with_context(|| format!("missing MongoDB connection string in {dsn_env}"))?;
        let mut options = ClientOptions::parse(uri)
            .await
            .context("invalid MongoDB connection string")?;
        if let Some(mongodb::options::Tls::Enabled(tls)) = &options.tls {
            ensure!(
                tls.allow_invalid_certificates != Some(true),
                "MongoDB TLS requires certificate verification"
            );
        }
        options.app_name = Some("GraphLake".into());
        options.connect_timeout = Some(timeout);
        options.server_selection_timeout = Some(timeout);
        options.max_pool_size = Some(2);
        Ok(Self {
            client: Client::with_options(options)?,
            sample_size: *schema_sample_size,
            timeout,
            runtime,
            stats,
        })
    }

    #[cfg(feature = "admin")]
    pub(super) async fn browse(&self, namespace: Option<&str>) -> Result<Vec<String>> {
        self.stats.request();
        let mut names = match namespace {
            Some(database) => {
                self.client
                    .database(database)
                    .list_collection_names()
                    .await?
            }
            None => self.client.list_database_names().await?,
        };
        names.sort();
        names.truncate(2001);
        Ok(names)
    }

    async fn schema(
        &self,
        collection: &mongodb::Collection<Document>,
        columns: &[String],
    ) -> Result<SchemaRef> {
        self.stats.request();
        let mut sample = collection
            .find(doc! {})
            .limit(self.sample_size as i64)
            .batch_size(self.sample_size.min(128))
            .max_time(self.timeout);
        if !columns.is_empty() {
            sample = sample.projection(projection(columns.iter().map(String::as_str)));
        }
        let mut cursor = sample.await?;
        let raw = MemoryConsumer::new("GraphLake MongoDB schema sample")
            .register(&self.runtime.memory_pool);
        let mut fields = BTreeMap::new();
        while let Some(document) = cursor.try_next().await? {
            raw.try_grow(mongodb::bson::to_vec(&document)?.len().saturating_mul(3))?;
            let mut values = BTreeMap::new();
            flatten(&document, "", &mut values, 0)?;
            for (name, value) in values {
                if !columns.is_empty() && !columns.contains(&name) {
                    continue;
                }
                if let Some(kind) = kind(value)? {
                    if let Some(previous) = fields.insert(name.clone(), kind.clone()) {
                        ensure!(
                            previous == kind,
                            "mixed MongoDB types in field {name}; normalize the collection or view"
                        );
                    }
                }
            }
            raw.free();
        }
        let selected = if columns.is_empty() {
            fields.keys().cloned().collect()
        } else {
            columns.to_vec()
        };
        ensure!(
            !fields.is_empty(),
            "MongoDB schema sample has no typed fields; an empty collection needs a typed view or existing data"
        );
        Ok(Arc::new(Schema::new(selected.iter().map(|name| {
            Ok(Field::new(name, fields.get(name).with_context(|| format!("cannot infer MongoDB field {name}; increase schema_sample_size or normalize the view"))?.clone(), true))
        }).collect::<Result<Vec<_>>>()?)))
    }

    pub(super) async fn table(
        &self,
        table: &str,
        columns: &[String],
        filters: &[crate::Predicate],
        batch_size: usize,
        explain: bool,
    ) -> Result<(Arc<dyn TableProvider>, MemoryReservation)> {
        let (database, name) = table
            .split_once('.')
            .context("expected database.collection")?;
        let collection = self.client.database(database).collection::<Document>(name);
        let schema = self.schema(&collection, columns).await?;
        let mut buffer = Buffer::new(
            &self.runtime.memory_pool,
            (!explain).then_some(&self.runtime.disk_manager),
        );
        if explain {
            return buffer.finish_schema(schema);
        }
        let projection = projection(schema.fields().iter().map(|field| field.name().as_str()));
        self.stats.request();
        let terms = filters
            .iter()
            .filter_map(|filter| mongo_predicate(&schema, filter))
            .collect::<Vec<_>>();
        let filter = if terms.is_empty() {
            doc! {}
        } else {
            doc! { "$and": terms }
        };
        let mut cursor = collection
            .find(filter)
            .projection(projection)
            .batch_size(batch_size.min(1024) as u32)
            .max_time(self.timeout)
            .await?;
        let raw = MemoryConsumer::new("GraphLake MongoDB rows").register(&self.runtime.memory_pool);
        let mut rows = Vec::new();
        loop {
            let document = cursor.try_next().await?;
            let done = document.is_none();
            if let Some(document) = document {
                let mut values = BTreeMap::new();
                flatten(&document, "", &mut values, 0)?;
                let values = schema
                    .fields()
                    .iter()
                    .map(|field| cell(values.get(field.name()), field.data_type()))
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
                let batch = types::batch(schema.clone(), &rows, Encoding::Rfc3339)?;
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

fn mongo_predicate(schema: &Schema, filter: &crate::Predicate) -> Option<Document> {
    use crate::Comparison;
    if filter.column.contains('.') || filter.column.starts_with('$') {
        return None;
    }
    let kind = schema.field_with_name(&filter.column).ok()?.data_type();
    let value = match kind {
        DataType::Int64 => Bson::Int64(filter.value.as_i64()?),
        DataType::Boolean if filter.op == Comparison::Eq => Bson::Boolean(filter.value.as_bool()?),
        _ => return None,
    };
    let op = match filter.op {
        Comparison::Eq => "$eq",
        Comparison::Lt => "$lt",
        Comparison::Le => "$lte",
        Comparison::Gt => "$gt",
        Comparison::Ge => "$gte",
        _ => return None,
    };
    Some(doc! { &filter.column: { op: value } })
}
