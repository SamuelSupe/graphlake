use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, State},
};
use datafusion::{
    catalog::TableProvider,
    execution::{memory_pool::MemoryPool, runtime_env::RuntimeEnvBuilder},
};
use serde::{Deserialize, Serialize};

use crate::{
    Catalog, Source,
    source::{ReadStats, parquet::ParquetTable, postgres::Snapshot},
};

use super::{ApiResult, App, Error};

#[derive(Deserialize)]
pub(super) struct Request {
    yaml: String,
    source: String,
    schema: Option<String>,
    table: Option<String>,
    object_prefix: Option<String>,
}

#[derive(Default, Serialize)]
pub(super) struct Listing {
    schemas: Vec<String>,
    tables: Vec<String>,
    columns: Vec<Column>,
    truncated: bool,
    objects: Vec<String>,
}

#[derive(Serialize)]
struct Column {
    name: String,
    data_type: String,
    nullable: bool,
}

pub(super) async fn browse(
    State(app): State<Arc<App>>,
    Path(id): Path<String>,
    Json(request): Json<Request>,
) -> ApiResult<Json<Listing>> {
    let _permit = app.metadata_work.try_acquire().map_err(|_| Error::busy())?;
    let path = app.models.lock().unwrap().source_path(&id)?;
    if request.yaml.len() > super::CATALOG_LIMIT {
        return Err(Error::invalid("catalog_limit", "YAML 文件超过 1 MiB"));
    }
    let catalog = Catalog::parse_paths(&request.yaml, &path)
        .map_err(|error| Error::invalid("invalid_model", format!("{error:#}")))?;
    let source = catalog
        .sources
        .get(&request.source)
        .ok_or_else(Error::not_found)?;
    let listing_objects =
        request.object_prefix.is_some() && matches!(source, Source::ObjectStorage { .. });
    let result = tokio::time::timeout(
        app.options.timeout,
        read(&catalog, source, &request, &app, &id),
    )
    .await
    .map_err(|_| {
        Error::new(
            axum::http::StatusCode::GATEWAY_TIMEOUT,
            "timeout",
            if listing_objects {
                "读取对象列表超时"
            } else {
                "读取表结构超时"
            },
        )
    })?
    .map_err(|_| {
        if listing_objects {
            Error::invalid(
                "object_list_failed",
                "无法读取对象列表，请检查存储端点、桶或容器名称，以及列出对象的权限",
            )
        } else {
            Error::invalid(
                "browse_failed",
                "无法读取表结构，请检查数据源配置、权限和表名",
            )
        }
    })?;
    Ok(Json(result))
}

async fn read(
    catalog: &Catalog,
    source: &Source,
    request: &Request,
    app: &App,
    model: &str,
) -> anyhow::Result<Listing> {
    let mut listing = Listing::default();
    let stats = Arc::new(ReadStats::default());
    let timeout = app.options.timeout;
    let runtime = Arc::new(
        RuntimeEnvBuilder::new()
            .with_memory_limit(app.options.memory_bytes, 1.0)
            .build()?,
    );
    let pool: &Arc<dyn MemoryPool> = &runtime.memory_pool;
    let disk = &runtime.disk_manager;
    let size = app.options.batch_size;
    let mut relation = None;
    if let Some(table) = request.table.as_deref() {
        let reference = match &request.schema {
            Some(schema) => format!("{}.{schema}.{table}", request.source),
            None => format!("{}.{table}", request.source),
        };
        catalog.source(&reference)?;
        relation = Some(reference[request.source.len() + 1..].to_owned());
    }
    let relation = relation.as_deref();
    let mut provider: Option<Arc<dyn TableProvider>> = None;
    match source {
        Source::Duckdb { .. }
        | Source::SqlserverNative { .. }
        | Source::OracleNative { .. }
        | Source::MongodbNative { .. } => {
            let mut driver = Box::pin(crate::source::direct::Driver::open(
                source,
                timeout,
                Arc::clone(&runtime),
                stats,
                1,
            ))
            .await?;
            if let Some(name) = relation {
                provider = Some(driver.table(name, &[], &[], size, true).await?.0);
            } else if request.schema.is_some() {
                listing.tables = driver.browse(request.schema.as_deref()).await?;
            } else {
                listing.schemas = driver.browse(None).await?;
            }
        }
        Source::Databricks { .. } => {
            let driver = crate::source::warehouse::databricks::Databricks::new(
                source,
                timeout,
                Arc::clone(pool),
                stats,
            )?;
            if let Some(name) = relation {
                provider = Some(driver.table(name, &[], &[], size, disk, true).await?.0);
            } else if request.schema.is_some() {
                listing.tables = driver.browse(request.schema.as_deref()).await?;
            } else {
                listing.schemas = driver.browse(None).await?;
            }
        }
        Source::ObjectStorage {
            storage,
            format,
            tables,
        } => {
            if let Some(prefix) = &request.object_prefix {
                use futures::TryStreamExt;
                let store = crate::source::storage::open(storage)?;
                let prefix = if prefix.is_empty() {
                    None
                } else {
                    Some(crate::source::storage::object_path(prefix)?)
                };
                let mut stream = store.list(prefix.as_ref());
                let budget = datafusion::execution::memory_pool::MemoryConsumer::new(
                    "GraphLake object listing",
                )
                .register(pool);
                while let Some(object) = stream.try_next().await? {
                    let path = object.location.to_string();
                    budget.try_grow(path.len() + 64)?;
                    listing.objects.push(path);
                    if listing.objects.len() > 2000 {
                        listing.truncated = true;
                        break;
                    }
                }
                listing.objects.sort();
                listing.objects.truncate(2000);
                return Ok(listing);
            }
            if let Some(name) = relation {
                let mapping = &tables[name];
                provider = Some(if matches!(format, crate::ObjectFormat::Parquet) {
                    Arc::new(
                        ParquetTable::open_storage(
                            &mapping.files,
                            &[],
                            Some(storage),
                            Arc::clone(pool),
                            stats,
                            false,
                        )
                        .await?
                        .0,
                    )
                } else {
                    crate::source::text::open(
                        mapping,
                        matches!(format, crate::ObjectFormat::Csv),
                        &[],
                        size,
                        Arc::clone(pool),
                        disk,
                        stats,
                        crate::source::text::ReadMode::Probe,
                        Some(storage),
                    )
                    .await?
                    .0
                });
            } else {
                listing.tables = tables.keys().cloned().collect();
            }
        }
        Source::Sqlite { path } => {
            let snapshot =
                Arc::new(crate::source::sqlite::Snapshot::open(path, timeout, stats).await?);
            if let Some(name) = relation {
                provider = Some(
                    snapshot
                        .table(name, &[], &[], None, false, false, &runtime, size, true)
                        .await?
                        .0,
                );
            } else {
                listing.tables = snapshot.browse().await?;
            }
        }
        Source::Postgres { dsn_env } => {
            let snapshot = Snapshot::open(dsn_env, app.options.timeout, stats).await?;
            if let Some(schema) = &request.schema {
                if let Some(table) = &request.table {
                    listing.columns = snapshot
                        .describe(schema, table)
                        .await?
                        .into_iter()
                        .map(|(name, data_type, nullable)| Column {
                            name,
                            data_type,
                            nullable,
                        })
                        .collect();
                } else {
                    listing.tables = snapshot.browse(Some(schema)).await?;
                }
            } else {
                listing.schemas = snapshot.browse(None).await?;
            }
        }
        Source::Parquet { tables } => {
            if let Some(table) = &request.table {
                let files = tables
                    .get(table)
                    .ok_or_else(|| anyhow::anyhow!("unknown Parquet table"))?;
                let ids: std::collections::BTreeSet<_> = files
                    .iter()
                    .filter_map(|p| crate::source::parquet_snapshot::logical_id(p))
                    .collect();
                let (snapshots, _selection) = if ids.is_empty() {
                    (Default::default(), None)
                } else {
                    let managed = super::local_tables::managed::backend(app)
                        .ok_or_else(|| anyhow::anyhow!("managed local tables are disabled"))?;
                    let selected = managed
                        .pin_model(ids, model, timeout.as_secs() + 30)
                        .await
                        .map_err(|e| anyhow::anyhow!(e.message))?;
                    (managed.snapshots(&selected.pins()?).await?, Some(selected))
                };
                let (table, _) = ParquetTable::open_with_managed(
                    files,
                    &snapshots,
                    &[],
                    Arc::clone(pool),
                    stats,
                    false,
                )
                .await?;
                provider = Some(Arc::new(table));
            } else {
                listing.tables = tables.keys().take(2001).cloned().collect();
            }
        }
        Source::Csv { tables } | Source::Jsonl { tables } => {
            if let Some(name) = relation {
                provider = Some(
                    crate::source::text::open(
                        &tables[name],
                        matches!(source, Source::Csv { .. }),
                        &[],
                        size,
                        Arc::clone(pool),
                        disk,
                        stats,
                        crate::source::text::ReadMode::Probe,
                        None,
                    )
                    .await?
                    .0,
                );
            } else {
                listing.tables = tables.keys().take(2001).cloned().collect();
            }
        }
        Source::Delta { tables } => {
            if let Some(name) = relation {
                let session = datafusion::prelude::SessionContext::new_with_config_rt(
                    datafusion::prelude::SessionConfig::new().with_batch_size(size),
                    Arc::clone(&runtime),
                );
                provider = Some(
                    crate::source::delta::open(&tables[name], &session, &stats)
                        .await?
                        .0,
                );
            } else {
                listing.tables.extend(tables.keys().take(2001).cloned());
            }
        }
        Source::Iceberg {
            uri,
            warehouse,
            token_env,
            snapshots,
        } => {
            use iceberg::{Catalog as _, NamespaceIdent};
            let client = crate::source::iceberg::catalog(
                &request.source,
                uri,
                warehouse.as_deref(),
                token_env.as_deref(),
            )
            .await?;
            if let Some(name) = relation {
                let (table, _) = crate::source::iceberg::IcebergTable::open(
                    &client,
                    name,
                    name,
                    &[],
                    &[],
                    snapshots.get(name).copied(),
                    stats,
                )
                .await?;
                provider = Some(Arc::new(table));
            } else if let Some(schema) = &request.schema {
                let namespace = NamespaceIdent::from_strs(schema.split('.'))?;
                listing.tables.extend(
                    client
                        .list_tables(&namespace)
                        .await?
                        .into_iter()
                        .take(2001)
                        .map(|table| table.name().to_owned()),
                );
            } else {
                listing.schemas.extend(
                    client
                        .list_namespaces(None)
                        .await?
                        .into_iter()
                        .take(2001)
                        .map(|name| name.as_ref().join(".")),
                );
            }
        }
        Source::Mysql { .. }
        | Source::Doris(_)
        | Source::Starrocks(_)
        | Source::Tidb(_)
        | Source::Oceanbase(_)
        | Source::Mariadb(_) => {
            let mut session =
                crate::source::mysql::Snapshot::open_source(source, timeout, stats).await?;
            if let Some(name) = relation {
                listing
                    .columns
                    .extend(session.describe(name).await?.into_iter().map(
                        |(name, data_type, nullable)| Column {
                            name,
                            data_type,
                            nullable,
                        },
                    ));
            } else if request.schema.is_some() {
                listing.tables = session.browse(request.schema.as_deref()).await?;
            } else {
                listing.schemas = session.browse(None).await?;
            }
            session.close().await?;
        }
        Source::Trino { .. }
        | Source::Mongodb(_)
        | Source::Elasticsearch(_)
        | Source::Redshift(_)
        | Source::Sqlserver(_)
        | Source::Oracle(_) => {
            let session = crate::source::warehouse::trino::Trino::new(
                source,
                timeout,
                Arc::clone(pool),
                stats,
            )?;
            if let Some(name) = relation {
                listing
                    .columns
                    .extend(
                        session
                            .describe(name)
                            .await?
                            .into_iter()
                            .map(|(name, data_type)| Column {
                                name,
                                data_type,
                                nullable: true,
                            }),
                    );
            } else if request.schema.is_some() {
                listing.tables = session.browse(request.schema.as_deref()).await?;
            } else {
                listing.schemas = session.browse(None).await?;
            }
        }
        Source::Clickhouse { .. } => {
            let mut session = crate::source::warehouse::clickhouse::ClickHouse::new(
                source,
                timeout,
                Arc::clone(pool),
                stats,
            )?;
            if let Some(name) = relation {
                provider = Some(
                    session
                        .table(name, &[], &[], size, disk, true, false)
                        .await?
                        .0,
                );
            } else {
                let context = datafusion::prelude::SessionContext::new_with_config_rt(
                    datafusion::prelude::SessionConfig::new().with_batch_size(size),
                    Arc::clone(&runtime),
                );
                let names = session
                    .browse(request.schema.as_deref(), &context, disk)
                    .await?;
                if request.schema.is_some() {
                    listing.tables = names;
                } else {
                    listing.schemas = names;
                }
            }
        }
        Source::Spanner { .. } => {
            let client = crate::source::warehouse::spanner::Client::default();
            let session = client
                .open(source, timeout, Arc::clone(pool), stats)
                .await?;
            if let Some(name) = relation {
                provider = Some(session.table(name, &[], &[], size, disk, true).await?.0);
            } else {
                listing.tables = session.browse().await?;
            }
        }
        Source::SapHana { .. } => {
            if let Some(namespace) = &request.schema {
                let session = crate::source::warehouse::hana::Hana::open(
                    source,
                    timeout,
                    size,
                    Arc::clone(pool),
                    stats,
                )
                .await?;
                if let Some(name) = relation {
                    provider = Some(session.table(name, &[], &[], size, disk, true).await?.0);
                } else {
                    listing.tables = session.browse(namespace).await?;
                }
            } else {
                listing.schemas = mapped_namespaces(catalog, &request.source);
            }
        }
        Source::Snowflake { .. } | Source::Bigquery { .. } => {
            if let Some(namespace) = &request.schema {
                let session = crate::source::warehouse::Snapshot::open(
                    source,
                    timeout,
                    Arc::clone(pool),
                    stats,
                )
                .await?;
                if let Some(name) = relation {
                    provider = Some(session.table(name, &[], &[], size, disk, true).await?.0);
                } else {
                    listing.tables = session.browse(namespace).await?;
                }
            } else {
                listing.schemas = mapped_namespaces(catalog, &request.source);
            }
        }
    }
    if let Some(provider) = provider {
        listing
            .columns
            .extend(provider.schema().fields().iter().map(|field| Column {
                name: field.name().clone(),
                data_type: field.data_type().to_string(),
                nullable: field.is_nullable(),
            }));
    }
    listing.truncated = listing.schemas.len() > 2000 || listing.tables.len() > 2000;
    listing.schemas.truncate(2000);
    listing.tables.truncate(2000);
    Ok(listing)
}

fn mapped_namespaces(catalog: &Catalog, source: &str) -> Vec<String> {
    use std::collections::BTreeSet;
    catalog
        .nodes
        .values()
        .flat_map(|node| {
            std::iter::once(node.source.as_str())
                .chain(node.inputs.iter().map(|input| input.table.as_str()))
        })
        .chain(catalog.edges.values().flat_map(|edge| {
            std::iter::once(edge.source.as_str())
                .chain(edge.inputs.iter().map(|input| input.table.as_str()))
        }))
        .filter_map(|reference| {
            let (alias, relation) = reference.split_once('.')?;
            let (namespace, _) = relation.rsplit_once('.')?;
            (alias == source).then_some(namespace.to_owned())
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .take(2001)
        .collect()
}
