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
}

#[derive(Default, Serialize)]
pub(super) struct Listing {
    schemas: Vec<String>,
    tables: Vec<String>,
    columns: Vec<Column>,
    truncated: bool,
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
    let result = tokio::time::timeout(app.options.timeout, read(&catalog, source, &request, &app))
        .await
        .map_err(|_| {
            Error::new(
                axum::http::StatusCode::GATEWAY_TIMEOUT,
                "timeout",
                "读取表结构超时",
            )
        })?
        .map_err(|_| {
            Error::invalid(
                "browse_failed",
                "无法读取表结构，请检查数据源配置、权限和表名",
            )
        })?;
    Ok(Json(result))
}

async fn read(
    catalog: &Catalog,
    source: &Source,
    request: &Request,
    app: &App,
) -> anyhow::Result<Listing> {
    let mut listing = Listing::default();
    let stats = Arc::new(ReadStats::default());
    let timeout = app.options.timeout;
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(app.options.memory_bytes, 1.0)
        .build()?;
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
                let (table, _) =
                    ParquetTable::open(files, &[], Arc::clone(pool), stats, false).await?;
                provider = Some(Arc::new(table));
            } else {
                listing.tables = tables.keys().take(2001).cloned().collect();
            }
        }
        Source::Delta { tables } => {
            if let Some(name) = relation {
                let session = datafusion::prelude::SessionContext::new();
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
        Source::Mysql {
            dsn_env,
            tls,
            ca_file,
        } => {
            let mut session = crate::source::mysql::Snapshot::open(
                dsn_env,
                *tls,
                ca_file.as_deref(),
                timeout,
                stats,
            )
            .await?;
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
        | Source::Redshift(_) => {
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
                provider = Some(session.table(name, &[], &[], size, disk, true).await?.0);
            } else {
                let context = datafusion::prelude::SessionContext::new();
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
                    provider = Some(session.table(name, &[], size, disk, true).await?.0);
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
