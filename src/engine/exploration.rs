use std::{collections::BTreeMap, sync::Arc};

use anyhow::Result;
use futures::{StreamExt, TryStreamExt};

#[cfg(feature = "admin")]
use crate::source::{iceberg, postgres::Snapshot};
use crate::{Source, SourceVersion, source::parquet};

type Stores = BTreeMap<String, Arc<dyn object_store::ObjectStore>>;

use super::{Engine, QueryScope};

impl Engine {
    pub(crate) async fn pinned_versions_match(
        &self,
        scope: &QueryScope,
        expected: &[SourceVersion],
    ) -> Result<bool> {
        if expected.is_empty() || expected.iter().any(|v| v.consistency != "pinned object") {
            return Ok(false);
        }
        let mut stores = Stores::new();
        let reads = expected.iter().map(|source| {
            let read = self.pinned_version(scope, &source.source, &mut stores);
            async move { Ok::<_, anyhow::Error>(read?.await?.as_ref() == Some(&source.version)) }
        });
        let mut checks = futures::stream::iter(reads).buffer_unordered(8);
        while let Some(matches) = checks.try_next().await? {
            if !matches {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn pinned_version(
        &self,
        scope: &QueryScope,
        name: &str,
        stores: &mut Stores,
    ) -> Result<impl std::future::Future<Output = Result<Option<String>>> + use<>> {
        let logical = crate::source::parquet_snapshot::logical_id(name);
        let managed = logical
            .and_then(|id| self.managed_parquet.get(&id))
            .map(|current| current.generation.to_string());
        let configured = self.catalog.sources.values().find_map(|source| {
            let Source::ObjectStorage {
                storage, tables, ..
            } = source
            else {
                return None;
            };
            let prefix = crate::source::storage::version_prefix(storage);
            let path = name.strip_prefix(&prefix)?;
            tables
                .values()
                .any(|table| table.files.iter().any(|file| file == path))
                .then_some((prefix, path, storage))
        });
        let (location, store) = match configured {
            Some((prefix, path, storage)) => {
                let store = match stores.entry(prefix) {
                    std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(crate::source::storage::open(storage)?)
                    }
                };
                (path, Some(Arc::clone(store)))
            }
            None => (name, None),
        };
        let location = location.to_owned();
        let pool = Arc::clone(&scope.runtime.memory_pool);
        let stats = Arc::clone(&scope.reads);
        Ok(async move {
            if logical.is_some() {
                return Ok(managed);
            }
            Ok(Some(
                parquet::object_version(&location, store.as_ref(), pool, stats)
                    .await?
                    .version,
            ))
        })
    }

    #[cfg(feature = "admin")]
    pub(crate) async fn exploration_versions_match(
        &self,
        scope: &QueryScope,
        expected: &[crate::SourceVersion],
        validated: &mut BTreeMap<String, String>,
    ) -> Result<bool> {
        let mut postgres = BTreeMap::new();
        let mut stores = BTreeMap::new();
        for source in expected {
            let name = &source.source;
            let current = match source.consistency {
                "PostgreSQL relation content" => {
                    let (catalog, table, Source::Postgres { dsn_env }) =
                        self.catalog.source(name)?
                    else {
                        return Ok(false);
                    };
                    let snapshot = match postgres.entry(catalog) {
                        std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                        std::collections::btree_map::Entry::Vacant(entry) => entry.insert(
                            Snapshot::open(
                                dsn_env,
                                scope.options.timeout,
                                Arc::clone(&scope.reads),
                            )
                            .await?,
                        ),
                    };
                    // A view, foreign partition or RLS policy can read data
                    // outside the transaction visibility recorded in the token.
                    let current = snapshot.exploration_snapshot.as_ref();
                    if current.is_some()
                        && current == validated.get(name)
                        && snapshot.relation_reuses_snapshot(table).await?
                    {
                        continue;
                    }
                    let version = snapshot.relation_version(table).await?;
                    if let Some(current) = current {
                        validated.insert(name.clone(), current.clone());
                    } else {
                        validated.remove(name);
                    }
                    version
                }
                "pinned object" => match self.pinned_version(scope, name, &mut stores)?.await? {
                    Some(version) => version,
                    None => return Ok(false),
                },
                "Delta version" => {
                    let (_, table, Source::Delta { tables }) = self.catalog.source(name)? else {
                        return Ok(false);
                    };
                    crate::source::delta::open(&tables[table], &scope.session(), &scope.reads)
                        .await?
                        .1
                        .to_string()
                }
                "Iceberg snapshot" => {
                    let (
                        catalog,
                        table,
                        Source::Iceberg {
                            uri,
                            warehouse,
                            token_env,
                            snapshots,
                        },
                    ) = self.catalog.source(name)?
                    else {
                        return Ok(false);
                    };
                    let client =
                        iceberg::catalog(catalog, uri, warehouse.as_deref(), token_env.as_deref())
                            .await?;
                    iceberg::IcebergTable::open(
                        &client,
                        name,
                        table,
                        &[],
                        &[],
                        snapshots.get(table).copied(),
                        Arc::clone(&scope.reads),
                    )
                    .await?
                    .1
                    .version
                }
                _ => continue,
            };
            if current != source.version {
                return Ok(false);
            }
        }
        Ok(true)
    }
}
