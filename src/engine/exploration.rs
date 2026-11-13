use std::{collections::BTreeMap, sync::Arc};

use anyhow::Result;

use crate::{
    Source,
    source::{iceberg, parquet, postgres::Snapshot},
};

use super::{Engine, QueryScope};

impl Engine {
    pub(crate) async fn exploration_versions_match(
        &self,
        scope: &QueryScope,
        expected: &[crate::SourceVersion],
        validated: &mut BTreeMap<String, String>,
    ) -> Result<bool> {
        let mut postgres = BTreeMap::new();
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
                "pinned object" => {
                    parquet::object_version(
                        name,
                        Arc::clone(&scope.runtime.memory_pool),
                        Arc::clone(&scope.reads),
                    )
                    .await?
                    .version
                }
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
