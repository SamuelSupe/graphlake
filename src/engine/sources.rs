use super::*;

pub(super) async fn prepare<'a>(
    catalog: &'a Catalog,
    spanner: &BTreeMap<String, source::warehouse::spanner::Client>,
    scope: &QueryScope,
    required: BTreeMap<&'a str, BTreeSet<String>>,
    filters: &BTreeMap<&str, Vec<crate::Predicate>>,
    read: model::ReadKind<'_>,
) -> Result<(SessionContext, BTreeMap<&'a str, Raw>, Prepared)> {
    #[cfg(feature = "admin")]
    let private = if read.distributed.is_some() {
        let mut state = scope.session().state();
        crate::admin::cluster::distributed::Distributed::configure(
            &mut state,
            scope.options.memory_bytes,
        );
        SessionContext::new_with_state(state)
    } else if read.exploration.is_some() {
        scope.exploration_session()
    } else {
        scope.session()
    };
    #[cfg(not(feature = "admin"))]
    let private = scope.session();
    let mut snapshots = BTreeMap::<&str, Arc<Snapshot>>::new();
    let mut mysql_snapshots = BTreeMap::new();
    let mut spanner_snapshots = BTreeMap::new();
    let mut hana = BTreeMap::new();
    let mut clickhouse = BTreeMap::new();
    let mut trino = BTreeMap::new();
    let mut warehouse_snapshots = BTreeMap::new();
    let mut reservations = Vec::new();
    let mut iceberg_catalogs = BTreeMap::new();
    let mut deltas = BTreeMap::new();
    let mut versions = BTreeMap::<String, SourceVersion>::new();
    let mut tables = BTreeMap::new();
    let mut postgres = BTreeMap::new();
    for (reference, fields) in required {
        let (name, relation, source) = catalog.source(reference)?;
        #[cfg(feature = "admin")]
        if read.probe_source.is_some_and(|probe| probe != name) {
            continue;
        }
        #[cfg(feature = "admin")]
        if read.transaction.is_some() {
            ensure!(
                matches!(source, Source::Postgres { .. } | Source::Parquet { .. }),
                "Bolt read transactions currently support PostgreSQL and Parquet sources"
            );
        }
        let fields = fields.into_iter().collect::<Vec<_>>();
        let predicates = &filters[reference];
        let table: Arc<dyn TableProvider> = match source {
            Source::Postgres { dsn_env } => {
                let snapshot = match snapshots.entry(name) {
                    std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        #[cfg(feature = "admin")]
                        let pinned = if let Some(transaction) = read.transaction {
                            Some(transaction.postgres(name, dsn_env, scope).await?)
                        } else {
                            None
                        };
                        #[cfg(not(feature = "admin"))]
                        let pinned = None;
                        let snapshot = match pinned {
                            Some(snapshot) => snapshot,
                            None => Arc::new(
                                Snapshot::open(dsn_env, scope.options.timeout, scope.reads.clone())
                                    .await
                                    .with_context(|| format!("opening PostgreSQL source {name}"))?,
                            ),
                        };
                        versions.insert(
                            name.into(),
                            SourceVersion {
                                source: name.into(),
                                version: snapshot.version.clone(),
                                consistency: "PostgreSQL repeatable read",
                            },
                        );
                        entry.insert(snapshot)
                    }
                };
                let table = Arc::new(
                    snapshot
                        .table(relation, &fields)
                        .await
                        .with_context(|| crate::catalog::MappedTable(reference.to_owned()))?,
                );
                #[cfg(feature = "admin")]
                if read.probe_source.is_some() {
                    table.probe().await?;
                    continue;
                }
                #[cfg(feature = "admin")]
                if read.graph_versions {
                    versions.insert(
                        reference.into(),
                        SourceVersion {
                            source: reference.into(),
                            version: snapshot.relation_version(relation).await?,
                            consistency: "PostgreSQL relation content",
                        },
                    );
                }
                if scope.options.graph.require_consistent_snapshot {
                    table.verify_local_snapshot().await?;
                }
                postgres.insert(reference.to_owned(), Arc::clone(&table));
                table
            }
            Source::Mysql {
                dsn_env,
                tls,
                ca_file,
            } => {
                let snapshot = match mysql_snapshots.entry(name) {
                    std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        let snapshot = source::mysql::Snapshot::open(
                            dsn_env,
                            *tls,
                            ca_file.as_deref(),
                            scope.options.timeout,
                            Arc::clone(&scope.reads),
                        )
                        .await
                        .with_context(|| format!("opening MySQL source {name}"))?;
                        versions.insert(
                            name.into(),
                            SourceVersion {
                                source: name.into(),
                                version: snapshot.version.clone(),
                                consistency: "MySQL InnoDB repeatable read",
                            },
                        );
                        entry.insert(snapshot)
                    }
                };
                let (table, budget) = snapshot
                    .table(
                        relation,
                        &fields,
                        predicates,
                        &scope.runtime,
                        scope.options.batch_size,
                        read.explain_only,
                    )
                    .await?;
                reservations.push(budget);
                table
            }
            Source::Snowflake { .. } | Source::Bigquery { .. } => {
                let snapshot = match warehouse_snapshots.entry(name) {
                    std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        let snapshot = source::warehouse::Snapshot::open(
                            source,
                            scope.options.timeout,
                            Arc::clone(&scope.runtime.memory_pool),
                            Arc::clone(&scope.reads),
                        )
                        .await
                        .with_context(|| format!("opening warehouse source {name}"))?;
                        versions.insert(
                            name.into(),
                            SourceVersion {
                                source: name.into(),
                                version: snapshot.version.clone(),
                                consistency: snapshot.consistency,
                            },
                        );
                        entry.insert(snapshot)
                    }
                };
                let (table, budget) = snapshot
                    .table(
                        relation,
                        &fields,
                        scope.options.batch_size,
                        &scope.runtime.disk_manager,
                        read.explain_only,
                    )
                    .await
                    .with_context(|| format!("reading warehouse table {reference}"))?;
                reservations.push(budget);
                table
            }
            Source::Clickhouse { .. } => {
                let driver = match clickhouse.entry(name) {
                    std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(source::warehouse::clickhouse::ClickHouse::new(
                            source,
                            scope.options.timeout,
                            Arc::clone(&scope.runtime.memory_pool),
                            Arc::clone(&scope.reads),
                        )?)
                    }
                };
                let (table, budget, version) = driver
                    .table(
                        relation,
                        &fields,
                        predicates,
                        scope.options.batch_size,
                        &scope.runtime.disk_manager,
                        read.explain_only,
                    )
                    .await
                    .with_context(|| format!("reading ClickHouse table {reference}"))?;
                versions.insert(
                    reference.into(),
                    SourceVersion {
                        source: reference.into(),
                        version,
                        consistency: "ClickHouse statement",
                    },
                );
                reservations.push(budget);
                table
            }
            Source::Trino { .. }
            | Source::Mongodb(_)
            | Source::Elasticsearch(_)
            | Source::Redshift(_) => {
                let driver = match trino.entry(name) {
                    std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(source::warehouse::trino::Trino::new(
                            source,
                            scope.options.timeout,
                            Arc::clone(&scope.runtime.memory_pool),
                            Arc::clone(&scope.reads),
                        )?)
                    }
                };
                let (table, budget, version) = driver
                    .table(
                        relation,
                        &fields,
                        predicates,
                        scope.options.batch_size,
                        &scope.runtime.disk_manager,
                        read.explain_only,
                    )
                    .await
                    .with_context(|| format!("reading Trino table {reference}"))?;
                versions.insert(
                    reference.into(),
                    SourceVersion {
                        source: reference.into(),
                        version,
                        consistency: "Trino statement",
                    },
                );
                reservations.push(budget);
                table
            }
            Source::Spanner { .. } => {
                let snapshot = match spanner_snapshots.entry(name) {
                    std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        // Keep SDK futures out of the shared query preparation stack frame.
                        let snapshot = Box::pin(spanner[name].open(
                            source,
                            scope.options.timeout,
                            Arc::clone(&scope.runtime.memory_pool),
                            Arc::clone(&scope.reads),
                        ))
                        .await
                        .with_context(|| format!("opening Spanner source {name}"))?;
                        versions.insert(
                            name.into(),
                            SourceVersion {
                                source: name.into(),
                                version: snapshot.version.clone(),
                                consistency: "Spanner read-only transaction",
                            },
                        );
                        entry.insert(snapshot)
                    }
                };
                let (table, budget) = Box::pin(snapshot.table(
                    relation,
                    &fields,
                    predicates,
                    scope.options.batch_size,
                    &scope.runtime.disk_manager,
                    read.explain_only,
                ))
                .await
                .with_context(|| format!("reading Spanner table {reference}"))?;
                reservations.push(budget);
                table
            }
            Source::SapHana { .. } => {
                let driver = match hana.entry(name) {
                    std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                    std::collections::btree_map::Entry::Vacant(entry) => entry.insert(
                        Box::pin(source::warehouse::hana::Hana::open(
                            source,
                            scope.options.timeout,
                            scope.options.batch_size,
                            Arc::clone(&scope.runtime.memory_pool),
                            Arc::clone(&scope.reads),
                        ))
                        .await
                        .with_context(|| format!("opening SAP HANA source {name}"))?,
                    ),
                };
                let (table, budget, version) = Box::pin(driver.table(
                    relation,
                    &fields,
                    predicates,
                    scope.options.batch_size,
                    &scope.runtime.disk_manager,
                    read.explain_only,
                ))
                .await
                .with_context(|| format!("reading SAP HANA table {reference}"))?;
                versions.insert(
                    reference.into(),
                    SourceVersion {
                        source: reference.into(),
                        version,
                        consistency: "SAP HANA statement",
                    },
                );
                reservations.push(budget);
                table
            }
            Source::Delta { tables } => {
                let mapping = &tables[relation];
                let (table, version) = match deltas.entry((&mapping.path, mapping.version)) {
                    std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                    std::collections::btree_map::Entry::Vacant(entry) => entry.insert(
                        source::delta::open(mapping, &private, &scope.reads)
                            .await
                            .with_context(|| format!("opening Delta table {reference}"))?,
                    ),
                };
                versions.insert(
                    reference.into(),
                    SourceVersion {
                        source: reference.into(),
                        version: version.to_string(),
                        consistency: "Delta version",
                    },
                );
                Arc::clone(table)
            }
            Source::Iceberg {
                uri,
                warehouse,
                token_env,
                snapshots,
            } => {
                let catalog = match iceberg_catalogs.entry(name) {
                    std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                    std::collections::btree_map::Entry::Vacant(entry) => entry.insert(
                        source::iceberg::catalog(
                            name,
                            uri,
                            warehouse.as_deref(),
                            token_env.as_deref(),
                        )
                        .await?,
                    ),
                };
                let (table, version) = source::iceberg::IcebergTable::open(
                    catalog,
                    reference,
                    relation,
                    &fields,
                    snapshots.get(relation).copied(),
                    Arc::clone(&scope.reads),
                )
                .await?;
                versions.insert(reference.into(), version);
                Arc::new(table)
            }
            Source::Parquet { tables } => {
                let (table, objects) = ParquetTable::open(
                    &tables[relation],
                    &fields,
                    Arc::clone(&scope.runtime.memory_pool),
                    Arc::clone(&scope.reads),
                    read.probe_source.is_none(),
                )
                .await
                .with_context(|| crate::catalog::MappedTable(reference.to_owned()))?;
                #[cfg(feature = "admin")]
                let table = if read.exploration.is_some() {
                    table.for_exploration()
                } else {
                    table
                };
                for object in objects {
                    #[cfg(feature = "admin")]
                    if let Some(transaction) = read.transaction {
                        transaction.check_parquet(&object)?;
                    }
                    if let Some(previous) = versions.get(&object.source) {
                        ensure!(
                            previous.version == object.version,
                            "Parquet object changed while preparing query: {}",
                            object.source
                        );
                    }
                    versions.insert(object.source.clone(), object);
                }
                Arc::new(table)
            }
        };
        #[cfg(feature = "admin")]
        if read.probe_source.is_some() {
            continue;
        }
        let alias = format!("raw_{}", tables.len());
        let schema = table.schema();
        private.register_table(TableReference::bare(alias.as_str()), table)?;
        tables.insert(reference, Raw { alias, schema });
    }
    for snapshot in mysql_snapshots.into_values() {
        snapshot.close().await?;
    }
    Ok((
        private,
        tables,
        Prepared {
            versions: versions.into_values().collect(),
            reservations,
            postgres,
        },
    ))
}

#[cfg(feature = "admin")]
pub(crate) async fn table(
    catalog: &Catalog,
    reference: &str,
    columns: &[String],
    predicates: Vec<crate::Predicate>,
    scope: &QueryScope,
) -> Result<(DataFrame, Prepared)> {
    let spanner = catalog
        .sources
        .iter()
        .filter(|(_, source)| matches!(source, Source::Spanner { .. }))
        .map(|(name, _)| (name.clone(), Default::default()))
        .collect();
    let (context, _, prepared) = prepare(
        catalog,
        &spanner,
        scope,
        BTreeMap::from([(reference, columns.iter().cloned().collect())]),
        &BTreeMap::from([(reference, predicates)]),
        model::ReadKind::default(),
    )
    .await?;
    Ok((context.table("raw_0").await?, prepared))
}
