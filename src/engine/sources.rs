use super::*;

pub(super) async fn prepare<'a, 'q>(
    catalog: &'a Catalog,
    spanner: &BTreeMap<String, source::warehouse::spanner::Client>,
    managed: &BTreeMap<uuid::Uuid, source::parquet_snapshot::RemoteSnapshot>,
    scope: &QueryScope,
    required: BTreeMap<&'a str, BTreeSet<String>>,
    filters: &BTreeMap<&str, Vec<crate::Predicate>>,
    read: model::ReadKind<'q>,
) -> Result<(
    SessionContext,
    BTreeMap<&'a str, Raw>,
    Prepared,
    Option<&'q str>,
)> {
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
    let mut direct = BTreeMap::new();
    let mut aggregate = None;
    let mut proven_key_label = None;
    let mut databricks = BTreeMap::new();
    let mut sqlite_snapshots = BTreeMap::new();
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
    let duckdb_sources = required
        .keys()
        .filter_map(|reference| match catalog.source(reference).ok()? {
            (name, _, Source::Duckdb { .. }) => Some(name),
            _ => None,
        })
        .collect::<BTreeSet<_>>()
        .len();
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
        let native = read
            .sql
            .and_then(|sql| sql.native.as_ref())
            .filter(|(label, _)| catalog.nodes[label].source == reference);
        let single = read
            .sql
            .and_then(|sql| sql.single.as_deref())
            .filter(|label| catalog.nodes[*label].source == reference);
        let stream =
            read.stream_raw || single.is_some() && scope.options.graph.logical_partition.is_empty();
        let scan = sql_read::Pushdown {
            operation: sql_read::Native::Filter,
            filters: Vec::new(),
        };
        let proof = native
            .map(|(label, plan)| (label.as_str(), plan))
            .or_else(|| single.filter(|_| stream).map(|label| (label, &scan)));
        #[cfg(feature = "admin")]
        let watermark = read.watermark;
        #[cfg(not(feature = "admin"))]
        let watermark = None;
        let mut parquet_versions = None;
        let table: Arc<dyn TableProvider> = match source {
            Source::Duckdb { .. }
            | Source::SqlserverNative { .. }
            | Source::OracleNative { .. }
            | Source::MongodbNative { .. } => {
                let driver = match direct.entry(name) {
                    std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                    std::collections::btree_map::Entry::Vacant(entry) => entry.insert(
                        Box::pin(source::direct::Driver::open(
                            source,
                            scope.options.timeout,
                            Arc::clone(&scope.runtime),
                            Arc::clone(&scope.reads),
                            duckdb_sources,
                        ))
                        .await?,
                    ),
                };
                let native = read
                    .sql
                    .and_then(|sql| sql.native.as_ref())
                    .filter(|(label, _)| {
                        matches!(source, Source::Duckdb { .. })
                            && catalog.nodes[label].source == reference
                    });
                let pushed = if let Some((label, operation)) = native {
                    driver
                        .pushdown(
                            relation,
                            &fields,
                            &catalog.nodes[label].key,
                            operation,
                            scope.options.batch_size,
                        )
                        .await?
                } else {
                    None
                };
                let (table, budget) = if let Some(pushed) = pushed {
                    proven_key_label = native.map(|(label, _)| label.as_str());
                    aggregate = pushed.aggregate;
                    (pushed.table, pushed.budget)
                } else {
                    driver
                        .table(
                            relation,
                            &fields,
                            predicates,
                            scope.options.batch_size,
                            read.explain_only,
                        )
                        .await?
                };
                let (key, consistency) = match source {
                    Source::Duckdb { .. } => (name, "DuckDB read transaction"),
                    Source::OracleNative { .. } => (name, "Oracle read-only transaction"),
                    Source::SqlserverNative { .. } => (reference, "SQL Server statement"),
                    _ => (reference, "MongoDB cursor"),
                };
                versions.entry(key.into()).or_insert_with(|| SourceVersion {
                    source: key.into(),
                    version: uuid::Uuid::new_v4().to_string(),
                    consistency,
                });
                reservations.push(budget);
                table
            }
            Source::Databricks { .. } => {
                let driver = match databricks.entry(name) {
                    std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(source::warehouse::databricks::Databricks::new(
                            source,
                            scope.options.timeout,
                            Arc::clone(&scope.runtime.memory_pool),
                            Arc::clone(&scope.reads),
                        )?)
                    }
                };
                let (table, budget) = driver
                    .table(
                        relation,
                        &fields,
                        predicates,
                        scope.options.batch_size,
                        &scope.runtime.disk_manager,
                        read.explain_only,
                    )
                    .await?;
                versions.insert(
                    reference.into(),
                    SourceVersion {
                        source: reference.into(),
                        version: uuid::Uuid::new_v4().to_string(),
                        consistency: "Databricks statement",
                    },
                );
                reservations.push(budget);
                table
            }
            Source::ObjectStorage {
                storage,
                format,
                tables,
            } => {
                let mapping = &tables[relation];
                let (table, objects): (Arc<dyn TableProvider>, _) =
                    if matches!(format, crate::ObjectFormat::Parquet) {
                        let (table, objects) = ParquetTable::open_storage(
                            &mapping.files,
                            &fields,
                            Some(storage),
                            Arc::clone(&scope.runtime.memory_pool),
                            Arc::clone(&scope.reads),
                            false,
                        )
                        .await?;
                        (Arc::new(table), objects)
                    } else {
                        let (table, budget, objects) = source::text::open(
                            mapping,
                            matches!(format, crate::ObjectFormat::Csv),
                            &fields,
                            scope.options.batch_size,
                            Arc::clone(&scope.runtime.memory_pool),
                            &scope.runtime.disk_manager,
                            Arc::clone(&scope.reads),
                            read.explain_only,
                            Some(storage),
                        )
                        .await?;
                        reservations.push(budget);
                        (table, objects)
                    };
                for mut object in objects {
                    object.source = format!(
                        "{}:{}:{}:{}/{}",
                        storage.provider,
                        storage.endpoint.as_deref().unwrap_or("default"),
                        storage.account.as_deref().unwrap_or("default"),
                        storage.bucket,
                        object.source
                    );
                    if let Some(previous) = versions.get(&object.source) {
                        ensure!(
                            previous.version == object.version,
                            "storage object changed while preparing query"
                        );
                    }
                    versions.insert(object.source.clone(), object);
                }
                table
            }
            Source::Sqlite { path } => {
                let snapshot = match sqlite_snapshots.entry(name) {
                    std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        let snapshot = Arc::new(
                            source::sqlite::Snapshot::open(
                                path,
                                scope.options.timeout,
                                Arc::clone(&scope.reads),
                            )
                            .await?,
                        );
                        versions.insert(
                            name.into(),
                            SourceVersion {
                                source: name.into(),
                                version: snapshot.version.clone(),
                                consistency: "SQLite read transaction",
                            },
                        );
                        entry.insert(snapshot)
                    }
                };
                let (table, budget, pushed) = snapshot
                    .table(
                        relation,
                        &fields,
                        predicates,
                        proof.map(|(label, plan)| (catalog.nodes[label].key.as_slice(), plan)),
                        stream,
                        read.stream_raw,
                        &scope.runtime,
                        scope.options.batch_size,
                        read.explain_only,
                    )
                    .await?;
                if pushed {
                    proven_key_label = proof.map(|(label, _)| label);
                }
                reservations.push(budget);
                table
            }
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
                let mut table = snapshot
                    .table(relation, &fields)
                    .await
                    .with_context(|| crate::catalog::MappedTable(reference.to_owned()))?;
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
                if let Some((label, operation)) = read.sql.and_then(|sql| sql.native.as_ref())
                    && catalog.nodes[label].source == reference
                {
                    aggregate = table.pushdown(&catalog.nodes[label].key, operation).await?;
                }
                #[cfg(feature = "admin")]
                if let Some((column, value)) = watermark {
                    table = table.watermark(column, value);
                }
                let table = Arc::new(table);
                postgres.insert(reference.to_owned(), Arc::clone(&table));
                table
            }
            Source::Mysql { .. }
            | Source::Doris(_)
            | Source::Starrocks(_)
            | Source::Tidb(_)
            | Source::Oceanbase(_)
            | Source::Mariadb(_) => {
                let snapshot = match mysql_snapshots.entry(name) {
                    std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        let snapshot = source::mysql::Snapshot::open_source(
                            source,
                            scope.options.timeout,
                            Arc::clone(&scope.reads),
                        )
                        .await
                        .with_context(|| format!("opening MySQL source {name}"))?;
                        if matches!(source, Source::Mysql { .. } | Source::Mariadb(_)) {
                            versions.insert(
                                name.into(),
                                SourceVersion {
                                    source: name.into(),
                                    version: snapshot.version.clone(),
                                    consistency: "MySQL InnoDB repeatable read",
                                },
                            );
                        }
                        entry.insert(snapshot)
                    }
                };
                let (table, budget, pushed) = snapshot
                    .table(
                        relation,
                        &fields,
                        predicates,
                        proof.map(|(label, plan)| (catalog.nodes[label].key.as_slice(), plan)),
                        watermark,
                        stream,
                        read.stream_raw,
                        &scope.runtime,
                        scope.options.batch_size,
                        read.explain_only,
                    )
                    .await?;
                if pushed {
                    proven_key_label = proof.map(|(label, _)| label);
                }
                if !matches!(source, Source::Mysql { .. } | Source::Mariadb(_)) {
                    versions.insert(
                        reference.into(),
                        SourceVersion {
                            source: reference.into(),
                            version: uuid::Uuid::new_v4().to_string(),
                            consistency: "MySQL-compatible statement",
                        },
                    );
                }
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
                        predicates,
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
                        read.stream_raw || stream && read.lazy_validation,
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
            | Source::Redshift(_)
            | Source::Sqlserver(_)
            | Source::Oracle(_) => {
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
                    predicates,
                    snapshots.get(relation).copied(),
                    Arc::clone(&scope.reads),
                )
                .await?;
                versions.insert(reference.into(), version);
                Arc::new(table)
            }
            Source::Csv { tables } | Source::Jsonl { tables } => {
                let (table, budget, objects) = source::text::open(
                    &tables[relation],
                    matches!(source, Source::Csv { .. }),
                    &fields,
                    scope.options.batch_size,
                    Arc::clone(&scope.runtime.memory_pool),
                    &scope.runtime.disk_manager,
                    Arc::clone(&scope.reads),
                    read.explain_only,
                    None,
                )
                .await
                .with_context(|| crate::catalog::MappedTable(reference.to_owned()))?;
                for object in objects {
                    if let Some(previous) = versions.get(&object.source) {
                        ensure!(
                            previous.version == object.version,
                            "text object changed while preparing query: {}",
                            object.source
                        );
                    }
                    versions.insert(object.source.clone(), object);
                }
                reservations.push(budget);
                table
            }
            Source::Parquet { tables } => {
                #[cfg(feature = "admin")]
                let prune_snapshot = read.probe_source.is_none();
                #[cfg(not(feature = "admin"))]
                let prune_snapshot = true;
                let (table, objects) = ParquetTable::open_with_managed(
                    &tables[relation],
                    managed,
                    &fields,
                    Arc::clone(&scope.runtime.memory_pool),
                    Arc::clone(&scope.reads),
                    prune_snapshot,
                )
                .await
                .with_context(|| crate::catalog::MappedTable(reference.to_owned()))?;
                #[cfg(feature = "admin")]
                let table = if read.exploration.is_some() {
                    table.for_exploration()
                } else {
                    table
                };
                parquet_versions = Some(
                    objects
                        .iter()
                        .map(|object| (object.source.clone(), object.version.clone()))
                        .collect(),
                );
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
        tables.insert(
            reference,
            Raw {
                alias,
                schema,
                parquet_versions,
            },
        );
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
            aggregate,
        },
        proven_key_label,
    ))
}

#[cfg(feature = "admin")]
pub(crate) async fn table(
    catalog: &Catalog,
    reference: &str,
    columns: &[String],
    predicates: Vec<crate::Predicate>,
    watermark: Option<(&str, &datafusion::common::ScalarValue)>,
    scope: &QueryScope,
) -> Result<(DataFrame, Prepared)> {
    let spanner = catalog
        .sources
        .iter()
        .filter(|(_, source)| matches!(source, Source::Spanner { .. }))
        .map(|(name, _)| (name.clone(), Default::default()))
        .collect();
    let (context, _, prepared, _) = prepare(
        catalog,
        &spanner,
        &BTreeMap::new(),
        scope,
        BTreeMap::from([(reference, columns.iter().cloned().collect())]),
        &BTreeMap::from([(reference, predicates)]),
        model::ReadKind {
            watermark,
            stream_raw: true,
            ..Default::default()
        },
    )
    .await?;
    Ok((context.table("raw_0").await?, prepared))
}
