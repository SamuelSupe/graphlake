#[cfg(feature = "admin")]
mod exploration;
mod export;
#[cfg(feature = "admin")]
pub(crate) use model::{Exploration, exploration_seek_type};
mod model;
mod query;
mod query_functions;
#[cfg(feature = "admin")]
pub(crate) mod query_graph;
pub(crate) mod read_filters;

pub use query::{EngineLimits, QueryMetrics, QueryOptions, QueryResult};
pub(crate) use query::{QueryScope, graph_frame};

#[cfg(feature = "admin")]
pub(crate) fn variant_fields(fields: &crate::arrow::datatypes::Fields) -> bool {
    query_functions::cypher::is_variant(fields)
}

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, OnceLock},
};

use anyhow::{Context, Result, ensure};
use datafusion::{
    arrow::{
        array::{RecordBatch, UInt64Array},
        datatypes::{DataType, Field, Schema, SchemaRef},
    },
    catalog::TableProvider,
    common::TableReference,
    execution::{context::SQLOptions, memory_pool::MemoryReservation},
    prelude::{DataFrame, SessionContext},
    sql::{
        parser::Statement,
        sqlparser::ast::{
            Expr as SqlExpr, LimitClause, Statement as SqlStatement, Value as SqlValue,
        },
    },
};
use futures::TryStreamExt;

use crate::{
    Catalog, Source, SourceVersion,
    catalog::{ROW_ID, ident, qualified},
    cypher,
    source::{
        self,
        parquet::ParquetTable,
        postgres::{PgTable, Snapshot},
    },
};

#[derive(Debug, Clone, Copy)]
pub enum Language {
    Cypher,
    Sql,
    Gremlin,
}

#[derive(Clone)]
pub struct Engine {
    pub(crate) catalog: Arc<Catalog>,
    pub(crate) cache: Option<crate::traversal::Cache>,
    admission: Arc<query::Admission>,
    spanner: Arc<BTreeMap<String, source::warehouse::spanner::Client>>,
}

impl Engine {
    pub fn new(catalog: Catalog) -> Result<Self> {
        static DEFAULT_ADMISSION: OnceLock<Arc<query::Admission>> = OnceLock::new();
        let admission = DEFAULT_ADMISSION.get_or_init(|| {
            Arc::new(query::Admission::new(EngineLimits::default()).expect("default engine limits"))
        });
        Self::with_admission(catalog, Arc::clone(admission))
    }

    /// Create an engine with a dedicated resource budget shared by its clones.
    pub fn new_with_limits(catalog: Catalog, limits: EngineLimits) -> Result<Self> {
        Self::with_admission(catalog, Arc::new(query::Admission::new(limits)?))
    }

    fn with_admission(catalog: Catalog, admission: Arc<query::Admission>) -> Result<Self> {
        catalog.validate()?;
        let spanner = catalog
            .sources
            .iter()
            .filter(|(_, source)| matches!(source, Source::Spanner { .. }))
            .map(|(name, _)| (name.clone(), Default::default()))
            .collect();
        Ok(Self {
            catalog: Arc::new(catalog),
            cache: None,
            admission,
            spanner: Arc::new(spanner),
        })
    }

    pub(crate) async fn scope(&self, options: QueryOptions) -> Result<Arc<QueryScope>> {
        Ok(Arc::new(QueryScope::new(options, &self.admission).await?))
    }

    /// Start a read-only query. The deadline covers planning, source reads and stream consumption.
    pub async fn query(
        &self,
        language: Language,
        query: &str,
        options: QueryOptions,
    ) -> Result<QueryResult> {
        self.query_inner(
            language,
            query,
            options,
            false,
            #[cfg(feature = "admin")]
            false,
            #[cfg(feature = "admin")]
            None,
        )
        .await
    }

    #[cfg(feature = "admin")]
    pub(crate) async fn preview_query(
        &self,
        language: Language,
        query: &str,
        options: QueryOptions,
    ) -> Result<QueryResult> {
        self.query_inner(language, query, options, true, true, None)
            .await
    }

    #[cfg(feature = "admin")]
    pub(crate) async fn driver_query(
        &self,
        language: Language,
        query: &str,
        options: QueryOptions,
        transaction: Option<&ReadTransaction>,
    ) -> Result<QueryResult> {
        self.query_inner(language, query, options, false, true, transaction)
            .await
    }

    async fn query_inner(
        &self,
        language: Language,
        query: &str,
        mut options: QueryOptions,
        _preview: bool,
        #[cfg(feature = "admin")] graph_context: bool,
        #[cfg(feature = "admin")] transaction: Option<&ReadTransaction>,
    ) -> Result<QueryResult> {
        ensure!(query.len() <= 64 * 1024, "query exceeds 64 KiB");
        let (prefix, text) = explain_prefix(query);
        let bound;
        let text = if matches!(language, Language::Cypher) {
            bound = cypher::bind_parameters(text, &options.parameters)?;
            &bound
        } else {
            ensure!(
                options.parameters.is_empty(),
                "named parameters are supported by Cypher queries"
            );
            text
        };
        let (destination, text) = export::parse(text, &self.catalog.export_locations)?;
        let exporting = destination.is_some();
        let destination = destination.filter(|_| prefix.is_empty());
        let text = if matches!(language, Language::Cypher) {
            crate::graph_options::using(text, &mut options.graph)?
        } else {
            text
        };
        let scope = self.scope(options).await?;
        scope
            .run(async {
                let context = if matches!(language, Language::Sql) {
                    scope.session()
                } else {
                    scope.graph_session()
                };
                let state = context.state();
                #[cfg(feature = "admin")]
                let mut graph_output = query_graph::GraphOutput::default();
                let (statement, labels, validate_endpoints, expansions, calls, procedures, repeats) =
                    match language {
                        Language::Cypher => {
                            let plan = cypher::plan(&self.catalog, text)?;
                            #[cfg(feature = "admin")]
                            { graph_output = plan.graph_output; }
                            (
                                query::graph_statement(&state, &format!("{prefix}{}", plan.sql))?,
                                plan.labels,
                                false,
                                plan.expansions,
                                plan.calls,
                                plan.procedures,
                                plan.repeats,
                            )
                        }
                        Language::Gremlin => {
                            let plan = crate::gremlin::plan(&self.catalog, text)?;
                            #[cfg(feature = "admin")]
                            { graph_output = plan.graph_output; }
                            (
                                query::graph_statement(&state, &format!("{prefix}{}", plan.sql))?,
                                plan.labels,
                                true,
                                plan.expansions,
                                plan.calls,
                                plan.procedures,
                                plan.repeats,
                            )
                        }
                        Language::Sql => {
                            let export_sql;
                            let sql = if exporting {
                                export_sql = format!("{prefix}{text}");
                                &export_sql
                            } else {
                                query
                            };
                            let statement = state.sql_to_statement(
                                sql,
                                &state.config_options().sql_parser.dialect,
                            )?;
                            ensure!(
                                select_statement(&statement),
                                "SQL supports read-only SELECT and EXPLAIN"
                            );
                            let labels = state
                                .resolve_table_references(&statement)?
                                .into_iter()
                                .map(|table| table.table().to_owned())
                                .filter(|name| {
                                    self.catalog.nodes.contains_key(name)
                                        || self.catalog.edges.contains_key(name)
                                })
                                .collect();
                            (statement, labels, true, Vec::new(), Vec::new(), Vec::new(), Vec::new())
                        }
                    };
                let explain_only =
                    matches!(&statement, Statement::Explain(explain) if !explain.options.analyze);
                let schema_only = explain_only || zero_limit(&statement);
                let pinned = if repeats.is_empty() || schema_only {
                    BTreeSet::new()
                } else {
                    labels.clone()
                };
                let filter = match &statement {
                    Statement::Statement(stmt) => read_filters::query_filter(stmt, &self.catalog),
                    _ => None,
                };
                let mut prepared = self
                    .prepare_query(
                        &scope,
                        &context,
                        labels,
                        model::ReadKind {
                            validate_endpoints,
                            #[cfg(feature = "admin")]
                            transaction,
                            explain_only: schema_only,
                            #[cfg(feature = "admin")]
                            graph_versions: _preview && !schema_only
                                && (!graph_output.columns.is_empty() || graph_output.row_entity.is_some()),
                            lazy_validation: !calls.is_empty()
                                || filter.as_ref().is_some_and(|(_, keys)| !keys.is_empty()),
                            ..Default::default()
                        },
                        filter.as_ref(),
                    )
                    .await?;
                if !pinned.is_empty() {
                    prepared
                        .reservations
                        .extend(cypher::apply::pin_tables(&context, &pinned, &scope).await?);
                    prepared.postgres.clear();
                }
                for expansion in expansions {
                    expansion
                        .register(&self.catalog, &context, &scope, &prepared)
                        .await?;
                }
                for procedure in procedures {
                    prepared.reservations.push(
                        procedure
                            .register(&self.catalog, &context, &scope, schema_only)
                            .await?,
                    );
                }
                let mut ordered_call_output = false;
                for call in calls {
                    ordered_call_output |= !call.order().is_empty();
                    prepared.reservations.push(
                        call.register(&self.catalog, &context, &scope, schema_only)
                            .await?,
                    );
                }
                for repeat in repeats {
                    prepared.reservations.push(
                        repeat.register(&self.catalog, &context, &scope, schema_only).await?
                    );
                }
                let mut state = context.state();
                if ordered_call_output {
                    // Keep the ordered spill stream in one partition. A round-robin
                    // split would coalesce wide batches and merge them again.
                    state.config_mut().options_mut().optimizer.enable_round_robin_repartition =
                        false;
                }
                let plan = query::graph_logical_plan(&state, statement).await?;
                SQLOptions::new()
                    .with_allow_ddl(false)
                    .with_allow_dml(false)
                    .with_allow_statements(false)
                    .verify_plan(&plan)?;
                let frame = if ordered_call_output {
                    DataFrame::new(state, plan)
                } else {
                    context.execute_logical_plan(plan).await?
                };
                let result = scope
                    .result(&self.catalog.graph, &context, frame, prepared, destination)
                    .await?;
                #[cfg(feature = "admin")]
                let result = QueryResult {
                    graph_context: (graph_context && (!graph_output.columns.is_empty() || graph_output.row_entity.is_some())).then_some(context),
                    graph_output,
                    ..result
                };
                Ok(result)
            })
            .await
    }

    pub(crate) async fn prepare(
        &self,
        scope: &QueryScope,
        context: &SessionContext,
        labels: BTreeSet<String>,
    ) -> Result<Prepared> {
        self.prepare_query(scope, context, labels, model::ReadKind::default(), None)
            .await
    }

    #[cfg(feature = "admin")]
    pub(crate) async fn inspect_model(
        &self,
        options: QueryOptions,
        check_data: bool,
    ) -> Result<BTreeMap<String, SchemaRef>> {
        let scope = self.scope(options).await?;
        scope
            .run(async {
                let context = scope.session();
                let labels = self
                    .catalog
                    .nodes
                    .keys()
                    .chain(self.catalog.edges.keys())
                    .cloned()
                    .collect::<BTreeSet<_>>();
                let _prepared = Box::pin(self.prepare_query(
                    &scope,
                    &context,
                    labels.clone(),
                    model::ReadKind {
                        explain_only: !check_data,
                        validate_endpoints: true,
                        check_data,
                        ..Default::default()
                    },
                    None,
                ))
                .await?;
                let mut schemas = BTreeMap::new();
                for label in labels {
                    let frame = context.table(TableReference::bare(label.clone())).await?;
                    schemas.insert(label, Arc::clone(frame.schema().inner()));
                }
                Ok(schemas)
            })
            .await
    }

    #[cfg(feature = "admin")]
    pub(crate) async fn prepare_exploration(
        &self,
        scope: &QueryScope,
        context: &SessionContext,
        labels: BTreeSet<String>,
        exploration: Option<&model::Exploration<'_>>,
    ) -> Result<Prepared> {
        self.prepare_query(
            scope,
            context,
            labels,
            model::ReadKind {
                // Neighbor execution already joins both endpoint relations.
                validate_endpoints: exploration.is_none(),
                lazy_validation: true,
                graph_versions: true,
                exploration,
                ..Default::default()
            },
            None,
        )
        .await
    }

    /// Prepare a whole graph scan whose loader validates all node and edge keys.
    pub(crate) async fn prepare_full_topology(
        &self,
        scope: &QueryScope,
        context: &SessionContext,
        labels: BTreeSet<String>,
        weight: Option<&str>,
    ) -> Result<Prepared> {
        self.prepare_query(
            scope,
            context,
            labels,
            model::ReadKind {
                topology: Some(model::TopologyFields(weight)),
                ..Default::default()
            },
            None,
        )
        .await
    }

    async fn prepare_query(
        &self,
        scope: &QueryScope,
        context: &SessionContext,
        mut labels: BTreeSet<String>,
        read: model::ReadKind<'_>,
        filter: Option<&(String, Vec<crate::Predicate>)>,
    ) -> Result<Prepared> {
        let catalog = &self.catalog;
        let required = model::required(catalog, &mut labels, &scope.options.graph, read.topology)?;
        if scope.options.graph.require_consistent_snapshot {
            verify_consistent_snapshot(catalog, required.keys().copied())?;
        }
        let filters = read_filters::for_labels(catalog, &labels, &scope.options.graph, filter);
        #[cfg(feature = "admin")]
        let private = if read.exploration.is_some() {
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
                                    Snapshot::open(
                                        dsn_env,
                                        scope.options.timeout,
                                        scope.reads.clone(),
                                    )
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
                            let snapshot = Box::pin(self.spanner[name].open(
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
            let alias = format!("raw_{}", tables.len());
            let schema = table.schema();
            private.register_table(TableReference::bare(alias.as_str()), table)?;
            tables.insert(reference, Raw { alias, schema });
        }
        for snapshot in mysql_snapshots.into_values() {
            snapshot.close().await?;
        }
        reservations.extend(
            model::register(
                catalog,
                context,
                &private,
                labels,
                model::Tables {
                    raw: &tables,
                    postgres: &postgres,
                },
                scope,
                read,
            )
            .await?,
        );
        Ok(Prepared {
            versions: versions.into_values().collect(),
            reservations,
            postgres,
        })
    }
}

/// Pins repeatable-read transactions and detects replacement of read Parquet objects.
#[cfg(feature = "admin")]
#[derive(Default)]
pub(crate) struct ReadTransaction {
    postgres: tokio::sync::Mutex<BTreeMap<String, Arc<Snapshot>>>,
    parquet: std::sync::Mutex<BTreeMap<String, String>>,
}

#[cfg(feature = "admin")]
impl ReadTransaction {
    async fn postgres(
        &self,
        name: &str,
        dsn_env: &str,
        scope: &QueryScope,
    ) -> Result<Arc<Snapshot>> {
        let mut snapshots = self.postgres.lock().await;
        if let Some(snapshot) = snapshots.get(name) {
            return Ok(snapshot.clone());
        }
        let snapshot = Arc::new(
            Snapshot::open(dsn_env, scope.options.timeout, scope.reads.clone())
                .await
                .with_context(|| format!("opening PostgreSQL source {name}"))?,
        );
        snapshots.insert(name.into(), snapshot.clone());
        Ok(snapshot)
    }

    fn check_parquet(&self, object: &SourceVersion) -> Result<()> {
        let mut versions = self.parquet.lock().unwrap();
        let pinned = versions
            .entry(object.source.clone())
            .or_insert_with(|| object.version.clone());
        ensure!(
            pinned == &object.version,
            "Parquet object changed within read transaction"
        );
        Ok(())
    }
}

fn verify_consistent_snapshot<'a>(
    catalog: &Catalog,
    references: impl ExactSizeIterator<Item = &'a str>,
) -> Result<()> {
    let count = references.len();
    let mut first_source = None;
    for reference in references {
        let (name, relation, source) = catalog.source(reference)?;
        let first = *first_source.get_or_insert(name);
        let supported = match source {
            Source::Postgres { .. }
            | Source::Mysql { .. }
            | Source::Snowflake { .. }
            | Source::Bigquery { .. }
            | Source::Spanner { .. } => name == first,
            Source::Parquet { tables } => count == 1 && tables[relation].len() == 1,
            _ => count == 1,
        };
        ensure!(
            supported,
            "query requires one consistent snapshot, which the selected tables cannot share ({reference})"
        );
    }
    Ok(())
}

pub(crate) struct Prepared {
    pub(crate) versions: Vec<SourceVersion>,
    pub(crate) reservations: Vec<MemoryReservation>,
    pub(crate) postgres: BTreeMap<String, Arc<PgTable>>,
}

struct Raw {
    alias: String,
    schema: SchemaRef,
}

async fn materialize_edges(
    context: &SessionContext,
    raw: &Raw,
    columns: &[String],
    alias: &str,
    scope: &QueryScope,
    explain_only: bool,
) -> Result<MemoryReservation> {
    let frame = context
        .sql(&format!(
            "SELECT {} FROM {} AS src",
            select_columns("src", columns),
            ident(&raw.alias)
        ))
        .await?;
    let fields = frame
        .schema()
        .inner()
        .fields()
        .iter()
        .cloned()
        .chain([Arc::new(Field::new(ROW_ID, DataType::UInt64, false))])
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(fields));
    let mut buffer = source::snapshot::Buffer::new(
        &scope.runtime.memory_pool,
        (!explain_only).then_some(&scope.runtime.disk_manager),
    );
    if explain_only {
        let (table, reservation) = buffer.finish_schema(schema)?;
        context.register_table(TableReference::bare(alias), table)?;
        return Ok(reservation);
    }
    let mut stream = frame.execute_stream().await?;
    let mut next = 0_u64;
    while let Some(batch) = stream.try_next().await? {
        let count = batch.num_rows();
        let end = next
            .checked_add(count as u64)
            .context("too many materialized edges")?;
        let ids = UInt64Array::from_iter_values(next..end);
        let mut arrays = batch.columns().to_vec();
        arrays.push(Arc::new(ids));
        buffer.push(RecordBatch::try_new(Arc::clone(&schema), arrays)?)?;
        next = end;
    }
    let (table, reservation) = buffer.finish(schema, &scope.reads)?;
    context.register_table(TableReference::bare(alias), table)?;
    Ok(reservation)
}

fn select_columns(alias: &str, columns: &[String]) -> String {
    columns
        .iter()
        .map(|column| qualified(alias, column))
        .collect::<Vec<_>>()
        .join(", ")
}

fn check_key_type(ty: &DataType, label: &str, key: &str) -> Result<()> {
    ensure!(
        matches!(
            ty,
            DataType::Boolean
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Utf8
                | DataType::Utf8View
                | DataType::LargeUtf8
                | DataType::Binary
                | DataType::BinaryView
                | DataType::LargeBinary
                | DataType::FixedSizeBinary(_)
                | DataType::Decimal128(..)
                | DataType::Date32
                | DataType::Date64
                | DataType::Timestamp(..)
        ),
        "unsupported business key type {ty} for {label}.{key}"
    );
    Ok(())
}

fn explain_prefix(query: &str) -> (&'static str, &str) {
    let text = query.trim_start();
    let Some((word, rest)) = text.split_once(char::is_whitespace) else {
        return ("", text);
    };
    if !word.eq_ignore_ascii_case("explain") {
        return ("", text);
    }
    let rest = rest.trim_start();
    if let Some((word, query)) = rest.split_once(char::is_whitespace)
        && word.eq_ignore_ascii_case("analyze")
    {
        return ("EXPLAIN ANALYZE ", query);
    }
    ("EXPLAIN ", rest)
}

fn zero_limit(statement: &Statement) -> bool {
    let statement = match statement {
        Statement::Explain(explain) => explain.statement.as_ref(),
        statement => statement,
    };
    let Statement::Statement(statement) = statement else {
        return false;
    };
    let SqlStatement::Query(query) = statement.as_ref() else {
        return false;
    };
    let Some(LimitClause::LimitOffset {
        limit: Some(SqlExpr::Value(value)),
        ..
    }) = &query.limit_clause
    else {
        return false;
    };
    matches!(&value.value, SqlValue::Number(value, _) if value.parse::<u64>() == Ok(0))
}

fn select_statement(statement: &Statement) -> bool {
    match statement {
        Statement::Statement(statement) => matches!(statement.as_ref(), SqlStatement::Query(_)),
        Statement::Explain(explain) => matches!(
            explain.statement.as_ref(),
            Statement::Statement(statement) if matches!(statement.as_ref(), SqlStatement::Query(_))
        ),
        _ => false,
    }
}
