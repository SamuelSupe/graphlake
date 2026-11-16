mod exploration;
mod export;
#[cfg(feature = "admin")]
pub(crate) use model::{Exploration, exploration_seek_type};
#[cfg(feature = "admin")]
pub(crate) use model::{KEY_VALIDATOR, key_validator};
mod model;
mod query;
mod query_functions;
pub(crate) mod sources;
#[cfg(feature = "admin")]
pub(crate) use query_functions::register as register_graph_functions;
#[cfg(feature = "admin")]
pub(crate) mod query_graph;
pub(crate) mod read_filters;
pub(crate) mod sql_read;

#[cfg(feature = "admin")]
pub(crate) use query::Admission;
#[cfg(feature = "admin")]
pub(crate) use query::spilled_bytes as execution_spill;
pub use query::{EngineLimits, QueryMetrics, QueryOptions, QueryResult};
pub(crate) use query::{QueryScope, graph_frame, gremlin_frame};

#[cfg(feature = "admin")]
pub(crate) fn variant_fields(fields: &crate::arrow::datatypes::Fields) -> bool {
    query_functions::cypher::is_variant(fields)
}

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex, OnceLock},
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
    parquet_keys: Arc<model::KeyChecks>,
    pub(crate) managed_parquet: Arc<BTreeMap<uuid::Uuid, source::parquet_snapshot::RemoteSnapshot>>,
    spanner: Arc<BTreeMap<String, source::warehouse::spanner::Client>>,
    trino_schemas: Arc<source::warehouse::trino::SchemaCache>,
    delta_snapshots: Arc<source::delta::SnapshotCache>,
}

impl Engine {
    pub fn new(catalog: Catalog) -> Result<Self> {
        Self::with_admission(catalog, Arc::clone(default_admission()))
    }

    /// Create an engine with a dedicated resource budget shared by its clones.
    pub fn new_with_limits(catalog: Catalog, limits: EngineLimits) -> Result<Self> {
        Self::with_admission(catalog, Arc::new(query::Admission::new(limits)?))
    }

    pub(crate) fn with_admission(
        catalog: Catalog,
        admission: Arc<query::Admission>,
    ) -> Result<Self> {
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
            parquet_keys: Arc::new(Mutex::new(BTreeMap::new())),
            spanner: Arc::new(spanner),
            trino_schemas: Arc::default(),
            delta_snapshots: Arc::default(),
            managed_parquet: Arc::default(),
        })
    }

    pub(crate) async fn scope(&self, options: QueryOptions) -> Result<Arc<QueryScope>> {
        Ok(Arc::new(QueryScope::new(options, &self.admission).await?))
    }

    #[cfg(feature = "admin")]
    pub(crate) fn use_admission(&mut self, admission: Arc<query::Admission>) {
        self.admission = admission;
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
            #[cfg(feature = "admin")]
            None,
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
        self.query_inner(language, query, options, true, true, None, None, None)
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
        self.query_inner(
            language,
            query,
            options,
            false,
            true,
            transaction,
            None,
            None,
        )
        .await
    }

    #[cfg(feature = "admin")]
    pub(crate) async fn cluster_query(
        &self,
        language: Language,
        query: &str,
        mut options: QueryOptions,
        preview: bool,
        distributed: &crate::admin::cluster::distributed::Distributed,
        csr: Option<&crate::admin::cluster::csr::CypherCsr>,
    ) -> Result<QueryResult> {
        options.partitions = options.partitions.max(distributed.partitions);
        self.query_inner(
            language,
            query,
            options,
            preview,
            preview,
            None,
            Some(distributed),
            csr,
        )
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
        #[cfg(feature = "admin")] distributed: Option<
            &crate::admin::cluster::distributed::Distributed,
        >,
        #[cfg(feature = "admin")] csr: Option<&crate::admin::cluster::csr::CypherCsr>,
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
        #[cfg(feature = "admin")]
        if let Some(csr) = csr {
            let mut explicit = crate::GraphOptions::default();
            ensure!(
                crate::graph_options::using(text, &mut explicit)? == text,
                "CSR queries use the generation's graph scope"
            );
            options.graph = csr.options();
        }
        let text = if matches!(language, Language::Cypher) {
            crate::graph_options::using(text, &mut options.graph)?
        } else {
            text
        };
        let scope = self.scope(options).await?;
        scope
            .run(query::planner_stack(Box::pin(async {
                let mut context = if matches!(language, Language::Sql) {
                    scope.session()
                } else {
                    scope.graph_session()
                };
                let mut state = context.state();
                #[cfg(feature = "admin")]
                let mut graph_output = query_graph::GraphOutput::default();
                let (
                    statement,
                    labels,
                    validate_endpoints,
                    expansions,
                    calls,
                    procedures,
                    gremlin_stages,
                    schema_procedures,
                ) = match language {
                    Language::Cypher => {
                        #[cfg(feature = "admin")]
                        let partitioned_csr = csr.is_some();
                        #[cfg(not(feature = "admin"))]
                        let partitioned_csr = false;
                        #[cfg(feature = "admin")]
                        let indexed_csr = csr.is_some_and(|csr| csr.indexed());
                        #[cfg(not(feature = "admin"))]
                        let indexed_csr = false;
                        let plan = cypher::plan(&self.catalog, text, partitioned_csr, indexed_csr)?;
                        if plan.has_row_expression_lambda {
                            // DataFusion 55 can move a lambda's captured column
                            // to the index already assigned to its parameter.
                            let rules = state
                                .physical_optimizers()
                                .iter()
                                .filter(|rule| rule.name() != "ProjectionPushdown")
                                .cloned()
                                .collect();
                            state = datafusion::execution::SessionStateBuilder::new_from_existing(
                                state,
                            )
                            .with_physical_optimizer_rules(rules)
                            .build();
                            context =
                                datafusion::prelude::SessionContext::new_with_state(state.clone());
                        }
                        if plan.has_row_expressions
                            && !partitioned_csr
                            && scope.options.memory_bytes <= 32 * 1024 * 1024
                            && scope.options.spill_bytes > 0
                        {
                            // A decorrelated expression can join large relations even
                            // when the outer key restricts the final result to one row.
                            // DataFusion only selects its spillable merge join with
                            // more than one target partition.
                            let config = state.config_mut().options_mut();
                            config.execution.target_partitions = 2;
                            config.execution.batch_size =
                                datafusion::config::ConfigNonZeroUsize::try_new(
                                    config.execution.batch_size.get().min(128),
                                )?;
                            config.execution.sort_spill_reservation_bytes = 64 * 1024;
                            config.optimizer.prefer_hash_join = false;
                            if !plan.calls.is_empty() {
                                // Nested CALL can keep an upstream stream open while
                                // preparing the next group. Avoid speculative scan
                                // and round-robin partitions competing for its sort
                                // reservations.
                                config.optimizer.repartition_file_scans = false;
                                config.optimizer.enable_round_robin_repartition = false;
                                // The migration aggregate reserves a large hash
                                // state even for a narrow page over a few groups.
                                config.execution.enable_migration_aggregate = false;
                                scope.runtime.disk_manager.set_max_spill_merge_fan_in(4);
                            }
                            context =
                                datafusion::prelude::SessionContext::new_with_state(state.clone());
                        }
                        #[cfg(feature = "admin")]
                        {
                            graph_output = plan.graph_output;
                        }
                        (
                            query::graph_statement(&state, &format!("{prefix}{}", plan.sql))?,
                            plan.labels,
                            false,
                            plan.expansions,
                            plan.calls,
                            plan.procedures,
                            plan.gremlin_stages,
                            plan.schema_procedures,
                        )
                    }
                    Language::Gremlin => {
                        let plan = crate::gremlin::plan(&self.catalog, text)?;
                        let statement =
                            query::gremlin_statement(&state, &format!("{prefix}{}", plan.sql))?;
                        #[cfg(feature = "admin")]
                        {
                            graph_output = plan.graph_output;
                        }
                        (
                            statement,
                            plan.labels,
                            true,
                            plan.expansions,
                            plan.calls,
                            plan.procedures,
                            plan.gremlin_stages,
                            Vec::new(),
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
                        let statement = state
                            .sql_to_statement(sql, &state.config_options().sql_parser.dialect)?;
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
                        (
                            statement,
                            labels,
                            true,
                            Vec::new(),
                            Vec::new(),
                            Vec::new(),
                            Vec::new(),
                            Vec::new(),
                        )
                    }
                };
                let explain_only =
                    matches!(&statement, Statement::Explain(explain) if !explain.options.analyze);
                let schema_only = explain_only || zero_limit(&statement);
                let pinned = if schema_only
                    || gremlin_stages
                        .iter()
                        .all(|stage| !matches!(stage, crate::gremlin::repeat::Stage::Repeat(_)))
                {
                    BTreeSet::new()
                } else {
                    labels.clone()
                };
                let filter = match &statement {
                    Statement::Statement(stmt) => read_filters::query_filter(stmt, &self.catalog),
                    _ => None,
                };
                let mut sql = match &statement {
                    Statement::Statement(stmt)
                        if matches!(language, Language::Sql) && !exporting && !schema_only =>
                    {
                        sql_read::hint(stmt, &self.catalog)
                    }
                    Statement::Statement(stmt)
                        if !exporting
                            && !schema_only
                            && labels.len() == 1
                            && calls.is_empty()
                            && expansions.is_empty()
                            && gremlin_stages.is_empty()
                            && procedures.is_empty()
                            && schema_procedures.is_empty() =>
                    {
                        sql_read::graph_hint(stmt, &self.catalog)
                    }
                    _ => None,
                };
                if !scope.options.graph.logical_partition.is_empty()
                    && let Some(sql) = sql.as_mut()
                {
                    sql.native = None;
                }
                #[cfg(feature = "admin")]
                let distributed = distributed.filter(|_| {
                    csr.is_none()
                        && !schema_only
                        && prefix.is_empty()
                        && calls
                            .iter()
                            .all(|call| matches!(call.mode, cypher::apply::Mode::Materialize))
                        && gremlin_stages.is_empty()
                        && expansions.is_empty()
                        && procedures.is_empty()
                        && schema_procedures.is_empty()
                });
                #[cfg(feature = "admin")]
                if (distributed.is_some() || transaction.is_some())
                    && let Some(sql) = sql.as_mut()
                {
                    sql.native = None;
                    sql.single = None;
                }
                #[cfg(feature = "admin")]
                let snapshot = if let Some(csr) = csr {
                    Some(
                        csr.prepare(&context, &scope, &self.catalog, labels.clone())
                            .await?,
                    )
                } else {
                    None
                };
                #[cfg(not(feature = "admin"))]
                let snapshot = None;
                let mut prepared = if let Some(prepared) = snapshot {
                    prepared
                } else {
                    self.prepare_query(
                        &scope,
                        &context,
                        labels,
                        model::ReadKind {
                            validate_endpoints,
                            #[cfg(feature = "admin")]
                            transaction,
                            #[cfg(feature = "admin")]
                            distributed,
                            explain_only: schema_only,
                            sql: sql.as_ref(),
                            #[cfg(feature = "admin")]
                            graph_versions: _preview
                                && !schema_only
                                && (!graph_output.columns.is_empty()
                                    || graph_output.row_entity.is_some()),
                            lazy_validation: !calls.is_empty()
                                || filter.as_ref().is_some_and(|(_, keys)| !keys.is_empty()),
                            ..Default::default()
                        },
                        filter.as_ref(),
                    )
                    .await?
                };
                if !pinned.is_empty() {
                    prepared
                        .reservations
                        .extend(cypher::apply::pin_tables(&context, &pinned, &scope).await?);
                    prepared.postgres.clear();
                }
                if !schema_procedures.is_empty() {
                    let schema_context = scope.graph_session();
                    let labels: BTreeSet<String> = schema_procedures
                        .iter()
                        .flat_map(|procedure| procedure.labels(&self.catalog))
                        .collect();
                    #[cfg(feature = "admin")]
                    let schema_snapshot = if let Some(csr) = csr {
                        Some(
                            csr.prepare(&schema_context, &scope, &self.catalog, labels.clone())
                                .await?,
                        )
                    } else {
                        None
                    };
                    #[cfg(not(feature = "admin"))]
                    let schema_snapshot = None;
                    let _schema = if let Some(snapshot) = schema_snapshot {
                        snapshot
                    } else {
                        self.prepare_query(
                            &scope,
                            &schema_context,
                            labels,
                            model::ReadKind {
                                explain_only: true,
                                ..Default::default()
                            },
                            None,
                        )
                        .await?
                    };
                    for procedure in schema_procedures {
                        procedure
                            .register(&self.catalog, &schema_context, &context)
                            .await?;
                    }
                }
                for expansion in expansions {
                    expansion
                        .register(
                            &self.catalog,
                            &context,
                            &scope,
                            &prepared,
                            #[cfg(feature = "admin")]
                            csr.filter(|_| !schema_only),
                        )
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
                for stage in gremlin_stages {
                    prepared.reservations.push(
                        stage
                            .register(&self.catalog, &context, &scope, schema_only)
                            .await?,
                    );
                }
                let mut state = context.state();
                if ordered_call_output {
                    // Keep the ordered spill stream in one partition. A round-robin
                    // split would coalesce wide batches and merge them again.
                    state
                        .config_mut()
                        .options_mut()
                        .optimizer
                        .enable_round_robin_repartition = false;
                }
                #[cfg(feature = "admin")]
                if distributed.is_some() {
                    crate::admin::cluster::distributed::Distributed::configure(
                        &mut state,
                        scope.options.memory_bytes,
                    );
                }
                let plan = query::graph_logical_plan(&state, statement).await?;
                SQLOptions::new()
                    .with_allow_ddl(false)
                    .with_allow_dml(false)
                    .with_allow_statements(false)
                    .verify_plan(&plan)?;
                #[cfg(feature = "admin")]
                let distributed_planning = distributed.is_some();
                #[cfg(not(feature = "admin"))]
                let distributed_planning = false;
                let frame = if let Some(batch) = prepared.aggregate.take() {
                    let schema = Arc::clone(plan.schema().inner());
                    ensure!(
                        schema.fields().len() == 1
                            && schema.field(0).data_type() == batch.column(0).data_type(),
                        "source aggregate did not match the SQL result type"
                    );
                    context.read_batch(RecordBatch::try_new(schema, batch.columns().to_vec())?)?
                } else if ordered_call_output || distributed_planning {
                    DataFrame::new(state, plan)
                } else {
                    context.execute_logical_plan(plan).await?
                };
                #[cfg(feature = "admin")]
                let result = if let Some(distributed) = distributed {
                    let ctx = Arc::new(frame.task_ctx());
                    let plan = frame.create_physical_plan().await?;
                    let plan = distributed
                        .execute(
                            plan,
                            &scope,
                            ctx.clone(),
                            !matches!(language, Language::Sql),
                            true,
                        )
                        .await?;
                    scope.execute(&self.catalog.graph, &context, plan, prepared, destination)?
                } else {
                    scope
                        .result(&self.catalog.graph, &context, frame, prepared, destination)
                        .await?
                };
                #[cfg(not(feature = "admin"))]
                let result = scope
                    .result(&self.catalog.graph, &context, frame, prepared, destination)
                    .await?;
                #[cfg(feature = "admin")]
                let result = {
                    let mut result = result;
                    if graph_context
                        && (!graph_output.columns.is_empty() || graph_output.row_entity.is_some())
                    {
                        result.retain_graph_resources();
                        result.graph_context = Some(context);
                    }
                    result.graph_csr = csr.filter(|csr| csr.indexed()).cloned();
                    result.graph_output = graph_output;
                    result
                };
                Ok(result)
            })))
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
    pub(crate) async fn probe_model_sources(
        &self,
        options: QueryOptions,
        admission: &Arc<query::Admission>,
    ) -> Result<(BTreeMap<String, bool>, Result<()>)> {
        use futures::{StreamExt, stream};

        let scope = QueryScope::new(options, admission).await?;
        let mut labels: BTreeSet<_> = self
            .catalog
            .nodes
            .keys()
            .chain(self.catalog.edges.keys())
            .cloned()
            .collect();
        let references =
            model::required(&self.catalog, &mut labels, &scope.options.graph, None, None)?;
        let sources = references
            .keys()
            .map(|reference| {
                self.catalog
                    .source(reference)
                    .map(|(name, _, _)| name.to_owned())
            })
            .collect::<Result<BTreeSet<_>>>()?;
        scope
            .run(async {
                let sources: BTreeMap<String, bool> = stream::iter(sources)
                    .map(|source: String| {
                        let context = scope.session();
                        let scope = &scope;
                        let labels = labels.clone();
                        async move {
                            let result = Box::pin(self.prepare_query(
                                scope,
                                &context,
                                labels,
                                model::ReadKind {
                                    explain_only: true,
                                    probe_source: Some(&source),
                                    ..Default::default()
                                },
                                None,
                            ))
                            .await;
                            (source, result.is_ok())
                        }
                    })
                    .buffered(4)
                    .collect()
                    .await;
                let mapping = if sources.values().all(|available| *available) {
                    // Check the mapping across sources after collecting individual failures.
                    Box::pin(self.prepare_query(
                        &scope,
                        &scope.session(),
                        labels,
                        model::ReadKind {
                            explain_only: true,
                            validate_endpoints: true,
                            ..Default::default()
                        },
                        None,
                    ))
                    .await
                    .map(|_| ())
                } else {
                    Ok(())
                };
                Ok((sources, mapping))
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

    #[cfg(feature = "admin")]
    pub(crate) async fn prepare_partitioned_topology(
        &self,
        scope: &QueryScope,
        context: &SessionContext,
    ) -> Result<Prepared> {
        self.prepare_query(
            scope,
            context,
            self.catalog
                .nodes
                .keys()
                .chain(self.catalog.edges.keys())
                .cloned()
                .collect(),
            model::ReadKind {
                validate_endpoints: true,
                partitioned_topology: true,
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
        let required = model::required(
            catalog,
            &mut labels,
            &scope.options.graph,
            read.topology,
            read.sql,
        )?;
        if scope.options.graph.require_consistent_snapshot {
            verify_consistent_snapshot(catalog, required.keys().copied())?;
        }
        let filters = read_filters::for_labels(catalog, &labels, &scope.options.graph, filter);
        let (private, tables, mut prepared, proven_key_label) = sources::prepare(
            catalog,
            &self.spanner,
            &self.trino_schemas,
            Some(&self.delta_snapshots),
            &self.managed_parquet,
            scope,
            required,
            &filters,
            read,
        )
        .await?;
        #[cfg(feature = "admin")]
        if read.probe_source.is_some() {
            return Ok(prepared);
        }
        prepared.reservations.extend(
            model::register(
                catalog,
                context,
                &private,
                labels,
                model::Tables {
                    raw: &tables,
                    postgres: &prepared.postgres,
                    proven_key_label,
                    parquet_keys: &self.parquet_keys,
                },
                scope,
                read,
            )
            .await?,
        );
        Ok(prepared)
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
            | Source::Sqlite { .. }
            | Source::Duckdb { .. }
            | Source::OracleNative { .. }
            | Source::Mariadb(_)
            | Source::Mysql { .. }
            | Source::Snowflake { .. }
            | Source::Bigquery { .. }
            | Source::Spanner { .. } => name == first,
            Source::MongodbNative { .. } => false,
            Source::ObjectStorage { tables, .. } => count == 1 && tables[relation].files.len() == 1,
            Source::Parquet { tables } => count == 1 && tables[relation].len() == 1,
            Source::Csv { tables } | Source::Jsonl { tables } => {
                count == 1 && tables[relation].files.len() == 1
            }
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
    pub(crate) aggregate: Option<RecordBatch>,
    pub(crate) versions: Vec<SourceVersion>,
    pub(crate) reservations: Vec<MemoryReservation>,
    pub(crate) postgres: BTreeMap<String, Arc<PgTable>>,
}

struct Raw {
    alias: String,
    schema: SchemaRef,
    parquet_versions: Option<Vec<(String, String)>>,
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

fn default_admission() -> &'static Arc<query::Admission> {
    static ADMISSION: OnceLock<Arc<query::Admission>> = OnceLock::new();
    ADMISSION.get_or_init(|| {
        Arc::new(query::Admission::new(EngineLimits::default()).expect("default engine limits"))
    })
}
