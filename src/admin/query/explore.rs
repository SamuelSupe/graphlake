use std::collections::{BTreeMap, BTreeSet};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use datafusion::prelude::SessionContext;
use datafusion::{arrow::datatypes::DataType, common::TableReference};
use sha2::{Digest, Sha256};

use crate::catalog::{equal_keys, ident, qualified};
use crate::{Comparison, Predicate, ScalarValue};

use super::{
    graph::{self, Collector, Element, Identity, Kind},
    *,
};

mod pages;
pub(in crate::admin) use pages::Sessions;

const NEIGHBOR_LIMIT: usize = 50;
const SEARCH_PAGE_SIZE: usize = 100;

#[derive(Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Direction {
    In,
    Out,
    #[default]
    Both,
}

impl Direction {
    fn incoming(self) -> bool {
        !matches!(self, Self::Out)
    }

    fn outgoing(self) -> bool {
        !matches!(self, Self::In)
    }
}

#[derive(Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct NeighborFilter {
    #[serde(default)]
    direction: Direction,
    edge_label: Option<String>,
    #[serde(default)]
    properties: Vec<Predicate>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::admin) struct ExploreRequest {
    model_revision: String,
    csr_generation: Option<Uuid>,
    element: Identity,
    neighbors: bool,
    graph_options: crate::GraphOptions,
    timeout_ms: Option<u64>,
    #[serde(default)]
    filter: NeighborFilter,
    cursor: Option<String>,
    search: Option<ExploreSearch>,
    #[serde(default)]
    source_versions: BTreeMap<String, String>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExploreSearch {
    query: String,
    #[serde(default)]
    offset: usize,
    #[serde(default)]
    hidden_nodes: std::collections::BTreeSet<String>,
    #[serde(default)]
    hidden_edges: std::collections::BTreeSet<String>,
}

#[derive(Serialize)]
struct ExploreMatch {
    id: String,
    #[serde(flatten)]
    identity: Identity,
    page: usize,
    cursor: String,
}

#[derive(Serialize)]
struct ExploreSearchResult {
    matches: Vec<ExploreMatch>,
    total: usize,
}

fn search_match(item: &pages::SearchElement, query: &str) -> bool {
    let properties = item
        .properties
        .iter()
        .flat_map(|properties| properties.iter());
    if let Some((name, value)) = query.split_once('=')
        && !name.trim().is_empty()
    {
        return properties.into_iter().any(|(key, text)| {
            key.to_lowercase() == name.trim()
                && text
                    .as_deref()
                    .unwrap_or("null")
                    .to_lowercase()
                    .contains(value.trim())
        });
    }
    std::iter::once(item.label.as_str())
        .chain(item.key.iter().map(String::as_str))
        .chain(properties.filter_map(|(_, value)| value.as_deref()))
        .any(|value| value.to_lowercase().contains(query))
}

impl ExploreSearch {
    fn visible(&self, item: &pages::SearchElement) -> bool {
        match item.kind {
            Kind::Node => !self.hidden_nodes.contains(&item.label),
            Kind::Edge => {
                !self.hidden_edges.contains(&item.label)
                    && (self.hidden_nodes.is_empty()
                        || [&item.from, &item.to].into_iter().flatten().all(|id| {
                            serde_json::from_str::<Identity>(id)
                                .is_ok_and(|end| !self.hidden_nodes.contains(&end.label))
                        }))
            }
        }
    }
}

#[derive(Serialize)]
pub(in crate::admin) struct ExploreResult {
    selected: Option<Element>,
    graph: Box<serde_json::value::RawValue>,
    sources: Vec<SourceVersion>,
    next_cursor: Option<String>,
    page_cursor: Option<String>,
    previous_cursor: Option<String>,
    page_number: usize,
}

fn invalid_filter(message: &str) -> Error {
    Error::invalid("invalid_neighbor_filter", message)
}

fn invalid_cursor() -> Error {
    Error::invalid("invalid_neighbor_cursor", "续页游标无效，请重新展开邻居")
}

fn comparable_versions(sources: &[SourceVersion]) -> BTreeMap<String, String> {
    sources
        .iter()
        .filter(|item| is_comparable(item))
        .map(|item| (item.source.clone(), item.version.clone()))
        .collect()
}

pub(in crate::admin) async fn explore(
    State(app): State<Arc<App>>,
    Path(id): Path<String>,
    Extension(principal): Extension<Principal>,
    Json(mut request): Json<ExploreRequest>,
) -> ApiResult<Response> {
    let _permit = app.explore_work.try_acquire().map_err(|_| Error::busy())?;
    if request.search.as_ref().is_some_and(|search| {
        !request.neighbors
            || request.cursor.is_none()
            || search.query.trim().is_empty()
            || search.query.len() > 256
            || search.hidden_nodes.len() + search.hidden_edges.len() > 4096
    }) {
        return Err(Error::invalid(
            "invalid_neighbor_search",
            "请选择邻域并输入 1 至 256 字节的搜索词",
        ));
    }
    crate::admin::access::check_query(&app, &principal, &id)?;
    let mut engine = if request.csr_generation.is_some() {
        let files = app.models.lock().unwrap().read_files();
        let admission = app.admission.clone();
        let (id, revision) = (id.clone(), request.model_revision.clone());
        tokio::task::spawn_blocking(move || files.snapshot_engine(&id, &revision, &admission))
            .await
            .map_err(|_| Error::storage())??
    } else {
        app.query_model(&id, &request.model_revision, &principal)
            .await?
            .2
    };
    // A blocked neighbor scan must not consume the foreground query's slot.
    engine.use_admission(app.exploration_admission.clone());
    let selection = crate::admin::local_tables::managed::resolve(
        &app,
        &mut engine,
        &id,
        app.options.timeout.as_secs() + 120,
    )
    .await?;
    if engine.catalog.freshness.policy != crate::FreshnessPolicy::default() {
        return Err(super::freshness::conflict(
            "此模型的严格查询策略需通过查询 API 或 MCP 执行",
        ));
    }
    if let Some(term) = access::partition(&app, &principal, &id)? {
        if request.csr_generation.is_some() {
            return Err(access::forbidden());
        }
        crate::admin::users::apply_partition(&engine.catalog, &mut request.graph_options, &term)?;
    }
    // Source identifiers come from prepared relations, never from request paths.
    let catalog = &engine.catalog;
    let target = &request.element;
    if target.key.iter().any(|key| key.len() > 1024)
        || graph::identity(
            catalog,
            target.kind == Kind::Node,
            target.label.clone(),
            target.key.clone(),
        )
        .is_none()
        || request.neighbors && target.kind != Kind::Node
    {
        return Err(Error::invalid(
            "invalid_graph_element",
            "图元素或业务键无效",
        ));
    }
    if request.csr_generation.is_none()
        && target.kind == Kind::Edge
        && catalog.edges[&target.label].key.is_empty()
    {
        return Err(Error::invalid(
            "transient_graph_edge",
            "此边没有业务键，无法从新数据快照重新定位",
        ));
    }
    let mut options = app.options.clone();
    set_timeout(&mut options, request.timeout_ms)?;
    request
        .graph_options
        .validate()
        .map_err(|_| Error::invalid("invalid_graph_options", "图查询过滤条件无效"))?;
    let snapshot = if let Some(generation) = request.csr_generation {
        let snapshot = super::super::cluster::csr::ExplorationSnapshot::open(
            &app,
            &id,
            &request.model_revision,
            generation,
        )
        .await?;
        options.graph = snapshot.options().clone();
        Some(snapshot)
    } else {
        options.graph = request.graph_options.clone();
        None
    };
    let filter = &request.filter;
    if filter.properties.len() > 8
        || filter
            .properties
            .iter()
            .any(|p| p.validate().is_err() || p.value.as_str().is_some_and(|v| v.len() > 4096))
        || !filter.properties.is_empty() && filter.edge_label.is_none()
    {
        return Err(invalid_filter(
            "请选择边类型，并填写有效的属性筛选条件（最多 8 条）",
        ));
    }
    let edges = catalog
        .edges
        .iter()
        .filter(|(label, edge)| {
            request.neighbors
                && filter.edge_label.as_ref().is_none_or(|name| name == *label)
                && (filter.direction.outgoing() && edge.from.node == target.label
                    || filter.direction.incoming() && edge.to.node == target.label)
        })
        .collect::<Vec<_>>();
    if filter.edge_label.is_some() && edges.is_empty() {
        return Err(invalid_filter("选中的边类型与节点或方向不匹配"));
    }
    if edges.iter().any(|(_, edge)| {
        filter
            .properties
            .iter()
            .any(|p| !edge.properties.contains(&p.column))
    }) {
        return Err(invalid_filter("筛选列必须是所选边类型的映射属性"));
    }
    let mut signature = Sha256::new();
    signature.update(
        serde_json::to_vec(&(
            &principal,
            &id,
            &request.model_revision,
            target,
            filter,
            &options.graph,
        ))
        .expect("serializing neighbor scope"),
    );
    if let Some(generation) = request.csr_generation {
        signature.update(generation.as_bytes());
    }
    let signature = URL_SAFE_NO_PAD.encode(signature.finalize());
    let cursors = app.explorations.lock().unwrap().cursors;
    let mut resume = request
        .cursor
        .as_deref()
        .map(|encoded| {
            cursors
                .decode(encoded, &signature)
                .filter(|_| request.neighbors)
                .ok_or_else(invalid_cursor)
        })
        .transpose()?;
    let time_dependent = |label: &str| {
        catalog
            .mapping(label)
            .scd2
            .is_some_and(|scd2| scd2.current_flag.is_none())
    };
    let temporal_current = snapshot.is_none()
        && options
            .graph
            .snapshot_time
            .as_deref()
            .is_none_or(|time| time.eq_ignore_ascii_case("current"))
        && (time_dependent(&target.label)
            || edges.iter().any(|(label, edge)| {
                time_dependent(label)
                    || time_dependent(&edge.from.node)
                    || time_dependent(&edge.to.node)
            }));
    let temporal_at = if temporal_current {
        Some(match &resume {
            Some(cursor) => cursor.temporal_at.ok_or_else(source_changed)?,
            None => chrono::Utc::now(),
        })
    } else {
        None
    };
    if let Some(cursor) = &resume {
        if let Some(search) = &request.search {
            let index = app
                .explorations
                .lock()
                .unwrap()
                .search_index(cursor.session);
            if let Some(index) = index {
                if index.versions != cursor.versions {
                    return Err(source_changed());
                }
                check_versions(
                    &engine,
                    options.clone(),
                    &request,
                    &index.sources,
                    &index.postgres,
                )
                .await?;
                return Ok(Json(index.search(search)).into_response());
            }
        }
        let saved = {
            let mut sessions = app.explorations.lock().unwrap();
            sessions.get(cursor.session).filter(|pages| {
                // A later page may not have been produced before a prior request timed out.
                pages.can_serve(if request.search.is_some() {
                    0
                } else {
                    cursor.page
                })
            })
        };
        if let Some(saved) = saved {
            if saved.versions != cursor.versions {
                return Err(source_changed());
            }
            check_page_versions(&engine, options.clone(), &request, &saved).await?;
            return render_exploration(
                &request,
                &app,
                &saved,
                &cursors,
                cursor.session,
                cursor.page,
                options.timeout,
            )
            .await;
        }
    } else if request.neighbors && !temporal_current {
        let existing = app.explorations.lock().unwrap().find(&signature);
        if let Some((session, saved)) =
            existing.filter(|(_, saved)| snapshot.is_some() || can_replay(&saved.sources))
        {
            // A new graph query may deliberately start over after a source change.
            if check_page_versions(&engine, options.clone(), &request, &saved)
                .await
                .is_ok()
            {
                return render_exploration(
                    &request,
                    &app,
                    &saved,
                    &cursors,
                    session,
                    0,
                    options.timeout,
                )
                .await;
            }
        }
    }
    if request.search.is_some()
        && let Some(cursor) = &mut resume
    {
        cursor.page = 0;
        cursor.anchor = None;
        cursor.history.clear();
    }
    let session = resume
        .as_ref()
        .map_or_else(Uuid::new_v4, |cursor| cursor.session);
    let index = resume.as_ref().map_or(0, |cursor| cursor.page);
    let labels = edges.into_iter().map(|(name, _)| name.clone()).collect();
    let neighbors = request.neighbors;
    let search = request.search.clone();
    let timeout = options.timeout;
    // Keep enough engine capacity for foreground queries while pages are filled in the background.
    let queue = if resume.is_some() {
        &app.explore_recovery
    } else {
        &app.explore_producers
    };
    let producer = Arc::clone(queue)
        .try_acquire_owned()
        .map_err(|_| Error::busy())?;
    let (send, ready) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let _producer = producer;
        let _selection = selection;
        let mut published = None;
        let result = Box::pin(read(
            &engine,
            &request,
            labels,
            signature,
            session,
            resume,
            options,
            snapshot.as_ref(),
            temporal_at,
            |pages: Arc<pages::Pages>| {
                published = Some(pages::Production(Arc::clone(&pages)));
                let _ = send.send(pages);
            },
        ))
        .await;
        if let Some(pages) = published {
            pages.0.complete(result.clone());
        }
        result
    });
    let result = match ready.await {
        Ok(pages) => pages,
        Err(_) => {
            task.await.map_err(|_| Error::storage())??;
            return Err(Error::storage());
        }
    };
    if neighbors {
        app.explorations
            .lock()
            .unwrap()
            .insert(session, Arc::clone(&result));
    }
    if let Some(search) = search {
        return run_search(&app, &result, &cursors, session, &search, timeout).await;
    }
    let mut page = result.response(&cursors, session, index).await?;
    if !neighbors {
        page.page_cursor = None;
    }
    Ok(Json(page).into_response())
}

async fn render_exploration(
    request: &ExploreRequest,
    app: &App,
    pages: &pages::Pages,
    cursors: &pages::Cursors,
    session: Uuid,
    index: usize,
    timeout: Duration,
) -> ApiResult<Response> {
    if let Some(search) = &request.search {
        run_search(app, pages, cursors, session, search, timeout).await
    } else {
        Ok(Json(pages.response(cursors, session, index).await?).into_response())
    }
}

async fn run_search(
    app: &App,
    pages: &pages::Pages,
    cursors: &pages::Cursors,
    session: Uuid,
    search: &ExploreSearch,
    timeout: Duration,
) -> ApiResult<Response> {
    let (results, index) =
        tokio::time::timeout(timeout, pages.build_search_index(cursors, session, search))
            .await
            .map_err(|_| query_error(anyhow::anyhow!("query deadline exceeded")))??;
    if let Some(index) = index {
        app.explorations
            .lock()
            .unwrap()
            .remember_search(session, index);
    }
    Ok(Json(results).into_response())
}

fn versions_digest(sources: &[SourceVersion]) -> [u8; 32] {
    Sha256::digest(
        serde_json::to_vec(&comparable_versions(sources)).expect("serializing source versions"),
    )
    .into()
}

fn can_replay(sources: &[SourceVersion]) -> bool {
    sources.iter().all(|source| {
        matches!(source.consistency, "PostgreSQL repeatable read") || is_comparable(source)
    })
}

fn is_comparable(source: &SourceVersion) -> bool {
    !source.version.is_empty()
        && matches!(
            source.consistency,
            "PostgreSQL relation content" | "pinned object" | "Delta version" | "Iceberg snapshot"
        )
}

fn source_changed() -> Error {
    Error::new(
        StatusCode::CONFLICT,
        "graph_sources_changed",
        "来源版本已变化，请重新运行原查询",
    )
}

fn check_expected(request: &ExploreRequest, sources: &[SourceVersion]) -> ApiResult<()> {
    for (source, version) in comparable_versions(sources) {
        if request
            .source_versions
            .get(&source)
            .is_some_and(|old| old != &version)
        {
            return Err(source_changed());
        }
    }
    Ok(())
}

async fn check_page_versions(
    engine: &crate::Engine,
    options: crate::QueryOptions,
    request: &ExploreRequest,
    saved: &pages::Pages,
) -> ApiResult<()> {
    check_versions(engine, options, request, &saved.sources, &saved.postgres).await
}

async fn check_versions(
    engine: &crate::Engine,
    options: crate::QueryOptions,
    request: &ExploreRequest,
    sources: &[SourceVersion],
    snapshots: &std::sync::Mutex<BTreeMap<String, String>>,
) -> ApiResult<()> {
    check_expected(request, sources)?;
    if request.csr_generation.is_some() {
        return Ok(());
    }
    let scope = engine.scope(options).await.map_err(query_error)?;
    let mut postgres = snapshots.lock().unwrap().clone();
    let unchanged = scope
        .run(engine.exploration_versions_match(&scope, sources, &mut postgres))
        .await
        .map_err(source_read_error)?;
    if !unchanged {
        return Err(source_changed());
    }
    *snapshots.lock().unwrap() = postgres;
    Ok(())
}

fn source_read_error(error: anyhow::Error) -> Error {
    if error.chain().any(|cause| {
        cause
            .downcast_ref::<object_store::Error>()
            .is_some_and(|error| {
                matches!(
                    error,
                    object_store::Error::NotFound { .. } | object_store::Error::Precondition { .. }
                )
            })
            || cause
                .downcast_ref::<tokio_postgres::Error>()
                .and_then(tokio_postgres::Error::code)
                .is_some_and(|code| {
                    *code == tokio_postgres::error::SqlState::UNDEFINED_TABLE
                        || *code == tokio_postgres::error::SqlState::UNDEFINED_COLUMN
                })
    }) {
        return source_changed();
    }
    query_error(error)
}

async fn read(
    engine: &crate::Engine,
    request: &ExploreRequest,
    edges: Vec<String>,
    signature: String,
    session: Uuid,
    resume: Option<pages::Cursor>,
    options: crate::QueryOptions,
    snapshot: Option<&super::super::cluster::csr::ExplorationSnapshot>,
    temporal_at: Option<chrono::DateTime<chrono::Utc>>,
    publish: impl FnOnce(Arc<pages::Pages>),
) -> ApiResult<()> {
    let catalog = &engine.catalog;
    let target = &request.element;
    let node = target.kind == Kind::Node;
    let keys = if node {
        catalog.nodes[&target.label].key.clone()
    } else {
        catalog.edges[&target.label].identity()
    };
    let labels = std::iter::once(target.label.clone())
        .chain(edges.iter().cloned())
        .collect::<BTreeSet<_>>();
    let spill_bytes = options.spill_bytes;
    let mut scope = engine.scope(options).await.map_err(query_error)?;
    if let Some(time) = temporal_at {
        Arc::get_mut(&mut scope)
            .expect("new exploration scope")
            .wall_time = time;
    }
    scope
        .run(async {
            let context = scope.exploration_session();
            let anchor = resume.as_ref().and_then(|cursor| cursor.anchor.as_ref());
            let exploration = crate::engine::Exploration {
                endpoints: edges
                    .iter()
                    .map(|label| {
                        let edge = &catalog.edges[label];
                        let mut endpoints = Vec::new();
                        if request.filter.direction.outgoing() && edge.from.node == target.label {
                            endpoints.push(edge.from.columns.as_slice());
                        }
                        if request.filter.direction.incoming() && edge.to.node == target.label {
                            endpoints.push(edge.to.columns.as_slice());
                        }
                        (label.clone(), endpoints)
                    })
                    .collect(),
                key: &target.key,
                start: anchor.map(|anchor| (anchor.label.as_str(), anchor.key.as_slice())),
            };
            let prepared = if let Some(snapshot) = snapshot {
                snapshot.prepare(&context, &scope, catalog, labels).await?
            } else {
                engine
                    .prepare_exploration(
                        &scope,
                        &context,
                        labels,
                        request.neighbors.then_some(&exploration),
                    )
                    .await?
            };
            if let Err(error) = check_expected(request, &prepared.versions) {
                return Ok(Err(error));
            }
            if let Some(cursor) = &resume {
                if snapshot.is_none() && !can_replay(&prepared.versions) {
                    return Ok(Err(Error::new(
                        StatusCode::CONFLICT,
                        "neighbor_session_expired",
                        "此数据源没有可验证的版本，缓存过期后请重新运行原查询",
                    )));
                }
                if versions_digest(&prepared.versions) != cursor.versions {
                    return Ok(Err(source_changed()));
                }
            }
            let mut parameters = Vec::new();
            let selected_table = context
                .table(TableReference::bare(target.label.as_str()))
                .await?;
            let terms = exploration_key_match(
                "n",
                &keys,
                &target.key,
                selected_table.schema().inner(),
                &mut parameters,
            )?;
            let sql = format!(
                "SELECT n.* FROM {} AS n WHERE {terms} LIMIT 1",
                ident(&target.label)
            );
            let selected = context
                .sql(&sql)
                .await?
                .with_param_values(parameters)?
                .collect()
                .await?
                .iter()
                .find(|batch| batch.num_rows() > 0)
                .and_then(|batch| {
                    graph::from_fields(catalog, &target.label, node, 0, |name| {
                        batch.column_by_name(name)
                    })
                });
            let postgres = prepared
                .postgres
                .iter()
                .filter_map(|(name, table)| {
                    Some((name.clone(), table.exploration_snapshot()?.to_owned()))
                })
                .collect();
            let found = selected.is_some();
            let first_page = resume
                .as_ref()
                .filter(|cursor| cursor.anchor.is_some())
                .map_or(0, |cursor| cursor.page);
            let pages = pages::Pages::new(
                signature,
                temporal_at,
                prepared.versions,
                postgres,
                selected,
                first_page,
                anchor.cloned(),
                resume
                    .as_ref()
                    .filter(|cursor| cursor.anchor.is_some())
                    .map(|cursor| cursor.history.clone())
                    .unwrap_or_default(),
            )?;
            let mut writer = pages::Writer::new(Arc::clone(&pages), spill_bytes);
            publish(pages);
            if found {
                for label in &edges {
                    if anchor.is_some_and(|anchor| label < &anchor.label) {
                        continue;
                    }
                    let edge = &catalog.edges[label];
                    let start = anchor.filter(|anchor| label == &anchor.label);
                    let mut ordinal = start.map_or(0, |anchor| anchor.ordinal.saturating_sub(1));
                    let (sql, values) = match neighbor_sql(
                        &context,
                        catalog,
                        label,
                        edge,
                        target,
                        &request.filter,
                        start,
                        snapshot.is_some(),
                    )
                    .await?
                    {
                        Ok(query) => query,
                        Err(error) => return Ok(Err(error)),
                    };
                    let mut batches = context
                        .sql(&sql)
                        .await?
                        .with_param_values(values)?
                        .execute_stream()
                        .await?;
                    while let Some(batch) = batches.try_next().await? {
                        for row in 0..batch.num_rows() {
                            scope.check()?;
                            ordinal += 1;
                            let Some(from) =
                                from_alias(catalog, &edge.from.node, true, "a", &batch, row)
                            else {
                                writer.mark_truncated();
                                continue;
                            };
                            let Some(to) =
                                from_alias(catalog, &edge.to.node, true, "b", &batch, row)
                            else {
                                writer.mark_truncated();
                                continue;
                            };
                            let mut relation = if edge.key.is_empty() && snapshot.is_none() {
                                // Only indistinguishable parallel edges can swap positions
                                // when replaying the sort over all mapped values.
                                graph::from_identity(
                                    catalog,
                                    Identity {
                                        label: label.clone(),
                                        kind: Kind::Edge,
                                        key: vec![format!("{session}:{ordinal}")],
                                    },
                                    row,
                                    |name| batch.column_by_name(&format!("e:{name}")),
                                )
                            } else if let Some(edge) =
                                from_alias(catalog, label, false, "e", &batch, row)
                            {
                                edge
                            } else {
                                writer.mark_truncated();
                                continue;
                            };
                            relation.endpoints(&from.identity, &to.identity, true);
                            if writer.push(from, to, relation, ordinal)? {
                                tokio::task::yield_now().await;
                            }
                        }
                    }
                }
            }
            writer.finish()?;
            Ok(Ok(()))
        })
        .await
        .map_err(|error| {
            if error.to_string() == "neighbor page budget exceeded" {
                Error::invalid(
                    "neighbor_page_limit",
                    "邻域超过临时结果预算，请缩小筛选范围",
                )
            } else {
                source_read_error(error)
            }
        })?
}

async fn neighbor_sql(
    context: &SessionContext,
    catalog: &crate::Catalog,
    label: &str,
    edge: &crate::catalog::Edge,
    target: &Identity,
    filter: &NeighborFilter,
    start_at: Option<&pages::Anchor>,
    frozen: bool,
) -> anyhow::Result<ApiResult<(String, Vec<ScalarValue>)>> {
    let schema = context.table(TableReference::bare(label)).await?;
    let mut values = Vec::new();
    let mut endpoints = Vec::new();
    for (enabled, endpoint) in [
        (filter.direction.outgoing(), &edge.from),
        (filter.direction.incoming(), &edge.to),
    ] {
        if enabled && endpoint.node == target.label {
            endpoints.push(exploration_key_match(
                "e",
                &endpoint.columns,
                &target.key,
                schema.schema().inner(),
                &mut values,
            )?);
        }
    }
    let endpoints = endpoints.join(" OR ");
    let mut predicates = vec![format!("({endpoints})")];
    let identity = edge.identity();
    for predicate in &filter.properties {
        let column = qualified("e", &predicate.column);
        let operator = match predicate.op {
            Comparison::IsNull => {
                predicates.push(format!("{column} IS NULL"));
                continue;
            }
            Comparison::IsNotNull => {
                predicates.push(format!("{column} IS NOT NULL"));
                continue;
            }
            Comparison::Eq => "=",
            Comparison::Ne => "<>",
            Comparison::Lt => "<",
            Comparison::Le => "<=",
            Comparison::Gt => ">",
            Comparison::Ge => ">=",
        };
        let ty = schema
            .schema()
            .inner()
            .field_with_name(&predicate.column)?
            .data_type();
        let value = predicate
            .value
            .as_str()
            .map_or_else(|| predicate.value.to_string(), str::to_owned);
        let value = match ScalarValue::try_from_string(value, ty) {
            Ok(value) if !value.is_null() => value,
            _ => return Ok(Err(invalid_filter("属性筛选值与映射列的类型不匹配"))),
        };
        values.push(value);
        predicates.push(format!("{column} {operator} ${}", values.len()));
    }
    let mut source_columns = edge.source_columns();
    if frozen && edge.key.is_empty() {
        source_columns.extend(identity.iter().cloned());
    }
    let order = if edge.key.is_empty() && !frozen {
        &source_columns
    } else {
        &identity
    };
    let sort: Vec<_> = order.iter().map(|key| qualified("e", key)).collect();
    let mut offset = 0;
    if let Some(anchor) = start_at {
        let seekable = !frozen
            && !edge.key.is_empty()
            && anchor.key.len() == edge.key.len()
            && edge.key.iter().all(|key| {
                schema
                    .schema()
                    .inner()
                    .field_with_name(key)
                    .is_ok_and(|field| crate::engine::exploration_seek_type(field.data_type()))
            });
        if !seekable {
            offset = anchor.ordinal.saturating_sub(1);
        }
    }
    let start = &catalog.nodes[&edge.from.node];
    let end = &catalog.nodes[&edge.to.node];
    let columns = [
        columns("e", &source_columns),
        columns(
            "a",
            &crate::catalog::columns([&start.key, &start.properties]),
        ),
        columns("b", &crate::catalog::columns([&end.key, &end.properties])),
    ]
    .join(", ");
    let sql = format!(
        "SELECT {columns} FROM {} AS e JOIN {} AS a ON {} JOIN {} AS b ON {} WHERE {} ORDER BY {} OFFSET {offset}",
        ident(label),
        ident(&edge.from.node),
        equal_keys("e", &edge.from.columns, "a", &start.key),
        ident(&edge.to.node),
        equal_keys("e", &edge.to.columns, "b", &end.key),
        predicates.join(" AND "),
        sort.join(", ")
    );
    Ok(Ok((sql, values)))
}

pub(super) async fn connect_returned_edges(
    catalog: &crate::Catalog,
    result: &mut QueryResult,
    graph: &mut Collector,
    capture_graph: bool,
) {
    let Some(context) = result.graph_context.take() else {
        return;
    };
    let csr = result.graph_csr.take();
    let indexed = csr.as_ref().map(|_| {
        graph
            .unresolved_properties()
            .map(|id| crate::cypher::format_element_id(&id.label, &id.key))
            .collect::<BTreeSet<_>>()
    });
    let lookups = if indexed.is_none() {
        graph.unresolved_edges()
    } else {
        BTreeMap::new()
    };
    if indexed.as_ref().is_none_or(BTreeSet::is_empty) && lookups.is_empty() {
        return;
    }
    let scope = result.graph_scope();
    // This uses the modeled relations of the original query and its source snapshots.
    // A slow optional graph enrichment must not turn a completed table query into an error.
    let hydrate = result.graph_read(async {
        if let (Some(csr), Some(ids)) = (csr, indexed) {
            csr.visit_properties(&scope, ids, |label, _, batch, row| {
                if let Some(item) = graph::from_fields(
                    catalog,
                    label,
                    catalog.nodes.contains_key(label),
                    row,
                    |name| batch.column_by_name(name),
                ) {
                    graph.add_endpoints(&item, catalog, batch, row);
                    graph.insert(item);
                }
                Ok(())
            })
            .await?;
            return Ok(());
        }
        for (label, keys) in lookups {
            let Some(edge) = catalog.edges.get(&label) else {
                continue;
            };
            let fields = edge.identity();
            let width = fields.len();
            let schema = context
                .table_provider(TableReference::bare(label.as_str()))
                .await?
                .schema();
            for group in keys.chunks((512 / width).max(1)) {
                let mut parameters = Vec::with_capacity(group.len() * width);
                let filter = group
                    .iter()
                    .map(|key| exploration_key_match("e", &fields, key, &schema, &mut parameters))
                    .collect::<anyhow::Result<Vec<_>>>()?
                    .join(" OR ");
                let projection = crate::catalog::columns([
                    &fields,
                    &edge.from.columns,
                    &edge.to.columns,
                    &edge.properties,
                ]);
                let projection = projection
                    .iter()
                    .map(|field| qualified("e", field))
                    .collect::<Vec<_>>()
                    .join(", ");
                let sql = format!(
                    "SELECT {projection} FROM {} AS e WHERE {filter}",
                    ident(&label)
                );
                let mut batches = context
                    .sql(&sql)
                    .await?
                    .with_param_values(parameters)?
                    .execute_stream()
                    .await?;
                while let Some(batch) = batches.try_next().await? {
                    for row in 0..batch.num_rows() {
                        if let Some(item) =
                            graph::from_fields(catalog, &label, false, row, |name| {
                                batch.column_by_name(name)
                            })
                        {
                            graph.add_endpoints(&item, catalog, &batch, row);
                            graph.insert(item);
                        }
                    }
                }
            }
        }
        Ok(())
    });
    let connected = if capture_graph {
        hydrate.await.is_ok()
    } else {
        matches!(
            tokio::time::timeout(Duration::from_millis(750), hydrate).await,
            Ok(Ok(()))
        )
    };
    if !connected {
        graph.mark_truncated();
    }
}

pub(in crate::admin) fn exploration_key_match(
    alias: &str,
    fields: &[String],
    keys: &[String],
    schema: &crate::arrow::datatypes::Schema,
    values: &mut Vec<ScalarValue>,
) -> anyhow::Result<String> {
    let mut tests = Vec::with_capacity(fields.len());
    let mut bound = Vec::with_capacity(fields.len());
    for (field, key) in fields.iter().zip(keys) {
        let mut operand = qualified(alias, field);
        let ty = schema.field_with_name(field)?.data_type();
        let value = if ty.is_integer() {
            let Ok(value) = ScalarValue::try_from_string(key.clone(), ty) else {
                return Ok("FALSE".into());
            };
            if value.to_string() != *key {
                return Ok("FALSE".into());
            }
            value
        } else if matches!(
            ty,
            DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8
        ) {
            ScalarValue::try_from_string(key.clone(), ty)?
        } else {
            operand = format!("CAST({operand} AS VARCHAR)");
            ScalarValue::Utf8(Some(key.clone()))
        };
        bound.push(value);
        tests.push(format!("{operand} = ${}", values.len() + bound.len()));
    }
    values.extend(bound);
    Ok(format!("({})", tests.join(" AND ")))
}

fn columns(alias: &str, fields: &[String]) -> String {
    fields
        .iter()
        .map(|field| {
            format!(
                "{} AS {}",
                qualified(alias, field),
                ident(&format!("{alias}:{field}"))
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn from_alias(
    catalog: &crate::Catalog,
    label: &str,
    node: bool,
    alias: &str,
    batch: &crate::arrow::array::RecordBatch,
    row: usize,
) -> Option<Element> {
    graph::from_fields(catalog, label, node, row, |name| {
        batch.column_by_name(&format!("{alias}:{name}"))
    })
}
