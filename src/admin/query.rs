use std::{fmt, sync::Arc, time::Duration};

pub(super) mod download;
pub(super) mod explore;
pub(super) mod graph;
pub(super) use explore::{Sessions, explore};
pub(super) mod paths;
pub(super) use download::{Download, ResultSnapshot, download, export, graph_page, snapshot};

use axum::{
    Extension, Json,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use tokio::sync::OwnedSemaphorePermit;
use uuid::Uuid;

use crate::{
    CancellationToken, Language, QueryOptions, QueryResult, SourceVersion,
    arrow::util::display::{ArrayFormatter, FormatOptions},
};

use super::{
    ApiResult, App, Error,
    access::{self, AuditModel, Principal},
};

const MAX_ROWS: usize = 500;
const MAX_BYTES: usize = 1024 * 1024;

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub(super) enum Dialect {
    Cypher,
    Sql,
    Gremlin,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Request {
    pub(super) request_id: String,
    pub(super) model_revision: String,
    pub(super) timeout_ms: Option<u64>,
    #[serde(default)]
    language: Option<Dialect>,
    #[serde(default)]
    pub(super) query: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) csr: Option<super::cluster::csr::Request>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) csr_generation: Option<Uuid>,
    parameters_json: Option<String>,
    graph_options: Option<GraphScope>,
    #[serde(default)]
    capture_result: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub(super) preview_without_arrow: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) tenant_partition: Option<crate::Predicate>,

    #[serde(default)]
    capture_graph: bool,
}

#[derive(Deserialize, Serialize)]
#[serde(untagged)]
enum GraphScope {
    Options(crate::GraphOptions),
    Json(String),
}

#[derive(Serialize)]
struct Column {
    name: String,
    data_type: String,
}

#[derive(Serialize)]
pub(super) struct ResultPage {
    model_revision: String,
    paths: Vec<paths::PathPreview>,
    graph: graph::Fragment,
    graph_options: crate::GraphOptions,
    columns: Vec<Column>,
    rows: Vec<Vec<Option<String>>>,
    sources: Vec<SourceVersion>,
    elapsed_ms: u64,
    truncated: bool,
    snapshot: Option<Uuid>,
    snapshot_complete: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    graph_next: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    graph_snapshot: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    execution_worker: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    distributed: Option<super::cluster::distributed::Stats>,
    #[serde(skip_serializing_if = "Option::is_none")]
    csr_generation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    csr_stats: Option<super::cluster::csr::Stats>,
}

pub(super) struct Preview {
    pub(super) page: ResultPage,
    pub(super) arrow: Option<download::Captured>,
    pub(super) graph: Option<(Vec<graph::Fragment>, bool)>,
}

pub(super) struct ActiveQuery {
    pub(super) running: bool,
    token: CancellationToken,
    model: String,
    principal: Principal,
}

pub(super) struct Active {
    app: Arc<App>,
    id: Uuid,
    pub(super) token: CancellationToken,
    _permit: Option<OwnedSemaphorePermit>,
}

impl Drop for Active {
    fn drop(&mut self) {
        self.token.cancel();
        self.app.active.lock().unwrap().remove(&self.id);
    }
}

fn set_timeout(options: &mut crate::QueryOptions, timeout_ms: Option<u64>) -> ApiResult<()> {
    if let Some(ms) = timeout_ms {
        if !(1..=60_000).contains(&ms) {
            return Err(Error::invalid(
                "invalid_timeout",
                "超时必须为 1 至 60000 毫秒",
            ));
        }
        options.timeout = Duration::from_millis(ms);
    }
    Ok(())
}

fn register(app: &Arc<App>, id: Uuid, model: &str, principal: &Principal) -> ApiResult<Active> {
    let mut active = app.active.lock().unwrap();
    if active.contains_key(&id) {
        return Err(Error::new(
            StatusCode::CONFLICT,
            "duplicate_request_id",
            "查询请求标识已在使用",
        ));
    }
    let token = app.shutdown.child_token();
    active.insert(
        id,
        ActiveQuery {
            running: false,
            token: token.clone(),
            model: model.into(),
            principal: principal.clone(),
        },
    );
    Ok(Active {
        app: app.clone(),
        id,
        token,
        _permit: None,
    })
}

pub(super) async fn admit(
    app: &Arc<App>,
    id: Uuid,
    model: &str,
    principal: &Principal,
    options: &mut crate::QueryOptions,
) -> ApiResult<Active> {
    let mut active = register(app, id, model, principal)?;
    active.acquire(options).await?;
    Ok(active)
}

impl Active {
    async fn acquire(&mut self, options: &mut crate::QueryOptions) -> ApiResult<()> {
        let started = tokio::time::Instant::now();
        let app = &self.app;
        let permit = acquire(
            app,
            &app.work,
            &app.query_waiters,
            options.timeout,
            &self.token,
        )
        .await;
        app.metrics.observe(
            "internal",
            "query_slot",
            permit
                .as_ref()
                .map_or_else(|error| error.status.as_u16(), |_| 200),
            started.elapsed(),
        );
        let permit = permit?;
        options.timeout = remaining(options.timeout, started)?;
        options.cancellation = self.token.clone();
        app.active
            .lock()
            .unwrap()
            .get_mut(&self.id)
            .expect("registered query")
            .running = true;
        self._permit = Some(permit);
        Ok(())
    }
}

pub(super) fn remaining(timeout: Duration, started: tokio::time::Instant) -> ApiResult<Duration> {
    let remaining = timeout.saturating_sub(started.elapsed());
    if remaining.is_zero() {
        Err(Error::new(
            StatusCode::GATEWAY_TIMEOUT,
            "timeout",
            "查询或校验超时",
        ))
    } else {
        Ok(remaining)
    }
}

pub(super) async fn acquire(
    app: &App,
    slots: &Arc<tokio::sync::Semaphore>,
    waiters: &tokio::sync::Semaphore,
    timeout: Duration,
    token: &CancellationToken,
) -> ApiResult<OwnedSemaphorePermit> {
    let draining = || Error::new(StatusCode::SERVICE_UNAVAILABLE, "draining", "服务正在停止");
    if app.shutdown.is_cancelled() {
        return Err(draining());
    }
    if let Ok(slot) = slots.clone().try_acquire_owned() {
        return Ok(slot);
    }
    let _waiting = waiters.try_acquire().map_err(|_| Error::busy())?;
    tokio::select! {
        biased;
        _ = app.shutdown.cancelled() => Err(draining()),
        _ = token.cancelled() => Err(Error::new(StatusCode::CONFLICT, "cancelled", "查询已取消")),
        result = tokio::time::timeout(timeout.min(Duration::from_millis(500)), slots.clone().acquire_owned()) =>
            match result {
                Ok(Ok(permit)) => Ok(permit),
                Ok(Err(_)) => Err(draining()),
                Err(_) if timeout <= Duration::from_millis(500) =>
                    Err(Error::new(StatusCode::GATEWAY_TIMEOUT, "timeout", "查询或校验超时")),
                Err(_) => Err(Error::busy()),
            },
    }
}

pub(super) fn validate_request(
    request: &Request,
    mut options: QueryOptions,
) -> ApiResult<(Uuid, Language, QueryOptions)> {
    let request_id = Uuid::parse_str(&request.request_id)
        .map_err(|_| Error::invalid("invalid_request_id", "查询请求标识必须是 UUID"))?;
    if request.csr_generation.is_some()
        && (request.csr.is_some()
            || !matches!(request.language, Some(Dialect::Cypher))
            || request.graph_options.is_some())
    {
        return Err(Error::invalid(
            "invalid_csr_scope",
            "CSR 代次适用于普通 Cypher，图范围由代次确定",
        ));
    }
    if request.csr.is_some()
        && (!request.query.trim().is_empty()
            || request.parameters_json.is_some()
            || request.graph_options.is_some())
    {
        return Err(Error::invalid(
            "invalid_query",
            "CSR 查询使用发布代次及其图范围，请省略语句、参数和图范围",
        ));
    }
    if request.csr.is_none() && (request.query.trim().is_empty() || request.query.len() > 64 * 1024)
    {
        return Err(Error::invalid(
            "invalid_query",
            "查询语句需为 1 字节至 64 KiB",
        ));
    }
    let language = match request.language {
        Some(Dialect::Cypher) => Language::Cypher,
        Some(Dialect::Sql) => Language::Sql,
        Some(Dialect::Gremlin) => Language::Gremlin,
        None if request.csr.is_some() => Language::Cypher,
        None => return Err(Error::invalid("invalid_language", "查询必须指定语言")),
    };
    set_timeout(&mut options, request.timeout_ms)?;
    if let Some(graph) = &request.graph_options {
        let graph = match graph {
            GraphScope::Options(graph) => graph.clone(),
            GraphScope::Json(text) => crate::GraphOptions::from_json(text)
                .map_err(|_| Error::invalid("invalid_graph_options", "图查询范围无效"))?,
        };
        graph
            .validate()
            .map_err(|_| Error::invalid("invalid_graph_options", "图查询范围无效"))?;
        options.graph = graph;
    }
    if let Some(params) = &request.parameters_json {
        if !matches!(language, Language::Cypher) || params.len() > 64 * 1024 {
            return Err(Error::invalid(
                "invalid_parameters",
                "仅 Cypher 接受参数，参数上限为 64 KiB",
            ));
        }
        options = options
            .with_parameters_json(params)
            .map_err(|_| Error::invalid("invalid_parameters", "Cypher 参数必须是 JSON 对象"))?;
    }
    Ok((request_id, language, options))
}

async fn start(
    app: &Arc<App>,
    id: &str,
    request: &Request,
    principal: &Principal,
    preview: bool,
) -> ApiResult<(
    Active,
    QueryResult,
    crate::Engine,
    Option<OwnedSemaphorePermit>,
)> {
    if request.csr.is_some() || request.csr_generation.is_some() {
        return Err(Error::not_found());
    }
    let (request_id, language, mut options) = validate_request(request, app.options.clone())?;
    let mut active = register(app, request_id, id, principal)?;
    let export = if preview {
        None
    } else {
        let started = tokio::time::Instant::now();
        let permit = acquire(
            app,
            &app.export_work,
            &app.export_waiters,
            options.timeout,
            &active.token,
        )
        .await?;
        options.timeout = remaining(options.timeout, started)?;
        Some(permit)
    };
    active.acquire(&mut options).await?;
    let (_, _, engine) = app
        .query_model(id, &request.model_revision, principal)
        .await?;
    if let Some(term) = access::partition(app, principal, id)? {
        super::users::apply_partition(&engine.catalog, &mut options.graph, &term)?;
    }
    let result = if preview {
        Box::pin(engine.preview_query(language, &request.query, options)).await
    } else {
        Box::pin(engine.query(language, &request.query, options)).await
    }
    .map_err(query_error)?;
    Ok((active, result, engine, export))
}

pub(super) async fn run(
    State(app): State<Arc<App>>,
    Path(id): Path<String>,
    Extension(principal): Extension<Principal>,
    Json(mut request): Json<Request>,
) -> ApiResult<Response> {
    request.preview_without_arrow = access::check_export(&app, &principal, &id).is_err();
    if request.capture_result && request.preview_without_arrow {
        return Err(access::forbidden());
    }
    if let Some(cluster) = &app.cluster {
        return cluster.execute(&app, id, principal, request, false).await;
    }
    let (active, result, engine, _) = start(&app, &id, &request, &principal, true).await?;
    let snapshot = Uuid::new_v4();
    let options = QueryOptions {
        cancellation: active.token.clone(),
        ..app.options.clone()
    };
    let Preview { page, arrow, graph } = preview(
        result,
        &engine.catalog,
        &request,
        &options,
        snapshot,
        None,
        None,
        None,
        None,
    )
    .await?;
    if arrow.is_some() || graph.is_some() {
        download::save_snapshot(
            &app,
            snapshot,
            id,
            page.model_revision.clone(),
            principal,
            arrow,
            graph,
        );
    }
    Ok(Json(page).into_response())
}

pub(super) async fn preview(
    mut result: QueryResult,
    catalog: &crate::Catalog,
    request: &Request,
    options: &QueryOptions,
    snapshot: Uuid,
    worker: Option<Uuid>,
    distributed: Option<super::cluster::distributed::Stats>,
    csr_stats: Option<super::cluster::csr::Stats>,
    cypher_csr: Option<&super::cluster::csr::CypherCsr>,
) -> ApiResult<Preview> {
    let cancellation = &options.cancellation;
    let mut capture = if request.preview_without_arrow {
        None
    } else if request.capture_result {
        Some(download::Capture::new(
            &result.batches.schema(),
            options.spill_bytes,
        )?)
    } else {
        download::Capture::new(&result.batches.schema(), 16 * 1024 * 1024).ok()
    };
    let mut page = ResultPage {
        model_revision: request.model_revision.clone(),
        paths: Vec::new(),
        graph: graph::Fragment::default(),
        graph_options: result.graph_options().clone(),
        columns: result
            .batches
            .schema()
            .fields()
            .iter()
            .map(|field| Column {
                name: field.name().clone(),
                data_type: field.data_type().to_string(),
            })
            .collect(),
        rows: Vec::new(),
        sources: result.versions.clone(),
        elapsed_ms: 0,
        truncated: false,
        snapshot: Some(snapshot),
        snapshot_complete: false,
        graph_next: request.capture_graph.then_some(usize::MAX),
        graph_snapshot: request.capture_graph.then_some(snapshot),
        execution_worker: worker,
        distributed,
        csr_generation: result.metrics().csr_generation,
        csr_stats: cypher_csr
            .map(|csr| csr.stats(&result.metrics()))
            .or(csr_stats),
    };
    // Reserve the largest possible elapsed-time field before adding rows.
    page.elapsed_ms = u64::MAX;
    let mut encoded_size = serde_json::to_vec(&page)
        .map_err(|_| Error::storage())?
        .len();
    if encoded_size > MAX_BYTES {
        return Err(Error::invalid("result_limit", "结果列和来源信息超过 1 MiB"));
    }
    let format = FormatOptions::default();
    let capture_graph = request.capture_graph
        && (!result.graph_output.columns.is_empty() || result.graph_output.row_entity.is_some());
    let mut graph = if capture_graph {
        graph::Collector::bounded(12_000, (8 * 1024 * 1024).min(options.memory_bytes / 8))
    } else {
        graph::Collector::default()
    };
    while let Some(batch) = result
        .batches
        .try_next()
        .await
        .map_err(|error| query_error(error.into()))?
    {
        if request.capture_result {
            capture.as_mut().expect("full capture").write(&batch)?;
        }
        if capture_graph && !graph.saturated() {
            for index in 0..batch.num_rows() {
                graph.row(catalog, &result.graph_output, &batch, index);
                if graph.saturated() {
                    break;
                }
            }
        }
        if page.truncated {
            if !request.capture_result && (!capture_graph || graph.saturated()) {
                break;
            }
            continue;
        }
        let before = page.rows.len();
        let path_columns = batch
            .schema_ref()
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(i, field)| {
                let crate::engine::query_graph::GraphColumn::Path { labels } =
                    result.graph_output.columns.get(field.name())?
                else {
                    return None;
                };
                Some((i, field.name(), labels.as_ref()))
            })
            .collect::<Vec<_>>();
        let formatters = batch
            .columns()
            .iter()
            .map(|column| ArrayFormatter::try_new(column.as_ref(), &format))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| formatting_error())?;
        for index in 0..batch.num_rows() {
            if page.rows.len() == MAX_ROWS {
                page.truncated = true;
                break;
            }
            let mut remaining = MAX_BYTES - encoded_size;
            let mut row = Vec::with_capacity(formatters.len());
            'cells: for (column, formatter) in batch.columns().iter().zip(&formatters) {
                if column.is_null(index) {
                    row.push(None);
                    continue;
                }
                let mut text = CellText {
                    value: String::new(),
                    limit: remaining,
                    exceeded: false,
                };
                if formatter.value(index).write(&mut text).is_err() {
                    if text.exceeded {
                        page.truncated = true;
                        break 'cells;
                    }
                    return Err(formatting_error());
                }
                remaining -= text.value.len();
                row.push(Some(text.value));
            }
            if page.truncated {
                break;
            }
            let size = serde_json::to_vec(&row)
                .map_err(|_| formatting_error())?
                .len()
                + usize::from(!page.rows.is_empty());
            if encoded_size + size > MAX_BYTES {
                page.truncated = true;
                break;
            }
            encoded_size += size;
            if page.paths.len() < 30 {
                for &(i, name, labels) in &path_columns {
                    if page.paths.len() == 30 {
                        break;
                    }
                    if let Some(path) = paths::preview(
                        batch.column(i).as_ref(),
                        index,
                        page.rows.len(),
                        name,
                        labels,
                    ) {
                        let size = serde_json::to_vec(&path)
                            .map_err(|_| formatting_error())?
                            .len()
                            + usize::from(!page.paths.is_empty());
                        // Prefer table rows over the optional visual preview.
                        if size <= 4096 && encoded_size + size + 65_536 <= MAX_BYTES {
                            encoded_size += size;
                            page.paths.push(path);
                        }
                    }
                }
            }
            if !capture_graph {
                graph.row(catalog, &result.graph_output, &batch, index);
            }
            page.rows.push(row);
        }
        let added = page.rows.len() - before;
        if !request.capture_result
            && added > 0
            && let Some(writer) = &mut capture
            && writer.write(&batch.slice(0, added)).is_err()
        {
            capture = None;
        }
        if page.truncated && !request.capture_result && (!capture_graph || graph.saturated()) {
            break;
        }
    }
    if cancellation.is_cancelled() {
        return Err(Error::new(StatusCode::CONFLICT, "cancelled", "查询已取消"));
    }
    let graph_started = std::time::Instant::now();
    page.elapsed_ms = result.metrics().elapsed_ms;
    explore::connect_returned_edges(catalog, &mut result, &mut graph, capture_graph).await;
    super::ops::observe_query_io(&result);
    if cancellation.is_cancelled() {
        return Err(Error::new(StatusCode::CONFLICT, "cancelled", "查询已取消"));
    }
    if let Some(csr) = cypher_csr {
        let old = serde_json::to_vec(&page.csr_stats)
            .map_err(|_| formatting_error())?
            .len();
        page.csr_stats = Some(csr.stats(&result.metrics()));
        let new = serde_json::to_vec(&page.csr_stats)
            .map_err(|_| formatting_error())?
            .len();
        encoded_size = encoded_size - old + new;
        if encoded_size > MAX_BYTES {
            return Err(Error::invalid("result_limit", "结果超过 1 MiB"));
        }
    }
    page.elapsed_ms += graph_started.elapsed().as_millis() as u64;
    let mut graph = graph.finish();
    graph.prune_missing_endpoints();
    let (graph_pages, graph_incomplete) = if capture_graph {
        let (pages, incomplete) = graph::result_pages(graph);
        graph = pages.first().cloned().unwrap_or_default();
        (pages, incomplete)
    } else {
        (Vec::new(), graph.truncated)
    };
    let mut graph_bytes = serde_json::to_vec(&graph)
        .map_err(|_| formatting_error())?
        .len();
    let allowance = MAX_BYTES - encoded_size
        + serde_json::to_vec(&page.graph)
            .map_err(|_| formatting_error())?
            .len();
    let needs_first_page = graph_bytes > allowance;
    while graph_bytes > allowance {
        let removed = if let Some(step) = graph.steps.pop() {
            serde_json::to_vec(&step)
        } else if let Some(item) = graph.elements.pop() {
            serde_json::to_vec(&item)
        } else {
            break;
        };
        graph_bytes -= removed.map_err(|_| formatting_error())?.len();
        graph.truncated = true;
    }
    graph.prune_missing_endpoints();
    page.graph_next = if !graph_pages.is_empty() && needs_first_page {
        Some(0)
    } else if graph_pages.len() > 1 {
        Some(1)
    } else {
        None
    };
    page.graph_snapshot = page.graph_next.map(|_| snapshot);
    graph.truncated |= page.graph_next.is_some() || graph_incomplete;
    page.graph = graph;
    let saved = capture.map(download::Capture::finish).transpose();
    let saved = if request.capture_result {
        saved?
    } else {
        saved.ok().flatten()
    };
    page.snapshot_complete = saved.is_some() && (request.capture_result || !page.truncated);
    if saved.is_none() {
        page.snapshot = None;
    }
    let graph = page.graph_next.map(|_| (graph_pages, graph_incomplete));
    Ok(Preview {
        page,
        arrow: saved,
        graph,
    })
}

struct CellText {
    value: String,
    limit: usize,
    exceeded: bool,
}

impl fmt::Write for CellText {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        if text.len() > self.limit - self.value.len() {
            self.exceeded = true;
            return Err(fmt::Error);
        }
        self.value.push_str(text);
        Ok(())
    }
}

fn formatting_error() -> Error {
    Error::invalid("format_result", "无法显示结果中的列值")
}

pub(super) async fn cancel(
    State(app): State<Arc<App>>,
    Path(id): Path<String>,
    Extension(principal): Extension<Principal>,
) -> ApiResult<impl IntoResponse> {
    let id = Uuid::parse_str(&id).map_err(|_| Error::not_found())?;
    if let Some(cluster) = &app.cluster
        && let Some(model) = cluster.cancel(&app, &principal, id).await?
    {
        return Ok((Extension(AuditModel(model)), StatusCode::NO_CONTENT));
    }
    let model = {
        let active = app.active.lock().unwrap();
        let query = active
            .get(&id)
            .filter(|q| access::owns_query(&principal, &q.principal))
            .ok_or_else(Error::not_found)?;
        crate::admin::access::check_query(&app, &principal, &query.model)?;
        query.token.cancel();
        query.model.clone()
    };
    if let Some(cluster) = &app.cluster {
        // The request can acquire its shared job between the first lookup and local cancellation.
        cluster.cancel(&app, &principal, id).await?;
    }
    Ok((Extension(AuditModel(model)), StatusCode::NO_CONTENT))
}

pub(super) fn query_error(error: anyhow::Error) -> Error {
    let message = explain_error(&error);
    if super::cluster::distributed::snapshot_changed(&error) {
        return Error::new(
            StatusCode::CONFLICT,
            "graph_sources_changed",
            "Parquet 来源已变化，无法继续原查询",
        );
    }
    if error.chain().any(|cause| {
        cause
            .to_string()
            .contains("distributed shuffle exceeds query spill budget")
    }) {
        return Error::new(
            StatusCode::INSUFFICIENT_STORAGE,
            "shuffle_budget",
            "分布式查询超出 Shuffle 溢写上限",
        );
    }
    if message == "查询已取消" {
        return Error::new(StatusCode::CONFLICT, "cancelled", message);
    }
    if message == "查询或校验超时" {
        return Error::new(StatusCode::GATEWAY_TIMEOUT, "timeout", message);
    }
    if error.chain().any(|cause| {
        cause
            .downcast_ref::<datafusion::common::DataFusionError>()
            .is_some_and(|error| {
                matches!(
                    error,
                    datafusion::common::DataFusionError::ResourcesExhausted(_)
                )
            })
    }) {
        return Error::new(
            StatusCode::INSUFFICIENT_STORAGE,
            "query_memory_budget",
            "查询超过内存预算",
        );
    }
    if error.chain().any(|cause| {
        cause
            .downcast_ref::<tokio_postgres::Error>()
            .is_some_and(|e| {
                e.is_closed()
                    || e.code().is_some_and(|code| {
                        code.code().starts_with("08")
                            || matches!(code.code(), "57P01" | "57P02" | "57P03")
                    })
            })
            || cause
                .downcast_ref::<object_store::Error>()
                .is_some_and(|e| matches!(e, object_store::Error::NotFound { .. }))
            || cause.downcast_ref::<std::io::Error>().is_some_and(|e| {
                matches!(
                    e.kind(),
                    std::io::ErrorKind::ConnectionRefused
                        | std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::ConnectionAborted
                        | std::io::ErrorKind::NotConnected
                        | std::io::ErrorKind::TimedOut
                        | std::io::ErrorKind::BrokenPipe
                        | std::io::ErrorKind::HostUnreachable
                        | std::io::ErrorKind::NetworkUnreachable
                )
            })
            || cause.downcast_ref::<reqwest::Error>().is_some_and(|e| {
                e.is_connect()
                    || e.is_timeout()
                    || e.status().is_some_and(|status| status.is_server_error())
            })
    }) {
        return Error::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "source_unavailable",
            message,
        );
    }
    Error::invalid("query_failed", message)
}

// Connector errors can include driver URLs and credentials. Return the engine's
// mapping diagnostics or its outer source context rather than the driver chain.
pub(super) fn explain_error(error: &anyhow::Error) -> String {
    for cause in error.chain() {
        let message = cause.to_string();
        if message.contains("query cancelled") {
            return "查询已取消".into();
        }
        if message.contains("query deadline exceeded") {
            return "查询或校验超时".into();
        }
        if message.starts_with("edge ") && message.contains(" has type ")
            || message.starts_with("column ") && message.contains(" has type ")
            || message.starts_with("unsupported business key type ")
        {
            return message;
        }
        if message.starts_with("duplicate business key in ")
            || message.starts_with("conflicting records for ")
            || message.starts_with("标签 ") && message.contains("存在空业务键")
            || message.starts_with("边 ") && message.contains("端点无法匹配")
        {
            return message;
        }
        if message.starts_with("missing column ")
            && let Some((column, _)) = message.split_once(" in ")
        {
            return column.to_owned();
        }
        if message.starts_with("missing PostgreSQL column in ")
            || message.starts_with("PostgreSQL ")
                && message.ends_with("expose the column as text in a PostgreSQL view")
            || message.starts_with("invalid Cypher: ")
            || message.starts_with("missing Cypher parameter $")
        {
            return message;
        }
        for (prefix, diagnostic) in [
            ("unsupported Cypher procedure ", "不支持的 Cypher 过程"),
            ("unsupported Cypher function ", "不支持的 Cypher 函数"),
            ("unknown property ", "图模型中没有此属性"),
        ] {
            if let Some(name) = message.strip_prefix(prefix) {
                return format!(
                    "{diagnostic} · {}",
                    name.chars().take(160).collect::<String>()
                );
            }
        }
        if message == "correlated row subquery supports MATCH, WITH, UNWIND, CALL and RETURN" {
            return "相关 EXISTS 和 COUNT 子查询支持 MATCH、WITH、UNWIND、CALL 和 RETURN".into();
        }
        if message.starts_with("query requires one consistent snapshot,") {
            return "所选来源无法共享同一个可验证快照。请调整来源或选择已发布的 CSR 代次。".into();
        }
        if message.starts_with("unknown export location ") {
            return "网页查询不允许写入服务器端导出位置".into();
        }
    }
    let outer = error.to_string();
    if outer.starts_with("opening ")
        || outer.starts_with("reading ")
        || outer.starts_with("binding graph label ")
    {
        return format!("数据源或字段校验失败（{outer}）");
    }
    "执行失败，请检查查询语句、映射字段或数据源连接".into()
}
