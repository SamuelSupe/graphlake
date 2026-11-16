use std::io::{BufWriter, Write};

use axum::{body::Body, http::header};
use tokio::sync::OwnedSemaphorePermit;
use tokio_util::io::ReaderStream;

use crate::arrow::ipc::writer::StreamWriter;

use super::*;

const SNAPSHOTS_PER_OWNER: usize = 4;
const SNAPSHOTS_TOTAL: usize = 32;
const DOWNLOAD_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const SNAPSHOT_BUDGET: u64 = 1024 * 1024 * 1024;
const SNAPSHOT_TTL: Duration = Duration::from_secs(30 * 60);
const TICKET_TTL: Duration = Duration::from_secs(60);

pub(in crate::admin) struct ResultSnapshot {
    pub(super) id: Uuid,
    model: String,
    revision: String,
    owner: Principal,
    file: Option<tempfile::NamedTempFile>,
    length: u64,
    created: std::time::Instant,
    graph: Option<(Vec<Arc<graph::Fragment>>, bool)>,
}

pub(super) struct Capture {
    file: tempfile::NamedTempFile,
    ipc: StreamWriter<Bounded>,
}

pub(in crate::admin) struct Captured {
    pub file: tempfile::NamedTempFile,
    pub length: u64,
}

impl Capture {
    pub(super) fn new(schema: &crate::arrow::datatypes::Schema, budget: u64) -> ApiResult<Self> {
        let file = tempfile::NamedTempFile::new().map_err(|_| write_error())?;
        let writer = Bounded {
            file: BufWriter::new(file.reopen().map_err(|_| write_error())?),
            remaining: budget.min(SNAPSHOT_BUDGET),
        };
        let ipc = StreamWriter::try_new(writer, schema).map_err(|_| write_error())?;
        Ok(Self { file, ipc })
    }

    pub(super) fn write(&mut self, batch: &crate::arrow::array::RecordBatch) -> ApiResult<()> {
        tokio::task::block_in_place(|| self.ipc.write(batch)).map_err(|_| write_error())
    }

    pub(super) fn finish(self) -> ApiResult<Captured> {
        tokio::task::block_in_place(|| {
            self.ipc
                .into_inner()
                .map_err(|_| write_error())?
                .flush()
                .map_err(|_| write_error())?;
            let length = self
                .file
                .as_file()
                .metadata()
                .map_err(|_| write_error())?
                .len();
            Ok(Captured {
                file: self.file,
                length,
            })
        })
    }
}

pub(super) fn save_snapshot(
    app: &Arc<App>,
    id: Uuid,
    model: String,
    revision: String,
    owner: Principal,
    arrow: Option<Captured>,
    graph: Option<(Vec<graph::Fragment>, bool)>,
) {
    let length = arrow.as_ref().map_or(0, |arrow| arrow.length);
    let snapshot = ResultSnapshot {
        id,
        model,
        revision,
        owner,
        file: arrow.map(|arrow| arrow.file),
        length,
        created: std::time::Instant::now(),
        graph: graph
            .map(|(pages, incomplete)| (pages.into_iter().map(Arc::new).collect(), incomplete)),
    };
    let mut results = app.results.lock().unwrap();
    results.retain(|saved| saved.created.elapsed() < SNAPSHOT_TTL);
    let first_owned = |results: &std::collections::VecDeque<ResultSnapshot>| {
        results
            .iter()
            .position(|saved| saved.owner == snapshot.owner)
    };
    if results
        .iter()
        .filter(|saved| saved.owner == snapshot.owner)
        .count()
        >= SNAPSHOTS_PER_OWNER
    {
        if let Some(index) = first_owned(&results) {
            results.remove(index);
        }
    }
    while results.len() >= SNAPSHOTS_TOTAL
        || snapshot.length + results.iter().map(|saved| saved.length).sum::<u64>() > SNAPSHOT_BUDGET
    {
        let index = first_owned(&results).unwrap_or(0);
        results.remove(index);
    }
    results.push_back(snapshot);
    let app = Arc::downgrade(app);
    tokio::spawn(async move {
        tokio::time::sleep(SNAPSHOT_TTL).await;
        if let Some(app) = app.upgrade() {
            app.results.lock().unwrap().retain(|saved| saved.id != id);
        }
    });
}

pub(in crate::admin) async fn snapshot(
    State(app): State<Arc<App>>,
    Path((model, id)): Path<(String, Uuid)>,
    Extension(principal): Extension<Principal>,
    method: axum::http::Method,
) -> ApiResult<Response> {
    if let Some(cluster) = &app.cluster {
        return cluster
            .arrow(&app, &principal, Some(&model), id, method)
            .await;
    }
    let (file, length, revision) = {
        let mut results = app.results.lock().unwrap();
        results.retain(|saved| saved.created.elapsed() < SNAPSHOT_TTL);
        let saved = results
            .iter()
            .find(|saved| {
                saved.id == id
                    && saved.model == model
                    && saved.file.is_some()
                    && access::owns_query(&principal, &saved.owner)
            })
            .ok_or_else(|| {
                Error::new(
                    StatusCode::NOT_FOUND,
                    "result_expired",
                    "本次结果已过期，请重新运行查询",
                )
            })?;
        (
            saved
                .file
                .as_ref()
                .expect("Arrow result")
                .reopen()
                .map_err(|_| write_error())?,
            saved.length,
            saved.revision.clone(),
        )
    };
    let permit = if method == axum::http::Method::HEAD {
        None
    } else {
        Some(download_slot(&app).await?)
    };
    Ok(attachment(
        file,
        length,
        format!("{model}-result.arrow"),
        revision,
        permit,
    ))
}

pub(in crate::admin) async fn graph_page(
    State(app): State<Arc<App>>,
    Path((model, id, index)): Path<(String, Uuid, usize)>,
    Extension(principal): Extension<Principal>,
) -> ApiResult<Response> {
    if let Some(cluster) = &app.cluster {
        return cluster
            .graph_page(&app, &principal, &model, id, index)
            .await;
    }
    let (graph, next, incomplete, revision) = {
        let results = app.results.lock().unwrap();
        let saved = results
            .iter()
            .find(|saved| {
                saved.id == id
                    && saved.model == model
                    && saved.created.elapsed() < SNAPSHOT_TTL
                    && access::owns_query(&principal, &saved.owner)
            })
            .ok_or_else(|| {
                Error::new(
                    StatusCode::NOT_FOUND,
                    "result_expired",
                    "本次结果已过期，请重新运行查询",
                )
            })?;
        let (pages, incomplete) = saved.graph.as_ref().ok_or_else(Error::not_found)?;
        let graph = pages.get(index).ok_or_else(Error::not_found)?;
        (
            Arc::clone(graph),
            (index + 1 < pages.len()).then_some(index + 1),
            *incomplete,
            saved.revision.clone(),
        )
    };
    Ok(Json(
        serde_json::json!({ "graph": graph.as_ref(), "next": next, "incomplete": incomplete, "model_revision": revision }),
    ).into_response())
}

pub(in crate::admin) struct Download {
    ticket: Uuid,
    owner: Principal,
    model: String,
    file: std::fs::File,
    length: u64,
    filename: String,
    model_revision: String,
    created: std::time::Instant,
}

pub(in crate::admin) async fn export(
    State(app): State<Arc<App>>,
    Path(id): Path<String>,
    Extension(principal): Extension<Principal>,
    Json(request): Json<Request>,
) -> ApiResult<Response> {
    access::check_export(&app, &principal, &id)?;
    if let Some(cluster) = &app.cluster {
        return cluster.execute(&app, id, principal, request, true).await;
    }
    let (active, result, engine, download_permit, policy, observations) =
        start(&app, &id, &request, &principal, false).await?;
    let _export_permit = download_permit.expect("export permit");
    let budget = app.options.spill_bytes.min(SNAPSHOT_BUDGET);
    let captured = capture_all(result, budget).await?;
    super::freshness::recheck(
        &app,
        &engine.catalog,
        &id,
        &request.model_revision,
        &policy,
        &observations,
        false,
    )
    .await?;
    if active.token.is_cancelled() {
        return Err(Error::new(StatusCode::CONFLICT, "cancelled", "查询已取消"));
    }
    let length = captured.length;
    let file = captured.file.into_file();
    drop(active);
    let ticket = Uuid::new_v4();
    {
        let mut downloads = app.downloads.lock().unwrap();
        downloads.retain(|download| download.created.elapsed() < TICKET_TTL);
        while downloads.len() >= 4
            || length
                + downloads
                    .iter()
                    .map(|download| download.length)
                    .sum::<u64>()
                > budget
        {
            let Some(index) = downloads
                .iter()
                .position(|download| download.owner == principal)
            else {
                return Err(Error::busy());
            };
            downloads.remove(index);
        }
        downloads.push_back(Download {
            ticket,
            owner: principal,
            model: id.clone(),
            file,
            length,
            filename: format!("{id}.arrow"),
            model_revision: request.model_revision,
            created: std::time::Instant::now(),
        });
    }
    let app = Arc::downgrade(&app);
    tokio::spawn(async move {
        tokio::time::sleep(TICKET_TTL).await;
        if let Some(app) = app.upgrade() {
            app.downloads.lock().unwrap().retain(|d| d.ticket != ticket);
        }
    });
    Ok(Json(serde_json::json!({ "ticket": ticket.to_string() })).into_response())
}

pub(in crate::admin) async fn download(
    State(app): State<Arc<App>>,
    Path(ticket): Path<Uuid>,
    Extension(principal): Extension<Principal>,
) -> ApiResult<Response> {
    if let Some(cluster) = &app.cluster {
        return cluster
            .arrow(&app, &principal, None, ticket, axum::http::Method::GET)
            .await;
    }
    let matching = |download: &Download| {
        download.ticket == ticket
            && download.created.elapsed() < TICKET_TTL
            && access::owns_query(&principal, &download.owner)
            && access::check_export(&app, &principal, &download.model).is_ok()
    };
    if !app.downloads.lock().unwrap().iter().any(&matching) {
        return Err(Error::not_found());
    }
    let permit = download_slot(&app).await?;
    let download = {
        let mut downloads = app.downloads.lock().unwrap();
        let index = downloads
            .iter()
            .position(matching)
            .ok_or_else(Error::not_found)?;
        downloads.remove(index).expect("matching download")
    };
    let mut response = attachment(
        download.file,
        download.length,
        download.filename,
        download.model_revision,
        Some(permit),
    );
    response.extensions_mut().insert(AuditModel(download.model));
    Ok(response)
}

pub(in crate::admin) async fn download_slot(app: &App) -> ApiResult<OwnedSemaphorePermit> {
    acquire(
        app,
        &app.download_work,
        &app.download_waiters,
        app.options.timeout,
        &app.shutdown,
    )
    .await
}

pub(in crate::admin) async fn capture_all(
    mut result: QueryResult,
    budget: u64,
) -> ApiResult<Captured> {
    let mut capture = Capture::new(&result.batches.schema(), budget)?;
    while let Some(batch) = result
        .batches
        .try_next()
        .await
        .map_err(|error| query_error(error.into()))?
    {
        capture.write(&batch)?;
    }
    crate::admin::ops::observe_query_io(&result);
    capture.finish()
}

fn attachment(
    file: std::fs::File,
    length: u64,
    filename: String,
    revision: String,
    permit: Option<OwnedSemaphorePermit>,
) -> Response {
    let stream = ReaderStream::new(tokio::fs::File::from_std(file));
    (
        [
            (
                header::CONTENT_TYPE,
                "application/vnd.apache.arrow.stream".to_owned(),
            ),
            (
                header::CONTENT_DISPOSITION,
                format!("attachment; filename=\"{filename}\""),
            ),
            (header::CONTENT_LENGTH, length.to_string()),
            (
                header::HeaderName::from_static("x-graphlake-model-revision"),
                revision,
            ),
        ],
        download_body(stream, permit),
    )
        .into_response()
}

pub(in crate::admin) fn download_body<E>(
    stream: impl futures::Stream<Item = Result<bytes::Bytes, E>> + Send + 'static,
    permit: Option<OwnedSemaphorePermit>,
) -> Body
where
    E: Into<axum::BoxError> + Send + 'static,
{
    let Some(permit) = permit else {
        return Body::empty();
    };
    download_stream(stream, permit, DOWNLOAD_IDLE_TIMEOUT)
}

fn download_stream<E>(
    stream: impl futures::Stream<Item = Result<bytes::Bytes, E>> + Send + 'static,
    permit: OwnedSemaphorePermit,
    idle: Duration,
) -> Body
where
    E: Into<axum::BoxError> + Send + 'static,
{
    use futures::StreamExt;
    use tokio::{sync::watch, time::Instant};
    let (progress, mut deadline) = watch::channel(Instant::now() + idle);
    let expired = tokio_util::sync::CancellationToken::new();
    let watchdog = expired.clone();
    tokio::spawn(async move {
        let _permit = permit;
        loop {
            let next = *deadline.borrow_and_update();
            tokio::select! {
                biased;
                changed = deadline.changed() => {
                    if changed.is_err() {
                        return;
                    }
                }
                _ = tokio::time::sleep_until(next) => {
                    // The watchdog also runs if TCP backpressure stops Hyper
                    // from polling the response body.
                    watchdog.cancel();
                    return;
                }
            }
        }
    });
    Body::from_stream(async_stream::stream! {
        let mut stream = Box::pin(stream);
        loop {
            let chunk = tokio::select! {
                biased;
                _ = expired.cancelled() => Some(Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut, "Arrow download made no progress"
                ).into())),
                chunk = stream.next() => chunk.map(|chunk| chunk.map_err(Into::into)),
            };
            match chunk {
                Some(Ok(chunk)) => {
                    if !chunk.is_empty() {
                        progress.send_replace(Instant::now() + idle);
                    }
                    yield Ok::<_, axum::BoxError>(chunk);
                }
                Some(Err(error)) => {
                    yield Err(error);
                    break;
                }
                None => break,
            }
        }
    })
}

fn write_error() -> Error {
    Error::new(
        StatusCode::INSUFFICIENT_STORAGE,
        "export_storage",
        "导出未完成：临时文件不可写或已超过导出磁盘上限",
    )
}

struct Bounded {
    file: BufWriter<std::fs::File>,
    remaining: u64,
}

impl Write for Bounded {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() as u64 > self.remaining {
            return Err(std::io::Error::other("export exceeds disk budget"));
        }
        let n = self.file.write(bytes)?;
        self.remaining -= n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[tokio::test]
    async fn download_progress_extends_deadline_and_unpolled_body_releases_permit()
    -> Result<(), axum::Error> {
        let slots = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = slots.clone().acquire_owned().await.unwrap();
        let chunks = futures::stream::iter(0..4).then(|_| async {
            tokio::time::sleep(Duration::from_millis(120)).await;
            Ok::<_, std::io::Error>(bytes::Bytes::from_static(b"a"))
        });
        let mut stream =
            download_stream(chunks, permit, Duration::from_millis(350)).into_data_stream();
        for _ in 0..4 {
            assert_eq!(
                stream.next().await.unwrap()?,
                bytes::Bytes::from_static(b"a")
            );
        }
        assert!(stream.next().await.is_none());
        drop(stream);

        let permit = slots.clone().acquire_owned().await.unwrap();
        let blocked =
            futures::stream::pending::<std::result::Result<bytes::Bytes, std::io::Error>>();
        let mut stream =
            download_stream(blocked, permit, Duration::from_millis(25)).into_data_stream();
        let released = tokio::time::timeout(Duration::from_secs(2), slots.acquire())
            .await
            .unwrap()
            .unwrap();
        assert!(stream.next().await.unwrap().is_err());
        drop(released);
        Ok(())
    }
}
