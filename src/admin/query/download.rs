use std::{
    collections::VecDeque,
    io::{BufWriter, Seek, Write},
};

use axum::{body::Body, http::header};
use tokio::sync::OwnedSemaphorePermit;
use tokio_util::io::ReaderStream;

use crate::arrow::ipc::writer::StreamWriter;

use super::*;

const SNAPSHOTS_PER_OWNER: usize = 4;
const PREVIEW_INLINE_BYTES: usize = 1024 * 1024;
const SNAPSHOTS_TOTAL: usize = 32;
pub(in crate::admin) const DOWNLOAD_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const SNAPSHOT_BUDGET: u64 = 1024 * 1024 * 1024;
const SNAPSHOT_TTL: Duration = Duration::from_secs(30 * 60);
const TICKET_TTL: Duration = Duration::from_secs(60);

pub(in crate::admin) struct ResultSnapshot {
    pub(super) id: Uuid,
    model: String,
    revision: String,
    owner: Principal,
    arrow: Option<Arc<Captured>>,
    length: u64,
    budget_bytes: u64,
    created: std::time::Instant,
    graph: Option<(Vec<Arc<graph::Fragment>>, bool)>,
}

pub(super) struct Capture {
    ipc: StreamWriter<Bounded>,
}

pub(in crate::admin) enum Captured {
    Memory(bytes::Bytes),
    File {
        file: tempfile::NamedTempFile,
        length: u64,
    },
}

impl Captured {
    pub(in crate::admin) fn len(&self) -> u64 {
        match self {
            Self::Memory(bytes) => bytes.len() as u64,
            Self::File { length, .. } => *length,
        }
    }

    fn body(&self) -> ApiResult<DownloadSource> {
        Ok(match self {
            Self::Memory(bytes) => DownloadSource::Memory(bytes.clone()),
            Self::File { file, .. } => {
                DownloadSource::File(file.reopen().map_err(|_| write_error())?)
            }
        })
    }
}

enum CaptureSink {
    Memory(Vec<u8>),
    File(BufWriter<tempfile::NamedTempFile>),
}

impl Capture {
    pub(super) fn new(schema: &crate::arrow::datatypes::Schema, budget: u64) -> ApiResult<Self> {
        crate::blocking(|| {
            let file = tempfile::NamedTempFile::new().map_err(|_| write_error())?;
            Self::open(schema, budget, CaptureSink::File(BufWriter::new(file)))
        })
    }

    pub(super) fn preview(schema: &crate::arrow::datatypes::Schema) -> ApiResult<Self> {
        Self::open(schema, 16 * 1024 * 1024, CaptureSink::Memory(Vec::new()))
    }

    fn open(
        schema: &crate::arrow::datatypes::Schema,
        budget: u64,
        sink: CaptureSink,
    ) -> ApiResult<Self> {
        let writer = Bounded {
            sink,
            remaining: budget.min(SNAPSHOT_BUDGET),
        };
        let ipc = StreamWriter::try_new(writer, schema).map_err(|_| write_error())?;
        Ok(Self { ipc })
    }

    pub(super) fn write(&mut self, batch: &crate::arrow::array::RecordBatch) -> ApiResult<()> {
        crate::blocking(|| self.ipc.write(batch)).map_err(|_| write_error())
    }

    pub(super) fn finish(self) -> ApiResult<Captured> {
        crate::blocking(|| {
            let sink = self.ipc.into_inner().map_err(|_| write_error())?.sink;
            match sink {
                CaptureSink::Memory(bytes) => Ok(Captured::Memory(bytes.into())),
                CaptureSink::File(writer) => {
                    let mut file = writer.into_inner().map_err(|_| write_error())?;
                    let length = file.as_file().metadata().map_err(|_| write_error())?.len();
                    file.as_file_mut().rewind().map_err(|_| write_error())?;
                    Ok(Captured::File { file, length })
                }
            }
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
    let length = arrow.as_ref().map_or(0, Captured::len);
    let graph_bytes = graph.as_ref().map_or(0, |(pages, _)| {
        pages
            .iter()
            .fold(0u64, |sum, page| sum.saturating_add(page.estimated_bytes()))
    });
    let budget_bytes = length.saturating_add(graph_bytes);
    let snapshot = ResultSnapshot {
        id,
        model,
        revision,
        owner,
        arrow: arrow.map(Arc::new),
        length,
        budget_bytes,
        created: std::time::Instant::now(),
        graph: graph
            .map(|(pages, incomplete)| (pages.into_iter().map(Arc::new).collect(), incomplete)),
    };
    let mut results = app.results.lock().unwrap();
    let mut removed = expired_snapshots(&mut results);
    let first_owned = |results: &VecDeque<ResultSnapshot>| {
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
            removed.extend(results.remove(index).and_then(|saved| saved.arrow));
        }
    }
    while results.len() >= SNAPSHOTS_TOTAL
        || (!results.is_empty()
            && snapshot
                .budget_bytes
                .saturating_add(results.iter().map(|saved| saved.budget_bytes).sum::<u64>())
                > SNAPSHOT_BUDGET)
    {
        let index = first_owned(&results).unwrap_or(0);
        removed.extend(results.remove(index).and_then(|saved| saved.arrow));
    }
    results.push_back(snapshot);
    drop(results);
    crate::blocking(|| drop(removed));
}

fn expired_snapshots(results: &mut VecDeque<ResultSnapshot>) -> Vec<Arc<Captured>> {
    let mut removed = Vec::new();
    results.retain(|saved| {
        if saved.created.elapsed() < SNAPSHOT_TTL {
            return true;
        }
        removed.extend(saved.arrow.iter().cloned());
        false
    });
    removed
}

pub(in crate::admin) async fn expire_results(app: std::sync::Weak<App>) {
    loop {
        tokio::time::sleep(Duration::from_secs(15)).await;
        let Some(app) = app.upgrade() else { return };
        if app.shutdown.is_cancelled() {
            return;
        }
        crate::blocking(|| {
            let removed = expired_snapshots(&mut app.results.lock().unwrap());
            drop(removed);
            app.downloads
                .lock()
                .unwrap()
                .retain(|download| download.created.elapsed() < TICKET_TTL);
        });
    }
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
    let (captured, length, revision) = {
        let results = app.results.lock().unwrap();
        let saved = results
            .iter()
            .find(|saved| {
                saved.id == id
                    && saved.model == model
                    && saved.arrow.is_some()
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
        (
            if method == axum::http::Method::HEAD {
                None
            } else {
                saved.arrow.clone()
            },
            saved.length,
            saved.revision.clone(),
        )
    };
    let (file, permit) = if let Some(captured) = captured {
        let permit = download_slot(&app, &principal).await?;
        let file = tokio::task::spawn_blocking(move || captured.body())
            .await
            .map_err(|_| write_error())??;
        (Some(file), Some(permit))
    } else {
        (None, None)
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
    let Captured::File { file, length } = captured else {
        unreachable!("exports always use a file");
    };
    let file = file.into_file();
    drop(active);
    let ticket = Uuid::new_v4();
    let mut removed = Vec::new();
    let busy = {
        let mut downloads = app.downloads.lock().unwrap();
        let mut index = 0;
        while index < downloads.len() {
            if downloads[index].created.elapsed() >= TICKET_TTL {
                if let Some(expired) = downloads.remove(index) {
                    removed.push(expired);
                }
            } else {
                index += 1;
            }
        }
        let mut busy = false;
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
                busy = true;
                break;
            };
            if let Some(evicted) = downloads.remove(index) {
                removed.push(evicted);
            }
        }
        if !busy {
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
        busy
    };
    if !removed.is_empty() {
        crate::blocking(|| drop(removed));
    }
    if busy {
        return Err(Error::busy());
    }
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
    let permit = download_slot(&app, &principal).await?;
    let download = {
        let mut downloads = app.downloads.lock().unwrap();
        let index = downloads
            .iter()
            .position(matching)
            .ok_or_else(Error::not_found)?;
        downloads.remove(index).expect("matching download")
    };
    let mut response = attachment(
        Some(DownloadSource::File(download.file)),
        download.length,
        download.filename,
        download.model_revision,
        Some(permit),
    );
    response.extensions_mut().insert(AuditModel(download.model));
    Ok(response)
}

pub(in crate::admin) struct DownloadPermit {
    _slot: OwnedSemaphorePermit,
    _owner: DownloadOwner,
}

struct DownloadOwner {
    owner: Principal,
    owners: Arc<std::sync::Mutex<Vec<Principal>>>,
}

impl Drop for DownloadOwner {
    fn drop(&mut self) {
        self.owners
            .lock()
            .unwrap()
            .retain(|owner| owner != &self.owner);
    }
}

pub(in crate::admin) async fn download_slot(
    app: &App,
    principal: &Principal,
) -> ApiResult<DownloadPermit> {
    let owner = {
        let mut owners = app.download_owners.lock().unwrap();
        if owners.contains(principal) {
            return Err(Error::busy());
        }
        owners.push(principal.clone());
        DownloadOwner {
            owner: principal.clone(),
            owners: app.download_owners.clone(),
        }
    };
    let slot = acquire(
        app,
        &app.download_work,
        &app.download_waiters,
        app.options.timeout,
        &app.shutdown,
    )
    .await?;
    Ok(DownloadPermit {
        _slot: slot,
        _owner: owner,
    })
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

enum DownloadSource {
    Memory(bytes::Bytes),
    File(std::fs::File),
}

fn attachment(
    source: Option<DownloadSource>,
    length: u64,
    filename: String,
    revision: String,
    permit: Option<DownloadPermit>,
) -> Response {
    let body = match source {
        Some(DownloadSource::Memory(bytes)) => download_body(
            futures::stream::once(async move { Ok::<_, std::io::Error>(bytes) }),
            permit,
        ),
        Some(DownloadSource::File(file)) => {
            download_body(ReaderStream::new(tokio::fs::File::from_std(file)), permit)
        }
        None => Body::empty(),
    };
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
        body,
    )
        .into_response()
}

pub(in crate::admin) fn download_body<E>(
    stream: impl futures::Stream<Item = Result<bytes::Bytes, E>> + Send + 'static,
    permit: Option<DownloadPermit>,
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
    permit: impl Send + 'static,
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
                    std::io::ErrorKind::TimedOut, "Arrow download stalled"
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
    sink: CaptureSink,
    remaining: u64,
}

impl Write for Bounded {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() as u64 > self.remaining {
            return Err(std::io::Error::other("export exceeds disk budget"));
        }
        if let CaptureSink::Memory(buffer) = &mut self.sink
            && buffer.len().saturating_add(bytes.len()) > PREVIEW_INLINE_BYTES
        {
            let mut file = BufWriter::new(tempfile::NamedTempFile::new()?);
            file.write_all(buffer)?;
            self.sink = CaptureSink::File(file);
        }
        match &mut self.sink {
            CaptureSink::Memory(buffer) => buffer.extend_from_slice(bytes),
            CaptureSink::File(file) => file.write_all(bytes)?,
        }
        self.remaining -= bytes.len() as u64;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match &mut self.sink {
            CaptureSink::Memory(_) => Ok(()),
            CaptureSink::File(file) => file.flush(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[test]
    fn preview_capture_retains_small_arrow_and_spills_large_arrow() -> anyhow::Result<()> {
        use crate::arrow::{
            array::{BinaryArray, RecordBatch},
            ipc::reader::StreamReader,
        };
        for size in [16, PREVIEW_INLINE_BYTES + 128] {
            let payload = vec![42; size];
            let batch = RecordBatch::try_from_iter([(
                "payload",
                Arc::new(BinaryArray::from_vec(vec![payload.as_slice()]))
                    as crate::arrow::array::ArrayRef,
            )])
            .unwrap();
            let into_error = |error: Error| anyhow::anyhow!(error.message);
            let mut capture = Capture::preview(&batch.schema()).map_err(into_error)?;
            capture.write(&batch).map_err(into_error)?;
            let captured = capture.finish().map_err(into_error)?;
            let len = captured.len();
            let mut body: Box<dyn std::io::Read> = match (size, captured) {
                (16, Captured::Memory(bytes)) => Box::new(std::io::Cursor::new(bytes)),
                (_, Captured::File { file, .. }) if size > PREVIEW_INLINE_BYTES => {
                    assert_eq!(file.as_file().metadata().unwrap().len(), len);
                    Box::new(file.reopen().unwrap())
                }
                _ => panic!("preview used the wrong storage"),
            };
            let mut reader = StreamReader::try_new(&mut body, None).unwrap();
            assert_eq!(reader.next().unwrap().unwrap(), batch);
            assert!(reader.next().is_none());
        }
        Ok(())
    }

    #[tokio::test]
    async fn download_progress_extends_deadline_and_unpolled_body_releases_permit()
    -> Result<(), axum::Error> {
        let slots = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = slots.clone().acquire_owned().await.unwrap();
        let chunks = futures::stream::iter(0..8).then(|_| async {
            tokio::time::sleep(Duration::from_millis(120)).await;
            Ok::<_, std::io::Error>(bytes::Bytes::from_static(b"a"))
        });
        let mut stream =
            download_stream(chunks, permit, Duration::from_millis(350)).into_data_stream();
        for _ in 0..8 {
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
