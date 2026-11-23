pub(super) mod key_index;
mod load;
pub(in crate::admin) mod managed;
mod overlay;
mod row_groups;
pub(super) mod store;

use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use serde::Deserialize;
use std::{
    collections::BTreeMap,
    io::Write as _,
    path::{Path as FilePath, PathBuf},
    sync::{Arc, Mutex, Weak},
    time::Duration,
};
use tokio::sync::Semaphore;
use uuid::Uuid;

use super::{ApiResult, App, Error};
use crate::{CancellationToken, Catalog};
use store::{Config, Mode, Run, Store, Table, View, now};

pub(super) struct LocalTables {
    workspace: Option<tempfile::TempDir>,
    pub(super) store: Mutex<Store>,
    running: Mutex<BTreeMap<String, CancellationToken>>,
    slots: Arc<Semaphore>,
    cached: Mutex<BTreeMap<Uuid, (Uuid, managed::WorkspaceVersion)>>,
    create_work: tokio::sync::Mutex<()>,
}

impl LocalTables {
    pub fn open(root: PathBuf, managed: bool) -> anyhow::Result<Self> {
        let store = Store::open(root)?;
        // The catalog may be shared over NFS. The mutable S3 sync workspace is node-local.
        let workspace = managed
            .then(|| {
                tempfile::Builder::new()
                    .prefix("graphlake-managed-sync-")
                    .tempdir()
            })
            .transpose()?;
        Ok(Self {
            workspace,
            store: Mutex::new(store),
            running: Mutex::default(),
            slots: Arc::new(Semaphore::new(2)),
            cached: Mutex::default(),
            create_work: tokio::sync::Mutex::new(()),
        })
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Create {
    model_revision: String,
    #[serde(flatten)]
    config: Config,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Schedule {
    revision: Uuid,
    mode: Mode,
    interval_seconds: Option<u64>,
    timeout_seconds: u64,
    #[serde(default)]
    reconcile_interval_seconds: Option<u64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Load {
    mode: Mode,
}

fn key(id: &str, name: &str) -> ApiResult<String> {
    if !super::models::valid_id(id) || !super::models::valid_id(name) {
        return Err(Error::not_found());
    }
    Ok(format!("{id}/{name}"))
}

pub(super) async fn list(
    State(app): State<Arc<App>>,
    Path(id): Path<String>,
) -> ApiResult<Json<Vec<View>>> {
    app.models.lock().unwrap().get(&id)?;
    let mut views: Vec<_> = {
        let app = Arc::clone(&app);
        let id = id.clone();
        tokio::task::spawn_blocking(move || {
            let store = app.local_tables.store.lock().unwrap();
            store
                .tables
                .values()
                .filter(|table| table.model == id && !table.managed)
                .map(|table| store.view(table))
                .collect::<Vec<_>>()
        })
        .await
        .map_err(|_| Error::storage())?
    };
    if let Some(backend) = managed::backend(&app) {
        views.extend(backend.views(&id).await?);
    }
    Ok(Json(views))
}

pub(super) async fn create(
    State(app): State<Arc<App>>,
    Path(id): Path<String>,
    Json(request): Json<Create>,
) -> ApiResult<(StatusCode, Json<View>)> {
    let config = request.config;
    config.validate()?;
    let catalog = {
        let models = app.models.lock().unwrap();
        let model = models.get(&id)?;
        if model.revision != request.model_revision {
            return Err(conflict("模型已变化，请重新加载后创建本地表"));
        }
        Catalog::parse_paths(&model.yaml, &models.source_path(&id)?)
            .map_err(|_| Error::invalid("invalid_model", "模型配置无效"))?
    };
    let (_, relation, source) = catalog
        .source(&config.source_table)
        .map_err(|_| Error::invalid("invalid_source", "来源表不存在或表名无效"))?;
    let key = key(&id, &config.name)?;
    let _serial = app.local_tables.create_work.lock().await;
    let (legacy_count, total_count) = {
        let store = app.local_tables.store.lock().unwrap();
        if store.tables.contains_key(&key) {
            return Err(conflict("本地表已存在"));
        }
        if let crate::Source::Parquet { tables } = source {
            let root = store.root.canonicalize().map_err(|_| Error::storage())?;
            if tables[relation].iter().any(|path| {
                is_local_source(path, &store.root, &root)
                    || crate::source::parquet_snapshot::logical_id(path).is_some()
            }) {
                return Err(Error::invalid(
                    "local_source",
                    "请选择外部源表，不能从本地副本创建同步链",
                ));
            }
        }
        (
            store.tables.values().filter(|table| !table.managed).count(),
            store.tables.len(),
        )
    };
    let table = Table {
        model: id,
        source: source.clone(),
        data_id: Uuid::new_v4(),
        managed: managed::backend(&app).is_some(),
        archived_generation: None,
        revision: Uuid::new_v4(),
        next_run_at_ms: config.interval_seconds.map(|s| now() + s * 1000),
        next_reconcile_at_ms: config.reconcile_interval_seconds.map(|s| now() + s * 1000),
        observed_at_ms: None,
        observation_finished_at_ms: None,
        reconciled_at_ms: None,
        schedule_paused: false,
        config,
        history: Vec::new(),
    };
    if let Some(backend) = managed::backend(&app) {
        return Ok((
            StatusCode::CREATED,
            Json(backend.create(&app, table, legacy_count).await?),
        ));
    }
    if total_count >= 256 {
        return Err(conflict("本地表最多 256 个"));
    }
    let mut store = app.local_tables.store.lock().unwrap();
    store.put(key, table.clone())?;
    Ok((StatusCode::CREATED, Json(store.view(&table))))
}

fn is_local_source(location: &str, root: &FilePath, canonical_root: &FilePath) -> bool {
    let path = if location.starts_with("file://") {
        let Some(path) = url::Url::parse(location)
            .ok()
            .and_then(|url| url.to_file_path().ok())
        else {
            return false;
        };
        path
    } else if location.contains("://") {
        return false;
    } else {
        PathBuf::from(location)
    };
    path.starts_with(root)
        || path.starts_with(canonical_root)
        || path
            .canonicalize()
            .is_ok_and(|resolved| resolved.starts_with(canonical_root))
}

pub(super) async fn schedule(
    State(app): State<Arc<App>>,
    Path((id, name)): Path<(String, String)>,
    Json(request): Json<Schedule>,
) -> ApiResult<Json<View>> {
    let key = key(&id, &name)?;
    if let Some(backend) = managed::backend(&app)
        && let Some(table) = backend.by_name(&id, &name).await.map_err(managed::error)?
    {
        return Ok(Json(backend.schedule(&app, table, &request).await?));
    }
    let mut store = app.local_tables.store.lock().unwrap();
    let mut table = store.tables.get(&key).ok_or_else(Error::not_found)?.clone();
    if table.managed {
        return Err(managed::unavailable());
    }
    apply_schedule(&mut table, &request)?;
    store.put(key, table.clone())?;
    Ok(Json(store.view(&table)))
}

fn apply_schedule(table: &mut Table, request: &Schedule) -> ApiResult<()> {
    if table.revision != request.revision {
        return Err(conflict("配置已变化，请重新读取后保存"));
    }
    let reset_reconcile = table.schedule_paused
        || table.config.reconcile_interval_seconds != request.reconcile_interval_seconds;
    table.config.mode = request.mode;
    table.config.interval_seconds = request.interval_seconds;
    table.config.timeout_seconds = request.timeout_seconds;
    table.config.reconcile_interval_seconds = request.reconcile_interval_seconds;
    table.config.validate()?;
    table.revision = Uuid::new_v4();
    table.schedule_paused = false;
    table.next_run_at_ms = request.interval_seconds.map(|s| now() + s * 1000);
    if reset_reconcile {
        table.next_reconcile_at_ms = request.reconcile_interval_seconds.map(|s| now() + s * 1000);
    }
    Ok(())
}

pub(super) async fn load(
    State(app): State<Arc<App>>,
    Path((id, name)): Path<(String, String)>,
    Json(request): Json<Load>,
) -> ApiResult<(StatusCode, Json<Run>)> {
    let key = key(&id, &name)?;
    if let Some(backend) = managed::backend(&app)
        && let Some(table) = backend.by_name(&id, &name).await.map_err(managed::error)?
    {
        let run = backend
            .start(&app, table, Some(request.mode), false)
            .await?;
        return Ok((StatusCode::ACCEPTED, Json(run)));
    }
    let run = start(&app, key, Some(request.mode), false)?;
    Ok((StatusCode::ACCEPTED, Json(run)))
}

pub(super) async fn cancel(
    State(app): State<Arc<App>>,
    Path((id, name)): Path<(String, String)>,
) -> ApiResult<StatusCode> {
    let key = key(&id, &name)?;
    let running = app.local_tables.running.lock().unwrap();
    running
        .get(&key)
        .ok_or_else(|| conflict("当前没有运行中的任务"))?
        .cancel();
    Ok(StatusCode::ACCEPTED)
}

fn conflict(message: &str) -> Error {
    Error::new(StatusCode::CONFLICT, "local_table_conflict", message)
}

fn prepare_run(
    table: &mut Table,
    mode: Option<Mode>,
    scheduled: bool,
    ready: bool,
) -> ApiResult<Run> {
    let reconcile = scheduled && table.next_reconcile_at_ms.is_some_and(|due| due <= now());
    if scheduled && !reconcile && table.next_run_at_ms.is_none_or(|due| due > now()) {
        return Err(Error::busy());
    }
    let mode = if reconcile {
        Mode::Full
    } else {
        mode.unwrap_or(table.config.mode)
    };
    if mode == Mode::Incremental
        && (table.config.key_columns.is_empty() || table.config.watermark_column.is_none())
    {
        return Err(Error::invalid(
            "incremental_key_required",
            "请创建带主键和增量字段的本地表",
        ));
    }
    let reconciled = mode == Mode::Full || !ready;
    let run = Run {
        id: Uuid::new_v4(),
        mode,
        reconciled,
        trigger: if reconcile {
            "reconcile"
        } else if scheduled {
            "scheduled"
        } else {
            "manual"
        }
        .into(),
        status: "running".into(),
        started_at_ms: now(),
        finished_at_ms: None,
        loaded_rows: None,
        source_reads: None,
        committed_generation: None,
        total_rows: None,
        error: None,
    };
    table.history.insert(0, run.clone());
    table.history.truncate(20);
    table.next_run_at_ms = None;
    Ok(run)
}

fn start(app: &Arc<App>, key: String, mode: Option<Mode>, scheduled: bool) -> ApiResult<Run> {
    let mut store = app.local_tables.store.lock().unwrap();
    store.flush()?;
    let mut table = store.tables.get(&key).ok_or_else(Error::not_found)?.clone();
    if table.managed {
        return Err(managed::unavailable());
    }
    let mut running = app.local_tables.running.lock().unwrap();
    if running.contains_key(&key) {
        return Err(conflict("此表正在加载，请等待完成或取消任务"));
    }
    let permit = Arc::clone(&app.local_tables.slots)
        .try_acquire_owned()
        .map_err(|_| Error::busy())?;
    let path = store.path(&table);
    let run = prepare_run(&mut table, mode, scheduled, path.is_file())?;
    let mode = run.mode;
    store.put(key.clone(), table.clone())?;
    let cancel = app.shutdown.child_token();
    running.insert(key.clone(), cancel.clone());
    let app = Arc::clone(app);
    tokio::spawn(async move {
        let _permit = permit;
        let options = crate::QueryOptions {
            timeout: Duration::from_secs(table.config.timeout_seconds),
            cancellation: cancel.clone(),
            ..app.options.clone()
        };
        use futures::FutureExt;
        let result = std::panic::AssertUnwindSafe(async {
            let anchor = path.clone();
            let limit = options.spill_bytes;
            tokio::task::spawn_blocking(move || overlay::check_retired_space(&anchor, limit))
                .await??;
            load::run(&table, &path, mode, options, &app.admission).await
        })
        .catch_unwind()
        .await
        .unwrap_or_else(|_| Err(anyhow::anyhow!("local-table load panicked")));
        if result.is_ok() {
            crate::blocking(|| load::clean_parts(&path));
        }
        let mut store = app.local_tables.store.lock().unwrap();
        if let Some(mut current) = store.tables.get(&key).cloned() {
            record_result(&mut current, result, cancel.is_cancelled());
            if let Err(error) = store.finish(key.clone(), current) {
                let _ = writeln!(
                    std::io::stderr().lock(),
                    "GraphLake local-table status persistence failed: {}",
                    error.message
                );
                // The watcher retries persistence before scheduling another run.
            }
        }
        app.local_tables.running.lock().unwrap().remove(&key);
    });
    Ok(run)
}

fn record_result(
    current: &mut Table,
    result: anyhow::Result<(u64, u64, crate::source::ReadMetrics, Option<Uuid>)>,
    cancelled: bool,
) {
    let run = &mut current.history[0];
    run.finished_at_ms = Some(now());
    match result {
        Ok((loaded, total, reads, generation)) => {
            run.status = "succeeded".into();
            run.loaded_rows = Some(loaded);
            run.total_rows = Some(total);
            run.source_reads = Some(reads);
            run.committed_generation = generation;
            current.observed_at_ms = Some(run.started_at_ms);
            current.observation_finished_at_ms = run.finished_at_ms;
            if run.reconciled {
                current.reconciled_at_ms = Some(run.started_at_ms);
                if !current.schedule_paused {
                    current.next_reconcile_at_ms = current
                        .config
                        .reconcile_interval_seconds
                        .map(|s| run.started_at_ms + s * 1000);
                }
            }
        }
        Err(error) => {
            run.status = if cancelled { "cancelled" } else { "failed" }.into();
            // Connector errors may contain connection URLs. Expose only
            // our actionable validation messages, never driver details.
            let message = error.to_string();
            run.error = Some(
                if matches!(
                    message.as_str(),
                    "增量字段必须是整数、日期或时间戳"
                        | "增量字段类型已变化，请新建本地表"
                        | "来源字段类型已变化，请新建本地表"
                        | "主键不能包含空值"
                        | "来源包含重复主键；请修正来源后重试"
                        | "增量字段不能包含空值"
                        | "本地副本旧分区等待查询释放，已超过保留空间预算"
                ) {
                    message
                } else if message.contains("deadline exceeded") {
                    "加载超时；旧副本保留，可提高超时后重试".into()
                } else if cancelled {
                    "任务已取消；已提交的副本保留".into()
                } else {
                    "加载失败；请检查来源连接、权限、字段和可用磁盘空间后重试".into()
                },
            );
        }
    }
    if !current.schedule_paused {
        current.next_run_at_ms = current.config.interval_seconds.map(|s| now() + s * 1000);
    }
}

pub(super) async fn watch(app: Weak<App>) {
    let mut managed_schedule = managed::ScheduleCache::default();
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let Some(app) = app.upgrade() else { return };
        if app.shutdown.is_cancelled() {
            return;
        }
        if let Some(ha) = &app.ha {
            if ha.recovery_pending() || ha.verify_backend(Duration::from_secs(1)).await.is_err() {
                continue;
            }
        }
        if let Some(backend) = managed::backend(&app) {
            backend.tick(&app, &mut managed_schedule).await;
        }
        if app.local_tables.store.lock().unwrap().flush().is_err() {
            continue;
        }
        let due: Vec<_> = app
            .local_tables
            .store
            .lock()
            .unwrap()
            .tables
            .iter()
            .filter(|(_, table)| {
                !table.managed
                    && table.history.first().is_none_or(|run| {
                        !matches!(run.status.as_str(), "failed" | "cancelled")
                            || run
                                .finished_at_ms
                                .is_none_or(|last| now().saturating_sub(last) >= 30_000)
                    })
            })
            .filter_map(|(key, table)| {
                [table.next_run_at_ms, table.next_reconcile_at_ms]
                    .into_iter()
                    .flatten()
                    .filter(|due| *due <= now())
                    .min()
                    .map(|due| (due, key.clone()))
            })
            .collect();
        let mut due = due;
        due.sort_unstable();
        for (_, key) in due {
            if let Err(error) = start(&app, key, None, true)
                && error.status.is_server_error()
                && error.status != StatusCode::SERVICE_UNAVAILABLE
            {
                let _ = writeln!(
                    std::io::stderr().lock(),
                    "GraphLake scheduled local-table load did not start: {}",
                    error.code
                );
            }
        }
    }
}
