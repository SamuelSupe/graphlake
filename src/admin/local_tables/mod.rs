mod load;
mod store;

use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use serde::Deserialize;
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};
use tokio::sync::Semaphore;
use uuid::Uuid;

use super::{ApiResult, App, Error};
use crate::{CancellationToken, Catalog};
use store::{Config, Mode, Run, Store, Table, View, now};

pub(super) struct LocalTables {
    store: Mutex<Store>,
    running: Mutex<BTreeMap<String, CancellationToken>>,
    slots: Arc<Semaphore>,
}

impl LocalTables {
    pub fn open(root: PathBuf) -> anyhow::Result<Self> {
        Ok(Self {
            store: Mutex::new(Store::open(root)?),
            running: Mutex::default(),
            slots: Arc::new(Semaphore::new(2)),
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
    let store = app.local_tables.store.lock().unwrap();
    Ok(Json(
        store
            .tables
            .values()
            .filter(|table| table.model == id)
            .map(|table| store.view(table))
            .collect(),
    ))
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
    let (_, _, source) = catalog
        .source(&config.source_table)
        .map_err(|_| Error::invalid("invalid_source", "来源表不存在或表名无效"))?;
    let key = key(&id, &config.name)?;
    let mut store = app.local_tables.store.lock().unwrap();
    if store.tables.contains_key(&key) {
        return Err(conflict("本地表已存在"));
    }
    if store.tables.len() >= 256 {
        return Err(conflict("本地表最多 256 个"));
    }
    // A local copy must not load from another managed copy: it would create an
    // implicit dependency whose refresh order is not part of the schedule.
    if let crate::Source::Parquet { tables } = source {
        let (_, relation, _) = catalog
            .source(&config.source_table)
            .map_err(|_| Error::not_found())?;
        if tables[relation]
            .iter()
            .any(|path| std::path::Path::new(path).starts_with(&store.root))
        {
            return Err(Error::invalid(
                "local_source",
                "请选择外部源表，不能从本地副本创建同步链",
            ));
        }
    }
    let table = Table {
        model: id,
        source: source.clone(),
        data_id: Uuid::new_v4(),
        revision: Uuid::new_v4(),
        next_run_at_ms: config.interval_seconds.map(|s| now() + s * 1000),
        config,
        history: Vec::new(),
    };
    store.put(key, table.clone())?;
    Ok((StatusCode::CREATED, Json(store.view(&table))))
}

pub(super) async fn schedule(
    State(app): State<Arc<App>>,
    Path((id, name)): Path<(String, String)>,
    Json(request): Json<Schedule>,
) -> ApiResult<Json<View>> {
    let key = key(&id, &name)?;
    let mut store = app.local_tables.store.lock().unwrap();
    let mut table = store.tables.get(&key).ok_or_else(Error::not_found)?.clone();
    if table.revision != request.revision {
        return Err(conflict("配置已变化，请重新读取后保存"));
    }
    table.config.mode = request.mode;
    table.config.interval_seconds = request.interval_seconds;
    table.config.timeout_seconds = request.timeout_seconds;
    table.config.validate()?;
    table.revision = Uuid::new_v4();
    table.next_run_at_ms = request.interval_seconds.map(|s| now() + s * 1000);
    store.put(key, table.clone())?;
    Ok(Json(store.view(&table)))
}

pub(super) async fn load(
    State(app): State<Arc<App>>,
    Path((id, name)): Path<(String, String)>,
    Json(request): Json<Load>,
) -> ApiResult<(StatusCode, Json<Run>)> {
    let key = key(&id, &name)?;
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

fn start(app: &Arc<App>, key: String, mode: Option<Mode>, scheduled: bool) -> ApiResult<Run> {
    let mut store = app.local_tables.store.lock().unwrap();
    let mut table = store.tables.get(&key).ok_or_else(Error::not_found)?.clone();
    if scheduled && table.next_run_at_ms.is_none_or(|due| due > now()) {
        return Err(Error::busy());
    }
    let mut running = app.local_tables.running.lock().unwrap();
    if running.contains_key(&key) {
        return Err(conflict("此表正在加载，请等待完成或取消任务"));
    }
    let permit = Arc::clone(&app.local_tables.slots)
        .try_acquire_owned()
        .map_err(|_| Error::busy())?;
    let mode = mode.unwrap_or(table.config.mode);
    if mode == Mode::Incremental
        && (table.config.key_columns.is_empty() || table.config.watermark_column.is_none())
    {
        return Err(Error::invalid(
            "incremental_key_required",
            "请创建带主键和增量字段的本地表",
        ));
    }
    let run = Run {
        id: Uuid::new_v4(),
        mode,
        trigger: if scheduled { "scheduled" } else { "manual" }.into(),
        status: "running".into(),
        started_at_ms: now(),
        finished_at_ms: None,
        loaded_rows: None,
        total_rows: None,
        error: None,
    };
    table.history.insert(0, run.clone());
    table.history.truncate(20);
    table.next_run_at_ms = None;
    let path = store.path(&table);
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
        let result = load::run(&table, &path, mode, options).await;
        let mut store = app.local_tables.store.lock().unwrap();
        if let Some(mut current) = store.tables.get(&key).cloned() {
            let run = &mut current.history[0];
            run.finished_at_ms = Some(now());
            match result {
                Ok((loaded, total)) => {
                    run.status = "succeeded".into();
                    run.loaded_rows = Some(loaded);
                    run.total_rows = Some(total);
                }
                Err(error) => {
                    run.status = if cancel.is_cancelled() {
                        "cancelled"
                    } else {
                        "failed"
                    }
                    .into();
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
                        ) {
                            message
                        } else if message.contains("deadline exceeded") {
                            "加载超时；旧副本保留，可提高超时后重试".into()
                        } else if cancel.is_cancelled() {
                            "任务已取消；已提交的副本保留".into()
                        } else {
                            "加载失败；请检查来源连接、权限、字段和可用磁盘空间后重试".into()
                        },
                    );
                }
            }
            current.next_run_at_ms = current.config.interval_seconds.map(|s| now() + s * 1000);
            if let Err(error) = store.put(key.clone(), current) {
                eprintln!(
                    "GraphLake local-table status persistence failed: {}",
                    error.message
                );
                // Fail closed: a successful data commit is safe to replay, but
                // never launch another task with an unrecorded outcome.
                app.shutdown.cancel();
            }
        }
        app.local_tables.running.lock().unwrap().remove(&key);
    });
    Ok(run)
}

pub(super) async fn watch(app: Weak<App>) {
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
        let due: Vec<_> = app
            .local_tables
            .store
            .lock()
            .unwrap()
            .tables
            .iter()
            .filter(|(_, table)| table.next_run_at_ms.is_some_and(|due| due <= now()))
            .map(|(key, _)| key.clone())
            .collect();
        for key in due {
            let _ = start(&app, key, None, true);
        }
    }
}
