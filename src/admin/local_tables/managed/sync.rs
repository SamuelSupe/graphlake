use std::{collections::BTreeMap, io::Write as _, path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Result, ensure};
use uuid::Uuid;

use super::files::preserve_parquet_anchor;
use super::metadata::{Historical, Pending};
use super::{Head, Managed, Record, api_error, error, unavailable};
use crate::{
    CancellationToken, QueryOptions,
    admin::{
        ApiResult, App, Error,
        cluster::control::Leadership,
        local_tables::{
            Schedule, apply_schedule, conflict, load, prepare_run, record_result,
            store::{Mode, Run, Table, View, now},
        },
    },
};

fn leader(app: &App) -> ApiResult<&Leadership> {
    app.ha
        .as_ref()
        .map(|ha| ha.leader.as_ref())
        .ok_or_else(unavailable)
}

fn owner(leader: &Leadership) -> String {
    leader.owner()["value"]
        .as_str()
        .expect("encoded leadership owner")
        .to_owned()
}

pub(in crate::admin::local_tables) struct ScheduleCache {
    revision: i64,
    next_full: std::time::Instant,
    records: BTreeMap<Uuid, (Record, i64)>,
}

impl Default for ScheduleCache {
    fn default() -> Self {
        Self {
            revision: 0,
            next_full: std::time::Instant::now(),
            records: BTreeMap::new(),
        }
    }
}

fn view(record: &Record) -> View {
    let t = &record.table;
    let location = format!("local-table://{}", t.data_id);
    View {
        model: t.model.clone(),
        config: t.config.clone(),
        revision: t.revision,
        ready: record.head.is_some(),
        path: location.clone(),
        reference_path: location,
        schedule_paused: t.schedule_paused,
        next_run_at_ms: t.next_run_at_ms,
        next_reconcile_at_ms: t.next_reconcile_at_ms,
        observed_at_ms: t.observed_at_ms,
        observation_finished_at_ms: t.observation_finished_at_ms,
        reconciled_at_ms: t.reconciled_at_ms,
        generation: record.head.as_ref().map(|h| h.generation),
        watermark: record.head.as_ref().and_then(|h| h.watermark.clone()),
        history: t.history.clone(),
    }
}

fn recover(record: &mut Record, owner: &str, running: bool) -> bool {
    let Some(pending) = &record.pending else {
        return false;
    };
    let same_owner = pending.owner == owner;
    if same_owner && running {
        return false;
    }
    record.pending = None;
    if let Some(run) = record
        .table
        .history
        .first_mut()
        .filter(|r| r.status == "running")
    {
        run.status = "interrupted".into();
        run.finished_at_ms = Some(now());
        run.error = Some(if same_owner {
            "同步结果未确认，可基于已提交水位重试".into()
        } else {
            "Coordinator 切换中断了任务，可基于已提交水位重试".into()
        });
    }
    if !record.table.schedule_paused {
        record.table.next_run_at_ms = record
            .table
            .config
            .interval_seconds
            .map(|s| now() + s * 1000);
    }
    true
}

// The private workspace anchor is replaced atomically by load::run.
#[derive(PartialEq, Eq)]
pub(in crate::admin::local_tables) struct WorkspaceVersion {
    size: u64,
    modified: std::time::SystemTime,
    #[cfg(unix)]
    inode: u64,
}

impl WorkspaceVersion {
    fn read(path: &std::path::Path) -> Option<Self> {
        let info = std::fs::metadata(path).ok()?;
        #[cfg(unix)]
        use std::os::unix::fs::MetadataExt;
        Some(Self {
            size: info.len(),
            modified: info.modified().ok()?,
            #[cfg(unix)]
            inode: info.ino(),
        })
    }
}

type LoadResult = (u64, u64, crate::source::ReadMetrics, Option<Uuid>);

impl Managed {
    pub(in crate::admin::local_tables) async fn views(&self, model: &str) -> ApiResult<Vec<View>> {
        Ok(self
            .records()
            .await
            .map_err(error)?
            .into_iter()
            .filter(|(record, _)| record.table.model == model)
            .map(|(record, _)| view(&record))
            .collect())
    }

    pub(in crate::admin::local_tables) async fn create(
        &self,
        app: &App,
        table: Table,
        legacy_count: usize,
    ) -> ApiResult<View> {
        let records = self.records().await.map_err(error)?;
        if records
            .iter()
            .any(|(r, _)| r.table.model == table.model && r.table.config.name == table.config.name)
        {
            return Err(conflict("本地表已存在"));
        }
        if records.len() + legacy_count >= 256 {
            return Err(conflict("本地表最多 256 个"));
        }
        let record = Record {
            table,
            head: None,
            retired: Default::default(),
            pending: None,
        };
        if self.save(&record, 0, leader(app)?).await.map_err(error)? {
            Ok(view(&record))
        } else {
            Err(conflict("本地表创建冲突，请重试"))
        }
    }

    pub(in crate::admin::local_tables) async fn schedule(
        &self,
        app: &App,
        id: Uuid,
        request: &Schedule,
    ) -> ApiResult<View> {
        for _ in 0..8 {
            let (mut record, rev) = self
                .record(id)
                .await
                .map_err(error)?
                .ok_or_else(Error::not_found)?;
            apply_schedule(&mut record.table, request)?;
            if self.save(&record, rev, leader(app)?).await.map_err(error)? {
                return Ok(view(&record));
            }
        }
        Err(Error::busy())
    }

    pub(in crate::admin::local_tables) async fn start(
        &self,
        app: &Arc<App>,
        id: Uuid,
        mode: Option<Mode>,
        scheduled: bool,
    ) -> ApiResult<Run> {
        let permit = app
            .local_tables
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::busy())?;
        let _serial = app.local_tables.create_work.lock().await;
        let leader = leader(app)?;
        let lease = leader.confirm().await?;
        self.migrate_retirements(id, leader).await.map_err(error)?;
        let owner = owner(leader);
        for _ in 0..8 {
            let (mut record, rev) = self
                .record(id)
                .await
                .map_err(error)?
                .ok_or_else(Error::not_found)?;
            let key = format!("{}/{}", record.table.model, record.table.config.name);
            let running = app.local_tables.running.lock().unwrap().contains_key(&key);
            recover(&mut record, &owner, running);
            if record.pending.is_some() {
                return Err(conflict("此表正在加载，请等待完成或取消任务"));
            }
            if running {
                return Err(Error::busy());
            }
            let run = prepare_run(&mut record.table, mode, scheduled, record.head.is_some())?;
            record.pending = Some(Pending {
                run: run.id,
                owner: owner.clone(),
            });
            if !self.save(&record, rev, leader).await.map_err(error)? {
                continue;
            }

            let app = Arc::clone(app);
            let managed = self.clone();
            let task = run.clone();
            let cancel = app.shutdown.child_token();
            app.local_tables
                .running
                .lock()
                .unwrap()
                .insert(key.clone(), cancel.clone());
            tokio::spawn(async move {
                use futures::FutureExt;
                let sync_app = Arc::clone(&app);
                let sync_managed = managed.clone();
                let sync_record = record.clone();
                let sync_task = task.clone();
                let sync_cancel = cancel.clone();
                let mut worker = tokio::spawn(async move {
                    std::panic::AssertUnwindSafe(sync_managed.sync(
                        &sync_app,
                        &sync_record,
                        &sync_task,
                        &sync_cancel,
                    ))
                    .catch_unwind()
                    .await
                });
                let mut completed = false;
                let result = tokio::select! {
                    biased;
                    _ = lease.cancelled() => {
                        cancel.cancel();
                        Err(anyhow::anyhow!("managed sync interrupted by leadership change"))
                    }
                    _ = cancel.cancelled() => Err(anyhow::anyhow!("managed sync cancelled")),
                    result = tokio::time::timeout(
                        Duration::from_secs(record.table.config.timeout_seconds),
                        &mut worker,
                    ) => {
                        match result {
                            Ok(Ok(Ok(result))) => {
                                completed = true;
                                result
                            }
                            Ok(Ok(Err(_))) | Ok(Err(_)) => {
                                completed = true;
                                Err(anyhow::anyhow!("managed local-table load panicked"))
                            }
                            Err(_) => {
                                cancel.cancel();
                                Err(anyhow::anyhow!("deadline exceeded"))
                            }
                        }
                    }
                };
                drop(permit);
                let mut logged = false;
                let confirmation_deadline = tokio::time::sleep(Duration::from_secs(30));
                tokio::pin!(confirmation_deadline);
                let mut confirmed = false;
                loop {
                    let finished = tokio::select! {
                        biased;
                        _ = app.shutdown.cancelled() => break,
                        _ = &mut confirmation_deadline => break,
                        finished = managed.finish(&app, id, task.id, &owner, &result, cancel.is_cancelled()) => finished,
                    };
                    match finished {
                        Ok(ok) => {
                            confirmed = ok;
                            break;
                        }
                        Err(e) => {
                            if !logged {
                                let _ = writeln!(
                                    std::io::stderr().lock(),
                                    "GraphLake managed local sync result not confirmed, retrying: {e}"
                                );
                                logged = true;
                            }
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                }
                if !completed {
                    // Wait for any in-flight blocking task using this workspace to exit
                    // before clearing `running`, preventing concurrent workspace corruption.
                    tokio::select! {
                        _ = app.shutdown.cancelled() => {}
                        _ = worker => {}
                    }
                } else if confirmed && result.is_ok() {
                    let path = Self::workspace(&app, id);
                    let _ = tokio::task::spawn_blocking(move || load::clean_parts(&path)).await;
                }
                app.local_tables.running.lock().unwrap().remove(&key);
            });
            return Ok(run);
        }
        Err(Error::busy())
    }

    fn workspace(app: &App, id: Uuid) -> PathBuf {
        app.local_tables
            .workspace
            .as_ref()
            .expect("managed sync workspace")
            .path()
            .join(id.to_string())
            .join(".graphlake-local")
            .join(format!("{id}.parquet"))
    }

    async fn sync(
        &self,
        app: &App,
        record: &Record,
        run: &Run,
        cancel: &CancellationToken,
    ) -> Result<(LoadResult, Option<Head>)> {
        let id = record.table.data_id;
        let path = Self::workspace(app, id);
        let previous = match &record.head {
            Some(head) => Some(self.snapshot(id, head).await?),
            None => None,
        };
        let cached = app.local_tables.cached.lock().unwrap().remove(&id);
        let committed = record.head.as_ref().map(|h| h.generation);
        if !cached.is_some_and(|(generation, version)| {
            Some(generation) == committed && Some(version) == WorkspaceVersion::read(&path)
        }) {
            if let Some(previous) = &previous {
                self.download(id, previous, &path).await?;
            } else {
                let root = path.parent().unwrap();
                match tokio::fs::remove_dir_all(root).await {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error.into()),
                }
                tokio::fs::create_dir_all(root).await?;
            }
        }
        // Keep the authoritative files through the local rewrite and S3/etcd
        // publish. The next run can reclaim them once it knows the committed head.
        crate::blocking(|| load::clean_parts(&path));
        if previous.as_ref().is_some_and(|s| s.snapshot.is_none()) {
            preserve_parquet_anchor(&path).await?;
        }
        // A failure before replacing the anchor leaves the downloaded generation reusable.
        if let (Some(generation), Some(version)) = (committed, WorkspaceVersion::read(&path)) {
            app.local_tables
                .cached
                .lock()
                .unwrap()
                .insert(id, (generation, version));
        }
        let (loaded, rows, reads, _) = load::run(
            &record.table,
            &path,
            run.mode,
            QueryOptions {
                cancellation: cancel.clone(),
                timeout: Duration::from_secs(record.table.config.timeout_seconds),
                ..app.options.clone()
            },
            &app.admission,
        )
        .await?;
        ensure!(!cancel.is_cancelled(), "managed sync cancelled");
        let next = self
            .upload(id, run.id, &path, previous.as_ref(), None)
            .await?;
        let generation = next.as_ref().map(|h| h.generation);
        if let (Some(generation), Some(version)) =
            (generation.or(committed), WorkspaceVersion::read(&path))
        {
            app.local_tables
                .cached
                .lock()
                .unwrap()
                .insert(id, (generation, version));
        }
        Ok(((loaded, rows, reads, generation), next))
    }

    async fn finish(
        &self,
        app: &App,
        id: Uuid,
        run: Uuid,
        owner_id: &str,
        outcome: &Result<(LoadResult, Option<Head>)>,
        cancelled: bool,
    ) -> Result<bool> {
        let leader = leader(app).map_err(api_error)?;
        if leader.loss_token().is_cancelled() || owner_id != owner(leader) {
            return Ok(false);
        }
        leader.confirm().await.map_err(api_error)?;
        for _ in 0..8 {
            let Some((mut record, rev)) = self.record(id).await? else {
                return Ok(false);
            };
            if !record
                .pending
                .as_ref()
                .is_some_and(|p| p.run == run && p.owner == owner_id)
            {
                return Ok(false);
            }
            let mut retired = None;
            let result = match outcome {
                Ok((result, head)) => {
                    if let Some(head) = head.clone()
                        && let Some(old) = record.head.replace(head)
                    {
                        retired = Some(Historical {
                            head: old,
                            retired_ms: now(),
                            marked_ms: None,
                        });
                    }
                    Ok(result.clone())
                }
                Err(e) => Err(anyhow::anyhow!("{e}")),
            };
            record_result(&mut record.table, result, cancelled);
            record.pending = None;
            if self
                .save_retiring(&record, rev, leader, retired.as_ref())
                .await?
            {
                return Ok(true);
            }
            leader.confirm().await.map_err(api_error)?;
        }
        anyhow::bail!("managed table changed too often to finish sync")
    }

    pub(in crate::admin::local_tables) async fn tick(
        &self,
        app: &Arc<App>,
        cache: &mut ScheduleCache,
    ) {
        let Ok(leader) = leader(app) else {
            return;
        };
        let owner = owner(leader);
        // Incremental ranges omit deleted keys, so rebuild periodically.
        // Scheduling and orphan recovery still compare current etcd state.
        let full = cache.next_full <= std::time::Instant::now();
        let since = if full {
            0
        } else {
            cache.revision.saturating_add(1)
        };
        let Ok((revision, records)) = self.records_since(since).await else {
            return;
        };
        if full {
            cache.records.clear();
            cache.next_full = std::time::Instant::now() + Duration::from_secs(60);
        }
        cache.revision = revision;
        cache.records.extend(
            records
                .into_iter()
                .map(|(record, rev)| (record.table.data_id, (record, rev))),
        );
        let mut due = Vec::new();
        for (record, rev) in cache.records.values() {
            if record.pending.is_some() {
                // start() holds the same lock across publishing pending and
                // registering the task locally.
                let _serial = app.local_tables.create_work.lock().await;
                let key = format!("{}/{}", record.table.model, record.table.config.name);
                let running = app.local_tables.running.lock().unwrap().contains_key(&key);
                let mut record = record.clone();
                if recover(&mut record, &owner, running) {
                    let _ = self.save(&record, *rev, leader).await;
                }
                continue;
            }
            let failures = record
                .table
                .history
                .iter()
                .take_while(|run| {
                    matches!(run.status.as_str(), "failed" | "cancelled" | "interrupted")
                })
                .count();
            if failures > 0
                && record.table.history[0].finished_at_ms.is_some_and(|last| {
                    now().saturating_sub(last) < 30_000 * (1 << (failures - 1).min(5))
                })
            {
                continue;
            }
            if let Some(earliest) = [
                record.table.next_run_at_ms,
                record.table.next_reconcile_at_ms,
            ]
            .into_iter()
            .flatten()
            .filter(|due| *due <= now())
            .min()
            {
                due.push((earliest, record.table.data_id));
            }
        }
        due.sort_unstable();
        for (_, data_id) in due {
            if app.local_tables.slots.available_permits() == 0 {
                break;
            }
            let _ = self.start(app, data_id, None, true).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn orphaned_confirmation_can_recover_without_changing_the_committed_head() -> Result<()> {
        let id = Uuid::new_v4();
        let mut table: Table = serde_json::from_value(json!({
            "model":"model", "data_id":id, "revision":Uuid::new_v4(), "managed":true,
            "source":{"type":"sqlite","path":"unused"},
            "config":{"name":"local", "source_table":"main.events", "columns":["id"], "mode":"full"},
            "history":[]
        }))?;
        let run = prepare_run(&mut table, None, false, true).map_err(api_error)?;
        let head = Head {
            generation: Uuid::new_v4(),
            upload: Uuid::new_v4(),
            sha256: "digest".into(),
            watermark: Some(json!(42)),
        };
        let mut record = Record {
            table,
            head: Some(head.clone()),
            retired: Default::default(),
            pending: Some(Pending {
                run: run.id,
                owner: "leader".into(),
            }),
        };
        assert!(!recover(&mut record, "leader", true));
        assert_eq!(record.table.history[0].status, "running");
        assert!(recover(&mut record, "leader", false));
        assert_eq!(record.table.history[0].status, "interrupted");
        assert!(record.pending.is_none());
        assert_eq!(record.head, Some(head));
        Ok(())
    }
}
