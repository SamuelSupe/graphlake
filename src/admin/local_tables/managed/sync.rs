use std::{path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use uuid::Uuid;

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

fn recover(record: &mut Record, owner: &str) -> bool {
    if !record.pending.as_ref().is_some_and(|p| p.owner != owner) {
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
        run.error = Some("Coordinator 切换中断了任务，可基于已提交水位重试".into());
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
            retired: Vec::new(),
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
        let _serial = app.local_tables.create_work.lock().await;
        let leader = leader(app)?;
        let lease = leader.confirm().await?;
        let owner = owner(leader);
        let permit = app
            .local_tables
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::busy())?;
        for _ in 0..8 {
            let (mut record, rev) = self
                .record(id)
                .await
                .map_err(error)?
                .ok_or_else(Error::not_found)?;
            recover(&mut record, &owner);
            if record.pending.is_some() {
                return Err(conflict("此表正在加载，请等待完成或取消任务"));
            }
            let key = format!("{}/{}", record.table.model, record.table.config.name);
            if app.local_tables.running.lock().unwrap().contains_key(&key) {
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
                let _permit = permit;
                let result = tokio::select! {
                    biased;
                    _ = lease.cancelled() => Err(anyhow::anyhow!("managed sync interrupted by leadership change")),
                    _ = cancel.cancelled() => Err(anyhow::anyhow!("managed sync cancelled")),
                    result = tokio::time::timeout(Duration::from_secs(record.table.config.timeout_seconds),
                        managed.sync(&app, &record, &task, &cancel)) => {
                        result.unwrap_or_else(|_| Err(anyhow::anyhow!("deadline exceeded")))
                    }
                };
                let cached = result
                    .as_ref()
                    .ok()
                    .and_then(|(_, head)| head.as_ref().or(record.head.as_ref()))
                    .map(|head| head.generation);
                match managed
                    .finish(&app, id, task.id, &owner, result, cancel.is_cancelled())
                    .await
                {
                    Ok(true) => {
                        if let Some(generation) = cached {
                            app.local_tables
                                .cached
                                .lock()
                                .unwrap()
                                .insert(id, generation);
                        }
                    }
                    Ok(false) => {}
                    Err(e) => {
                        eprintln!("GraphLake managed local sync result not confirmed: {e}");
                        app.shutdown.cancel();
                    }
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
        let cached = app.local_tables.cached.lock().unwrap().remove(&id);
        let previous = match &record.head {
            Some(head) => Some(self.snapshot(id, head).await?),
            None => None,
        };
        if cached.is_none() || cached != record.head.as_ref().map(|h| h.generation) {
            let root = path.parent().unwrap();
            match tokio::fs::remove_dir_all(root).await {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            tokio::fs::create_dir_all(root).await?;
            if let Some(previous) = &previous {
                self.download(id, previous, &path).await?;
            }
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
        )
        .await?;
        ensure!(!cancel.is_cancelled(), "managed sync cancelled");
        let next = self
            .upload(id, run.id, &path, previous.as_ref(), None)
            .await?;
        let generation = next.as_ref().map(|h| h.generation);
        Ok(((loaded, rows, reads, generation), next))
    }

    async fn finish(
        &self,
        app: &App,
        id: Uuid,
        run: Uuid,
        owner_id: &str,
        outcome: Result<(LoadResult, Option<Head>)>,
        cancelled: bool,
    ) -> Result<bool> {
        let leader = leader(app).map_err(api_error)?;
        if leader.confirm().await.is_err() || owner_id != owner(leader) {
            return Ok(false);
        }
        for _ in 0..8 {
            let (mut record, rev) = self
                .record(id)
                .await?
                .context("managed table disappeared")?;
            if !record
                .pending
                .as_ref()
                .is_some_and(|p| p.run == run && p.owner == owner_id)
            {
                return Ok(false);
            }
            let result = match &outcome {
                Ok((result, head)) => {
                    if let Some(head) = head.clone()
                        && let Some(old) = record.head.replace(head)
                    {
                        record.retired.push(Historical {
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
            // The same transaction changes the head and its successful history
            // and compares both the previous record revision and etcd ownership.
            if self.save(&record, rev, leader).await? {
                return Ok(true);
            }
            if leader.confirm().await.is_err() {
                return Ok(false);
            }
        }
        anyhow::bail!("managed table changed too often to finish sync")
    }

    pub(in crate::admin::local_tables) async fn tick(&self, app: &Arc<App>) {
        let Ok(leader) = leader(app) else {
            return;
        };
        let owner = owner(leader);
        let Ok(records) = self.records().await else {
            return;
        };
        for (mut record, rev) in records {
            if recover(&mut record, &owner) {
                let _ = self.save(&record, rev, leader).await;
                continue;
            }
            if record.pending.is_none()
                && [
                    record.table.next_run_at_ms,
                    record.table.next_reconcile_at_ms,
                ]
                .into_iter()
                .flatten()
                .any(|due| due <= now())
            {
                let _ = self.start(app, record.table.data_id, None, true).await;
            }
        }
    }
}
