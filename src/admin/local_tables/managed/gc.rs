use std::{collections::BTreeSet, sync::Weak, time::Duration};

use anyhow::Result;
use uuid::Uuid;

use super::{Managed, api_error, backend};
use crate::admin::{App, cluster::control::Leadership, local_tables::store::now, ops::Metrics};

impl Managed {
    async fn collect(&self, table: Uuid, leader: &Leadership, metrics: &Metrics) -> Result<()> {
        // Take the object listing first. Any pre-existing object a later publisher
        // can reference is either in a retained manifest or in its pending upload.
        let listing = self.objects.local_objects(table).await?;
        let Some((mut state, revision)) = self.record(table).await? else {
            return Ok(());
        };
        let pinned = self.active_pins(table).await?;
        let now = now();
        let due = |item: &super::metadata::Historical| {
            item.marked_ms
                .is_some_and(|marked| marked.saturating_add(self.grace_ms) <= now)
                && !pinned.contains(&item.head.generation)
        };
        let mut keep = BTreeSet::new();
        for head in state.head.iter().chain(
            state
                .retired
                .iter()
                .filter(|item| !due(item))
                .map(|item| &item.head),
        ) {
            keep.insert(self.manifest_key(table, head));
            for file in self.snapshot(table, head).await?.files.values() {
                keep.insert(self.objects.local_key(table, &file.location)?);
            }
        }
        let pending = state
            .pending
            .as_ref()
            .map(|pending| self.upload_prefix(table, pending.run));
        for object in listing {
            if keep.contains(&object.location)
                || pending
                    .as_ref()
                    .is_some_and(|prefix| object.location.prefix_matches(prefix))
                || object.last_modified.timestamp_millis().max(0) as u64 + self.grace_ms > now
            {
                continue;
            }
            // Deleting an unreachable UUID object is irreversible; never keep
            // deleting through a loss of metadata access or coordinator ownership.
            leader.confirm().await.map_err(api_error)?;
            self.objects.delete_local(&object.location).await?;
            metrics.gc_deleted();
        }
        let before = state.retired.len();
        state.retired.retain(|item| !due(item));
        let mut changed = false;
        for item in &mut state.retired {
            if !pinned.contains(&item.head.generation)
                && item.marked_ms.is_none()
                && item.retired_ms.saturating_add(self.retention_ms) <= now
            {
                item.marked_ms = Some(now);
                changed = true;
            }
        }
        if changed || state.retired.len() != before {
            self.save(&state, revision, leader).await?;
        }
        Ok(())
    }

    async fn collect_all(&self, leader: &Leadership, app: &App) -> Result<()> {
        let mut result = Ok(());
        for (record, _) in self.records().await? {
            leader.admission().map_err(api_error)?;
            if let Err(error) = self
                .collect(record.table.data_id, leader, &app.metrics)
                .await
            {
                result =
                    Err(error.context(format!("collecting local table {}", record.table.data_id)));
            }
        }
        result
    }
}

pub(in crate::admin) async fn watch(app: Weak<App>) {
    loop {
        let Some(app) = app.upgrade() else {
            return;
        };
        let (Some(managed), Some(ha)) = (backend(&app), &app.ha) else {
            return;
        };
        if app.shutdown.is_cancelled() {
            return;
        }
        let interval = Duration::from_millis(managed.grace_ms.clamp(1000, 60_000));
        if !ha.recovery_pending() && ha.leader.confirm().await.is_ok() {
            let result = managed.collect_all(&ha.leader, &app).await;
            app.metrics
                .gc_finished(result.as_ref().ok().map(|()| now()));
            if let Err(error) = result {
                eprintln!("GraphLake managed local snapshot GC paused: {error}");
            }
        }
        drop(app);
        tokio::time::sleep(interval).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin::{
        cluster::{
            control::Control,
            store::{Metadata, Objects},
        },
        local_tables::managed::metadata::{Historical, Record},
    };
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    use serde_json::json;

    #[tokio::test]
    #[ignore = "requires disposable etcd and S3 fixtures"]
    async fn newly_retired_generation_does_not_delay_sweeping_an_old_one() -> Result<()> {
        let endpoint = std::env::var("GRAPHLAKE_TEST_ETCD")?.parse()?;
        let bucket = std::env::var("GRAPHLAKE_TEST_BUCKET")?;
        let name = format!("managed-gc-{}", Uuid::new_v4());
        let meta = Metadata::new(reqwest::Client::new(), vec![endpoint], &name);
        let objects = Objects::open(&format!("s3://{bucket}"), &name)?;
        let leader = Control::new(meta.clone(), objects.clone())
            .acquire(&crate::CancellationToken::new())
            .await?;
        let managed = Managed::new(meta.clone(), objects.clone(), 1, 1);
        let scratch = tempfile::NamedTempFile::new()?;
        std::fs::write(scratch.path(), b"test payload")?;
        let table = Uuid::new_v4();
        let old = managed
            .upload(table, Uuid::new_v4(), scratch.path(), None, None)
            .await?
            .unwrap();
        let fresh = managed
            .upload(table, Uuid::new_v4(), scratch.path(), None, None)
            .await?
            .unwrap();
        let retired_ms = now().saturating_sub(2_000);
        let record = Record {
            table: serde_json::from_value(json!({
                "model":"model", "data_id":table, "revision":Uuid::new_v4(), "managed":true,
                "source":{"type":"sqlite","path":"unused"},
                "config":{"name":"test", "source_table":"db.rows", "columns":["id"], "mode":"full"},
                "history":[]
            }))?,
            head: None,
            retired: vec![
                Historical {
                    head: old.clone(),
                    retired_ms,
                    marked_ms: Some(retired_ms),
                },
                Historical {
                    head: fresh.clone(),
                    retired_ms,
                    marked_ms: None,
                },
            ],
            pending: None,
        };
        meta.call(
            "/v3/kv/put",
            json!({
                "key": STANDARD.encode(format!("{}/local-table/state/{table}", meta.prefix)),
                "value": STANDARD.encode(serde_json::to_vec(&record)?),
            }),
        )
        .await?;
        // The object store timestamps uploads with second-level precision.
        tokio::time::sleep(Duration::from_millis(1200)).await;
        managed.collect(table, &leader, &Metrics::new(1)).await?;
        let (next, _) = managed.record(table).await?.unwrap();
        assert_eq!(next.retired.len(), 1);
        assert_eq!(next.retired[0].head, fresh);
        assert!(next.retired[0].marked_ms.is_some());
        assert!(!objects.exists(&managed.manifest_key(table, &old)).await?);
        assert!(objects.exists(&managed.manifest_key(table, &fresh)).await?);
        leader.release().await;
        Ok(())
    }
}
