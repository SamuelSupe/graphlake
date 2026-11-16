use std::{collections::BTreeSet, sync::Weak, time::Duration};

use anyhow::Result;
use uuid::Uuid;

use super::{Managed, api_error, backend};
use crate::admin::{App, cluster::control::Leadership, local_tables::store::now};

impl Managed {
    async fn collect(&self, table: Uuid, leader: &Leadership, app: &App) -> Result<()> {
        // Take the object listing first. Any pre-existing object a later publisher
        // can reference is either in a retained manifest or in its pending upload.
        let listing = self.objects.local_objects(table).await?;
        let Some((mut state, revision)) = self.record(table).await? else {
            return Ok(());
        };
        let pinned = self.active_pins(table).await?;
        let now = now();
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
        if changed {
            self.save(&state, revision, leader).await?;
            return Ok(());
        }
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
            app.metrics.gc_deleted();
        }
        if state.retired.iter().any(due) {
            state.retired.retain(|item| !due(item));
            self.save(&state, revision, leader).await?;
        }
        Ok(())
    }

    async fn collect_all(&self, leader: &Leadership, app: &App) -> Result<()> {
        for (record, _) in self.records().await? {
            self.collect(record.table.data_id, leader, app).await?;
        }
        Ok(())
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
