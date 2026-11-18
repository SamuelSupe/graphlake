use object_store::path::Path as Key;
use std::{
    collections::{BTreeSet, VecDeque},
    sync::Weak,
    time::Duration,
};

use anyhow::Result;
use uuid::Uuid;

use super::{Managed, api_error, backend};
use crate::admin::{App, cluster::control::Leadership, local_tables::store::now, ops::Metrics};

type ManifestFiles = VecDeque<(Key, String, Vec<Key>)>;

impl Managed {
    async fn collect(
        &self,
        table: Uuid,
        leader: &Leadership,
        metrics: &Metrics,
        cache: &mut ManifestFiles,
    ) -> Result<()> {
        self.migrate_retirements(table, leader).await?;
        // Take the object listing first. Any pre-existing object a later publisher
        // can reference is either in a retained manifest or in its pending upload.
        let listing = self.objects.local_objects(table).await?;
        let Some((state, _)) = self.record(table).await? else {
            return Ok(());
        };
        let retired = self.retirements(table).await?;
        let pinned = self.active_pins(table).await?;
        let now = now();
        let due = |item: &super::metadata::Historical| {
            item.marked_ms
                .is_some_and(|marked| marked.saturating_add(self.grace_ms) <= now)
                && !pinned.contains(&item.head.generation)
        };
        let mut keep = BTreeSet::new();
        for head in state.head.iter().chain(
            retired
                .iter()
                .filter(|item| !due(item))
                .map(|item| &item.head),
        ) {
            let key = self.manifest_key(table, head);
            let entry = if let Some(index) = cache
                .iter()
                .position(|(cached, hash, _)| *cached == key && *hash == head.sha256)
            {
                cache.remove(index).expect("cached manifest")
            } else {
                let files = self.snapshot(table, head).await?.files;
                let keys = files
                    .values()
                    .map(|file| self.objects.local_key(table, &file.location))
                    .collect::<Result<Vec<_>>>()?;
                (key.clone(), head.sha256.clone(), keys)
            };
            keep.insert(key);
            keep.extend(entry.2.iter().cloned());
            cache.push_front(entry);
            cache.truncate(256);
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
        let mut reclaimed = Vec::new();
        let mut mark = Vec::new();
        for item in retired {
            if due(&item) {
                reclaimed.push(item);
            } else if item.marked_ms.is_none()
                && !pinned.contains(&item.head.generation)
                && item.retired_ms.saturating_add(self.retention_ms) <= now
            {
                mark.push(item);
            }
        }
        self.update_retirements(table, leader, &reclaimed, None)
            .await?;
        self.update_retirements(table, leader, &mark, Some(now))
            .await
    }

    async fn collect_all(
        &self,
        leader: &Leadership,
        app: &App,
        cache: &mut ManifestFiles,
    ) -> Result<()> {
        let mut result = Ok(());
        for (record, _) in self.records().await? {
            leader.admission().map_err(api_error)?;
            if let Err(error) = self
                .collect(record.table.data_id, leader, &app.metrics, cache)
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
    let mut cache = ManifestFiles::new();
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
            let result = managed.collect_all(&ha.leader, &app, &mut cache).await;
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
        local_tables::managed::metadata::{Historical, Record, Retirements},
    };
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    use serde_json::json;

    #[tokio::test]
    #[ignore = "requires disposable etcd"]
    async fn full_legacy_history_migrates_and_can_be_marked() -> Result<()> {
        let endpoint = std::env::var("GRAPHLAKE_TEST_ETCD")?.parse()?;
        let directory = tempfile::tempdir()?;
        let name = format!("managed-history-{}", Uuid::new_v4());
        let meta = Metadata::new(reqwest::Client::new(), vec![endpoint], &name);
        let objects = Objects::open(directory.path().to_str().unwrap(), &name)?;
        let leader = Control::new(meta.clone(), objects.clone())
            .acquire(&crate::CancellationToken::new())
            .await?;
        let managed = Managed::new(meta, objects, 1, 1);
        let table = Uuid::new_v4();
        let head = super::super::metadata::Head {
            generation: Uuid::new_v4(),
            upload: Uuid::new_v4(),
            sha256: "0".repeat(64),
            watermark: Some(json!(123_456_789)),
        };
        let at = now();
        let mut record = Record {
            table: serde_json::from_value(json!({
                "model":"model", "data_id":table, "revision":Uuid::new_v4(), "managed":true,
                "source":{"type":"sqlite","path":"unused"},
                "config":{"name":"test","source_table":"db.rows","columns":["id"],"mode":"full"},
                "history":[]
            }))?,
            head: Some(head.clone()),
            retired: Retirements::Inline(Vec::new()),
            pending: None,
        };
        let template = Historical {
            head,
            retired_ms: at,
            marked_ms: None,
        };
        let limit = 768 * 1024;
        let base = serde_json::to_vec(&record)?.len();
        let entry = serde_json::to_vec(&template)?.len() + 1;
        let count = (limit - base) / entry;
        let items: Vec<_> = (0..count)
            .map(|_| {
                let mut item = template.clone();
                item.head.generation = Uuid::new_v4();
                item
            })
            .collect();
        record.retired = Retirements::Inline(items);
        assert!(managed.save(&record, 0, &leader).await?);
        if let Retirements::Inline(items) = &mut record.retired {
            for item in items {
                item.marked_ms = Some(at);
            }
        }
        assert!(serde_json::to_vec(&record)?.len() > limit);

        managed.migrate_retirements(table, &leader).await?;
        let (mut current, revision) = managed.record(table).await?.unwrap();
        assert!(serde_json::to_vec(&current)?.len() < 1024);
        let mut replacement = template.clone();
        replacement.head.generation = Uuid::new_v4();
        current.head = Some(replacement.head);
        assert!(
            managed
                .save_retiring(&current, revision, &leader, Some(&template))
                .await?
        );

        let retired = managed.retirements(table).await?;
        assert_eq!(retired.len(), count + 1);
        managed
            .update_retirements(table, &leader, &retired, Some(at))
            .await?;
        let marked = managed.retirements(table).await?;
        assert!(marked.iter().all(|item| item.marked_ms == Some(at)));
        managed
            .update_retirements(table, &leader, &marked, None)
            .await?;
        assert!(managed.retirements(table).await?.is_empty());

        let empty = Uuid::new_v4();
        current.table.data_id = empty;
        current.retired = Retirements::Inline(Vec::new());
        assert!(managed.save(&current, 0, &leader).await?);
        managed.migrate_retirements(empty, &leader).await?;
        let (migrated, _) = managed.record(empty).await?.unwrap();
        assert!(
            matches!(migrated.retired, Retirements::Separate { remaining } if remaining.is_empty())
        );
        Ok(())
    }

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
        let live = managed
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
            head: Some(live.clone()),
            retired: Retirements::Inline(vec![
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
            ]),
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
        managed.migrate_retirements(table, &leader).await?;
        let (_, revision) = managed.record(table).await?.unwrap();
        let lease = managed.grant(10).await?;
        let (_, pin) = managed.pin_current(table, Uuid::new_v4(), &lease).await?;
        assert_eq!(pin.unwrap().head, live);
        assert_eq!(managed.record(table).await?.unwrap().1, revision);
        // The object store timestamps uploads with second-level precision.
        tokio::time::sleep(Duration::from_millis(1200)).await;
        managed
            .collect(table, &leader, &Metrics::new(1), &mut ManifestFiles::new())
            .await?;
        let history = managed.retirements(table).await?;
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].head, fresh);
        assert!(history[0].marked_ms.is_some());
        assert!(!objects.exists(&managed.manifest_key(table, &old)).await?);
        assert!(objects.exists(&managed.manifest_key(table, &fresh)).await?);
        assert!(objects.exists(&managed.manifest_key(table, &live)).await?);
        managed.release(&lease).await;
        leader.release().await;
        Ok(())
    }
}
