use std::collections::{BTreeMap, HashMap, HashSet};

use anyhow::{Context, Result, ensure};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use chrono::{DateTime, TimeDelta, Utc};
use futures::{StreamExt, TryStreamExt, stream};
use object_store::{ObjectMeta, path::Path as Key};
use serde_json::{Value, json};
use uuid::Uuid;

use super::{
    Metadata, Objects,
    control::Receipt,
    store::{Job, JobState},
};

const GRACE: TimeDelta = TimeDelta::hours(24);

pub(super) async fn sweep(
    metadata: &Metadata,
    objects: &Objects,
    now: DateTime<Utc>,
) -> Result<()> {
    let csr = objects.prefix.clone().join("csr");
    let catalog = objects.prefix.clone().join("control").join("catalog");
    let retired = objects.prefix.clone().join("retired");
    let (csr_objects, catalog_objects, markers) = tokio::try_join!(
        inventory(objects, &csr),
        inventory(objects, &catalog),
        inventory(objects, &retired),
    )?;
    let mut generations = BTreeMap::<Key, Vec<ObjectMeta>>::new();
    for object in csr_objects {
        let Some(relative) = object.location.prefix_match(&csr) else {
            continue;
        };
        let parts = relative.collect::<Vec<_>>();
        if let [model, revision, generation, _, ..] = parts.as_slice()
            && Uuid::parse_str(generation.as_ref()).is_ok()
        {
            let key = csr
                .clone()
                .join(model.clone())
                .join(revision.clone())
                .join(generation.clone());
            generations.entry(key).or_default().push(object);
        }
    }
    for object in catalog_objects {
        if object.location.parts_count() == catalog.parts_count() + 1
            && object
                .location
                .filename()
                .is_some_and(|name| Uuid::parse_str(name).is_ok())
        {
            generations.insert(object.location.clone(), vec![object]);
        }
    }
    let mut markers = markers
        .into_iter()
        .map(|meta| (meta.location, meta.last_modified))
        .collect::<HashMap<_, _>>();
    if generations.is_empty() && markers.is_empty() {
        return Ok(());
    }
    let live = live_generations(metadata, objects).await?;
    let cutoff = now - GRACE;
    let mut actions = Vec::new();
    for (generation, files) in generations {
        let marker = retired
            .clone()
            .join(crate::admin::models::revision(generation.as_ref()));
        let marked = markers.remove(&marker);
        if live.contains(&generation) {
            if marked.is_some() {
                actions.push((marker, Some(Vec::new())));
            }
            continue;
        }
        if files.iter().any(|file| file.last_modified > cutoff) {
            continue;
        }
        match marked {
            Some(since) if since <= cutoff => actions.push((marker, Some(files))),
            None => actions.push((marker, None)),
            Some(_) => {}
        }
    }
    actions.extend(markers.into_keys().map(|marker| (marker, Some(Vec::new()))));
    stream::iter(actions)
        .map(Ok)
        .try_for_each_concurrent(8, |(marker, files)| async move {
            match files {
                None => objects.put_immutable(&marker, Vec::new()).await,
                Some(mut files) => {
                    // Close the snapshot to new exploration before removing
                    // its blocks. Keep the retirement marker across retries.
                    if let Some(index) = files
                        .iter()
                        .position(|file| file.location.filename() == Some("manifest.json"))
                    {
                        objects
                            .delete_local(&files.swap_remove(index).location)
                            .await?;
                    }
                    let deletions = stream::iter(files.into_iter().map(|file| Ok(file.location)));
                    let mut deleted = objects.store.delete_stream(deletions.boxed());
                    while deleted.try_next().await?.is_some() {}
                    objects.delete_local(&marker).await
                }
            }
        })
        .await
}

async fn inventory(objects: &Objects, prefix: &Key) -> Result<Vec<ObjectMeta>> {
    Ok(objects.store.list(Some(prefix)).try_collect().await?)
}

async fn live_generations(metadata: &Metadata, objects: &Objects) -> Result<HashSet<Key>> {
    let key = |suffix: &str| format!("{}/{suffix}", metadata.prefix);
    let range = |prefix: &str| {
        let mut end = key(prefix).into_bytes();
        *end.last_mut().expect("nonempty prefix") += 1;
        json!({"request_range":{"key":STANDARD.encode(key(prefix)), "range_end":STANDARD.encode(end)}})
    };
    // A single linearizable revision covers jobs, current pointers and receipts.
    // UUIDs are published once. The extra age and retirement windows cover
    // unpublished writes and readers admitted before a pointer is replaced.
    let result = metadata
        .call(
            "/v3/kv/txn",
            json!({"success":[
                range("csr/"), range("jobs/"),
                {"request_range":{"key":STANDARD.encode(key("coordinator/catalog"))}},
                range("coordinator/receipts/"),
            ]}),
        )
        .await?;
    let responses = result["responses"]
        .as_array()
        .context("missing retention roots")?;
    ensure!(responses.len() == 4, "incomplete retention roots");
    let entries = |i: usize| -> Result<&[Value]> {
        let response = responses[i]["response_range"]
            .as_object()
            .context("missing retention range")?;
        ensure!(
            response.get("more") != Some(&Value::Bool(true)),
            "incomplete retention range"
        );
        match response.get("kvs") {
            Some(values) => Ok(values.as_array().context("invalid retention entries")?),
            None => Ok(&[]),
        }
    };
    let decode = |entry: &Value, field| -> Result<Vec<u8>> {
        Ok(STANDARD.decode(entry[field].as_str().context("invalid retention entry")?)?)
    };
    let csr_prefix = key("csr/");
    let mut live = HashSet::new();
    for entry in entries(0)? {
        let key = String::from_utf8(decode(entry, "key")?)?;
        let (model, revision) = key
            .strip_prefix(&csr_prefix)
            .and_then(|suffix| suffix.split_once('/'))
            .context("invalid CSR pointer key")?;
        live.insert(objects.graph(model, revision, Uuid::from_slice(&decode(entry, "value")?)?));
    }
    for entry in entries(1)? {
        let job: Job = serde_json::from_slice(&decode(entry, "value")?)?;
        if let Some(generation) = job.csr_generation
            && matches!(job.state, JobState::Running)
        {
            live.insert(objects.graph(&job.model, &job.revision, generation));
        }
    }
    for entry in entries(2)? {
        let generation: String = serde_json::from_slice(&decode(entry, "value")?)?;
        live.insert(objects.control_catalog(&generation));
    }
    for entry in entries(3)? {
        let receipt: Receipt = serde_json::from_slice(&decode(entry, "value")?)?;
        live.insert(objects.control_catalog(&receipt.generation));
    }
    Ok(live)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin::{access::Principal, cluster::control::Control};

    #[tokio::test]
    #[ignore = "requires GRAPHLAKE_TEST_ETCD"]
    async fn sweep_keeps_current_state_jobs_and_leased_receipts() -> Result<()> {
        let endpoints = std::env::var("GRAPHLAKE_TEST_ETCD_MEMBERS")
            .or_else(|_| std::env::var("GRAPHLAKE_TEST_ETCD"))?
            .split(',')
            .map(str::parse)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let name = format!("retention-test-{}", Uuid::new_v4());
        let metadata = Metadata::new(reqwest::Client::new(), endpoints, &name);
        let dir = tempfile::tempdir()?;
        let storage = std::env::var("GRAPHLAKE_TEST_RETENTION_STORAGE")
            .unwrap_or_else(|_| dir.path().display().to_string());
        let objects = Objects::open(&storage, &name)?;
        let control = Control::new(metadata.clone(), objects.clone());
        let stop = crate::CancellationToken::new();
        let leader = control.acquire(&stop).await?;

        let old = Uuid::new_v4();
        let current = Uuid::new_v4();
        let job_id = Uuid::new_v4();
        let csr = |generation| objects.graph("model", "revision", generation);
        let old_files = [
            csr(old).join("manifest.json"),
            csr(old).join("0").join("graph"),
        ];
        for key in old_files
            .iter()
            .chain(&[csr(current).join("manifest.json")])
        {
            objects.put_bytes(key, vec![1]).await?;
        }
        assert!(
            metadata
                .publish_graph("model", "revision", 0, old, Some(&leader))
                .await?
        );
        let (_, version) = metadata.graph("model", "revision").await?.unwrap();
        assert!(
            metadata
                .publish_graph("model", "revision", version, current, Some(&leader))
                .await?
        );
        assert!(
            metadata
                .create(
                    job_id,
                    &Job {
                        model: "model".into(),
                        revision: "revision".into(),
                        owner: Principal::Admin,
                        attempt: Uuid::new_v4(),
                        worker: None,
                        worker_seen_ms: 0,
                        retried: false,
                        managed_only: false,
                        fingerprint: [0; 32],
                        export: false,
                        csr_generation: Some(old),
                        freshness: None,
                        deadline_ms: super::super::now_ms() + 60_000,
                        state: JobState::Running,
                    },
                    60,
                    Some(&leader)
                )
                .await?
        );

        let previous = Uuid::new_v4().to_string();
        let published = Uuid::new_v4().to_string();
        let orphan = objects.control_catalog(&Uuid::new_v4().to_string());
        let old_manifest = objects.control_catalog(&previous);
        let current_manifest = objects.control_catalog(&published);
        let blob = objects.control_catalog_blob(&"a".repeat(64));
        for key in [&old_manifest, &current_manifest, &orphan, &blob] {
            objects.put_bytes(key, vec![1]).await?;
        }
        let receipt = Receipt {
            fingerprint: "test".into(),
            owner: Principal::Admin,
            model: "model".into(),
            revision: "revision".into(),
            active_revision: None,
            status: 200,
            generation: previous.clone(),
            validated: false,
            committed_at_ms: super::super::now_ms(),
        };
        let request = Uuid::new_v4();
        leader
            .commit("catalog", 0, &published, Some((request, &receipt)))
            .await
            .map_err(|err| anyhow::anyhow!(err.message))?
            .context("commit catalog")?;

        let now = Utc::now();
        let retired = objects.prefix.clone().join("retired");
        sweep(&metadata, &objects, now).await?;
        assert!(inventory(&objects, &retired).await?.is_empty());
        let later = now + GRACE + TimeDelta::seconds(1);
        sweep(&metadata, &objects, later).await?;
        assert_eq!(inventory(&objects, &retired).await?.len(), 1);
        sweep(&metadata, &objects, now).await?;
        objects.object_meta(&orphan).await?;
        sweep(&metadata, &objects, later).await?;
        assert!(objects.object_meta(&orphan).await.is_err());
        for key in old_files
            .iter()
            .chain([&old_manifest, &current_manifest, &blob])
        {
            objects.object_meta(key).await?;
        }

        // Removing the leased entries simulates expiry without sleeping for their TTL.
        for suffix in [
            format!("jobs/{job_id}"),
            format!("coordinator/receipts/{request}"),
        ] {
            metadata
                .call(
                    "/v3/kv/deleterange",
                    json!({
                        "key": STANDARD.encode(format!("{}/{suffix}", metadata.prefix))
                    }),
                )
                .await?;
        }
        sweep(&metadata, &objects, later).await?;
        assert_eq!(inventory(&objects, &retired).await?.len(), 2);
        sweep(&metadata, &objects, later).await?;
        for key in old_files.iter().chain([&old_manifest]) {
            assert!(objects.object_meta(key).await.is_err(), "{key}");
        }
        for key in [
            &csr(current).join("manifest.json"),
            &current_manifest,
            &blob,
        ] {
            objects.object_meta(key).await?;
        }
        assert!(inventory(&objects, &retired).await?.is_empty());
        leader.release().await;
        Ok(())
    }
}
