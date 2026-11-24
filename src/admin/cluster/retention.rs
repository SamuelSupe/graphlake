use std::collections::{BTreeMap, HashMap, HashSet};

use anyhow::{Context, Result, ensure};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use chrono::{DateTime, TimeDelta, Utc};
use futures::{StreamExt, TryStreamExt, stream};
use object_store::{ObjectMeta, path::Path as Key};
use serde::Deserialize;
use serde_json::{Value, json};
use uuid::Uuid;

use super::{App, Metadata, Objects, control::Leadership};

const GRACE: TimeDelta = TimeDelta::hours(24);

pub(super) async fn sweep(
    metadata: &Metadata,
    objects: &Objects,
    now: DateTime<Utc>,
    leader: Option<&Leadership>,
    app: Option<&App>,
) -> Result<()> {
    confirm(leader).await?;
    let csr = objects.prefix.clone().join("csr");
    let catalog = objects.prefix.clone().join("control").join("catalog");
    let retired = objects.prefix.clone().join("retired");
    let (roots, csr_generations, catalog_objects, markers) = tokio::try_join!(
        live_generations(metadata, objects),
        csr_generations(objects, &csr),
        inventory(objects, &catalog),
        inventory(objects, &retired),
    )?;
    if let Some(app) = app {
        prune_pointers(metadata, app, &roots.pointers).await?;
    }
    let mut generations = BTreeMap::<Key, Vec<ObjectMeta>>::new();
    let cutoff = now - GRACE;
    let mut markers = markers
        .into_iter()
        .map(|meta| (meta.location, meta.last_modified))
        .collect::<HashMap<_, _>>();
    let marker_key = |generation: &Key| {
        retired
            .clone()
            .join(crate::admin::models::revision(generation.as_ref()))
    };
    // Active generations can have thousands of blocks. The directory listing
    // identifies generations without reading the block inventory of each one.
    let csr_files = stream::iter(csr_generations.into_iter().filter_map(|generation| {
        if roots.live.contains(&generation) {
            return Some((generation, false));
        }
        if markers
            .get(&marker_key(&generation))
            .is_some_and(|at| *at > cutoff)
        {
            return None;
        }
        Some((generation, true))
    }))
    .map(|(generation, read)| async move {
        let files = if read {
            inventory(objects, &generation).await?
        } else {
            Vec::new()
        };
        Ok::<_, anyhow::Error>((generation, files))
    })
    .buffer_unordered(8)
    .try_collect::<Vec<_>>()
    .await?;
    generations.extend(csr_files);
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
    let mut actions = Vec::new();
    for (generation, files) in generations {
        let marker = marker_key(&generation);
        let marked = markers.remove(&marker);
        if roots.live.contains(&generation) {
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
    actions.extend(
        markers
            .into_iter()
            .filter_map(|(marker, at)| (at <= cutoff).then_some((marker, Some(Vec::new())))),
    );
    stream::iter(actions)
        .map(Ok)
        .try_for_each_concurrent(8, |(marker, files)| async move {
            confirm(leader).await?;
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
                    while let Some(result) = deleted.next().await {
                        match result {
                            Ok(_) | Err(object_store::Error::NotFound { .. }) => {}
                            Err(error) => return Err(error.into()),
                        }
                        if let Some(leader) = leader {
                            leader
                                .admission()
                                .map_err(|error| anyhow::anyhow!(error.message))?;
                        }
                    }
                    objects.delete_local(&marker).await
                }
            }
        })
        .await
}

async fn csr_generations(objects: &Objects, root: &Key) -> Result<Vec<Key>> {
    async fn children(objects: &Objects, roots: Vec<Key>) -> Result<Vec<Key>> {
        let pages: Vec<Vec<Key>> = stream::iter(roots)
            .map(|root| async move {
                Ok::<_, object_store::Error>(
                    objects
                        .store
                        .list_with_delimiter(Some(&root))
                        .await?
                        .common_prefixes,
                )
            })
            .buffer_unordered(8)
            .try_collect()
            .await?;
        Ok(pages.into_iter().flatten().collect())
    }
    let models = children(objects, vec![root.clone()]).await?;
    let revisions = children(objects, models).await?;
    let generations = children(objects, revisions).await?;
    Ok(generations
        .into_iter()
        .filter(|key| {
            key.filename()
                .is_some_and(|name| Uuid::parse_str(name).is_ok())
        })
        .collect())
}

struct GraphPointer {
    model: String,
    revision: String,
    key: Vec<u8>,
    modified: String,
}

struct Roots {
    live: HashSet<Key>,
    pointers: Vec<GraphPointer>,
}

async fn prune_pointers(metadata: &Metadata, app: &App, pointers: &[GraphPointer]) -> Result<()> {
    // Read publication state without holding an async mutation lock. Recheck
    // under that lock before deleting, and fence HA changes in etcd as well.
    let files = app.models.lock().unwrap().read_files();
    let ids = pointers
        .iter()
        .map(|p| p.model.clone())
        .collect::<HashSet<_>>();
    let active = tokio::task::spawn_blocking(move || {
        let mut map = HashMap::new();
        for id in ids {
            match files.active_revision(&id) {
                Ok(revision) => {
                    map.insert(id, revision);
                }
                Err(error) => {
                    crate::admin::telemetry::event(
                        opentelemetry::logs::Severity::Warn,
                        "retention_model_revision_unreadable",
                        json!({"model": id, "error": error.message}),
                    );
                }
            }
        }
        map
    })
    .await?;
    for pointer in pointers {
        let Some(active_rev) = active.get(&pointer.model) else {
            continue;
        };
        if active_rev.as_deref() == Some(&pointer.revision) {
            continue;
        }
        let (ha, local) = if let Some(ha) = &app.ha {
            let Ok(lock) = ha.catalog.try_lock() else {
                return Ok(());
            };
            (Some(lock), None)
        } else {
            let Ok(lock) = app.users.writes.try_lock() else {
                return Ok(());
            };
            (None, Some(lock))
        };
        let files = app.models.lock().unwrap().read_files();
        let model = pointer.model.clone();
        let current =
            match tokio::task::spawn_blocking(move || files.active_revision(&model)).await? {
                Ok(revision) => revision,
                Err(error) => {
                    crate::admin::telemetry::event(
                        opentelemetry::logs::Severity::Warn,
                        "retention_model_revision_unreadable",
                        json!({"model": pointer.model, "error": error.message}),
                    );
                    drop((ha, local));
                    continue;
                }
            };
        if current.as_deref() != Some(&pointer.revision) {
            let fence = ha
                .as_ref()
                .zip(app.ha.as_ref())
                .map(|(catalog, ha)| (catalog.1, ha.leader.as_ref()));
            delete_pointer(metadata, pointer, fence).await?;
        }
        drop((ha, local));
    }
    Ok(())
}

async fn delete_pointer(
    metadata: &Metadata,
    pointer: &GraphPointer,
    fence: Option<(i64, &Leadership)>,
) -> Result<()> {
    let key = STANDARD.encode(&pointer.key);
    let mut compare = vec![json!({
        "key": key, "target":"MOD", "result":"EQUAL", "mod_revision":pointer.modified,
    })];
    if let Some((catalog_revision, leader)) = fence {
        leader
            .admission()
            .map_err(|error| anyhow::anyhow!(error.message))?;
        compare.push(leader.owner());
        compare.push(json!({
            "key":STANDARD.encode(format!("{}/coordinator/catalog", metadata.prefix)),
            "target":"MOD", "result":"EQUAL", "mod_revision":catalog_revision.to_string(),
        }));
    }
    metadata
        .call(
            "/v3/kv/txn",
            json!({
                "compare":compare, "success":[{"request_delete_range":{"key":key}}],
            }),
        )
        .await?;
    Ok(())
}

async fn inventory(objects: &Objects, prefix: &Key) -> Result<Vec<ObjectMeta>> {
    Ok(objects.store.list(Some(prefix)).try_collect().await?)
}

async fn confirm(leader: Option<&Leadership>) -> Result<()> {
    if let Some(leader) = leader {
        leader
            .confirm()
            .await
            .map_err(|error| anyhow::anyhow!(error.message))?;
    }
    Ok(())
}

#[derive(Deserialize)]
struct JobRoot {
    model: String,
    revision: String,
    csr_generation: Option<Uuid>,
}

#[derive(Deserialize)]
struct ReceiptRoot {
    generation: String,
}

fn entries(response: &Value) -> Result<&[Value]> {
    match response.get("kvs") {
        Some(values) => Ok(values.as_array().context("invalid retention entries")?),
        None => Ok(&[]),
    }
}

fn decode(entry: &Value, field: &str) -> Result<Vec<u8>> {
    Ok(STANDARD.decode(entry[field].as_str().context("invalid retention entry")?)?)
}

async fn visit_range(
    metadata: &Metadata,
    prefix: &str,
    revision: &str,
    mut visit: impl FnMut(&Value) -> Result<()>,
) -> Result<()> {
    let mut start = prefix.as_bytes().to_vec();
    let mut end = start.clone();
    *end.last_mut().expect("nonempty prefix") += 1;
    let end = STANDARD.encode(end);
    loop {
        let response = metadata
            .retention_range(json!({
                "key":STANDARD.encode(&start), "range_end":end, "revision":revision,
                "limit":"256", "sort_order":"ASCEND", "sort_target":"KEY",
            }))
            .await?;
        ensure!(
            response["header"]["revision"].as_str().is_some(),
            "missing retention page revision"
        );
        let page = entries(&response)?;
        for entry in page {
            visit(entry)?;
        }
        if response["more"] != true {
            return Ok(());
        }
        let mut next = decode(
            page.last()
                .context("empty retention page with continuation")?,
            "key",
        )?;
        next.push(0);
        ensure!(next > start, "retention range did not advance");
        start = next;
    }
}

async fn live_generations(metadata: &Metadata, objects: &Objects) -> Result<Roots> {
    let key = |suffix: &str| format!("{}/{suffix}", metadata.prefix);
    // Every page uses the revision of the initial linearizable read. A failed
    // or compacted page aborts collection before any objects can be deleted.
    let catalog = metadata
        .retention_range(json!({
            "key":STANDARD.encode(key("coordinator/catalog"))
        }))
        .await?;
    let revision = catalog["header"]["revision"]
        .as_str()
        .context("missing retention revision")?;
    ensure!(revision.parse::<i64>()? > 0, "invalid retention revision");
    let mut live = HashSet::new();
    for entry in entries(&catalog)? {
        let generation: String = serde_json::from_slice(&decode(entry, "value")?)?;
        live.insert(objects.control_catalog(&generation));
    }
    let prefix = key("csr/");
    let mut pointers = Vec::new();
    visit_range(metadata, &prefix, revision, |entry| {
        let key = String::from_utf8(decode(entry, "key")?)?;
        let (model, revision) = key
            .strip_prefix(&prefix)
            .and_then(|suffix| suffix.split_once('/'))
            .context("invalid CSR pointer key")?;
        live.insert(objects.graph(model, revision, Uuid::from_slice(&decode(entry, "value")?)?));
        pointers.push(GraphPointer {
            model: model.to_owned(),
            revision: revision.to_owned(),
            key: key.into_bytes(),
            modified: entry["mod_revision"]
                .as_str()
                .context("missing CSR pointer revision")?
                .into(),
        });
        Ok(())
    })
    .await?;
    visit_range(metadata, &key("jobs/"), revision, |entry| {
        // Leases remove finished jobs. Reading only their object references
        // also protects jobs written by a newer Coordinator during an upgrade.
        let job: JobRoot = serde_json::from_slice(&decode(entry, "value")?)?;
        if let Some(generation) = job.csr_generation {
            live.insert(objects.graph(&job.model, &job.revision, generation));
        }
        Ok(())
    })
    .await?;
    visit_range(metadata, &key("coordinator/receipts/"), revision, |entry| {
        let receipt: ReceiptRoot = serde_json::from_slice(&decode(entry, "value")?)?;
        live.insert(objects.control_catalog(&receipt.generation));
        Ok(())
    })
    .await?;
    Ok(Roots { live, pointers })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin::{
        access::Principal,
        cluster::{
            control::{Control, Receipt},
            store::{Job, JobState},
        },
    };

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
        let future_generation = Uuid::new_v4();
        let job_id = Uuid::new_v4();
        let csr = |generation| objects.graph("model", "revision", generation);
        let old_files = [
            csr(old).join("manifest.json"),
            csr(old).join("0").join("graph"),
        ];
        for key in old_files.iter().chain(&[
            csr(current).join("manifest.json"),
            csr(future_generation).join("manifest.json"),
        ]) {
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
                        worker_claim: None,
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
        let catalog_revision = leader
            .commit("catalog", 0, &published, Some((request, &receipt)))
            .await
            .map_err(|err| anyhow::anyhow!(err.message))?
            .context("commit catalog")?;
        let stale = Uuid::new_v4();
        assert!(
            metadata
                .publish_graph("withdrawn", "revision", 0, stale, Some(&leader))
                .await?
        );
        let stale_file = objects
            .graph("withdrawn", "revision", stale)
            .join("manifest.json");
        objects.put_bytes(&stale_file, vec![1]).await?;
        let roots = live_generations(&metadata, &objects).await?;
        let pointer = roots
            .pointers
            .iter()
            .find(|pointer| pointer.model == "withdrawn")
            .unwrap();
        delete_pointer(&metadata, pointer, Some((catalog_revision + 1, &leader))).await?;
        assert_eq!(
            metadata.graph("withdrawn", "revision").await?.unwrap().0,
            stale
        );
        delete_pointer(&metadata, pointer, Some((catalog_revision, &leader))).await?;
        assert!(metadata.graph("withdrawn", "revision").await?.is_none());
        for start in [0, 100, 200] {
            let writes = (start..start + 100).map(|id| json!({
                "request_put":{
                    "key": STANDARD.encode(format!("{}/jobs/page-{id:04}", metadata.prefix)),
                    "value": STANDARD.encode(serde_json::to_vec(&json!({"model":"model","revision":"revision"})).unwrap()),
                }
            })).collect::<Vec<_>>();
            metadata
                .call("/v3/kv/txn", json!({"success":writes}))
                .await?;
        }
        let future_job = format!("{}/jobs/zz-upgraded", metadata.prefix);
        metadata
            .call(
                "/v3/kv/put",
                json!({
                    "key": STANDARD.encode(&future_job),
                    "value": STANDARD.encode(serde_json::to_vec(&json!({
                        "model":"model","revision":"revision","csr_generation":future_generation,
                        "state":"new_state_from_a_later_version"
                    }))?)
                }),
            )
            .await?;

        let now = Utc::now();
        let retired = objects.prefix.clone().join("retired");
        sweep(&metadata, &objects, now, Some(&leader), None).await?;
        assert!(inventory(&objects, &retired).await?.is_empty());
        let later = now + GRACE + TimeDelta::seconds(1);
        sweep(&metadata, &objects, later, Some(&leader), None).await?;
        assert_eq!(inventory(&objects, &retired).await?.len(), 2);
        sweep(&metadata, &objects, now, Some(&leader), None).await?;
        objects.object_meta(&orphan).await?;
        let after_marking = Utc::now() + GRACE + TimeDelta::seconds(1);
        sweep(&metadata, &objects, after_marking, Some(&leader), None).await?;
        assert!(objects.object_meta(&orphan).await.is_err());
        assert!(objects.object_meta(&stale_file).await.is_err());
        for key in old_files
            .iter()
            .chain([&old_manifest, &current_manifest, &blob])
        {
            objects.object_meta(key).await?;
        }
        objects
            .object_meta(&csr(future_generation).join("manifest.json"))
            .await?;

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
        sweep(&metadata, &objects, later, Some(&leader), None).await?;
        assert_eq!(inventory(&objects, &retired).await?.len(), 2);
        let after_marking = Utc::now() + GRACE + TimeDelta::seconds(1);
        sweep(&metadata, &objects, after_marking, Some(&leader), None).await?;
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
