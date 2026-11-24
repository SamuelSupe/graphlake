use std::{
    collections::VecDeque,
    path::Path,
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result, ensure};
use datafusion::{catalog::TableProvider, prelude::SessionContext};
use deltalake_core::{DeltaTable, logstore::default_logstore};
use object_store::{
    ObjectMeta, ObjectStore, ObjectStoreExt, path::Path as ObjectPath, prefix::PrefixStore,
};
use sha2::{Digest, Sha256};
use url::Url;

use super::ReadStats;

const MAX_CACHE_ENTRIES: usize = 64;

pub(crate) struct SnapshotCache {
    entries: Mutex<VecDeque<CachedSnapshot>>,
    max_bytes: usize,
}

impl SnapshotCache {
    pub(crate) fn new(memory_bytes: usize) -> Self {
        Self {
            entries: Mutex::default(),
            max_bytes: (memory_bytes / 16).min(64 * 1024 * 1024),
        }
    }

    fn get(&self, matches: impl Fn(&CachedSnapshot) -> bool) -> Option<CachedSnapshot> {
        let mut entries = self.entries.lock().unwrap();
        let pos = entries.iter().position(matches)?;
        let entry = entries.remove(pos).unwrap();
        let cloned = entry.clone();
        entries.push_back(entry);
        Some(cloned)
    }

    fn replace(
        &self,
        matches: impl Fn(&CachedSnapshot) -> bool,
        entry: Option<CachedSnapshot>,
    ) -> Option<String> {
        let mut entries = self.entries.lock().unwrap();
        entries.retain(|entry| !matches(entry));
        let entry = entry.filter(|entry| entry.bytes <= self.max_bytes)?;
        while entries.len() >= MAX_CACHE_ENTRIES
            || entries
                .iter()
                .map(|entry| entry.bytes)
                .sum::<usize>()
                .saturating_add(entry.bytes)
                > self.max_bytes
        {
            entries.pop_front();
        }
        let proof = entry.proof.clone();
        entries.push_back(entry);
        Some(proof)
    }
}

#[derive(Clone)]
struct CachedSnapshot {
    url: Url,
    requested: Option<u64>,
    remote_store: Option<Arc<dyn ObjectStore>>,
    commit: ObjectMeta,
    table: DeltaTable,
    proof: String,
    bytes: usize,
}

fn snapshot_proof(table: &DeltaTable, version: u64, commit: &ObjectMeta) -> String {
    let mut hash = Sha256::new();
    if let Some(state) = table.state.as_ref() {
        hash.update(state.snapshot().metadata().id().as_bytes());
    }
    hash.update(b"\0");
    hash.update(version.to_le_bytes());
    hash.update(b"\0");
    if let Some(etag) = &commit.e_tag {
        hash.update(etag.as_bytes());
    }
    hash.update(b"\0");
    if let Some(ver) = &commit.version {
        hash.update(ver.as_bytes());
    }
    hash.update(b"\0");
    hash.update(
        commit
            .last_modified
            .timestamp_nanos_opt()
            .unwrap_or(0)
            .to_le_bytes(),
    );
    hash.update(commit.size.to_le_bytes());
    hash.finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

struct ByteCounter(usize);

impl std::io::Write for ByteCounter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0 = self.0.saturating_add(buf.len());
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

// Bound retention conservatively using active-file metadata, including variable-width
// stats and partition values. Large snapshots are loaded for the query but not retained.
fn cache_weight(table: &DeltaTable, limit: usize) -> Option<usize> {
    let snapshot = table.state.as_ref()?.snapshot();
    let log = snapshot.try_log_data().ok()?;
    let per_file = 1024usize.saturating_add(snapshot.arrow_schema().fields().len() * 128);
    let mut bytes = 4096usize.saturating_add(log.num_files().saturating_mul(per_file));
    if bytes > limit {
        return None;
    }
    for file in log.iter() {
        // Count the serialized add action without heap-allocating per-file buffers.
        let mut counter = ByteCounter(0);
        #[allow(deprecated)]
        serde_json::to_writer(&mut counter, &file.add_action()).ok()?;
        bytes = bytes.saturating_add(counter.0.saturating_mul(2));
        if bytes > limit {
            return None;
        }
    }
    Some(bytes)
}

#[cfg(feature = "admin")]
pub(crate) async fn open(
    mapping: &crate::DeltaTable,
    context: &SessionContext,
    stats: &ReadStats,
) -> Result<(Arc<dyn TableProvider>, u64)> {
    let (provider, version, _) = open_cached(mapping, context, stats, None).await?;
    Ok((provider, version))
}

pub(crate) async fn open_cached(
    mapping: &crate::DeltaTable,
    context: &SessionContext,
    stats: &ReadStats,
    cache: Option<&SnapshotCache>,
) -> Result<(Arc<dyn TableProvider>, u64, Option<String>)> {
    let url = if mapping.path.contains("://") {
        Url::parse(&mapping.path).context("invalid Delta table URI")?
    } else {
        Url::from_directory_path(std::path::absolute(Path::new(&mapping.path))?)
            .map_err(|_| anyhow::anyhow!("invalid local Delta table path"))?
    };
    ensure!(
        matches!(url.scheme(), "file" | "s3"),
        "Delta table path must be local or S3"
    );
    ensure!(
        url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none(),
        "Delta table URI must exclude credentials, query parameters and fragments"
    );
    let (store, prefix): (Arc<dyn ObjectStore>, ObjectPath) = if url.scheme() == "s3" {
        let store =
            super::storage::s3_from_env(url.host_str().context("Delta S3 URI requires a bucket")?)?;
        (store, ObjectPath::from_url_path(url.path())?)
    } else {
        let (store, prefix) = object_store::parse_url(&url)?;
        (Arc::from(store), prefix)
    };
    // Use the existing read-only object-store configuration for Delta's transaction log and data.
    let prefixed = Arc::new(PrefixStore::new(Arc::clone(&store), prefix));
    let matches =
        |cached: &CachedSnapshot| cached.url == url && cached.requested == mapping.version;
    let cached = cache.and_then(|cache| {
        cache.get(|entry| {
            matches(entry)
                && entry
                    .remote_store
                    .as_ref()
                    .is_none_or(|cached| Arc::ptr_eq(cached, &store))
        })
    });
    let cached = if let Some(cached) = cached {
        stats.request();
        let unchanged = prefixed
            .head(&cached.commit.location)
            .await
            .is_ok_and(|meta| {
                meta.e_tag == cached.commit.e_tag
                    && meta.version == cached.commit.version
                    && meta.last_modified == cached.commit.last_modified
                    && meta.size == cached.commit.size
            });
        unchanged.then_some(cached)
    } else {
        None
    };
    let mut table = cached
        .as_ref()
        .map(|entry| entry.table.clone())
        .unwrap_or_else(|| {
            DeltaTable::new(default_logstore(
                prefixed.clone(),
                store.clone(),
                &url,
                &Default::default(),
            ))
        });
    stats.request();
    match mapping.version {
        Some(_) if cached.is_some() => {}
        Some(version) => table.load_version(version).await?,
        None => table.load().await?,
    }
    let version = table
        .version()
        .context("Delta table has no committed snapshot")?;
    let mut proof = cached
        .as_ref()
        .filter(|entry| entry.table.version() == Some(version))
        .map(|entry| entry.proof.clone());
    if let Some(cache) = cache
        && proof.is_none()
    {
        let commit_path = ObjectPath::from(format!("_delta_log/{version:020}.json"));
        // Tables restored at the same path must start a new transaction log.
        // Only issue a proof when the immutable commit metadata is verifiable.
        stats.request();
        let commit = prefixed
            .head(&commit_path)
            .await
            .ok()
            .filter(|commit| commit.e_tag.is_some() || commit.version.is_some());
        let verified_proof = commit
            .as_ref()
            .map(|commit| snapshot_proof(&table, version, commit));
        let entry = if let (Some(commit), Some(verified_proof)) = (commit, verified_proof.as_ref())
        {
            let max_bytes = cache.max_bytes;
            let bytes = crate::blocking(|| cache_weight(&table, max_bytes));
            bytes.map(|bytes| CachedSnapshot {
                url: url.clone(),
                requested: mapping.version,
                remote_store: (url.scheme() == "s3").then_some(store),
                commit,
                table: table.clone(),
                proof: verified_proof.clone(),
                bytes,
            })
        } else {
            None
        };
        let _ = cache.replace(matches, entry);
        proof = verified_proof;
    }
    let mut session = context.state();
    session
        .config_mut()
        .options_mut()
        .execution
        .parquet
        .schema_force_view_types = false;
    let provider = table
        .table_provider()
        .with_session(Arc::new(session))
        .build()
        .await?;
    Ok((Arc::new(provider), version, proof))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_cache_evicts_by_total_weight_and_skips_oversized_snapshots() -> Result<()> {
        let cache = SnapshotCache::new(16 * 100);
        let dir = tempfile::tempdir()?;
        let url = Url::from_directory_path(dir.path()).unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let table = DeltaTable::new(default_logstore(
            store.clone(),
            store,
            &url,
            &Default::default(),
        ));
        let entry = |version, bytes| CachedSnapshot {
            url: url.clone(),
            requested: Some(version),
            remote_store: None,
            commit: ObjectMeta {
                location: ObjectPath::from("commit"),
                last_modified: chrono::Utc::now(),
                size: 1,
                e_tag: Some("test".into()),
                version: None,
            },
            table: table.clone(),
            proof: uuid::Uuid::new_v4().to_string(),
            bytes,
        };
        for (version, bytes) in [(0, 30), (1, 30), (2, 60)] {
            assert!(
                cache
                    .replace(
                        |cached| cached.requested == Some(version),
                        Some(entry(version, bytes))
                    )
                    .is_some()
            );
        }
        assert_eq!(
            cache
                .entries
                .lock()
                .unwrap()
                .iter()
                .map(|entry| entry.requested)
                .collect::<Vec<_>>(),
            vec![Some(1), Some(2)]
        );
        assert!(
            cache
                .replace(|cached| cached.requested == Some(1), Some(entry(1, 101)))
                .is_none()
        );
        assert_eq!(cache.entries.lock().unwrap().len(), 1);
        Ok(())
    }

    #[test]
    fn shared_cache_retains_more_than_four_tables_and_uses_lru() -> Result<()> {
        let cache = SnapshotCache::new(16 * 100);
        let dir = tempfile::tempdir()?;
        let url = Url::from_directory_path(dir.path()).unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let table = DeltaTable::new(default_logstore(
            store.clone(),
            store,
            &url,
            &Default::default(),
        ));
        let entry = |version, bytes| CachedSnapshot {
            url: url.clone(),
            requested: Some(version),
            remote_store: None,
            commit: ObjectMeta {
                location: ObjectPath::from("commit"),
                last_modified: chrono::Utc::now(),
                size: 1,
                e_tag: Some("test".into()),
                version: None,
            },
            table: table.clone(),
            proof: format!("proof-{version}"),
            bytes,
        };
        for version in 0..6 {
            assert!(
                cache
                    .replace(
                        |cached| cached.requested == Some(version),
                        Some(entry(version, 15))
                    )
                    .is_some()
            );
        }
        assert_eq!(cache.entries.lock().unwrap().len(), 6);
        // Promote version 0 via LRU hit; inserting a 25-byte entry must evict 1 (not 0).
        assert!(cache.get(|cached| cached.requested == Some(0)).is_some());
        assert!(
            cache
                .replace(|cached| cached.requested == Some(6), Some(entry(6, 25)))
                .is_some()
        );
        let retained: Vec<_> = cache
            .entries
            .lock()
            .unwrap()
            .iter()
            .map(|entry| entry.requested)
            .collect();
        assert!(
            retained.contains(&Some(0)),
            "LRU hit was evicted: {retained:?}"
        );
        assert!(
            !retained.contains(&Some(1)),
            "oldest unaccessed entry should be evicted: {retained:?}"
        );
        Ok(())
    }
}
