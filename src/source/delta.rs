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
use url::Url;

use super::ReadStats;

#[derive(Default)]
pub(crate) struct SnapshotCache(Mutex<VecDeque<CachedSnapshot>>);

#[derive(Clone)]
struct CachedSnapshot {
    url: Url,
    requested: Option<u64>,
    remote_store: Option<Arc<dyn ObjectStore>>,
    commit: ObjectMeta,
    table: DeltaTable,
}

#[cfg(feature = "admin")]
pub(crate) async fn open(
    mapping: &crate::DeltaTable,
    context: &SessionContext,
    stats: &ReadStats,
) -> Result<(Arc<dyn TableProvider>, u64)> {
    open_cached(mapping, context, stats, None).await
}

pub(crate) async fn open_cached(
    mapping: &crate::DeltaTable,
    context: &SessionContext,
    stats: &ReadStats,
    cache: Option<&SnapshotCache>,
) -> Result<(Arc<dyn TableProvider>, u64)> {
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
        cache
            .0
            .lock()
            .unwrap()
            .iter()
            .find(|entry| {
                matches(entry)
                    && entry
                        .remote_store
                        .as_ref()
                        .is_none_or(|cached| Arc::ptr_eq(cached, &store))
            })
            .cloned()
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
    if let Some(cache) = cache
        && cached
            .as_ref()
            .is_none_or(|entry| entry.table.version() != Some(version))
    {
        let commit = ObjectPath::from(format!("_delta_log/{version:020}.json"));
        stats.request();
        // Tables restored at the same path must start a new transaction log.
        // Only reuse a snapshot while its immutable commit is still present.
        if let Ok(commit) = prefixed.head(&commit).await
            && (commit.e_tag.is_some() || commit.version.is_some())
        {
            let mut entries = cache.0.lock().unwrap();
            entries.retain(|entry| !matches(entry));
            if entries.len() == 32 {
                entries.pop_front();
            }
            entries.push_back(CachedSnapshot {
                url: url.clone(),
                requested: mapping.version,
                remote_store: (url.scheme() == "s3").then_some(store),
                commit,
                table: table.clone(),
            });
        }
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
    Ok((Arc::new(provider), version))
}
