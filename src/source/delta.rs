use std::{path::Path, sync::Arc};

use anyhow::{Context, Result, ensure};
use datafusion::{catalog::TableProvider, prelude::SessionContext};
use deltalake_core::{DeltaTable, logstore::default_logstore};
use object_store::{
    ObjectStore, aws::AmazonS3Builder, path::Path as ObjectPath, prefix::PrefixStore,
};
use url::Url;

use super::ReadStats;

pub(crate) async fn open(
    mapping: &crate::DeltaTable,
    context: &SessionContext,
    stats: &ReadStats,
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
        let store = AmazonS3Builder::from_env()
            .with_bucket_name(url.host_str().context("Delta S3 URI requires a bucket")?)
            .build()?;
        (Arc::new(store), ObjectPath::from_url_path(url.path())?)
    } else {
        let (store, prefix) = object_store::parse_url(&url)?;
        (Arc::from(store), prefix)
    };
    // Use the existing read-only object-store configuration for Delta's transaction log and data.
    let prefixed = Arc::new(PrefixStore::new(Arc::clone(&store), prefix));
    let log = default_logstore(prefixed, store, &url, &Default::default());
    let mut table = DeltaTable::new(log);
    stats.request();
    match mapping.version {
        Some(version) => table.load_version(version).await?,
        None => table.load().await?,
    }
    let version = table
        .version()
        .context("Delta table has no committed snapshot")?;
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
