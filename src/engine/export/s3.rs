use std::{
    fs::File,
    io::{Read, Seek},
    path::Path,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use datafusion::execution::memory_pool::MemoryConsumer;
use object_store::{
    MultipartUpload, ObjectStore, ObjectStoreExt, PutMode,
    aws::{AmazonS3Builder, S3CopyIfNotExists},
    path::Path as Key,
};

use crate::engine::QueryScope;

pub(super) struct Target {
    store: Arc<dyn ObjectStore>,
    key: Key,
    prefix: Key,
}

impl Target {
    pub(super) fn new(directory: &str, path: &Path) -> Result<Self> {
        let url = url::Url::parse(directory).context("invalid S3 export location")?;
        ensure!(
            url.username().is_empty()
                && url.password().is_none()
                && url.port().is_none()
                && url.query().is_none()
                && url.fragment().is_none(),
            "S3 export location must be a bucket and optional prefix"
        );
        let store = AmazonS3Builder::from_env()
            .with_bucket_name(url.host_str().context("missing S3 export bucket")?)
            .with_copy_if_not_exists(S3CopyIfNotExists::Multipart)
            .build()?;
        let prefix = Key::from_url_path(url.path())?;
        let key = path.iter().fold(prefix.clone(), |key, part| {
            key.join(part.to_string_lossy().as_ref())
        });
        Ok(Self {
            store: Arc::new(store),
            key,
            prefix,
        })
    }

    pub(super) async fn publish(&self, path: &Path, scope: &QueryScope) -> Result<()> {
        let mut file = File::open(path)?;
        let size = file.metadata()?.len();
        const PART: usize = 5 * 1024 * 1024;
        ensure!(
            size <= 5 * 1024 * 1024 * 1024,
            "S3 export exceeds the 5 GiB conditional copy limit"
        );
        let budget =
            MemoryConsumer::new("GraphLake S3 upload part").register(&scope.runtime.memory_pool);
        if size <= PART as u64 {
            budget.try_grow(size as usize)?;
            let mut data = Vec::with_capacity(size as usize);
            file.read_to_end(&mut data)?;
            scope
                .run(async {
                    self.store
                        .put_opts(&self.key, data.into(), PutMode::Create.into())
                        .await?;
                    Ok(())
                })
                .await?;
            return Ok(());
        }
        budget.try_grow(PART)?;
        let key = self.prefix.clone().join(format!(
            ".graphlake-export-{}",
            uuid::Uuid::new_v4().simple()
        ));
        let mut stage = Staging {
            store: Arc::clone(&self.store),
            key,
            upload: None,
            cleaned: false,
        };
        let result = scope
            .run(async {
                stage.upload = Some(self.store.put_multipart(&stage.key).await?);
                while file.stream_position()? < size {
                    scope.check()?;
                    let mut part =
                        vec![0; (size - file.stream_position()?).min(PART as u64) as usize];
                    file.read_exact(&mut part)?;
                    stage
                        .upload
                        .as_mut()
                        .expect("open multipart upload")
                        .put_part(part.into())
                        .await?;
                }
                stage
                    .upload
                    .as_mut()
                    .expect("open multipart upload")
                    .complete()
                    .await?;
                stage.upload = None;
                self.store.copy_if_not_exists(&stage.key, &self.key).await?;
                Ok(())
            })
            .await;
        if stage.upload.is_none() {
            stage.cleaned = scope
                .run(async { Ok(self.store.delete(&stage.key).await?) })
                .await
                .is_ok();
        }
        result
    }
}

struct Staging {
    store: Arc<dyn ObjectStore>,
    key: Key,
    upload: Option<Box<dyn MultipartUpload>>,
    cleaned: bool,
}

impl Drop for Staging {
    fn drop(&mut self) {
        if !self.cleaned
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            let store = Arc::clone(&self.store);
            let key = self.key.clone();
            let upload = self.upload.take();
            runtime.spawn(async move {
                let _ = tokio::time::timeout(Duration::from_secs(5), async move {
                    if let Some(mut upload) = upload {
                        let _ = upload.abort().await;
                    }
                    let _ = store.delete(&key).await;
                })
                .await;
            });
        }
    }
}
