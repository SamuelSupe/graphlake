use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use object_store::{
    ObjectStore, ObjectStoreExt, PutMode, aws::AmazonS3Builder, local::LocalFileSystem,
    path::Path as Key,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use tokio::io::AsyncReadExt;
use uuid::Uuid;

use crate::admin::{access::Principal, query::download::Captured};

#[derive(Clone, Deserialize, Serialize)]
pub(super) struct Job {
    pub model: String,
    pub revision: String,
    pub owner: Principal,
    pub attempt: Uuid,
    pub worker: Option<Uuid>,
    pub worker_seen_ms: u64,
    #[serde(default, skip_serializing_if = "is_false")]
    pub retried: bool,
    pub fingerprint: [u8; 32],
    pub export: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub csr_generation: Option<Uuid>,
    pub deadline_ms: u64,
    pub state: JobState,
}

fn is_false(value: &bool) -> bool {
    !*value
}

impl Job {
    pub fn retention_ms(&self) -> u64 {
        if self.export {
            60_000
        } else {
            super::RESULT_TTL * 1000
        }
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub(super) enum JobState {
    Running,
    Done {
        arrow: bool,
        graph_pages: usize,
        completed_ms: u64,
    },
    Failed {
        code: String,
        message: String,
        status: u16,
    },
    Cancelled,
    Downloaded,
}

pub(super) struct VersionedJob {
    pub job: Job,
    pub revision: i64,
}

#[derive(Clone)]
pub(super) struct Metadata {
    client: reqwest::Client,
    endpoints: Arc<[url::Url]>,
    preferred: Arc<AtomicUsize>,
    pub(super) prefix: String,
}

impl Metadata {
    pub fn new(client: reqwest::Client, endpoints: Vec<url::Url>, name: &str) -> Self {
        Self {
            client,
            endpoints: endpoints.into(),
            preferred: Arc::default(),
            prefix: format!("/graphlake/v1/{name}"),
        }
    }

    pub(super) async fn call(&self, path: &str, body: Value) -> Result<Value> {
        let read_only = path == "/v3/kv/range"
            || path == "/v3/kv/txn"
                && ["success", "failure"].iter().all(|branch| {
                    body[*branch].is_null()
                        || body[*branch].as_array().is_some_and(|ops| {
                            ops.iter().all(|op| op.get("request_range").is_some())
                        })
                });
        // A lost lease-grant reply can leave an unused lease until its TTL.
        // It carries no lock or job, so trying another member is safe.
        let retry = read_only || path == "/v3/lease/grant";
        self.try_endpoints(retry, |endpoint| {
            let request = self
                .client
                .post(endpoint.join(path).expect("validated etcd endpoint"))
                .timeout(Duration::from_millis(
                    if retry && self.endpoints.len() > 1 {
                        210
                    } else {
                        2000
                    },
                ))
                .json(&body);
            async move { Ok(request.send().await?.error_for_status()?.json().await?) }
        })
        .await
    }

    pub async fn keep_alive(&self, id: &str) -> Result<bool> {
        self.try_endpoints(true, |endpoint| {
            let request = self
                .client
                .post(
                    endpoint
                        .join("/v3/lease/keepalive")
                        .expect("validated etcd endpoint"),
                )
                .timeout(Duration::from_millis(if self.endpoints.len() > 1 {
                    210
                } else {
                    950
                }))
                .json(&json!({"ID":id}));
            async move {
                let response = request.send().await?.error_for_status()?;
                Self::lease_response(response).await
            }
        })
        .await
    }

    async fn try_endpoints<T, F, R>(&self, retry: bool, request: F) -> Result<T>
    where
        F: Fn(&url::Url) -> R,
        R: Future<Output = Result<T>>,
    {
        let start = self.preferred.load(Ordering::Relaxed);
        let mut last = None;
        for offset in 0..self.endpoints.len() {
            let index = (start + offset) % self.endpoints.len();
            match request(&self.endpoints[index]).await {
                Ok(value) => {
                    self.preferred.store(index, Ordering::Relaxed);
                    return Ok(value);
                }
                Err(error) => {
                    // An ambiguous write can already be committed. Let the caller
                    // reconcile it through the fence or existing mutation receipt.
                    if !retry
                        && !error
                            .downcast_ref::<reqwest::Error>()
                            .is_some_and(|e| e.is_connect())
                    {
                        self.preferred
                            .store((index + 1) % self.endpoints.len(), Ordering::Relaxed);
                        return Err(error);
                    }
                    last = Some(error);
                }
            }
        }
        Err(last.expect("at least one configured etcd endpoint"))
    }

    async fn lease_response(mut response: reqwest::Response) -> Result<bool> {
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            bytes.extend_from_slice(&chunk);
            ensure!(bytes.len() <= 16384, "oversized lease response");
            if let Ok(value) = serde_json::from_slice::<Value>(&bytes) {
                let result = value["result"]
                    .as_object()
                    .context("invalid lease response")?;
                // etcd omits the zero TTL in its JSON response for an expired lease.
                ensure!(
                    result.get("ID").and_then(Value::as_str).is_some(),
                    "missing lease ID"
                );
                return match result.get("TTL") {
                    None => Ok(false),
                    Some(ttl) => Ok(ttl.as_str().context("invalid lease TTL")?.parse::<i64>()? > 0),
                };
            }
        }
        anyhow::bail!("missing lease response")
    }

    pub async fn ping(&self) -> Result<()> {
        self.call("/v3/kv/range", json!({"key":self.key(Uuid::nil())}))
            .await?;
        Ok(())
    }

    pub(super) fn has_alternates(&self) -> bool {
        self.endpoints.len() > 1
    }

    fn key(&self, id: Uuid) -> String {
        STANDARD.encode(format!("{}/jobs/{id}", self.prefix))
    }

    pub async fn get(&self, id: Uuid) -> Result<Option<VersionedJob>> {
        let response = self
            .call("/v3/kv/range", json!({"key":self.key(id)}))
            .await?;
        let Some(entry) = response["kvs"].as_array().and_then(|items| items.first()) else {
            return Ok(None);
        };
        let data = STANDARD.decode(entry["value"].as_str().context("missing etcd job value")?)?;
        let revision = entry["mod_revision"]
            .as_str()
            .context("missing etcd revision")?
            .parse()?;
        Ok(Some(VersionedJob {
            job: serde_json::from_slice(&data)?,
            revision,
        }))
    }

    pub async fn create(
        &self,
        id: Uuid,
        job: &Job,
        ttl: u64,
        leader: Option<&super::control::Leadership>,
    ) -> Result<bool> {
        let lease = self
            .call("/v3/lease/grant", json!({"TTL":ttl.to_string()}))
            .await?;
        let lease = lease["ID"].as_str().context("missing etcd lease")?;
        let created = self
            .write(
                id,
                job,
                json!({"target":"CREATE","create_revision":"0"}),
                json!({"lease":lease}),
                leader,
            )
            .await?;
        if !created {
            let _ = self.call("/v3/lease/revoke", json!({"ID":lease})).await;
        }
        Ok(created)
    }

    pub async fn replace(&self, id: Uuid, revision: i64, job: &Job) -> Result<bool> {
        self.write(
            id,
            job,
            json!({"target":"MOD","mod_revision":revision.to_string()}),
            json!({"ignore_lease":true}),
            None,
        )
        .await
    }

    fn graph_key(&self, model: &str, revision: &str) -> String {
        STANDARD.encode(format!("{}/csr/{model}/{revision}", self.prefix))
    }

    pub async fn graph(&self, model: &str, revision: &str) -> Result<Option<(Uuid, i64)>> {
        let response = self
            .call(
                "/v3/kv/range",
                json!({"key": self.graph_key(model, revision)}),
            )
            .await?;
        let Some(entry) = response["kvs"].as_array().and_then(|items| items.first()) else {
            return Ok(None);
        };
        let generation =
            STANDARD.decode(entry["value"].as_str().context("missing CSR generation")?)?;
        Ok(Some((
            Uuid::from_slice(&generation)?,
            entry["mod_revision"]
                .as_str()
                .context("missing CSR revision")?
                .parse()?,
        )))
    }

    pub async fn publish_graph(
        &self,
        model: &str,
        revision: &str,
        previous: i64,
        generation: Uuid,
        leader: Option<&super::control::Leadership>,
    ) -> Result<bool> {
        let key = self.graph_key(model, revision);
        let mut compare = vec![
            json!({"key": key, "target": "MOD", "result": "EQUAL", "mod_revision": previous.to_string()}),
        ];
        compare.extend(leader.map(super::control::Leadership::owner));
        let response = self.call("/v3/kv/txn", json!({
            "compare": compare,
            "success": [{"request_put": {"key": key, "value": STANDARD.encode(generation.as_bytes())}}]
        })).await?;
        Ok(response["succeeded"].as_bool().unwrap_or(false))
    }

    async fn write(
        &self,
        id: Uuid,
        job: &Job,
        mut compare: Value,
        mut put: Value,
        leader: Option<&super::control::Leadership>,
    ) -> Result<bool> {
        let key = self.key(id);
        compare["key"] = json!(key);
        compare["result"] = json!("EQUAL");
        put["key"] = json!(key);
        put["value"] = json!(STANDARD.encode(serde_json::to_vec(job)?));
        let mut compare = vec![compare];
        compare.extend(leader.map(super::control::Leadership::owner));
        let result = self
            .call(
                "/v3/kv/txn",
                json!({
                    "compare":compare, "success":[{"request_put":put}]
                }),
            )
            .await?;
        // Protobuf JSON can omit false.
        Ok(result["succeeded"].as_bool().unwrap_or(false))
    }
}

#[derive(Clone)]
pub(super) struct Objects {
    store: Arc<dyn ObjectStore>,
    remote: bool,
    prefix: Key,
}

impl Objects {
    pub fn open(location: &str, cluster: &str) -> Result<Self> {
        let (store, prefix): (Arc<dyn ObjectStore>, _) =
            if let Some(path) = location.strip_prefix("s3://") {
                let (bucket, prefix) = path.split_once('/').unwrap_or((path, ""));
                ensure!(
                    !bucket.is_empty()
                        && bucket
                            .bytes()
                            .all(|b| b.is_ascii_alphanumeric() || b".-".contains(&b)),
                    "invalid cluster S3 bucket"
                );
                (
                    Arc::new(
                        AmazonS3Builder::from_env()
                            .with_bucket_name(bucket)
                            .build()?,
                    ),
                    Key::parse(prefix)?,
                )
            } else {
                let path = Path::new(location);
                ensure!(
                    path.is_absolute() && path.is_dir(),
                    "cluster storage must be an S3 URI or an existing absolute directory"
                );
                (
                    Arc::new(LocalFileSystem::new_with_prefix(path)?),
                    Key::default(),
                )
            };
        Ok(Self {
            store,
            remote: location.starts_with("s3://"),
            prefix: prefix.join("graphlake-v1").join(cluster),
        })
    }

    pub fn remote(&self) -> bool {
        self.remote
    }

    pub fn control_catalog(&self, generation: &str) -> Key {
        self.prefix
            .clone()
            .join("control")
            .join("catalog")
            .join(generation)
    }

    pub fn control_catalog_blob(&self, digest: &str) -> Key {
        self.prefix
            .clone()
            .join("control")
            .join("catalog-blobs")
            .join(digest)
    }

    pub fn artifact(&self, id: Uuid, attempt: Uuid, name: &str) -> Key {
        self.prefix
            .clone()
            .join("results")
            .join(id.to_string())
            .join(attempt.to_string())
            .join(name)
    }

    pub fn graph(&self, model: &str, revision: &str, generation: Uuid) -> Key {
        self.prefix
            .clone()
            .join("csr")
            .join(model)
            .join(revision)
            .join(generation.to_string())
    }

    pub async fn put_bytes(&self, key: &Key, bytes: Vec<u8>) -> Result<()> {
        self.store.put(key, bytes.into()).await?;
        Ok(())
    }

    pub async fn put_immutable(&self, key: &Key, bytes: Vec<u8>) -> Result<()> {
        match self
            .store
            .put_opts(key, bytes.into(), PutMode::Create.into())
            .await
        {
            Ok(_) | Err(object_store::Error::AlreadyExists { .. }) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    pub async fn model(&self, model: &str, revision: &str, yaml: String) -> Result<()> {
        let key = self
            .prefix
            .clone()
            .join("models")
            .join(model)
            .join(revision);
        match self
            .store
            .put_opts(&key, yaml.into_bytes().into(), PutMode::Create.into())
            .await
        {
            Ok(_) | Err(object_store::Error::AlreadyExists { .. }) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    pub async fn read_model(&self, model: &str, revision: &str) -> Result<String> {
        let key = self
            .prefix
            .clone()
            .join("models")
            .join(model)
            .join(revision);
        let result = self.get(&key).await?;
        ensure!(
            result.meta.size <= super::super::CATALOG_LIMIT as u64,
            "cluster model is too large"
        );
        Ok(String::from_utf8(result.bytes().await?.to_vec())?)
    }

    pub async fn put_json(&self, key: &Key, value: &impl Serialize) -> Result<()> {
        self.store
            .put(key, serde_json::to_vec(value)?.into())
            .await?;
        Ok(())
    }

    pub async fn delete(&self, key: &Key) {
        let _ = self.store.delete(key).await;
    }

    pub async fn get_json<T: DeserializeOwned>(&self, key: &Key, limit: u64) -> Result<T> {
        let result = self.get(key).await?;
        ensure!(result.meta.size <= limit, "cluster object is too large");
        Ok(serde_json::from_slice(&result.bytes().await?)?)
    }

    pub async fn head(&self, key: &Key) -> Result<()> {
        self.store.head(key).await?;
        Ok(())
    }

    pub async fn exists(&self, key: &Key) -> Result<bool> {
        match self.store.head(key).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    pub async fn get(&self, key: &Key) -> Result<object_store::GetResult> {
        Ok(self.store.get(key).await?)
    }

    pub async fn get_ranges(
        &self,
        key: &Key,
        ranges: &[std::ops::Range<u64>],
    ) -> Result<Vec<bytes::Bytes>> {
        Ok(
            object_store::coalesce_ranges(ranges, |range| self.store.get_range(key, range), 0)
                .await?,
        )
    }

    pub async fn put_arrow(&self, key: &Key, arrow: Captured) -> Result<()> {
        self.put_file(key, arrow.file.path(), arrow.length).await
    }

    pub async fn put_file(&self, key: &Key, path: &Path, length: u64) -> Result<()> {
        const PART: usize = 5 * 1024 * 1024;
        let mut file = tokio::fs::File::open(path).await?;
        if length <= PART as u64 {
            let mut bytes = Vec::with_capacity(length as usize);
            file.read_to_end(&mut bytes).await?;
            self.store.put(key, bytes.into()).await?;
            return Ok(());
        }
        let upload = self.store.put_multipart(key).await?;
        let mut guard = UploadGuard(Some(upload));
        let transfer = async {
            let mut remaining = length;
            while remaining > 0 {
                let mut bytes = vec![0; remaining.min(PART as u64) as usize];
                file.read_exact(&mut bytes).await?;
                remaining -= bytes.len() as u64;
                guard
                    .0
                    .as_mut()
                    .expect("active upload")
                    .put_part(bytes.into())
                    .await?;
            }
            guard.0.as_mut().expect("active upload").complete().await?;
            anyhow::Ok(())
        }
        .await;
        if transfer.is_ok() {
            guard.0 = None;
        }
        transfer
    }

    fn shuffle_prefix(&self, id: Uuid, attempt: Uuid) -> Key {
        self.prefix
            .clone()
            .join("results")
            .join(id.to_string())
            .join(attempt.to_string())
            .join("shuffle")
    }

    pub fn stage(&self, id: Uuid, attempt: Uuid, task: Uuid, name: &str) -> Key {
        self.shuffle_prefix(id, attempt)
            .join(task.to_string())
            .join(name)
    }

    pub async fn clean_stage(&self, id: Uuid, attempt: Uuid, task: Uuid) {
        self.clean_prefix(&self.shuffle_prefix(id, attempt).join(task.to_string()))
            .await;
    }

    pub async fn clean_shuffle(&self, id: Uuid, attempt: Uuid) {
        self.clean_prefix(&self.shuffle_prefix(id, attempt)).await;
    }

    pub async fn clean_results(&self, id: Uuid, attempt: Uuid) {
        let prefix = self
            .prefix
            .clone()
            .join("results")
            .join(id.to_string())
            .join(attempt.to_string());
        self.clean_prefix(&prefix).await;
    }

    pub(super) async fn clean_prefix(&self, prefix: &Key) {
        use futures::{StreamExt, TryStreamExt};
        let objects = self.store.list(Some(prefix)).map_ok(|meta| meta.location);
        let _ = self
            .store
            .delete_stream(objects.boxed())
            .try_collect::<Vec<_>>()
            .await;
    }

    pub async fn expire_results(&self) -> Result<()> {
        use futures::{StreamExt, TryStreamExt};
        let prefix = self.prefix.clone().join("results");
        // Leave time for the longest accepted query and readers starting at the TTL boundary.
        let oldest =
            chrono::Utc::now() - chrono::TimeDelta::seconds((super::RESULT_TTL + 120) as i64);
        let objects = self
            .store
            .list(Some(&prefix))
            .try_filter_map(move |meta| async move {
                Ok((meta.last_modified < oldest).then_some(meta.location))
            });
        let mut deleted = self.store.delete_stream(objects.boxed());
        while deleted.try_next().await?.is_some() {}
        Ok(())
    }
}

struct UploadGuard(Option<Box<dyn object_store::MultipartUpload>>);

impl Drop for UploadGuard {
    fn drop(&mut self) {
        if let Some(mut upload) = self.0.take()
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            runtime.spawn(async move {
                let _ = tokio::time::timeout(Duration::from_secs(5), upload.abort()).await;
            });
        }
    }
}

pub(super) fn now_ms() -> u64 {
    chrono::Utc::now().timestamp_millis().max(0) as u64
}
