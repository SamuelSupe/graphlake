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
    ObjectStore, ObjectStoreExt, PutMode, local::LocalFileSystem, path::Path as Key,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_claim: Option<Uuid>,
    pub worker_seen_ms: u64,
    #[serde(default, skip_serializing_if = "is_false")]
    pub retried: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub managed_only: bool,
    pub fingerprint: [u8; 32],
    pub export: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub csr_generation: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub freshness: Option<crate::admin::freshness::Proof>,
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
pub(in crate::admin) struct Metadata {
    client: reqwest::Client,
    endpoints: Arc<[url::Url]>,
    preferred: Arc<AtomicUsize>,
    query_lease: Arc<tokio::sync::Mutex<Option<(tokio::time::Instant, u64, String)>>>,
    pub(in crate::admin) prefix: String,
}

impl Metadata {
    pub fn new(client: reqwest::Client, endpoints: Vec<url::Url>, name: &str) -> Self {
        Self {
            client,
            endpoints: endpoints.into(),
            preferred: Arc::default(),
            query_lease: Arc::default(),
            prefix: format!("/graphlake/v1/{name}"),
        }
    }

    pub(in crate::admin) async fn call(&self, path: &str, body: Value) -> Result<Value> {
        let read_only = matches!(path, "/v3/kv/range" | "/v3/lease/timetolive")
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
        let timeout = if retry && self.has_alternates() {
            210
        } else {
            2000
        };
        let response = self
            .request(path, &body, retry, Duration::from_millis(timeout))
            .await;
        if retry
            && response.as_ref().is_err_and(|error| {
                error.downcast_ref::<reqwest::Error>().is_some_and(|error| {
                    error.is_timeout()
                        || error.is_connect()
                        || error
                            .status()
                            .is_some_and(|status| status.is_server_error())
                })
            })
        {
            // A brief election or an overloaded quorum can outlast the fast
            // failover pass. Retrying reads and idempotent lease grants cannot
            // duplicate a mutation.
            self.request(path, &body, true, Duration::from_secs(2))
                .await
        } else {
            response
        }
    }

    pub(super) async fn retention_range(&self, body: Value) -> Result<Value> {
        self.request("/v3/kv/range", &body, true, Duration::from_secs(2))
            .await
    }

    async fn request(
        &self,
        path: &str,
        body: &Value,
        retry: bool,
        timeout: Duration,
    ) -> Result<Value> {
        self.try_endpoints(retry, |endpoint| {
            let request = self
                .client
                .post(endpoint.join(path).expect("validated etcd endpoint"))
                .timeout(timeout)
                .json(body);
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

    pub(in crate::admin::cluster) async fn get(&self, id: Uuid) -> Result<Option<VersionedJob>> {
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

    pub(in crate::admin::cluster) async fn create(
        &self,
        id: Uuid,
        job: &Job,
        ttl: u64,
        leader: Option<&super::control::Leadership>,
    ) -> Result<bool> {
        const WINDOW: u64 = 5;
        let valid = |slot: &Option<(tokio::time::Instant, u64, String)>| {
            slot.as_ref()
                .filter(|(expires, duration, _)| {
                    *duration == ttl && tokio::time::Instant::now() < *expires
                })
                .map(|(_, _, lease)| lease.clone())
        };
        let cached = valid(&*self.query_lease.lock().await);
        let lease = if let Some(lease) = cached {
            lease
        } else {
            let started = tokio::time::Instant::now();
            let response = self
                .call("/v3/lease/grant", json!({"TTL":(ttl + WINDOW).to_string()}))
                .await?;
            let granted = response["ID"]
                .as_str()
                .context("missing etcd lease")?
                .to_owned();
            let mut slot = self.query_lease.lock().await;
            if let Some(lease) = valid(&slot) {
                // Concurrent grants may leave an unused lease, which will expire.
                lease
            } else {
                *slot = Some((started + Duration::from_secs(WINDOW), ttl, granted.clone()));
                granted
            }
        };
        let result = self
            .write(
                id,
                job,
                json!({"target":"CREATE","create_revision":"0"}),
                json!({"lease":lease.clone()}),
                leader,
            )
            .await;
        if result.is_err() {
            let mut slot = self.query_lease.lock().await;
            if slot
                .as_ref()
                .is_some_and(|(_, _, current)| current == &lease)
            {
                *slot = None;
            }
        }
        result
    }

    pub(in crate::admin::cluster) async fn replace(
        &self,
        id: Uuid,
        revision: i64,
        job: &Job,
    ) -> Result<bool> {
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

const MODEL_CACHE_TTL: Duration = Duration::from_secs(300);

#[derive(Clone)]
pub(in crate::admin) struct Objects {
    pub(super) store: Arc<dyn ObjectStore>,
    arrow_store: Arc<dyn ObjectStore>,
    remote: bool,
    bucket: Option<String>,
    pub(super) prefix: Key,
    pub(super) csr_manifests: Arc<std::sync::Mutex<super::csr::ManifestCache>>,
    uploaded_models:
        Arc<std::sync::Mutex<std::collections::VecDeque<(String, String, std::time::Instant)>>>,
}

impl Objects {
    pub fn open(location: &str, cluster: &str) -> Result<Self> {
        let (store, arrow_store, prefix): (Arc<dyn ObjectStore>, Arc<dyn ObjectStore>, _) =
            if let Some(path) = location.strip_prefix("s3://") {
                let (bucket, prefix) = path.split_once('/').unwrap_or((path, ""));
                ensure!(
                    !bucket.is_empty()
                        && bucket
                            .bytes()
                            .all(|b| b.is_ascii_alphanumeric() || b".-".contains(&b)),
                    "invalid cluster S3 bucket"
                );
                let (store, arrow_store) = super::s3::open(bucket)?;
                (store, arrow_store, Key::parse(prefix)?)
            } else {
                let path = Path::new(location);
                ensure!(
                    path.is_absolute() && path.is_dir(),
                    "cluster storage must be an S3 URI or an existing absolute directory"
                );
                let store = Arc::new(LocalFileSystem::new_with_prefix(path)?);
                (store.clone(), store, Key::default())
            };
        Ok(Self {
            store,
            arrow_store,
            remote: location.starts_with("s3://"),
            bucket: location
                .strip_prefix("s3://")
                .and_then(|uri| uri.split('/').next())
                .map(str::to_owned),
            prefix: prefix.join("graphlake-v1").join(cluster),
            csr_manifests: Default::default(),
            uploaded_models: Default::default(),
        })
    }

    pub fn local_table(&self, table: Uuid) -> Key {
        self.prefix
            .clone()
            .join("local-tables")
            .join(table.to_string())
    }

    pub fn object_url(&self, key: &Key) -> Result<String> {
        Ok(format!(
            "s3://{}/{}",
            self.bucket
                .as_deref()
                .context("managed store requires S3")?,
            key
        ))
    }

    pub fn local_key(&self, table: Uuid, url: &str) -> Result<Key> {
        let relative = url
            .strip_prefix(&format!(
                "s3://{}/",
                self.bucket
                    .as_deref()
                    .context("managed store requires S3")?
            ))
            .context("unexpected managed object bucket")?;
        let key = Key::parse(relative)?;
        ensure!(
            key.prefix_matches(&self.local_table(table)),
            "unexpected managed object prefix"
        );
        Ok(key)
    }

    pub async fn local_objects(&self, table: Uuid) -> Result<Vec<object_store::ObjectMeta>> {
        use futures::TryStreamExt;
        Ok(self
            .store
            .list(Some(&self.local_table(table)))
            .try_collect()
            .await?)
    }

    pub async fn object_meta(&self, key: &Key) -> Result<object_store::ObjectMeta> {
        Ok(self.store.head(key).await?)
    }

    pub async fn delete_local(&self, key: &Key) -> Result<()> {
        match self.store.delete(key).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    pub async fn delete_local_batch(&self, keys: Vec<Key>) -> Result<usize> {
        use futures::StreamExt;
        if keys.is_empty() {
            return Ok(0);
        }
        let stream = futures::stream::iter(keys.into_iter().map(Ok)).boxed();
        let mut deleted = self.store.delete_stream(stream);
        let mut count = 0usize;
        while let Some(result) = deleted.next().await {
            match result {
                Ok(_) => count += 1,
                Err(object_store::Error::NotFound { .. }) => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(count)
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

    fn model_key(&self, model: &str, revision: &str) -> Key {
        self.prefix
            .clone()
            .join("models")
            .join(model)
            .join(revision)
    }

    fn is_model_cached(&self, model: &str, revision: &str) -> bool {
        let now = std::time::Instant::now();
        let mut cache = self.uploaded_models.lock().unwrap();
        if let Some(pos) = cache
            .iter()
            .position(|(m, r, _)| m == model && r == revision)
        {
            let entry = cache.remove(pos).unwrap();
            if now.duration_since(entry.2) < MODEL_CACHE_TTL {
                cache.push_back(entry);
                return true;
            }
        }
        false
    }

    fn remember_model(&self, model: &str, revision: &str) {
        let now = std::time::Instant::now();
        let mut cache = self.uploaded_models.lock().unwrap();
        cache.retain(|(m, r, ts)| {
            !(m == model && r == revision) && now.duration_since(*ts) < MODEL_CACHE_TTL
        });
        if cache.len() >= 256 {
            cache.pop_front();
        }
        cache.push_back((model.to_owned(), revision.to_owned(), now));
    }

    pub async fn ensure_model(&self, model: &str, revision: &str, yaml: String) -> Result<()> {
        if self.is_model_cached(model, revision) {
            return Ok(());
        }
        let key = self.model_key(model, revision);
        self.put_immutable(&key, yaml.into_bytes()).await?;
        self.remember_model(model, revision);
        Ok(())
    }

    pub async fn model(&self, model: &str, revision: &str, yaml: String) -> Result<()> {
        let key = self.model_key(model, revision);
        if self.is_model_cached(model, revision) && self.exists(&key).await? {
            return Ok(());
        }
        self.put_immutable(&key, yaml.into_bytes()).await?;
        self.remember_model(model, revision);
        Ok(())
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
        let (value, _) = self.get_json_with_size(key, limit).await?;
        Ok(value)
    }

    pub async fn get_json_with_size<T: DeserializeOwned>(
        &self,
        key: &Key,
        limit: u64,
    ) -> Result<(T, u64)> {
        let result = self.get(key).await?;
        ensure!(result.meta.size <= limit, "cluster object is too large");
        let bytes = result.bytes().await?;
        let len = bytes.len() as u64;
        Ok((serde_json::from_slice(&bytes)?, len))
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

    pub(super) async fn head_arrow(&self, key: &Key) -> super::ApiResult<object_store::ObjectMeta> {
        tokio::time::timeout(
            crate::admin::query::download::DOWNLOAD_IDLE_TIMEOUT,
            self.arrow_store.head(key),
        )
        .await
        .map_err(|_| super::timeout())?
        .map_err(super::storage)
    }

    pub(super) async fn get_arrow(&self, key: &Key) -> super::ApiResult<object_store::GetResult> {
        tokio::time::timeout(
            crate::admin::query::download::DOWNLOAD_IDLE_TIMEOUT,
            self.arrow_store.get(key),
        )
        .await
        .map_err(|_| super::timeout())?
        .map_err(super::storage)
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
        match arrow {
            Captured::Memory(bytes) => {
                self.store.put(key, bytes.into()).await?;
                Ok(())
            }
            Captured::File { file, length } => self.put_file(key, file.path(), length).await,
        }
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

    pub(in crate::admin) async fn expire_results(
        &self,
        leader: Option<&super::control::Leadership>,
    ) -> Result<()> {
        use futures::TryStreamExt;
        if let Some(leader) = leader {
            leader
                .confirm()
                .await
                .map_err(|error| anyhow::anyhow!(error.message))?;
        }
        let prefix = self.prefix.clone().join("results");
        // Leave time for the longest accepted query and readers starting at the TTL boundary.
        let oldest =
            chrono::Utc::now() - chrono::TimeDelta::seconds((super::RESULT_TTL + 120) as i64);
        let mut listing = self.store.list(Some(&prefix));
        let mut batch = Vec::with_capacity(64);
        while let Some(meta) = listing.try_next().await? {
            if let Some(leader) = leader {
                leader
                    .admission()
                    .map_err(|error| anyhow::anyhow!(error.message))?;
            }
            if meta.last_modified < oldest {
                batch.push(meta.location);
                if batch.len() >= 64 {
                    if let Some(leader) = leader {
                        leader
                            .confirm()
                            .await
                            .map_err(|error| anyhow::anyhow!(error.message))?;
                    }
                    self.delete_local_batch(std::mem::take(&mut batch)).await?;
                }
            }
        }
        if !batch.is_empty() {
            if let Some(leader) = leader {
                leader
                    .confirm()
                    .await
                    .map_err(|error| anyhow::anyhow!(error.message))?;
            }
            self.delete_local_batch(batch).await?;
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Json, Router, routing::post};

    #[tokio::test]
    async fn read_retries_when_the_fast_etcd_pass_times_out() -> Result<()> {
        let requests = Arc::new(AtomicUsize::new(0));
        let seen = requests.clone();
        let router = Router::new().route(
            "/v3/kv/range",
            post(move || {
                let count = seen.fetch_add(1, Ordering::Relaxed);
                async move {
                    if count < 2 {
                        tokio::time::sleep(Duration::from_millis(450)).await;
                    }
                    Json(json!({"header":{"revision":"3"}}))
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?).parse::<url::Url>()?;
        let server = tokio::spawn(async move { axum::serve(listener, router).await });
        let metadata = Metadata::new(
            reqwest::Client::new(),
            vec![endpoint.clone(), endpoint],
            "test",
        );
        let result = metadata.call("/v3/kv/range", json!({"key":"YQ=="})).await;
        server.abort();
        assert_eq!(result?["header"]["revision"], "3");
        assert_eq!(requests.load(Ordering::Relaxed), 3);
        Ok(())
    }

    #[tokio::test]
    async fn lease_grant_retries_when_the_fast_etcd_pass_times_out() -> Result<()> {
        let requests = Arc::new(AtomicUsize::new(0));
        let seen = requests.clone();
        let router = Router::new().route(
            "/v3/lease/grant",
            post(move || {
                let count = seen.fetch_add(1, Ordering::Relaxed);
                async move {
                    if count < 2 {
                        tokio::time::sleep(Duration::from_millis(450)).await;
                    }
                    Json(json!({"ID":"42","TTL":"60"}))
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?).parse::<url::Url>()?;
        let server = tokio::spawn(async move { axum::serve(listener, router).await });
        let metadata = Metadata::new(
            reqwest::Client::new(),
            vec![endpoint.clone(), endpoint],
            "test",
        );
        let result = metadata.call("/v3/lease/grant", json!({"TTL":"60"})).await;
        server.abort();
        assert_eq!(result?["ID"], "42");
        assert_eq!(requests.load(Ordering::Relaxed), 3);
        Ok(())
    }
}
