//! Authoritative HA state. Only immutable candidates are uploaded before the
//! etcd transaction. A former owner cannot change the live catalog pointer.
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use axum::http::StatusCode;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use futures::{StreamExt, TryStreamExt, stream};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use tokio::time::Instant;
use uuid::Uuid;

use super::{Metadata, Objects, now_ms};
use crate::{
    CancellationToken,
    admin::{ApiResult, Error, models},
};

const LEASE_SECONDS: u64 = 6;
const LOCAL_VALIDITY: Duration = Duration::from_secs(3);
const CONFIRM_TIMEOUT: Duration = Duration::from_millis(650);
const CONFIRM_RETRY: Duration = Duration::from_millis(100);
const HEARTBEAT: Duration = Duration::from_millis(450);
const LEGACY_CATALOG_LIMIT: u64 = 128 * 1024 * 1024;
const CATALOG_MANIFEST_LIMIT: u64 = 32 * 1024 * 1024;
const CATALOG_BLOB_LIMIT: u64 = (8 * crate::admin::CATALOG_LIMIT) as u64;
const CATALOG_UPLOAD_IDLE: Duration = Duration::from_secs(60);
pub(in crate::admin) const CATALOG_BYTES_LIMIT: u64 = 512 * 1024 * 1024;

#[derive(Clone)]
pub(in crate::admin) struct Control {
    meta: Metadata,
    objects: Objects,
}

pub(in crate::admin) struct Leadership {
    control: Control,
    lease: String,
    value: String,
    window: Mutex<LeaseWindow>,
    metrics: Arc<ElectionMetrics>,
    elected: bool,
    stop: CancellationToken,
}

struct LeaseWindow {
    until: Instant,
    uncertain: CancellationToken,
    generation: u64,
    loss: Option<ConfirmationFailure>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ConfirmationFailure {
    Timeout,
    Unavailable,
    OwnerChanged,
    LeaseExpired,
    SafetyWindowExpired,
}

impl ConfirmationFailure {
    const ALL: [Self; 5] = [
        Self::Timeout,
        Self::Unavailable,
        Self::OwnerChanged,
        Self::LeaseExpired,
        Self::SafetyWindowExpired,
    ];

    fn reason(self) -> &'static str {
        match self {
            Self::Timeout => "timeout",
            Self::Unavailable => "unavailable",
            Self::OwnerChanged => "owner_changed",
            Self::LeaseExpired => "lease_expired",
            Self::SafetyWindowExpired => "safety_window_expired",
        }
    }

    fn definitive(self) -> bool {
        !matches!(self, Self::Timeout | Self::Unavailable)
    }

    fn transport(error: anyhow::Error) -> Self {
        if error
            .downcast_ref::<reqwest::Error>()
            .is_some_and(reqwest::Error::is_timeout)
        {
            Self::Timeout
        } else {
            Self::Unavailable
        }
    }
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum PromotionReason {
    Initial,
    LeaderReleased,
    SafetyWindowExpired,
    LeaseDisappeared,
}

#[derive(Deserialize, Serialize)]
struct ElectionRecord {
    release: Option<PromotionReason>,
}

#[derive(Default, Deserialize, Serialize)]
#[serde(default)]
struct ElectionCounts {
    failures: [u64; 5],
    losses: [u64; 5],
    promotions: [u64; 4],
}

#[derive(Default)]
pub(in crate::admin) struct ElectionMetrics {
    counts: Mutex<ElectionCounts>,
    file: Option<std::path::PathBuf>,
}

impl ElectionMetrics {
    pub(in crate::admin) fn load(file: Option<std::path::PathBuf>) -> Result<Self> {
        let counts = file
            .as_deref()
            .map(|path| crate::admin::models::read_file(path, 4096))
            .transpose()
            .map_err(|error| anyhow::anyhow!(error.message))?
            .flatten()
            .map(|value| serde_json::from_str(&value))
            .transpose()?
            .unwrap_or_default();
        Ok(Self {
            counts: Mutex::new(counts),
            file,
        })
    }

    fn record(&self, update: impl FnOnce(&mut ElectionCounts)) {
        let mut counts = self.counts.lock().unwrap();
        update(&mut counts);
        if let Some(file) = &self.file {
            let bytes = serde_json::to_vec(&*counts).expect("serializing election counters");
            if let Err(error) = crate::admin::models::atomic_write(file, &bytes, false) {
                eprintln!(
                    "GraphLake could not persist election counters: {}",
                    error.message
                );
            }
        }
    }

    pub(in crate::admin) fn render(&self) -> String {
        use std::fmt::Write as _;
        let counts = self.counts.lock().unwrap();
        let mut text = String::new();
        for (name, help, values, reasons) in [
            (
                "graphlake_etcd_confirmation_failures_total",
                "Failed attempts to confirm etcd leadership.",
                counts.failures.as_slice(),
                ConfirmationFailure::ALL
                    .map(ConfirmationFailure::reason)
                    .to_vec(),
            ),
            (
                "graphlake_coordinator_leadership_losses_total",
                "Definitive leadership losses by reason.",
                &counts.losses[2..],
                ConfirmationFailure::ALL[2..]
                    .iter()
                    .map(|r| r.reason())
                    .collect(),
            ),
            (
                "graphlake_coordinator_promotions_total",
                "Coordinator promotions by observed predecessor outcome.",
                counts.promotions.as_slice(),
                vec![
                    "initial",
                    "leader_released",
                    "safety_window_expired",
                    "lease_disappeared",
                ],
            ),
        ] {
            let _ = writeln!(text, "# HELP {name} {help}\n# TYPE {name} counter");
            for (reason, count) in reasons.into_iter().zip(values) {
                let _ = writeln!(text, "{name}{{reason=\"{reason}\"}} {count}");
            }
        }
        text
    }
}

#[derive(Clone, Default, Deserialize, Serialize)]
pub(in crate::admin) struct CatalogState {
    pub files: BTreeMap<String, String>,
}

#[derive(Deserialize, Serialize)]
#[serde(untagged)]
enum CatalogSnapshot {
    Manifest {
        entries: BTreeMap<String, CatalogObject>,
    },
    Inline(CatalogState),
}

#[derive(Deserialize, Serialize)]
struct CatalogObject {
    digest: String,
    bytes: u64,
}

pub(in crate::admin) fn lost() -> Error {
    Error::new(
        StatusCode::SERVICE_UNAVAILABLE,
        "coordinator_not_leader",
        "当前 Coordinator 已失去主权，请通过集群入口重试",
    )
}

pub(in crate::admin) const UNCERTAIN: &str = "coordinator_leadership_uncertain";

pub(in crate::admin) fn interrupted() -> Error {
    Error::new(
        StatusCode::SERVICE_UNAVAILABLE,
        UNCERTAIN,
        "Coordinator 正在确认主权，请通过集群入口重试",
    )
}

pub(in crate::admin) const MUTATION_UNKNOWN: &str = "control_mutation_unknown";

pub(in crate::admin) fn unavailable() -> Error {
    Error::new(
        StatusCode::SERVICE_UNAVAILABLE,
        "control_storage_unavailable",
        "集群状态存储暂不可用，请使用同一请求标识重试",
    )
}

pub(in crate::admin) fn timeout() -> Error {
    Error::new(
        StatusCode::GATEWAY_TIMEOUT,
        "mutation_timeout",
        "操作已超时，请用原请求标识查询结果或重试",
    )
}

impl Control {
    pub(in crate::admin) fn new(meta: Metadata, objects: Objects) -> Self {
        Self { meta, objects }
    }

    fn key(&self, suffix: &str) -> String {
        STANDARD.encode(format!("{}/coordinator/{suffix}", self.meta.prefix))
    }

    async fn txn(&self, compare: Vec<Value>, success: Vec<Value>) -> Result<Option<i64>> {
        let r = self
            .meta
            .call("/v3/kv/txn", json!({"compare":compare,"success":success}))
            .await?;
        let revision = r["header"]["revision"]
            .as_str()
            .context("missing txn revision")?
            .parse()?;
        // The gRPC JSON gateway omits the default false value on failed compares.
        match r.get("succeeded") {
            Some(Value::Bool(true)) => Ok(Some(revision)),
            None | Some(Value::Bool(false)) => Ok(None),
            _ => anyhow::bail!("invalid txn outcome"),
        }
    }

    pub async fn checkpoint(&self) -> ApiResult<(CatalogState, String, Value)> {
        let keys = ["catalog", "policy", "users"];
        let reads: Vec<_> = keys
            .into_iter()
            .map(|name| json!({"request_range":{"key":self.key(name)}}))
            .collect();
        let result = self
            .meta
            .call("/v3/kv/txn", json!({"success":reads}))
            .await
            .map_err(|_| unavailable())?;
        let responses = result["responses"]
            .as_array()
            .filter(|responses| responses.len() == keys.len())
            .ok_or_else(unavailable)?;
        let mut values = responses.iter().map(|response| {
            let Some(entry) = response["response_range"]["kvs"]
                .as_array()
                .and_then(|kvs| kvs.first())
            else {
                return Ok(None);
            };
            let encoded = entry["value"].as_str().ok_or_else(unavailable)?;
            let value = STANDARD.decode(encoded).map_err(|_| unavailable())?;
            serde_json::from_slice::<Value>(&value)
                .map(Some)
                .map_err(|_| unavailable())
        });
        let generation = values
            .next()
            .unwrap()?
            .and_then(|value| value.as_str().map(str::to_owned))
            .ok_or_else(unavailable)?;
        let policy = values
            .next()
            .unwrap()?
            .and_then(|value| value.as_str().map(str::to_owned))
            .ok_or_else(unavailable)?;
        let users = values
            .next()
            .unwrap()?
            .unwrap_or_else(|| json!({"accounts":{}, "groups":{}}));
        let catalog = self.load_catalog(&generation).await?;
        Ok((catalog, policy, users))
    }

    pub(super) async fn check_ingress(meta: Metadata, node: &str) -> Result<bool> {
        if meta.has_alternates() {
            // The ingress check runs in a fresh process. Select a reachable member
            // before changing the lease, so a dead first address does not pin the VIP.
            meta.ping().await?;
        }
        let key = STANDARD.encode(format!("{}/coordinator/ingress", meta.prefix));
        let value = encoded(&node).map_err(|err| anyhow::anyhow!(err.message))?;
        let granted = meta
            .call("/v3/lease/grant", json!({"TTL":LEASE_SECONDS.to_string()}))
            .await?;
        let lease = granted["ID"].as_str().context("etcd ingress lease ID")?;
        let put = json!({"request_put":{"key":key,"value":value,"lease":lease}});
        let renew = json!({
            "compare":[{"key":key,"target":"VALUE","result":"EQUAL","value":value}],
            "success":[put.clone()],
        });
        let request = json!({
            "compare":[{"key":key,"target":"CREATE","result":"EQUAL","create_revision":"0"}],
            "success":[put],
            "failure":[{"request_txn":renew}],
        });
        let result = meta.call("/v3/kv/txn", request).await?;
        let owned = result["succeeded"].as_bool() == Some(true)
            || result["responses"][0]["response_txn"]["succeeded"].as_bool() == Some(true);
        if !owned {
            let _ = meta.call("/v3/lease/revoke", json!({"ID":lease})).await;
        }
        Ok(owned)
    }

    async fn read_snapshot(&self, generation: &str) -> Result<CatalogSnapshot> {
        ensure!(
            Uuid::parse_str(generation).is_ok(),
            "invalid catalog generation"
        );
        let key = self.objects.control_catalog(generation);
        self.objects.get_json(&key, LEGACY_CATALOG_LIMIT).await
    }

    async fn load_catalog(&self, generation: &str) -> ApiResult<CatalogState> {
        let snapshot = tokio::time::timeout(CATALOG_UPLOAD_IDLE, self.read_snapshot(generation))
            .await
            .map_err(|_| timeout())?
            .map_err(|_| unavailable())?;
        match snapshot {
            CatalogSnapshot::Inline(state) => Ok(state),
            CatalogSnapshot::Manifest { entries } => {
                if entries
                    .values()
                    .fold(0u64, |sum, object| sum.saturating_add(object.bytes))
                    > CATALOG_BYTES_LIMIT
                {
                    return Err(unavailable());
                }
                let mut unique = BTreeMap::<String, u64>::new();
                for object in entries.values() {
                    if !models::versions::valid_revision(&object.digest)
                        || object.bytes > CATALOG_BLOB_LIMIT
                    {
                        return Err(unavailable());
                    }
                    if let Some(&prev) = unique.get(&object.digest) {
                        if prev != object.bytes {
                            return Err(unavailable());
                        }
                    } else {
                        unique.insert(object.digest.clone(), object.bytes);
                    }
                }
                let blobs: BTreeMap<String, String> = stream::iter(unique)
                    .map(|(digest, expected_bytes)| async move {
                        let key = self.objects.control_catalog_blob(&digest);
                        let contents = tokio::time::timeout(CATALOG_UPLOAD_IDLE, async {
                            let result = self.objects.get(&key).await?;
                            ensure!(
                                result.meta.size == expected_bytes,
                                "catalog object length mismatch"
                            );
                            let data = result.bytes().await?;
                            let contents = std::str::from_utf8(&data)?;
                            ensure!(
                                models::revision(contents) == digest,
                                "catalog object checksum mismatch"
                            );
                            Ok::<_, anyhow::Error>(contents.to_owned())
                        })
                        .await
                        .map_err(|_| timeout())?
                        .map_err(|_| unavailable())?;
                        Ok::<_, Error>((digest, contents))
                    })
                    .buffer_unordered(8)
                    .try_collect()
                    .await?;
                let files = entries
                    .into_iter()
                    .map(|(name, object)| {
                        let contents = blobs
                            .get(&object.digest)
                            .expect("fetched deduplicated catalog blob")
                            .clone();
                        (name, contents)
                    })
                    .collect();
                Ok(CatalogState { files })
            }
        }
    }

    pub async fn acquire(&self, stop: &CancellationToken) -> Result<Arc<Leadership>> {
        self.acquire_with_metrics(stop, None).await
    }

    pub async fn acquire_with_metrics(
        &self,
        stop: &CancellationToken,
        metrics: Option<Arc<ElectionMetrics>>,
    ) -> Result<Arc<Leadership>> {
        eprintln!("GraphLake Coordinator waiting for etcd leadership");
        loop {
            let attempt = async {
                let started = Instant::now();
                let granted = self
                    .meta
                    .call("/v3/lease/grant", json!({"TTL":LEASE_SECONDS.to_string()}))
                    .await?;
                let lease = granted["ID"].as_str().context("etcd lease ID")?.to_owned();
                let value = STANDARD.encode(Uuid::new_v4().as_bytes());
                let key = self.key("leader");
                let claimed = self.txn(
                    vec![json!({"key":key,"target":"CREATE","result":"EQUAL","create_revision":"0"})],
                    vec![json!({"request_put":{"key":key,"value":value,"lease":lease}})],
                ).await?;
                if claimed.is_none() {
                    self.revoke(&lease).await;
                    return Ok(None);
                }
                let leader = Arc::new(Leadership {
                    control: self.clone(),
                    lease,
                    value,
                    window: Mutex::new(LeaseWindow {
                        until: started + LOCAL_VALIDITY,
                        uncertain: CancellationToken::new(),
                        generation: 0,
                        loss: None,
                    }),
                    metrics: metrics.clone().unwrap_or_default(),
                    elected: metrics.is_some(),
                    stop: stop.child_token(),
                });
                // The candidate is still private. A failed first check must not
                // cancel the whole process or leave its lease blocking another candidate.
                if leader
                    .validate(true, started + LOCAL_VALIDITY)
                    .await
                    .is_err()
                {
                    leader.control.revoke(&leader.lease).await;
                    return Ok(None);
                }
                if let Some(metrics) = &metrics {
                    let promote = async {
                        let previous = self.current::<ElectionRecord>("elected").await?;
                        let reason =
                            previous
                                .as_ref()
                                .map_or(PromotionReason::Initial, |(record, _)| {
                                    record.release.unwrap_or(PromotionReason::LeaseDisappeared)
                                });
                        let committed = leader
                            .commit(
                                "elected",
                                previous.map_or(0, |(_, revision)| revision),
                                &ElectionRecord { release: None },
                                None::<(Uuid, &())>,
                            )
                            .await
                            .map_err(|err| anyhow::anyhow!(err.message))?;
                        anyhow::ensure!(committed.is_some(), "leadership changed during promotion");
                        Ok::<_, anyhow::Error>(reason)
                    };
                    match tokio::time::timeout_at(started + LOCAL_VALIDITY, promote).await {
                        Ok(Ok(reason)) => {
                            metrics.record(|counts| counts.promotions[reason as usize] += 1)
                        }
                        _ => {
                            leader.control.revoke(&leader.lease).await;
                            return Ok(None);
                        }
                    }
                }
                // Link the serving process only after the candidate has won.
                let process = stop.clone();
                let held = leader.stop.clone();
                tokio::spawn(async move {
                    held.cancelled().await;
                    process.cancel();
                });
                tokio::spawn(Leadership::heartbeat(leader.clone()));
                Ok::<_, anyhow::Error>(Some(leader))
            };
            tokio::select! {
                _ = stop.cancelled() => anyhow::bail!("Coordinator stopped while awaiting leadership"),
                result = attempt => match result {
                    Ok(Some(leader)) => return Ok(leader),
                    Ok(None) => {}
                    Err(error) => eprintln!("GraphLake Coordinator election unavailable: {error}"),
                }
            }
            tokio::select! {
                _ = stop.cancelled() => anyhow::bail!("Coordinator stopped while awaiting leadership"),
                _ = tokio::time::sleep(HEARTBEAT) => {}
            }
        }
    }

    async fn revoke(&self, lease: &str) {
        let _ = self
            .meta
            .call("/v3/lease/revoke", json!({"ID":lease}))
            .await;
    }

    async fn current<T: DeserializeOwned>(&self, suffix: &str) -> Result<Option<(T, i64)>> {
        let result = self
            .meta
            .call("/v3/kv/range", json!({"key":self.key(suffix)}))
            .await?;
        ensure!(
            result["header"]["revision"].as_str().is_some(),
            "missing range revision"
        );
        let Some(entry) = result["kvs"].as_array().and_then(|values| values.first()) else {
            return Ok(None);
        };
        let data = STANDARD.decode(entry["value"].as_str().context("missing etcd value")?)?;
        let rev = entry["mod_revision"]
            .as_str()
            .context("missing etcd revision")?
            .parse()?;
        Ok(Some((serde_json::from_slice(&data)?, rev)))
    }
}

impl Leadership {
    async fn validate(&self, renew: bool, until: Instant) -> Result<Instant, ConfirmationFailure> {
        let started = Instant::now();
        tokio::time::timeout_at(until.min(started + CONFIRM_TIMEOUT), async {
            if renew
                && !self
                    .control
                    .meta
                    .keep_alive(&self.lease)
                    .await
                    .map_err(ConfirmationFailure::transport)?
            {
                return Err(ConfirmationFailure::LeaseExpired);
            }
            if self
                .control
                .txn(vec![self.owner()], vec![])
                .await
                .map_err(ConfirmationFailure::transport)?
                .is_none()
            {
                return Err(ConfirmationFailure::OwnerChanged);
            }
            Ok(started)
        })
        .await
        .unwrap_or(Err(ConfirmationFailure::Timeout))
    }

    async fn heartbeat(self: Arc<Self>) {
        let mut delay = HEARTBEAT;
        loop {
            tokio::select! {
                _ = self.stop.cancelled() => break,
                _ = tokio::time::sleep(delay) => {}
            }
            delay = if self.check(true).await.is_ok() {
                HEARTBEAT
            } else {
                CONFIRM_RETRY
            };
        }
    }

    pub fn metrics(&self) -> &ElectionMetrics {
        &self.metrics
    }

    pub fn eligible(&self) -> bool {
        self.admission().is_ok()
    }

    pub fn admission(&self) -> ApiResult<CancellationToken> {
        let window = self.window.lock().unwrap();
        if self.stop.is_cancelled() || Instant::now() >= window.until {
            return Err(lost());
        }
        if window.uncertain.is_cancelled() {
            return Err(interrupted());
        }
        Ok(window.uncertain.clone())
    }

    pub async fn confirm(&self) -> ApiResult<CancellationToken> {
        self.check(false).await
    }

    async fn check(&self, renew: bool) -> ApiResult<CancellationToken> {
        if self.stop.is_cancelled() {
            return Err(lost());
        }
        let (until, generation) = {
            let window = self.window.lock().unwrap();
            if !renew && window.uncertain.is_cancelled() && Instant::now() < window.until {
                return Err(interrupted());
            }
            (window.until, window.generation)
        };
        let result = if Instant::now() >= until {
            Err(ConfirmationFailure::SafetyWindowExpired)
        } else {
            self.validate(renew, until).await
        };
        let (reason, loss) = {
            let mut window = self.window.lock().unwrap();
            if self.stop.is_cancelled() {
                return Err(lost());
            }
            let result = match result {
                Err(reason) if reason.definitive() => Err(reason),
                Ok(_) if Instant::now() >= window.until => {
                    Err(ConfirmationFailure::SafetyWindowExpired)
                }
                result => result,
            };
            match result {
                Ok(started) => {
                    // A check started before a pause cannot clear that pause.
                    if window.generation == generation {
                        if renew {
                            window.until = window.until.max(started + LOCAL_VALIDITY);
                        }
                        if window.uncertain.is_cancelled() {
                            window.uncertain = CancellationToken::new();
                            window.generation += 1;
                        }
                    }
                    return if window.uncertain.is_cancelled() {
                        Err(interrupted())
                    } else {
                        Ok(window.uncertain.clone())
                    };
                }
                Err(reason) => {
                    let loss = if reason.definitive() {
                        Some(reason)
                    } else if Instant::now() >= window.until {
                        Some(ConfirmationFailure::SafetyWindowExpired)
                    } else {
                        None
                    };
                    if !window.uncertain.is_cancelled() {
                        window.uncertain.cancel();
                        window.generation += 1;
                    }
                    if let Some(reason) = loss {
                        window.loss = Some(reason);
                        self.stop.cancel();
                    }
                    (reason, loss)
                }
            }
        };
        // Publish the fence before touching metrics or the filesystem.
        let metrics = self.metrics.clone();
        tokio::task::spawn_blocking(move || {
            metrics.record(|counts| {
                counts.failures[reason as usize] += 1;
                if let Some(loss) = loss {
                    counts.losses[loss as usize] += 1;
                }
            });
        })
        .await
        .ok();
        if let Some(reason) = loss {
            crate::admin::telemetry::event(
                opentelemetry::logs::Severity::Error,
                "coordinator_leadership_lost",
                json!({"reason":reason.reason()}),
            );
            Err(lost())
        } else {
            Err(interrupted())
        }
    }

    pub(in crate::admin) fn loss_token(&self) -> CancellationToken {
        self.stop.clone()
    }

    pub fn retire(&self) {
        self.stop.cancel();
        self.window.lock().unwrap().uncertain.cancel();
    }

    pub async fn release(&self) {
        self.stop.cancel();
        self.window.lock().unwrap().uncertain.cancel();
        if self.elected {
            let reason = match self.window.lock().unwrap().loss {
                Some(ConfirmationFailure::SafetyWindowExpired) => {
                    PromotionReason::SafetyWindowExpired
                }
                Some(_) => PromotionReason::LeaseDisappeared,
                None => PromotionReason::LeaderReleased,
            };
            let record = ElectionRecord {
                release: Some(reason),
            };
            if let Ok(value) = encoded(&record) {
                // The reason and removal of the owner key must become visible together.
                let _ = tokio::time::timeout(CONFIRM_TIMEOUT, self.control.txn(
                    vec![self.owner()],
                    vec![
                        json!({"request_put":{"key":self.control.key("elected"),"value":value}}),
                        json!({"request_delete_range":{"key":self.control.key("leader")}}),
                    ]
                )).await;
            }
        }
        self.control.revoke(&self.lease).await;
    }

    pub(in crate::admin) fn owner(&self) -> Value {
        json!({"key":self.control.key("leader"),"target":"VALUE","result":"EQUAL","value":self.value})
    }

    pub async fn get<T: DeserializeOwned>(&self, key: &str) -> ApiResult<Option<(T, i64)>> {
        self.control.current(key).await.map_err(|_| unavailable())
    }

    pub async fn commit<T: Serialize>(
        &self,
        key: &str,
        previous: i64,
        value: &T,
        receipt: Option<(Uuid, &impl Serialize)>,
    ) -> ApiResult<Option<i64>> {
        self.admission()?;
        let key = self.control.key(key);
        let mut compares = vec![
            self.owner(),
            json!({"key":key,"target":"MOD","result":"EQUAL","mod_revision":previous.to_string()}),
        ];
        let mut puts = vec![json!({"request_put":{"key":key,"value":encoded(value)?}})];
        let receipt_lease = if let Some((id, value)) = receipt {
            let key = self.control.key(&format!("receipts/{id}"));
            let value = encoded(value)?;
            let lease = self
                .control
                .meta
                .call("/v3/lease/grant", json!({"TTL":"86400"}))
                .await
                .map_err(|_| unavailable())?;
            let lease = lease["ID"].as_str().ok_or_else(unavailable)?;
            compares
                .push(json!({"key":key,"target":"CREATE","result":"EQUAL","create_revision":"0"}));
            puts.push(json!({"request_put":{"key":key,"value":value,"lease":lease}}));
            Some(lease.to_owned())
        } else {
            None
        };
        if let Err(error) = self.admission() {
            if let Some(lease) = &receipt_lease {
                self.control.revoke(lease).await;
            }
            return Err(error);
        }
        let written = match self.control.txn(compares, puts).await {
            Ok(written) => written,
            Err(error)
                if error
                    .downcast_ref::<reqwest::Error>()
                    .is_some_and(|e| e.is_connect()) =>
            {
                if let Some(lease) = &receipt_lease {
                    self.control.revoke(lease).await;
                }
                return Err(unavailable());
            }
            // An ambiguous write may have attached the receipt to the lease.
            Err(_) => {
                return Err(Error::new(
                    StatusCode::SERVICE_UNAVAILABLE,
                    MUTATION_UNKNOWN,
                    "集群写入结果未知，请使用同一请求标识查询或重试",
                ));
            }
        };
        if written.is_none() {
            if let Some(lease) = &receipt_lease {
                self.control.revoke(lease).await;
            }
            self.confirm().await?;
        }
        Ok(written)
    }

    pub async fn ensure_empty_checkpoint(&self) -> ApiResult<()> {
        for name in ["catalog", "policy", "users", "sessions", "recovery"] {
            if self.get::<Value>(name).await?.is_some() {
                return Err(Error::new(
                    StatusCode::CONFLICT,
                    "cluster_not_empty",
                    "目标集群已包含控制状态",
                ));
            }
        }
        Ok(())
    }

    pub async fn restore_checkpoint(
        &self,
        state: &CatalogState,
        policy: &str,
        users: &Value,
        recovery: &impl Serialize,
    ) -> ApiResult<()> {
        self.ensure_empty_checkpoint().await?;
        let mut comparisons = vec![self.owner()];
        for name in ["catalog", "policy", "users", "sessions", "recovery"] {
            let key = self.control.key(name);
            comparisons
                .push(json!({"key":key,"target":"CREATE","result":"EQUAL","create_revision":"0"}));
        }
        let generation = self.store_catalog(state, None).await?;
        let puts = [
            ("catalog", encoded(&generation)?),
            ("policy", encoded(&policy)?),
            ("users", encoded(users)?),
            ("recovery", encoded(recovery)?),
        ]
        .into_iter()
        .map(|(name, value)| json!({"request_put":{"key": self.control.key(name), "value":value}}))
        .collect();
        self.admission()?;
        if self
            .control
            .txn(comparisons, puts)
            .await
            .map_err(|_| unavailable())?
            .is_none()
        {
            return Err(lost());
        }
        Ok(())
    }

    pub async fn store_catalog(
        &self,
        state: &CatalogState,
        previous: Option<&str>,
    ) -> ApiResult<String> {
        self.admission()?;
        let mut bytes = 0u64;
        let entries: BTreeMap<_, _> = state
            .files
            .iter()
            .map(|(name, contents)| {
                let size = contents.len() as u64;
                bytes = bytes.saturating_add(size);
                (
                    name.clone(),
                    CatalogObject {
                        digest: models::revision(contents),
                        bytes: size,
                    },
                )
            })
            .collect();
        if bytes > CATALOG_BYTES_LIMIT
            || entries.values().any(|file| file.bytes > CATALOG_BLOB_LIMIT)
        {
            return Err(Error::invalid(
                "cluster_catalog_limit",
                "集群模型目录超过 512 MiB 或单个文件超过 8 MiB",
            ));
        }
        let snapshot = CatalogSnapshot::Manifest { entries };
        let manifest = serde_json::to_vec(&snapshot).map_err(|_| unavailable())?;
        if manifest.len() as u64 > CATALOG_MANIFEST_LIMIT {
            return Err(Error::invalid(
                "cluster_catalog_limit",
                "集群模型目录索引超过 32 MiB",
            ));
        }
        let CatalogSnapshot::Manifest { entries } = snapshot else {
            unreachable!()
        };
        let generation = Uuid::new_v4().to_string();
        let upload = async {
            let mut saved: BTreeSet<String> = match previous {
                Some(previous) => match tokio::time::timeout(
                    Duration::from_secs(10),
                    self.control.read_snapshot(previous),
                )
                .await
                .map_err(|_| timeout())?
                .map_err(|_| unavailable())?
                {
                    CatalogSnapshot::Manifest { entries } => {
                        entries.into_values().map(|entry| entry.digest).collect()
                    }
                    CatalogSnapshot::Inline(_) => BTreeSet::new(),
                },
                None => BTreeSet::new(),
            };
            let objects = &self.control.objects;
            let blobs = entries
                .into_iter()
                .filter(|(_, entry)| saved.insert(entry.digest.clone()));
            let mut uploads = stream::iter(blobs)
                .map(|(name, entry)| async move {
                    let key = objects.control_catalog_blob(&entry.digest);
                    // Full uploads and timed-out attempts share these
                    // immutable objects even without a prior manifest.
                    if !objects.exists(&key).await? {
                        objects
                            .put_immutable(&key, state.files[&name].as_bytes().to_vec())
                            .await?;
                    }
                    Ok::<_, anyhow::Error>(())
                })
                .buffer_unordered(8);
            // Full migrations can take minutes. Bound the interval between
            // completed objects so a slow but progressing upload can finish.
            while tokio::time::timeout(CATALOG_UPLOAD_IDLE, uploads.try_next())
                .await
                .map_err(|_| timeout())?
                .map_err(|_| unavailable())?
                .is_some()
            {}
            let key = objects.control_catalog(&generation);
            tokio::time::timeout(CATALOG_UPLOAD_IDLE, objects.put_immutable(&key, manifest))
                .await
                .map_err(|_| timeout())?
                .map_err(|_| unavailable())
        };
        upload.await?;
        Ok(generation)
    }

    pub async fn check_catalog(&self, generation: &str) -> ApiResult<()> {
        let key = self.control.objects.control_catalog(generation);
        self.control
            .objects
            .head(&key)
            .await
            .map_err(|_| unavailable())?;
        Ok(())
    }

    pub async fn load_catalog(&self, generation: &str) -> ApiResult<CatalogState> {
        self.control.load_catalog(generation).await
    }
}

fn encoded(value: &impl Serialize) -> ApiResult<String> {
    serde_json::to_vec(value)
        .map(|bytes| STANDARD.encode(bytes))
        .map_err(|_| unavailable())
}

#[derive(Clone, Deserialize, Serialize)]
pub(in crate::admin) struct Receipt {
    pub fingerprint: String,
    #[serde(default)]
    pub owner: crate::admin::access::Principal,
    pub model: String,
    pub revision: String,
    pub active_revision: Option<String>,
    pub status: u16,
    pub generation: String,
    #[serde(default)]
    pub validated: bool,
    pub committed_at_ms: u64,
}

impl Receipt {
    pub fn stamp(&mut self) {
        self.committed_at_ms = now_ms();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn catalog_manifests_keep_legacy_and_previous_receipts_readable() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let objects = Objects::open(dir.path().to_str().unwrap(), "catalog-test")?;
        let leader = Leadership {
            control: Control::new(
                Metadata::new(reqwest::Client::new(), Vec::new(), "catalog-test"),
                objects.clone(),
            ),
            lease: "1".into(),
            value: "test".into(),
            window: Mutex::new(LeaseWindow {
                until: Instant::now() + Duration::from_secs(30),
                uncertain: CancellationToken::new(),
                generation: 0,
                loss: None,
            }),
            metrics: Arc::default(),
            elected: false,
            stop: CancellationToken::new(),
        };
        let api = |error: Error| anyhow::anyhow!(error.message);
        let state = CatalogState {
            files: BTreeMap::from([
                ("one.yaml".into(), "first".into()),
                ("two.yaml".into(), "unchanged".into()),
            ]),
        };
        let legacy = Uuid::new_v4().to_string();
        objects
            .put_json(&objects.control_catalog(&legacy), &state)
            .await?;
        assert_eq!(
            leader.load_catalog(&legacy).await.map_err(api)?.files,
            state.files
        );

        let first = leader
            .store_catalog(&state, Some(&legacy))
            .await
            .map_err(api)?;
        let mut next = state.clone();
        next.files.insert("one.yaml".into(), "second".into());
        let second = leader
            .store_catalog(&next, Some(&first))
            .await
            .map_err(api)?;
        assert_eq!(
            leader.load_catalog(&first).await.map_err(api)?.files,
            state.files
        );
        assert_eq!(
            leader.load_catalog(&second).await.map_err(api)?.files,
            next.files
        );
        let blobs = dir
            .path()
            .join("graphlake-v1/catalog-test/control/catalog-blobs");
        assert_eq!(std::fs::read_dir(blobs)?.count(), 3);

        let key = objects.control_catalog_blob(&models::revision("second"));
        objects.put_bytes(&key, b"forged".to_vec()).await?;
        assert!(leader.load_catalog(&second).await.is_err());
        assert_eq!(
            leader.load_catalog(&first).await.map_err(api)?.files,
            state.files
        );
        Ok(())
    }

    #[tokio::test]
    async fn confirmation_failure_pauses_then_resumes_or_fences() -> Result<()> {
        use axum::{Json, Router, response::IntoResponse, routing::post};
        use std::sync::atomic::{AtomicU8, Ordering};

        let gateway = Arc::new(AtomicU8::new(0));
        let router = Router::new()
            .route(
                "/v3/kv/txn",
                post({
                    let gateway = gateway.clone();
                    move || async move {
                        match gateway.load(Ordering::Relaxed) {
                            1 => StatusCode::SERVICE_UNAVAILABLE.into_response(),
                            2 => Json(json!({"header":{"revision":"2"}})).into_response(),
                            _ => Json(json!({"succeeded":true,"header":{"revision":"1"}}))
                                .into_response(),
                        }
                    }
                }),
            )
            .route(
                "/v3/lease/keepalive",
                post(|| async { Json(json!({"result":{"ID":"1","TTL":"6"}})) }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?).parse()?;
        let server = tokio::spawn(async move { axum::serve(listener, router).await });
        let scratch = tempfile::tempdir()?;
        let control = Control::new(
            Metadata::new(reqwest::Client::new(), vec![endpoint], "jitter"),
            Objects::open(scratch.path().to_str().unwrap(), "jitter")?,
        );
        let stop = CancellationToken::new();
        let leader = Leadership {
            control,
            lease: "1".into(),
            value: "test".into(),
            window: Mutex::new(LeaseWindow {
                until: Instant::now() + LOCAL_VALIDITY,
                uncertain: CancellationToken::new(),
                generation: 0,
                loss: None,
            }),
            metrics: Arc::default(),
            elected: false,
            stop: stop.clone(),
        };

        let error = leader
            .commit("catalog", 0, &"new", Some((Uuid::new_v4(), &"receipt")))
            .await
            .unwrap_err();
        assert_eq!(
            error.code,
            unavailable().code,
            "an unsent write is retryable"
        );
        gateway.store(1, Ordering::Relaxed);
        let error = leader
            .commit("catalog", 0, &"new", None::<(Uuid, &())>)
            .await
            .unwrap_err();
        assert_eq!(
            error.code, MUTATION_UNKNOWN,
            "an absent acknowledgment is ambiguous"
        );
        gateway.store(0, Ordering::Relaxed);

        let epoch = leader
            .confirm()
            .await
            .map_err(|err| anyhow::anyhow!(err.message))?;
        gateway.store(1, Ordering::Relaxed);
        assert_eq!(leader.confirm().await.unwrap_err().code, UNCERTAIN);
        assert!(epoch.is_cancelled());
        assert!(!leader.eligible() && !stop.is_cancelled());
        gateway.store(0, Ordering::Relaxed);
        assert_eq!(leader.confirm().await.unwrap_err().code, UNCERTAIN);
        let resumed = leader
            .check(true)
            .await
            .map_err(|err| anyhow::anyhow!(err.message))?;
        assert!(!resumed.is_cancelled() && epoch.is_cancelled());

        gateway.store(2, Ordering::Relaxed);
        assert_eq!(
            leader.confirm().await.unwrap_err().code,
            "coordinator_not_leader"
        );
        assert!(resumed.is_cancelled() && stop.is_cancelled());
        gateway.store(0, Ordering::Relaxed);
        assert!(leader.check(true).await.is_err());
        let counts = leader.metrics.counts.lock().unwrap();
        assert_eq!(
            counts.failures[ConfirmationFailure::Unavailable as usize],
            1
        );
        assert_eq!(counts.losses[ConfirmationFailure::OwnerChanged as usize], 1);
        server.abort();
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires GRAPHLAKE_TEST_ETCD"]
    async fn stale_leadership_cannot_replace_committed_state() -> Result<()> {
        let endpoint = std::env::var("GRAPHLAKE_TEST_ETCD")?.parse()?;
        let name = format!("ha-test-{}", Uuid::new_v4());
        let scratch = tempfile::tempdir()?;
        let control = Control::new(
            Metadata::new(reqwest::Client::new(), vec![endpoint], &name),
            Objects::open(scratch.path().to_str().unwrap(), &name)?,
        );
        let old_stop = CancellationToken::new();
        let old = control.acquire(&old_stop).await?;
        let receipt_id = Uuid::new_v4();
        let first = old
            .commit("sessions", 0, &"old", Some((receipt_id, &"saved")))
            .await
            .map_err(|e| anyhow::anyhow!(e.message))?
            .context("initial write")?;
        // Simulate an expired lease without shutting down the old service.
        control.revoke(&old.lease).await;
        let new_stop = CancellationToken::new();
        let new = control.acquire(&new_stop).await?;
        let fresh = new
            .commit("sessions", first, &"new", None::<(Uuid, &())>)
            .await
            .map_err(|e| anyhow::anyhow!(e.message))?
            .context("new owner write")?;
        let blocked = old
            .commit("sessions", fresh, &"stale", None::<(Uuid, &())>)
            .await
            .unwrap_err();
        assert_eq!(blocked.code, "coordinator_not_leader");
        assert_eq!(
            control.current::<String>("sessions").await?.unwrap().0,
            "new"
        );
        assert_eq!(
            control
                .current::<String>(&format!("receipts/{receipt_id}"))
                .await?
                .unwrap()
                .0,
            "saved"
        );
        new.release().await;
        Ok(())
    }
}
