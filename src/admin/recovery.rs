use std::{
    collections::BTreeMap,
    path::{Component, Path},
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use axum::{
    Json,
    body::to_bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tokio::io::AsyncReadExt as _;
use uuid::Uuid;

use super::{ApiResult, App, Error, access::Principal, models, query};
use crate::source::{ReadStats, postgres::Snapshot};

const LIMIT: usize = 256 * 1024;
const TIMEOUT: Duration = Duration::from_secs(240);

#[derive(Clone, Deserialize, Serialize)]
pub(super) struct Gate {
    pub manifest_sha256: String,
    pub token_sha256: String,
    pub approved: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bundle: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    format: u32,
    control_sha256: String,
    postgres: Postgres,
    parquet: BTreeMap<String, String>,
    canaries: Vec<Canary>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Postgres {
    dump_sha256: String,
    dsn_env: String,
    relations: BTreeMap<String, String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Canary {
    model: String,
    revision: String,
    language: query::Dialect,
    query: String,
    expected_rows: Vec<Vec<Option<String>>>,
}

#[derive(Deserialize)]
struct VerifiedQuery {
    model_revision: String,
    rows: Vec<Vec<Option<String>>>,
    truncated: bool,
}

fn digest(value: &[u8]) -> String {
    Sha256::digest(value)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn relative_file(path: &str) -> bool {
    !path.is_empty()
        && Path::new(path)
            .components()
            .all(|part| matches!(part, Component::Normal(_)))
}

fn versioned_manifest(dir: &Path) -> Result<(Manifest, String)> {
    let raw = models::read_file(&dir.join("manifest.json"), LIMIT)
        .map_err(|err| anyhow::anyhow!(err.message))?
        .context("recovery manifest.json is missing")?;
    let manifest: Manifest = serde_json::from_str(&raw).context("invalid recovery manifest")?;
    ensure!(
        manifest.format == 1
            && valid_digest(&manifest.control_sha256)
            && valid_digest(&manifest.postgres.dump_sha256)
            && !manifest.postgres.dsn_env.is_empty()
            && !manifest.postgres.relations.is_empty()
            && manifest
                .postgres
                .relations
                .values()
                .all(|sha| valid_digest(sha))
            && !manifest.parquet.is_empty()
            && manifest
                .parquet
                .iter()
                .all(|(path, sha)| relative_file(path) && valid_digest(sha))
            && (1..=16).contains(&manifest.canaries.len())
            && manifest
                .canaries
                .iter()
                .all(|check| models::valid_id(&check.model)
                    && models::versions::valid_revision(&check.revision)
                    && !check.query.trim().is_empty()
                    && check.query.len() <= 64 * 1024
                    && (1..=50).contains(&check.expected_rows.len())),
        "invalid recovery manifest fields"
    );
    Ok((manifest, digest(raw.as_bytes())))
}

async fn checksum(dir: &Path, path: &str) -> Result<String> {
    let root = tokio::fs::canonicalize(dir).await?;
    let file = tokio::fs::canonicalize(root.join(path)).await?;
    ensure!(
        file.starts_with(&root),
        "recovery file escapes its data directory"
    );
    ensure!(
        tokio::fs::metadata(&file).await?.is_file(),
        "expected a recovery file"
    );
    let mut file = tokio::fs::File::open(file).await?;
    let mut hash = Sha256::new();
    let mut buffer = vec![0; 128 * 1024];
    loop {
        let bytes = file.read(&mut buffer).await?;
        if bytes == 0 {
            break;
        }
        hash.update(&buffer[..bytes]);
    }
    Ok(hash
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

pub(super) async fn prepare(dir: &Path, control_sha256: &str) -> Result<Gate> {
    let (manifest, manifest_sha256) = versioned_manifest(dir)?;
    ensure!(
        manifest.control_sha256 == control_sha256,
        "control checkpoint does not match recovery manifest"
    );
    ensure!(
        checksum(dir, "postgres.dump").await? == manifest.postgres.dump_sha256,
        "PostgreSQL dump does not match recovery manifest"
    );
    Ok(Gate {
        manifest_sha256,
        token_sha256: configured_token()?,
        approved: false,
        bundle: None,
    })
}

fn configured_token() -> Result<String> {
    let token =
        std::env::var("GRAPHLAKE_RECOVERY_TOKEN").context("missing GRAPHLAKE_RECOVERY_TOKEN")?;
    ensure!(
        valid_digest(&token),
        "set GRAPHLAKE_RECOVERY_TOKEN using openssl rand -hex 32"
    );
    Ok(digest(token.as_bytes()))
}

pub async fn fingerprint_postgres(
    env: &str,
    relations: &[String],
) -> Result<BTreeMap<String, String>> {
    tokio::time::timeout(TIMEOUT, async {
        let snapshot = Snapshot::open(env, TIMEOUT, Arc::new(ReadStats::default())).await?;
        let mut hashes = BTreeMap::new();
        for relation in relations {
            hashes.insert(relation.clone(), snapshot.recovery_digest(relation).await?);
        }
        Ok(hashes)
    })
    .await
    .context("timed out fingerprinting PostgreSQL recovery data")?
}

pub(super) fn blocked() -> Error {
    Error::new(
        StatusCode::SERVICE_UNAVAILABLE,
        "recovery_pending",
        "恢复集群正在等待来源及业务查询验收",
    )
}

pub(super) async fn probe(State(app): State<Arc<App>>) -> StatusCode {
    if let Some(ha) = &app.ha
        && ha.recovery_configured
        && ha.leader.confirm().await.is_ok()
    {
        return StatusCode::OK;
    }
    StatusCode::SERVICE_UNAVAILABLE
}

#[derive(Serialize)]
pub(super) struct RecoveryStatus {
    manifest_sha256: String,
    approved: bool,
    ready: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    restored_backup: Option<String>,
}

#[derive(Serialize)]
pub(super) struct ManifestStatus {
    manifest_sha256: String,
    postgres_relations: usize,
    parquet_files: usize,
    canaries: usize,
}

fn state(app: &App, gate: Gate) -> RecoveryStatus {
    RecoveryStatus {
        approved: gate.approved,
        ready: gate.approved
            && !app.shutdown.is_cancelled()
            && app.ha_ready.load(Ordering::Acquire)
            && app
                .ha
                .as_ref()
                .is_some_and(|ha| !ha.recovery_pending() && ha.leader.eligible()),
        manifest_sha256: gate.manifest_sha256,
        restored_backup: gate.bundle,
    }
}

async fn protected_gate(app: &App, headers: &HeaderMap) -> ApiResult<(Gate, i64)> {
    let ha = app
        .ha
        .as_ref()
        .filter(|ha| ha.recovery_configured)
        .ok_or_else(Error::not_found)?;
    let (gate, version) = ha
        .leader
        .get::<Gate>("recovery")
        .await?
        .ok_or_else(Error::not_found)?;
    authorize(headers, &gate.token_sha256)?;
    Ok((gate, version))
}

fn authorize(headers: &HeaderMap, expected: &str) -> ApiResult<()> {
    let mut tokens = headers.get_all("x-graphlake-recovery-token").iter();
    let token = tokens
        .next()
        .and_then(|value| value.to_str().ok())
        .filter(|token| valid_digest(token))
        .ok_or_else(Error::not_found)?;
    if tokens.next().is_some() {
        return Err(Error::not_found());
    }
    if !bool::from(
        digest(token.as_bytes())
            .as_bytes()
            .ct_eq(expected.as_bytes()),
    ) {
        return Err(Error::not_found());
    }
    Ok(())
}

pub(super) async fn status(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
) -> ApiResult<Json<RecoveryStatus>> {
    let (gate, _) = protected_gate(&app, &headers).await?;
    Ok(Json(state(&app, gate)))
}

pub(super) async fn preflight(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
) -> ApiResult<Json<ManifestStatus>> {
    let (gate, _) = protected_gate(&app, &headers).await?;
    let ha = app.ha.as_ref().ok_or_else(Error::not_found)?;
    let _slot = ha.recovery_check.try_lock().map_err(|_| Error::busy())?;
    let manifest = tokio::time::timeout(TIMEOUT, artifacts(&app, &gate))
        .await
        .map_err(|_| timed_out())??;
    Ok(Json(ManifestStatus {
        manifest_sha256: gate.manifest_sha256,
        postgres_relations: manifest.postgres.relations.len(),
        parquet_files: manifest.parquet.len(),
        canaries: manifest.canaries.len(),
    }))
}

// The canaries use the normal Coordinator query and Worker execution path.
pub(super) async fn verify(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
) -> ApiResult<Json<RecoveryStatus>> {
    let ha = app.ha.as_ref().ok_or_else(Error::not_found)?;
    let (mut gate, version) = protected_gate(&app, &headers).await?;
    let _slot = ha.recovery_check.try_lock().map_err(|_| Error::busy())?;
    if !gate.approved {
        require_ready(&app)?;
        let leader = ha.leader.admission()?;
        let result = tokio::select! {
            biased;
            _ = leader.cancelled() => Err(super::cluster::control::interrupted()),
            _ = app.shutdown.cancelled() => Err(super::cluster::control::lost()),
            result = tokio::time::timeout(TIMEOUT, check(&app, &gate)) => result.map_err(|_| timed_out())?,
        };
        result?;
        require_ready(&app)?;
        gate.approved = true;
        ha.leader.confirm().await?;
        if ha
            .leader
            .commit("recovery", version, &gate, None::<(Uuid, &String)>)
            .await?
            .is_none()
        {
            return Err(blocked());
        }
        super::telemetry::event(
            opentelemetry::logs::Severity::Info,
            "recovery_approved",
            serde_json::json!({"manifest_sha256": gate.manifest_sha256}),
        );
    }
    ha.recovery_pending.store(false, Ordering::Release);
    Ok(Json(state(&app, gate)))
}

fn require_ready(app: &App) -> ApiResult<()> {
    if app.ha_ready.load(Ordering::Acquire) {
        return Ok(());
    }
    Err(Error::new(
        StatusCode::SERVICE_UNAVAILABLE,
        "recovery_not_ready",
        "目标站的 Worker 或控制存储未就绪",
    ))
}

fn timed_out() -> Error {
    Error::new(
        StatusCode::GATEWAY_TIMEOUT,
        "recovery_timeout",
        "恢复检查超时",
    )
}

async fn artifacts(app: &App, gate: &Gate) -> ApiResult<Manifest> {
    let dir = match &gate.bundle {
        Some(bundle) => import::inbox().and_then(|root| import::bundle_dir(&root, bundle)),
        None => std::env::var_os("GRAPHLAKE_RECOVERY_DIR")
            .map(std::path::PathBuf::from)
            .context("未配置恢复目录 GRAPHLAKE_RECOVERY_DIR"),
    }
    .map_err(|error| failed(error.to_string()))?;
    let (manifest, sha) = versioned_manifest(&dir).map_err(|_| failed("恢复清单无效"))?;
    if sha != gate.manifest_sha256 {
        return Err(failed("恢复清单与控制版本不匹配"));
    }
    if checksum(&dir, "postgres.dump").await.ok().as_deref() != Some(&manifest.postgres.dump_sha256)
    {
        return Err(failed("PostgreSQL 备份摘要不匹配"));
    }
    let source_dir = app.models.lock().unwrap().source_directory().to_owned();
    for (path, expected) in &manifest.parquet {
        if checksum(&source_dir, path).await.ok().as_deref() != Some(expected.as_str()) {
            return Err(failed(format!("Parquet 摘要不匹配 · {path}")));
        }
    }
    Ok(manifest)
}

async fn check(app: &Arc<App>, gate: &Gate) -> ApiResult<()> {
    let manifest = artifacts(app, gate).await?;
    let relations: Vec<_> = manifest.postgres.relations.keys().cloned().collect();
    let actual = fingerprint_postgres(&manifest.postgres.dsn_env, &relations)
        .await
        .map_err(|_| failed("无法读取已恢复的 PostgreSQL 数据"))?;
    if actual != manifest.postgres.relations {
        return Err(failed("PostgreSQL 数据与清单中的恢复点不匹配"));
    }
    let cluster = app.cluster.as_ref().ok_or_else(blocked)?;
    for canary in manifest.canaries {
        let revision = app
            .models
            .lock()
            .unwrap()
            .require_published(&canary.model)?;
        if revision != canary.revision {
            return Err(failed(format!("模型发布版本不匹配 · {}", canary.model)));
        }
        let request = serde_json::from_value::<query::Request>(serde_json::json!({
            "request_id": Uuid::new_v4(), "model_revision": canary.revision,
            "language": canary.language, "query": canary.query, "preview_without_arrow": true,
        }))
        .map_err(|_| failed("业务查询配置无效"))?;
        let response = cluster
            .execute(app, canary.model.clone(), Principal::Admin, request, false)
            .await?;
        if !response.status().is_success() {
            return Err(failed(format!("业务查询执行失败 · {}", canary.model)));
        }
        let body = to_bytes(response.into_body(), 2 * 1024 * 1024)
            .await
            .map_err(|_| failed("无法读取业务查询结果"))?;
        let actual: VerifiedQuery =
            serde_json::from_slice(&body).map_err(|_| failed("业务查询响应格式无效"))?;
        if actual.truncated
            || actual.rows != canary.expected_rows
            || actual.model_revision != canary.revision
        {
            return Err(failed(format!("业务查询结果不匹配 · {}", canary.model)));
        }
    }
    Ok(())
}

fn failed(message: impl Into<String>) -> Error {
    Error::new(
        StatusCode::UNPROCESSABLE_ENTITY,
        "recovery_verification_failed",
        message,
    )
}
mod data;
pub(super) mod import;
