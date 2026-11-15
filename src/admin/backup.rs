use std::{
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use axum::{Extension, Json, extract::State as AppState};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{
    ApiResult, App, Error, access,
    cluster::{Coordinator, control::CatalogState},
    ha, models, users,
};
use crate::CancellationToken;

const LIMIT: usize = 132 * 1024 * 1024;
const FORMAT: u32 = 1;

#[derive(Deserialize, Serialize)]
struct State {
    catalog: CatalogState,
    policy: String,
    users: serde_json::Value,
}

#[derive(Deserialize, Serialize)]
pub(super) struct Backup {
    format: u32,
    pub sha256: String,
    state: State,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    timestamp_sha256: Option<String>,
}

#[derive(Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct BackupStatus {
    ok: bool,
    attempted_ms: u64,
    last_success_ms: Option<u64>,
    checkpoint_ms: Option<u64>,
}

#[derive(Serialize)]
#[serde(tag = "state", content = "result", rename_all = "snake_case")]
enum JobStatus {
    NotRecorded,
    Unavailable,
    Recorded(BackupStatus),
}

#[derive(Serialize)]
pub(super) struct BackupOverview {
    export: JobStatus,
    offline: JobStatus,
    recovery_configured: bool,
}

fn job_status(dir: &Path, name: &str) -> JobStatus {
    match models::read_file(&dir.join(name), 4096) {
        Ok(None) => JobStatus::NotRecorded,
        Ok(Some(raw)) => {
            serde_json::from_str(&raw).map_or(JobStatus::Unavailable, JobStatus::Recorded)
        }
        Err(_) => JobStatus::Unavailable,
    }
}

pub(super) async fn overview(
    AppState(app): AppState<Arc<App>>,
    Extension(principal): Extension<access::Principal>,
) -> ApiResult<Json<BackupOverview>> {
    if principal != access::Principal::Admin {
        return Err(access::forbidden());
    }
    let recovery_configured = app.ha.as_ref().is_some_and(|ha| ha.recovery_configured);
    let dir = std::env::var_os("GRAPHLAKE_BACKUP_STATUS_DIR").map_or_else(
        || PathBuf::from("/var/lib/graphlake-backups"),
        PathBuf::from,
    );
    let (export, offline) = tokio::time::timeout(
        Duration::from_secs(2),
        tokio::task::spawn_blocking(move || {
            (
                job_status(&dir, "export-status.json"),
                job_status(&dir, "verify-status.json"),
            )
        }),
    )
    .await
    .ok()
    .and_then(Result::ok)
    .unwrap_or((JobStatus::Unavailable, JobStatus::Unavailable));
    Ok(Json(BackupOverview {
        export,
        offline,
        recovery_configured,
    }))
}

fn now_ms() -> u64 {
    chrono::Utc::now().timestamp_millis() as u64
}

fn timestamp_checksum(state_hash: &str, timestamp: u64) -> String {
    super::models::revision(&format!("{state_hash}:{timestamp}"))
}

fn format_error(error: Error) -> anyhow::Error {
    anyhow::anyhow!(error.message)
}

fn checksum(state: &State) -> Result<String> {
    Ok(Sha256::digest(serde_json::to_vec(state)?)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

fn verify(state: &State) -> Result<()> {
    access::Policy::validate_config(&state.policy).map_err(format_error)?;
    users::verify_backup(&state.users).map_err(format_error)?;
    let dir = tempfile::tempdir()?;
    ha::restore(dir.path(), &state.catalog).map_err(format_error)?;
    let catalog = models::Models::new(dir.path().to_owned());
    let (_, broken) = catalog.check_published().map_err(format_error)?;
    ensure!(
        broken.is_empty(),
        "published model archives failed verification: {broken:?}"
    );
    catalog.check_archive_history().map_err(format_error)
}

pub(super) fn read(file: &Path) -> Result<Backup> {
    let mut options = fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = options.open(file)?;
    let metadata = file.metadata()?;
    ensure!(
        metadata.is_file() && metadata.len() <= LIMIT as u64,
        "invalid backup file"
    );
    super::auth::private_permissions(&metadata, true)?;
    let mut bytes = Vec::new();
    file.take(LIMIT as u64 + 1).read_to_end(&mut bytes)?;
    ensure!(bytes.len() <= LIMIT, "backup exceeds size limit");
    let backup: Backup = serde_json::from_slice(&bytes).context("invalid HA backup")?;
    ensure!(backup.format == FORMAT, "unsupported HA backup format");
    ensure!(
        checksum(&backup.state)? == backup.sha256,
        "HA backup checksum mismatch"
    );
    ensure!(
        backup
            .created_at_ms
            .map(|timestamp| timestamp_checksum(&backup.sha256, timestamp))
            == backup.timestamp_sha256,
        "HA backup timestamp checksum mismatch"
    );
    verify(&backup.state)?;
    Ok(backup)
}

fn write(path: &Path, state: State) -> Result<()> {
    verify(&state)?;
    let timestamp = now_ms();
    let sha256 = checksum(&state)?;
    let backup = Backup {
        format: FORMAT,
        timestamp_sha256: Some(timestamp_checksum(&sha256, timestamp)),
        sha256,
        state,
        created_at_ms: Some(timestamp),
    };
    let parent = path
        .parent()
        .filter(|dir| !dir.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let mut temp = tempfile::NamedTempFile::new_in(parent)?;
    serde_json::to_writer(&mut temp, &backup)?;
    temp.flush()?;
    ensure!(
        temp.as_file().metadata()?.len() <= LIMIT as u64,
        "backup exceeds size limit"
    );
    temp.as_file().sync_all()?;
    temp.persist_noclobber(path)
        .context("publishing private backup file")?;
    #[cfg(unix)]
    fs::File::open(parent)?.sync_all()?;
    Ok(())
}

/// Export a consistent control-plane checkpoint or restore into a fresh namespace.
pub async fn run(config: &Path, file: &Path, recovery_dir: Option<&Path>) -> Result<()> {
    let control = Coordinator::load(config)?.control()?;
    let Some(recovery_dir) = recovery_dir else {
        let (catalog, policy, users) = control.checkpoint().await.map_err(format_error)?;
        write(
            file,
            State {
                catalog,
                policy,
                users,
            },
        )?;
        super::telemetry::event(
            opentelemetry::logs::Severity::Info,
            "backup_exported",
            serde_json::json!({}),
        );
        return Ok(());
    };

    let backup = read(file)?;
    let recovery = super::recovery::prepare(recovery_dir, &backup.sha256).await?;
    let stop = CancellationToken::new();
    let leader = tokio::time::timeout(Duration::from_secs(2), control.acquire(&stop))
        .await
        .context("could not acquire restore lock; stop all target Coordinators and check etcd")??;
    let result = async {
        ensure!(
            leader
                .get::<serde_json::Value>(super::recovery::import::JOB_KEY)
                .await
                .map_err(format_error)?
                .is_none(),
            "this namespace is reserved for UI recovery; resume from the recovery page"
        );
        backup.restore(&leader, &recovery).await
    }
    .await;
    leader.release().await;
    result?;
    eprintln!(
        "GraphLake HA checkpoint restored. Member accounts are disabled, role memberships and legacy service tokens cleared, and sessions invalidated."
    );
    Ok(())
}

impl Backup {
    pub(super) async fn restore(
        self,
        leader: &super::cluster::control::Leadership,
        recovery: &super::recovery::Gate,
    ) -> Result<()> {
        let mut backup = self.state;
        // The checkpoint predates later credential revocations.
        users::disable_restored_accounts(&mut backup.users).map_err(format_error)?;
        leader
            .restore_checkpoint(&backup.catalog, "{}", &backup.users, recovery)
            .await
            .map_err(format_error)?;
        super::telemetry::event(
            opentelemetry::logs::Severity::Info,
            "backup_restored",
            serde_json::json!({}),
        );
        Ok(())
    }
}

/// Check a transported backup, including all archives, without connecting to a cluster.
pub fn check(file: &Path) -> Result<()> {
    check_checkpoint(file).map(|_| ())
}

/// Verify a checkpoint and report the originating timestamp when available.
pub fn check_checkpoint(file: &Path) -> Result<Option<u64>> {
    let backup = read(file)?;
    println!(
        "GraphLake HA backup verified ({} catalog files).",
        backup.state.catalog.files.len()
    );
    Ok(backup.created_at_ms)
}

/// Publish the outcome separately from the private backup artifact.
pub fn record_status(path: &Path, result: Result<Option<u64>>) -> Result<()> {
    let mut lock = fs::OpenOptions::new();
    lock.write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        lock.mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let mut lock_name = path.as_os_str().to_owned();
    lock_name.push(".lock");
    let lock = lock.open(lock_name)?;
    ensure!(lock.metadata()?.is_file(), "invalid backup status lock");
    lock.lock()?;
    let previous = models::read_file(path, 4096).map_err(format_error)?;
    let mut status: BackupStatus = previous
        .as_deref()
        .map(serde_json::from_str)
        .transpose()
        .context("invalid backup status file")?
        .unwrap_or_default();
    let now = now_ms();
    status.ok = result.is_ok();
    status.attempted_ms = now;
    if let Ok(checkpoint_ms) = &result {
        status.last_success_ms = Some(now);
        status.checkpoint_ms = *checkpoint_ms;
    }
    models::atomic_write(path, &serde_json::to_vec(&status)?, previous.is_none())
        .map_err(format_error)?;
    result.map(|_| ())
}

pub async fn ingress(config: &Path, node: &str) -> Result<()> {
    ensure!(models::valid_id(node), "invalid ingress node ID");
    let owner = tokio::time::timeout(
        Duration::from_millis(700),
        Coordinator::check_ingress(config, node),
    )
    .await
    .context("ingress arbitration timed out")??;
    ensure!(owner, "another ingress holds the VIP lease");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overview_distinguishes_missing_invalid_and_failed_jobs() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let export = dir.path().join("export-status.json");
        assert!(matches!(
            job_status(dir.path(), "export-status.json"),
            JobStatus::NotRecorded
        ));
        record_status(&export, Ok(Some(123)))?;
        let JobStatus::Recorded(first) = job_status(dir.path(), "export-status.json") else {
            panic!("successful job must show up in the overview")
        };
        assert!(first.ok);
        assert_eq!(first.checkpoint_ms, Some(123));
        record_status(
            &export,
            Err(anyhow::anyhow!("backup destination unavailable")),
        )
        .expect_err("recording a failure must still return the task error");
        let JobStatus::Recorded(after) = job_status(dir.path(), "export-status.json") else {
            panic!("failed job must show up in the overview")
        };
        assert!(!after.ok);
        assert_eq!(after.last_success_ms, first.last_success_ms);
        assert_eq!(after.checkpoint_ms, first.checkpoint_ms);
        fs::write(&export, b"{invalid json")?;
        assert!(matches!(
            job_status(dir.path(), "export-status.json"),
            JobStatus::Unavailable
        ));
        Ok(())
    }
}
