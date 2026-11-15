use std::{
    fs,
    io::{BufReader, Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use axum::{Extension, Json, extract::State as AppState, http::StatusCode};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{
    ApiResult, App, Error, access,
    cluster::{Coordinator, control::CatalogState},
    ha, models, users,
};
use crate::CancellationToken;

// The backup still uses the portable v1 JSON format, including the files
// rather than references into the HA object store.
const LIMIT: u64 = 2 * super::cluster::control::CATALOG_BYTES_LIMIT + 4 * 1024 * 1024;
const FORMAT: u32 = 1;
const MANUAL_BACKUPS: usize = 16;

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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    artifact: Option<String>,
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
    manual: ManualStatus,
}

#[derive(Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
enum ManualStatus {
    Unsupported,
    NotConfigured,
    Enabled { running: bool, job: JobStatus },
}

#[derive(Serialize)]
pub(super) struct Exported {
    file: String,
    checkpoint_ms: u64,
    status_recorded: bool,
}

const MANUAL_STATUS: &str = "manual-export-status.json";

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
    let manual_dir = app.manual_backup_dir.clone();
    let (export, offline, manual_job) = tokio::time::timeout(
        Duration::from_secs(2),
        tokio::task::spawn_blocking(move || {
            (
                job_status(&dir, "export-status.json"),
                job_status(&dir, "verify-status.json"),
                manual_dir.map_or(JobStatus::NotRecorded, |dir| {
                    job_status(&dir, MANUAL_STATUS)
                }),
            )
        }),
    )
    .await
    .ok()
    .and_then(Result::ok)
    .unwrap_or((
        JobStatus::Unavailable,
        JobStatus::Unavailable,
        JobStatus::Unavailable,
    ));
    let manual = if app.ha.is_none() {
        ManualStatus::Unsupported
    } else if app.manual_backup_dir.is_none() {
        ManualStatus::NotConfigured
    } else {
        ManualStatus::Enabled {
            running: app.backup_work.available_permits() == 0,
            job: manual_job,
        }
    };
    Ok(Json(BackupOverview {
        export,
        offline,
        recovery_configured,
        manual,
    }))
}

pub(super) async fn export(
    AppState(app): AppState<Arc<App>>,
    Extension(principal): Extension<access::Principal>,
) -> ApiResult<Json<Exported>> {
    if principal != access::Principal::Admin {
        return Err(access::forbidden());
    }
    let dir = app.manual_backup_dir.clone().ok_or_else(|| {
        if app.ha.is_some() {
            Error::invalid("backup_not_configured", "请在服务主机配置手动备份目录")
        } else {
            Error::invalid(
                "backup_not_supported",
                "手动控制状态备份需要启用 Coordinator 故障接管",
            )
        }
    })?;
    let slot = app
        .backup_work
        .clone()
        .try_acquire_owned()
        .map_err(|_| Error::busy())?;
    let control = app
        .cluster
        .as_ref()
        .expect("HA requires a cluster")
        .control()
        .map_err(|_| Error::storage())?;
    // Once accepted, keep the slot until the file and status have been written even if the client leaves.
    tokio::spawn(async move {
        let state = match tokio::time::timeout(Duration::from_secs(30), control.checkpoint()).await
        {
            Ok(Ok((catalog, policy, users))) => Ok(State {
                catalog,
                policy,
                users,
            }),
            Ok(Err(_)) => Err(Error::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "backup_checkpoint_unavailable",
                "无法读取集群控制状态，请检查 etcd 和对象存储",
            )),
            Err(_) => Err(Error::new(
                StatusCode::GATEWAY_TIMEOUT,
                "backup_checkpoint_timeout",
                "读取集群控制状态超时",
            )),
        };
        tokio::task::spawn_blocking(move || {
            let _slot = slot;
            persist_export(dir, state)
        })
        .await
        .map_err(|_| Error::storage())?
    })
    .await
    .map_err(|_| Error::storage())?
    .map(Json)
}

fn persist_export(dir: PathBuf, state: ApiResult<State>) -> ApiResult<Exported> {
    let meta = fs::symlink_metadata(&dir).ok();
    if !dir.is_absolute()
        || !meta.as_ref().is_some_and(|meta| {
            meta.is_dir() && super::auth::private_permissions(meta, false).is_ok()
        })
    {
        return Err(Error::invalid(
            "backup_directory",
            "手动备份目录必须是权限为 0700 的现有绝对路径",
        ));
    }
    let file = format!(
        "control-manual-{}-{}.json",
        chrono::Utc::now().format("%Y%m%dT%H%M%SZ"),
        uuid::Uuid::new_v4().simple()
    );
    let result =
        state.and_then(|state| {
            let files = fs::read_dir(&dir).map_err(|_| Error::storage())?;
            let mut count = 0;
            for entry in files {
                let entry = entry.map_err(|_| Error::storage())?;
                if entry.file_name().to_str().is_some_and(|name| {
                    name.starts_with("control-manual-") && name.ends_with(".json")
                }) && entry.file_type().map_err(|_| Error::storage())?.is_file()
                {
                    count += 1;
                    if count >= MANUAL_BACKUPS {
                        return Err(Error::new(
                            StatusCode::INSUFFICIENT_STORAGE,
                            "backup_retention_limit",
                            "手动备份目录已保留 16 份备份，请移走已有备份后重试",
                        ));
                    }
                }
            }
            write(&dir.join(&file), state).map_err(|_| {
                Error::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "backup_write_failed",
                    "校验或写入备份失败，请检查模型归档和目录权限",
                )
            })
        });
    let status = record_export_status(
        &dir.join(MANUAL_STATUS),
        result
            .as_ref()
            .map(|timestamp| Some(*timestamp))
            .map_err(|error| format_error(error.clone())),
        Some(&file),
    );
    let checkpoint_ms = result?;
    super::telemetry::event(
        opentelemetry::logs::Severity::Info,
        "backup_exported",
        serde_json::json!({}),
    );
    Ok(Exported {
        file,
        checkpoint_ms,
        status_recorded: status.is_ok(),
    })
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
    struct HashWriter(Sha256);
    impl Write for HashWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.update(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut writer = HashWriter(Sha256::new());
    serde_json::to_writer(&mut writer, state)?;
    Ok(writer
        .0
        .finalize()
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
        metadata.is_file() && metadata.len() <= LIMIT,
        "invalid backup file"
    );
    super::auth::private_permissions(&metadata, true)?;
    let mut reader = BufReader::new(file.take(LIMIT + 1));
    let backup: Backup = serde_json::from_reader(&mut reader).context("invalid HA backup")?;
    ensure!(reader.into_inner().limit() > 0, "backup exceeds size limit");
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

fn write(path: &Path, state: State) -> Result<u64> {
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
        temp.as_file().metadata()?.len() <= LIMIT,
        "backup exceeds size limit"
    );
    temp.as_file().sync_all()?;
    temp.persist_noclobber(path)
        .context("publishing private backup file")?;
    #[cfg(unix)]
    fs::File::open(parent)?.sync_all()?;
    Ok(timestamp)
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
    record_export_status(path, result, None)
}

fn record_export_status(
    path: &Path,
    result: Result<Option<u64>>,
    artifact: Option<&str>,
) -> Result<()> {
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
        status.artifact = artifact.map(str::to_owned);
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

    #[test]
    fn manual_export_keeps_the_verified_file_and_checkpoint_after_a_failed_attempt() -> Result<()> {
        let dir = tempfile::tempdir()?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700))?;
        }
        let scheduled = dir.path().join("export-status.json");
        record_status(&scheduled, Ok(Some(123)))?;
        let result = persist_export(
            dir.path().to_owned(),
            Ok(State {
                catalog: CatalogState::default(),
                policy: "{}".to_owned(),
                users: serde_json::json!({"accounts": {}, "groups": {}}),
            }),
        )
        .map_err(format_error)?;
        assert!(result.status_recorded);
        let archive = read(&dir.path().join(&result.file))?;
        assert_eq!(archive.created_at_ms, Some(result.checkpoint_ms));

        let failed = persist_export(dir.path().to_owned(), Err(Error::busy()));
        assert!(matches!(failed, Err(error) if error.code == "busy"));
        let JobStatus::Recorded(manual) = job_status(dir.path(), MANUAL_STATUS) else {
            panic!("manual export result must survive a failed subsequent attempt");
        };
        assert!(!manual.ok);
        assert_eq!(manual.artifact.as_deref(), Some(result.file.as_str()));
        assert_eq!(manual.checkpoint_ms, Some(result.checkpoint_ms));
        let JobStatus::Recorded(timer) = job_status(dir.path(), "export-status.json") else {
            panic!("the scheduled result must remain readable");
        };
        assert!(timer.ok);
        assert_eq!(timer.checkpoint_ms, Some(123));
        Ok(())
    }
}
