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

use super::local_tables::managed::{self, Archive, Managed};

mod local;

const LIMIT: u64 = 2 * super::cluster::control::CATALOG_BYTES_LIMIT + 24 * 1024 * 1024;
const FORMAT: u32 = 1;
const MANUAL_BACKUPS: usize = 16;
pub(super) const CLI_RESTORE_KEY: &str = "restore_cli";

#[derive(Deserialize, Serialize)]
struct State {
    catalog: CatalogState,
    policy: String,
    users: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    local_tables: Option<local::Local>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    local_directory: Option<String>,
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
    let sources = app.models.lock().unwrap().source_directory().to_owned();
    let managed = managed::backend(&app).cloned();
    // Once accepted, keep the slot until the file and status have been written even if the client leaves.
    tokio::spawn(async move {
        let state = match tokio::time::timeout(Duration::from_secs(30), control.checkpoint()).await
        {
            Ok(Ok((catalog, policy, users))) => Ok(State {
                catalog,
                policy,
                users,
                local_tables: None,
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
        let (state, archive) = match (state, managed) {
            (Ok(state), Some(managed)) => match managed.backup().await {
                Ok(archive) => (Ok(state), archive),
                Err(error) => (Err(managed::error(error)), None),
            },
            (state, _) => (state, None),
        };
        let (result, archive) = tokio::task::spawn_blocking(move || {
            let _slot = slot;
            let result = persist_export(dir, state, Some(&sources), archive.as_ref());
            (result, archive)
        })
        .await
        .map_err(|_| Error::storage())?;
        if let Some(archive) = archive {
            archive.release().await;
        }
        result
    })
    .await
    .map_err(|_| Error::storage())?
    .map(Json)
}

fn persist_export(
    dir: PathBuf,
    state: ApiResult<State>,
    sources: Option<&Path>,
    archive: Option<&Archive>,
) -> ApiResult<Exported> {
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
            write(&dir.join(&file), state, sources, archive).map_err(|error| {
                Error::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "backup_write_failed",
                    format!("校验或写入备份失败 · {error}"),
                )
            })
        });
    let status = record_export_status(
        &dir.join(MANUAL_STATUS),
        result
            .as_ref()
            .map(|(timestamp, _)| Some(*timestamp))
            .map_err(|error| format_error(error.clone())),
        Some(&file),
    );
    let (checkpoint_ms, local_directory) = result?;
    super::telemetry::event(
        opentelemetry::logs::Severity::Info,
        "backup_exported",
        serde_json::json!({}),
    );
    Ok(Exported {
        file,
        checkpoint_ms,
        status_recorded: status.is_ok(),
        local_directory,
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

fn verify(state: &State, target: Option<&Path>) -> Result<()> {
    access::Policy::validate_config(&state.policy).map_err(format_error)?;
    users::verify_backup(&state.users).map_err(format_error)?;
    let dir = tempfile::tempdir()?;
    ha::restore(dir.path(), &state.catalog).map_err(format_error)?;
    let root = target.or_else(|| state.local_tables.as_ref().map(local::Local::origin));
    let catalog =
        models::Models::with_sources(dir.path().to_owned(), root.unwrap_or(dir.path()).to_owned());
    let (_, broken) = catalog.check_published().map_err(format_error)?;
    ensure!(
        broken.is_empty(),
        "published model archives failed verification: {broken:?}"
    );
    catalog.check_archive_history().map_err(format_error)?;
    if let Some(root) = root {
        if let Some(local) = &state.local_tables {
            local.validate(&catalog, root)?;
        } else {
            local::validate_models(&catalog, root, &Default::default(), &Default::default())?;
        }
    }
    Ok(())
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
    verify(&backup.state, None)?;
    Ok(backup)
}

fn write(
    path: &Path,
    mut state: State,
    sources: Option<&Path>,
    archive: Option<&Archive>,
) -> Result<(u64, Option<String>)> {
    let parent = parent(path);
    ensure!(
        matches!(fs::symlink_metadata(path), Err(e) if e.kind() == std::io::ErrorKind::NotFound),
        "backup file already exists or is inaccessible"
    );
    let sources = sources.map(Path::canonicalize).transpose()?;
    let disk = sources
        .as_deref()
        .map(|root| local::Capture::take(root, false))
        .transpose()?
        .flatten();
    let remote = archive
        .map(|archive| local::Capture::take(archive.root.path(), true))
        .transpose()?
        .flatten();
    let captured = if remote.is_some() {
        local::Capture::combine(
            disk,
            remote,
            sources
                .as_deref()
                .context("managed backup requires --catalog-dir")?,
        )?
    } else {
        disk
    };
    let timestamp = now_ms();
    state.local_tables = captured
        .map(|snapshot| snapshot.write(parent))
        .transpose()?;
    let local_directory = state.local_tables.as_ref().map(local::Local::directory);
    let result = publish(path, state, sources.as_deref(), timestamp);
    if result.is_err()
        && let Some(dir) = &local_directory
    {
        let _ = fs::remove_dir_all(parent.join(dir));
    }
    result?;
    #[cfg(unix)]
    fs::File::open(parent)?.sync_all()?;
    Ok((timestamp, local_directory))
}

fn parent(path: &Path) -> &Path {
    path.parent()
        .filter(|dir| !dir.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

fn publish(path: &Path, state: State, sources: Option<&Path>, timestamp: u64) -> Result<()> {
    let unspecified = Path::new("/");
    verify(&state, Some(sources.unwrap_or(unspecified)))?;
    let sha256 = checksum(&state)?;
    let backup = Backup {
        format: FORMAT,
        timestamp_sha256: Some(timestamp_checksum(&sha256, timestamp)),
        sha256,
        state,
        created_at_ms: Some(timestamp),
    };
    let parent = parent(path);
    let mut temp = tempfile::NamedTempFile::new_in(parent)?;
    {
        let mut writer = std::io::BufWriter::with_capacity(64 * 1024, temp.as_file_mut());
        serde_json::to_writer(&mut writer, &backup)?;
        writer.flush()?;
    }
    ensure!(
        temp.as_file().metadata()?.len() <= LIMIT,
        "backup exceeds size limit"
    );
    temp.as_file().sync_all()?;
    temp.persist_noclobber(path)
        .context("publishing private backup file")?;
    Ok(())
}

pub async fn run(config: &Path, file: &Path, recovery_dir: Option<&Path>) -> Result<()> {
    run_with_catalog(config, file, recovery_dir, None)
        .await
        .map(|_| ())
}

/// Export a checkpoint or restore into a fresh namespace and source directory.
pub async fn run_with_catalog(
    config: &Path,
    file: &Path,
    recovery_dir: Option<&Path>,
    catalog_dir: Option<&Path>,
) -> Result<Option<u64>> {
    let cluster = Coordinator::load(config)?;
    let control = cluster.control()?;
    let Some(recovery_dir) = recovery_dir else {
        let (catalog, policy, users) = control.checkpoint().await.map_err(format_error)?;
        let archive = match &cluster.managed {
            Some(managed) => managed.backup().await?,
            None => None,
        };
        let written = write(
            file,
            State {
                catalog,
                policy,
                users,
                local_tables: None,
            },
            catalog_dir,
            archive.as_ref(),
        );
        if let Some(archive) = archive {
            archive.release().await;
        }
        let (timestamp, local_directory) = written?;
        if let Some(directory) = local_directory {
            eprintln!("GraphLake local-table backup: {directory}");
        }
        super::telemetry::event(
            opentelemetry::logs::Severity::Info,
            "backup_exported",
            serde_json::json!({}),
        );
        return Ok(Some(timestamp));
    };

    let backup = read(file)?;
    let mut recovery = super::recovery::prepare(recovery_dir, &backup.sha256).await?;
    if backup.local_count() != 0 {
        let bound = read(&recovery_dir.join("control.json"))?;
        ensure!(
            bound.sha256 == backup.sha256,
            "recovery directory control.json does not match --file"
        );
        recovery.local_checkpoint = Some(backup.sha256.clone());
    }
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
        leader
            .ensure_empty_checkpoint()
            .await
            .map_err(format_error)?;
        match leader
            .get::<String>(CLI_RESTORE_KEY)
            .await
            .map_err(format_error)?
        {
            Some((bound, _)) => ensure!(
                bound == recovery.manifest_sha256,
                "target is reserved for a different CLI recovery checkpoint"
            ),
            None => ensure!(
                leader
                    .commit(
                        CLI_RESTORE_KEY,
                        0,
                        &recovery.manifest_sha256,
                        None::<(uuid::Uuid, &String)>
                    )
                    .await
                    .map_err(format_error)?
                    .is_some(),
                "could not reserve the CLI recovery namespace"
            ),
        }
        let backup = if backup.local_count() != 0 {
            let target = catalog_dir
                .context("restoring local tables requires --catalog-dir")?
                .canonicalize()?;
            backup
                .install_local(&leader, parent(file), &target, cluster.managed.as_ref())
                .await?
        } else {
            backup
        };
        backup.restore(&leader, &recovery).await
    }
    .await;
    leader.release().await;
    result?;
    eprintln!(
        "GraphLake HA checkpoint restored. Member accounts are disabled, role memberships and legacy service tokens cleared, and sessions invalidated."
    );
    Ok(None)
}

impl Backup {
    pub(super) fn local_count(&self) -> usize {
        self.state
            .local_tables
            .as_ref()
            .map_or(0, local::Local::count)
    }

    pub(super) fn validate_target(&self, target: &Path) -> Result<()> {
        if self.state.local_tables.is_some() {
            verify(&self.state, Some(target))?;
        }
        Ok(())
    }

    pub(super) async fn install_local(
        self,
        leader: &super::cluster::control::Leadership,
        from: &Path,
        target: &Path,
        managed: Option<&Managed>,
    ) -> Result<Self> {
        let Some(local) = &self.state.local_tables else {
            return Ok(self);
        };
        ensure!(
            !local.has_managed() || managed.is_some(),
            "managed backup requires managed_local_tables on the target"
        );
        let restore_managed = local.has_managed();
        let from = from.to_owned();
        let destination = target.to_owned();
        let (staged, backup) = tokio::task::spawn_blocking(move || -> Result<_> {
            self.validate_target(&destination)?;
            let staged = self
                .state
                .local_tables
                .as_ref()
                .unwrap()
                .stage(&from, &destination)?;
            Ok((staged, self))
        })
        .await??;
        leader.confirm().await.map_err(format_error)?;
        if let Some(staged) = staged {
            local::publish(staged, target)?;
        }
        if restore_managed {
            managed
                .expect("validated managed recovery backend")
                .restore_archive(target, leader)
                .await?;
        }
        Ok(backup)
    }

    pub(super) fn verify_local_destination(&self, target: &Path) -> Result<()> {
        self.state
            .local_tables
            .as_ref()
            .context("checkpoint has no local table archive")?
            .verify_installed(target)
    }

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
    if let Some(local) = &backup.state.local_tables {
        local.verify(&parent(file).join(local.directory()))?;
    }
    println!(
        "GraphLake HA backup verified ({} catalog files, {} local tables).",
        backup.state.catalog.files.len(),
        backup.local_count()
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
                local_tables: None,
            }),
            None,
            None,
        )
        .map_err(format_error)?;
        assert!(result.status_recorded);
        let archive = read(&dir.path().join(&result.file))?;
        assert_eq!(archive.created_at_ms, Some(result.checkpoint_ms));

        let failed = persist_export(dir.path().to_owned(), Err(Error::busy()), None, None);
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
