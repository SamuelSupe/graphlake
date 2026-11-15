use std::{
    path::{Path as FsPath, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, Request, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use tokio::{net::TcpListener, sync::Mutex};

use super::{Gate, Manifest, authorize, configured_token, data, valid_digest, versioned_manifest};
use crate::{
    CancellationToken,
    admin::{
        ApiResult, Error, assets::Assets, backup, cluster::control::Leadership, exact_header,
        models,
    },
};

pub(in crate::admin) const JOB_KEY: &str = "restore_job";
const RESTORE_TIMEOUT: Duration = Duration::from_secs(4 * 60 * 60);

#[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Phase {
    Validate,
    Parquet,
    Postgres,
    Control,
}

#[derive(Clone, Serialize, Deserialize)]
struct Job {
    bundle: String,
    phase: Phase,
    bound: Option<String>,
    postgres_started: bool,
    failure: Option<String>,
}

#[derive(Serialize)]
struct Status {
    mode: &'static str,
    job: Option<JobView>,
}

#[derive(Serialize)]
struct JobView {
    bundle: String,
    phase: Phase,
    running: bool,
    bound: bool,
    failure: Option<String>,
}

#[derive(Serialize)]
struct Candidate {
    id: String,
    modified_ms: Option<u64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Start {
    bundle: String,
    #[serde(default)]
    manifest_sha256: Option<String>,
}

#[derive(Serialize)]
struct CandidateCheck {
    bundle: String,
    checkpoint_ms: Option<u64>,
    manifest_sha256: String,
    postgres_relations: usize,
    parquet_files: usize,
}

struct Stage {
    assets: Arc<Assets>,
    leader: Arc<Leadership>,
    token: String,
    origin: String,
    inbox: PathBuf,
    target: PathBuf,
    dsn_env: String,
    job: Arc<Mutex<()>>,
    review: Arc<Mutex<()>>,
    shutdown: CancellationToken,
}

pub(super) fn inbox() -> Result<PathBuf> {
    let root = std::env::var_os("GRAPHLAKE_RESTORE_INBOX")
        .context("未配置异地备份目录 GRAPHLAKE_RESTORE_INBOX")?;
    let root = FsPath::new(&root)
        .canonicalize()
        .context("无法读取异地备份目录")?;
    ensure!(root.is_dir(), "异地备份目录无效");
    Ok(root)
}

pub(super) fn bundle_dir(root: &FsPath, bundle: &str) -> Result<PathBuf> {
    ensure!(models::valid_id(bundle), "异地备份标识无效");
    let dir = root.join(bundle);
    ensure!(
        dir.symlink_metadata().is_ok_and(|meta| meta.is_dir()),
        "异地备份目录不可读"
    );
    Ok(dir)
}

pub(in crate::admin) async fn serve(
    listener: TcpListener,
    assets: Arc<Assets>,
    leader: Arc<Leadership>,
    origin: String,
    target: PathBuf,
    shutdown: CancellationToken,
) -> Result<()> {
    leader
        .ensure_empty_checkpoint()
        .await
        .map_err(|error| anyhow::anyhow!(error.message))?;
    let inbox = inbox()?;
    ensure!(
        !inbox.starts_with(&target) && !target.starts_with(&inbox),
        "restore inbox and catalog must be disjoint"
    );
    let dsn_env = std::env::var("GRAPHLAKE_RESTORE_POSTGRES_DSN_ENV")
        .context("missing GRAPHLAKE_RESTORE_POSTGRES_DSN_ENV")?;
    // Persist the reservation before the first web request. Peers may never bootstrap a
    // normal catalog in this namespace, even before an operator picks a backup.
    if leader
        .get::<Option<Job>>(JOB_KEY)
        .await
        .map_err(|e| anyhow::anyhow!(e.message))?
        .is_none()
    {
        ensure!(
            leader
                .commit(JOB_KEY, 0, &None::<Job>, None::<(uuid::Uuid, &String)>)
                .await
                .map_err(|e| anyhow::anyhow!(e.message))?
                .is_some(),
            "could not reserve the recovery namespace"
        );
    }
    let stage = Arc::new(Stage {
        assets,
        leader,
        origin,
        inbox,
        target,
        dsn_env,
        token: configured_token()?,
        job: Arc::new(Mutex::new(())),
        review: Arc::new(Mutex::new(())),
        shutdown: shutdown.clone(),
    });
    let router = Router::new()
        .route("/coordinator/recovery", get(|| async { StatusCode::OK }))
        .route(
            "/healthz",
            get(|| async { Json(serde_json::json!({"status":"restoring"})) }),
        )
        .route("/recovery/status", get(status))
        .route("/recovery/backups", get(backups))
        .route(
            "/recovery/check",
            post(check_candidate).layer(DefaultBodyLimit::max(1024)),
        )
        .route(
            "/recovery/import",
            post(start).layer(DefaultBodyLimit::max(1024)),
        )
        .route("/recovery", get(page))
        .route("/recovery/", get(page))
        .route("/assets/{name}", get(asset))
        .fallback(|| async { super::blocked() })
        .layer(middleware::from_fn_with_state(stage.clone(), guard))
        .with_state(stage.clone());
    eprintln!("GraphLake restore {}", stage.origin);
    let until = async move {
        loop {
            if stage
                .leader
                .get::<Gate>("recovery")
                .await
                .is_ok_and(|gate| gate.is_some())
            {
                break;
            }
            tokio::select! {
                _ = stage.shutdown.cancelled() => break,
                _ = tokio::time::sleep(Duration::from_millis(450)) => {}
            }
        }
    };
    axum::serve(listener, router)
        .with_graceful_shutdown(until)
        .await?;
    ensure!(
        !shutdown.is_cancelled(),
        "Coordinator stopped during recovery"
    );
    Ok(())
}

async fn page(State(stage): State<Arc<Stage>>) -> Response {
    stage.assets.page("/recovery")
}

async fn asset(State(stage): State<Arc<Stage>>, Path(name): Path<String>) -> Response {
    stage.assets.file(&name)
}

async fn guard(State(stage): State<Arc<Stage>>, request: Request, next: Next) -> Response {
    let headers = request.headers();
    let authority = stage
        .origin
        .strip_prefix("https://")
        .expect("validated HTTPS origin");
    let mut response = if !exact_header(headers, header::HOST, authority) {
        Error::new(
            StatusCode::FORBIDDEN,
            "invalid_host",
            "使用目标站 HTTPS 地址",
        )
        .into_response()
    } else if matches!(
        request.uri().path(),
        "/recovery/status" | "/recovery/backups" | "/recovery/check" | "/recovery/import"
    ) && headers.contains_key(header::ORIGIN)
        && !exact_header(headers, header::ORIGIN, &stage.origin)
    {
        Error::new(
            StatusCode::FORBIDDEN,
            "invalid_origin",
            "恢复操作需要来自同一站点",
        )
        .into_response()
    } else if let Err(error) = stage.leader.confirm().await {
        error.into_response()
    } else if stage.shutdown.is_cancelled() {
        super::blocked().into_response()
    } else {
        next.run(request).await
    };
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

async fn status(State(stage): State<Arc<Stage>>, headers: HeaderMap) -> ApiResult<Json<Status>> {
    authorize(&headers, &stage.token)?;
    let job = stage
        .leader
        .get::<Option<Job>>(JOB_KEY)
        .await?
        .and_then(|(job, _)| job);
    // Status is advisory. Starting a restore owns the slot and checks the
    // persisted revision itself, so polling must not hold that slot over I/O.
    let running = stage.job.try_lock().is_err()
        || job
            .as_ref()
            .is_some_and(|job| job.phase == Phase::Control && job.failure.is_none())
            && stage.leader.get::<Gate>("recovery").await?.is_some();
    Ok(Json(Status {
        mode: "restore",
        job: job.map(|job| JobView {
            bundle: job.bundle,
            phase: job.phase,
            bound: job.bound.is_some(),
            running: running && job.failure.is_none(),
            failure: job
                .failure
                .or_else(|| (!running).then(|| "恢复作业已中断，请重试".into())),
        }),
    }))
}

async fn backups(
    State(stage): State<Arc<Stage>>,
    headers: HeaderMap,
) -> ApiResult<Json<Vec<Candidate>>> {
    authorize(&headers, &stage.token)?;
    let root = stage.inbox.clone();
    let list = tokio::time::timeout(
        Duration::from_secs(3),
        tokio::task::spawn_blocking(move || -> Result<_> {
            let mut list = Vec::new();
            for entry in root.read_dir()? {
                let entry = entry?;
                let Ok(id) = entry.file_name().into_string() else {
                    continue;
                };
                if !models::valid_id(&id) || !entry.file_type()?.is_dir() {
                    continue;
                }
                let Ok(meta) = entry.path().join("manifest.json").symlink_metadata() else {
                    continue;
                };
                if !meta.is_file() {
                    continue;
                }
                let modified_ms = meta
                    .modified()
                    .ok()
                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                    .and_then(|t| t.as_millis().try_into().ok());
                list.push(Candidate { id, modified_ms });
            }
            list.sort_unstable_by(|a, b| {
                b.modified_ms
                    .cmp(&a.modified_ms)
                    .then_with(|| a.id.cmp(&b.id))
            });
            Ok(list)
        }),
    )
    .await
    .map_err(|_| super::timed_out())?
    .ok()
    .and_then(Result::ok)
    .ok_or_else(|| super::failed("无法读取异地备份列表"))?;
    Ok(Json(list))
}

async fn check_candidate(
    State(stage): State<Arc<Stage>>,
    headers: HeaderMap,
    Json(form): Json<Start>,
) -> ApiResult<Json<CandidateCheck>> {
    authorize(&headers, &stage.token)?;
    let slot = stage
        .review
        .clone()
        .try_lock_owned()
        .map_err(|_| Error::busy())?;
    let timeout = tokio::time::timeout(
        Duration::from_secs(30),
        tokio::task::spawn_blocking(move || {
            let _slot = slot;
            let dir = bundle_dir(&stage.inbox, &form.bundle)
                .map_err(|_| super::failed("异地备份目录不可读"))?;
            let (backup, manifest, manifest_sha256) = read_bundle(&dir, &stage.dsn_env)
                .map_err(|error| super::failed(error.to_string()))?;
            Ok(Json(CandidateCheck {
                bundle: form.bundle,
                checkpoint_ms: backup.created_at_ms,
                manifest_sha256,
                postgres_relations: manifest.postgres.relations.len(),
                parquet_files: manifest.parquet.len(),
            }))
        }),
    )
    .await;
    timeout
        .map_err(|_| super::timed_out())?
        .map_err(|_| Error::storage())?
}

fn read_bundle(dir: &FsPath, dsn_env: &str) -> Result<(backup::Backup, Manifest, String)> {
    let backup = backup::read(&dir.join("control.json")).context("控制备份无效或不可读")?;
    let (manifest, digest) = versioned_manifest(dir).context("恢复清单无效或不可读")?;
    ensure!(
        manifest.control_sha256 == backup.sha256,
        "恢复清单与控制备份摘要不匹配"
    );
    ensure!(
        manifest.postgres.dsn_env == dsn_env,
        "清单中的 PostgreSQL 连接与目标站配置不一致"
    );
    Ok((backup, manifest, digest))
}

async fn start(
    State(stage): State<Arc<Stage>>,
    headers: HeaderMap,
    Json(form): Json<Start>,
) -> ApiResult<StatusCode> {
    authorize(&headers, &stage.token)?;
    if !models::valid_id(&form.bundle) {
        return Err(super::failed("异地备份标识无效"));
    }
    if form
        .manifest_sha256
        .as_deref()
        .is_some_and(|digest| !valid_digest(digest))
    {
        return Err(super::failed("恢复清单摘要无效"));
    }
    let slot = stage
        .job
        .clone()
        .try_lock_owned()
        .map_err(|_| Error::busy())?;
    let leader = stage.leader.confirm().await?;
    let (previous, revision) = stage
        .leader
        .get::<Option<Job>>(JOB_KEY)
        .await?
        .ok_or_else(super::blocked)?;
    let (bound, postgres_started) =
        if let Some(previous) = previous.filter(|job| job.bound.is_some()) {
            if previous.bundle != form.bundle {
                return Err(Error::new(
                    StatusCode::CONFLICT,
                    "restore_bound",
                    "目标已绑定到此前的备份，请重试同一备份",
                ));
            }
            (previous.bound, previous.postgres_started)
        } else {
            (None, false)
        };
    let reviewed = form.manifest_sha256;
    let mut job = Job {
        bundle: form.bundle,
        phase: Phase::Validate,
        bound,
        postgres_started,
        failure: None,
    };
    let mut revision = stage.write(revision, &job).await?;
    tokio::spawn(async move {
        let _slot = slot;
        let result = tokio::select! {
            biased;
            _ = leader.cancelled() => Err(anyhow::anyhow!("Coordinator 主权暂不可用，请重试")),
            _ = stage.shutdown.cancelled() => return,
            result = tokio::time::timeout(RESTORE_TIMEOUT, stage.run(&mut job, &mut revision, reviewed.as_deref())) =>
                result.unwrap_or_else(|_| Err(anyhow::anyhow!("恢复作业超时"))),
        };
        if let Err(error) = result {
            job.failure = Some(error.to_string());
            let _ = stage.write(revision, &job).await;
        }
        super::super::telemetry::event(
            if job.failure.is_some() {
                opentelemetry::logs::Severity::Error
            } else {
                opentelemetry::logs::Severity::Info
            },
            "recovery_import",
            serde_json::json!({"bundle":job.bundle, "phase":job.phase, "failure":job.failure}),
        );
    });
    Ok(StatusCode::ACCEPTED)
}

impl Stage {
    async fn write(&self, revision: i64, job: &Job) -> ApiResult<i64> {
        self.leader
            .commit(JOB_KEY, revision, &Some(job), None::<(uuid::Uuid, &String)>)
            .await?
            .ok_or_else(super::blocked)
    }

    async fn advance(&self, job: &mut Job, revision: &mut i64, phase: Phase) -> Result<()> {
        job.phase = phase;
        *revision = self
            .write(*revision, job)
            .await
            .map_err(|e| anyhow::anyhow!(e.message))?;
        Ok(())
    }

    async fn run(&self, job: &mut Job, revision: &mut i64, reviewed: Option<&str>) -> Result<()> {
        let bundle = bundle_dir(&self.inbox, &job.bundle)?;
        let dir = bundle.clone();
        let dsn_env = self.dsn_env.clone();
        let (backup, manifest, manifest_sha256) =
            tokio::task::spawn_blocking(move || read_bundle(&dir, &dsn_env))
                .await
                .context("无法读取备份")??;
        ensure!(
            reviewed.is_none_or(|digest| digest == manifest_sha256),
            "恢复清单在检查后已变更，请重新检查恢复点"
        );
        ensure!(
            job.bound
                .as_ref()
                .is_none_or(|bound| bound == &manifest_sha256),
            "此备份清单已变更，请恢复原清单"
        );
        let postgres = data::Destination::load(&self.dsn_env)?;
        let mut archive = data::archive(&bundle, &manifest.postgres.dump_sha256).await?;
        let empty = data::Destination::empty(&self.dsn_env).await?;
        if !empty {
            ensure!(
                job.postgres_started,
                "目标 PostgreSQL 数据库已有对象，请配置空的恢复数据库"
            );
            data::Destination::verify(&manifest.postgres).await?;
        } else {
            data::Destination::check_archive(&mut archive).await?;
        }
        job.bound = Some(manifest_sha256.clone());
        self.advance(job, revision, Phase::Parquet).await?;
        data::parquet(&bundle, &self.target, &manifest).await?;

        // Pin this step before starting the transaction. On interrupt the next owner
        // can distinguish a committed restore from a target that was already populated.
        job.postgres_started = true;
        self.advance(job, revision, Phase::Postgres).await?;
        if empty {
            postgres.restore(archive).await?;
            data::Destination::verify(&manifest.postgres).await?;
        }

        self.advance(job, revision, Phase::Control).await?;
        let gate = Gate {
            manifest_sha256,
            token_sha256: self.token.clone(),
            approved: false,
            bundle: Some(job.bundle.clone()),
        };
        backup.restore(&self.leader, &gate).await?;
        Ok(())
    }
}
