use std::{
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use axum::{
    Extension, Json,
    extract::{Path as RoutePath, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::{
    ApiResult, App, Error,
    access::{self, Principal},
    cluster::control::{self, CatalogState, Leadership, Receipt},
    models,
    users::{self, Role},
};
use crate::CancellationToken;

pub(super) struct Ha {
    pub leader: Arc<Leadership>,
    pub catalog: tokio::sync::Mutex<(String, i64)>,
    probe_generation: std::sync::RwLock<String>,
    pub recovery_configured: bool,
    pub recovery_pending: AtomicBool,
    pub recovery_check: tokio::sync::Mutex<()>,
}

pub(super) async fn source_available(path: PathBuf) -> bool {
    tokio::time::timeout(
        Duration::from_millis(850),
        tokio::task::spawn_blocking(move || {
            #[cfg(unix)]
            {
                use std::{ffi::CString, mem::MaybeUninit, os::unix::ffi::OsStrExt};
                let Ok(path) = CString::new(path.as_os_str().as_bytes()) else {
                    return false;
                };
                let mut stat = MaybeUninit::<libc::statvfs>::uninit();
                // statvfs checks the mounted filesystem itself, independently of a cached directory entry.
                unsafe { libc::statvfs(path.as_ptr(), stat.as_mut_ptr()) == 0 }
            }
            #[cfg(not(unix))]
            path.is_dir()
        }),
    )
    .await
    .is_ok_and(|task| matches!(task, Ok(true)))
}

pub(super) fn watch_source(path: PathBuf, stop: CancellationToken) {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = stop.cancelled() => break,
                _ = tokio::time::sleep(Duration::from_millis(450)) => {}
            }
            if !source_available(path.clone()).await {
                super::telemetry::event(
                    opentelemetry::logs::Severity::Error,
                    "catalog_storage_lost",
                    serde_json::json!({}),
                );
                stop.cancel();
                break;
            }
        }
    });
}

impl Ha {
    pub fn recovery_pending(&self) -> bool {
        self.recovery_pending.load(Ordering::Acquire)
    }

    pub async fn open(leader: Arc<Leadership>, original: &Path, local: &Path) -> Result<Self> {
        let recovery = leader
            .get::<super::recovery::Gate>("recovery")
            .await
            .map_err(|e| anyhow::anyhow!(e.message))?;
        if recovery.is_none() {
            ensure!(
                leader
                    .get::<serde_json::Value>(super::recovery::import::JOB_KEY)
                    .await
                    .map_err(|e| anyhow::anyhow!(e.message))?
                    .is_none(),
                "this namespace is reserved for UI recovery; configure the restore inbox on this Coordinator"
            );
        }
        let (generation, version) = match leader
            .get::<String>("catalog")
            .await
            .map_err(|e| anyhow::anyhow!(e.message))?
        {
            Some(pointer) => pointer,
            None => {
                let original_path = original.to_owned();
                let original = original_path.clone();
                let state = tokio::time::timeout(
                    Duration::from_secs(4),
                    tokio::task::spawn_blocking(move || capture(&original)),
                )
                .await
                .context("reading the catalog during HA migration")??
                .map_err(|e| anyhow::anyhow!(e.message))?;
                restore(local, &state).map_err(|e| anyhow::anyhow!(e.message))?;
                models::Models::with_sources(local.to_owned(), original_path)
                    .adopt_existing()
                    .map_err(|e| anyhow::anyhow!(e.message))?;
                let state = capture(local).map_err(|e| anyhow::anyhow!(e.message))?;
                let generation = leader
                    .store_catalog(&state, None)
                    .await
                    .map_err(|e| anyhow::anyhow!(e.message))?;
                ensure!(
                    leader
                        .commit("catalog", 0, &generation, None::<(Uuid, &Receipt)>)
                        .await
                        .map_err(|e| anyhow::anyhow!(e.message))?
                        .is_some(),
                    "HA catalog changed during migration"
                );
                leader
                    .get("catalog")
                    .await
                    .map_err(|e| anyhow::anyhow!(e.message))?
                    .context("missing HA catalog pointer")?
            }
        };
        let state = leader
            .load_catalog(&generation)
            .await
            .map_err(|e| anyhow::anyhow!(e.message))?;
        restore(local, &state).map_err(|e| anyhow::anyhow!(e.message))?;
        Ok(Self {
            leader,
            probe_generation: std::sync::RwLock::new(generation.clone()),
            recovery_configured: recovery.is_some(),
            recovery_pending: AtomicBool::new(recovery.is_some_and(|(gate, _)| !gate.approved)),
            recovery_check: tokio::sync::Mutex::new(()),
            catalog: tokio::sync::Mutex::new((generation, version)),
        })
    }

    pub async fn verify_backend(&self, timeout: Duration) -> ApiResult<()> {
        let generation = self.probe_generation.read().unwrap().clone();
        tokio::time::timeout(timeout, self.leader.check_catalog(&generation))
            .await
            .map_err(|_| control::unavailable())?
    }
}

fn capture(dir: &Path) -> ApiResult<CatalogState> {
    let mut state = CatalogState::default();
    for entry in fs::read_dir(dir).map_err(|_| Error::storage())? {
        let entry = entry.map_err(|_| Error::storage())?;
        let name = entry.file_name();
        let Some(name) = name.to_str().filter(|name| catalog_file(name)) else {
            continue;
        };
        if let Some(value) = models::read_file(&entry.path(), 8 * super::CATALOG_LIMIT)? {
            state.files.insert(name.to_owned(), value);
        }
    }
    Ok(state)
}

pub(super) fn restore(dir: &Path, state: &CatalogState) -> ApiResult<()> {
    for (name, contents) in &state.files {
        if !catalog_file(name) {
            return Err(Error::storage());
        }
        fs::write(dir.join(name), contents).map_err(|_| Error::storage())?;
    }
    Ok(())
}

fn catalog_file(name: &str) -> bool {
    if let Some(suffix) = name.strip_prefix(".graphlake-") {
        if let Some(id) = suffix.strip_suffix(".json") {
            return models::valid_id(id);
        }
        return suffix
            .strip_suffix(".yaml")
            .and_then(|s| s.split_once('.'))
            .is_some_and(|(id, hash)| {
                models::valid_id(id) && models::versions::valid_revision(hash)
            });
    }
    name.strip_suffix(".yaml").is_some_and(models::valid_id)
}

#[derive(Serialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub(super) enum Mutation {
    Create {
        yaml: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        template_model: Option<String>,
    },
    Save {
        yaml: String,
        expected: String,
    },
    Publish(models::versions::Activation),
    Rollback(models::versions::Activation),
    Unpublish(models::versions::Deactivation),
    Restore {
        revision: String,
        yaml: String,
    },
}

pub(super) async fn mutate(
    app: &Arc<App>,
    id: String,
    principal: &Principal,
    headers: &HeaderMap,
    mutation: Mutation,
) -> ApiResult<Response> {
    if !models::valid_id(&id) {
        return Err(Error::invalid("invalid_id", "模型标识无效"));
    }
    let request = match headers.get("idempotency-key") {
        Some(value) => value
            .to_str()
            .ok()
            .and_then(|value| Uuid::parse_str(value).ok())
            .filter(|id| !id.is_nil())
            .ok_or_else(|| {
                Error::invalid("invalid_idempotency_key", "Idempotency-Key 需要为 UUID")
            })?,
        None => Uuid::new_v4(),
    };
    let fingerprint =
        Sha256::digest(serde_json::to_vec(&(&id, &mutation)).map_err(|_| Error::storage())?);
    let fingerprint = fingerprint
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let application = app.clone();
    let principal = principal.clone();
    let deadline = tokio::time::Instant::now()
        + Duration::from_millis(app.options.timeout.as_millis().clamp(5000, 15_000) as u64);
    let task = tokio::spawn(async move {
        let _permit = application
            .metadata_work
            .try_acquire()
            .map_err(|_| Error::busy())?;
        mutate_inner(
            &application,
            &id,
            principal,
            mutation,
            request,
            fingerprint,
            deadline,
        )
        .await
    });
    let result = tokio::time::timeout_at(deadline, task).await;
    let response = match result {
        Ok(Ok(Ok(response))) => response,
        Ok(Ok(Err(error))) => error.into_response(),
        Ok(Err(_)) => Error::storage().into_response(),
        Err(_) => control::timeout().into_response(),
    };
    Ok(with_receipt(response, request))
}

fn with_receipt(mut response: Response, request: Uuid) -> Response {
    response
        .headers_mut()
        .insert("idempotency-key", request.to_string().parse().unwrap());
    response.headers_mut().insert(
        axum::http::header::LOCATION,
        format!("/api/v1/mutations/{request}").parse().unwrap(),
    );
    response
}

async fn mutate_inner(
    app: &Arc<App>,
    id: &str,
    principal: Principal,
    mutation: Mutation,
    request: Uuid,
    fingerprint: String,
    deadline: tokio::time::Instant,
) -> ApiResult<Response> {
    let ha = app.ha.as_ref().expect("HA mode");
    let mut pointer = tokio::time::timeout_at(deadline, ha.catalog.lock())
        .await
        .map_err(|_| control::timeout())?;
    ha.leader.confirm().await?;
    let role = match mutation {
        Mutation::Create { .. } | Mutation::Save { .. } => Role::Modeler,
        _ => Role::Publisher,
    };
    users::check(app, &principal, id, role)?;
    if let Some((receipt, _)) = ha
        .leader
        .get::<Receipt>(&format!("receipts/{request}"))
        .await?
    {
        if !access::owns(&principal, &receipt.owner) {
            return Err(Error::not_found());
        }
        if receipt.fingerprint != fingerprint {
            return Err(Error::new(
                StatusCode::CONFLICT,
                "idempotency_conflict",
                "请求标识已用于不同的操作",
            ));
        }
        let state = ha.leader.load_catalog(&receipt.generation).await?;
        let temp = tempfile::tempdir().map_err(|_| Error::storage())?;
        restore(temp.path(), &state)?;
        let model = models::Models::new(temp.path().to_owned()).ha_model(id, receipt.validated)?;
        return Ok((
            StatusCode::from_u16(receipt.status).unwrap(),
            models::model_response(model),
        )
            .into_response());
    }
    // All synchronous metadata work is against a private, local candidate.
    let (local, sources) = {
        let models = app.models.lock().unwrap();
        (
            models.directory().to_owned(),
            models.source_directory().to_owned(),
        )
    };
    let base = capture(&local)?;
    let candidate = Arc::new(tempfile::tempdir().map_err(|_| Error::storage())?);
    restore(candidate.path(), &base)?;
    let mut models = models::Models::in_snapshot(Arc::clone(&candidate), sources.clone());
    if let Mutation::Create {
        yaml,
        template_model,
    } = &mutation
    {
        models.check_create_input(app, id, yaml, template_model.as_deref(), &principal)?;
    }
    let template = match &mutation {
        Mutation::Create { template_model, .. } => template_model.clone(),
        _ => None,
    };
    if let Mutation::Save { yaml, .. } = &mutation {
        models.check_modeler_input(id, yaml, &principal)?;
    }
    let mut options = app.options.clone();
    options.timeout = options.timeout.min(Duration::from_secs(8));
    let prepared = async {
        if matches!(mutation, Mutation::Publish(_) | Mutation::Rollback(_))
            && !source_available(sources.clone()).await
        {
            return Err(control::timeout());
        }
        models.apply_ha(id, mutation, &options).await
    };
    let (status, model) = tokio::time::timeout_at(deadline, prepared)
        .await
        .map_err(|_| control::timeout())??;
    let state = capture(candidate.path())?;
    let generation = if state.files == base.files {
        pointer.0.clone()
    } else {
        tokio::time::timeout_at(deadline, ha.leader.store_catalog(&state, Some(&pointer.0)))
            .await
            .map_err(|_| control::timeout())??
    };
    let mut receipt = Receipt {
        fingerprint,
        owner: principal.clone(),
        model: id.into(),
        revision: model.revision.clone(),
        active_revision: model.active_revision.clone(),
        status: status.as_u16(),
        generation: generation.clone(),
        validated: model.validated(),
        committed_at_ms: 0,
    };
    receipt.stamp();
    let _policy = tokio::time::timeout_at(deadline, app.users.writes.lock())
        .await
        .map_err(|_| control::timeout())?;
    users::check(app, &principal, id, role)?;
    if let Some(template) = template {
        users::check(app, &principal, &template, Role::Modeler)?;
    }
    if tokio::time::Instant::now() >= deadline {
        return Err(control::timeout());
    }
    let committed = ha
        .leader
        .commit("catalog", pointer.1, &generation, Some((request, &receipt)))
        .await;
    let revision = match committed {
        Ok(Some(revision)) => revision,
        failed => {
            let error = failed.err().unwrap_or_else(control::lost);
            // A preflight pause did not send a write. A lost write response can
            // hide a commit, so retire instead of serving the previous snapshot.
            if error.code != control::UNCERTAIN {
                ha.leader.retire();
            }
            return Err(error);
        }
    };
    // Never await between successful CAS and installing the local candidate.
    let old = std::mem::replace(&mut *app.models.lock().unwrap(), models);
    tokio::task::spawn_blocking(move || drop(old));
    *ha.probe_generation.write().unwrap() = generation.clone();
    *pointer = (generation, revision);
    Ok((status, models::model_response(model)).into_response())
}

#[derive(Serialize)]
struct Outcome {
    status: &'static str,
    model: Option<String>,
    revision: Option<String>,
    active_revision: Option<String>,
    committed_at_ms: Option<u64>,
}

pub(super) async fn receipt(
    State(app): State<Arc<App>>,
    RoutePath(id): RoutePath<Uuid>,
    Extension(principal): Extension<Principal>,
) -> ApiResult<Response> {
    let ha = app.ha.as_ref().ok_or_else(Error::not_found)?;
    let receipt = ha.leader.get::<Receipt>(&format!("receipts/{id}")).await?;
    Ok(match receipt {
        Some((ref r, _))
            if !access::owns(&principal, &r.owner)
                || (principal != Principal::Admin
                    && app.users.roles(&principal, &r.model).is_empty()) =>
        {
            return Err(Error::not_found());
        }
        Some((r, _)) => Json(Outcome {
            status: "committed",
            model: Some(r.model),
            revision: Some(r.revision),
            active_revision: r.active_revision,
            committed_at_ms: Some(r.committed_at_ms),
        })
        .into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(Outcome {
                status: "uncommitted",
                model: None,
                revision: None,
                active_revision: None,
                committed_at_ms: None,
            }),
        )
            .into_response(),
    })
}

pub(super) async fn active(State(app): State<Arc<App>>) -> StatusCode {
    if app.shutdown.is_cancelled() {
        return StatusCode::SERVICE_UNAVAILABLE;
    }
    let Some(ha) = &app.ha else {
        return StatusCode::OK;
    };
    if ha.leader.confirm().await.is_err() {
        return StatusCode::SERVICE_UNAVAILABLE;
    }
    if ha.recovery_pending() || !app.ha_ready.load(std::sync::atomic::Ordering::Acquire) {
        return StatusCode::SERVICE_UNAVAILABLE;
    }
    StatusCode::OK
}

pub(super) async fn watch_backend(app: std::sync::Weak<App>) {
    let mut previous = None;
    loop {
        let Some(app) = app.upgrade() else {
            return;
        };
        if app.shutdown.is_cancelled() {
            return;
        }
        let probe = async {
            let cluster = app.cluster.as_ref().unwrap();
            let ha = app.ha.as_ref().unwrap();
            let (worker, storage) = tokio::join!(
                cluster.readiness_failure(),
                ha.verify_backend(Duration::from_millis(800))
            );
            if storage.is_err() {
                Some("object_store_unavailable")
            } else {
                worker
            }
        };
        let reason = tokio::time::timeout(Duration::from_millis(950), probe)
            .await
            .unwrap_or(Some("probe_timeout"));
        let ready = reason.is_none();
        if let Some(ha) = &app.ha
            && ha.recovery_pending()
            && ha
                .leader
                .get::<super::recovery::Gate>("recovery")
                .await
                .is_ok_and(|gate| gate.is_some_and(|(gate, _)| gate.approved))
        {
            ha.recovery_pending.store(false, Ordering::Release);
        }
        app.ha_ready
            .store(ready, std::sync::atomic::Ordering::Release);
        if previous != Some(reason) {
            previous = Some(reason);
            super::telemetry::event(
                if ready {
                    opentelemetry::logs::Severity::Info
                } else {
                    opentelemetry::logs::Severity::Warn
                },
                "coordinator_readiness_changed",
                serde_json::json!({"ready": ready, "reason": reason}),
            );
        }
        drop(app);
        tokio::time::sleep(Duration::from_millis(450)).await;
    }
}

pub(super) async fn credentials(leader: &Leadership, seed: &Path, local: &Path) -> Result<()> {
    let dir = local.join(".graphlake-auth");
    fs::create_dir(&dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o700))?;
    }
    let seed = seed.join(".graphlake-auth");
    let password_path = seed.join("password.phc");
    let hash = tokio::time::timeout(
        Duration::from_secs(2),
        tokio::task::spawn_blocking(move || {
            super::auth::private_permissions(&fs::symlink_metadata(&password_path)?, true)?;
            models::read_file(&password_path, 512)
                .map_err(|e| anyhow::anyhow!(e.message))?
                .context("administrator credentials missing")
        }),
    )
    .await
    .context("reading administrator credentials timed out")???;
    models::atomic_write(&dir.join("password.phc"), hash.as_bytes(), true)
        .map_err(|e| anyhow::anyhow!(e.message))?;
    let policy = match leader
        .get::<String>("policy")
        .await
        .map_err(|e| anyhow::anyhow!(e.message))?
    {
        Some((policy, _)) => policy,
        None => {
            let path = seed.join("service-accounts.json");
            if let Ok(meta) = fs::symlink_metadata(&path) {
                super::auth::private_permissions(&meta, true)?;
            }
            let policy = models::read_file(&path, 256 * 1024)
                .map_err(|e| anyhow::anyhow!(e.message))?
                .unwrap_or_else(|| "{}".into());
            super::access::Policy::validate_config(&policy)
                .map_err(|e| anyhow::anyhow!(e.message))?;
            ensure!(
                leader
                    .commit("policy", 0, &policy, None::<(Uuid, &())>)
                    .await
                    .map_err(|e| anyhow::anyhow!(e.message))?
                    .is_some(),
                "HA policy changed during migration"
            );
            policy
        }
    };
    models::atomic_write(&dir.join("service-accounts.json"), policy.as_bytes(), true)
        .map_err(|e| anyhow::anyhow!(e.message))?;
    Ok(())
}

pub(super) async fn change_policy(
    State(app): State<Arc<App>>,
    body: String,
) -> ApiResult<StatusCode> {
    let ha = app.ha.as_ref().ok_or_else(Error::not_found)?;
    super::access::Policy::validate_config(&body)?;
    let leader = ha.leader.clone();
    tokio::spawn(async move {
        let _serialized = app.ha.as_ref().unwrap().catalog.lock().await;
        let (_, version) = leader
            .get::<String>("policy")
            .await?
            .ok_or_else(control::unavailable)?;
        match leader
            .commit("policy", version, &body, None::<(Uuid, &())>)
            .await
        {
            Ok(Some(_)) => app.access.apply_config(&body)?,
            result => {
                let error = result.err().unwrap_or_else(control::lost);
                if error.code != control::UNCERTAIN {
                    leader.retire();
                }
                return Err(error);
            }
        }
        Ok(StatusCode::NO_CONTENT)
    })
    .await
    .map_err(|_| Error::storage())?
}
