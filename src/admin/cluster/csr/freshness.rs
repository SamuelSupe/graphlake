use std::sync::{Arc, Weak};
use std::time::Duration;

use axum::{
    Extension, Json,
    extract::{Path, State},
};
use serde::Serialize;
use serde_json::{Value, json};

use super::{Manifest, Refresh, build_and_publish, manifest};
use crate::admin::{
    ApiResult, App, Error,
    access::{self, Principal},
    freshness,
    local_tables::store::now,
};

#[derive(Clone, Serialize)]
pub(in crate::admin) struct RefreshStatus {
    attempted_at_ms: u64,
    status: &'static str,
    error: Option<&'static str>,
    #[serde(skip)]
    revision: String,
    #[serde(skip)]
    failures: u32,
}

pub(super) async fn refresh_model(
    app: &Arc<App>,
    model: &str,
    principal: &Principal,
    request: Refresh,
) -> ApiResult<Manifest> {
    let cluster = app
        .cluster
        .as_ref()
        .filter(|c| c.graph_partitions > 0)
        .ok_or_else(Error::not_found)?;
    let _slot = cluster.csr_work.try_acquire().map_err(|_| Error::busy())?;
    let attempted_at_ms = now();
    let revision = request.model_revision.clone();
    let failures = {
        let mut status = cluster.csr_refresh.lock().unwrap();
        let previous = status
            .get(model)
            .filter(|s| s.revision == revision)
            .map_or(0, |s| s.failures);
        status.insert(
            model.into(),
            RefreshStatus {
                attempted_at_ms,
                status: "building",
                error: None,
                revision: revision.clone(),
                failures: previous,
            },
        );
        previous
    };
    struct BuildingGuard<'a> {
        cluster: &'a crate::admin::cluster::Coordinator,
        model: String,
        revision: String,
        failures: u32,
        completed: bool,
    }
    impl Drop for BuildingGuard<'_> {
        fn drop(&mut self) {
            if !self.completed {
                self.cluster.csr_refresh.lock().unwrap().insert(
                    self.model.clone(),
                    RefreshStatus {
                        attempted_at_ms: now(),
                        status: "failed",
                        error: Some("cancelled"),
                        revision: self.revision.clone(),
                        failures: self.failures.saturating_add(1),
                    },
                );
            }
        }
    }
    let mut guard = BuildingGuard {
        cluster,
        model: model.into(),
        revision: revision.clone(),
        failures,
        completed: false,
    };
    let result = build_and_publish(app, model, principal, request).await;
    guard.completed = true;
    let (status, error, failures) = match &result {
        Ok(_) => ("succeeded", None, 0),
        Err(error) => ("failed", Some(error.code), failures.saturating_add(1)),
    };
    let status = RefreshStatus {
        attempted_at_ms: if error.is_some() {
            now()
        } else {
            attempted_at_ms
        },
        status,
        error,
        revision,
        failures,
    };
    cluster
        .csr_refresh
        .lock()
        .unwrap()
        .insert(model.into(), status);
    result
}

async fn current(app: &App, model: &str, revision: &str) -> ApiResult<Option<Arc<Manifest>>> {
    let Some(cluster) = app.cluster.as_ref().filter(|c| c.graph_partitions > 0) else {
        return Ok(None);
    };
    let Some((id, _)) = cluster
        .shared
        .metadata
        .graph(model, revision)
        .await
        .map_err(super::storage)?
    else {
        return Ok(None);
    };
    manifest(&cluster.shared.objects, model, revision, id)
        .await
        .map(Some)
        .map_err(super::storage)
}

pub(super) fn is_current(manifest: &Manifest, inputs: &[freshness::Observation]) -> bool {
    manifest.built_at_ms.is_some()
        && freshness::generations(inputs)
            .into_iter()
            .all(|(location, version)| {
                version.is_some() && version.as_ref() == manifest.local_generations.get(&location)
            })
}

pub(in crate::admin) async fn health(
    State(app): State<Arc<App>>,
    Path(model): Path<String>,
    Extension(principal): Extension<Principal>,
) -> ApiResult<Json<Value>> {
    access::check_query(&app, &principal, &model)?;
    let revision = app.published_model_revision(&model).await?;
    let (_, _, engine) = app.query_model(&model, &revision, &principal).await?;
    let inputs = freshness::observe(&app, &engine.catalog).await;
    let snapshot = current(&app, &model, &revision).await?;
    let refresh = app
        .cluster
        .as_ref()
        .and_then(|c| c.csr_refresh.lock().unwrap().get(&model).cloned());
    Ok(Json(json!({
        "model_revision": revision, "local_tables": inputs, "policy": engine.catalog.freshness.policy,
        "csr": snapshot.as_ref().map(|head| json!({"generation": head.generation, "built_at_ms": head.built_at_ms, "current": is_current(head, &inputs)})),
        "auto_refresh_csr": engine.catalog.freshness.auto_refresh_csr, "refresh": refresh,
        "algorithms": crate::ALGORITHM_CAPABILITIES,
    })))
}

pub(in crate::admin) async fn watch(app: Weak<App>) {
    loop {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let Some(app) = app.upgrade() else {
            return;
        };
        let Some(cluster) = app.cluster.as_ref().filter(|c| c.graph_partitions > 0) else {
            return;
        };
        if app.shutdown.is_cancelled() {
            return;
        }
        if let Some(ha) = &app.ha {
            if ha.recovery_pending() || ha.verify_backend(Duration::from_secs(1)).await.is_err() {
                continue;
            }
        }
        if cluster.managed.is_none() && app.local_tables.store.lock().unwrap().tables.is_empty() {
            continue;
        }
        let files = app.models.lock().unwrap().read_files();
        let models = tokio::task::spawn_blocking(move || {
            files.published_auto_refresh_csr().unwrap_or_default()
        })
        .await
        .unwrap_or_default();
        for (model, revision, catalog) in models {
            if cluster
                .csr_refresh
                .lock()
                .unwrap()
                .get(&model)
                .is_some_and(|s| {
                    s.revision == revision
                        && s.status == "failed"
                        && now().saturating_sub(s.attempted_at_ms)
                            < 30_000 * (1 << s.failures.saturating_sub(1).min(5))
                })
            {
                continue;
            }
            let inputs = freshness::observe(&app, &catalog).await;
            if inputs.is_empty()
                || freshness::generations(&inputs)
                    .values()
                    .any(Option::is_none)
            {
                continue;
            }
            match current(&app, &model, &revision).await {
                Ok(Some(head)) if is_current(&head, &inputs) => continue,
                Err(_) => continue,
                _ => {}
            }
            let graph_options = crate::GraphOptions {
                require_consistent_snapshot: catalog.freshness.policy.require_consistent_snapshot,
                ..Default::default()
            };
            let _ = refresh_model(
                &app,
                &model,
                &Principal::Admin,
                Refresh {
                    model_revision: revision,
                    graph_options,
                    timeout_ms: None,
                },
            )
            .await;
        }
    }
}
