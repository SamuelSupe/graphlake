use axum::{
    Json,
    extract::{Extension, Path, State},
    http::{Method, header},
    response::{IntoResponse, Response},
};

use super::*;
use crate::admin::access::AuditModel;

impl Coordinator {
    pub(super) async fn result_freshness(
        &self,
        app: &App,
        job: &Job,
    ) -> ApiResult<Vec<freshness::Observation>> {
        if !matches!(job.state, JobState::Done { completed_ms, .. }
            if now_ms().saturating_sub(completed_ms) < job.retention_ms())
            || job.export && job.freshness.is_none()
        {
            return Ok(Vec::new());
        }
        let files = app.models.lock().unwrap().read_files();
        let admission = app.admission.clone();
        let model = job.model.clone();
        let model = tokio::task::spawn_blocking(move || {
            let revision = files.published_revision(&model)?;
            let (_, engine) = files.cached_engine(&model, &revision, &admission)?;
            Ok((revision, engine.catalog))
        })
        .await
        .map_err(|_| Error::storage())?;
        let (revision, catalog) = match model {
            Ok(model) => model,
            Err(error) if job.freshness.is_some() => return Err(error),
            Err(_) => return Ok(Vec::new()),
        };
        if revision != job.revision {
            if job.freshness.is_some() {
                return Err(freshness::conflict("已发布模型版本变化"));
            }
            return Ok(Vec::new());
        }
        let inputs = freshness::observe(app, &catalog).await;
        if let Some(proof) = &job.freshness {
            freshness::check(&proof.policy, &inputs, job.csr_generation.is_some())?;
            if freshness::generations(&inputs) != proof.generations {
                return Err(freshness::conflict("查询期间本地副本代次已更新"));
            }
            if let Some(generation) = job.csr_generation {
                csr::verify_freshness(
                    self,
                    &job.model,
                    &job.revision,
                    generation,
                    &proof.policy,
                    &inputs,
                )
                .await?;
            }
        }
        Ok(inputs)
    }

    pub(super) async fn response_with_freshness(
        &self,
        app: &App,
        id: Uuid,
        job: &Job,
    ) -> ApiResult<Response> {
        let mut inputs = self.result_freshness(app, job).await?;
        if job.export || inputs.is_empty() {
            return self.response(id, job).await;
        }
        if matches!(job.state, JobState::Done { completed_ms, .. } if now_ms().saturating_sub(completed_ms) < job.retention_ms())
        {
            let key = self
                .shared
                .objects
                .artifact(id, job.attempt, "preview.json");
            let mut page: Value = self
                .shared
                .objects
                .get_json(&key, JOB_LIMIT)
                .await
                .map_err(storage)?;
            for item in &mut inputs {
                item.used(|path| {
                    page["sources"]
                        .as_array()?
                        .iter()
                        .find(|version| version["source"] == path)?["version"]
                        .as_str()
                        .map(str::to_owned)
                });
            }
            page["freshness"] = serde_json::to_value(inputs).map_err(|_| Error::storage())?;
            return Ok(Json(page).into_response());
        }
        self.response(id, job).await
    }
    pub(super) async fn response(&self, id: Uuid, job: &Job) -> ApiResult<Response> {
        let response = match &job.state {
            JobState::Running if now_ms() >= job.deadline_ms => return Err(timeout()),
            JobState::Running => (StatusCode::ACCEPTED, Json(json!({"status":"running","model_revision":job.revision,"execution_worker":job.worker}))).into_response(),
            JobState::Done { completed_ms, .. } if now_ms().saturating_sub(*completed_ms) >= job.retention_ms() => return Err(expired()),
            JobState::Done { .. } if job.export => Json(json!({"ticket":id})).into_response(),
            JobState::Done { .. } => {
                let key = self.shared.objects.artifact(id, job.attempt, "preview.json");
                let data: Value = self.shared.objects.get_json(&key, JOB_LIMIT).await.map_err(storage)?;
                Json(data).into_response()
            }
            JobState::Failed { code, message, status } => (
                StatusCode::from_u16(*status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                Extension(crate::admin::ErrorCode::class(code)),
                Json(json!({"code":code,"message":message})),
            ).into_response(),
            JobState::Cancelled => return Err(cancelled()),
            JobState::Downloaded => return Err(expired()),
        };
        Ok(response)
    }

    pub async fn graph_page(
        &self,
        app: &App,
        principal: &Principal,
        model: &str,
        id: Uuid,
        index: usize,
    ) -> ApiResult<Response> {
        let saved = self
            .owned(app, principal, id)
            .await?
            .filter(|saved| saved.job.model == model)
            .ok_or_else(expired)?;
        if saved.job.freshness.is_some() {
            self.result_freshness(app, &saved.job).await?;
        }
        let JobState::Done {
            graph_pages,
            completed_ms,
            ..
        } = saved.job.state
        else {
            return Err(expired());
        };
        if now_ms().saturating_sub(completed_ms) >= saved.job.retention_ms() {
            return Err(expired());
        }
        if index >= graph_pages {
            return Err(Error::not_found());
        }
        let key =
            self.shared
                .objects
                .artifact(id, saved.job.attempt, &format!("graph-{index}.json"));
        let data: Value = self
            .shared
            .objects
            .get_json(&key, GRAPH_PAGE_LIMIT)
            .await
            .map_err(storage)?;
        Ok(Json(data).into_response())
    }

    pub async fn arrow(
        &self,
        app: &App,
        principal: &Principal,
        model: Option<&str>,
        id: Uuid,
        method: Method,
    ) -> ApiResult<Response> {
        let saved = self
            .owned(app, principal, id)
            .await?
            .filter(|saved| {
                model.map_or(saved.job.export, |model| {
                    !saved.job.export && saved.job.model == model
                })
            })
            .ok_or_else(expired)?;
        if saved.job.freshness.is_some() {
            self.result_freshness(app, &saved.job).await?;
        }
        let mut job = saved.job;
        crate::admin::access::check_export(app, principal, &job.model)?;
        if !matches!(job.state, JobState::Done { arrow: true, completed_ms, .. } if now_ms().saturating_sub(completed_ms) < job.retention_ms())
        {
            return Err(expired());
        }
        let permit = if method == Method::HEAD {
            None
        } else {
            Some(query::download::download_slot(app, principal).await?)
        };
        let key = self
            .shared
            .objects
            .artifact(id, job.attempt, "result.arrow");
        let (length, body) = if method == Method::HEAD {
            let meta = self.shared.objects.head_arrow(&key).await?;
            (meta.size, axum::body::Body::empty())
        } else {
            let result = self.shared.objects.get_arrow(&key).await?;
            if job.export {
                job.state = JobState::Downloaded;
                if !self
                    .shared
                    .metadata
                    .replace(id, saved.revision, &job)
                    .await
                    .map_err(storage)?
                {
                    return Err(expired());
                }
            }
            (
                result.meta.size,
                query::download::download_body(result.into_stream(), permit),
            )
        };
        let mut response = (
            [
                (
                    header::CONTENT_TYPE,
                    "application/vnd.apache.arrow.stream".to_owned(),
                ),
                (
                    header::CONTENT_DISPOSITION,
                    format!("attachment; filename=\"{}-result.arrow\"", job.model),
                ),
                (header::CONTENT_LENGTH, length.to_string()),
                (
                    header::HeaderName::from_static("x-graphlake-model-revision"),
                    job.revision,
                ),
            ],
            body,
        )
            .into_response();
        response.extensions_mut().insert(AuditModel(job.model));
        Ok(response)
    }
}

pub(in crate::admin) async fn resume(
    State(app): State<Arc<App>>,
    Path(id): Path<Uuid>,
    Extension(principal): Extension<Principal>,
    method: Method,
) -> ApiResult<Response> {
    let cluster = app.cluster.as_ref().ok_or_else(Error::not_found)?;
    let saved = cluster
        .owned(&app, &principal, id)
        .await?
        .ok_or_else(Error::not_found)?;
    let saved = if method == Method::GET
        && matches!(saved.job.state, JobState::Running)
        && saved.job.worker.is_none()
    {
        cluster.recover_dispatch(id, &saved.job).await?;
        cluster
            .owned(&app, &principal, id)
            .await?
            .ok_or_else(Error::not_found)?
    } else {
        saved
    };
    if method == Method::GET && cluster.mark_worker_lost(id, &saved).await? {
        return Err(worker_lost());
    }
    if saved.job.export {
        crate::admin::access::check_export(&app, &principal, &saved.job.model)?;
    }
    let mut response = cluster
        .response_with_freshness(&app, id, &saved.job)
        .await?;
    response
        .extensions_mut()
        .insert(AuditModel(saved.job.model));
    Ok(response)
}
