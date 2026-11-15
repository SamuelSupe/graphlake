use axum::{
    Json,
    body::Body,
    extract::{Extension, Path, State},
    http::{Method, header},
    response::{IntoResponse, Response},
};

use super::*;
use crate::admin::access::AuditModel;

impl Coordinator {
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
        let mut job = saved.job;
        crate::admin::access::check_export(app, principal, &job.model)?;
        if !matches!(job.state, JobState::Done { arrow: true, completed_ms, .. } if now_ms().saturating_sub(completed_ms) < job.retention_ms())
        {
            return Err(expired());
        }
        let permit = if method == Method::HEAD {
            None
        } else {
            Some(query::download::download_slot(app).await?)
        };
        let key = self
            .shared
            .objects
            .artifact(id, job.attempt, "result.arrow");
        let result = self.shared.objects.get(&key).await.map_err(storage)?;
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
        let length = result.meta.size;
        let stream = async_stream::stream! {
            let _permit = permit;
            let mut bytes = result.into_stream();
            while let Some(chunk) = futures::StreamExt::next(&mut bytes).await { yield chunk; }
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
            Body::from_stream(stream),
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
    let mut response = cluster.response(id, &saved.job).await?;
    response
        .extensions_mut()
        .insert(AuditModel(saved.job.model));
    Ok(response)
}
