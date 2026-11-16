use super::*;

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Action {
    Adopt,
    Publish,
    Rollback,
}

#[derive(Clone, Serialize, Deserialize)]
struct Entry {
    revision: String,
    action: Action,
    at_ms: u64,
}

#[derive(Default, Serialize, Deserialize)]
pub(super) struct VersionState {
    pub(super) active: Option<String>,
    history: Vec<Entry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    offline_at_ms: Option<u64>,
}

#[derive(Deserialize, Serialize)]
pub(crate) struct Activation {
    revision: String,
    active_revision: Option<String>,
}

#[derive(Deserialize, Serialize)]
pub(crate) struct Deactivation {
    active_revision: String,
}

#[derive(Serialize)]
pub(crate) struct History {
    active_revision: Option<String>,
    offline_at_ms: Option<u64>,
    versions: Vec<Entry>,
}

#[derive(Deserialize)]
pub(crate) struct RestoreArchive {
    yaml: String,
}

pub(super) fn archive_error(id: &str, revision: &str, reason: &str) -> Error {
    Error::new(
        StatusCode::SERVICE_UNAVAILABLE,
        "model_archive_unavailable",
        format!("模型 {id} 的版本 {revision} 无法读取（{reason}）"),
    )
}

pub(in crate::admin) fn valid_revision(revision: &str) -> bool {
    revision.len() == 43
        && revision
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
}

pub(in crate::admin) fn active_conflict() -> Error {
    Error::new(
        StatusCode::CONFLICT,
        "active_revision_changed",
        "生效版本已变化，请重新加载模型后再执行",
    )
}

impl Models {
    pub(super) fn state_path(&self, id: &str) -> ApiResult<PathBuf> {
        self.path(id)?;
        Ok(self.dir.join(format!(".graphlake-{id}.json")))
    }

    pub(super) fn version_state(&self, id: &str) -> ApiResult<Option<VersionState>> {
        let path = self.state_path(id)?;
        let manifest_error = |reason: &str| {
            Error::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "model_manifest_unavailable",
                format!("模型 {id} 的版本清单不可用（{reason}）"),
            )
        };
        if let Some(text) =
            read_file(&path, 8 * CATALOG_LIMIT).map_err(|error| manifest_error(&error.message))?
        {
            let state: VersionState =
                serde_json::from_str(&text).map_err(|_| manifest_error("清单格式无效"))?;
            if let Some(active) = &state.active
                && (!valid_revision(active)
                    || !state.history.iter().any(|entry| &entry.revision == active))
            {
                return Err(manifest_error("活动版本不在历史记录中"));
            }
            return Ok(Some(state));
        }
        // An archive without its manifest is incomplete state, even when a valid draft exists.
        if self.archived_ids()?.contains(id) {
            return Err(manifest_error("归档存在但缺少版本清单"));
        }
        Ok(None)
    }

    pub(in crate::admin) fn check_archive_history(&self) -> ApiResult<()> {
        for id in self.managed_ids()? {
            if let Some(state) = self.version_state(&id)? {
                for entry in state.history {
                    self.archived(&id, &entry.revision)?;
                }
            }
        }
        Ok(())
    }

    pub(super) fn write_state(&self, id: &str, state: &VersionState) -> ApiResult<()> {
        let content = serde_json::to_vec(state).map_err(|_| Error::storage())?;
        if content.len() > 8 * CATALOG_LIMIT {
            return Err(Error::invalid("history_limit", "模型版本历史超过大小上限"));
        }
        atomic_write(&self.state_path(id)?, &content, false)
    }

    pub(super) fn archive_path(&self, id: &str, revision: &str) -> ApiResult<PathBuf> {
        self.path(id)?;
        if !valid_revision(revision) {
            return Err(Error::invalid("invalid_revision", "模型版本无效"));
        }
        Ok(self.dir.join(format!(".graphlake-{id}.{revision}.yaml")))
    }

    pub(in crate::admin) fn archived(&self, id: &str, expected: &str) -> ApiResult<String> {
        let path = self.archive_path(id, expected)?;
        let yaml = read_file(&path, CATALOG_LIMIT)
            .map_err(|error| archive_error(id, expected, &error.message))?
            .ok_or_else(|| archive_error(id, expected, "归档文件缺失"))?;
        if revision(&yaml) != expected {
            return Err(archive_error(id, expected, "归档内容哈希不匹配"));
        }
        Ok(yaml)
    }

    fn archive(&self, id: &str, model: &Model) -> ApiResult<()> {
        let path = self.archive_path(id, &model.revision)?;
        if metadata(&path)?.is_none() {
            atomic_write(&path, model.yaml.as_bytes(), true)?;
        } else {
            self.archived(id, &model.revision)?;
        }
        Ok(())
    }

    pub(super) fn adopt(&self, id: &str, model: &Model) -> ApiResult<()> {
        if self.version_state(id)?.is_none() {
            self.archive(id, model)?;
            let mut state = VersionState::default();
            state.activate(&model.revision, Action::Adopt);
            self.write_state(id, &state)?;
        }
        Ok(())
    }

    fn activate(&self, model: Model, request: Activation, action: Action) -> ApiResult<Model> {
        if model.active_revision != request.active_revision {
            return Err(active_conflict());
        }
        self.adopt(&model.id, &model)?;
        let mut state = self.version_state(&model.id)?.ok_or_else(Error::storage)?;
        match action {
            Action::Publish => {
                if model.revision != request.revision {
                    return Err(conflict());
                }
                self.archive(&model.id, &model)?;
            }
            Action::Rollback => {
                if !state
                    .history
                    .iter()
                    .any(|entry| entry.revision == request.revision)
                {
                    return Err(Error::not_found());
                }
                self.archived(&model.id, &request.revision)?;
            }
            Action::Adopt => unreachable!(),
        }
        if state.active.as_deref() != Some(request.revision.as_str()) {
            state.activate(&request.revision, action);
            self.write_state(&model.id, &state)?;
        }
        self.get(&model.id)
    }
}

impl VersionState {
    pub(super) fn is_offline(&self) -> bool {
        self.offline_at_ms.is_some()
    }

    fn activate(&mut self, revision: &str, action: Action) {
        self.active = Some(revision.to_owned());
        self.offline_at_ms = None;
        self.history.push(Entry {
            revision: revision.to_owned(),
            action,
            at_ms: chrono::Utc::now().timestamp_millis().max(0) as u64,
        });
    }
}

pub(crate) async fn history(
    State(app): State<Arc<App>>,
    RoutePath(id): RoutePath<String>,
) -> ApiResult<Json<History>> {
    let models = app.models.lock().unwrap();
    let model = models.get(&id)?;
    let mut state = models.version_state(&id)?.unwrap_or_else(|| VersionState {
        active: model.active_revision.clone(),
        offline_at_ms: None,
        history: vec![Entry {
            revision: model.revision,
            action: Action::Adopt,
            at_ms: 0,
        }],
    });
    state.history.reverse();
    Ok(Json(History {
        active_revision: state.active,
        offline_at_ms: state.offline_at_ms,
        versions: state.history,
    }))
}

pub(crate) async fn archived_yaml(
    State(app): State<Arc<App>>,
    RoutePath((id, revision)): RoutePath<(String, String)>,
) -> ApiResult<Json<Edit>> {
    let models = app.models.lock().unwrap();
    let model = models.get(&id)?;
    let state = models.version_state(&id)?;
    let listed = state
        .as_ref()
        .is_some_and(|state| state.history.iter().any(|entry| entry.revision == revision));
    let yaml = if listed {
        models.archived(&id, &revision)?
    } else if state.is_none() && model.active_revision.as_ref() == Some(&revision) {
        model.yaml
    } else {
        return Err(Error::not_found());
    };
    Ok(Json(Edit { yaml }))
}

pub(crate) async fn restore_archive(
    State(app): State<Arc<App>>,
    RoutePath((id, expected)): RoutePath<(String, String)>,
    axum::Extension(principal): axum::Extension<Principal>,
    headers: HeaderMap,
    Json(request): Json<RestoreArchive>,
) -> ApiResult<axum::response::Response> {
    use axum::response::IntoResponse;
    if app.ha.is_some() {
        return super::super::ha::mutate(
            &app,
            id,
            &principal,
            &headers,
            super::super::ha::Mutation::Restore {
                revision: expected,
                yaml: request.yaml,
            },
        )
        .await;
    }
    let _policy = app.users.writes.lock().await;
    super::super::users::check(&app, &principal, &id, super::super::users::Role::Publisher)?;
    Ok(Json(
        app.models
            .lock()
            .unwrap()
            .restore(&id, &expected, &request.yaml)?,
    )
    .into_response())
}

impl Models {
    fn restore(&mut self, id: &str, expected: &str, yaml: &str) -> ApiResult<Model> {
        self.get(id)?;
        let path = self.archive_path(id, expected)?;
        let state = self.version_state(id)?.ok_or_else(Error::not_found)?;
        if !state.history.iter().any(|entry| entry.revision == expected) {
            return Err(Error::not_found());
        }
        parse(yaml, &self.source_path(id)?)?;
        if revision(yaml) != expected {
            return Err(Error::invalid(
                "archive_revision_mismatch",
                "备份 YAML 的内容哈希与该版本不一致",
            ));
        }
        if self.archived(id, expected).is_err() {
            atomic_write(&path, yaml.as_bytes(), false)?;
            self.engines.lock().unwrap().retain(|entry| entry.id != id);
        }
        self.get(id)
    }
}

pub(crate) async fn publish(
    State(app): State<Arc<App>>,
    RoutePath(id): RoutePath<String>,
    axum::Extension(principal): axum::Extension<Principal>,
    headers: HeaderMap,
    Json(request): Json<Activation>,
) -> ApiResult<axum::response::Response> {
    use axum::response::IntoResponse;
    if app.ha.is_some() {
        return super::super::ha::mutate(
            &app,
            id,
            &principal,
            &headers,
            super::super::ha::Mutation::Publish(request),
        )
        .await;
    }
    let _permit = app.metadata_work.try_acquire().map_err(|_| Error::busy())?;
    let (model, path) = {
        let models = app.models.lock().unwrap();
        (models.get(&id)?, models.source_path(&id)?)
    };
    if model.revision != request.revision {
        return Err(conflict());
    }
    if model.active_revision != request.active_revision {
        return Err(active_conflict());
    }
    if let Err(error) = inspect(&model.yaml, &path, &app.options, true, &app, &id).await {
        app.models
            .lock()
            .unwrap()
            .checks
            .insert(id, (model.revision, Status::Failed));
        return Err(error);
    }
    let _policy = app.users.writes.lock().await;
    super::super::users::check(&app, &principal, &id, super::super::users::Role::Publisher)?;
    let mut models = app.models.lock().unwrap();
    let model = models.get(&id)?;
    let published = models.activate(model, request, Action::Publish)?;
    models
        .checks
        .insert(id, (published.revision.clone(), Status::Passed));
    Ok(Json(Model {
        status: Status::Passed,
        ..published
    })
    .into_response())
}

pub(crate) async fn rollback(
    State(app): State<Arc<App>>,
    RoutePath(id): RoutePath<String>,
    axum::Extension(principal): axum::Extension<Principal>,
    headers: HeaderMap,
    Json(request): Json<Activation>,
) -> ApiResult<axum::response::Response> {
    use axum::response::IntoResponse;
    if app.ha.is_some() {
        return super::super::ha::mutate(
            &app,
            id,
            &principal,
            &headers,
            super::super::ha::Mutation::Rollback(request),
        )
        .await;
    }
    let _permit = app.metadata_work.try_acquire().map_err(|_| Error::busy())?;
    let (yaml, path) = {
        let models = app.models.lock().unwrap();
        let model = models.get(&id)?;
        if model.active_revision != request.active_revision {
            return Err(active_conflict());
        }
        let state = models.version_state(&id)?.ok_or_else(Error::not_found)?;
        if !state
            .history
            .iter()
            .any(|entry| entry.revision == request.revision)
        {
            return Err(Error::not_found());
        }
        (
            models.archived(&id, &request.revision)?,
            models.source_path(&id)?,
        )
    };
    inspect(&yaml, &path, &app.options, true, &app, &id).await?;
    let _policy = app.users.writes.lock().await;
    super::super::users::check(&app, &principal, &id, super::super::users::Role::Publisher)?;
    let models = app.models.lock().unwrap();
    let model = models.get(&id)?;
    Ok(Json(models.activate(model, request, Action::Rollback)?).into_response())
}

pub(crate) async fn unpublish(
    State(app): State<Arc<App>>,
    RoutePath(id): RoutePath<String>,
    axum::Extension(principal): axum::Extension<Principal>,
    headers: HeaderMap,
    Json(request): Json<Deactivation>,
) -> ApiResult<axum::response::Response> {
    use axum::response::IntoResponse;
    if app.ha.is_some() {
        return super::super::ha::mutate(
            &app,
            id,
            &principal,
            &headers,
            super::super::ha::Mutation::Unpublish(request),
        )
        .await;
    }
    let _policy = app.users.writes.lock().await;
    super::super::users::check(&app, &principal, &id, super::super::users::Role::Publisher)?;
    Ok(Json(app.models.lock().unwrap().deactivate(&id, request)?).into_response())
}

impl Models {
    fn deactivate(&mut self, id: &str, request: Deactivation) -> ApiResult<Model> {
        let mut model = self.get(id)?;
        if model.active_revision.as_deref() != Some(request.active_revision.as_str()) {
            return Err(active_conflict());
        }
        if self.version_state(id)?.is_none() {
            parse(&model.yaml, &self.source_path(id)?)?;
            self.adopt(id, &model)?;
        }
        let mut state = self.version_state(id)?.ok_or_else(Error::storage)?;
        state.active = None;
        state.offline_at_ms = Some(chrono::Utc::now().timestamp_millis().max(0) as u64);
        self.write_state(id, &state)?;
        self.engines.lock().unwrap().retain(|entry| entry.id != id);
        model.active_revision = None;
        Ok(model)
    }

    pub(in crate::admin) async fn apply_ha(
        &mut self,
        id: &str,
        mutation: crate::admin::ha::Mutation,
        options: &crate::QueryOptions,
        app: &crate::admin::App,
    ) -> ApiResult<(StatusCode, Model)> {
        use crate::admin::ha::Mutation;
        let model = match mutation {
            Mutation::Create { yaml, .. } => {
                return self
                    .save(id, yaml, None)
                    .map(|model| (StatusCode::CREATED, model));
            }
            Mutation::Save { yaml, expected } => self.save(id, yaml, Some(&expected))?,
            Mutation::Publish(request) => {
                let model = self.get(id)?;
                if model.revision != request.revision {
                    return Err(conflict());
                }
                if model.active_revision != request.active_revision {
                    return Err(active_conflict());
                }
                inspect(&model.yaml, &self.source_path(id)?, options, true, app, id).await?;
                let mut model = self.activate(model, request, Action::Publish)?;
                model.status = Status::Passed;
                self.checks
                    .insert(id.into(), (model.revision.clone(), Status::Passed));
                model
            }
            Mutation::Rollback(request) => {
                let model = self.get(id)?;
                if model.active_revision != request.active_revision {
                    return Err(active_conflict());
                }
                let yaml = self.archived(id, &request.revision)?;
                inspect(&yaml, &self.source_path(id)?, options, true, app, id).await?;
                self.activate(model, request, Action::Rollback)?
            }
            Mutation::Unpublish(request) => self.deactivate(id, request)?,
            Mutation::Restore { revision, yaml } => self.restore(id, &revision, &yaml)?,
        };
        Ok((StatusCode::OK, model))
    }
}
