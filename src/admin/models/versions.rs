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
}

#[derive(Deserialize)]
pub(crate) struct Activation {
    revision: String,
    active_revision: Option<String>,
}

#[derive(Serialize)]
pub(crate) struct History {
    active_revision: Option<String>,
    versions: Vec<Entry>,
}

pub(super) fn valid_revision(revision: &str) -> bool {
    revision.len() == 43
        && revision
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
}

pub(super) fn active_conflict() -> Error {
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
        if let Some(text) = read_file(&self.state_path(id)?, 8 * CATALOG_LIMIT)? {
            return serde_json::from_str(&text)
                .map(Some)
                .map_err(|_| Error::storage());
        }
        // An archive without its manifest is incomplete state, even when a valid draft exists.
        if self.archived_ids()?.contains(id) {
            return Err(Error::storage());
        }
        Ok(None)
    }

    pub(super) fn write_state(&self, id: &str, state: &VersionState) -> ApiResult<()> {
        let content = serde_json::to_vec(state).map_err(|_| Error::storage())?;
        if content.len() > 8 * CATALOG_LIMIT {
            return Err(Error::invalid("history_limit", "模型版本历史超过大小上限"));
        }
        atomic_write(&self.state_path(id)?, &content, false)
    }

    fn archive_path(&self, id: &str, revision: &str) -> ApiResult<PathBuf> {
        self.path(id)?;
        if !valid_revision(revision) {
            return Err(Error::invalid("invalid_revision", "模型版本无效"));
        }
        Ok(self.dir.join(format!(".graphlake-{id}.{revision}.yaml")))
    }

    pub(super) fn archived(&self, id: &str, expected: &str) -> ApiResult<String> {
        let yaml = read_file(&self.archive_path(id, expected)?, CATALOG_LIMIT)?
            .ok_or_else(Error::not_found)?;
        if revision(&yaml) != expected {
            return Err(Error::storage());
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
    fn activate(&mut self, revision: &str, action: Action) {
        self.active = Some(revision.to_owned());
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
        history: vec![Entry {
            revision: model.revision,
            action: Action::Adopt,
            at_ms: 0,
        }],
    });
    state.history.reverse();
    Ok(Json(History {
        active_revision: state.active,
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

pub(crate) async fn publish(
    State(app): State<Arc<App>>,
    RoutePath(id): RoutePath<String>,
    Json(request): Json<Activation>,
) -> ApiResult<Json<Model>> {
    let _permit = app.metadata_work.try_acquire().map_err(|_| Error::busy())?;
    let (model, path) = {
        let models = app.models.lock().unwrap();
        (models.get(&id)?, models.path(&id)?)
    };
    if model.revision != request.revision {
        return Err(conflict());
    }
    if model.active_revision != request.active_revision {
        return Err(active_conflict());
    }
    if let Err(error) = inspect(&model.yaml, &path, &app.options, true).await {
        app.models
            .lock()
            .unwrap()
            .checks
            .insert(id, (model.revision, Status::Failed));
        return Err(error);
    }
    let mut models = app.models.lock().unwrap();
    let model = models.get(&id)?;
    let published = models.activate(model, request, Action::Publish)?;
    models
        .checks
        .insert(id, (published.revision.clone(), Status::Passed));
    Ok(Json(Model {
        status: Status::Passed,
        ..published
    }))
}

pub(crate) async fn rollback(
    State(app): State<Arc<App>>,
    RoutePath(id): RoutePath<String>,
    Json(request): Json<Activation>,
) -> ApiResult<Json<Model>> {
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
        (models.archived(&id, &request.revision)?, models.path(&id)?)
    };
    inspect(&yaml, &path, &app.options, true).await?;
    let models = app.models.lock().unwrap();
    let model = models.get(&id)?;
    Ok(Json(models.activate(model, request, Action::Rollback)?))
}
