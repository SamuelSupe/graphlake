use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use axum::{
    Json,
    extract::{Path as RoutePath, State},
    http::{HeaderMap, StatusCode, header},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::catalog::{MappedTable, ValidationSite};
use crate::{Catalog, Engine};

use super::{ApiResult, App, CATALOG_LIMIT, Error, access::Principal};

mod versions;

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum Status {
    Unchecked,
    Passed,
    Failed,
}

#[derive(Serialize)]
pub(super) struct Summary {
    id: String,
    name: String,
    sources: usize,
    nodes: usize,
    edges: usize,
    status: Status,
    active_revision: Option<String>,
}

#[derive(Serialize)]
pub(super) struct Model {
    id: String,
    yaml: String,
    revision: String,
    status: Status,
    active_revision: Option<String>,
}

#[derive(Deserialize)]
pub(super) struct Draft {
    id: String,
    yaml: String,
}

#[derive(Deserialize)]
pub(super) struct CheckDraft {
    id: String,
    yaml: String,
    #[serde(default)]
    check_data: bool,
}

#[derive(Deserialize, Serialize)]
pub(super) struct Edit {
    yaml: String,
}

#[derive(Serialize)]
pub(super) struct Column {
    name: String,
    data_type: String,
    nullable: bool,
}

#[derive(Serialize)]
pub(super) struct Validation {
    status: Status,
    check_data: bool,
    labels: BTreeMap<String, Vec<Column>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    location: Option<ValidationSite>,
}

pub(super) struct Models {
    dir: PathBuf,
    checks: BTreeMap<String, (String, Status)>,
    engine: Option<(String, String, Engine)>,
}

pub(super) fn valid_id(id: &str) -> bool {
    let mut bytes = id.bytes();
    id.len() <= 64
        && bytes.next().is_some_and(|b| b.is_ascii_alphanumeric())
        && bytes.all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

impl Models {
    pub(super) fn new(dir: PathBuf) -> Self {
        Self {
            dir,
            checks: BTreeMap::new(),
            engine: None,
        }
    }

    pub(super) fn path(&self, id: &str) -> ApiResult<PathBuf> {
        if !valid_id(id) {
            return Err(Error::invalid(
                "invalid_id",
                "模型标识需为 1–64 位字母、数字、下划线或连字符，首字符为字母或数字",
            ));
        }
        Ok(self.dir.join(format!("{id}.yaml")))
    }

    fn get(&self, id: &str) -> ApiResult<Model> {
        let path = self.path(id)?;
        let yaml = read_file(&path, CATALOG_LIMIT)?.ok_or_else(Error::not_found)?;
        let version = revision(&yaml);
        let active_revision = match self.version_state(id)? {
            Some(state) => state.active,
            None => Some(version.clone()),
        };
        Ok(Model {
            id: id.into(),
            status: self.status(id, &version),
            revision: version,
            yaml,
            active_revision,
        })
    }

    fn status(&self, id: &str, revision: &str) -> Status {
        self.checks
            .get(id)
            .filter(|(version, _)| version == revision)
            .map_or(Status::Unchecked, |(_, status)| *status)
    }

    fn ids(&self) -> ApiResult<Vec<String>> {
        let mut ids = Vec::new();
        for entry in fs::read_dir(&self.dir).map_err(|_| Error::storage())? {
            let entry = entry.map_err(|_| Error::storage())?;
            let path = entry.path();
            if path.extension().and_then(|value| value.to_str()) != Some("yaml")
                || !entry.file_type().map_err(|_| Error::storage())?.is_file()
            {
                continue;
            }
            let Some(id) = path.file_stem().and_then(|value| value.to_str()) else {
                continue;
            };
            if self.path(id).is_ok() {
                ids.push(id.to_owned());
            }
        }
        ids.sort_unstable();
        Ok(ids)
    }

    pub(super) fn adopt_existing(&self) -> ApiResult<()> {
        for id in self.ids()? {
            if !matches!(self.version_state(&id), Ok(None)) {
                continue;
            }
            let path = self.path(&id)?;
            if let Ok(model) = self.get(&id)
                && parse(&model.yaml, &path).is_ok()
            {
                self.adopt(&id, &model)?;
            }
        }
        Ok(())
    }

    pub(super) fn directory(&self) -> &Path {
        &self.dir
    }

    pub(super) fn check_published(&self) -> ApiResult<usize> {
        let mut ids = self.archived_ids()?;
        for entry in fs::read_dir(&self.dir).map_err(|_| Error::storage())? {
            let name = entry.map_err(|_| Error::storage())?.file_name();
            let Some(id) = name
                .to_str()
                .and_then(|name| name.strip_prefix(".graphlake-")?.strip_suffix(".json"))
                .filter(|id| valid_id(id))
            else {
                continue;
            };
            ids.insert(id.to_owned());
        }
        let mut count = 0;
        for id in ids {
            if let Some(revision) = self.active_revision(&id)? {
                let path = self.path(&id)?;
                metadata(&path)?.ok_or_else(Error::not_found)?;
                let yaml = self.archived(&id, &revision)?;
                parse(&yaml, &path)?;
                count += 1;
            }
        }
        Ok(count)
    }

    fn archived_ids(&self) -> ApiResult<std::collections::BTreeSet<String>> {
        let mut ids = std::collections::BTreeSet::new();
        for entry in fs::read_dir(&self.dir).map_err(|_| Error::storage())? {
            let name = entry.map_err(|_| Error::storage())?.file_name();
            if let Some((id, revision)) = name.to_str().and_then(|name| {
                name.strip_prefix(".graphlake-")?
                    .strip_suffix(".yaml")?
                    .split_once('.')
            }) && valid_id(id)
                && versions::valid_revision(revision)
            {
                ids.insert(id.to_owned());
            }
        }
        Ok(ids)
    }

    pub(super) fn active_revision(&self, id: &str) -> ApiResult<Option<String>> {
        Ok(self.version_state(id)?.and_then(|state| state.active))
    }

    fn list(
        &self,
        published_only: bool,
        permits: impl Fn(&str) -> bool,
    ) -> ApiResult<Vec<Summary>> {
        let mut result = Vec::new();
        for id in self.ids()?.into_iter().filter(|id| permits(id)) {
            let path = self.path(&id)?;
            if published_only {
                if let Some(active) = self.active_revision(&id)? {
                    let yaml = self.archived(&id, &active)?;
                    let catalog = parse(&yaml, &path)?;
                    result.push(Summary {
                        id,
                        name: catalog.graph,
                        sources: catalog.sources.len(),
                        nodes: catalog.nodes.len(),
                        edges: catalog.edges.len(),
                        status: Status::Passed,
                        active_revision: Some(active),
                    });
                }
                continue;
            }
            let mut summary = Summary {
                id: id.clone(),
                name: id.clone(),
                sources: 0,
                nodes: 0,
                edges: 0,
                status: Status::Failed,
                active_revision: None,
            };
            if let Ok(model) = self.get(&id) {
                summary.active_revision = model.active_revision;
                if let Ok(catalog) = Catalog::parse_at(&model.yaml, &path) {
                    summary.name = catalog.graph;
                    summary.sources = catalog.sources.len();
                    summary.nodes = catalog.nodes.len();
                    summary.edges = catalog.edges.len();
                    summary.status = model.status;
                }
            }
            result.push(summary);
        }
        Ok(result)
    }

    fn save(&mut self, id: &str, yaml: String, expected: Option<&str>) -> ApiResult<Model> {
        let path = self.path(id)?;
        parse(&yaml, &path)?;
        if let Some(expected) = expected {
            let previous = self.get(id)?;
            if expected != etag(&previous.revision) {
                return Err(conflict());
            }
            self.adopt(id, &previous)?;
            if expected != etag(&self.get(id)?.revision) {
                return Err(conflict());
            }
            atomic_write(&path, yaml.as_bytes(), false)?;
        } else {
            if metadata(&path)?.is_some() {
                return Err(exists());
            }
            // A crash before the draft rename can leave an empty state behind.
            match self.version_state(id)? {
                Some(state) if state.active.is_some() => return Err(exists()),
                Some(_) => {}
                None => self.write_state(id, &versions::VersionState::default())?,
            }
            if let Err(error) = atomic_write(&path, yaml.as_bytes(), true) {
                if metadata(&path)?.is_none() {
                    let _ = fs::remove_file(self.state_path(id)?);
                }
                return Err(error);
            }
        }
        let version = revision(&yaml);
        if self
            .checks
            .get(id)
            .is_some_and(|(checked, _)| checked != &version)
        {
            self.checks.remove(id);
        }
        Ok(Model {
            id: id.into(),
            status: self.status(id, &version),
            revision: version,
            yaml,
            active_revision: self.version_state(id)?.and_then(|state| state.active),
        })
    }

    pub(super) fn query_engine(
        &mut self,
        id: &str,
        expected: &str,
        principal: &Principal,
    ) -> ApiResult<Engine> {
        let path = self.path(id)?;
        let state = self.version_state(id)?;
        let (active, legacy) = if let Some(state) = state {
            // Check the visible entry's identity without reading the draft. A broken
            // external edit must not prevent queries against the published archive.
            metadata(&path)?.ok_or_else(Error::not_found)?;
            let active = state.active.ok_or_else(|| {
                Error::new(StatusCode::CONFLICT, "unpublished", "请先发布模型版本")
            })?;
            (active, None)
        } else if *principal == Principal::Admin {
            let model = self.get(id)?;
            if model.revision != expected {
                return Err(versions::active_conflict());
            }
            parse(&model.yaml, &path)?;
            self.adopt(id, &model)?;
            (model.revision, Some(model.yaml))
        } else {
            return Err(Error::not_found());
        };
        if active != expected {
            return Err(versions::active_conflict());
        }
        if let Some((key, version, engine)) = &self.engine
            && key == id
            && version == &active
        {
            return Ok(engine.clone());
        }
        let yaml = match legacy {
            Some(yaml) => yaml,
            None => self.archived(id, &active)?,
        };
        let mut catalog = parse(&yaml, &path)?;
        // The engine refuses EXPORT to any unregistered destination.
        catalog.export_locations.clear();
        let engine =
            Engine::new(catalog).map_err(|_| Error::invalid("invalid_model", "图模型配置无效"))?;
        self.engine = Some((id.into(), active, engine.clone()));
        Ok(engine)
    }
}

pub(super) fn read_file(path: &Path, limit: usize) -> ApiResult<Option<String>> {
    let Some(meta) = metadata(path)? else {
        return Ok(None);
    };
    let too_large = || Error::invalid("catalog_limit", "配置文件超过大小上限");
    if meta.len() > limit as u64 {
        return Err(too_large());
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = options.open(path).map_err(|_| Error::storage())?;
    if !file.metadata().map_err(|_| Error::storage())?.is_file() {
        return Err(Error::storage());
    }
    let mut value = String::new();
    file.take((limit + 1) as u64)
        .read_to_string(&mut value)
        .map_err(|_| Error::invalid("invalid_yaml", "配置文件必须使用 UTF-8"))?;
    if value.len() > limit {
        return Err(too_large());
    }
    Ok(Some(value))
}

pub(super) fn atomic_write(path: &Path, contents: &[u8], create: bool) -> ApiResult<()> {
    let permissions = metadata(path)?.map(|meta| meta.permissions());
    if create && permissions.is_some() {
        return Err(exists());
    }
    let dir = path.parent().ok_or_else(Error::storage)?;
    let mut temp = tempfile::NamedTempFile::new_in(dir).map_err(|_| Error::storage())?;
    if let Some(permissions) = permissions {
        temp.as_file()
            .set_permissions(permissions)
            .map_err(|_| Error::storage())?;
    }
    temp.write_all(contents)
        .and_then(|_| temp.as_file().sync_all())
        .map_err(|_| Error::storage())?;
    if create {
        temp.persist_noclobber(path).map_err(|error| {
            if error.error.kind() == std::io::ErrorKind::AlreadyExists {
                exists()
            } else {
                Error::storage()
            }
        })?;
    } else {
        temp.persist(path).map_err(|_| Error::storage())?;
    }
    #[cfg(unix)]
    fs::File::open(dir)
        .and_then(|file| file.sync_all())
        .map_err(|_| Error::storage())?;
    Ok(())
}

fn metadata(path: &Path) -> ApiResult<Option<fs::Metadata>> {
    match fs::symlink_metadata(path) {
        Ok(meta) if meta.is_file() => Ok(Some(meta)),
        Ok(_) => Err(Error::new(
            StatusCode::FORBIDDEN,
            "invalid_path",
            "模型必须是目录内的普通文件",
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(_) => Err(Error::storage()),
    }
}

fn exists() -> Error {
    Error::new(StatusCode::CONFLICT, "model_exists", "模型标识已存在")
}

fn conflict() -> Error {
    Error::new(
        StatusCode::PRECONDITION_FAILED,
        "revision_conflict",
        "磁盘上的配置已变化，请复制草稿后重新加载",
    )
}

fn check_size(yaml: &str) -> ApiResult<()> {
    if yaml.len() > CATALOG_LIMIT {
        return Err(Error::invalid("catalog_limit", "YAML 文件超过 1 MiB"));
    }
    Ok(())
}

fn parse(yaml: &str, path: &Path) -> ApiResult<Catalog> {
    check_size(yaml)?;
    Catalog::parse_at(yaml, path).map_err(|error| {
        let mut response = Error::invalid("invalid_model", format!("{error:#}"));
        response.location = error.downcast_ref::<ValidationSite>().cloned();
        response
    })
}

fn revision(yaml: &str) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(yaml.as_bytes()))
}

fn etag(revision: &str) -> String {
    format!("\"{revision}\"")
}

fn model_response(model: Model) -> ([(header::HeaderName, String); 1], Json<Model>) {
    ([(header::ETAG, etag(&model.revision))], Json(model))
}

pub(super) async fn list(
    State(app): State<Arc<App>>,
    axum::Extension(principal): axum::Extension<Principal>,
) -> ApiResult<Json<Vec<Summary>>> {
    Ok(Json(
        app.models
            .lock()
            .unwrap()
            .list(principal != Principal::Admin, |id| {
                app.access.permits(&principal, id)
            })?,
    ))
}

pub(super) async fn read(
    State(app): State<Arc<App>>,
    RoutePath(id): RoutePath<String>,
) -> ApiResult<impl axum::response::IntoResponse> {
    Ok(model_response(app.models.lock().unwrap().get(&id)?))
}

#[derive(Serialize)]
pub(super) struct GraphSchema {
    graph: String,
    nodes: BTreeMap<String, GraphNode>,
    edges: BTreeMap<String, GraphEdge>,
}

#[derive(Serialize)]
struct GraphNode {
    key: Vec<String>,
    properties: Vec<String>,
}

#[derive(Serialize)]
struct GraphEdge {
    key: Vec<String>,
    properties: Vec<String>,
    from: crate::catalog::Endpoint,
    to: crate::catalog::Endpoint,
}

pub(super) async fn schema(
    State(app): State<Arc<App>>,
    RoutePath((id, revision)): RoutePath<(String, String)>,
    axum::Extension(principal): axum::Extension<Principal>,
) -> ApiResult<Json<GraphSchema>> {
    let engine = app
        .models
        .lock()
        .unwrap()
        .query_engine(&id, &revision, &principal)?;
    let catalog = &engine.catalog;
    Ok(Json(GraphSchema {
        graph: catalog.graph.clone(),
        nodes: catalog
            .nodes
            .iter()
            .map(|(name, node)| {
                (
                    name.clone(),
                    GraphNode {
                        key: node.key.clone(),
                        properties: node.properties.clone(),
                    },
                )
            })
            .collect(),
        edges: catalog
            .edges
            .iter()
            .map(|(name, edge)| {
                (
                    name.clone(),
                    GraphEdge {
                        key: edge.key.clone(),
                        properties: edge.properties.clone(),
                        from: edge.from.clone(),
                        to: edge.to.clone(),
                    },
                )
            })
            .collect(),
    }))
}

pub(super) async fn create(
    State(app): State<Arc<App>>,
    Json(draft): Json<Draft>,
) -> ApiResult<impl axum::response::IntoResponse> {
    let model = app
        .models
        .lock()
        .unwrap()
        .save(&draft.id, draft.yaml, None)?;
    let audited_model = super::access::AuditModel(model.id.clone());
    Ok((
        StatusCode::CREATED,
        axum::Extension(audited_model),
        model_response(model),
    ))
}

pub(super) async fn update(
    State(app): State<Arc<App>>,
    RoutePath(id): RoutePath<String>,
    headers: HeaderMap,
    Json(edit): Json<Edit>,
) -> ApiResult<impl axum::response::IntoResponse> {
    let expected = headers
        .get(header::IF_MATCH)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| {
            Error::new(
                StatusCode::PRECONDITION_REQUIRED,
                "revision_required",
                "保存前需要读取模型版本",
            )
        })?;
    Ok(model_response(app.models.lock().unwrap().save(
        &id,
        edit.yaml,
        Some(expected),
    )?))
}

pub(super) async fn validate(
    State(app): State<Arc<App>>,
    Json(draft): Json<CheckDraft>,
) -> ApiResult<impl axum::response::IntoResponse> {
    let _permit = app.metadata_work.try_acquire().map_err(|_| Error::busy())?;
    let path = app.models.lock().unwrap().path(&draft.id)?;
    let result = inspect(&draft.yaml, &path, &app.options, draft.check_data).await;
    let response = match result {
        Ok(schemas) => Validation {
            status: Status::Passed,
            check_data: draft.check_data,
            labels: schemas
                .into_iter()
                .map(|(label, schema)| {
                    let columns = schema
                        .fields()
                        .iter()
                        .map(|field| Column {
                            name: field.name().clone(),
                            data_type: field.data_type().to_string(),
                            nullable: field.is_nullable(),
                        })
                        .collect();
                    (label, columns)
                })
                .collect(),
            error: None,
            location: None,
        },
        Err(error) => Validation {
            status: Status::Failed,
            check_data: draft.check_data,
            labels: BTreeMap::new(),
            error: Some(error.message),
            location: error.location,
        },
    };
    let mut models = app.models.lock().unwrap();
    if models.checks.len() >= 128
        && !models.checks.contains_key(&draft.id)
        && let Some(oldest) = models.checks.keys().next().cloned()
    {
        models.checks.remove(&oldest);
    }
    models
        .checks
        .insert(draft.id.clone(), (revision(&draft.yaml), response.status));
    Ok((
        axum::Extension(super::access::AuditModel(draft.id)),
        Json(response),
    ))
}

async fn inspect(
    yaml: &str,
    path: &Path,
    options: &crate::QueryOptions,
    check_data: bool,
) -> ApiResult<BTreeMap<String, crate::arrow::datatypes::SchemaRef>> {
    let catalog = parse(yaml, path)?;
    let engine =
        Engine::new(catalog).map_err(|_| Error::invalid("invalid_model", "图模型配置无效"))?;
    Box::pin(engine.inspect_model(options.clone(), check_data))
        .await
        .map_err(|error| {
            let mut response =
                Error::invalid("validation_failed", super::query::explain_error(&error));
            let site = error.downcast_ref::<ValidationSite>().cloned();
            response.location = match (error.downcast_ref::<MappedTable>(), site) {
                (Some(table), Some(ValidationSite::Column { column })) => {
                    Some(ValidationSite::Table {
                        table: table.0.clone(),
                        column: Some(column),
                    })
                }
                (Some(table), None) => Some(ValidationSite::Table {
                    table: table.0.clone(),
                    column: None,
                }),
                (_, site) => site,
            };
            response
        })
}

pub(super) use versions::{archived_yaml, history, publish, rollback};
