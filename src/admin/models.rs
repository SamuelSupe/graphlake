use std::{
    collections::{BTreeMap, VecDeque},
    fs::{self, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
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

mod schema;
pub(super) use schema::{mcp_schema, schema};
pub(super) mod versions;

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum Status {
    Unchecked,
    Passed,
    Failed,
}

#[derive(Serialize)]
pub(super) struct Summary {
    pub(super) id: String,
    name: String,
    sources: usize,
    nodes: usize,
    edges: usize,
    status: Status,
    pub(super) active_revision: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) archive_error: Option<String>,
}

#[derive(Serialize)]
pub(super) struct Model {
    id: String,
    pub(super) yaml: String,
    pub(super) revision: String,
    status: Status,
    pub(super) active_revision: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    archive_error: Option<String>,
}

impl Model {
    pub(super) fn validated(&self) -> bool {
        matches!(self.status, Status::Passed)
    }
}

#[derive(Deserialize)]
pub(super) struct Draft {
    id: String,
    yaml: String,
    template_model: Option<String>,
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
    sources: PathBuf,
    snapshot: Option<Arc<tempfile::TempDir>>,
    checks: BTreeMap<String, (String, Status)>,
    engines: Arc<Mutex<VecDeque<CachedEngine>>>,
    activations: Arc<Mutex<VecDeque<(String, ArchiveStamp, Option<String>)>>>,
}

struct CachedEngine {
    id: String,
    revision: String,
    archive: ArchiveStamp,
    yaml: Arc<str>,
    engine: Engine,
}

#[derive(PartialEq, Eq)]
struct ArchiveStamp {
    modified: std::time::SystemTime,
    size: u64,
    #[cfg(unix)]
    identity: (u64, u64, i64, i64),
}

impl ArchiveStamp {
    fn new(meta: &fs::Metadata) -> Option<Self> {
        #[cfg(unix)]
        use std::os::unix::fs::MetadataExt;
        Some(Self {
            modified: meta.modified().ok()?,
            size: meta.len(),
            #[cfg(unix)]
            identity: (meta.dev(), meta.ino(), meta.ctime(), meta.ctime_nsec()),
        })
    }
}

pub(super) fn valid_id(id: &str) -> bool {
    let mut bytes = id.bytes();
    id.len() <= 64
        && bytes.next().is_some_and(|b| b.is_ascii_alphanumeric())
        && bytes.all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

impl Models {
    pub(super) fn new(dir: PathBuf) -> Self {
        Self::with_sources(dir.clone(), dir)
    }

    pub(super) fn with_sources(dir: PathBuf, sources: PathBuf) -> Self {
        Self {
            dir,
            sources,
            snapshot: None,
            checks: BTreeMap::new(),
            engines: Default::default(),
            activations: Default::default(),
        }
    }

    pub(super) fn in_snapshot(snapshot: Arc<tempfile::TempDir>, sources: PathBuf) -> Self {
        let mut models = Self::with_sources(snapshot.path().to_owned(), sources);
        models.snapshot = Some(snapshot);
        models
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

    pub(super) fn source_path(&self, id: &str) -> ApiResult<PathBuf> {
        let name = self.path(id)?;
        Ok(self.sources.join(name.file_name().unwrap()))
    }

    pub(super) fn source_directory(&self) -> &Path {
        &self.sources
    }

    pub(super) fn check_modeler_input(
        &self,
        id: &str,
        yaml: &str,
        principal: &Principal,
    ) -> ApiResult<()> {
        if *principal == Principal::Admin {
            return Ok(());
        }
        let path = self.source_path(id)?;
        let current = parse(&self.get(id)?.yaml, &path)?;
        let proposed = parse(yaml, &path)?;
        let mut tables = physical_tables(&current);
        if let Ok((_, _, published)) = self.published_catalog(id)
            && same_sources(&current, &published)
        {
            tables.extend(physical_tables(&published));
        }
        if !same_sources(&current, &proposed)
            || current.export_locations != proposed.export_locations
            || !physical_tables(&proposed).is_subset(&tables)
        {
            return Err(Error::new(
                StatusCode::FORBIDDEN,
                "source_scope",
                "建模者只能使用该模型已批准的数据源和物理表。请联系管理员修改来源范围。",
            ));
        }
        Ok(())
    }

    pub(super) fn check_create_input(
        &self,
        app: &App,
        id: &str,
        yaml: &str,
        template: Option<&str>,
        principal: &Principal,
    ) -> ApiResult<()> {
        if *principal == Principal::Admin {
            return Ok(());
        }
        super::users::check(app, principal, id, super::users::Role::Modeler)?;
        let template = template
            .ok_or_else(|| Error::invalid("template_required", "新建模型需要已授权的模板模型"))?;
        super::users::check(app, principal, template, super::users::Role::Modeler)?;
        self.check_modeler_input(template, yaml, principal)
    }

    pub(super) fn ha_model(&self, id: &str, validated: bool) -> ApiResult<Model> {
        let mut model = self.get(id)?;
        if validated {
            model.status = Status::Passed;
        }
        Ok(model)
    }

    pub(super) fn get(&self, id: &str) -> ApiResult<Model> {
        let path = self.path(id)?;
        let yaml = read_file(&path, CATALOG_LIMIT)?.ok_or_else(Error::not_found)?;
        let version = revision(&yaml);
        let (active_revision, archive_error) = match self.version_state(id) {
            Ok(Some(state)) => {
                let error = state
                    .active
                    .as_deref()
                    .and_then(|active| self.archived(id, active).err().map(|error| error.message));
                (state.active, error)
            }
            Ok(None) => (
                (!self.requires_task_publish(id, &yaml)?).then(|| version.clone()),
                None,
            ),
            Err(error) => (None, Some(error.message)),
        };
        Ok(Model {
            id: id.into(),
            status: if archive_error.is_some() {
                Status::Failed
            } else {
                self.status(id, &version)
            },
            revision: version,
            yaml,
            active_revision,
            archive_error,
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
            if let Ok(model) = self.get(&id)
                && parse(&model.yaml, &self.source_path(&id)?).is_ok()
            {
                if let Err(error) = self.adopt(&id, &model) {
                    eprintln!("GraphLake model {id}: {}", error.message);
                }
            }
        }
        Ok(())
    }

    pub(super) fn directory(&self) -> &Path {
        &self.dir
    }

    pub(super) fn managed_ids(&self) -> ApiResult<std::collections::BTreeSet<String>> {
        let mut ids = self.archived_ids()?;
        ids.extend(self.ids()?);
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
        Ok(ids)
    }

    pub(super) fn check_published(&self) -> ApiResult<(usize, Vec<(String, String)>)> {
        let mut count = 0;
        let mut failures = Vec::new();
        for id in self.managed_ids()? {
            let check = (|| {
                let Some(revision) = self.active_revision(&id)? else {
                    return Ok::<bool, Error>(false);
                };
                let path = self.path(&id)?;
                metadata(&path)?
                    .ok_or_else(|| versions::archive_error(&id, &revision, "模型草稿文件缺失"))?;
                parse(&self.archived(&id, &revision)?, &self.source_path(&id)?)?;
                Ok(true)
            })();
            match check {
                Ok(healthy) => count += usize::from(healthy),
                Err(error) => failures.push((id, error.message)),
            }
        }
        Ok((count, failures))
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
        Ok(self.activation(id)?.flatten())
    }

    fn activation(&self, id: &str) -> ApiResult<Option<Option<String>>> {
        let path = self.state_path(id)?;
        let stamp = || {
            metadata(&path)
                .ok()
                .flatten()
                .as_ref()
                .and_then(ArchiveStamp::new)
        };
        let before = stamp();
        if let Some(stamp) = &before {
            let mut activations = self.activations.lock().unwrap();
            if let Some(index) = activations
                .iter()
                .position(|(model, cached, _)| model == id && cached == stamp)
            {
                let cached = activations.remove(index).expect("cached model activation");
                let active = cached.2.clone();
                activations.push_front(cached);
                return Ok(Some(active));
            }
        }
        let active = self.version_state(id)?.map(|state| state.active);
        if let Some(active) = &active
            && let Some(before) = before
            && stamp().as_ref() == Some(&before)
        {
            let mut activations = self.activations.lock().unwrap();
            activations.retain(|(model, _, _)| model != id);
            activations.push_front((id.to_owned(), before, active.clone()));
            activations.truncate(64);
        }
        Ok(active)
    }

    pub(super) fn has_driver_model(&self, id: &str) -> ApiResult<bool> {
        self.path(id)?;
        match self.version_state(id) {
            Ok(state) => {
                Ok(state.is_some_and(|state| state.active.is_some() || state.is_offline()))
            }
            // A damaged manifest must not prevent other driver models from serving.
            Err(_) => Ok(self.managed_ids()?.contains(id)),
        }
    }

    pub(super) fn require_published(&self, id: &str) -> ApiResult<String> {
        let active = self.published_revision(id)?;
        self.archived(id, &active)?;
        Ok(active)
    }

    pub(super) fn published_revision(&self, id: &str) -> ApiResult<String> {
        let path = self.path(id)?;
        metadata(&path)?.ok_or_else(Error::not_found)?;
        self.active_revision(id)?.ok_or_else(unpublished)
    }

    pub(super) fn published_catalog(&self, id: &str) -> ApiResult<(String, String, Catalog)> {
        let revision = self.published_revision(id)?;
        let yaml = self.archived(id, &revision)?;
        let catalog = parse(&yaml, &self.source_path(id)?)?;
        Ok((revision, yaml, catalog))
    }

    pub(super) fn published_auto_refresh_csr(&self) -> ApiResult<Vec<(String, String, Catalog)>> {
        let mut models = Vec::new();
        for id in self.managed_ids()? {
            let Ok((revision, _, catalog)) = self.published_catalog(&id) else {
                continue;
            };
            if catalog.freshness.auto_refresh_csr {
                models.push((id, revision, catalog));
            }
        }
        Ok(models)
    }

    pub(super) fn inherit_caches(&mut self, previous: &Models, unchanged: impl Fn(&str) -> bool) {
        let carried: Vec<CachedEngine> = {
            let old = previous.engines.lock().unwrap();
            old.iter()
                .filter_map(|entry| {
                    let rel = format!(".graphlake-{}.{}.yaml", entry.id, entry.revision);
                    if !unchanged(&rel) {
                        return None;
                    }
                    let path = self.archive_path(&entry.id, &entry.revision).ok()?;
                    let stamp = metadata(&path).ok()?.as_ref().and_then(ArchiveStamp::new)?;
                    Some(CachedEngine {
                        id: entry.id.clone(),
                        revision: entry.revision.clone(),
                        archive: stamp,
                        yaml: Arc::clone(&entry.yaml),
                        engine: entry.engine.clone(),
                    })
                })
                .collect()
        };
        if !carried.is_empty() {
            let mut engines = self.engines.lock().unwrap();
            for entry in carried {
                if engines
                    .iter()
                    .any(|existing| existing.id == entry.id && existing.revision == entry.revision)
                {
                    continue;
                }
                if engines.len() >= 16 {
                    engines.pop_front();
                }
                engines.push_back(entry);
            }
        }
    }

    pub(super) fn list(&self, permits: impl Fn(&str) -> Option<bool>) -> ApiResult<Vec<Summary>> {
        let mut result = Vec::new();
        for id in self.managed_ids()? {
            let Some(published_only) = permits(&id) else {
                continue;
            };
            let path = self.path(&id)?;
            if published_only {
                if let Ok((active, _, catalog)) = self.published_catalog(&id) {
                    result.push(Summary {
                        id,
                        name: catalog.graph,
                        sources: catalog.sources.len(),
                        nodes: catalog.nodes.len(),
                        edges: catalog.edges.len(),
                        status: Status::Passed,
                        active_revision: Some(active),
                        archive_error: None,
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
                archive_error: None,
            };
            if let Ok(model) = self.get(&id) {
                summary.active_revision = model.active_revision;
                summary.archive_error = model.archive_error;
                if let Ok(catalog) = Catalog::parse_at(&model.yaml, &path) {
                    summary.name = catalog.graph;
                    summary.sources = catalog.sources.len();
                    summary.nodes = catalog.nodes.len();
                    summary.edges = catalog.edges.len();
                    if summary.archive_error.is_none() {
                        summary.status = model.status;
                    }
                }
            } else if let Err(error) = self.published_catalog(&id) {
                summary.archive_error = Some(error.message);
            }
            result.push(summary);
        }
        Ok(result)
    }

    fn save(&mut self, id: &str, yaml: String, expected: Option<&str>) -> ApiResult<Model> {
        let path = self.path(id)?;
        parse(&yaml, &self.source_path(id)?)?;
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
                None => self.write_state(id, versions::VersionState::default())?,
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
        self.get(id)
    }

    // Keep a coordinator snapshot on disk for as long as an unlocked reader
    // can still open its manifests and archives by path.
    pub(super) fn read_files(&self) -> Self {
        Self {
            dir: self.dir.clone(),
            sources: self.sources.clone(),
            snapshot: self.snapshot.clone(),
            checks: self.checks.clone(),
            engines: Arc::clone(&self.engines),
            activations: Arc::clone(&self.activations),
        }
    }

    pub(super) fn query_engine(
        &self,
        app: &App,
        id: &str,
        expected: Option<&str>,
        principal: &Principal,
    ) -> ApiResult<(String, String, Engine)> {
        let path = self.path(id)?;
        let active = match self.activation(id)? {
            Some(active) => {
                metadata(&path)?.ok_or_else(Error::not_found)?;
                let active = active.ok_or_else(unpublished)?;
                if expected.is_some_and(|expected| active != expected) {
                    return Err(versions::active_conflict());
                }
                active
            }
            None if *principal == Principal::Admin
                && self.dir == self.sources
                && expected.is_some() =>
            {
                app.models
                    .lock()
                    .unwrap()
                    .adopt_for_query(id, expected.unwrap())?
            }
            None if expected.is_none() => {
                metadata(&path)?.ok_or_else(Error::not_found)?;
                return Err(unpublished());
            }
            None => return Err(Error::not_found()),
        };
        let (yaml, engine) = self.cached_engine(id, &active, &app.admission)?;
        if self.active_revision(id)?.as_deref() != Some(&active) {
            return Err(versions::active_conflict());
        }
        Ok((active, yaml, engine))
    }

    fn adopt_for_query(&mut self, id: &str, expected: &str) -> ApiResult<String> {
        if self.version_state(id)?.is_some() {
            let active = self.published_revision(id)?;
            if active != expected {
                return Err(versions::active_conflict());
            }
            return Ok(active);
        }
        let model = self.get(id)?;
        if model.revision != expected {
            return Err(versions::active_conflict());
        }
        parse(&model.yaml, &self.source_path(id)?)?;
        self.adopt(id, &model)?;
        if self.active_revision(id)?.as_deref() != Some(expected) {
            return Err(unpublished());
        }
        Ok(model.revision)
    }

    pub(super) fn cached_engine(
        &self,
        id: &str,
        revision: &str,
        admission: &Arc<crate::engine::Admission>,
    ) -> ApiResult<(String, Engine)> {
        let path = self.archive_path(id, revision)?;
        let stamp = metadata(&path)
            .map_err(|error| versions::archive_error(id, revision, &error.message))?
            .as_ref()
            .and_then(ArchiveStamp::new);
        #[cfg(unix)]
        if let Some(stamp) = &stamp {
            let mut engines = self.engines.lock().unwrap();
            if let Some(index) = engines.iter().position(|entry| {
                entry.id == id && entry.revision == revision && entry.archive == *stamp
            }) {
                let entry = engines.remove(index).expect("cached engine");
                let cached = (entry.yaml.clone(), entry.engine.clone());
                engines.push_back(entry);
                drop(engines);
                return Ok((cached.0.to_string(), cached.1));
            }
        }
        let yaml = self.archived(id, revision)?;
        let mut catalog = parse(&yaml, &self.source_path(id)?)?;
        // The engine refuses EXPORT to any unregistered destination.
        catalog.export_locations.clear();
        let engine = Engine::with_admission(catalog, admission.clone())
            .map_err(|_| Error::invalid("invalid_model", "图模型配置无效"))?;
        if let Some(archive) = stamp {
            let mut engines = self.engines.lock().unwrap();
            engines.retain(|entry| entry.id != id || entry.revision != revision);
            if engines.len() >= 16 {
                engines.pop_front();
            }
            engines.push_back(CachedEngine {
                id: id.into(),
                revision: revision.into(),
                archive,
                yaml: Arc::from(yaml.as_str()),
                engine: engine.clone(),
            });
        }
        Ok((yaml, engine))
    }

    pub(super) fn snapshot_engine(
        &self,
        id: &str,
        revision: &str,
        admission: &Arc<crate::engine::Admission>,
    ) -> ApiResult<Engine> {
        let active = self.published_revision(id)?;
        if active != revision {
            self.cached_engine(id, &active, admission)?;
        }
        self.cached_engine(id, revision, admission)
            .map(|(_, engine)| engine)
    }
}

impl App {
    pub(super) async fn read_models<T: Send + 'static>(
        self: &Arc<Self>,
        read: impl FnOnce(Models) -> ApiResult<T> + Send + 'static,
    ) -> ApiResult<T> {
        let app = self.clone();
        tokio::task::spawn_blocking(move || {
            let files = app.models.lock().unwrap().read_files();
            read(files)
        })
        .await
        .map_err(|_| Error::storage())?
    }

    pub(super) async fn published_model_revision(&self, id: &str) -> ApiResult<String> {
        let files = crate::blocking(|| self.models.lock().unwrap().read_files());
        let admission = self.admission.clone();
        let id = id.to_owned();
        tokio::task::spawn_blocking(move || {
            let revision = files.published_revision(&id)?;
            files.cached_engine(&id, &revision, &admission)?;
            Ok(revision)
        })
        .await
        .map_err(|_| Error::storage())?
    }

    pub(super) async fn query_model(
        self: &Arc<Self>,
        id: &str,
        expected: &str,
        principal: &Principal,
    ) -> ApiResult<(String, String, Engine)> {
        let (app, id, expected, principal) = (
            Arc::clone(self),
            id.to_owned(),
            expected.to_owned(),
            principal.clone(),
        );
        tokio::task::spawn_blocking(move || {
            let files = app.models.lock().unwrap().read_files();
            files.query_engine(&app, &id, Some(&expected), &principal)
        })
        .await
        .map_err(|_| Error::storage())?
    }
}

fn same_sources(left: &Catalog, right: &Catalog) -> bool {
    serde_json::to_value(&left.sources).expect("catalog sources serialize")
        == serde_json::to_value(&right.sources).expect("catalog sources serialize")
}

pub(super) fn physical_tables(catalog: &Catalog) -> std::collections::BTreeSet<String> {
    catalog
        .nodes
        .values()
        .map(|node| (&node.source, &node.inputs))
        .chain(
            catalog
                .edges
                .values()
                .map(|edge| (&edge.source, &edge.inputs)),
        )
        .flat_map(|(table, inputs)| {
            std::iter::once(table)
                .filter(|table| !table.is_empty())
                .chain(inputs.iter().map(|input| &input.table))
                .cloned()
        })
        .collect()
}

fn unpublished() -> Error {
    Error::new(StatusCode::CONFLICT, "unpublished", "请先发布模型版本")
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

pub(super) fn parse(yaml: &str, path: &Path) -> ApiResult<Catalog> {
    check_size(yaml)?;
    Catalog::parse_at(yaml, path).map_err(|error| {
        let mut response = Error::invalid("invalid_model", format!("{error:#}"));
        response.location = error.downcast_ref::<ValidationSite>().cloned();
        response
    })
}

pub(super) fn revision(yaml: &str) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(yaml.as_bytes()))
}

fn etag(revision: &str) -> String {
    format!("\"{revision}\"")
}

pub(super) fn model_response(model: Model) -> ([(header::HeaderName, String); 1], Json<Model>) {
    ([(header::ETAG, etag(&model.revision))], Json(model))
}

pub(super) async fn list(
    State(app): State<Arc<App>>,
    axum::Extension(principal): axum::Extension<Principal>,
) -> ApiResult<Json<Vec<Summary>>> {
    use super::users::Role;
    let access = app.clone();
    let summaries = app
        .read_models(move |models| {
            models.list(|id| match &principal {
                Principal::Admin => Some(false),
                Principal::Service(_) | Principal::ServiceMember { .. } => {
                    super::access::query_allowed(&access, &principal, id).then_some(true)
                }
                Principal::User { .. } => {
                    let roles = access.users.roles(&principal, id);
                    let config = [Role::Modeler, Role::Publisher, Role::Auditor]
                        .iter()
                        .any(|role| roles.contains(role));
                    (config || roles.contains(&Role::Query)).then_some(!config)
                }
            })
        })
        .await?;
    Ok(Json(summaries))
}

pub(super) async fn read(
    State(app): State<Arc<App>>,
    RoutePath(id): RoutePath<String>,
) -> ApiResult<impl axum::response::IntoResponse> {
    Ok(model_response(
        app.read_models(move |models| models.get(&id)).await?,
    ))
}

pub(super) async fn create(
    State(app): State<Arc<App>>,
    axum::Extension(principal): axum::Extension<Principal>,
    headers: HeaderMap,
    Json(draft): Json<Draft>,
) -> ApiResult<axum::response::Response> {
    use axum::response::IntoResponse;
    if app.ha.is_some() {
        return super::ha::mutate(
            &app,
            draft.id,
            &principal,
            &headers,
            super::ha::Mutation::Create {
                yaml: draft.yaml,
                template_model: draft.template_model,
            },
        )
        .await;
    }
    let _policy = app.users.writes.lock().await;
    let model = crate::blocking(|| {
        let mut models = app.models.lock().unwrap();
        models.check_create_input(
            &app,
            &draft.id,
            &draft.yaml,
            draft.template_model.as_deref(),
            &principal,
        )?;
        models.save(&draft.id, draft.yaml, None)
    })?;
    let audited_model = super::access::AuditModel(model.id.clone());
    Ok((
        StatusCode::CREATED,
        axum::Extension(audited_model),
        model_response(model),
    )
        .into_response())
}

pub(super) async fn update(
    State(app): State<Arc<App>>,
    RoutePath(id): RoutePath<String>,
    axum::Extension(principal): axum::Extension<Principal>,
    headers: HeaderMap,
    Json(edit): Json<Edit>,
) -> ApiResult<axum::response::Response> {
    use axum::response::IntoResponse;
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
    if app.ha.is_some() {
        return super::ha::mutate(
            &app,
            id,
            &principal,
            &headers,
            super::ha::Mutation::Save {
                yaml: edit.yaml,
                expected: expected.into(),
            },
        )
        .await;
    }
    let _policy = app.users.writes.lock().await;
    super::users::check(&app, &principal, &id, super::users::Role::Modeler)?;
    let model = crate::blocking(|| {
        let mut models = app.models.lock().unwrap();
        models.check_modeler_input(&id, &edit.yaml, &principal)?;
        models.save(&id, edit.yaml, Some(expected))
    })?;
    Ok(model_response(model).into_response())
}

pub(super) async fn validate(
    State(app): State<Arc<App>>,
    axum::Extension(principal): axum::Extension<Principal>,
    Json(draft): Json<CheckDraft>,
) -> ApiResult<impl axum::response::IntoResponse> {
    let _permit = app.metadata_work.try_acquire().map_err(|_| Error::busy())?;
    super::users::check(&app, &principal, &draft.id, super::users::Role::Modeler)?;
    let files = app.models.lock().unwrap().read_files();
    let draft_id = draft.id.clone();
    let draft_yaml = draft.yaml.clone();
    let principal_check = principal.clone();
    let path = tokio::task::spawn_blocking(move || {
        files.check_modeler_input(&draft_id, &draft_yaml, &principal_check)?;
        files.source_path(&draft_id)
    })
    .await
    .map_err(|_| Error::storage())??;
    let result = inspect(
        &draft.yaml,
        &path,
        &app.options,
        draft.check_data,
        &app,
        &draft.id,
    )
    .await;
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
    app: &App,
    model: &str,
) -> ApiResult<BTreeMap<String, crate::arrow::datatypes::SchemaRef>> {
    let catalog = parse(yaml, path)?;
    let mut options = options.clone();
    let admission = if check_data {
        &app.admission
    } else {
        // Metadata checks must still run while a query waits on a source lock.
        super::ops::limit_model_probe(&mut options);
        &app.probe_admission
    };
    let mut engine = Engine::with_admission(catalog, admission.clone())
        .map_err(|_| Error::invalid("invalid_model", "图模型配置无效"))?;
    let _selection = super::local_tables::managed::resolve(
        app,
        &mut engine,
        model,
        options.timeout.as_secs() + 30,
    )
    .await?;
    let schemas = Box::pin(engine.inspect_model(options, check_data))
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
        })?;
    let catalog = &engine.catalog;
    for (label, semantics) in catalog
        .nodes
        .iter()
        .map(|(name, node)| (name, &node.semantics))
        .chain(
            catalog
                .edges
                .iter()
                .map(|(name, edge)| (name, &edge.semantics)),
        )
    {
        if let Some(semantics) = semantics {
            semantics.validate_types(&schemas[label]).map_err(|error| {
                let mut response = Error::invalid("validation_failed", error.to_string());
                response.location = Some(ValidationSite::field(label, "semantics", None));
                response
            })?;
        }
    }
    for task in &catalog.tasks {
        for label in task.labels() {
            label
                .validate_types(catalog, &schemas[label.name])
                .map_err(|error| {
                    let mut response = Error::invalid("validation_failed", error.to_string());
                    response.location = Some(ValidationSite::field(label.name, "tasks", None));
                    response
                })?;
        }
    }
    Ok(schemas)
}

pub(super) use versions::{archived_yaml, history, publish, restore_archive, rollback, unpublish};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retired_catalog_snapshot_survives_an_unlocked_reader() -> anyhow::Result<()> {
        let snapshot = Arc::new(tempfile::tempdir()?);
        let dir = snapshot.path().to_owned();
        let yaml = "graph: prior generation";
        fs::write(dir.join("fixture.yaml"), yaml)?;
        let models = Models::in_snapshot(snapshot, dir.clone());
        let reader = models.read_files();
        drop(models);
        assert_eq!(
            reader
                .get("fixture")
                .map_err(|error| anyhow::anyhow!(error.message))?
                .yaml,
            yaml
        );
        drop(reader);
        assert!(!dir.exists());
        Ok(())
    }

    #[test]
    fn inherit_caches_preserves_unchanged_engines_across_ha_swap() -> anyhow::Result<()> {
        let dir1 = Arc::new(tempfile::tempdir()?);
        let dir2 = Arc::new(tempfile::tempdir()?);
        let sources = tempfile::tempdir()?;
        let data = sources.path().join("nodes.jsonl");
        fs::write(&data, "{\"id\":1}\n")?;
        let yaml = format!(
            "graph: cache-test\nsources:\n  s:\n    type: parquet\n    tables:\n      nodes: [\"{}\"]\nnodes:\n  N:\n    source: s.nodes\n    key: [id]\n",
            data.display()
        );
        let rev = revision(&yaml);
        let rel = format!(".graphlake-m.{rev}.yaml");
        fs::write(dir1.path().join("m.yaml"), &yaml)?;
        fs::write(dir1.path().join(&rel), &yaml)?;
        fs::write(dir2.path().join("m.yaml"), &yaml)?;
        fs::write(dir2.path().join(&rel), &yaml)?;

        let m1 = Models::in_snapshot(dir1, sources.path().to_path_buf());
        let admission = Arc::new(crate::engine::Admission::new(
            crate::EngineLimits::default(),
        )?);
        let _ = m1
            .cached_engine("m", &rev, &admission)
            .map_err(|e| anyhow::anyhow!(e.message))?;
        assert_eq!(m1.engines.lock().unwrap().len(), 1);

        let mut m2 = Models::in_snapshot(dir2, sources.path().to_path_buf());
        m2.inherit_caches(&m1, |file| file == rel);
        assert_eq!(m2.engines.lock().unwrap().len(), 1);
        let (cached_yaml, _) = m2
            .cached_engine("m", &rev, &admission)
            .map_err(|e| anyhow::anyhow!(e.message))?;
        assert_eq!(cached_yaml, yaml);
        Ok(())
    }
}
