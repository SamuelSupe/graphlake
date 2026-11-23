use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, Write},
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, ensure};
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    Source,
    admin::{
        local_tables::{
            key_index,
            store::{Store, Table},
        },
        models,
    },
    source::parquet_snapshot::{self, Snapshot},
};

pub(super) const ROOT: &str = ".graphlake-local";

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Local {
    id: Uuid,
    origin: PathBuf,
    tables: BTreeMap<String, Table>,
    files: BTreeMap<PathBuf, String>,
}

struct Pinned {
    file: File,
    snapshot: Option<Snapshot>,
    source_root: PathBuf,
    _lease: Option<Arc<File>>,
}

pub(super) struct Capture {
    origin: PathBuf,
    tables: BTreeMap<String, Table>,
    anchors: BTreeMap<Uuid, Pinned>,
}

fn open(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = options.open(path)?;
    if !file.metadata()?.is_file() {
        return Err(io::Error::other("expected a regular local backup file"));
    }
    Ok(file)
}

fn directory(path: &Path) -> Result<()> {
    ensure!(
        fs::symlink_metadata(path)?.is_dir(),
        "invalid local backup directory"
    );
    Ok(())
}

fn anchor(id: Uuid) -> PathBuf {
    PathBuf::from(format!("{id}.parquet"))
}

fn pin(path: &Path) -> Result<Option<Pinned>> {
    let mut lease = parquet_snapshot::pin(path)?;
    let mut file = match open(path) {
        Ok(file) => file,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let mut snapshot = Snapshot::read_file(&mut file)?;
    if snapshot.is_some() && lease.is_none() {
        // A first manifest creates the reader lock before publishing the anchor.
        lease = parquet_snapshot::pin(path)?;
        ensure!(lease.is_some(), "missing local snapshot reader lock");
        file = open(path)?;
        snapshot = Snapshot::read_file(&mut file)?;
    }
    if let Some(head) = &snapshot
        && let Some(gen_lease) = parquet_snapshot::pin_generation(path, head.generation)?
    {
        lease = Some(gen_lease);
    }
    file.rewind()?;
    Ok(Some(Pinned {
        file,
        snapshot,
        source_root: path.parent().unwrap().to_owned(),
        _lease: lease,
    }))
}

fn same_inode(before: &File, after: &File) -> Result<bool> {
    let (a, b) = (before.metadata()?, after.metadata()?);
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        Ok((a.dev(), a.ino()) == (b.dev(), b.ino()))
    }
    #[cfg(not(unix))]
    {
        Ok(a.len() == b.len() && a.created()? == b.created()? && a.modified()? == b.modified()?)
    }
}

impl Capture {
    pub(super) fn take(origin: &Path, managed: bool) -> Result<Option<Self>> {
        let origin = origin.canonicalize()?;
        let root = origin.join(ROOT);
        match fs::symlink_metadata(&root) {
            Ok(meta) => ensure!(meta.is_dir(), "invalid local table directory"),
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        }
        // There is a common point between these collections if state and all
        // anchors still match. Open anchors and reader leases retain that point
        // while copying, even if a later sync replaces an anchor and runs GC.
        for _ in 0..4 {
            let state = Store::state_file(&root)?;
            let mut tables = Store::parse(&state)?;
            tables.retain(|_, table| table.managed == managed);
            let mut anchors = BTreeMap::new();
            for table in tables.values_mut() {
                let selected = pin(&root.join(anchor(table.data_id)))?;
                if let Some(selected) = selected {
                    anchors.insert(table.data_id, selected);
                } else {
                    ensure!(
                        table.observed_at_ms.is_none()
                            && !table.history.iter().any(|run| run.status == "succeeded"),
                        "local copy is missing its committed file · {}/{}",
                        table.model,
                        table.config.name
                    );
                }
            }
            let mut stable = true;
            for table in tables.values() {
                let current = open(&root.join(anchor(table.data_id)));
                stable &= match (anchors.get(&table.data_id), current) {
                    (Some(before), Ok(after)) => same_inode(&before.file, &after)?,
                    (None, Err(e)) if e.kind() == io::ErrorKind::NotFound => true,
                    (_, Err(e)) if e.kind() != io::ErrorKind::NotFound => return Err(e.into()),
                    _ => false,
                };
            }
            if !stable || Store::state_file(&root)? != state {
                continue;
            }
            if tables.is_empty() {
                return Ok(None);
            }
            for table in tables.values_mut() {
                let head = anchors
                    .get(&table.data_id)
                    .and_then(|p| p.snapshot.as_ref());
                for run in table
                    .history
                    .iter_mut()
                    .filter(|run| run.status == "running")
                {
                    run.status = "interrupted".into();
                    run.error = Some("备份时任务仍在执行；恢复后可重新加载".into());
                    if let Some(snapshot) =
                        head.filter(|s| s.indexed.as_ref().is_some_and(|i| i.run_id == run.id))
                    {
                        let index = snapshot.indexed.as_ref().unwrap();
                        run.committed_generation = Some(snapshot.generation);
                        run.total_rows = Some(snapshot.rows());
                        run.finished_at_ms = Some(index.committed_at_ms);
                        table.observed_at_ms = Some(run.started_at_ms);
                        table.observation_finished_at_ms = run.finished_at_ms;
                        if run.reconciled {
                            table.reconciled_at_ms = Some(run.started_at_ms);
                        }
                    }
                }
            }
            return Ok(Some(Self {
                origin,
                tables,
                anchors,
            }));
        }
        anyhow::bail!("local tables changed while taking the checkpoint; retry the backup")
    }

    pub(super) fn combine(
        disk: Option<Self>,
        remote: Option<Self>,
        origin: &Path,
    ) -> Result<Option<Self>> {
        match (disk, remote) {
            (None, None) => Ok(None),
            (Some(snapshot), None) => Ok(Some(snapshot)),
            (None, Some(mut snapshot)) => {
                snapshot.origin = origin.to_owned();
                Ok(Some(snapshot))
            }
            (Some(mut disk), Some(remote)) => {
                for (name, table) in remote.tables {
                    ensure!(
                        disk.tables.insert(name, table).is_none(),
                        "conflicting backup table names"
                    );
                }
                for (id, pinned) in remote.anchors {
                    ensure!(
                        disk.anchors.insert(id, pinned).is_none(),
                        "conflicting backup table IDs"
                    );
                }
                Ok(Some(disk))
            }
        }
    }

    pub(super) fn write(self, parent: &Path) -> Result<Local> {
        let mut local = Local {
            id: Uuid::new_v4(),
            origin: self.origin,
            tables: self.tables,
            files: BTreeMap::new(),
        };
        let work = tempfile::Builder::new()
            .prefix(".backup-local-")
            .tempdir_in(parent)?;
        for (id, mut pinned) in self.anchors {
            let file = anchor(id);
            let expected = transfer(&mut pinned.file, Some(&work.path().join(&file)))?;
            local.files.insert(file.clone(), expected);
            if let Some(head) = pinned.snapshot {
                let root = &pinned.source_root;
                let parts = parquet_snapshot::parts_dir(&file);
                directory(&root.join(&parts))?;
                fs::create_dir(work.path().join(&parts))?;
                for part in &head.parts {
                    let path = part.path(&file);
                    let mut input = open(&root.join(&path))?;
                    let hash = transfer(&mut input, Some(&work.path().join(&path)))?;
                    local.files.insert(path, hash);
                }
                File::open(work.path().join(parts))?.sync_all()?;
            }
        }
        local.verify_structure(work.path())?;
        File::open(work.path())?.sync_all()?;
        fs::rename(work.path(), parent.join(local.directory()))?;
        File::open(parent)?.sync_all()?;
        Ok(local)
    }
}

fn transfer(input: &mut File, to: Option<&Path>) -> Result<String> {
    let mut output = to
        .map(|path| OpenOptions::new().create_new(true).write(true).open(path))
        .transpose()?;
    let mut hash = Sha256::new();
    let mut buffer = vec![0; 256 * 1024];
    loop {
        let count = input.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hash.update(&buffer[..count]);
        if let Some(file) = &mut output {
            file.write_all(&buffer[..count])?;
        }
    }
    if let Some(file) = output {
        file.sync_all()?;
    }
    Ok(hash
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

fn file_at(root: &Path, path: &Path) -> Result<File> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        directory(&root.join(parent))?;
    }
    Ok(open(&root.join(path)).with_context(|| {
        format!(
            "local backup file is missing or invalid · {}",
            path.display()
        )
    })?)
}

impl Local {
    pub(super) fn origin(&self) -> &Path {
        &self.origin
    }

    pub(super) fn directory(&self) -> String {
        format!("local-{}", self.id)
    }

    pub(super) fn has_managed(&self) -> bool {
        self.tables.values().any(|t| t.managed)
    }

    pub(super) fn count(&self) -> usize {
        self.tables.len()
    }

    pub(super) fn validate(&self, models: &models::Models, target: &Path) -> Result<()> {
        Store::validate(&self.tables)?;
        ensure!(!self.tables.is_empty(), "empty local backup section");
        ensure!(self.origin.is_absolute(), "invalid local backup origin");
        let mut allowed = BTreeSet::new();
        let ids = self
            .tables
            .values()
            .map(|t| t.data_id)
            .collect::<BTreeSet<_>>();
        for path in self.files.keys() {
            let parts: Vec<_> = path.components().collect();
            ensure!(
                parts.iter().all(|p| matches!(p, Component::Normal(_)))
                    && path.extension().is_some_and(|s| s == "parquet"),
                "invalid local backup path"
            );
            let leaf = path
                .file_stem()
                .and_then(|v| v.to_str())
                .and_then(|s| Uuid::parse_str(s).ok());
            let table = match parts.as_slice() {
                [_] => leaf.filter(|id| *path == anchor(*id)),
                [Component::Normal(parent), _] => parent
                    .to_str()
                    .and_then(|s| s.strip_suffix(".parts"))
                    .and_then(|s| Uuid::parse_str(s).ok())
                    .filter(|id| {
                        *parent == parquet_snapshot::parts_dir(&anchor(*id)).as_os_str()
                            && leaf.is_some_and(|leaf| path.file_name() == anchor(leaf).file_name())
                    }),
                _ => None,
            };
            ensure!(
                table.is_some_and(|id| ids.contains(&id)),
                "invalid local backup table file"
            );
            let hash = &self.files[path];
            ensure!(
                super::super::recovery::valid_digest(hash),
                "invalid local backup digest"
            );
            allowed.insert(path.clone());
        }
        for table in self.tables.values() {
            ensure!(
                allowed.contains(&anchor(table.data_id))
                    || (table.observed_at_ms.is_none()
                        && !table.history.iter().any(|run| run.status == "succeeded")),
                "committed local table anchor missing"
            );
        }
        validate_models(models, target, &allowed, &self.tables)
    }

    fn verify_structure(&self, dir: &Path) -> Result<()> {
        let mut referenced = BTreeSet::new();
        for table in self.tables.values() {
            let path = anchor(table.data_id);
            if !self.files.contains_key(&path) {
                continue;
            }
            referenced.insert(path.clone());
            let mut file = file_at(dir, &path)?;
            if let Some(snapshot) = Snapshot::read_file(&mut file)? {
                if let Some(index) = &snapshot.indexed {
                    ensure!(
                        index.keys == table.config.key_columns
                            && Some(&index.watermark_column)
                                == table.config.watermark_column.as_ref(),
                        "local snapshot keys or watermark column do not match its definition"
                    );
                    let checkpoint = key_index::checkpoint(index)?;
                    let field = index.schema.field_with_name(&index.watermark_column)?;
                    ensure!(
                        field.data_type() == &checkpoint.data_type(),
                        "invalid local snapshot watermark type"
                    );
                }
                let schema = snapshot
                    .indexed
                    .as_ref()
                    .map(|v| &v.schema)
                    .or_else(|| snapshot.pruning.as_ref().map(|v| &v.schema));
                for part in &snapshot.parts {
                    let part_path = part.path(&path);
                    ensure!(
                        self.files.contains_key(&part_path),
                        "local snapshot is missing a referenced part"
                    );
                    referenced.insert(part_path.clone());
                    parquet(file_at(dir, &part_path)?, Some(part.rows), schema)?;
                }
            } else {
                parquet(file, None, None)?;
            }
        }
        ensure!(
            referenced.len() == self.files.len(),
            "local backup contains unreferenced files"
        );
        Ok(())
    }

    pub(super) fn verify(&self, root: &Path) -> Result<()> {
        directory(root)?;
        for (path, expected) in &self.files {
            ensure!(
                transfer(&mut file_at(root, path)?, None)? == *expected,
                "local backup checksum mismatch · {}",
                path.display()
            );
        }
        self.verify_structure(root)
    }

    fn restored_state(&self, target: &Path) -> Result<Vec<u8>> {
        let mut tables = self.tables.clone();
        for t in tables.values_mut() {
            t.next_run_at_ms = None;
            t.next_reconcile_at_ms = None;
            t.schedule_paused = true;
            t.source.visit_paths(|path| {
                if let Ok(relative) = Path::new(path).strip_prefix(&self.origin) {
                    ensure!(
                        relative
                            .components()
                            .all(|v| matches!(v, Component::Normal(_) | Component::CurDir)),
                        "invalid local table source path"
                    );
                    *path = target
                        .join(relative)
                        .into_os_string()
                        .into_string()
                        .map_err(|_| anyhow::anyhow!("target source path is not UTF-8"))?;
                }
                Ok(())
            })?;
        }
        let bytes = serde_json::to_vec(&tables)?;
        ensure!(
            bytes.len() <= 16 * 1024 * 1024,
            "restored local table definitions exceed size limit"
        );
        Ok(bytes)
    }

    pub(super) fn verify_installed(&self, target: &Path) -> Result<()> {
        let root = target.join(ROOT);
        self.verify(&root)?;
        ensure!(
            Store::state_file(&root)?.as_bytes() == self.restored_state(target)?,
            "target already has different local table definitions"
        );
        Ok(())
    }

    pub(super) fn stage(&self, from: &Path, target: &Path) -> Result<Option<tempfile::TempDir>> {
        let destination = target.join(ROOT);
        match fs::symlink_metadata(&destination) {
            Ok(_) => {
                self.verify(&from.join(self.directory()))?;
                self.verify_installed(target)?;
                return Ok(None);
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        let source = from.join(self.directory());
        directory(&source)?;
        let pending = tempfile::Builder::new()
            .prefix(".restore-local-")
            .tempdir_in(target)?;
        let mut dirs = BTreeSet::new();
        for (path, expected) in &self.files {
            if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty())
                && dirs.insert(parent)
            {
                fs::create_dir(pending.path().join(parent))?;
                let lock = parquet_snapshot::reader_lock(
                    &pending.path().join(parent).with_extension("parquet"),
                );
                File::create(lock)?.sync_all()?;
            }
            let mut file = file_at(&source, path)?;
            let hash = transfer(&mut file, Some(&pending.path().join(path)))?;
            ensure!(
                hash == *expected,
                "local backup checksum mismatch · {}",
                path.display()
            );
        }
        self.verify_structure(pending.path())?;
        let mut state = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(pending.path().join("state.json"))?;
        state.write_all(&self.restored_state(target)?)?;
        state.sync_all()?;
        for dir in dirs {
            File::open(pending.path().join(dir))?.sync_all()?;
        }
        File::open(pending.path())?.sync_all()?;
        Ok(Some(pending))
    }
}

fn parquet(
    file: File,
    rows: Option<usize>,
    schema: Option<&datafusion::arrow::datatypes::Schema>,
) -> Result<()> {
    let metadata = ArrowReaderMetadata::load(&file, ArrowReaderOptions::default())?;
    ensure!(
        rows.is_none_or(
            |rows| usize::try_from(metadata.metadata().file_metadata().num_rows()).ok()
                == Some(rows)
        ),
        "local Parquet row count does not match manifest"
    );
    if let Some(schema) = schema {
        let actual = metadata.schema();
        ensure!(
            schema.fields().len() == actual.fields().len()
                && schema
                    .fields()
                    .iter()
                    .zip(actual.fields())
                    .all(|(a, b)| a.name() == b.name() && a.data_type() == b.data_type()),
            "local Parquet schema does not match manifest"
        );
    }
    Ok(())
}

pub(super) fn validate_models(
    models: &models::Models,
    target: &Path,
    files: &BTreeSet<PathBuf>,
    definitions: &BTreeMap<String, Table>,
) -> Result<()> {
    let root = target.join(ROOT);
    for item in models.list(|_| Some(true)).map_err(super::format_error)? {
        let (_, _, catalog) = models
            .published_catalog(&item.id)
            .map_err(super::format_error)?;
        for reference in models::physical_tables(&catalog) {
            let (.., relation, source) = catalog.source(&reference)?;
            if let Source::Parquet { tables } = source {
                for location in &tables[relation] {
                    if let Some(id) = parquet_snapshot::logical_id(location) {
                        ensure!(
                            definitions
                                .values()
                                .any(|t| t.managed && t.data_id == id && t.model == item.id)
                                && files.contains(&anchor(id)),
                            "model {} refers to a managed table missing from the backup",
                            item.id
                        );
                    } else if let Some(path) = parquet_snapshot::managed_location(location) {
                        ensure!(
                            path.parent() == Some(root.as_path()),
                            "model {} uses an absolute local table path outside the target; publish a relative .graphlake-local reference before export or restore to the original path",
                            item.id
                        );
                        ensure!(
                            path.file_name()
                                .is_some_and(|name| files.contains(Path::new(name))),
                            "model {} refers to a local table missing from the backup",
                            item.id
                        );
                    }
                }
            }
        }
    }
    Ok(())
}

pub(super) fn publish(staged: tempfile::TempDir, target: &Path) -> Result<()> {
    let destination = target.join(ROOT);
    ensure!(
        matches!(fs::symlink_metadata(&destination), Err(e) if e.kind() == io::ErrorKind::NotFound),
        "target local table directory already exists"
    );
    fs::rename(staged.path(), &destination)?;
    File::open(target)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::{
        array::{Int64Array, RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema},
    };
    use parquet::arrow::ArrowWriter;

    #[test]
    fn parquet_validation_allows_nullable_mismatch_after_compaction() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("part.parquet");
        let compacted_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            compacted_schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )?;
        let mut writer = ArrowWriter::try_new(File::create(&path)?, batch.schema(), None)?;
        writer.write(&batch)?;
        writer.close()?;

        let manifest_non_null = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]);
        parquet(File::open(&path)?, Some(2), Some(&manifest_non_null))?;

        let wrong_type = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Int64, false),
        ]);
        assert!(parquet(File::open(&path)?, Some(2), Some(&wrong_type)).is_err());
        Ok(())
    }
}
