mod maintenance;

use std::{
    collections::BTreeSet,
    fs::{self, File},
    io::{self, Read, Write},
    path::Path,
    time::Duration,
};

use anyhow::{Result, ensure};
use datafusion::{
    arrow::{
        array::{Array, Int64Array},
        compute::cast,
        datatypes::{DataType, Schema},
    },
    execution::memory_pool::MemoryConsumer,
};
use parquet::{
    arrow::{
        ArrowWriter, ProjectionMask,
        arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder},
    },
    file::metadata::{ParquetMetaData, RowGroupMetaData},
    schema::types::SchemaDescriptor,
};
use tempfile::NamedTempFile;
use uuid::Uuid;

use super::{
    load::{staging_file, writer_properties},
    row_groups::signed_bounds,
};
use crate::{
    engine::QueryScope,
    source::parquet_snapshot::{
        MAX_BASE_PARTS, MAX_DELETED, MAX_DELTA_PARTS, Part, Snapshot, SnapshotPruning,
        generation_lock, locks_dir, parts_dir, reader_lock, row_selection,
    },
};

pub(super) const MAX_DELTA_BYTES: u64 = 16 * 1024 * 1024;
const MAX_COMPACTED_BYTES: u64 = 64 * 1024 * 1024;
const MAX_SEALED_BYTES: u64 = MAX_COMPACTED_BYTES + MAX_DELTA_BYTES * MAX_DELTA_PARTS as u64;
const SCATTERED_GROUPS: usize = 8;

pub(super) struct Prepared {
    pub snapshot: Snapshot,
    pub(super) staged: Vec<(Uuid, NamedTempFile)>,
    pub(super) anchor_part: Option<Uuid>,
}

pub(super) fn prepare(
    anchor: &Path,
    previous: Option<Snapshot>,
    delta: File,
    key: &str,
    scope: &QueryScope,
) -> Result<Option<Prepared>> {
    scope.check()?;
    let delta_meta = metadata(&delta)?;
    let Some(leaf) = key_leaf(delta_meta.parquet_schema(), key) else {
        return Ok(None);
    };
    let count = row_count(&delta_meta)?;
    let memory =
        MemoryConsumer::new("local table overlay prepare").register(&scope.runtime.memory_pool);
    memory.try_grow(count.saturating_mul(std::mem::size_of::<i64>()))?;
    let mut keys = Vec::with_capacity(count);
    let projection = ProjectionMask::leaves(delta_meta.parquet_schema(), [leaf]);
    for batch in
        ParquetRecordBatchReaderBuilder::new_with_metadata(delta.try_clone()?, delta_meta.clone())
            .with_projection(projection)
            .with_batch_size(scope.options.batch_size)
            .build()?
    {
        scope.check()?;
        let array = cast(batch?.column(0), &DataType::Int64)?;
        keys.extend_from_slice(integers(array.as_ref())?.values());
    }
    keys.sort_unstable();

    let mut changed = if let Some(previous) = &previous {
        previous.parts.clone()
    } else {
        vec![Part {
            id: Uuid::new_v4(),
            rows: 0,
            deleted: Vec::new(),
            key_range: None,
        }]
    };
    let base_parts = previous.as_ref().map_or(1, |snapshot| snapshot.base_parts);
    for part in &mut changed {
        scope.check()?;
        let file = File::open(if previous.is_some() {
            part.path(anchor)
        } else {
            anchor.to_owned()
        })?;
        let meta = metadata(&file)?;
        if !same_columns(&meta, &delta_meta) {
            return Ok(None);
        }
        let Some(leaf) = key_leaf(meta.parquet_schema(), key) else {
            return Ok(None);
        };
        let rows = row_count(&meta)?;
        ensure!(
            previous.is_none() || rows == part.rows,
            "local Parquet snapshot part row count changed"
        );
        part.rows = rows;
        part.key_range = key_range(meta.metadata().row_groups(), leaf);

        let mut offset = 0;
        let mut candidates = Vec::new();
        for (index, group) in meta.metadata().row_groups().iter().enumerate() {
            if signed_bounds(group, leaf).is_none_or(|(min, max)| {
                keys.get(keys.partition_point(|&key| key < min))
                    .is_some_and(|&key| key <= max)
            }) {
                candidates.push((index, offset));
            }
            offset += usize::try_from(group.num_rows())?;
        }
        if previous.is_none() && candidates.len() < SCATTERED_GROUPS {
            return Ok(None);
        }
        let projection = ProjectionMask::leaves(meta.parquet_schema(), [leaf]);
        for (index, mut row) in candidates {
            for batch in
                ParquetRecordBatchReaderBuilder::new_with_metadata(file.try_clone()?, meta.clone())
                    .with_row_groups(vec![index])
                    .with_projection(projection.clone())
                    .with_batch_size(scope.options.batch_size)
                    .build()?
            {
                scope.check()?;
                let array = cast(batch?.column(0), &DataType::Int64)?;
                for value in integers(array.as_ref())?.values() {
                    if keys.binary_search(value).is_ok() {
                        memory.try_grow(std::mem::size_of::<usize>())?;
                        part.deleted.push(row);
                    }
                    row += 1;
                }
            }
        }
        part.deleted.sort_unstable();
        part.deleted.dedup();
    }

    let anchor_part = previous.is_none().then(|| changed[0].id);
    finish(
        anchor,
        anchor_part,
        changed,
        base_parts,
        delta,
        &delta_meta,
        Some(key),
        scope,
    )
}

pub(super) fn finish(
    anchor: &Path,
    anchor_part: Option<Uuid>,
    mut changed: Vec<Part>,
    mut base_parts: usize,
    delta: File,
    delta_meta: &ArrowReaderMetadata,
    key: Option<&str>,
    scope: &QueryScope,
) -> Result<Option<Prepared>> {
    let mut staged = Vec::new();
    let base_deleted = changed[..base_parts]
        .iter()
        .map(|part| part.deleted.len())
        .sum::<usize>();
    if base_deleted > MAX_DELETED {
        let mut base = changed.drain(..base_parts).collect();
        if !maintenance::reclaim(
            anchor,
            anchor_part,
            key,
            &mut base,
            base_deleted,
            &mut staged,
            scope,
        )? {
            return Ok(None);
        }
        base_parts = base.len();
        changed.splice(..0, base);
    }
    let delta_count = changed.len() - base_parts;
    let total_deleted = changed.iter().map(|part| part.deleted.len()).sum::<usize>();
    let id = Uuid::new_v4();
    let (rows, key_range) = if delta_count >= MAX_DELTA_PARTS
        || total_deleted > MAX_DELETED
        || base_deleted > MAX_DELETED
    {
        let Some((file, meta)) =
            compact_deltas(anchor, &changed[base_parts..], delta, delta_meta, scope)?
        else {
            return Ok(None);
        };
        changed.truncate(base_parts);
        if file.as_file().metadata()?.len() > MAX_COMPACTED_BYTES {
            if base_parts == MAX_BASE_PARTS {
                return Ok(None);
            }
            // The compacted delta contains only live rows. It can be sealed as
            // an immutable base partition and still carries the latest watermark.
            base_parts += 1;
        }
        let rows = usize::try_from(meta.file_metadata().num_rows())?;
        let bounds = key
            .and_then(|key| key_leaf(meta.file_metadata().schema_descr(), key))
            .and_then(|leaf| key_range(meta.row_groups(), leaf));
        staged.push((id, file));
        (rows, bounds)
    } else {
        (
            row_count(delta_meta)?,
            key.and_then(|key| key_leaf(delta_meta.parquet_schema(), key))
                .and_then(|leaf| key_range(delta_meta.metadata().row_groups(), leaf)),
        )
    };
    changed.push(Part {
        id,
        rows,
        deleted: Vec::new(),
        key_range,
    });
    Ok(Some(Prepared {
        snapshot: Snapshot {
            generation: Uuid::new_v4(),
            base_parts,
            parts: changed,
            indexed: None,
            pruning: key.map(|key| SnapshotPruning {
                key: key.to_owned(),
                schema: Schema::new(delta_meta.schema().fields().clone()),
            }),
        },
        staged,
        anchor_part,
    }))
}

fn compact_deltas(
    anchor: &Path,
    deltas: &[Part],
    incoming: File,
    incoming_meta: &ArrowReaderMetadata,
    scope: &QueryScope,
) -> Result<Option<(NamedTempFile, ParquetMetaData)>> {
    let file = staging_file(anchor.parent().expect("managed local table path"))?;
    // A previous delta may allow nulls in a field that the latest source
    // declares non-null. The logical field types have already been checked.
    let schema = incoming_meta.schema();
    let schema = std::sync::Arc::new(Schema::new_with_metadata(
        schema
            .fields()
            .iter()
            .map(|field| field.as_ref().clone().with_nullable(true))
            .collect::<Vec<_>>(),
        schema.metadata().clone(),
    ));
    let mut writer = ArrowWriter::try_new(file.reopen()?, schema, Some(writer_properties()))?;
    let mut rows = 0;
    // The masks already include all incoming keys. Reading the surviving rows
    // preserves exactly one version of each key, without visiting the base.
    for part in deltas {
        let live = part.rows - part.deleted.len();
        if live == 0 {
            continue;
        }
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(File::open(part.path(anchor))?)?
            .with_batch_size(scope.options.batch_size);
        if let Some(selection) = row_selection(part.rows, &part.deleted) {
            reader = reader.with_row_selection(selection);
        }
        if !append_batches(&mut writer, reader.build()?, &mut rows, scope)? {
            return Ok(None);
        }
    }
    let reader =
        ParquetRecordBatchReaderBuilder::new_with_metadata(incoming, incoming_meta.clone())
            .with_batch_size(scope.options.batch_size)
            .build()?;
    if !append_batches(&mut writer, reader, &mut rows, scope)? {
        return Ok(None);
    }
    let expected = deltas
        .iter()
        .map(|part| part.rows - part.deleted.len())
        .sum::<usize>()
        + row_count(incoming_meta)?;
    ensure!(
        rows == expected,
        "local Parquet delta compaction row count changed"
    );
    let meta = writer.close()?;
    Ok((file.as_file().metadata()?.len() <= MAX_SEALED_BYTES).then_some((file, meta)))
}

fn append_batches(
    writer: &mut ArrowWriter<File>,
    reader: parquet::arrow::arrow_reader::ParquetRecordBatchReader,
    rows: &mut usize,
    scope: &QueryScope,
) -> Result<bool> {
    for batch in reader {
        scope.check()?;
        let batch = batch?;
        writer.write(&batch)?;
        *rows += batch.num_rows();
        if writer.memory_size() > scope.options.memory_bytes / 8 {
            writer.flush()?;
        }
        if writer.bytes_written() as u64 > MAX_SEALED_BYTES {
            return Ok(false);
        }
    }
    Ok(true)
}

fn metadata(file: &File) -> Result<ArrowReaderMetadata> {
    Ok(ArrowReaderMetadata::load(file, ArrowReaderOptions::new())?)
}

fn row_count(meta: &ArrowReaderMetadata) -> Result<usize> {
    Ok(usize::try_from(meta.metadata().file_metadata().num_rows())?)
}

fn key_leaf(schema: &SchemaDescriptor, key: &str) -> Option<usize> {
    schema
        .columns()
        .iter()
        .position(|column| column.path().parts() == [key])
}

fn key_range(groups: &[RowGroupMetaData], leaf: usize) -> Option<[i64; 2]> {
    // Bounds cover physical rows, so updating a deletion mask cannot narrow them.
    let (first, rest) = groups.split_first()?;
    let (min, max) = signed_bounds(first, leaf)?;
    rest.iter().try_fold([min, max], |[min, max], group| {
        let (lo, hi) = signed_bounds(group, leaf)?;
        Some([min.min(lo), max.max(hi)])
    })
}

fn same_columns(left: &ArrowReaderMetadata, right: &ArrowReaderMetadata) -> bool {
    let left = left.schema().fields();
    let right = right.schema().fields();
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(a, b)| a.name() == b.name() && a.data_type() == b.data_type())
}

fn integers(array: &dyn Array) -> Result<&Int64Array> {
    ensure!(array.null_count() == 0, "主键不能包含空值");
    Ok(array.as_any().downcast_ref().expect("signed integer cast"))
}

pub(super) fn publish(
    anchor: &Path,
    delta: Option<&NamedTempFile>,
    prepared: &Prepared,
    scope: &QueryScope,
    before_manifest: impl FnOnce() -> Result<()>,
) -> Result<bool> {
    let root = anchor.parent().expect("managed local table path");
    let directory = parts_dir(anchor);
    let snapshot = &prepared.snapshot;
    let encoded_manifest = snapshot.encode()?;
    fs::create_dir_all(&directory)?;
    File::options()
        .create(true)
        .write(true)
        .truncate(false)
        .open(reader_lock(anchor))?
        .sync_all()?;
    scope.check()?;
    if let Some(part) = snapshot
        .parts
        .iter()
        .find(|part| Some(part.id) == prepared.anchor_part)
    {
        match fs::hard_link(anchor, part.path(anchor)) {
            Ok(()) => {}
            Err(error) if links_unavailable(&error) => return Ok(false),
            Err(error) => return Err(error.into()),
        }
    }
    let incoming = delta.and_then(|delta| {
        let latest = snapshot.parts.last()?;
        (!prepared.staged.iter().any(|(id, _)| *id == latest.id)).then_some((latest.id, delta))
    });
    for (id, file) in prepared
        .staged
        .iter()
        .map(|(id, file)| (*id, file))
        .chain(incoming)
    {
        scope.check()?;
        file.as_file().sync_all()?;
        let part = directory.join(format!("{id}.parquet"));
        match fs::hard_link(file.path(), &part) {
            Ok(()) => {}
            Err(error) if links_unavailable(&error) => {
                fs::copy(file.path(), &part)?;
                File::open(part)?.sync_all()?;
            }
            Err(error) => return Err(error.into()),
        }
    }
    write_generation_lock(anchor, snapshot)?;
    File::open(&directory)?.sync_all()?;
    File::open(root)?.sync_all()?;

    let mut manifest = staging_file(root)?;
    manifest.write_all(&encoded_manifest)?;
    manifest.as_file().sync_all()?;
    scope.check()?;
    before_manifest()?;
    scope.check()?;
    manifest.persist(anchor)?;
    File::open(root)?.sync_all()?;
    Ok(true)
}

fn write_generation_lock(anchor: &Path, snapshot: &Snapshot) -> Result<()> {
    let locks = locks_dir(anchor);
    fs::create_dir_all(&locks)?;
    let path = generation_lock(anchor, snapshot.generation);
    let mut temp = staging_file(&locks)?;
    for part in &snapshot.parts {
        temp.write_all(part.id.as_bytes())?;
    }
    temp.as_file().sync_all()?;
    temp.persist(path)?;
    File::open(&locks)?.sync_all()?;
    Ok(())
}

pub(super) fn links_unavailable(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::Unsupported
            | io::ErrorKind::PermissionDenied
            | io::ErrorKind::CrossesDevices
    )
}

fn lock_readers_briefly(lock: &File) -> Result<bool> {
    for attempt in 0..20 {
        match lock.try_lock() {
            Ok(()) => return Ok(true),
            Err(std::fs::TryLockError::WouldBlock) => {
                if attempt + 1 < 20 {
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
            Err(std::fs::TryLockError::Error(error)) => return Err(error.into()),
        }
    }
    Ok(false)
}

fn read_generation_parts(file: &mut File) -> Result<Vec<Uuid>> {
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    ensure!(
        !bytes.is_empty() && bytes.len() % 16 == 0,
        "invalid generation lock file"
    );
    bytes
        .chunks_exact(16)
        .map(|chunk| Ok(Uuid::from_slice(chunk)?))
        .collect()
}

fn generation_lock_id(path: &Path) -> Option<Uuid> {
    let name = path.file_name()?.to_str()?;
    let id = name.strip_prefix("gen-")?.strip_suffix(".lock")?;
    Uuid::parse_str(id).ok()
}

pub(super) fn clean(anchor: &Path) -> Result<bool> {
    let lock = match File::options()
        .read(true)
        .write(true)
        .open(reader_lock(anchor))
    {
        Ok(lock) => lock,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(true),
        Err(error) => return Err(error.into()),
    };
    if !lock_readers_briefly(&lock)? {
        return Ok(false);
    }
    let directory = parts_dir(anchor);
    let entries = match fs::read_dir(&directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(true),
        Err(error) => return Err(error.into()),
    };
    let live_snapshot = Snapshot::read(anchor)?;
    let live_generation = live_snapshot.as_ref().map(|snapshot| snapshot.generation);
    let live_parts = live_snapshot
        .map(|snapshot| snapshot.parts)
        .unwrap_or_default();
    let mut protected: BTreeSet<Uuid> = live_parts.iter().map(|part| part.id).collect();
    let mut part_files = Vec::new();
    for entry in entries {
        let path = entry?.path();
        if path.extension().is_some_and(|ext| ext == "parquet") {
            part_files.push(path);
        }
    }
    let mut lock_files = Vec::new();
    match fs::read_dir(locks_dir(anchor)) {
        Ok(lock_entries) => {
            for entry in lock_entries {
                let path = entry?.path();
                if generation_lock_id(&path).is_some() {
                    lock_files.push(path);
                }
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    for lock_path in lock_files {
        let Some(gen_id) = generation_lock_id(&lock_path) else {
            continue;
        };
        if Some(gen_id) == live_generation {
            continue;
        }
        let mut gen_file = match File::options().read(true).write(true).open(&lock_path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        match gen_file.try_lock() {
            Ok(()) => {
                let _ = fs::remove_file(&lock_path);
            }
            Err(std::fs::TryLockError::WouldBlock) => {
                // An active reader is still pinned to this older generation.
                let Ok(parts) = read_generation_parts(&mut gen_file) else {
                    return Ok(false);
                };
                protected.extend(parts);
            }
            Err(std::fs::TryLockError::Error(error)) => return Err(error.into()),
        }
    }
    // Release the reader gate before fsyncing the directory and unlinking retired
    // part files so concurrent query opens are not stalled on disk maintenance.
    drop(lock);
    // Establish the current manifest on disk before reclaiming any previous parts.
    File::open(anchor.parent().expect("managed local table path"))?.sync_all()?;
    let mut remaining_retired = false;
    for path in part_files {
        if let Some(id) = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .and_then(|stem| Uuid::parse_str(stem).ok())
            && !live_parts.iter().any(|part| part.id == id)
        {
            if protected.contains(&id) {
                remaining_retired = true;
            } else {
                fs::remove_file(path)?;
            }
        }
    }
    Ok(!remaining_retired)
}

fn retired(path: &Path, live: &[Part]) -> bool {
    path.extension().is_some_and(|ext| ext == "parquet")
        && path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .and_then(|stem| Uuid::parse_str(stem).ok())
            .is_some_and(|id| !live.iter().any(|part| part.id == id))
}

pub(super) fn check_retired_space(anchor: &Path, limit: u64) -> Result<()> {
    if clean(anchor)? {
        return Ok(());
    }
    let entries = match fs::read_dir(parts_dir(anchor)) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    let live = Snapshot::read(anchor)?
        .map(|snapshot| snapshot.parts)
        .unwrap_or_default();
    let mut bytes = 0u64;
    for entry in entries {
        let entry = entry?;
        if retired(&entry.path(), &live) {
            bytes = bytes.saturating_add(entry.metadata()?.len());
            ensure!(
                bytes <= limit,
                "本地副本旧分区等待查询释放，已超过保留空间预算"
            );
        }
    }
    Ok(())
}
