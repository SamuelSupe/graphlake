mod maintenance;

use std::{
    fs::{self, File},
    io::{self, Write},
    path::Path,
};

use anyhow::{Result, ensure};
use datafusion::arrow::{
    array::{Array, Int64Array},
    compute::cast,
    datatypes::{DataType, Schema},
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
        MAX_BASE_PARTS, MAX_DELETED, MAX_DELTA_PARTS, Part, Snapshot, SnapshotPruning, parts_dir,
        row_selection,
    },
};

pub(super) const MAX_DELTA_BYTES: u64 = 16 * 1024 * 1024;
const MAX_COMPACTED_BYTES: u64 = 64 * 1024 * 1024;
const MAX_SEALED_BYTES: u64 = MAX_COMPACTED_BYTES + MAX_DELTA_BYTES * MAX_DELTA_PARTS as u64;
const SCATTERED_GROUPS: usize = 8;

pub(super) struct Prepared {
    pub snapshot: Snapshot,
    staged: Vec<(Uuid, NamedTempFile)>,
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
    let delta_rows = row_count(&delta_meta)?;
    let mut keys = Vec::with_capacity(delta_rows);
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
    let mut base_parts = previous.as_ref().map_or(1, |snapshot| snapshot.base_parts);
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
                        part.deleted.push(row);
                    }
                    row += 1;
                }
            }
        }
        part.deleted.sort_unstable();
        part.deleted.dedup();
    }

    let mut staged = Vec::new();
    let base_deleted = changed[..base_parts]
        .iter()
        .map(|part| part.deleted.len())
        .sum::<usize>();
    if base_deleted > MAX_DELETED {
        let mut base = changed.drain(..base_parts).collect();
        if !maintenance::reclaim(anchor, key, &mut base, base_deleted, &mut staged, scope)? {
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
            compact_deltas(anchor, &changed[base_parts..], delta, &delta_meta, scope)?
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
        let bounds = key_leaf(meta.file_metadata().schema_descr(), key)
            .and_then(|leaf| key_range(meta.row_groups(), leaf));
        staged.push((id, file));
        (rows, bounds)
    } else {
        (
            delta_rows,
            key_range(delta_meta.metadata().row_groups(), leaf),
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
            pruning: Some(SnapshotPruning {
                key: key.to_owned(),
                schema: Schema::new(delta_meta.schema().fields().clone()),
            }),
        },
        staged,
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
    delta: &NamedTempFile,
    first: bool,
    prepared: &Prepared,
    scope: &QueryScope,
) -> Result<bool> {
    let root = anchor.parent().expect("managed local table path");
    let directory = parts_dir(anchor);
    let snapshot = &prepared.snapshot;
    fs::create_dir_all(&directory)?;
    scope.check()?;
    if first {
        match fs::hard_link(anchor, snapshot.parts[0].path(anchor)) {
            Ok(()) => {}
            Err(error) if links_unavailable(&error) => return Ok(false),
            Err(error) => return Err(error.into()),
        }
    }
    let latest = snapshot.parts.last().expect("incoming part");
    let incoming =
        (!prepared.staged.iter().any(|(id, _)| *id == latest.id)).then_some((latest.id, delta));
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
    File::open(directory)?.sync_all()?;
    File::open(root)?.sync_all()?;

    let mut manifest = staging_file(root)?;
    manifest.write_all(&snapshot.encode()?)?;
    manifest.as_file().sync_all()?;
    scope.check()?;
    manifest.persist(anchor)?;
    File::open(root)?.sync_all()?;
    Ok(true)
}

fn links_unavailable(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::Unsupported
            | io::ErrorKind::PermissionDenied
            | io::ErrorKind::CrossesDevices
    )
}

pub(super) fn clean(anchor: &Path) -> Result<()> {
    let directory = parts_dir(anchor);
    let entries = match fs::read_dir(&directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    let live = Snapshot::read(anchor)?
        .map(|snapshot| snapshot.parts)
        .unwrap_or_default();
    // Establish the current manifest on disk before reclaiming any previous parts.
    File::open(anchor.parent().expect("managed local table path"))?.sync_all()?;
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        let id = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .and_then(|stem| Uuid::parse_str(stem).ok());
        if path.extension().is_some_and(|ext| ext == "parquet")
            && let Some(id) = id
            && !live.iter().any(|part| part.id == id)
        {
            fs::remove_file(path)?;
        }
    }
    Ok(())
}
