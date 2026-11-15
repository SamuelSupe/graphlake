mod splice;

use std::{fs::File, path::Path};

use anyhow::{Result, ensure};
use datafusion::arrow::{
    array::{Array, BooleanArray, Int64Array},
    compute::{cast, filter_record_batch},
    datatypes::DataType,
};
use parquet::{
    arrow::{
        ArrowWriter, ProjectionMask,
        arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder},
    },
    file::{
        metadata::{PageIndexPolicy, RowGroupMetaData},
        statistics::Statistics,
    },
};
use tempfile::NamedTempFile;

use super::load::{staging_file, writer_properties};
use crate::engine::QueryScope;

pub(super) fn merge(
    previous: File,
    delta: File,
    root: &Path,
    key: &str,
    scope: &QueryScope,
) -> Result<Option<(NamedTempFile, u64)>> {
    scope.check()?;
    let old = ArrowReaderMetadata::load(
        &previous,
        ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional),
    )?;
    let new = ArrowReaderMetadata::load(&delta, ArrowReaderOptions::new())?;
    if old.parquet_schema() != new.parquet_schema() {
        return Ok(None);
    }
    let key_index = old.schema().index_of(key)?;
    let leaf = old
        .parquet_schema()
        .columns()
        .iter()
        .position(|column| column.path().parts() == [key]);
    let Some(leaf) = leaf else { return Ok(None) };
    let projection = ProjectionMask::leaves(old.parquet_schema(), [leaf]);
    let mut keys = Vec::new();
    for batch in reader(&delta, &new, scope)?
        .with_projection(projection.clone())
        .build()?
    {
        scope.check()?;
        let key_array = cast(batch?.column(0), &DataType::Int64)?;
        keys.extend_from_slice(integers(key_array.as_ref())?.values());
    }
    keys.sort_unstable();

    let groups = old.metadata().row_groups();
    let table_bytes: i64 = groups.iter().map(RowGroupMetaData::compressed_size).sum();
    let mut changed = vec![false; groups.len()];
    let mut changed_bytes = 0;
    for (index, group) in groups.iter().enumerate() {
        scope.check()?;
        let bound_matches = if let Some((min, max)) = signed_bounds(group, leaf) {
            let first = keys.partition_point(|&key| key < min);
            let Some(&first) = keys.get(first).filter(|&&key| key <= max) else {
                continue;
            };
            first == min || keys.binary_search(&max).is_ok()
        } else {
            false
        };
        // Exact bounds prove a hit at the minimum or maximum. Overlapping or
        // missing bounds require a key-only scan, also for unsorted snapshots.
        if bound_matches || contains_key(&previous, &old, &projection, index, &keys, scope)? {
            changed[index] = true;
            changed_bytes += group.compressed_size();
            // The general merge avoids a second staging write for broad updates.
            if changed_bytes > table_bytes / 2
                || group.total_byte_size() > (scope.options.memory_bytes / 2) as i64
            {
                return Ok(None);
            }
        }
    }

    let staged = staging_file(root)?;
    let mut writer = ArrowWriter::try_new(
        staged.reopen()?,
        old.schema().clone(),
        Some(writer_properties()),
    )?;
    let affected = changed
        .iter()
        .enumerate()
        .filter_map(|(i, &changed)| changed.then_some(i))
        .collect::<Vec<_>>();
    if !affected.is_empty() {
        for batch in reader(&previous, &old, scope)?
            .with_row_groups(affected)
            .build()?
        {
            scope.check()?;
            let batch = batch?;
            let key_array = cast(batch.column(key_index), &DataType::Int64)?;
            let keep = BooleanArray::from_iter(
                integers(key_array.as_ref())?
                    .values()
                    .iter()
                    .map(|key| Some(keys.binary_search(key).is_err())),
            );
            writer.write(&filter_record_batch(&batch, &keep)?)?;
            if writer.memory_size() > scope.options.memory_bytes / 8 {
                writer.flush()?;
            }
        }
    }
    for batch in reader(&delta, &new, scope)?.build()? {
        scope.check()?;
        writer.write(&batch?)?;
        if writer.memory_size() > scope.options.memory_bytes / 8 {
            writer.flush()?;
        }
    }
    let replacement = writer.close()?;
    if old.parquet_schema() != replacement.file_metadata().schema_descr() {
        return Ok(None);
    }
    let kept = changed
        .iter()
        .enumerate()
        .filter_map(|(i, &changed)| (!changed).then_some(i));
    splice::assemble(
        &previous,
        old.metadata(),
        kept,
        &staged,
        &replacement,
        root,
        scope,
    )
}

fn contains_key(
    previous: &File,
    metadata: &ArrowReaderMetadata,
    projection: &ProjectionMask,
    group: usize,
    keys: &[i64],
    scope: &QueryScope,
) -> Result<bool> {
    for batch in reader(previous, metadata, scope)?
        .with_row_groups(vec![group])
        .with_projection(projection.clone())
        .build()?
    {
        scope.check()?;
        let key_array = cast(batch?.column(0), &DataType::Int64)?;
        if integers(key_array.as_ref())?
            .values()
            .iter()
            .any(|key| keys.binary_search(key).is_ok())
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn reader(
    file: &File,
    metadata: &ArrowReaderMetadata,
    scope: &QueryScope,
) -> Result<ParquetRecordBatchReaderBuilder<File>> {
    Ok(
        ParquetRecordBatchReaderBuilder::new_with_metadata(file.try_clone()?, metadata.clone())
            .with_batch_size(scope.options.batch_size),
    )
}

fn integers(array: &dyn Array) -> Result<&Int64Array> {
    ensure!(array.null_count() == 0, "主键不能包含空值");
    Ok(array
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("signed integer cast"))
}

fn signed_bounds(group: &RowGroupMetaData, leaf: usize) -> Option<(i64, i64)> {
    let statistics = group.column(leaf).statistics()?;
    if !statistics.min_is_exact() || !statistics.max_is_exact() {
        return None;
    }
    match statistics {
        Statistics::Int32(s) => Some((i64::from(*s.min_opt()?), i64::from(*s.max_opt()?))),
        Statistics::Int64(s) => Some((*s.min_opt()?, *s.max_opt()?)),
        _ => None,
    }
}
