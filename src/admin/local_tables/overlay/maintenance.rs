use std::{fs::File, path::Path};

use anyhow::{Result, ensure};
use parquet::{
    arrow::{
        ArrowWriter,
        arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder},
    },
    file::metadata::PageIndexPolicy,
};
use tempfile::NamedTempFile;
use uuid::Uuid;

use super::{key_leaf, key_range, staging_file, writer_properties};
use crate::{
    admin::local_tables::row_groups::splice,
    engine::QueryScope,
    source::parquet_snapshot::{MAX_BASE_PARTS, MAX_DELETED, Part, row_selection},
};

const TARGET_PART_BYTES: u64 = 256 * 1024 * 1024;

pub(super) fn reclaim(
    anchor: &Path,
    key: Option<&str>,
    base: &mut Vec<Part>,
    mut deleted: usize,
    staged: &mut Vec<(Uuid, NamedTempFile)>,
    scope: &QueryScope,
) -> Result<bool> {
    let root = anchor.parent().expect("managed local table path");
    // Existing snapshots can have one large base file. Slice its encoded row
    // groups once, translating physical row offsets to each new file.
    let available = MAX_BASE_PARTS - base.len() + 1;
    if available > 1 {
        let mut largest = None;
        for (index, part) in base
            .iter()
            .enumerate()
            .filter(|(_, p)| !p.deleted.is_empty())
        {
            let size = part.path(anchor).metadata()?.len();
            if size > TARGET_PART_BYTES * 2 && largest.is_none_or(|(_, previous)| size > previous) {
                largest = Some((index, size));
            }
        }
        if let Some((index, size)) = largest {
            let count = available.min(size.div_ceil(TARGET_PART_BYTES) as usize);
            let Some(parts) = split(anchor, key, &base[index], count, staged, scope)? else {
                return Ok(false);
            };
            base.splice(index..=index, parts);
        }
    }

    let mut candidates = Vec::new();
    for (index, part) in base
        .iter()
        .enumerate()
        .filter(|(_, p)| !p.deleted.is_empty())
    {
        let size = part_file(anchor, part, staged)?.metadata()?.len();
        candidates.push((index, part.deleted.len() as u128, size as u128));
    }
    candidates.sort_by(|a, b| (b.1 * a.2).cmp(&(a.1 * b.2)).then_with(|| a.2.cmp(&b.2)));
    for (index, _, _) in candidates {
        if deleted <= MAX_DELETED / 2 {
            break;
        }
        scope.check()?;
        let part = &mut base[index];
        let file = part_file(anchor, part, staged)?;
        let output = staging_file(root)?;
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)?
            .with_batch_size(scope.options.batch_size);
        if let Some(selection) = row_selection(part.rows, &part.deleted) {
            reader = reader.with_row_selection(selection);
        }
        let mut writer = ArrowWriter::try_new(
            output.reopen()?,
            reader.schema().clone(),
            Some(writer_properties()),
        )?;
        let mut rows = 0;
        for batch in reader.build()? {
            scope.check()?;
            let batch = batch?;
            writer.write(&batch)?;
            rows += batch.num_rows();
            if writer.memory_size() > scope.options.memory_bytes / 8 {
                writer.flush()?;
            }
        }
        let meta = writer.close()?;
        ensure!(
            rows == part.rows - part.deleted.len(),
            "local base partition row count changed"
        );
        deleted -= part.deleted.len();
        staged.retain(|(id, _)| *id != part.id);
        part.id = Uuid::new_v4();
        part.rows = rows;
        part.deleted.clear();
        part.key_range = key
            .and_then(|key| key_leaf(meta.file_metadata().schema_descr(), key))
            .and_then(|leaf| key_range(meta.row_groups(), leaf));
        staged.push((part.id, output));
    }
    ensure!(
        deleted <= MAX_DELETED / 2,
        "local base partition reclamation did not reach budget"
    );
    Ok(true)
}

fn split(
    anchor: &Path,
    key: Option<&str>,
    part: &Part,
    count: usize,
    staged: &mut Vec<(Uuid, NamedTempFile)>,
    scope: &QueryScope,
) -> Result<Option<Vec<Part>>> {
    let file = File::open(part.path(anchor))?;
    let meta = ArrowReaderMetadata::load(
        &file,
        ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional),
    )?;
    let groups = meta.metadata().num_row_groups();
    let leaf = key.and_then(|key| key_leaf(meta.parquet_schema(), key));
    if groups < 2 {
        return Ok(Some(vec![part.clone()]));
    }
    let chunk = groups.div_ceil(count);
    let root = anchor.parent().expect("managed local table path");
    let mut offset = 0;
    let mut result = Vec::new();
    for start in (0..groups).step_by(chunk) {
        scope.check()?;
        let end = (start + chunk).min(groups);
        let Some((slice, rows)) =
            splice::assemble(&file, meta.metadata(), start..end, None, root, scope)?
        else {
            return Ok(None);
        };
        let rows = usize::try_from(rows)?;
        let a = part.deleted.partition_point(|row| *row < offset);
        let b = part.deleted.partition_point(|row| *row < offset + rows);
        let deleted = part.deleted[a..b].iter().map(|row| row - offset).collect();
        let id = Uuid::new_v4();
        result.push(Part {
            id,
            rows,
            deleted,
            key_range: leaf
                .and_then(|leaf| key_range(&meta.metadata().row_groups()[start..end], leaf)),
        });
        staged.push((id, slice));
        offset += rows;
    }
    ensure!(
        offset == part.rows,
        "local base partition split row count changed"
    );
    Ok(Some(result))
}

fn part_file(
    anchor: &Path,
    part: &Part,
    staged: &[(Uuid, NamedTempFile)],
) -> std::io::Result<File> {
    match staged.iter().find(|(id, _)| *id == part.id) {
        Some((_, file)) => file.reopen(),
        None => File::open(part.path(anchor)),
    }
}
