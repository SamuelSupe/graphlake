use std::{
    fs::File,
    io::{self, Read, Seek, SeekFrom, Write},
    ops::Range,
    path::Path,
};

use anyhow::{Result, ensure};
use bytes::Bytes;
use parquet::file::metadata::{
    ParquetColumnIndex, ParquetMetaData, ParquetMetaDataBuilder, ParquetMetaDataReader,
    ParquetMetaDataWriter, ParquetOffsetIndex, RowGroupMetaData,
};
use tempfile::NamedTempFile;

use super::staging_file;
use crate::engine::QueryScope;

pub(super) fn assemble(
    previous: &File,
    old: &ParquetMetaData,
    kept: impl Iterator<Item = usize>,
    staged: &NamedTempFile,
    replacement: &ParquetMetaData,
    root: &Path,
    scope: &QueryScope,
) -> Result<Option<(NamedTempFile, u64)>> {
    let replacement_file = staged.reopen()?;
    let old_spans = spans(old, kept, previous.metadata()?.len());
    let new_spans = spans(
        replacement,
        0..replacement.num_row_groups(),
        replacement_file.metadata()?.len(),
    );
    let (Some(old_spans), Some(new_spans)) = (old_spans, new_spans) else {
        return Ok(None);
    };
    if old_spans.len() + new_spans.len() > i16::MAX as usize {
        return Ok(None);
    }

    let mut output = staging_file(root)?;
    let file = output.as_file_mut();
    file.write_all(b"PAR1")?;
    let mut groups = Vec::new();
    let mut column_indexes = Some(ParquetColumnIndex::new());
    let mut offset_indexes = Some(ParquetOffsetIndex::new());
    let mut alignment = clone_alignment(file);
    for (source, metadata, spans) in [
        (previous, old, old_spans),
        (&replacement_file, replacement, new_spans),
    ] {
        let mut spans = spans.as_slice();
        while let Some((_, first)) = spans.first() {
            // Adjacent groups can share one copy or reflink, even when column chunks
            // and individual groups are not aligned to filesystem blocks.
            let mut end = first.end;
            let mut count = 1;
            for (_, next) in &spans[1..] {
                if next.start != end {
                    break;
                }
                end = next.end;
                count += 1;
            }
            let target = copy_span(source, file, first.start..end, &mut alignment, scope)?;
            let shift = i64::try_from(target)? - i64::try_from(first.start)?;
            for (index, range) in &spans[..count] {
                let group = metadata.row_group(*index);
                groups.push(relocate(
                    group,
                    groups.len() as i16,
                    i64::try_from(range.start)? + shift,
                    shift,
                )?);
                column_indexes =
                    column_indexes
                        .zip(metadata.column_index())
                        .map(|(mut indexes, source)| {
                            indexes.push(source[*index].clone());
                            indexes
                        });
                offset_indexes =
                    offset_indexes
                        .zip(metadata.offset_index())
                        .map(|(mut indexes, source)| {
                            let mut columns = source[*index].clone();
                            for column in &mut columns {
                                for page in &mut column.page_locations {
                                    page.offset += shift;
                                }
                            }
                            indexes.push(columns);
                            indexes
                        });
            }
            spans = &spans[count..];
        }
    }
    let total = groups.iter().map(|group| group.num_rows() as u64).sum();
    let metadata = ParquetMetaDataBuilder::new(old.file_metadata().clone())
        .set_row_groups(groups)
        .set_column_index(column_indexes)
        .set_offset_index(offset_indexes)
        .build();
    write_footer(file, metadata)?;
    let end = file.stream_position()?;
    file.set_len(end)?;
    Ok(Some((output, total)))
}

fn spans(
    metadata: &ParquetMetaData,
    ids: impl Iterator<Item = usize>,
    file_len: u64,
) -> Option<Vec<(usize, Range<u64>)>> {
    ids.map(|index| {
        let columns = metadata.row_group(index).columns();
        let start = chunk_start(columns.first()?)?;
        let mut end = start;
        for column in columns {
            if column.file_path().is_some()
                || column.index_page_offset().is_some()
                || column.bloom_filter_offset().is_some()
                || chunk_start(column)? != end
            {
                return None;
            }
            end = end.checked_add(u64::try_from(column.compressed_size()).ok()?)?;
        }
        (start >= 4 && end <= file_len && start < end).then_some((index, start..end))
    })
    .collect()
}

fn chunk_start(column: &parquet::file::metadata::ColumnChunkMetaData) -> Option<u64> {
    u64::try_from(
        column
            .dictionary_page_offset()
            .unwrap_or_else(|| column.data_page_offset()),
    )
    .ok()
}

fn relocate(
    group: &RowGroupMetaData,
    ordinal: i16,
    start: i64,
    shift: i64,
) -> Result<RowGroupMetaData> {
    let columns = group
        .columns()
        .iter()
        .map(|column| {
            column
                .clone()
                .into_builder()
                .set_data_page_offset(column.data_page_offset() + shift)
                .set_dictionary_page_offset(
                    column.dictionary_page_offset().map(|offset| offset + shift),
                )
                .set_column_index_offset(None)
                .set_column_index_length(None)
                .set_offset_index_offset(None)
                .set_offset_index_length(None)
                .build()
        })
        .collect::<parquet::errors::Result<Vec<_>>>()?;
    Ok(group
        .clone()
        .into_builder()
        .set_ordinal(ordinal)
        .set_file_offset(start)
        .set_column_metadata(columns)
        .build()?)
}

fn write_footer(file: &mut File, metadata: ParquetMetaData) -> Result<()> {
    if metadata.column_index().is_none() && metadata.offset_index().is_none() {
        ParquetMetaDataWriter::new(file, &metadata).finish()?;
        return Ok(());
    }
    // The metadata writer computes page-index locations relative to its stream.
    // Encode the small indexes first, then rebase their locations in the footer.
    let start = i64::try_from(file.stream_position()?)?;
    let mut bytes = Vec::new();
    ParquetMetaDataWriter::new(&mut bytes, &metadata).finish()?;
    let bytes = Bytes::from(bytes);
    let trailer = bytes.len() - 8;
    let footer_len = u32::from_le_bytes(bytes[trailer..trailer + 4].try_into()?) as usize;
    let indexed = ParquetMetaDataReader::new().parse_and_finish(&bytes)?;
    let mut builder = ParquetMetaDataBuilder::new_from_metadata(indexed);
    for group in builder.take_row_groups() {
        let columns = group
            .columns()
            .iter()
            .map(|column| {
                column
                    .clone()
                    .into_builder()
                    .set_column_index_offset(
                        column.column_index_offset().map(|offset| offset + start),
                    )
                    .set_offset_index_offset(
                        column.offset_index_offset().map(|offset| offset + start),
                    )
                    .build()
            })
            .collect::<parquet::errors::Result<Vec<_>>>()?;
        builder = builder.add_row_group(group.into_builder().set_column_metadata(columns).build()?);
    }
    file.write_all(&bytes[..trailer - footer_len])?;
    ParquetMetaDataWriter::new(file, &builder.build()).finish()?;
    Ok(())
}

fn copy_span(
    source: &File,
    target: &mut File,
    range: Range<u64>,
    alignment: &mut Option<u64>,
    scope: &QueryScope,
) -> Result<u64> {
    scope.check()?;
    let offset = target.stream_position()?;
    let len = range.end - range.start;
    if let Some(block) = *alignment {
        let prefix = (block - range.start % block) % block;
        let pad = (range.start % block + block - offset % block) % block;
        let shared = len.saturating_sub(prefix) / block * block;
        if shared >= block * 2 {
            let dest = offset + pad;
            if clone_range(source, target, range.start + prefix, dest + prefix, shared) {
                target.seek(SeekFrom::Start(dest))?;
                copy_bytes(source, target, range.start, prefix, scope)?;
                target.seek(SeekFrom::Start(dest + prefix + shared))?;
                copy_bytes(
                    source,
                    target,
                    range.start + prefix + shared,
                    len - prefix - shared,
                    scope,
                )?;
                return Ok(dest);
            }
            *alignment = None;
        }
    }
    copy_bytes(source, target, range.start, len, scope)?;
    Ok(offset)
}

fn copy_bytes(
    mut source: &File,
    target: &mut File,
    start: u64,
    mut remaining: u64,
    scope: &QueryScope,
) -> Result<()> {
    source.seek(SeekFrom::Start(start))?;
    while remaining > 0 {
        scope.check()?;
        let chunk = remaining.min(8 * 1024 * 1024);
        ensure!(
            io::copy(&mut source.take(chunk), target)? == chunk,
            "truncated local Parquet row group"
        );
        remaining -= chunk;
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn clone_alignment(file: &File) -> Option<u64> {
    use std::os::unix::fs::MetadataExt;
    let block = file.metadata().ok()?.blksize();
    (block > 0).then_some(block)
}

#[cfg(not(target_os = "linux"))]
fn clone_alignment(_: &File) -> Option<u64> {
    None
}

#[cfg(target_os = "linux")]
fn clone_range(source: &File, target: &File, start: u64, dest: u64, len: u64) -> bool {
    use std::os::fd::AsRawFd;
    let range = libc::file_clone_range {
        src_fd: i64::from(source.as_raw_fd()),
        src_offset: start,
        src_length: len,
        dest_offset: dest,
    };
    // Both descriptors and the ioctl argument remain valid for the syscall.
    unsafe { libc::ioctl(target.as_raw_fd(), libc::FICLONERANGE, &range) == 0 }
}

#[cfg(not(target_os = "linux"))]
fn clone_range(_: &File, _: &File, _: u64, _: u64, _: u64) -> bool {
    false
}
