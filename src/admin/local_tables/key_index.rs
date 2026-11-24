use std::{
    collections::BTreeMap,
    fs::File,
    io::{self, Read},
    path::Path,
    sync::Arc,
};

use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use datafusion::{
    arrow::{
        array::{Array, BooleanArray, RecordBatch, RecordBatchReader},
        compute::{cast, filter_record_batch, not},
        datatypes::{DataType, Schema},
        row::{RowConverter, Rows, SortField},
    },
    execution::memory_pool::MemoryConsumer,
};
use parquet::{
    arrow::{
        ArrowWriter, ProjectionMask,
        arrow_reader::{
            ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowSelection,
            RowSelector,
        },
    },
    errors::Result as ParquetResult,
    file::{
        metadata::PageIndexPolicy,
        reader::{ChunkReader, Length},
    },
};
use rusqlite::{OptionalExtension, Transaction, params};
use tempfile::NamedTempFile;
use uuid::Uuid;

use super::{
    load::{staging_file, writer_properties},
    overlay::{self, Prepared},
    store::now,
};
use crate::{
    ScalarValue,
    engine::QueryScope,
    source::{
        ReadStats,
        parquet_snapshot::{IndexedSnapshot, MAX_DELETED, Part, Snapshot, row_selection},
    },
};

use datafusion_proto::protobuf::ScalarValue as WireScalar;
use prost::Message;

mod storage;
use storage::{
    checkpoint_truncate, generation, is_corrupt, open, reset, scratch_file, set_generation,
};

pub(super) fn supports(schema: &Schema, keys: &[String], signed: bool) -> bool {
    !keys.is_empty()
        && keys.iter().all(|key| {
            schema.field_with_name(key).is_ok_and(|field| {
                field.data_type().is_integer()
                    || matches!(
                        field.data_type(),
                        DataType::Utf8
                            | DataType::LargeUtf8
                            | DataType::Utf8View
                            | DataType::Binary
                            | DataType::LargeBinary
                            | DataType::BinaryView
                            | DataType::FixedSizeBinary(16)
                    )
            })
        })
        && (signed
            || keys.len() != 1
            || !schema
                .field_with_name(&keys[0])
                .is_ok_and(|field| field.data_type().is_signed_integer()))
}

pub(in crate::admin) fn checkpoint(index: &IndexedSnapshot) -> Result<ScalarValue> {
    Ok(ScalarValue::try_from(&WireScalar::decode(
        index.checkpoint.as_slice(),
    )?)?)
}

fn indexed(
    schema: &Schema,
    keys: &[String],
    column: &str,
    value: &ScalarValue,
    run_id: Uuid,
) -> Result<IndexedSnapshot> {
    Ok(IndexedSnapshot {
        keys: keys.to_vec(),
        schema: schema.clone(),
        watermark_column: column.into(),
        checkpoint: WireScalar::try_from(value)?.encode_to_vec(),
        committed_at_ms: now(),
        run_id,
    })
}

pub(super) fn full(
    anchor: &Path,
    managed: bool,
    file: &NamedTempFile,
    keys: &[String],
    column: &str,
    value: &ScalarValue,
    run_id: Uuid,
    scope: &QueryScope,
) -> Result<()> {
    match full_inner(anchor, managed, file, keys, column, value, run_id, scope) {
        Err(error) if is_corrupt(&error) => {
            reset(anchor, managed)?;
            full_inner(anchor, managed, file, keys, column, value, run_id, scope)
        }
        result => result,
    }
}

fn full_inner(
    anchor: &Path,
    managed: bool,
    file: &NamedTempFile,
    keys: &[String],
    column: &str,
    value: &ScalarValue,
    run_id: Uuid,
    scope: &QueryScope,
) -> Result<()> {
    let data = counted(file.reopen()?, scope);
    let meta = ArrowReaderMetadata::load(&data, ArrowReaderOptions::new())?;
    let part = Part {
        id: Uuid::new_v4(),
        rows: row_count(&meta)?,
        deleted: Vec::new(),
        key_range: None,
    };
    let snapshot = Snapshot {
        generation: Uuid::new_v4(),
        base_parts: 1,
        parts: vec![part.clone()],
        pruning: None,
        indexed: Some(indexed(meta.schema(), keys, column, value, run_id)?),
    };
    let mut conn = open(anchor, managed, scope)?;
    let tx = conn.transaction()?;
    tx.execute_batch("DROP INDEX IF EXISTS position_part; DELETE FROM position;")?;
    insert_file(&tx, &part, data, keys, scope)?;
    tx.execute(
        "CREATE INDEX IF NOT EXISTS position_part ON position(part)",
        [],
    )?;
    let staged = Prepared {
        snapshot,
        staged: Vec::new(),
        anchor_part: None,
    };
    let written = overlay::publish(anchor, Some(file), &staged, scope, || {
        set_generation(&tx, staged.snapshot.generation)?;
        tx.commit()?;
        Ok(())
    })?;
    ensure!(written, "could not publish indexed local snapshot");
    checkpoint_truncate(&conn);
    Ok(())
}

pub(super) fn compact(
    anchor: &Path,
    previous: Option<&Snapshot>,
    delta: &NamedTempFile,
    output: File,
    keys: &[String],
    deleted: Option<&str>,
    scope: &QueryScope,
) -> Result<u64> {
    let scratch = scratch_file()?;
    let mut index = rusqlite::Connection::open(scratch.path())?;
    let cancelled = scope.options.cancellation.clone();
    let deadline = scope.deadline;
    index.progress_handler(
        1024,
        Some(move || cancelled.is_cancelled() || tokio::time::Instant::now() >= deadline),
    )?;
    let cache_kib = (scope.options.memory_bytes / 8192).clamp(32, 8192);
    let batch_size = scope.options.batch_size.min(128);
    index.pragma_update(None, "cache_size", -(cache_kib as i64))?;
    index.execute_batch(
        "PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF;
         CREATE TABLE incoming (key BLOB PRIMARY KEY) WITHOUT ROWID;",
    )?;
    let memory = MemoryConsumer::new("local table compaction").register(&scope.runtime.memory_pool);
    memory.try_grow(cache_kib * 1024)?;
    let batch_memory = memory.new_empty();
    let data = counted(delta.reopen()?, scope);
    let metadata = ArrowReaderMetadata::load(&data, ArrowReaderOptions::new())?;
    let codec = Codec::new(metadata.schema(), keys)?;
    let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(data, metadata.clone());
    let projection = ProjectionMask::roots(builder.parquet_schema(), codec.indices.iter().copied());
    let reader = builder
        .with_projection(projection)
        .with_batch_size(batch_size)
        .build()?;
    let key_codec = Codec::new(reader.schema().as_ref(), keys)?;
    let tx = index.transaction()?;
    {
        let mut insert = tx.prepare("INSERT INTO incoming VALUES (?)")?;
        for batch in reader {
            scope.check()?;
            let batch = batch?;
            batch_memory.try_resize(batch.get_array_memory_size())?;
            let encoded = key_codec.rows(&batch)?;
            batch_memory.try_grow(encoded.size())?;
            for key in encoded.iter() {
                insert.execute([key.as_ref()])?;
            }
        }
    }
    tx.commit()?;
    let mut contains = index.prepare("SELECT 1 FROM incoming WHERE key = ?")?;
    // A previous snapshot may contain NULLs in a column that the latest source
    // schema declares non-null. Normalize fields to nullable before writing so
    // ArrowWriter preserves all historical NULLs instead of writing raw buffers.
    let output_schema = Arc::new(Schema::new_with_metadata(
        metadata
            .schema()
            .fields()
            .iter()
            .map(|field| field.as_ref().clone().with_nullable(true))
            .collect::<Vec<_>>(),
        metadata.schema().metadata().clone(),
    ));
    let mut writer = ArrowWriter::try_new(output, output_schema, Some(writer_properties()))?;
    let mut total = 0;
    let mut append = |batch: RecordBatch| -> Result<()> {
        total += batch.num_rows() as u64;
        writer.write(&batch)?;
        if writer.memory_size() > scope.options.memory_bytes / 8 {
            writer.flush()?;
        }
        Ok(())
    };
    let mut copy_previous = |file: File, part: Option<&Part>| -> Result<()> {
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(counted(file, scope))?
            .with_batch_size(batch_size);
        if let Some(part) = part {
            ensure!(
                usize::try_from(reader.metadata().file_metadata().num_rows())? == part.rows,
                "local snapshot part row count changed"
            );
            if let Some(selection) = row_selection(part.rows, &part.deleted) {
                reader = reader.with_row_selection(selection);
            }
        }
        let reader = reader.build()?;
        let codec = Codec::new(reader.schema().as_ref(), keys)?;
        for batch in reader {
            scope.check()?;
            let batch = batch?;
            batch_memory.try_resize(batch.get_array_memory_size().saturating_mul(2))?;
            let encoded = codec.rows(&batch)?;
            batch_memory.try_grow(encoded.size())?;
            let keep = BooleanArray::from(
                encoded
                    .iter()
                    .map(|key| Ok(!contains.exists([key.as_ref()])?))
                    .collect::<Result<Vec<_>>>()?,
            );
            append(filter_record_batch(&batch, &keep)?)?;
        }
        Ok(())
    };
    if let Some(previous) = previous {
        for part in &previous.parts {
            copy_previous(File::open(part.path(anchor))?, Some(part))?;
        }
    } else {
        copy_previous(File::open(anchor)?, None)?;
    }
    let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(
        counted(delta.reopen()?, scope),
        metadata,
    )
    .with_batch_size(batch_size)
    .build()?;
    for batch in reader {
        scope.check()?;
        let mut batch = batch?;
        batch_memory.try_resize(batch.get_array_memory_size().saturating_mul(2))?;
        if let Some(deleted) = deleted {
            let marks = cast(
                batch
                    .column_by_name(deleted)
                    .context("missing deletion column")?,
                &DataType::Boolean,
            )?;
            let marks = marks
                .as_any()
                .downcast_ref::<BooleanArray>()
                .context("invalid deletion column")?;
            batch = filter_record_batch(&batch, &not(marks)?)?;
        }
        append(batch)?;
    }
    writer.close()?;
    Ok(total)
}

/// None means the batch cannot be published within the existing overlay bounds.
#[allow(clippy::too_many_arguments)]
pub(super) fn incremental(
    anchor: &Path,
    managed: bool,
    previous: Option<&Snapshot>,
    file: &NamedTempFile,
    keys: &[String],
    column: &str,
    value: &ScalarValue,
    deleted_column: Option<&str>,
    run_id: Uuid,
    scope: &QueryScope,
) -> Result<Option<u64>> {
    match incremental_inner(
        anchor,
        managed,
        previous,
        file,
        keys,
        column,
        value,
        deleted_column,
        run_id,
        scope,
    ) {
        Err(error) if is_corrupt(&error) => {
            reset(anchor, managed)?;
            incremental_inner(
                anchor,
                managed,
                previous,
                file,
                keys,
                column,
                value,
                deleted_column,
                run_id,
                scope,
            )
        }
        result => result,
    }
}

#[allow(clippy::too_many_arguments)]
fn incremental_inner(
    anchor: &Path,
    managed: bool,
    previous: Option<&Snapshot>,
    file: &NamedTempFile,
    keys: &[String],
    column: &str,
    value: &ScalarValue,
    deleted_column: Option<&str>,
    run_id: Uuid,
    scope: &QueryScope,
) -> Result<Option<u64>> {
    let first = previous.is_none();
    if previous.is_some_and(|snapshot| {
        snapshot
            .indexed
            .as_ref()
            .is_none_or(|index| index.keys != keys || index.watermark_column != column)
    }) {
        return Ok(None);
    }
    let data = counted(file.reopen()?, scope);
    let delta_meta = ArrowReaderMetadata::load(&data, ArrowReaderOptions::new())?;
    let base_parts = previous.map_or(1, |snapshot| snapshot.base_parts);
    let mut parts = if let Some(previous) = previous {
        previous.parts.clone()
    } else {
        let old = counted(File::open(anchor)?, scope);
        let meta = ArrowReaderMetadata::load(&old, ArrowReaderOptions::new())?;
        vec![Part {
            id: Uuid::new_v4(),
            rows: row_count(&meta)?,
            deleted: Vec::new(),
            key_range: None,
        }]
    };
    // A missing immutable part invalidates incremental refresh even if the
    // current batch would not touch it.
    for part in &parts {
        if !first {
            ensure!(
                part.path(anchor).metadata()?.is_file(),
                "missing local snapshot part"
            );
        }
    }
    let mut connection = open(anchor, managed, scope)?;
    let tx = connection.transaction()?;
    let cached = tx
        .query_row("SELECT value FROM generation", [], |row| {
            row.get::<_, String>(0)
        })
        .optional()?;
    let old = previous.map(|snapshot| generation(snapshot.generation));
    let rebuilt = old.is_none() || cached != old;
    if rebuilt {
        tx.execute_batch("DROP INDEX IF EXISTS position_part; DELETE FROM position;")?;
        for part in &parts {
            let path = if first {
                anchor.to_owned()
            } else {
                part.path(anchor)
            };
            insert_file(&tx, part, counted(File::open(path)?, scope), keys, scope)?;
        }
        tx.execute(
            "CREATE INDEX IF NOT EXISTS position_part ON position(part)",
            [],
        )?;
    }

    let memory = datafusion::execution::memory_pool::MemoryConsumer::new("GraphLake indexed delta")
        .register(&scope.runtime.memory_pool);
    let watermark_unchanged = previous
        .and_then(|snapshot| snapshot.indexed.as_ref())
        .map(checkpoint)
        .transpose()?
        .is_none_or(|bound| &bound == value);
    let mut same = watermark_unchanged;

    // Read only key columns (plus optional deleted_column) to avoid decompressing
    // and buffering full payload columns on normal incremental updates.
    let mut projected_roots: Vec<usize> = keys
        .iter()
        .map(|name| delta_meta.schema().index_of(name))
        .collect::<Result<Vec<_>, _>>()?;
    if let Some(del_col) = deleted_column {
        let del_index = delta_meta.schema().index_of(del_col)?;
        if !projected_roots.contains(&del_index) {
            projected_roots.push(del_index);
        }
    }
    let projection = ProjectionMask::roots(delta_meta.parquet_schema(), projected_roots);
    let key_reader = ParquetRecordBatchReaderBuilder::new_with_metadata(data, delta_meta.clone())
        .with_projection(projection)
        .with_batch_size(scope.options.batch_size)
        .build()?;
    let key_codec = Codec::new(key_reader.schema().as_ref(), keys)?;

    let mut touched: BTreeMap<Uuid, BTreeMap<usize, usize>> = BTreeMap::new();
    let mut tombstone_keys: Vec<Vec<u8>> = Vec::new();
    let mut live_count = 0usize;
    let mut row_offset = 0usize;
    {
        let mut lookup = tx.prepare("SELECT part, row FROM position WHERE key = ?")?;
        for batch in key_reader {
            scope.check()?;
            let batch = batch?;
            let encoded = key_codec.rows(&batch)?;
            let marks = if let Some(del_col) = deleted_column {
                let marks = cast(
                    batch
                        .column_by_name(del_col)
                        .context("missing deletion column")?,
                    &DataType::Boolean,
                )?;
                Some(marks)
            } else {
                None
            };
            let marks_bool = marks
                .as_ref()
                .map(|m| {
                    m.as_any()
                        .downcast_ref::<BooleanArray>()
                        .context("invalid deletion column")
                })
                .transpose()?;
            for (local_i, key) in encoded.iter().enumerate() {
                scope.check()?;
                let index = row_offset + local_i;
                let prev = lookup
                    .query_row([key.as_ref()], |row| {
                        Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, i64>(1)?))
                    })
                    .optional()?;
                let is_tombstone = marks_bool.is_some_and(|m| m.value(local_i));
                if let Some((ref part, row)) = prev {
                    memory.try_grow(48)?;
                    ensure!(
                        touched
                            .entry(Uuid::from_slice(part)?)
                            .or_default()
                            .insert(usize::try_from(row)?, index)
                            .is_none(),
                        "invalid duplicate position in local key index"
                    );
                    if is_tombstone {
                        same = false;
                    }
                } else if !is_tombstone {
                    same = false;
                }
                if is_tombstone {
                    if prev.is_some() {
                        memory.try_grow(key.as_ref().len() + std::mem::size_of::<Vec<u8>>())?;
                        tombstone_keys.push(key.as_ref().to_vec());
                    }
                } else {
                    live_count += 1;
                }
            }
            row_offset += batch.num_rows();
        }
    }

    // Only read full payload columns if watermark did not advance and every
    // incoming key matched an existing live row.
    if same && !touched.is_empty() {
        let full_memory = memory.new_empty();
        let mut batches = Vec::new();
        let mut total_bytes = 0usize;
        for batch in ParquetRecordBatchReaderBuilder::new_with_metadata(
            counted(file.reopen()?, scope),
            delta_meta.clone(),
        )
        .with_batch_size(scope.options.batch_size)
        .build()?
        {
            scope.check()?;
            let batch = batch?;
            let bytes = batch.get_array_memory_size();
            total_bytes = total_bytes.saturating_add(bytes);
            full_memory.try_grow(bytes)?;
            batches.push(batch);
        }
        full_memory.try_grow(total_bytes)?;
        let delta = datafusion::arrow::compute::concat_batches(delta_meta.schema(), &batches)?;
        drop(batches);
        full_memory.shrink(total_bytes);
        let encoded = Codec::new(delta.schema().as_ref(), keys)?.rows(&delta)?;
        full_memory.try_grow(encoded.size())?;
        for part in &parts {
            let Some(rows) = touched.get(&part.id) else {
                continue;
            };
            scope.check()?;
            let selected = points(part, rows)?;
            if same {
                let path = if first {
                    anchor.to_owned()
                } else {
                    part.path(anchor)
                };
                let reader = ParquetRecordBatchReaderBuilder::try_new_with_options(
                    counted(File::open(path)?, scope),
                    ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional),
                )?
                .with_row_selection(selected)
                .with_batch_size(scope.options.batch_size)
                .build()?;
                let mut positions = rows.values();
                let codec = Codec::new(reader.schema().as_ref(), keys)?;
                for batch in reader {
                    scope.check()?;
                    let batch = batch?;
                    let old_keys = codec.rows(&batch)?;
                    for (i, key) in old_keys.iter().enumerate() {
                        let j = *positions
                            .next()
                            .context("invalid local key index selection")?;
                        ensure!(
                            key.as_ref() == encoded.row(j).as_ref(),
                            "local key index position changed"
                        );
                        same &= equal_row(&batch, i, &delta, j);
                    }
                }
                ensure!(
                    positions.next().is_none(),
                    "incomplete local key index selection"
                );
            }
        }
    }

    let mut matched = 0;
    for part in &mut parts {
        let Some(rows) = touched.get(&part.id) else {
            continue;
        };
        scope.check()?;
        for &row in rows.keys() {
            ensure!(
                row < part.rows && part.deleted.binary_search(&row).is_err(),
                "local key index points to a nonvisible row"
            );
        }
        matched += rows.len();
        part.deleted.extend(rows.keys().copied());
        part.deleted.sort_unstable();
    }
    ensure!(
        matched == touched.values().map(BTreeMap::len).sum::<usize>(),
        "local key index references an unknown part"
    );
    if same {
        if let Some(previous) = previous {
            if rebuilt {
                set_generation(&tx, previous.generation)?;
                tx.commit()?;
            }
            return Ok(Some(previous.rows()));
        }
        return Ok(None);
    }
    drop(touched);
    let anchor_part = first.then(|| parts[0].id);

    // Pure soft-delete batch (0 surviving incoming rows): mark tombstones in
    // existing parts and advance the manifest watermark without rewriting base.
    if deleted_column.is_some() && live_count == 0 {
        let total_deleted: usize = parts.iter().map(|part| part.deleted.len()).sum();
        if total_deleted > MAX_DELETED {
            return Ok(None);
        }
        let prepared = Prepared {
            snapshot: Snapshot {
                generation: Uuid::new_v4(),
                base_parts,
                parts,
                pruning: None,
                indexed: Some(indexed(delta_meta.schema(), keys, column, value, run_id)?),
            },
            staged: Vec::new(),
            anchor_part,
        };
        let committed = overlay::publish(anchor, None, &prepared, scope, || {
            let mut remove = tx.prepare("DELETE FROM position WHERE key = ?")?;
            for key in &tombstone_keys {
                remove.execute([key.as_slice()])?;
            }
            drop(remove);
            set_generation(&tx, prepared.snapshot.generation)?;
            tx.commit()?;
            Ok(())
        })?;
        return Ok(committed.then(|| prepared.snapshot.rows()));
    }

    // If soft-delete tombstones exist alongside live upserts, filter them out
    // into a clean live delta file before passing to overlay::finish.
    let filtered_live: Option<(NamedTempFile, ArrowReaderMetadata)> =
        if let Some(del_col) = deleted_column {
            let live_file = staging_file(anchor.parent().context("local table directory")?)?;
            let mut writer = ArrowWriter::try_new(
                live_file.reopen()?,
                delta_meta.schema().clone(),
                Some(writer_properties()),
            )?;
            let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(
                counted(file.reopen()?, scope),
                delta_meta.clone(),
            )
            .with_batch_size(scope.options.batch_size)
            .build()?;
            for batch in reader {
                scope.check()?;
                let batch = batch?;
                let marks = cast(
                    batch
                        .column_by_name(del_col)
                        .context("missing deletion column")?,
                    &DataType::Boolean,
                )?;
                let marks = marks
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .context("invalid deletion column")?;
                let kept = filter_record_batch(&batch, &not(marks)?)?;
                if kept.num_rows() > 0 {
                    writer.write(&kept)?;
                    if writer.memory_size() > scope.options.memory_bytes / 8 {
                        writer.flush()?;
                    }
                }
            }
            writer.close()?;
            let meta = ArrowReaderMetadata::load(
                &counted(live_file.reopen()?, scope),
                ArrowReaderOptions::new(),
            )?;
            Some((live_file, meta))
        } else {
            None
        };
    let (append_file, append_meta) = match &filtered_live {
        Some((live_file, live_meta)) => (live_file, live_meta),
        None => (file, &delta_meta),
    };

    drop(memory);
    let Some(mut prepared) = overlay::finish(
        anchor,
        anchor_part,
        parts,
        base_parts,
        append_file.reopen()?,
        append_meta,
        None,
        scope,
    )?
    else {
        return Ok(None);
    };
    prepared.snapshot.indexed = Some(indexed(delta_meta.schema(), keys, column, value, run_id)?);
    for (_, file) in &prepared.staged {
        scope
            .reads
            .local_parquet_write(file.as_file().metadata()?.len());
    }
    let committed = overlay::publish(anchor, Some(append_file), &prepared, scope, || {
        let old_ids = anchor_part.into_iter().chain(
            previous
                .into_iter()
                .flat_map(|s| s.parts.iter().map(|p| p.id)),
        );
        for id in old_ids {
            if !prepared.snapshot.parts.iter().any(|p| p.id == id) {
                tx.execute(
                    "DELETE FROM position WHERE part = ?",
                    [id.as_bytes().as_slice()],
                )?;
            }
        }
        if !tombstone_keys.is_empty() {
            let mut remove = tx.prepare("DELETE FROM position WHERE key = ?")?;
            for key in &tombstone_keys {
                remove.execute([key.as_slice()])?;
            }
        }
        for (id, staged) in &prepared.staged {
            let part = prepared
                .snapshot
                .parts
                .iter()
                .find(|part| part.id == *id)
                .context("missing compacted local snapshot part")?;
            insert_file(&tx, part, counted(staged.reopen()?, scope), keys, scope)?;
        }
        let tail = prepared
            .snapshot
            .parts
            .last()
            .context("missing incoming local snapshot part")?;
        if !prepared.staged.iter().any(|(id, _)| *id == tail.id) {
            insert_file(
                &tx,
                tail,
                counted(append_file.reopen()?, scope),
                keys,
                scope,
            )?;
        }
        set_generation(&tx, prepared.snapshot.generation)?;
        tx.commit()?;
        Ok(())
    })?;
    Ok(committed.then(|| prepared.snapshot.rows()))
}

fn points(part: &Part, rows: &BTreeMap<usize, usize>) -> Result<RowSelection> {
    let mut pos = 0;
    let mut selectors = Vec::new();
    for &row in rows.keys() {
        ensure!(
            row < part.rows && part.deleted.binary_search(&row).is_err(),
            "local key index points to a nonvisible row"
        );
        if row > pos {
            selectors.push(RowSelector::skip(row - pos));
        }
        selectors.push(RowSelector::select(1));
        pos = row + 1;
    }
    if pos < part.rows {
        selectors.push(RowSelector::skip(part.rows - pos));
    }
    Ok(RowSelection::from(selectors))
}

fn equal_row(a: &RecordBatch, i: usize, b: &RecordBatch, j: usize) -> bool {
    a.columns().len() == b.columns().len()
        && a.columns()
            .iter()
            .zip(b.columns())
            .all(|(a, b)| a.to_data().slice(i, 1) == b.to_data().slice(j, 1))
}

struct Codec {
    indices: Vec<usize>,
    converter: RowConverter,
}

impl Codec {
    fn new(schema: &Schema, keys: &[String]) -> Result<Self> {
        let indices = keys
            .iter()
            .map(|name| schema.index_of(name))
            .collect::<Result<Vec<_>, _>>()?;
        let converter = RowConverter::new(
            indices
                .iter()
                .map(|index| SortField::new(schema.field(*index).data_type().clone()))
                .collect(),
        )?;
        Ok(Self { indices, converter })
    }

    fn rows(&self, batch: &RecordBatch) -> Result<Rows> {
        let keys = self
            .indices
            .iter()
            .map(|&index| batch.column(index).clone())
            .collect::<Vec<_>>();
        ensure!(
            keys.iter().all(|key| key.null_count() == 0),
            "主键不能包含空值"
        );
        Ok(self.converter.convert_columns(&keys)?)
    }
}

fn insert_file(
    tx: &Transaction,
    part: &Part,
    file: CountedFile,
    keys: &[String],
    scope: &QueryScope,
) -> Result<()> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    ensure!(
        usize::try_from(builder.metadata().file_metadata().num_rows())? == part.rows,
        "local snapshot part row count changed"
    );
    let roots = keys
        .iter()
        .map(|name| builder.schema().index_of(name))
        .collect::<Result<Vec<_>, _>>()?;
    let projection = ProjectionMask::roots(builder.parquet_schema(), roots);
    let reader = builder
        .with_projection(projection)
        .with_batch_size(scope.options.batch_size)
        .build()?;
    let codec = Codec::new(reader.schema().as_ref(), keys)?;
    let mut position = 0;
    let mut masked = part.deleted.iter().copied().peekable();
    let mut insert = tx.prepare(
        "INSERT INTO position (key, part, row) VALUES (?1, ?2, ?3)
         ON CONFLICT (key) DO UPDATE SET part = excluded.part, row = excluded.row",
    )?;
    for batch in reader {
        scope.check()?;
        for key in codec.rows(&batch?)?.iter() {
            if masked.peek() == Some(&position) {
                masked.next();
            } else {
                insert.execute(params![
                    key.as_ref(),
                    part.id.as_bytes(),
                    i64::try_from(position)?
                ])?;
            }
            position += 1;
        }
    }
    ensure!(
        position == part.rows,
        "local key index did not read all expected rows"
    );
    Ok(())
}

fn row_count(meta: &ArrowReaderMetadata) -> Result<usize> {
    Ok(usize::try_from(meta.metadata().file_metadata().num_rows())?)
}

struct CountedFile(File, Arc<ReadStats>);

fn counted(file: File, scope: &QueryScope) -> CountedFile {
    CountedFile(file, scope.reads.clone())
}

impl Length for CountedFile {
    fn len(&self) -> u64 {
        self.0.len()
    }
}

struct CountedRead<R>(R, Arc<ReadStats>);

impl<R: Read> Read for CountedRead<R> {
    fn read(&mut self, dest: &mut [u8]) -> io::Result<usize> {
        let bytes = self.0.read(dest)?;
        self.1.object_bytes(bytes);
        Ok(bytes)
    }
}

impl ChunkReader for CountedFile {
    type T = CountedRead<<File as ChunkReader>::T>;

    fn get_read(&self, offset: u64) -> ParquetResult<Self::T> {
        Ok(CountedRead(self.0.get_read(offset)?, self.1.clone()))
    }
    fn get_bytes(&self, offset: u64, len: usize) -> ParquetResult<Bytes> {
        let bytes = self.0.get_bytes(offset, len)?;
        self.1.object_bytes(bytes.len());
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests;
