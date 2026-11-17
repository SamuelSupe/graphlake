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
    load::writer_properties,
    overlay::{self, Prepared},
    store::now,
};
use crate::{
    ScalarValue,
    engine::QueryScope,
    source::{
        ReadStats,
        parquet_snapshot::{IndexedSnapshot, Part, Snapshot, row_selection},
    },
};

use datafusion_proto::protobuf::ScalarValue as WireScalar;
use prost::Message;

mod storage;
use storage::{generation, open, set_generation};

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
    tx.execute("DELETE FROM position", [])?;
    insert_file(&tx, &part, data, keys, scope)?;
    let staged = Prepared {
        snapshot,
        staged: Vec::new(),
    };
    let written = overlay::publish(anchor, file, false, &staged, scope, || {
        set_generation(&tx, staged.snapshot.generation)?;
        tx.commit()?;
        Ok(())
    })?;
    ensure!(written, "could not publish indexed local snapshot");
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
    let scratch = tempfile::Builder::new()
        .prefix(".sync-")
        .tempfile_in(anchor.parent().context("local table directory")?)?;
    let mut index = rusqlite::Connection::open(scratch.path())?;
    let cache_kib = (scope.options.memory_bytes / 8192).clamp(32, 8192);
    let batch_size = scope.options.batch_size.min(128);
    index.pragma_update(None, "cache_size", -(cache_kib as i64))?;
    index.execute_batch(
        "PRAGMA journal_mode=OFF;
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
    let mut writer =
        ArrowWriter::try_new(output, metadata.schema().clone(), Some(writer_properties()))?;
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
pub(super) fn incremental(
    anchor: &Path,
    managed: bool,
    previous: Option<&Snapshot>,
    file: &NamedTempFile,
    keys: &[String],
    column: &str,
    value: &ScalarValue,
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
    if old.is_none() || cached != old {
        tx.execute("DELETE FROM position", [])?;
        for part in &parts {
            let path = if first {
                anchor.to_owned()
            } else {
                part.path(anchor)
            };
            insert_file(&tx, part, counted(File::open(path)?, scope), keys, scope)?;
        }
    }

    let memory = datafusion::execution::memory_pool::MemoryConsumer::new("GraphLake indexed delta")
        .register(&scope.runtime.memory_pool);
    let mut batches = Vec::new();
    for batch in ParquetRecordBatchReaderBuilder::new_with_metadata(data, delta_meta.clone())
        .with_batch_size(scope.options.batch_size)
        .build()?
    {
        scope.check()?;
        let batch = batch?;
        memory.try_grow(batch.get_array_memory_size())?;
        batches.push(batch);
    }
    let delta = datafusion::arrow::compute::concat_batches(delta_meta.schema(), &batches)?;
    memory.try_grow(delta.get_array_memory_size())?;
    drop(batches);
    let encoded = Codec::new(delta.schema().as_ref(), keys)?.rows(&delta)?;
    let mut touched: BTreeMap<Uuid, BTreeMap<usize, usize>> = BTreeMap::new();
    let mut same = previous
        .and_then(|snapshot| snapshot.indexed.as_ref())
        .map(checkpoint)
        .transpose()?
        .is_none_or(|bound| &bound == value);
    {
        let mut lookup = tx.prepare("SELECT part, row FROM position WHERE key = ?")?;
        for (index, key) in encoded.iter().enumerate() {
            scope.check()?;
            let previous = lookup
                .query_row([key.as_ref()], |row| {
                    Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, i64>(1)?))
                })
                .optional()?;
            if let Some((part, row)) = previous {
                ensure!(
                    touched
                        .entry(Uuid::from_slice(&part)?)
                        .or_default()
                        .insert(usize::try_from(row)?, index)
                        .is_none(),
                    "invalid duplicate position in local key index"
                );
            } else {
                same = false;
            }
        }
    }
    let mut matched = 0;
    for part in &mut parts {
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
            set_generation(&tx, previous.generation)?;
            tx.commit()?;
            return Ok(Some(previous.rows()));
        }
        return Ok(None);
    }
    drop(encoded);
    drop(delta);
    drop(memory);
    let Some(mut prepared) = overlay::finish(
        anchor,
        parts,
        base_parts,
        file.reopen()?,
        &delta_meta,
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
    let committed = overlay::publish(anchor, file, first, &prepared, scope, || {
        if let Some(previous) = previous {
            for part in &previous.parts {
                if !prepared.snapshot.parts.iter().any(|p| p.id == part.id) {
                    tx.execute(
                        "DELETE FROM position WHERE part = ?",
                        [part.id.as_bytes().as_slice()],
                    )?;
                }
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
            insert_file(&tx, tail, counted(file.reopen()?, scope), keys, scope)?;
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
