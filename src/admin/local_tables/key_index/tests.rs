use super::*;
use crate::{
    EngineLimits, QueryOptions,
    engine::{Admission, QueryScope},
};
use datafusion::arrow::array::{Int64Array, StringArray};
use parquet::arrow::ArrowWriter;
use std::fs;

fn stage(root: &Path, version: i64) -> Result<NamedTempFile> {
    let staged = super::super::load::staging_file(root)?;
    let batch = RecordBatch::try_from_iter([
        ("id", Arc::new(StringArray::from(vec!["uuid-1"])) as _),
        ("version", Arc::new(Int64Array::from(vec![version])) as _),
    ])?;
    let mut writer = ArrowWriter::try_new(staged.reopen()?, batch.schema(), None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(staged)
}

#[tokio::test]
async fn index_ahead_of_manifest_is_rebuilt_before_retry() -> Result<()> {
    let root = tempfile::tempdir()?;
    let anchor = root.path().join("copy.parquet");
    let index = anchor.with_extension("keys.sqlite");
    let keys = vec!["id".to_owned()];
    let old_value = ScalarValue::Int64(Some(5));
    let next_value = ScalarValue::Int64(Some(6));
    let admission = Arc::new(Admission::new(EngineLimits::default())?);
    let current = QueryScope::new(QueryOptions::default(), &admission).await?;
    full(
        &anchor,
        true,
        &stage(root.path(), 5)?,
        &keys,
        "version",
        &old_value,
        Uuid::new_v4(),
        &current,
    )?;
    assert!(index.is_file());
    let old = Snapshot::read(&anchor)?.context("missing committed snapshot")?;
    let mut snapshot = old.clone();
    snapshot.generation = Uuid::new_v4();
    snapshot.parts[0].id = Uuid::new_v4();
    snapshot.indexed.as_mut().unwrap().checkpoint =
        WireScalar::try_from(&next_value)?.encode_to_vec();
    let interrupted = Prepared {
        snapshot,
        staged: Vec::new(),
        anchor_part: None,
    };
    let file = stage(root.path(), 6)?;
    let mut conn = open(&anchor, true, &current)?;
    let tx = conn.transaction()?;
    tx.execute("DELETE FROM position", [])?;
    insert_file(
        &tx,
        &interrupted.snapshot.parts[0],
        counted(file.reopen()?, &current),
        &keys,
        &current,
    )?;
    let result = overlay::publish(&anchor, Some(&file), &interrupted, &current, || {
        set_generation(&tx, interrupted.snapshot.generation)?;
        tx.commit()?;
        current.options.cancellation.cancel();
        Ok(())
    });
    assert!(result.is_err());
    assert_eq!(fs::read(&anchor)?, old.encode()?);
    let ahead: String = conn.query_row("SELECT value FROM generation", [], |row| row.get(0))?;
    assert_eq!(ahead, generation(interrupted.snapshot.generation));
    drop(conn);

    let retry = QueryScope::new(QueryOptions::default(), &admission).await?;
    assert_eq!(
        incremental(
            &anchor,
            true,
            Some(&old),
            &file,
            &keys,
            "version",
            &next_value,
            None,
            Uuid::new_v4(),
            &retry
        )?,
        Some(1)
    );
    let current = Snapshot::read(&anchor)?.unwrap();
    assert_eq!(checkpoint(current.indexed.as_ref().unwrap())?, next_value);
    assert_eq!(current.parts[0].id, old.parts[0].id);
    assert_eq!(current.parts[0].deleted, [0]);
    overlay::clean(&anchor)?;
    assert!(!interrupted.snapshot.parts[0].path(&anchor).exists());
    drop(file);
    root.close()?;
    assert!(!index.exists());
    Ok(())
}

#[tokio::test]
async fn first_incremental_load_reclaims_the_single_file_anchor() -> Result<()> {
    use crate::source::parquet_snapshot::MAX_DELETED;

    let root = tempfile::tempdir()?;
    let anchor = root.path().join("copy.parquet");
    let keys = vec!["id".to_owned()];
    let admission = Arc::new(Admission::new(EngineLimits::default())?);
    let scope = QueryScope::new(QueryOptions::default(), &admission).await?;
    let count = MAX_DELETED + 1;
    let write = |file: &NamedTempFile, version, rows| -> Result<()> {
        let batch = RecordBatch::try_from_iter([
            ("id", Arc::new(Int64Array::from_iter_values(0..rows)) as _),
            (
                "version",
                Arc::new(Int64Array::from_value(version, rows as usize)) as _,
            ),
        ])?;
        let mut writer = ArrowWriter::try_new(file.reopen()?, batch.schema(), None)?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(())
    };
    let base = super::super::load::staging_file(root.path())?;
    write(&base, 5, count as i64 + 1)?;
    base.persist(&anchor)?;
    let delta = super::super::load::staging_file(root.path())?;
    write(&delta, 6, count as i64)?;
    let watermark = ScalarValue::Int64(Some(6));
    assert_eq!(
        incremental(
            &anchor,
            true,
            None,
            &delta,
            &keys,
            "version",
            &watermark,
            None,
            Uuid::new_v4(),
            &scope
        )?,
        Some(count as u64 + 1)
    );
    let snapshot = Snapshot::read(&anchor)?.context("missing committed snapshot")?;
    assert_eq!(snapshot.parts[0].rows, 1);
    assert_eq!(checkpoint(snapshot.indexed.as_ref().unwrap())?, watermark);
    let data =
        ParquetRecordBatchReaderBuilder::try_new(File::open(snapshot.parts[0].path(&anchor))?)?
            .build()?
            .next()
            .context("missing retained row")??;
    assert_eq!(
        ScalarValue::try_from_array(data.column(0), 0)?,
        ScalarValue::Int64(Some(count as i64))
    );
    Ok(())
}

#[tokio::test]
async fn compact_preserves_historical_nulls_when_incoming_schema_is_non_nullable() -> Result<()> {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    let root = tempfile::tempdir()?;
    let anchor = root.path().join("copy.parquet");
    let keys = vec!["id".to_owned()];
    let admission = Arc::new(Admission::new(EngineLimits::default())?);
    let scope = QueryScope::new(QueryOptions::default(), &admission).await?;

    // Base part has nullable payload column containing a NULL on id=1.
    let nullable_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("version", DataType::Int64, false),
        Field::new("note", DataType::Utf8, true),
    ]));
    let base_batch = RecordBatch::try_new(
        nullable_schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![1, 1])),
            Arc::new(StringArray::from(vec![None, Some("ok")])),
        ],
    )?;
    let base = super::super::load::staging_file(root.path())?;
    let mut writer = ArrowWriter::try_new(base.reopen()?, base_batch.schema(), None)?;
    writer.write(&base_batch)?;
    writer.close()?;

    full(
        &anchor,
        true,
        &base,
        &keys,
        "version",
        &ScalarValue::Int64(Some(1)),
        Uuid::new_v4(),
        &scope,
    )?;

    // Incoming delta tightens `note` to `nullable = false` and updates only id=2.
    let non_null_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("version", DataType::Int64, false),
        Field::new("note", DataType::Utf8, false),
    ]));
    let delta_batch = RecordBatch::try_new(
        non_null_schema,
        vec![
            Arc::new(Int64Array::from(vec![2])),
            Arc::new(Int64Array::from(vec![2])),
            Arc::new(StringArray::from(vec!["updated"])),
        ],
    )?;
    let delta = super::super::load::staging_file(root.path())?;
    let mut writer = ArrowWriter::try_new(delta.reopen()?, delta_batch.schema(), None)?;
    writer.write(&delta_batch)?;
    writer.close()?;

    let existing = Snapshot::read(&anchor)?.context("missing base snapshot")?;
    let out_file = super::super::load::staging_file(root.path())?;
    let rows = compact(
        &anchor,
        Some(&existing),
        &delta,
        out_file.reopen()?,
        &keys,
        None,
        &scope,
    )?;
    assert_eq!(rows, 2);

    let mut by_id = std::collections::BTreeMap::new();
    for batch in ParquetRecordBatchReaderBuilder::try_new(out_file.reopen()?)?.build()? {
        let batch = batch?;
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let notes = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            by_id.insert(
                ids.value(row),
                (!notes.is_null(row)).then(|| notes.value(row).to_owned()),
            );
        }
    }
    assert_eq!(by_id.get(&1), Some(&None));
    assert_eq!(by_id.get(&2), Some(&Some("updated".to_owned())));
    Ok(())
}

#[tokio::test]
async fn incremental_applies_soft_deletes_without_full_rewrite() -> Result<()> {
    use datafusion::arrow::array::BooleanArray;

    let root = tempfile::tempdir()?;
    let anchor = root.path().join("copy.parquet");
    let keys = vec!["id".to_owned()];
    let admission = Arc::new(Admission::new(EngineLimits::default())?);
    let scope = QueryScope::new(QueryOptions::default(), &admission).await?;

    let base_batch = RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![1, 2, 3])) as _),
        ("version", Arc::new(Int64Array::from(vec![1, 1, 1])) as _),
        (
            "name",
            Arc::new(StringArray::from(vec!["a", "b", "c"])) as _,
        ),
        (
            "is_deleted",
            Arc::new(BooleanArray::from(vec![
                Some(false),
                Some(false),
                Some(false),
            ])) as _,
        ),
    ])?;
    let base = super::super::load::staging_file(root.path())?;
    let mut writer = ArrowWriter::try_new(base.reopen()?, base_batch.schema(), None)?;
    writer.write(&base_batch)?;
    writer.close()?;

    full(
        &anchor,
        true,
        &base,
        &keys,
        "version",
        &ScalarValue::Int64(Some(1)),
        Uuid::new_v4(),
        &scope,
    )?;
    let before = Snapshot::read(&anchor)?.context("missing initial snapshot")?;
    let base_part_id = before.parts[0].id;

    // Delta soft-deletes id=2 and updates id=3 while preserving base part unchanged on disk.
    let delta_batch = RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![2, 3])) as _),
        ("version", Arc::new(Int64Array::from(vec![2, 2])) as _),
        (
            "name",
            Arc::new(StringArray::from(vec!["b-deleted", "c-new"])) as _,
        ),
        (
            "is_deleted",
            Arc::new(BooleanArray::from(vec![Some(true), Some(false)])) as _,
        ),
    ])?;
    let delta = super::super::load::staging_file(root.path())?;
    let mut writer = ArrowWriter::try_new(delta.reopen()?, delta_batch.schema(), None)?;
    writer.write(&delta_batch)?;
    writer.close()?;

    let live = incremental(
        &anchor,
        true,
        Some(&before),
        &delta,
        &keys,
        "version",
        &ScalarValue::Int64(Some(2)),
        Some("is_deleted"),
        Uuid::new_v4(),
        &scope,
    )?;
    assert_eq!(live, Some(2));
    let after = Snapshot::read(&anchor)?.context("missing updated snapshot")?;
    assert_eq!(
        after.parts[0].id, base_part_id,
        "base part must not be rewritten"
    );
    assert_eq!(after.parts[0].deleted, vec![1, 2]);
    assert_eq!(after.parts.len(), 2);
    assert_eq!(
        after.parts[1].rows, 1,
        "tombstone row should be filtered out of new delta part"
    );
    assert!(after.parts[1].deleted.is_empty());
    let live_batch =
        ParquetRecordBatchReaderBuilder::try_new(File::open(after.parts[1].path(&anchor))?)?
            .build()?
            .next()
            .context("missing live delta row")??;
    assert_eq!(
        live_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        3
    );
    assert_eq!(
        live_batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "c-new"
    );

    // Re-syncing the exact same boundary batch (already-deleted tombstone + identical live row)
    // must be a no-op that preserves the existing generation.
    let noop = incremental(
        &anchor,
        true,
        Some(&after),
        &delta,
        &keys,
        "version",
        &ScalarValue::Int64(Some(2)),
        Some("is_deleted"),
        Uuid::new_v4(),
        &scope,
    )?;
    assert_eq!(noop, Some(2));
    assert_eq!(
        Snapshot::read(&anchor)?.unwrap().generation,
        after.generation,
        "unchanged boundary re-sync with already-deleted tombstone must be a no-op"
    );

    // Pure tombstone batch deleting id=1 should also succeed incrementally without appending a part.
    let pure_delete_batch = RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![1])) as _),
        ("version", Arc::new(Int64Array::from(vec![3])) as _),
        ("name", Arc::new(StringArray::from(vec!["a-deleted"])) as _),
        (
            "is_deleted",
            Arc::new(BooleanArray::from(vec![Some(true)])) as _,
        ),
    ])?;
    let pure_delete = super::super::load::staging_file(root.path())?;
    let mut writer = ArrowWriter::try_new(pure_delete.reopen()?, pure_delete_batch.schema(), None)?;
    writer.write(&pure_delete_batch)?;
    writer.close()?;

    let live2 = incremental(
        &anchor,
        true,
        Some(&after),
        &pure_delete,
        &keys,
        "version",
        &ScalarValue::Int64(Some(3)),
        Some("is_deleted"),
        Uuid::new_v4(),
        &scope,
    )?;
    assert_eq!(live2, Some(1));
    let after2 = Snapshot::read(&anchor)?.context("missing second snapshot")?;
    assert_eq!(after2.parts[0].id, base_part_id);
    assert_eq!(after2.parts[0].deleted, vec![0, 1, 2]);
    assert_eq!(
        after2.parts.len(),
        2,
        "pure tombstone batch should not add a delta part"
    );
    Ok(())
}

#[tokio::test]
async fn per_generation_reader_pin_allows_cleaning_older_generations() -> Result<()> {
    use crate::source::parquet_snapshot;

    let root = tempfile::tempdir()?;
    let anchor = root.path().join("copy.parquet");
    let keys = vec!["id".to_owned()];
    let admission = Arc::new(Admission::new(EngineLimits::default())?);
    let scope = QueryScope::new(QueryOptions::default(), &admission).await?;

    full(
        &anchor,
        true,
        &stage(root.path(), 1)?,
        &keys,
        "version",
        &ScalarValue::Int64(Some(1)),
        Uuid::new_v4(),
        &scope,
    )?;
    let gen1 = Snapshot::read(&anchor)?.context("missing gen1")?;

    full(
        &anchor,
        true,
        &stage(root.path(), 2)?,
        &keys,
        "version",
        &ScalarValue::Int64(Some(2)),
        Uuid::new_v4(),
        &scope,
    )?;
    let gen2 = Snapshot::read(&anchor)?.context("missing gen2")?;
    let pin2 = parquet_snapshot::pin_generation(&anchor, gen2.generation)?
        .context("missing per-generation lock for gen2")?;

    full(
        &anchor,
        true,
        &stage(root.path(), 3)?,
        &keys,
        "version",
        &ScalarValue::Int64(Some(3)),
        Uuid::new_v4(),
        &scope,
    )?;
    let gen3 = Snapshot::read(&anchor)?.context("missing gen3")?;

    // With gen2 pinned, gen1 is unpinned and must be reclaimed while gen2 and gen3 remain.
    overlay::clean(&anchor)?;
    assert!(
        !gen1.parts[0].path(&anchor).exists(),
        "unpinned gen1 part should be cleaned"
    );
    assert!(
        gen2.parts[0].path(&anchor).exists(),
        "pinned gen2 part must survive cleanup"
    );
    assert!(
        gen3.parts[0].path(&anchor).exists(),
        "current gen3 part must survive cleanup"
    );

    drop(pin2);
    overlay::clean(&anchor)?;
    assert!(
        !gen2.parts[0].path(&anchor).exists(),
        "gen2 part should be cleaned once unpinned"
    );
    assert!(gen3.parts[0].path(&anchor).exists());
    Ok(())
}
