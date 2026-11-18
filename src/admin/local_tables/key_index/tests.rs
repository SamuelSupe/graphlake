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
    let result = overlay::publish(&anchor, &file, &interrupted, &current, || {
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
