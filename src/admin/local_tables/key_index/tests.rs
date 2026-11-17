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
    let result = overlay::publish(&anchor, &file, false, &interrupted, &current, || {
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
