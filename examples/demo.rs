use std::{path::PathBuf, sync::Arc};

use anyhow::{Context, Result};
use graphlake::arrow::array::{ArrayRef, Int64Array, RecordBatch};
use parquet::arrow::ArrowWriter;

fn main() -> Result<()> {
    let dir = PathBuf::from(
        std::env::args_os()
            .nth(1)
            .context("usage: demo OUTPUT_DIR")?,
    );
    std::fs::create_dir(&dir).context("choose a new output directory")?;
    let parquet = |name, columns: &[(&str, &[i64])]| -> Result<()> {
        let batch = RecordBatch::try_from_iter(columns.iter().map(|(name, values)| {
            (
                *name,
                Arc::new(Int64Array::from_iter_values(values.iter().copied())) as ArrayRef,
            )
        }))?;
        let file = std::fs::File::create(dir.join(name))?;
        let mut writer = ArrowWriter::try_new(file, batch.schema(), None)?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(())
    };
    parquet(
        "accounts.parquet",
        &[("tenant_id", &[1, 1, 1]), ("account_id", &[10, 20, 30])],
    )?;
    parquet(
        "transfers.parquet",
        &[
            ("tenant_id", &[1, 1, 1, 1]),
            ("transfer_id", &[1, 2, 3, 4]),
            ("from_account_id", &[10, 10, 20, 30]),
            ("to_account_id", &[20, 20, 30, 10]),
            ("amount", &[100, 200, 300, 400]),
        ],
    )?;
    std::fs::write(
        dir.join("graph.yaml"),
        "graph: finance
sources:
  lake:
    type: parquet
    tables:
      accounts: [accounts.parquet]
      transfers: [transfers.parquet]
nodes:
  Account:
    source: lake.accounts
    key: [tenant_id, account_id]
edges:
  Transfer:
    source: lake.transfers
    key: [tenant_id, transfer_id]
    from: {node: Account, columns: [tenant_id, from_account_id]}
    to: {node: Account, columns: [tenant_id, to_account_id]}
    properties: [amount]
",
    )?;
    Ok(())
}
