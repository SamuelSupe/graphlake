use std::{collections::BTreeMap, path::Path, time::Duration};

use anyhow::{Context, Result, ensure};
use uuid::Uuid;

use super::{Managed, Record, Selection, api_error};
use crate::admin::{cluster::control::Leadership, local_tables::store::Store};

pub(in crate::admin) struct Archive {
    pub root: tempfile::TempDir,
    _pin: Selection,
}

impl Managed {
    pub(in crate::admin) async fn backup(&self) -> Result<Option<Archive>> {
        let ids = self
            .records()
            .await?
            .into_iter()
            .map(|(r, _)| r.table.data_id)
            .collect();
        let pinned = self.pin(ids, 4 * 3600 + 120).await?;
        if pinned.tables.is_empty() {
            return Ok(None);
        }
        let root = tempfile::tempdir()?;
        let directory = root.path().join(".graphlake-local");
        tokio::fs::create_dir(&directory).await?;
        let materialize = async {
            let mut tables = BTreeMap::new();
            for (mut table, pin) in pinned.tables.iter().cloned() {
                if let Some(pin) = pin {
                    self.check_pin(&pin).await?;
                    let snapshot = self.snapshot(pin.table, &pin.head).await?;
                    self.download(
                        pin.table,
                        &snapshot,
                        &directory.join(format!("{}.parquet", pin.table)),
                    )
                    .await?;
                    table.archived_generation = Some(pin.head.generation);
                }
                tables.insert(format!("{}/{}", table.model, table.config.name), table);
            }
            tokio::fs::write(directory.join("state.json"), serde_json::to_vec(&tables)?).await?;
            anyhow::Ok(())
        };
        tokio::time::timeout(Duration::from_secs(4 * 3600), materialize).await??;
        Ok(Some(Archive { root, _pin: pinned }))
    }

    pub(in crate::admin) async fn restore_archive(
        &self,
        target: &Path,
        leader: &Leadership,
    ) -> Result<()> {
        let directory = target.join(".graphlake-local");
        let tables = Store::parse(&Store::state_file(&directory)?)?;
        for table in tables.into_values().filter(|t| t.managed) {
            let id = table.data_id;
            let path = directory.join(format!("{id}.parquet"));
            let archived_generation = table.archived_generation;
            if self.record(id).await?.is_none() {
                let state = Record {
                    table: table.clone(),
                    head: None,
                    retired: Vec::new(),
                    pending: None,
                };
                ensure!(
                    self.save(&state, 0, leader).await?,
                    "managed restore slot changed"
                );
            }
            let (mut record, rev) = self
                .record(id)
                .await?
                .context("managed restore lost table record")?;
            ensure!(
                serde_json::to_vec(&record.table)? == serde_json::to_vec(&table)?,
                "managed restore already includes a different table definition"
            );
            if let Some(head) = &record.head {
                ensure!(
                    Some(head.generation) == archived_generation,
                    "managed restore table generation changed"
                );
                self.snapshot(id, head).await?;
                continue;
            }
            if !path.is_file() {
                continue;
            }
            ensure!(
                archived_generation.is_some(),
                "managed backup does not record its generation"
            );
            let head = self
                .upload(id, Uuid::new_v4(), &path, None, archived_generation)
                .await?
                .context("managed restore did not produce a generation")?;
            ensure!(
                Some(head.generation) == archived_generation,
                "managed backup generation disagrees with its manifest"
            );
            record.head = Some(head);
            leader.confirm().await.map_err(api_error)?;
            ensure!(
                self.save(&record, rev, leader).await?,
                "managed restore pointer changed"
            );
        }
        Ok(())
    }
}
