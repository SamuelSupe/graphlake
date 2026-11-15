use std::{
    collections::BTreeMap,
    fs::{self, File},
    path::PathBuf,
};

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::super::{ApiResult, Error, models};
use crate::Source;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(in crate::admin) enum Mode {
    Full,
    Incremental,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(in crate::admin) struct Config {
    pub name: String,
    pub source_table: String,
    pub columns: Vec<String>,
    #[serde(default)]
    pub key_columns: Vec<String>,
    pub watermark_column: Option<String>,
    pub mode: Mode,
    pub interval_seconds: Option<u64>,
    #[serde(default = "default_timeout")]
    pub timeout_seconds: u64,
}
fn default_timeout() -> u64 {
    3600
}

impl Config {
    pub fn validate(&self) -> ApiResult<()> {
        let unique = |values: &[String]| {
            values
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                == values.len()
        };
        if !models::valid_id(&self.name)
            || self.name.len() > 48
            || self.columns.is_empty()
            || self.columns.len() > 512
            || !unique(&self.columns)
            || self
                .columns
                .iter()
                .any(|v| v.is_empty() || v.len() > 256 || v.contains('\0'))
            || !unique(&self.key_columns)
            || self.key_columns.iter().any(|v| !self.columns.contains(v))
            || self
                .watermark_column
                .as_ref()
                .is_some_and(|v| !self.columns.contains(v))
            || self
                .interval_seconds
                .is_some_and(|s| !(30..=2_592_000).contains(&s))
            || !(1..=43_200).contains(&self.timeout_seconds)
        {
            return Err(Error::invalid(
                "invalid_local_table",
                "检查表名（1–48 位字母、数字、下划线或连字符）、字段、刷新间隔（30–2592000 秒）及超时（1–43200 秒）",
            ));
        }
        if self.mode == Mode::Incremental
            && (self.key_columns.is_empty() || self.watermark_column.is_none())
        {
            return Err(Error::invalid(
                "incremental_key_required",
                "增量更新需要主键和增量字段",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Deserialize, Serialize)]
pub(in crate::admin) struct Run {
    pub id: Uuid,
    pub mode: Mode,
    pub trigger: String,
    pub status: String,
    pub started_at_ms: u64,
    pub finished_at_ms: Option<u64>,
    pub loaded_rows: Option<u64>,
    pub total_rows: Option<u64>,
    pub error: Option<String>,
}

#[derive(Clone, Deserialize, Serialize)]
pub(super) struct Table {
    pub model: String,
    pub config: Config,
    pub source: Source,
    pub data_id: Uuid,
    pub revision: Uuid,
    pub next_run_at_ms: Option<u64>,
    pub history: Vec<Run>,
}

#[derive(Serialize)]
pub(in crate::admin) struct View {
    pub model: String,
    #[serde(flatten)]
    pub config: Config,
    pub revision: Uuid,
    pub path: String,
    pub ready: bool,
    pub next_run_at_ms: Option<u64>,
    pub history: Vec<Run>,
}

pub(super) struct Store {
    pub root: PathBuf,
    pub tables: BTreeMap<String, Table>,
    _lease: File,
}

pub(super) fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

impl Store {
    pub fn open(catalog: PathBuf) -> Result<Self> {
        let root = catalog.join(".graphlake-local");
        fs::create_dir_all(&root)?;
        let lease = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(root.join("lock"))?;
        lease
            .try_lock()
            .context("another service owns local-table storage")?;
        let mut tables: BTreeMap<String, Table> =
            match models::read_file(&root.join("state.json"), 16 * 1024 * 1024)
                .map_err(|e| anyhow::anyhow!(e.message))?
            {
                Some(json) => serde_json::from_str(&json)?,
                None => BTreeMap::new(),
            };
        ensure!(tables.len() <= 256, "too many local tables");
        for (key, table) in &mut tables {
            table
                .config
                .validate()
                .map_err(|e| anyhow::anyhow!(e.message))?;
            ensure!(
                models::valid_id(&table.model)
                    && *key == format!("{}/{}", table.model, table.config.name),
                "invalid local table identity"
            );
            let interrupted = table.history.iter().any(|run| run.status == "running");
            if interrupted {
                table.next_run_at_ms = table
                    .config
                    .interval_seconds
                    .map(|seconds| now() + seconds * 1000);
            }
            for run in &mut table.history {
                if run.status == "running" {
                    run.status = "interrupted".into();
                    run.finished_at_ms = Some(now());
                    run.error = Some("服务重启中断了任务；可重新加载，已提交的数据保留".into());
                }
            }
        }
        for entry in fs::read_dir(&root)? {
            let entry = entry?;
            if entry.file_type()?.is_file()
                && entry.file_name().to_string_lossy().starts_with(".sync-")
            {
                fs::remove_file(entry.path())?;
            }
        }
        let store = Self {
            root,
            tables,
            _lease: lease,
        };
        for table in store.tables.values() {
            super::overlay::clean(&store.path(table))
                .context("recovering local Parquet snapshot parts")?;
        }
        store
            .persist(&store.tables)
            .map_err(|e| anyhow::anyhow!(e.message))?;
        Ok(store)
    }

    pub fn path(&self, table: &Table) -> PathBuf {
        self.root.join(format!("{}.parquet", table.data_id))
    }
    pub fn view(&self, table: &Table) -> View {
        let path = self.path(table);
        View {
            model: table.model.clone(),
            config: table.config.clone(),
            revision: table.revision,
            ready: path.is_file(),
            path: path.to_string_lossy().into_owned(),
            next_run_at_ms: table.next_run_at_ms,
            history: table.history.clone(),
        }
    }
    fn persist(&self, tables: &BTreeMap<String, Table>) -> ApiResult<()> {
        let bytes = serde_json::to_vec(tables).map_err(|_| Error::storage())?;
        if bytes.len() > 16 * 1024 * 1024 {
            return Err(Error::storage());
        }
        models::atomic_write(&self.root.join("state.json"), &bytes, false)
    }
    pub fn put(&mut self, key: String, table: Table) -> ApiResult<()> {
        let mut next = self.tables.clone();
        next.insert(key, table);
        self.persist(&next)?;
        self.tables = next;
        Ok(())
    }
}
