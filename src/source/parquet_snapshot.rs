use std::{
    fs::File,
    io::{self, Read},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, ensure};
use datafusion::arrow::datatypes::Schema;
use datafusion_datasource::PartitionedFile;
use datafusion_datasource_parquet::ParquetRowSelection;
use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub(crate) const MAGIC: &[u8] = b"GLPQ1\n";
pub(crate) const MAX_DELTA_PARTS: usize = 15;
pub(crate) const MAX_BASE_PARTS: usize = 16;
pub(crate) const MAX_DELETED: usize = 65_536;
const MAX_MANIFEST: u64 = 2 * 1024 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Snapshot {
    pub generation: Uuid,
    #[serde(default = "one", skip_serializing_if = "is_one")]
    pub base_parts: usize,
    pub parts: Vec<Part>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pruning: Option<SnapshotPruning>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SnapshotPruning {
    pub key: String,
    pub schema: Schema,
}

fn one() -> usize {
    1
}
fn is_one(value: &usize) -> bool {
    *value == 1
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Part {
    pub id: Uuid,
    pub rows: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deleted: Vec<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_range: Option<[i64; 2]>,
}

pub(crate) fn managed_location(location: &str) -> Option<PathBuf> {
    let path = if location.starts_with("file://") {
        let url = url::Url::parse(location).ok()?;
        if !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return None;
        }
        url.to_file_path().ok()?
    } else if location.contains("://") {
        return None;
    } else {
        PathBuf::from(location)
    };
    if path.parent()?.file_name()? != ".graphlake-local" || path.extension()? != "parquet" {
        return None;
    }
    Uuid::parse_str(path.file_stem()?.to_str()?).ok()?;
    Some(path)
}

pub(crate) fn parts_dir(path: &Path) -> PathBuf {
    path.with_extension("parts")
}

impl Part {
    pub(crate) fn path(&self, anchor: &Path) -> PathBuf {
        parts_dir(anchor).join(format!("{}.parquet", self.id))
    }
}

impl Snapshot {
    pub(crate) fn read(path: &Path) -> Result<Option<Self>> {
        let mut file = match File::open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let mut header = [0; MAGIC.len()];
        match file.read_exact(&mut header) {
            Ok(()) if header == MAGIC => {}
            Ok(()) => return Ok(None),
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(error) => return Err(error.into()),
        }
        let mut body = Vec::new();
        file.take(MAX_MANIFEST + 1).read_to_end(&mut body)?;
        ensure!(
            body.len() as u64 <= MAX_MANIFEST,
            "local Parquet snapshot manifest is too large"
        );
        let snapshot: Self =
            serde_json::from_slice(&body).context("invalid local Parquet snapshot manifest")?;
        snapshot.validate()?;
        Ok(Some(snapshot))
    }

    fn validate(&self) -> Result<()> {
        let mut seen = std::collections::BTreeSet::new();
        ensure!(
            (1..=MAX_BASE_PARTS).contains(&self.base_parts)
                && self.parts.len() >= self.base_parts
                && self.parts.len() >= 2
                && self.parts.len() - self.base_parts <= MAX_DELTA_PARTS,
            "invalid local Parquet snapshot part count"
        );
        let mut deleted = 0;
        if let Some(pruning) = &self.pruning {
            ensure!(
                pruning
                    .schema
                    .field_with_name(&pruning.key)
                    .is_ok_and(|field| field.data_type().is_signed_integer()),
                "invalid local Parquet snapshot key"
            );
        }
        for part in &self.parts {
            ensure!(
                seen.insert(part.id)
                    && part.deleted.windows(2).all(|v| v[0] < v[1])
                    && part.deleted.last().is_none_or(|&row| row < part.rows)
                    && part.key_range.is_none_or(|[min, max]| min <= max),
                "invalid local Parquet snapshot part"
            );
            deleted += part.deleted.len();
        }
        ensure!(
            deleted <= MAX_DELETED
                && self
                    .parts
                    .last()
                    .is_some_and(|p| p.rows > 0 && p.deleted.is_empty()),
            "invalid local Parquet snapshot deletions"
        );
        Ok(())
    }

    #[cfg(feature = "admin")]
    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let mut body = MAGIC.to_vec();
        serde_json::to_writer(&mut body, self)?;
        if body.len() as u64 > MAX_MANIFEST && self.pruning.is_some() {
            let mut unindexed = self.clone();
            unindexed.pruning = None;
            for part in &mut unindexed.parts {
                part.key_range = None;
            }
            return unindexed.encode();
        }
        ensure!(
            body.len() as u64 <= MAX_MANIFEST,
            "local Parquet snapshot manifest is too large"
        );
        Ok(body)
    }

    #[cfg(feature = "admin")]
    pub(crate) fn rows(&self) -> u64 {
        self.parts
            .iter()
            .map(|p| (p.rows - p.deleted.len()) as u64)
            .sum()
    }
}

pub(crate) fn select_rows(
    file: PartitionedFile,
    rows: usize,
    deleted: &[usize],
) -> PartitionedFile {
    match row_selection(rows, deleted) {
        Some(selection) => file.with_extension(ParquetRowSelection::new(selection)),
        None => file,
    }
}

pub(crate) fn row_selection(rows: usize, deleted: &[usize]) -> Option<RowSelection> {
    if deleted.is_empty() {
        return None;
    }
    let mut selectors = Vec::with_capacity(deleted.len() * 2 + 1);
    let mut next = 0;
    for &row in deleted {
        if row > next {
            selectors.push(RowSelector::select(row - next));
        }
        selectors.push(RowSelector::skip(1));
        next = row + 1;
    }
    if next < rows {
        selectors.push(RowSelector::select(rows - next));
    }
    Some(RowSelection::from(selectors))
}
