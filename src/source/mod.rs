pub(crate) mod delta;
pub(crate) mod iceberg;
pub(crate) mod mysql;
pub(crate) mod parquet;
pub(crate) mod parquet_snapshot;
pub(crate) mod postgres;
pub(crate) mod snapshot;
pub(crate) mod warehouse;

use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result, ensure};
use datafusion::arrow::array::RecordBatch;
use serde::Serialize;

#[derive(Clone, Debug, Serialize)]
pub struct SourceVersion {
    pub source: String,
    pub version: String,
    pub consistency: &'static str,
}

#[derive(Debug, Default)]
pub(crate) struct ReadStats {
    requests: AtomicU64,
    object_bytes: AtomicU64,
    postgres_rows: AtomicU64,
    postgres_arrow_bytes: AtomicU64,
    warehouse_rows: AtomicU64,
    warehouse_arrow_bytes: AtomicU64,
    snapshot_spilled_bytes: AtomicU64,
    mysql_rows: AtomicU64,
    mysql_arrow_bytes: AtomicU64,
    iceberg_rows: AtomicU64,
    iceberg_arrow_bytes: AtomicU64,
    frontier_requests: AtomicU64,
    frontier_scan_fallbacks: AtomicU64,
}

#[derive(Debug, Serialize)]
pub struct ReadMetrics {
    pub requests: u64,
    pub object_bytes: u64,
    pub postgres_rows: u64,
    pub postgres_arrow_bytes: u64,
    pub warehouse_rows: u64,
    pub warehouse_arrow_bytes: u64,
    pub snapshot_spilled_bytes: u64,
    pub mysql_rows: u64,
    pub mysql_arrow_bytes: u64,
    pub iceberg_rows: u64,
    pub iceberg_arrow_bytes: u64,
    pub frontier_requests: u64,
    pub frontier_scan_fallbacks: u64,
}

impl ReadStats {
    pub(crate) fn snapshot_spill(&self, bytes: u64) {
        self.snapshot_spilled_bytes
            .fetch_add(bytes, Ordering::Relaxed);
    }
    pub(crate) fn request(&self) {
        self.requests.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn object_bytes(&self, bytes: usize) {
        self.object_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub(crate) fn batch(&self, batch: &RecordBatch) {
        self.postgres_rows
            .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
        self.postgres_arrow_bytes
            .fetch_add(batch.get_array_memory_size() as u64, Ordering::Relaxed);
    }

    pub(crate) fn warehouse_batch(&self, batch: &RecordBatch) {
        self.warehouse_rows
            .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
        self.warehouse_arrow_bytes
            .fetch_add(batch.get_array_memory_size() as u64, Ordering::Relaxed);
    }

    pub(crate) fn mysql_batch(&self, batch: &RecordBatch) {
        self.mysql_rows
            .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
        self.mysql_arrow_bytes
            .fetch_add(batch.get_array_memory_size() as u64, Ordering::Relaxed);
    }

    pub(crate) fn iceberg_batch(&self, batch: &RecordBatch) {
        self.iceberg_rows
            .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
        self.iceberg_arrow_bytes
            .fetch_add(batch.get_array_memory_size() as u64, Ordering::Relaxed);
    }

    pub(crate) fn frontier_request(&self) {
        self.frontier_requests.fetch_add(1, Ordering::Relaxed);
        self.request();
    }

    pub(crate) fn frontier_scan_fallback(&self) {
        self.frontier_scan_fallbacks.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn metrics(&self) -> ReadMetrics {
        ReadMetrics {
            requests: self.requests.load(Ordering::Relaxed),
            object_bytes: self.object_bytes.load(Ordering::Relaxed),
            postgres_rows: self.postgres_rows.load(Ordering::Relaxed),
            postgres_arrow_bytes: self.postgres_arrow_bytes.load(Ordering::Relaxed),
            warehouse_rows: self.warehouse_rows.load(Ordering::Relaxed),
            warehouse_arrow_bytes: self.warehouse_arrow_bytes.load(Ordering::Relaxed),
            snapshot_spilled_bytes: self.snapshot_spilled_bytes.load(Ordering::Relaxed),
            mysql_rows: self.mysql_rows.load(Ordering::Relaxed),
            mysql_arrow_bytes: self.mysql_arrow_bytes.load(Ordering::Relaxed),
            iceberg_rows: self.iceberg_rows.load(Ordering::Relaxed),
            iceberg_arrow_bytes: self.iceberg_arrow_bytes.load(Ordering::Relaxed),
            frontier_requests: self.frontier_requests.load(Ordering::Relaxed),
            frontier_scan_fallbacks: self.frontier_scan_fallbacks.load(Ordering::Relaxed),
        }
    }
}

fn decimal128(value: &str, scale: i8) -> Result<i128> {
    let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));
    ensure!(fraction.len() <= scale as usize, "unexpected decimal scale");
    let scaled = format!(
        "{whole}{fraction}{:0<width$}",
        "",
        width = scale as usize - fraction.len()
    );
    scaled
        .parse::<i128>()
        .context("value cannot be represented as Decimal128")
}
