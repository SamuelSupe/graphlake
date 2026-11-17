pub(crate) mod delta;
pub(crate) mod direct;
pub(crate) mod iceberg;
pub(crate) mod mysql;
pub(crate) mod parquet;
pub(crate) mod parquet_snapshot;
pub(crate) mod postgres;
pub(crate) mod snapshot;
pub(crate) mod sqlite;
pub(crate) mod storage;
pub(crate) mod text;
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

// Preserve SourceVersion's public string interface and the on-disk labels.
macro_rules! source_consistency {
    ($($variant:ident => $label:literal),+ $(,)?) => {
        pub(crate) enum SourceConsistency {
            $($variant,)+
        }

        impl SourceConsistency {
            pub(crate) const fn label(self) -> &'static str {
                match self { $(Self::$variant => $label,)+ }
            }

            pub(crate) fn persisted_label(label: &str) -> Option<&'static str> {
                match label { $($label => Some($label),)+ _ => None }
            }
        }

        #[cfg(test)]
        #[test]
        fn source_consistency_round_trips() {
            $(
                let saved = serde_json::to_string(SourceConsistency::$variant.label()).unwrap();
                let restored: String = serde_json::from_str(&saved).unwrap();
                assert_eq!(SourceConsistency::persisted_label(&restored), Some($label));
            )+
        }
    };
}

source_consistency! {
    PinnedObject => "pinned object",
    PostgresRepeatableRead => "PostgreSQL repeatable read",
    PostgresContent => "PostgreSQL relation content",
    MysqlRepeatableRead => "MySQL InnoDB repeatable read",
    MysqlStatement => "MySQL-compatible statement",
    Sqlite => "SQLite read transaction",
    Duckdb => "DuckDB read transaction",
    Oracle => "Oracle read-only transaction",
    Sqlserver => "SQL Server statement",
    Mongodb => "MongoDB cursor",
    Databricks => "Databricks statement",
    Iceberg => "Iceberg snapshot",
    Delta => "Delta version",
    Snowflake => "Snowflake time travel",
    Bigquery => "BigQuery time travel",
    Trino => "Trino statement",
    Clickhouse => "ClickHouse statement",
    Spanner => "Spanner read-only transaction",
    Hana => "SAP HANA statement",
}

#[derive(Debug, Default)]
pub(crate) struct ReadStats {
    requests: AtomicU64,
    object_bytes: AtomicU64,
    postgres_rows: AtomicU64,
    postgres_arrow_bytes: AtomicU64,
    warehouse_rows: AtomicU64,
    warehouse_arrow_bytes: AtomicU64,
    clickhouse_http_bytes: AtomicU64,
    snapshot_spilled_bytes: AtomicU64,
    mysql_rows: AtomicU64,
    mysql_arrow_bytes: AtomicU64,
    sqlite_rows: AtomicU64,
    sqlite_arrow_bytes: AtomicU64,
    sqlite_fullscan_steps: AtomicU64,
    local_parquet_write_bytes: AtomicU64,
    iceberg_rows: AtomicU64,
    iceberg_arrow_bytes: AtomicU64,
    frontier_requests: AtomicU64,
    frontier_scan_fallbacks: AtomicU64,
}

#[derive(Clone, Debug, Default, serde::Deserialize, Serialize)]
#[serde(default)]
pub struct ReadMetrics {
    pub requests: u64,
    pub object_bytes: u64,
    pub postgres_rows: u64,
    pub postgres_arrow_bytes: u64,
    pub warehouse_rows: u64,
    pub warehouse_arrow_bytes: u64,
    pub clickhouse_http_bytes: u64,
    pub snapshot_spilled_bytes: u64,
    pub mysql_rows: u64,
    pub mysql_arrow_bytes: u64,
    pub sqlite_rows: u64,
    pub sqlite_arrow_bytes: u64,
    pub sqlite_fullscan_steps: u64,
    pub local_parquet_write_bytes: u64,
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

    pub(crate) fn clickhouse_bytes(&self, bytes: usize) {
        self.clickhouse_http_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
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

    pub(crate) fn sqlite_batch(&self, batch: &RecordBatch) {
        self.sqlite_rows
            .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
        self.sqlite_arrow_bytes
            .fetch_add(batch.get_array_memory_size() as u64, Ordering::Relaxed);
    }

    pub(crate) fn sqlite_fullscan(&self, steps: i32) {
        self.sqlite_fullscan_steps
            .fetch_add(steps.max(0) as u64, Ordering::Relaxed);
    }

    #[cfg(feature = "admin")]
    pub(crate) fn local_parquet_write(&self, bytes: u64) {
        self.local_parquet_write_bytes
            .fetch_add(bytes, Ordering::Relaxed);
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
            clickhouse_http_bytes: self.clickhouse_http_bytes.load(Ordering::Relaxed),
            snapshot_spilled_bytes: self.snapshot_spilled_bytes.load(Ordering::Relaxed),
            mysql_rows: self.mysql_rows.load(Ordering::Relaxed),
            mysql_arrow_bytes: self.mysql_arrow_bytes.load(Ordering::Relaxed),
            sqlite_rows: self.sqlite_rows.load(Ordering::Relaxed),
            sqlite_arrow_bytes: self.sqlite_arrow_bytes.load(Ordering::Relaxed),
            sqlite_fullscan_steps: self.sqlite_fullscan_steps.load(Ordering::Relaxed),
            local_parquet_write_bytes: self.local_parquet_write_bytes.load(Ordering::Relaxed),
            iceberg_rows: self.iceberg_rows.load(Ordering::Relaxed),
            iceberg_arrow_bytes: self.iceberg_arrow_bytes.load(Ordering::Relaxed),
            frontier_requests: self.frontier_requests.load(Ordering::Relaxed),
            frontier_scan_fallbacks: self.frontier_scan_fallbacks.load(Ordering::Relaxed),
        }
    }
}

fn order_by(order: &[(String, bool)], ident: impl Fn(&str) -> String) -> String {
    if order.is_empty() {
        return String::new();
    }
    let terms = order
        .iter()
        .map(|(name, asc)| format!("{} {}", ident(name), if *asc { "ASC" } else { "DESC" }))
        .collect::<Vec<_>>()
        .join(", ");
    format!(" ORDER BY {terms}")
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
