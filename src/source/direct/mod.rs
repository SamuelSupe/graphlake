mod duckdb;
mod mongodb;
mod oracle;
mod sqlserver;

use super::ReadStats;
use crate::{Predicate, Source, engine::sql_read::Pushdown};
use anyhow::{Result, bail};
use datafusion::{
    arrow::array::RecordBatch,
    catalog::TableProvider,
    execution::{memory_pool::MemoryReservation, runtime_env::RuntimeEnv},
};
use std::{sync::Arc, time::Duration};

pub(crate) enum Driver {
    Duckdb(duckdb::Driver),
    Sqlserver(sqlserver::Driver),
    Oracle(oracle::Driver),
    Mongodb(mongodb::Driver),
}

pub(crate) struct PushedRead {
    pub(crate) table: Arc<dyn TableProvider>,
    pub(crate) budget: MemoryReservation,
    pub(crate) aggregate: Option<RecordBatch>,
}

impl Driver {
    pub(crate) async fn pushdown(
        &self,
        table: &str,
        columns: &[String],
        keys: &[String],
        operation: &Pushdown,
        batch_size: usize,
    ) -> Result<Option<PushedRead>> {
        match self {
            Self::Duckdb(driver) => {
                driver
                    .pushdown(table, columns, keys, operation, batch_size)
                    .await
            }
            _ => Ok(None),
        }
    }
    pub(crate) async fn open(
        source: &Source,
        timeout: Duration,
        runtime: Arc<RuntimeEnv>,
        stats: Arc<ReadStats>,
        duckdb_sources: usize,
    ) -> Result<Self> {
        Ok(match source {
            Source::Duckdb { path } => {
                Self::Duckdb(duckdb::Driver::open(path, runtime, stats, duckdb_sources).await?)
            }
            Source::SqlserverNative { .. } => {
                Self::Sqlserver(sqlserver::Driver::open(source, timeout, runtime, stats).await?)
            }
            Source::OracleNative { .. } => {
                Self::Oracle(oracle::Driver::open(source, timeout, runtime, stats).await?)
            }
            Source::MongodbNative { .. } => {
                Self::Mongodb(mongodb::Driver::open(source, timeout, runtime, stats).await?)
            }
            _ => bail!("expected direct database source"),
        })
    }

    pub(crate) async fn table(
        &mut self,
        table: &str,
        columns: &[String],
        filters: &[Predicate],
        batch_size: usize,
        explain: bool,
    ) -> Result<(Arc<dyn TableProvider>, MemoryReservation)> {
        match self {
            Self::Duckdb(driver) => {
                driver
                    .table(table, columns, filters, batch_size, explain)
                    .await
            }
            Self::Sqlserver(driver) => {
                driver
                    .table(table, columns, filters, batch_size, explain)
                    .await
            }
            Self::Oracle(driver) => {
                driver
                    .table(table, columns, filters, batch_size, explain)
                    .await
            }
            Self::Mongodb(driver) => driver.table(table, columns, batch_size, explain).await,
        }
    }

    #[cfg(feature = "admin")]
    pub(crate) async fn browse(&mut self, schema: Option<&str>) -> Result<Vec<String>> {
        match self {
            Self::Duckdb(driver) => driver.browse(schema).await,
            Self::Sqlserver(driver) => driver.browse(schema).await,
            Self::Oracle(driver) => driver.browse(schema).await,
            Self::Mongodb(driver) => driver.browse(schema).await,
        }
    }
}

fn relation(value: &str, quote: fn(&str) -> String) -> String {
    value.split('.').map(quote).collect::<Vec<_>>().join(".")
}
fn projection(columns: &[String], quote: fn(&str) -> String) -> String {
    if columns.is_empty() {
        "*".into()
    } else {
        columns
            .iter()
            .map(|name| quote(name))
            .collect::<Vec<_>>()
            .join(", ")
    }
}
