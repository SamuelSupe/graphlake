use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use datafusion::{
    arrow::{array::RecordBatch, csv, datatypes::SchemaRef, json},
    catalog::TableProvider,
    execution::{
        disk_manager::DiskManager,
        memory_pool::{MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation},
    },
};
use parquet::arrow::async_reader::AsyncFileReader;

use super::{ReadStats, SourceVersion, parquet::PinnedFile, snapshot::Buffer};
use crate::TextTable;

enum Decoder {
    Csv(csv::reader::Decoder),
    Json(json::reader::Decoder),
}

impl Decoder {
    fn new(schema: SchemaRef, csv: bool, batch_size: usize) -> Result<Self> {
        Ok(if csv {
            Self::Csv(
                csv::ReaderBuilder::new(schema)
                    .with_header(true)
                    .with_header_validation(true)
                    .with_batch_size(batch_size)
                    .build_decoder(),
            )
        } else {
            Self::Json(
                json::ReaderBuilder::new(schema)
                    .with_batch_size(batch_size)
                    .build_decoder()?,
            )
        })
    }

    fn decode(&mut self, bytes: &[u8]) -> Result<usize> {
        Ok(match self {
            Self::Csv(decoder) => decoder.decode(bytes)?,
            Self::Json(decoder) => decoder.decode(bytes)?,
        })
    }

    fn flush(&mut self) -> Result<Option<RecordBatch>> {
        Ok(match self {
            Self::Csv(decoder) => decoder.flush()?,
            Self::Json(decoder) => decoder.flush()?,
        })
    }
}

pub(crate) async fn open(
    mapping: &TextTable,
    csv: bool,
    columns: &[String],
    batch_size: usize,
    pool: Arc<dyn MemoryPool>,
    disk: &Arc<DiskManager>,
    stats: Arc<ReadStats>,
    explain_only: bool,
    storage: Option<&crate::StorageConfig>,
) -> Result<(
    Arc<dyn TableProvider>,
    MemoryReservation,
    Vec<SourceVersion>,
)> {
    let schema = mapping.schema()?;
    let projection = if columns.is_empty() {
        (0..schema.fields().len()).collect::<Vec<_>>()
    } else {
        columns
            .iter()
            .map(|name| schema.index_of(name))
            .collect::<std::result::Result<Vec<_>, _>>()?
    };
    let projected = Arc::new(schema.project(&projection)?);
    let store = storage.map(super::storage::open).transpose()?;
    // Keep metadata probes small. Streaming remote files can amortize the
    // range requests, within the decoder's memory budget.
    let range_size = if store.is_some() && !explain_only {
        match pool.memory_limit() {
            MemoryLimit::Finite(limit) => (limit / 16).clamp(64 * 1024, 1024 * 1024) as u64,
            _ => 1024 * 1024,
        }
    } else {
        64 * 1024
    };
    let mut buffer = Buffer::new(&pool, (!explain_only).then_some(disk));
    let mut versions = Vec::new();
    let staging = MemoryConsumer::new("GraphLake text decoder").register(&pool);
    for location in &mapping.files {
        let mut file = PinnedFile::open_storage(
            location,
            store.as_ref(),
            Arc::clone(&pool),
            Arc::clone(&stats),
        )
        .await?;
        ensure!(
            !csv || file.len() > 0,
            "CSV file requires a header: {location}"
        );
        let mut decoder = Decoder::new(
            Arc::clone(&schema),
            csv,
            if explain_only { 1 } else { batch_size },
        )?;
        let mut offset = 0;
        loop {
            let end = file.len().min(offset + range_size);
            let bytes = file.get_bytes(offset..end).await?;
            let eof = offset == end;
            offset = end;
            let mut remaining = bytes.as_ref();
            let mut described = false;
            loop {
                // Account for partial records spanning object ranges before letting
                // Arrow grow its decoder; the output batch has a separate budget.
                staging.try_grow(remaining.len().saturating_mul(2))?;
                let consumed = decoder
                    .decode(remaining)
                    .with_context(|| format!("decoding text object {location}"))?;
                staging.shrink((remaining.len() - consumed).saturating_mul(2));
                remaining = &remaining[consumed..];
                if consumed == 0 {
                    let batch = decoder
                        .flush()
                        .with_context(|| format!("decoding text object {location}"))?;
                    staging.free();
                    if let Some(batch) = batch {
                        if explain_only {
                            described = true;
                            break;
                        }
                        buffer.push(batch.project(&projection)?)?;
                    } else {
                        ensure!(remaining.is_empty(), "text decoder made no progress");
                    }
                }
                if remaining.is_empty() && (!eof || consumed == 0) {
                    break;
                }
                tokio::task::yield_now().await;
            }
            if eof || described {
                break;
            }
            tokio::task::yield_now().await;
        }
        staging.free();
        versions.push(file.version(location));
    }
    let (table, budget) = if explain_only {
        buffer.finish_schema(projected)?
    } else {
        buffer.finish(projected, &stats)?
    };
    Ok((table, budget, versions))
}
