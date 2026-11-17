use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use datafusion::{
    arrow::{array::RecordBatch, csv, datatypes::SchemaRef, json},
    catalog::{TableProvider, streaming::StreamingTable},
    execution::{
        SendableRecordBatchStream, TaskContext,
        disk_manager::DiskManager,
        memory_pool::{MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation},
    },
    physical_plan::{stream::RecordBatchStreamAdapter, streaming::PartitionStream},
};
use futures::{TryStreamExt, stream::BoxStream};
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

#[derive(Clone, Copy, PartialEq)]
pub(crate) enum ReadMode {
    Probe,
    Snapshot,
    Stream,
}

#[derive(Clone, Debug)]
struct Reader {
    schema: SchemaRef,
    projection: Vec<usize>,
    csv: bool,
    batch_size: usize,
    range_size: u64,
    pool: Arc<dyn MemoryPool>,
}

impl Reader {
    fn batches<'a>(
        &'a self,
        location: &'a str,
        mut file: PinnedFile,
    ) -> BoxStream<'a, Result<RecordBatch>> {
        Box::pin(async_stream::try_stream! {
            let staging = MemoryConsumer::new("GraphLake text decoder").register(&self.pool);
            let mut decoder = Decoder::new(
                Arc::clone(&self.schema),
                self.csv,
                self.batch_size,
            )?;
            let mut offset = 0;
            loop {
                let end = file.len().min(offset + self.range_size);
                let bytes = file.get_bytes(offset..end).await?;
                let eof = offset == end;
                offset = end;
                let mut remaining = bytes.as_ref();
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
                            let projected = batch.project(&self.projection)?;
                            drop(batch);
                            yield projected;
                        } else if !remaining.is_empty() {
                            Err(anyhow::anyhow!("text decoder made no progress"))?;
                        }
                    }
                    if remaining.is_empty() && (!eof || consumed == 0) {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
                if eof {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
    }
}

#[derive(Debug)]
struct TextScan {
    schema: SchemaRef,
    reader: Reader,
    files: Arc<[(String, PinnedFile)]>,
}

impl PartitionStream for TextScan {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _: Arc<TaskContext>) -> SendableRecordBatchStream {
        let files = self.files.clone();
        let reader = self.reader.clone();
        let stream = async_stream::try_stream! {
            let output = MemoryConsumer::new("GraphLake text batch").register(&reader.pool);
            for (location, file) in files.iter() {
                let mut batches = reader.batches(location, file.clone());
                while let Some(batch) = batches.try_next().await.map_err(|error| datafusion::common::DataFusionError::External(error.into()))? {
                    output.try_resize(batch.get_array_memory_size())?;
                    yield batch;
                    output.free();
                }
            }
        };
        Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), stream))
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
    mode: ReadMode,
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
    let range_size = if store.is_some() && mode != ReadMode::Probe {
        match pool.memory_limit() {
            MemoryLimit::Finite(limit) => (limit / 16).clamp(64 * 1024, 1024 * 1024) as u64,
            _ => 1024 * 1024,
        }
    } else {
        64 * 1024
    };
    let reader = Reader {
        schema,
        projection,
        csv,
        batch_size: if mode == ReadMode::Probe {
            1
        } else {
            batch_size
        },
        range_size,
        pool: pool.clone(),
    };
    let mut buffer = Buffer::new(&pool, (mode == ReadMode::Snapshot).then_some(disk));
    let mut versions = Vec::new();
    let mut files = Vec::new();
    for location in &mapping.files {
        let file =
            PinnedFile::open_storage(location, store.as_ref(), pool.clone(), stats.clone()).await?;
        ensure!(
            !csv || file.len() > 0,
            "CSV file requires a header: {location}"
        );
        versions.push(file.version(location));
        if mode == ReadMode::Stream {
            files.push((location.clone(), file));
        } else {
            let mut batches = reader.batches(location, file);
            while let Some(batch) = batches.try_next().await? {
                if mode == ReadMode::Probe {
                    break;
                }
                buffer.push(batch)?;
            }
        }
    }
    let (table, budget) = match mode {
        ReadMode::Probe => buffer.finish_schema(projected)?,
        ReadMode::Snapshot => buffer.finish(projected, &stats)?,
        ReadMode::Stream => {
            let scan = TextScan {
                schema: projected.clone(),
                reader,
                files: files.into(),
            };
            let table = StreamingTable::try_new(projected, vec![Arc::new(scan)])?;
            let budget = MemoryConsumer::new("GraphLake text scan").register(&pool);
            (Arc::new(table) as Arc<dyn TableProvider>, budget)
        }
    };
    Ok((table, budget, versions))
}
