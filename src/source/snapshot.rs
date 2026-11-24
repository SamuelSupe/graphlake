use std::{
    collections::HashSet,
    fmt,
    fs::File,
    ptr::NonNull,
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result, ensure};
use datafusion::{
    arrow::{
        array::{ArrayData, RecordBatch},
        datatypes::SchemaRef,
        ipc::{
            CompressionType,
            reader::StreamReader,
            writer::{IpcWriteOptions, StreamWriter},
        },
    },
    catalog::{TableProvider, streaming::StreamingTable},
    datasource::MemTable,
    execution::{
        SendableRecordBatchStream, TaskContext,
        disk_manager::DiskManager,
        memory_pool::{MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation},
        spill_file::{SpillFile, SpillWriter},
    },
    logical_expr::SortExpr,
    physical_plan::{stream::RecordBatchStreamAdapter, streaming::PartitionStream},
};

use super::ReadStats;

type StartScan = Box<dyn FnOnce(Arc<TaskContext>) -> SendableRecordBatchStream + Send>;

/// Only use this provider for a plan with one consumer and no validation rescan.
pub(crate) fn stream_once(
    schema: SchemaRef,
    start: impl FnOnce(Arc<TaskContext>) -> SendableRecordBatchStream + Send + 'static,
) -> Result<Arc<dyn TableProvider>> {
    Ok(Arc::new(StreamingTable::try_new(
        schema.clone(),
        vec![Arc::new(OneScan {
            schema,
            start: Mutex::new(Some(Box::new(start))),
        })],
    )?))
}

struct OneScan {
    schema: SchemaRef,
    start: Mutex<Option<StartScan>>,
}

impl fmt::Debug for OneScan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("GraphLake source stream")
    }
}

impl PartitionStream for OneScan {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, context: Arc<TaskContext>) -> SendableRecordBatchStream {
        if let Some(start) = self.start.lock().unwrap().take() {
            return start(context);
        }
        Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            futures::stream::once(async {
                Err(datafusion::common::DataFusionError::Execution(
                    "single-consumer source was scanned again".into(),
                ))
            }),
        ))
    }
}

/// Store a statement's rows once, so local rescans see the same remote result.
pub(crate) struct Buffer {
    pool: Arc<dyn MemoryPool>,
    budget: MemoryReservation,
    source_reservation: usize,
    disk: Option<Arc<DiskManager>>,
    memory: Vec<RecordBatch>,
    file: Option<DiskBuffer>,
    compression: Option<CompressionType>,
}

struct DiskBuffer {
    file: Arc<dyn SpillFile>,
    writer: StreamWriter<Box<dyn SpillWriter>>,
}

impl Buffer {
    pub(crate) fn new(pool: &Arc<dyn MemoryPool>, disk: Option<&Arc<DiskManager>>) -> Self {
        Self {
            pool: Arc::clone(pool),
            budget: MemoryConsumer::new("GraphLake source snapshot").register(pool),
            source_reservation: 0,
            disk: disk
                .filter(|disk| disk.max_temp_directory_size() > 0)
                .cloned(),
            memory: Vec::new(),
            file: None,
            compression: None,
        }
    }

    pub(crate) fn with_source_reservation(mut self, bytes: usize) -> Self {
        self.source_reservation = bytes;
        self
    }

    #[cfg(feature = "admin")]
    pub(crate) fn new_compressed(pool: &Arc<dyn MemoryPool>, disk: &Arc<DiskManager>) -> Self {
        let mut buffer = Self::new(pool, Some(disk));
        buffer.compression = Some(CompressionType::LZ4_FRAME);
        buffer
    }

    pub(crate) fn memory(&self) -> &[RecordBatch] {
        &self.memory
    }

    pub(crate) fn push(&mut self, batch: RecordBatch) -> Result<()> {
        self.push_chunks(batch, usize::MAX)
    }

    pub(crate) fn push_chunks(&mut self, batch: RecordBatch, batch_size: usize) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let bytes = ipc_batch_bytes(&batch);
        if let MemoryLimit::Finite(limit) = self.pool.memory_limit() {
            ensure!(
                bytes <= limit,
                "source snapshot batch ({bytes} bytes) exceeds query memory limit ({limit} bytes)"
            );
        }
        // Leave room for the next connector page and downstream operators.
        // A source engine reservation already has its own share of the pool.
        if self.file.is_none()
            && let Some(disk) = &self.disk
            && let MemoryLimit::Finite(limit) = self.pool.memory_limit()
            && self
                .pool
                .reserved()
                .saturating_sub(self.source_reservation)
                .saturating_add(bytes)
                > limit.saturating_sub(self.source_reservation) / 4
        {
            self.file = Some(crate::blocking(|| {
                let file = disk.create_tmp_file("materializing a GraphLake source snapshot")?;
                let mut writer = StreamWriter::try_new_with_options(
                    file.open_writer()?,
                    &batch.schema(),
                    IpcWriteOptions::default().try_with_compression(self.compression)?,
                )?;
                for previous in self.memory.drain(..) {
                    writer.write(&previous)?;
                }
                Ok::<_, anyhow::Error>(DiskBuffer { file, writer })
            })?);
            self.budget.free();
        }
        let chunks = (0..batch.num_rows())
            .step_by(batch_size)
            .map(|offset| batch.slice(offset, batch_size.min(batch.num_rows() - offset)));
        if let Some(DiskBuffer { writer, .. }) = &mut self.file {
            crate::blocking(|| {
                for chunk in chunks {
                    writer.write(&chunk)?;
                }
                Ok::<_, anyhow::Error>(())
            })?;
        } else {
            self.budget.try_grow(bytes)?;
            self.memory.extend(chunks);
        }
        Ok(())
    }

    pub(crate) fn finish(
        self,
        schema: SchemaRef,
        stats: &ReadStats,
    ) -> Result<(Arc<dyn TableProvider>, MemoryReservation)> {
        self.finish_ordered(schema, stats, Vec::new())
    }

    pub(crate) fn finish_ordered(
        self,
        schema: SchemaRef,
        stats: &ReadStats,
        order: Vec<SortExpr>,
    ) -> Result<(Arc<dyn TableProvider>, MemoryReservation)> {
        let table: Arc<dyn TableProvider> = if let Some(DiskBuffer { file, writer }) = self.file {
            crate::blocking(|| writer.into_inner()?.finish().map_err(anyhow::Error::from))?;
            stats.snapshot_spill(file.size().unwrap_or_default());
            let table = StreamingTable::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Partition { schema, file })],
            )?;
            Arc::new(table.with_sort_order(order))
        } else {
            let table = MemTable::try_new(schema, vec![self.memory])?;
            Arc::new(if order.is_empty() {
                table
            } else {
                table.with_sort_order(vec![order])
            })
        };
        Ok((table, self.budget))
    }

    pub(crate) fn finish_schema(
        self,
        schema: SchemaRef,
    ) -> Result<(Arc<dyn TableProvider>, MemoryReservation)> {
        ensure!(
            self.memory.is_empty() && self.file.is_none(),
            "schema-only source query returned rows"
        );
        // EmptyTable reports an exact zero row count, which can fold away the plan being explained.
        let table =
            StreamingTable::try_new(Arc::clone(&schema), vec![Arc::new(SchemaOnly(schema))])?;
        Ok((Arc::new(table), self.budget))
    }
}

#[derive(Debug)]
struct SchemaOnly(SchemaRef);

impl PartitionStream for SchemaOnly {
    fn schema(&self) -> &SchemaRef {
        &self.0
    }

    fn execute(&self, _: Arc<TaskContext>) -> SendableRecordBatchStream {
        let stream = futures::stream::once(async {
            Err(datafusion::common::DataFusionError::Execution(
                "cannot execute an EXPLAIN source".into(),
            ))
        });
        Box::pin(RecordBatchStreamAdapter::new(Arc::clone(&self.0), stream))
    }
}

struct Partition {
    schema: SchemaRef,
    file: Arc<dyn SpillFile>,
}

impl fmt::Debug for Partition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GraphLake source snapshot")
            .finish_non_exhaustive()
    }
}

impl PartitionStream for Partition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, context: Arc<TaskContext>) -> SendableRecordBatchStream {
        let file = Arc::clone(&self.file);
        let stream = async_stream::try_stream! {
            let path = file.path().context("source snapshot has no local path")
                .map_err(|error| datafusion::common::DataFusionError::External(error.into()))?;
            let mut reader = crate::blocking(|| Ok::<_, datafusion::common::DataFusionError>(
                StreamReader::try_new_buffered(File::open(path)?, None)?
            ))?;
            let budget = MemoryConsumer::new("GraphLake source snapshot read").register(context.memory_pool());
            while let Some(batch) = crate::blocking(|| reader.next()) {
                let batch = batch?;
                let bytes = ipc_batch_bytes(&batch);
                budget.try_grow(bytes)?;
                yield batch;
                budget.shrink(bytes);
                tokio::task::yield_now().await;
            }
        };
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        ))
    }
}

pub(crate) fn ipc_batch_bytes(batch: &RecordBatch) -> usize {
    fn allocated(data: &ArrayData, seen: &mut HashSet<NonNull<u8>>) -> usize {
        let bytes = data
            .buffers()
            .iter()
            .chain(data.nulls().map(|nulls| nulls.buffer()))
            .filter(|buffer| seen.insert(buffer.data_ptr()))
            .map(|buffer| buffer.capacity())
            .sum::<usize>();
        size_of::<ArrayData>()
            + bytes
            + data
                .child_data()
                .iter()
                .map(|child| allocated(child, seen))
                .sum::<usize>()
    }

    // IPC arrays share the allocation containing the message body. Charge it
    // once even when a batch has many columns or nested buffers.
    let mut seen = HashSet::new();
    batch
        .columns()
        .iter()
        .map(|column| allocated(&column.to_data(), &mut seen))
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::{
        arrow::{
            array::StringArray,
            datatypes::{DataType, Field, Schema},
        },
        execution::memory_pool::{GreedyMemoryPool, MemoryPool},
    };
    use std::io::Cursor;

    #[test]
    fn ipc_batch_bytes_deduplicates_shared_buffer_in_push_chunks() -> Result<()> {
        let fields = (0..24)
            .map(|i| Field::new(format!("c{i}"), DataType::Utf8, false))
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(fields));
        let col = Arc::new(StringArray::from(vec!["x".repeat(256); 64]));
        let original = RecordBatch::try_new(Arc::clone(&schema), vec![col; 24])?;
        let mut wire = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut wire, &schema)?;
            writer.write(&original)?;
            writer.finish()?;
        }
        let mut reader = StreamReader::try_new(Cursor::new(wire), None)?;
        let decoded = reader.next().unwrap()?;
        let deduped = ipc_batch_bytes(&decoded);
        let naive = decoded.get_array_memory_size();
        assert!(
            deduped * 4 < naive,
            "expected shared IPC buffer dedup: deduped={deduped}, naive={naive}"
        );

        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(deduped * 2));
        let mut buffer = Buffer::new(&pool, None);
        buffer.push_chunks(decoded, 16)?;
        assert_eq!(buffer.memory().len(), 4);
        Ok(())
    }

    #[test]
    fn push_chunks_ignores_empty_batches() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("c0", DataType::Utf8, false)]));
        let empty = RecordBatch::new_empty(Arc::clone(&schema));
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(64));
        let mut buffer = Buffer::new(&pool, None);
        buffer.push_chunks(empty, 16)?;
        assert!(buffer.memory().is_empty());
        assert_eq!(buffer.budget.size(), 0);
        Ok(())
    }
}
