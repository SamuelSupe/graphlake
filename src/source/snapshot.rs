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
        let bytes = batch.get_array_memory_size();
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
            let file = disk.create_tmp_file("materializing a GraphLake source snapshot")?;
            let mut writer = StreamWriter::try_new_with_options(
                file.open_writer()?,
                &batch.schema(),
                IpcWriteOptions::default().try_with_compression(self.compression)?,
            )?;
            for previous in self.memory.drain(..) {
                writer.write(&previous)?;
            }
            self.budget.free();
            self.file = Some(DiskBuffer { file, writer });
        }
        self.budget.try_grow(bytes)?;
        let chunks = (0..batch.num_rows())
            .step_by(batch_size)
            .map(|offset| batch.slice(offset, batch_size.min(batch.num_rows() - offset)));
        if let Some(DiskBuffer { writer, .. }) = &mut self.file {
            for chunk in chunks {
                writer.write(&chunk)?;
            }
            self.budget.shrink(bytes);
        } else {
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
            writer.into_inner()?.finish()?;
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
            let reader = StreamReader::try_new_buffered(File::open(path)?, None)?;
            let budget = MemoryConsumer::new("GraphLake source snapshot read").register(context.memory_pool());
            for batch in reader {
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
