use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use datafusion::{
    arrow::{
        buffer::Buffer,
        datatypes::SchemaRef,
        ipc::{
            CompressionType,
            reader::StreamDecoder,
            writer::{IpcWriteOptions, StreamWriter},
        },
    },
    execution::{
        TaskContext,
        memory_pool::{MemoryConsumer, MemoryPool},
        spill_file::{SpillFile, SpillWriter},
    },
    physical_plan::{
        ExecutionPlan, ExecutionPlanProperties, Partitioning, SendableRecordBatchStream,
        metrics::Time, repartition::BatchPartitioner, stream::RecordBatchStreamAdapter,
    },
};
use futures::TryStreamExt;
use uuid::Uuid;

use super::{Distributed, Output};
use crate::engine::QueryScope;

struct Bucket {
    file: Arc<dyn SpillFile>,
    writer: StreamWriter<Box<dyn SpillWriter>>,
}

pub(super) async fn write(
    plan: Arc<dyn ExecutionPlan>,
    exchange: Option<&Partitioning>,
    partition: usize,
    ctx: &Arc<TaskContext>,
    scope: &QueryScope,
    cluster: &Distributed,
    task: Uuid,
    worker: Uuid,
) -> Result<Output> {
    let input_count = plan.output_partitioning().partition_count();
    ensure!(partition < input_count, "invalid stage partition");
    let mut stream = plan.execute(partition, Arc::clone(ctx))?;
    let schema = stream.schema();
    let mut router = exchange
        .map(|exchange| {
            BatchPartitioner::try_new(exchange.clone(), Time::new(), partition, input_count)
        })
        .transpose()?;
    let count = exchange.map_or(input_count, Partitioning::partition_count);
    let mut buckets = (0..count).map(|_| None::<Bucket>).collect::<Vec<_>>();
    let reservation =
        MemoryConsumer::new("GraphLake shuffle batch").register(&scope.runtime.memory_pool);
    while let Some(batch) = stream.try_next().await? {
        scope.check()?;
        if batch.num_rows() == 0 {
            continue;
        }
        // Operators can return slices of a much larger shared backing buffer.
        // The exchange allocates memory for the rows being routed and encoded.
        let bytes = batch.columns().iter().try_fold(0_usize, |sum, column| {
            Ok::<_, datafusion::arrow::error::ArrowError>(
                sum.saturating_add(column.to_data().get_slice_memory_size()?),
            )
        })?;
        reservation.try_resize(if router.is_some() {
            bytes.saturating_mul(2)
        } else {
            bytes
        })?;
        crate::blocking(|| -> Result<()> {
            let mut save = |bucket: usize,
                            batch: datafusion::arrow::array::RecordBatch|
             -> datafusion::common::Result<()> {
                if batch.num_rows() == 0 {
                    return Ok(());
                }
                let entry = &mut buckets[bucket];
                if entry.is_none() {
                    let file = scope
                        .runtime
                        .disk_manager
                        .create_tmp_file("writing distributed shuffle")?;
                    let ipc = IpcWriteOptions::default()
                        .try_with_compression(Some(CompressionType::LZ4_FRAME))?;
                    let writer =
                        StreamWriter::try_new_with_options(file.open_writer()?, &schema, ipc)?;
                    *entry = Some(Bucket { file, writer });
                }
                entry
                    .as_mut()
                    .expect("shuffle bucket")
                    .writer
                    .write(&batch)?;
                Ok(())
            };
            if let Some(router) = &mut router {
                router.partition(batch, save)?;
            } else {
                save(partition, batch)?;
            }
            Ok(())
        })?;
        reservation.free();
    }
    let mut output = Output {
        task,
        buckets: vec![0; count],
        worker,
        operator_spill: 0,
    };
    for (index, bucket) in buckets.into_iter().enumerate() {
        let Some(bucket) = bucket else { continue };
        let file = crate::blocking(|| -> Result<_> {
            bucket.writer.into_inner()?.finish()?;
            Ok(bucket.file)
        })?;
        let bytes = file.size().context("shuffle file length")?;
        let key = cluster.key(task, &format!("bucket-{index}.arrow"));
        let buffer =
            MemoryConsumer::new("GraphLake shuffle upload").register(&scope.runtime.memory_pool);
        buffer.try_grow(bytes.min(5 * 1024 * 1024) as usize)?;
        cluster
            .shared
            .objects
            .put_file(&key, file.path().context("shuffle file path")?, bytes)
            .await?;
        output.buckets[index] = bytes;
    }
    scope.record_spill(plan.as_ref());
    output.operator_spill = crate::engine::execution_spill(plan.as_ref());
    Ok(output)
}

pub(super) fn read(
    cluster: Distributed,
    pool: Arc<dyn MemoryPool>,
    schema: SchemaRef,
    files: Vec<(Uuid, usize, u64)>,
) -> SendableRecordBatchStream {
    let expected_schema = Arc::clone(&schema);
    let stream = async_stream::try_stream! {
        let reservation = MemoryConsumer::new("GraphLake shuffle decode").register(&pool);
        let batch_reservation = MemoryConsumer::new("GraphLake shuffle batch").register(&pool);
        for (task, bucket, expected_length) in files {
            let key = cluster.key(task, &format!("bucket-{bucket}.arrow"));
            let object = cluster.shared.objects.get(&key).await.map_err(super::plan::external)?;
            if object.meta.size != expected_length {
                Err(super::plan::external(anyhow::anyhow!("shuffle file length changed")))?;
            }
            let mut bytes = object.into_stream();
            let mut decoder = StreamDecoder::new();
            let mut received = 0_u64;
            while let Some(chunk) = bytes.try_next().await.map_err(super::plan::external)? {
                received = received.saturating_add(chunk.len() as u64);
                if received > expected_length {
                    Err(super::plan::external(anyhow::anyhow!("shuffle file exceeded expected length")))?;
                }
                reservation.try_resize(chunk.len())?;
                let mut buffer = Buffer::from(chunk);
                while !buffer.is_empty() {
                    if let Some(batch) = decoder.decode(&mut buffer)? {
                        // IPC preserves the field layout and metadata used by graph values.
                        if batch.schema() != expected_schema {
                            Err(datafusion::common::DataFusionError::Execution("shuffle schema mismatch".into()))?;
                        }
                        batch_reservation.try_resize(crate::source::snapshot::ipc_batch_bytes(&batch))?;
                        yield batch;
                        batch_reservation.free();
                    }
                }
                reservation.free();
            }
            if received != expected_length {
                Err(super::plan::external(anyhow::anyhow!("incomplete shuffle file")))?;
            }
            decoder.finish()?;
        }
    };
    Box::pin(RecordBatchStreamAdapter::new(schema, stream))
}
