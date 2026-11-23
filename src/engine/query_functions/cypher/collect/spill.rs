use std::{fs::File, sync::Arc, sync::atomic::Ordering};

use datafusion::{
    arrow::{
        array::{
            Array, ArrayRef, BinaryBuilder, ListArray, RecordBatch, UInt64Array,
            builder::{
                BinaryBuilder as BytesBuilder, LargeBinaryBuilder, LargeStringBuilder,
                StringBuilder,
            },
            new_empty_array,
        },
        compute::{concat, filter_record_batch},
        datatypes::{DataType, Field, Schema, SchemaRef},
        ipc::{reader::StreamReader, writer::StreamWriter},
    },
    common::{DataFusionError, Result, ScalarValue, utils::SingleRowListArrayBuilder},
    execution::{
        SendableRecordBatchStream, TaskContext,
        memory_pool::{MemoryConsumer, MemoryLimit},
        spill_file::SpillFile,
    },
    physical_expr::{LexOrdering, PhysicalSortExpr, expressions::Column},
    physical_plan::{
        ExecutionPlan, execute_stream,
        sorts::sort::SortExec,
        stream::RecordBatchStreamAdapter,
        streaming::{PartitionStream, StreamingTableExec},
    },
    prelude::{SessionConfig, SessionContext},
};
use futures::TryStreamExt;

use super::{super::encode_equivalence, Files, Store};

pub(super) fn finish(store: Arc<Store>, files: Files, ty: &DataType) -> Result<ScalarValue> {
    let output = if let Ok(handle) = tokio::runtime::Handle::try_current()
        && handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread
    {
        tokio::task::block_in_place(|| handle.block_on(complete(&store, files, ty)))?
    } else {
        finish_on_fallback(store.clone(), files, ty.clone())?
    };
    let budget = MemoryConsumer::new("materializing the distinct collect list")
        .register(&store.runtime.memory_pool);
    let copy = if ty.is_nested()
        || matches!(
            ty,
            DataType::Dictionary(..) | DataType::Utf8View | DataType::BinaryView
        ) {
        2
    } else {
        1
    };
    budget.try_grow(output.bytes.saturating_mul(copy))?;
    let values = if output.rows == 0 {
        new_empty_array(ty)
    } else {
        crate::blocking(|| build_values(output.reader()?, ty, output.rows, output.bytes, &store))?
    };
    Ok(ScalarValue::List(Arc::new(
        SingleRowListArrayBuilder::new(values).build_list_array(),
    )))
}

fn finish_on_fallback(store: Arc<Store>, files: Files, ty: DataType) -> Result<Output> {
    // Accumulator::evaluate is synchronous even in a single-threaded runtime.
    static RUNTIME: std::sync::OnceLock<std::io::Result<tokio::runtime::Runtime>> =
        std::sync::OnceLock::new();
    let runtime = RUNTIME
        .get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
        })
        .as_ref()
        .map_err(|error| DataFusionError::Execution(error.to_string()))?;
    let (send, receive) = std::sync::mpsc::sync_channel(1);
    runtime.spawn(async move {
        let _ = send.send(complete(&store, files, &ty).await);
    });
    receive
        .recv()
        .map_err(|_| DataFusionError::Execution("distinct collect spill worker stopped".into()))?
}

async fn complete(store: &Store, files: Files, ty: &DataType) -> Result<Output> {
    tokio::select! {
        result = ordered_winners(store, files, ty) => result,
        _ = store.cancellation.cancelled() => Err(DataFusionError::Execution("query cancelled".into())),
        _ = tokio::time::sleep_until(store.deadline) => Err(DataFusionError::Execution("query deadline exceeded".into())),
    }
}

struct Output {
    file: Arc<dyn SpillFile>,
    rows: usize,
    bytes: usize,
}

impl Output {
    fn reader(&self) -> Result<StreamReader<std::io::BufReader<File>>> {
        open(&self.file)
    }
}

async fn ordered_winners(store: &Store, files: Files, ty: &DataType) -> Result<Output> {
    let value = Field::new("value", ty.clone(), true);
    let order = Field::new("order", DataType::Binary, false);
    let sequence = Field::new("sequence", DataType::UInt64, false);
    // Homogeneous integers and strings already have exact Arrow equality.
    // Mixed types and entities still use the Cypher equivalence encoding.
    let native_key = ty.is_integer()
        || matches!(
            ty,
            DataType::Boolean
                | DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Utf8View
                | DataType::Binary
                | DataType::LargeBinary
                | DataType::BinaryView
        );
    let winner_schema = Arc::new(Schema::new(vec![order, sequence, value.clone()]));
    let source_schema = if native_key {
        Arc::clone(&winner_schema)
    } else {
        let mut fields = winner_schema.fields().to_vec();
        fields.push(Arc::new(Field::new("key", DataType::Binary, false)));
        Arc::new(Schema::new(fields))
    };
    let output_schema = Arc::new(Schema::new(vec![value]));
    let batch_size = if ty.is_primitive() { 1024 } else { 128 };
    let mut config = SessionConfig::new()
        .with_target_partitions(1)
        .with_batch_size(batch_size);
    let pool = &store.runtime.memory_pool;
    let available = match pool.memory_limit() {
        MemoryLimit::Finite(limit) => limit.saturating_sub(pool.reserved()),
        _ => store.flush_bytes * 16,
    };
    config.options_mut().execution.sort_spill_reservation_bytes =
        (available / 4).clamp(64 * 1024, 4 * 1024 * 1024);
    let context = SessionContext::new_with_config_rt(config, Arc::clone(&store.runtime));
    let source = Arc::new(StreamingTableExec::try_new(
        Arc::clone(&source_schema),
        vec![Arc::new(Runs {
            schema: source_schema,
            files,
            native_key,
            batch_size,
        })],
        None,
        [],
        false,
        None,
    )?);
    let key = if native_key { (2, "value") } else { (3, "key") };
    let by_key = Arc::new(sort(source, &[key, (0, "order"), (1, "sequence")]));
    let winners = Arc::new(StreamingTableExec::try_new(
        Arc::clone(&winner_schema),
        vec![Arc::new(Winners {
            schema: Arc::clone(&winner_schema),
            sorted: by_key.clone(),
            key: key.0,
        })],
        None,
        [],
        false,
        None,
    )?);
    // Materialize only the winners so the two external sort phases do not
    // hold competing merge reservations in the same query memory pool.
    let disk = spool(
        store,
        execute_stream(winners, context.task_ctx())?,
        &winner_schema,
        &[0, 1, 2],
    )
    .await
    .map_err(|e| e.context("merging distinct collect keys"))?;
    record_sort(store, &by_key);
    drop(by_key);
    let winners = Arc::new(StreamingTableExec::try_new(
        Arc::clone(&winner_schema),
        vec![Arc::new(SavedWinners {
            schema: winner_schema,
            file: disk.file,
        })],
        None,
        [],
        false,
        None,
    )?);
    let by_order = Arc::new(sort(winners, &[(0, "order"), (1, "sequence")]));
    let output = spool(
        store,
        execute_stream(by_order.clone(), context.task_ctx())?,
        &output_schema,
        &[2],
    )
    .await
    .map_err(|e| e.context("ordering distinct collect winners"))?;
    record_sort(store, &by_order);
    Ok(output)
}

async fn spool(
    store: &Store,
    mut stream: SendableRecordBatchStream,
    schema: &SchemaRef,
    columns: &[usize],
) -> Result<Output> {
    let (file, mut writer) = crate::blocking(|| {
        let file = store
            .runtime
            .disk_manager
            .create_tmp_file("assembling a distinct Cypher collect list")?;
        let writer = StreamWriter::try_new(file.open_writer()?, schema)?;
        Ok::<_, DataFusionError>((file, writer))
    })?;
    let mut rows = 0usize;
    let mut bytes = 0usize;
    while let Some(batch) = stream.try_next().await? {
        let batch = batch.project(columns)?;
        rows = rows
            .checked_add(batch.num_rows())
            .filter(|rows| i32::try_from(*rows).is_ok())
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "distinct collect list exceeds Arrow list capacity".into(),
                )
            })?;
        bytes = bytes.saturating_add(crate::engine::query::allocation::slice_bytes(
            batch.column(columns.len() - 1).as_ref(),
        )?);
        crate::blocking(|| writer.write(&batch))?;
    }
    crate::blocking(|| {
        writer.into_inner()?.finish()?;
        Ok::<_, DataFusionError>(())
    })?;
    store
        .spilled
        .fetch_add(file.size().unwrap_or_default(), Ordering::Relaxed);
    Ok(Output { file, rows, bytes })
}

fn record_sort(store: &Store, plan: &SortExec) {
    store.spilled.fetch_add(
        plan.metrics()
            .and_then(|m| m.spilled_bytes())
            .unwrap_or_default() as u64,
        Ordering::Relaxed,
    );
}

fn sort(input: Arc<dyn ExecutionPlan>, columns: &[(usize, &str)]) -> SortExec {
    let order = columns
        .iter()
        .map(|&(index, name)| PhysicalSortExpr::new_default(Arc::new(Column::new(name, index))));
    SortExec::new(
        LexOrdering::new(order).expect("nonempty collect sort order"),
        input,
    )
}

fn open(file: &Arc<dyn SpillFile>) -> Result<StreamReader<std::io::BufReader<File>>> {
    let path = file
        .path()
        .ok_or_else(|| DataFusionError::Execution("collect spill has no local path".into()))?;
    Ok(StreamReader::try_new_buffered(File::open(path)?, None)?)
}

struct Runs {
    schema: SchemaRef,
    files: Files,
    native_key: bool,
    batch_size: usize,
}

impl std::fmt::Debug for Runs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DistinctCollectRuns")
            .field("files", &self.files.len())
            .finish()
    }
}

impl PartitionStream for Runs {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _: Arc<TaskContext>) -> SendableRecordBatchStream {
        let files = self.files.clone();
        let native_key = self.native_key;
        let batch_size = self.batch_size;
        let schema = Arc::clone(&self.schema);
        let output = Arc::clone(&schema);
        let stream = async_stream::try_stream! {
            let mut sequence = 0u64;
            for file in &files {
                let mut reader = crate::blocking(|| file.reader())?;
                while let Some(batch) = crate::blocking(|| reader.next()) {
                    let batch = batch?;
                    let values = batch.column(0).as_any().downcast_ref::<ListArray>().expect("collect values");
                    let orders = batch.column(1).as_any().downcast_ref::<ListArray>().expect("collect order");
                    for row in 0..batch.num_rows() {
                        let values = values.value(row);
                        let orders = orders.value(row);
                        let mut key = Vec::new();
                        for offset in (0..values.len()).step_by(batch_size) {
                            let len = (values.len() - offset).min(batch_size);
                            let values = values.slice(offset, len);
                            let orders = orders.slice(offset, len);
                            let end = sequence + len as u64;
                            let positions = UInt64Array::from_iter_values(sequence..end);
                            sequence = end;
                            let mut columns = vec![orders, Arc::new(positions), values];
                            if !native_key {
                                let mut keys = BinaryBuilder::new();
                                for i in 0..len {
                                    key.clear();
                                    encode_equivalence(ScalarValue::try_from_array(&columns[2], i)?, &mut key)?;
                                    keys.append_value(&key);
                                }
                                columns.push(Arc::new(keys.finish()));
                            }
                            yield RecordBatch::try_new(Arc::clone(&output), columns)?;
                        }
                    }
                    tokio::task::yield_now().await;
                }
            }
        };
        Box::pin(RecordBatchStreamAdapter::new(schema, stream))
    }
}

#[derive(Debug)]
struct Winners {
    schema: SchemaRef,
    sorted: Arc<dyn ExecutionPlan>,
    key: usize,
}

impl PartitionStream for Winners {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, context: Arc<TaskContext>) -> SendableRecordBatchStream {
        let sorted = Arc::clone(&self.sorted);
        let key = self.key;
        let stream = async_stream::try_stream! {
            let mut sorted = execute_stream(sorted, context)?;
            let mut previous = None;
            while let Some(batch) = sorted.try_next().await? {
                let keys = batch.column(key);
                let mut keep = datafusion::arrow::array::BooleanBuilder::with_capacity(keys.len());
                for row in 0..keys.len() {
                    let value = ScalarValue::try_from_array(keys, row)?;
                    let distinct = previous.as_ref() != Some(&value);
                    if distinct {
                        previous = Some(value);
                    }
                    keep.append_value(distinct);
                }
                let batch = filter_record_batch(&batch.project(&[0, 1, 2])?, &keep.finish())?;
                if batch.num_rows() > 0 {
                    yield batch;
                }
            }
        };
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        ))
    }
}

struct SavedWinners {
    schema: SchemaRef,
    file: Arc<dyn SpillFile>,
}

impl std::fmt::Debug for SavedWinners {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DistinctCollectWinners")
    }
}

impl PartitionStream for SavedWinners {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _: Arc<TaskContext>) -> SendableRecordBatchStream {
        let file = Arc::clone(&self.file);
        let stream = async_stream::try_stream! {
            let mut reader = crate::blocking(|| open(&file))?;
            while let Some(batch) = crate::blocking(|| reader.next()) {
                yield batch?;
                tokio::task::yield_now().await;
            }
        };
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        ))
    }
}

fn check_store(store: &Store) -> Result<()> {
    if store.cancellation.is_cancelled() {
        return Err(DataFusionError::Execution("query cancelled".into()));
    }
    if tokio::time::Instant::now() >= store.deadline {
        return Err(DataFusionError::Execution("query deadline exceeded".into()));
    }
    Ok(())
}

fn build_values(
    reader: StreamReader<std::io::BufReader<File>>,
    ty: &DataType,
    rows: usize,
    bytes: usize,
    store: &Store,
) -> Result<ArrayRef> {
    macro_rules! bytes {
        ($builder:ident, $scalar:ident) => {{
            let mut builder = $builder::with_capacity(rows, bytes);
            for batch in reader {
                check_store(store)?;
                let batch = batch?;
                for i in 0..batch.num_rows() {
                    let ScalarValue::$scalar(Some(value)) =
                        ScalarValue::try_from_array(batch.column(0), i)?
                    else {
                        return Err(DataFusionError::Internal(
                            "invalid spilled collect value".into(),
                        ));
                    };
                    builder.append_value(value);
                }
            }
            Arc::new(builder.finish())
        }};
    }
    let offsets = match ty {
        DataType::Utf8 | DataType::Binary => 4,
        DataType::LargeUtf8 | DataType::LargeBinary => 8,
        _ => 0,
    };
    let bytes = bytes.saturating_sub(rows.saturating_mul(offsets));
    if offsets == 4 && bytes > i32::MAX as usize {
        return Err(DataFusionError::Execution(
            "distinct collect values exceed Arrow byte offsets".into(),
        ));
    }
    let array: ArrayRef = match ty {
        DataType::Utf8 => bytes!(StringBuilder, Utf8),
        DataType::LargeUtf8 => bytes!(LargeStringBuilder, LargeUtf8),
        DataType::Binary => bytes!(BytesBuilder, Binary),
        DataType::LargeBinary => bytes!(LargeBinaryBuilder, LargeBinary),
        ty if ty.is_nested()
            || matches!(
                ty,
                DataType::Dictionary(..) | DataType::Utf8View | DataType::BinaryView
            ) =>
        {
            let mut batches = Vec::new();
            for batch in reader {
                check_store(store)?;
                batches.push(batch?);
            }
            let values = batches
                .iter()
                .map(|b| b.column(0).as_ref())
                .collect::<Vec<_>>();
            concat(&values)?
        }
        _ => {
            let mut error = None;
            let mut reader = reader.flat_map(|batch| {
                if let Err(err) = check_store(store) {
                    return vec![Err(err)];
                }
                match batch {
                    Ok(batch) => (0..batch.num_rows())
                        .map(move |row| ScalarValue::try_from_array(batch.column(0), row))
                        .collect(),
                    Err(err) => vec![Err(err.into())],
                }
            });
            let values = std::iter::from_fn(|| match reader.next()? {
                Ok(value) => Some(value),
                Err(err) => {
                    error = Some(err);
                    None
                }
            });
            let result = ScalarValue::iter_to_array(values);
            if let Some(error) = error {
                return Err(error);
            }
            result?
        }
    };
    Ok(array)
}
