mod distinct;
mod spill;

use std::{
    collections::HashMap,
    fmt,
    fs::File,
    hash::{Hash, Hasher},
    io::{Read, Seek, SeekFrom},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use crate::source::snapshot::ipc_batch_bytes;
use datafusion::{
    arrow::{
        array::{Array, ArrayRef, RecordBatch, UInt64Array},
        datatypes::{DataType, Field, FieldRef, Schema},
        ipc::{reader::StreamReader, writer::StreamWriter},
        row::{RowConverter, SortField},
    },
    common::{Result, ScalarValue, exec_err},
    execution::{
        memory_pool::{MemoryConsumer, MemoryReservation},
        runtime_env::RuntimeEnv,
        spill_file::SpillFile,
    },
    functions_aggregate::array_agg::array_agg_udaf,
    logical_expr::{
        Accumulator, AggregateUDF, AggregateUDFImpl, Signature,
        function::{AccumulatorArgs, StateFieldsArgs},
        utils::AggregateOrderSensitivity,
    },
    prelude::SessionContext,
};

type Files = Vec<Run>;

#[derive(Clone)]
struct Run {
    file: Arc<dyn SpillFile>,
    offset: u64,
    length: u64,
}

impl Run {
    fn reader(&self) -> Result<StreamReader<std::io::BufReader<std::io::Take<File>>>> {
        let Some(path) = self.file.path() else {
            return exec_err!("collect spill has no local path");
        };
        let mut file = File::open(path)?;
        file.seek(SeekFrom::Start(self.offset))?;
        Ok(StreamReader::try_new_buffered(
            file.take(self.length),
            None,
        )?)
    }
}
type Factory = Box<dyn Fn() -> Result<Box<dyn Accumulator>> + Send + Sync>;

pub(in crate::engine) fn register(
    context: &SessionContext,
    runtime: Arc<RuntimeEnv>,
    memory_bytes: usize,
    spilled: Arc<AtomicU64>,
    cancellation: tokio_util::sync::CancellationToken,
    deadline: tokio::time::Instant,
) {
    let store = Store {
        runtime,
        flush_bytes: (memory_bytes / 16).clamp(1, 4 * 1024 * 1024),
        next: AtomicU64::new(0),
        tokens: Mutex::new(HashMap::new()),
        spill: Mutex::new(None),
        spilled,
        cancellation,
        deadline,
    };
    let store = Arc::new(store);
    for distinct in [false, true] {
        context.register_udaf(AggregateUDF::from(Collect {
            store: Arc::clone(&store),
            native: array_agg_udaf(),
            distinct,
        }));
    }
}

struct Store {
    runtime: Arc<RuntimeEnv>,
    flush_bytes: usize,
    next: AtomicU64,
    tokens: Mutex<HashMap<u64, Files>>,
    spill: Mutex<Option<Arc<dyn SpillFile>>>,
    spilled: Arc<AtomicU64>,
    cancellation: tokio_util::sync::CancellationToken,
    deadline: tokio::time::Instant,
}

struct Collect {
    store: Arc<Store>,
    native: Arc<AggregateUDF>,
    distinct: bool,
}

impl fmt::Debug for Collect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("GraphLake collect")
    }
}

impl PartialEq for Collect {
    fn eq(&self, other: &Self) -> bool {
        self.distinct == other.distinct && Arc::ptr_eq(&self.store, &other.store)
    }
}

impl Eq for Collect {}

impl Hash for Collect {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.store).hash(state);
        self.distinct.hash(state);
    }
}

impl AggregateUDFImpl for Collect {
    fn name(&self) -> &str {
        if self.distinct {
            "__graphlake_collect_distinct"
        } else {
            "__graphlake_collect"
        }
    }

    fn signature(&self) -> &Signature {
        self.native.signature()
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        self.native.return_type(args)
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        self.native.order_sensitivity()
    }

    fn with_beneficial_ordering(
        self: Arc<Self>,
        _ordered: bool,
    ) -> Result<Option<Arc<dyn AggregateUDFImpl>>> {
        // Each run carries its own ordering values. The native accumulator
        // still sorts and merges those runs even when the input is ordered.
        Ok(Some(self))
    }

    fn state_fields(&self, args: StateFieldsArgs<'_>) -> Result<Vec<FieldRef>> {
        let token = Arc::new(Field::new(
            format!("{}[spill]", args.name),
            DataType::UInt64,
            true,
        ));
        let mut fields = if self.distinct {
            [
                ("values", args.input_fields[0].data_type().clone()),
                ("order", DataType::Binary),
            ]
            .into_iter()
            .map(|(name, ty)| {
                Arc::new(Field::new_list(
                    format!("{}[{name}]", args.name),
                    Field::new_list_field(ty, true),
                    true,
                ))
            })
            .collect()
        } else {
            self.native.state_fields(args)?
        };
        fields.push(token);
        Ok(fields)
    }

    fn accumulator(&self, args: AccumulatorArgs<'_>) -> Result<Box<dyn Accumulator>> {
        let distinct_type = self
            .distinct
            .then(|| args.expr_fields[0].data_type().clone());
        let factory = if self.distinct {
            let ty = args.expr_fields[0].data_type().clone();
            let sort = args
                .order_bys
                .iter()
                .map(|sort| {
                    Ok(SortField::new_with_options(
                        sort.expr.data_type(args.schema)?,
                        sort.options,
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            let converter = if sort.is_empty() {
                None
            } else {
                Some(Arc::new(RowConverter::new(sort)?))
            };
            Box::new(move || {
                Ok(
                    Box::new(distinct::Distinct::new(ty.clone(), converter.clone()))
                        as Box<dyn Accumulator>,
                )
            }) as Factory
        } else {
            self.native_factory(args)
        };
        let reservation = (self.distinct
            && self.store.runtime.disk_manager.max_temp_directory_size() > 0)
            .then(|| {
                MemoryConsumer::new("Cypher distinct collect run")
                    .register(&self.store.runtime.memory_pool)
            });
        Ok(Box::new(CollectAccumulator {
            store: Arc::clone(&self.store),
            inner: factory()?,
            factory,
            files: Vec::new(),
            dirty: false,
            distinct_type,
            reservation,
        }))
    }
}

impl Collect {
    fn native_factory(&self, args: AccumulatorArgs<'_>) -> Factory {
        let AccumulatorArgs {
            return_field,
            schema,
            ignore_nulls,
            order_bys,
            is_reversed,
            name,
            is_distinct,
            exprs,
            expr_fields,
        } = args;
        let schema = schema.clone();
        let order_bys = order_bys.to_vec();
        let name = name.to_owned();
        let exprs = exprs.to_vec();
        let expr_fields = expr_fields.to_vec();
        let native = Arc::clone(&self.native);
        Box::new(move || {
            native.accumulator(AccumulatorArgs {
                return_field: Arc::clone(&return_field),
                schema: &schema,
                ignore_nulls,
                order_bys: &order_bys,
                is_reversed,
                name: &name,
                is_distinct,
                exprs: &exprs,
                expr_fields: &expr_fields,
            })
        })
    }
}

struct CollectAccumulator {
    store: Arc<Store>,
    inner: Box<dyn Accumulator>,
    factory: Factory,
    files: Files,
    dirty: bool,
    distinct_type: Option<DataType>,
    reservation: Option<MemoryReservation>,
}

impl fmt::Debug for CollectAccumulator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GraphLake collect accumulator")
            .field("files", &self.files.len())
            .finish()
    }
}

impl CollectAccumulator {
    fn maybe_flush(&mut self) -> Result<()> {
        if self.store.runtime.disk_manager.max_temp_directory_size() > 0 {
            let size = self.inner.size();
            if size >= self.store.flush_bytes
                || self
                    .reservation
                    .as_ref()
                    .is_some_and(|r| r.try_resize(size).is_err())
            {
                self.flush()?;
            }
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if !self.dirty {
            return Ok(());
        }
        let arrays = self
            .inner
            .state()?
            .iter()
            .map(|value| value.to_array())
            .collect::<Result<Vec<_>>>()?;
        let schema = Arc::new(Schema::new(
            arrays
                .iter()
                .enumerate()
                .map(|(i, array)| Field::new(format!("state_{i}"), array.data_type().clone(), true))
                .collect::<Vec<_>>(),
        ));
        let batch = RecordBatch::try_new(schema, arrays)?;
        let mut shared = self.store.spill.lock().expect("collect spill file");
        if shared.is_none() {
            *shared = Some(
                self.store
                    .runtime
                    .disk_manager
                    .create_tmp_file("spilling Cypher collect")?,
            );
        }
        let file = shared.as_ref().expect("collect spill file");
        let offset = file.size().expect("local spill file size");
        let mut writer = StreamWriter::try_new(file.open_writer()?, &batch.schema())?;
        writer.write(&batch)?;
        writer.into_inner()?.finish()?;
        let length = file.size().expect("local spill file size") - offset;
        self.store.spilled.fetch_add(length, Ordering::Relaxed);
        self.files.push(Run {
            file: file.clone(),
            offset,
            length,
        });
        self.inner = (self.factory)()?;
        if let Some(reservation) = &self.reservation {
            reservation.free();
        }
        self.dirty = false;
        Ok(())
    }

    fn merge_inline(&mut self, state: &[ArrayRef]) -> Result<()> {
        self.inner.merge_batch(state)?;
        self.dirty = true;
        self.maybe_flush()
    }
}

impl Accumulator for CollectAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if self.store.runtime.disk_manager.max_temp_directory_size() > 0
            && let Some(first) = values.first()
            && first.len() > 1
        {
            let bytes = values
                .iter()
                .map(|array| {
                    array
                        .to_data()
                        .get_slice_memory_size()
                        .unwrap_or_else(|_| array.get_array_memory_size())
                })
                .sum::<usize>();
            if bytes > self.store.flush_bytes {
                let chunk = first
                    .len()
                    .saturating_mul(self.store.flush_bytes)
                    .checked_div(bytes)
                    .unwrap_or_default()
                    .max(1);
                for offset in (0..first.len()).step_by(chunk) {
                    let rows = chunk.min(first.len() - offset);
                    let values = values
                        .iter()
                        .map(|array| array.slice(offset, rows))
                        .collect::<Vec<_>>();
                    self.inner.update_batch(&values)?;
                    self.dirty = true;
                    self.maybe_flush()?;
                }
                return Ok(());
            }
        }
        self.inner.update_batch(values)?;
        self.dirty = true;
        self.maybe_flush()
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let Some((tokens, state)) = states.split_last() else {
            return Ok(());
        };
        let tokens = tokens
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("collect spill tokens");
        if tokens.null_count() == tokens.len() {
            return self.merge_inline(state);
        }
        for (row, token) in tokens.iter().enumerate() {
            if let Some(token) = token {
                self.flush()?;
                let files = self
                    .store
                    .tokens
                    .lock()
                    .expect("collect spill state")
                    .remove(&token);
                let Some(files) = files else {
                    return exec_err!("missing collect spill state");
                };
                self.files.extend(files);
            } else {
                let row = state
                    .iter()
                    .map(|column| column.slice(row, 1))
                    .collect::<Vec<_>>();
                self.merge_inline(&row)?;
            }
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        if self.files.is_empty() {
            let mut state = self.inner.state()?;
            state.push(ScalarValue::UInt64(None));
            return Ok(state);
        }
        self.flush()?;
        let mut state = self.inner.state()?;
        let token = self.store.next.fetch_add(1, Ordering::Relaxed);
        self.store
            .tokens
            .lock()
            .expect("collect spill state")
            .insert(token, std::mem::take(&mut self.files));
        state.push(ScalarValue::UInt64(Some(token)));
        Ok(state)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.files.is_empty() {
            return self.inner.evaluate();
        }
        if let Some(ty) = self.distinct_type.clone() {
            self.flush()?;
            return spill::finish(self.store.clone(), std::mem::take(&mut self.files), &ty);
        }
        let mut merged = (self.factory)()?;
        let budget = MemoryConsumer::new("materializing a Cypher collect result")
            .register(&self.store.runtime.memory_pool);
        for file in &self.files {
            let reader = file.reader()?;
            for batch in reader {
                let batch = batch?;
                budget.try_grow(ipc_batch_bytes(&batch))?;
                merged.merge_batch(batch.columns())?;
                budget.try_resize(merged.size())?;
            }
        }
        if self.dirty {
            let tail = self
                .inner
                .state()?
                .iter()
                .map(|value| value.to_array())
                .collect::<Result<Vec<_>>>()?;
            merged.merge_batch(&tail)?;
            budget.try_resize(merged.size())?;
        }
        // Arrow exposes a contiguous list value. Its final assembly needs
        // room for the result alongside the merged accumulator state.
        budget.try_grow(merged.size())?;
        merged.evaluate()
    }

    fn size(&self) -> usize {
        // Spillable distinct runs own their reservation. DataFusion's
        // global aggregate charges positive growth without returning
        // reductions, so reporting both would accumulate stale charges.
        let inner = if self.reservation.is_none() {
            self.inner.size()
        } else {
            0
        };
        size_of::<Self>() + inner + self.files.capacity() * size_of::<Run>()
    }
}
