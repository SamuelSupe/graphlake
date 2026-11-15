use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use anyhow::{Context, Result, ensure};
use datafusion::{
    arrow::{
        array::{ArrayRef, BooleanArray, RecordBatch},
        compute::{and, cast, filter, filter_record_batch, not, or},
        datatypes::{Schema, SchemaRef},
        row::{RowConverter, Rows, SortField},
    },
    common::TableReference,
    execution::memory_pool::{MemoryConsumer, MemoryReservation},
    prelude::SessionContext,
};
use futures::TryStreamExt;

use crate::{
    Catalog,
    engine::{QueryScope, graph_frame},
    source::snapshot::Buffer,
};

pub(super) struct OutputLimit {
    pub(super) count: u64,
    pub(super) group: Option<String>,
    pub(super) distinct: Option<String>,
}

pub(crate) struct Repeat {
    pub(super) table: String,
    pub(super) seed: String,
    pub(super) seen: String,
    pub(super) initial: String,
    pub(super) body: String,
    pub(super) checked: String,
    pub(super) after: bool,
    pub(super) emitted: Option<String>,
    pub(super) eligible: Option<String>,
    pub(super) post_eligible: Option<String>,
    pub(super) emit_before: bool,
    pub(super) limit: Option<OutputLimit>,
    pub(super) nested: Vec<Repeat>,
    pub(super) checked_nested: Vec<Repeat>,
}

impl Repeat {
    pub(crate) async fn register(
        &self,
        catalog: &Catalog,
        context: &SessionContext,
        scope: &QueryScope,
        explain_only: bool,
    ) -> Result<MemoryReservation> {
        for name in [&self.table, &self.seed] {
            ensure!(
                !catalog.nodes.contains_key(name) && !catalog.edges.contains_key(name),
                "graph label collides with internal Gremlin loop name"
            );
        }
        let initial = graph_frame(context, &self.initial).await?;
        let schema = Arc::new(Schema::new(
            initial
                .schema()
                .fields()
                .iter()
                .map(|field| field.as_ref().clone().with_nullable(true))
                .collect::<Vec<_>>(),
        ));
        let output = TableReference::bare(self.table.as_str());
        if explain_only {
            let (provider, budget) = buffer(scope).finish_schema(schema)?;
            context.register_table(output, provider)?;
            return Ok(budget);
        }
        let mut quota = Quota::new(self.limit.as_ref(), &schema, scope)?;
        if quota.reached() {
            let (provider, budget) = buffer(scope).finish(schema, &scope.reads)?;
            context.register_table(output, provider)?;
            return Ok(budget);
        }
        let seed = TableReference::bare(self.seed.as_str());
        let mut finished = buffer(scope);
        let mut frontier = buffer(scope);
        let mut initial = initial.execute_stream().await?;
        let mut active = false;
        while let Some(batch) = initial.try_next().await? {
            scope.check()?;
            active |= batch.num_rows() > 0;
            push(&mut frontier, &schema, batch)?;
        }
        drop(initial);
        if (!self.after || self.emit_before) && active {
            let budget = install(context, scope, seed.clone(), &schema, frontier)?;
            (frontier, active) = self
                .partition(
                    catalog,
                    context,
                    scope,
                    &schema,
                    (&mut finished, &mut quota),
                    true,
                )
                .await?;
            context.deregister_table(seed.clone())?;
            drop(budget);
        }
        let mut rounds = 0;
        while active {
            anyhow::ensure!(
                rounds < super::MAX_REPEAT_ROUNDS,
                "repeat() is limited to 256 iterations"
            );
            rounds += 1;
            scope.check()?;
            let budget = install(context, scope, seed.clone(), &schema, frontier)?;
            let mut nested_budget = Vec::new();
            for nested in &self.nested {
                nested_budget
                    .push(Box::pin(nested.register(catalog, context, scope, false)).await?);
            }
            let mut body = graph_frame(context, &self.body)
                .await?
                .execute_stream()
                .await?;
            let mut next = buffer(scope);
            let mut produced = false;
            while let Some(batch) = body.try_next().await? {
                scope.check()?;
                produced |= batch.num_rows() > 0;
                push(&mut next, &schema, batch)?;
            }
            drop(body);
            for nested in &self.nested {
                context.deregister_table(TableReference::bare(nested.table.as_str()))?;
            }
            drop(nested_budget);
            context.deregister_table(seed.clone())?;
            drop(budget);
            if !produced {
                break;
            }
            let budget = install(context, scope, seed.clone(), &schema, next)?;
            (frontier, active) = self
                .partition(
                    catalog,
                    context,
                    scope,
                    &schema,
                    (&mut finished, &mut quota),
                    false,
                )
                .await?;
            context.deregister_table(seed.clone())?;
            drop(budget);
            tokio::task::yield_now().await;
        }
        let (provider, budget) = finished.finish(schema, &scope.reads)?;
        context.register_table(output, provider)?;
        Ok(budget)
    }

    async fn partition(
        &self,
        catalog: &Catalog,
        context: &SessionContext,
        scope: &QueryScope,
        schema: &SchemaRef,
        (finished, quota): (&mut Buffer, &mut Quota),
        initial: bool,
    ) -> Result<(Buffer, bool)> {
        let mut saturated = false;
        let mut frontier = buffer(scope);
        let mut active = false;
        let mut budget = Vec::new();
        for nested in &self.checked_nested {
            budget.push(Box::pin(nested.register(catalog, context, scope, false)).await?);
        }
        let mut checked = graph_frame(context, &self.checked)
            .await?
            .execute_stream()
            .await?;
        while let Some(batch) = checked.try_next().await? {
            scope.check()?;
            let flag = |name: &str| -> Result<&BooleanArray> {
                batch
                    .column_by_name(name)
                    .and_then(|array| array.as_any().downcast_ref::<BooleanArray>())
                    .context("Gremlin loop predicate did not return a Boolean")
            };
            let inactive =
                (initial && self.after).then(|| BooleanArray::from(vec![false; batch.num_rows()]));
            let done = match &inactive {
                Some(inactive) => inactive,
                None => flag(&self.seen)?,
            };
            let emitted = self
                .emitted
                .as_deref()
                .filter(|_| !initial || self.emit_before)
                .map(flag)
                .transpose()?;
            let columns = schema
                .fields()
                .iter()
                .map(|field| {
                    batch
                        .column_by_name(field.name())
                        .cloned()
                        .with_context(|| format!("missing Gremlin work column {}", field.name()))
                })
                .collect::<Result<Vec<_>>>()?;
            let rows = conform(schema, &columns)?;
            let output = emitted.map(|emitted| or(done, emitted)).transpose()?;
            let output = output.as_ref().unwrap_or(done);
            let eligible = self
                .eligible
                .as_deref()
                .map(|name| -> Result<_> { Ok(and(output, flag(name)?)?) })
                .transpose()?;
            let mask = eligible.as_ref().unwrap_or(output);
            let emitted = filter_record_batch(&rows, mask)?;
            let post = self.post_eligible.as_deref().map(flag).transpose()?;
            let (emitted, group_reached) = quota.take(emitted, &batch, mask, post)?;
            saturated |= group_reached;
            push(finished, schema, emitted)?;
            if quota.reached() {
                active = false;
                break;
            }
            let next = filter_record_batch(&rows, &not(done)?)?;
            active |= next.num_rows() > 0;
            push(&mut frontier, schema, next)?;
        }
        drop(checked);
        for nested in &self.checked_nested {
            context.deregister_table(TableReference::bare(nested.table.as_str()))?;
        }
        if saturated
            && active
            && let Capacity::Groups(groups) = &quota.capacity
        {
            return groups.prune(context, scope, schema, frontier).await;
        }
        Ok((frontier, active))
    }
}

enum Capacity {
    Unlimited,
    Global(u64),
    Groups(GroupQuota),
}

struct Quota {
    capacity: Capacity,
    distinct: Option<Distinct>,
}

struct Distinct {
    column: String,
    seen: HashSet<Vec<u8>>,
    budget: MemoryReservation,
}

impl Quota {
    fn new(limit: Option<&OutputLimit>, schema: &Schema, scope: &QueryScope) -> Result<Self> {
        let Some(limit) = limit else {
            return Ok(Self {
                capacity: Capacity::Unlimited,
                distinct: None,
            });
        };
        let capacity = if let Some(group) = &limit.group
            && limit.count > 0
        {
            let index = schema.index_of(group)?;
            Capacity::Groups(GroupQuota {
                index,
                count: limit.count,
                converter: RowConverter::new(vec![SortField::new(
                    schema.field(index).data_type().clone(),
                )])?,
                remaining: HashMap::new(),
                budget: MemoryConsumer::new("GraphLake Gremlin per-group limit")
                    .register(&scope.runtime.memory_pool),
            })
        } else {
            Capacity::Global(limit.count)
        };
        let distinct = limit.distinct.as_ref().map(|column| Distinct {
            column: column.clone(),
            seen: HashSet::new(),
            budget: MemoryConsumer::new("GraphLake Gremlin distinct limit")
                .register(&scope.runtime.memory_pool),
        });
        Ok(Self { capacity, distinct })
    }

    fn reached(&self) -> bool {
        matches!(self.capacity, Capacity::Global(0))
    }

    fn take(
        &mut self,
        batch: RecordBatch,
        checked: &RecordBatch,
        mask: &BooleanArray,
        post: Option<&BooleanArray>,
    ) -> Result<(RecordBatch, bool)> {
        let keys = self
            .distinct
            .as_ref()
            .map(|distinct| -> Result<Rows> {
                let keys = checked
                    .column_by_name(&distinct.column)
                    .context("missing Gremlin distinct keys")?;
                let keys = filter(keys, mask)?;
                ensure!(
                    keys.null_count() == 0,
                    "missing emitted Gremlin distinct key"
                );
                Ok(
                    RowConverter::new(vec![SortField::new(keys.data_type().clone())])?
                        .convert_columns(&[keys])?,
                )
            })
            .transpose()?;
        let mut distinct = self.distinct.as_mut().zip(keys.as_ref());
        let post = post.map(|post| filter(post, mask)).transpose()?;
        let post = post.as_ref().map(|post| {
            post.as_any()
                .downcast_ref::<BooleanArray>()
                .expect("post-dedup predicate")
        });
        let mut unique = |row| -> Result<bool> {
            let Some((state, keys)) = distinct.as_mut() else {
                return Ok(true);
            };
            let encoded = keys.row(row);
            let key = encoded.as_ref();
            if state.seen.contains(key) {
                return Ok(false);
            }
            state.budget.try_grow(key.len() + 64)?;
            state.seen.insert(key.to_vec());
            // A duplicate stays discarded even when the first traverser
            // failed a filter after dedup().
            Ok(post.is_none_or(|mask| mask.value(row)))
        };
        match &mut self.capacity {
            Capacity::Unlimited => Ok((batch, false)),
            Capacity::Global(remaining) if keys.is_none() => {
                let take = (*remaining).min(batch.num_rows() as u64);
                *remaining -= take;
                Ok((batch.slice(0, take as usize), false))
            }
            Capacity::Global(remaining) => {
                let mut keep = Vec::with_capacity(batch.num_rows());
                for row in 0..batch.num_rows() {
                    let accepted = *remaining > 0 && unique(row)?;
                    keep.push(accepted);
                    *remaining -= u64::from(accepted);
                }
                Ok((
                    filter_record_batch(&batch, &BooleanArray::from(keep))?,
                    false,
                ))
            }
            Capacity::Groups(groups) => groups.take(batch, unique),
        }
    }
}

struct GroupQuota {
    index: usize,
    count: u64,
    converter: RowConverter,
    remaining: HashMap<Vec<u8>, u64>,
    budget: MemoryReservation,
}

impl GroupQuota {
    fn keys(&self, batch: &RecordBatch) -> Result<Rows> {
        Ok(self
            .converter
            .convert_columns(std::slice::from_ref(batch.column(self.index)))?)
    }

    fn take(
        &mut self,
        batch: RecordBatch,
        mut unique: impl FnMut(usize) -> Result<bool>,
    ) -> Result<(RecordBatch, bool)> {
        let keys = self.keys(&batch)?;
        let mut keep = Vec::with_capacity(batch.num_rows());
        let mut saturated = false;
        for (row, key) in keys.iter().enumerate() {
            let key = key.as_ref();
            let count = if let Some(count) = self.remaining.get_mut(key) {
                count
            } else {
                self.budget.try_grow(key.len() + 64)?;
                self.remaining.entry(key.to_vec()).or_insert(self.count)
            };
            let accepted = *count > 0 && unique(row)?;
            keep.push(accepted);
            if accepted {
                *count -= 1;
                saturated |= *count == 0;
            }
        }
        Ok((
            filter_record_batch(&batch, &BooleanArray::from(keep))?,
            saturated,
        ))
    }

    async fn prune(
        &self,
        context: &SessionContext,
        scope: &QueryScope,
        schema: &SchemaRef,
        frontier: Buffer,
    ) -> Result<(Buffer, bool)> {
        // A group may have queued continuations in earlier batches of this
        // round. Drop them before evaluating the next body.
        let (provider, _budget) = frontier.finish(Arc::clone(schema), &scope.reads)?;
        let mut stream = context.read_table(provider)?.execute_stream().await?;
        let mut frontier = buffer(scope);
        let mut active = false;
        while let Some(batch) = stream.try_next().await? {
            scope.check()?;
            let keys = self.keys(&batch)?;
            let keep = BooleanArray::from_iter(
                keys.iter()
                    .map(|key| Some(self.remaining.get(key.as_ref()) != Some(&0))),
            );
            let batch = filter_record_batch(&batch, &keep)?;
            active |= batch.num_rows() > 0;
            push(&mut frontier, schema, batch)?;
        }
        Ok((frontier, active))
    }
}

fn buffer(scope: &QueryScope) -> Buffer {
    Buffer::new(
        &scope.runtime.memory_pool,
        Some(&scope.runtime.disk_manager),
    )
}

fn push(buffer: &mut Buffer, schema: &SchemaRef, batch: RecordBatch) -> Result<()> {
    if batch.num_rows() > 0 {
        buffer.push(conform(schema, batch.columns())?)?;
    }
    Ok(())
}

fn conform(schema: &SchemaRef, columns: &[ArrayRef]) -> Result<RecordBatch> {
    // SQL may choose Utf8 or Utf8View for a slot depending on whether its
    // current producer is a literal, a typed property, or a view.
    let columns = schema
        .fields()
        .iter()
        .zip(columns)
        .map(|(field, column)| {
            if field.data_type() == column.data_type() {
                Ok(Arc::clone(column))
            } else {
                cast(column.as_ref(), field.data_type())
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(RecordBatch::try_new(Arc::clone(schema), columns)?)
}

fn install(
    context: &SessionContext,
    scope: &QueryScope,
    name: TableReference,
    schema: &SchemaRef,
    buffer: Buffer,
) -> Result<MemoryReservation> {
    let (table, budget) = buffer.finish(Arc::clone(schema), &scope.reads)?;
    context.register_table(name, table)?;
    Ok(budget)
}
