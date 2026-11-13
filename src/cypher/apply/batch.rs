use std::{
    collections::{HashSet, VecDeque},
    sync::Arc,
};

use anyhow::{Context, Result, ensure};
use datafusion::{
    arrow::{
        array::{RecordBatch, UInt32Array},
        compute::take,
        datatypes::SchemaRef,
        row::{RowConverter, SortField},
    },
    catalog::TableProvider,
    common::{Column, TableReference},
    dataframe::DataFrame,
    execution::memory_pool::MemoryReservation,
    physical_plan::execute_stream,
    prelude::{SessionContext, col},
};
use futures::TryStreamExt;

use super::{Apply, Buffer, Catalog, QueryScope, replace};
use crate::engine::graph_frame;

pub(crate) struct Batch {
    pub(crate) order: Vec<String>,
    pub(crate) keys: String,
    pub(crate) groups: String,
    pub(crate) group_rank: Option<String>,
    pub(crate) push_group_limit: bool,
    pub(crate) table: String,
}

struct CachedBatch {
    keys: HashSet<Vec<u8>>,
    table: Arc<dyn TableProvider>,
    budget: MemoryReservation,
    bytes: usize,
}

impl Apply {
    pub(super) async fn apply_batches(
        &self,
        catalog: &Catalog,
        context: &SessionContext,
        scope: &QueryScope,
        outer: DataFrame,
        output: &mut Buffer,
        (plan, group_schema): (&Batch, &SchemaRef),
    ) -> Result<()> {
        const MAX_SEEDS: usize = 16;
        let mut remaining = self.limit.unwrap_or(usize::MAX);
        if remaining == 0 {
            return Ok(());
        }
        let seed = TableReference::bare(self.seed.as_str());
        let groups = TableReference::bare(plan.table.as_str());
        let cache_limit = (scope.options.memory_bytes / 8).min(8 * 1024 * 1024);
        let mut memo = VecDeque::<CachedBatch>::new();
        let mut cached_bytes = 0usize;
        let mut outer = outer.execute_stream().await?;
        while let Some(batch) = outer.try_next().await? {
            let mut offset = 0;
            while offset < batch.num_rows() {
                scope.check()?;
                let mut rows = MAX_SEEDS.min(batch.num_rows() - offset);
                let mut input = batch.slice(offset, rows);
                replace(context, seed.clone(), batch.schema(), vec![input.clone()])?;
                let keys = graph_frame(context, &plan.keys).await?.collect().await?;
                ensure!(
                    keys.iter().map(RecordBatch::num_rows).sum::<usize>() == rows,
                    "CALL batch key projection changed the input row count"
                );
                let key_type = keys
                    .first()
                    .context("missing CALL batch keys")?
                    .column(0)
                    .data_type();
                let converter = RowConverter::new(vec![SortField::new(key_type.clone())])?;
                let keys = keys
                    .iter()
                    .map(|batch| converter.convert_columns(batch.columns()))
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                let keys = keys.iter().flat_map(|rows| rows.iter()).collect::<Vec<_>>();
                let cached = |key: &[u8]| memo.iter().any(|cached| cached.keys.contains(key));
                if self.limit.is_some()
                    && let Some(first_miss) = keys.iter().position(|key| !cached(key.as_ref()))
                {
                    // A fresh group can satisfy the whole remaining limit.
                    // Reused groups can still be replayed as a batch.
                    rows = first_miss.max(1);
                    input = input.slice(0, rows);
                }
                let mut needed = HashSet::new();
                let mut missing = HashSet::new();
                let mut indexes = Vec::new();
                for (index, key) in keys.iter().take(rows).enumerate() {
                    let key = key.as_ref();
                    if !needed.insert(key.to_vec()) {
                        continue;
                    }
                    if !cached(key) {
                        missing.insert(key.to_vec());
                        indexes.push(index as u32);
                    }
                }
                let mut tables = memo
                    .iter()
                    .filter(|cached| !cached.keys.is_disjoint(&needed))
                    .map(|cached| Arc::clone(&cached.table))
                    .collect::<Vec<_>>();
                let fresh = if indexes.is_empty() {
                    None
                } else {
                    let indexes = UInt32Array::from(indexes);
                    let fields = input
                        .columns()
                        .iter()
                        .map(|column| take(column, &indexes, None))
                        .collect::<std::result::Result<Vec<_>, _>>()?;
                    let selected = RecordBatch::try_new(input.schema(), fields)?;
                    replace(context, seed.clone(), selected.schema(), vec![selected])?;
                    let prefix = self.limit.map(|_| remaining);
                    let _nested_budget = self
                        .prepare_inner(
                            catalog,
                            context,
                            scope,
                            false,
                            plan.push_group_limit && prefix.is_some_and(|n| n <= 64),
                        )
                        .await?;
                    let mut frame = graph_frame(context, &plan.groups).await?;
                    if let Some(left) = prefix {
                        if let Some(rank) = &plan.group_rank {
                            // A bounded sort cannot spill. Read the ordered prefix
                            // from a spillable sort instead.
                            frame =
                                frame.sort(vec![col(Column::from_name(rank)).sort(true, false)])?;
                        } else if plan.push_group_limit {
                            frame = frame.limit(0, Some(left))?;
                        }
                    }
                    let task = Arc::new(frame.task_ctx());
                    let physical = frame.create_physical_plan().await?;
                    let mut stream = execute_stream(Arc::clone(&physical), task)?;
                    let mut buffer = Buffer::new(
                        &scope.runtime.memory_pool,
                        Some(&scope.runtime.disk_manager),
                    );
                    let mut bytes = missing
                        .iter()
                        .map(|key| key.len().saturating_add(64))
                        .sum::<usize>();
                    let mut left = prefix.unwrap_or(usize::MAX);
                    while let Some(result) = stream.try_next().await? {
                        scope.check()?;
                        let result = result.slice(0, result.num_rows().min(left));
                        left -= result.num_rows();
                        bytes = bytes.saturating_add(result.get_array_memory_size());
                        buffer.push(result)?;
                        if left == 0 {
                            break;
                        }
                    }
                    drop(stream);
                    scope.record_spill(physical.as_ref());
                    self.remove_nested(context)?;
                    let (table, budget) = buffer.finish(Arc::clone(group_schema), &scope.reads)?;
                    tables.push(Arc::clone(&table));
                    Some(CachedBatch {
                        keys: missing,
                        table,
                        budget,
                        bytes,
                    })
                };
                let mut tables = tables.into_iter().map(|table| context.read_table(table));
                let mut group = tables.next().context("missing CALL batch result")??;
                for table in tables {
                    group = group.union(table?)?;
                }
                context.deregister_table(groups.clone())?;
                context.register_table(groups.clone(), group.into_view())?;
                replace(context, seed.clone(), batch.schema(), vec![input])?;
                // DataFusion's TopK sort cannot spill wide rows. Keep the
                // chunk's sort spillable and cap the stream after sorting.
                let mut inner = graph_frame(context, &self.inner)
                    .await?
                    .execute_stream()
                    .await?;
                while let Some(result) = inner.try_next().await? {
                    scope.check()?;
                    let rows = remaining.min(result.num_rows());
                    output.push(result.slice(0, rows))?;
                    remaining -= rows;
                    if remaining == 0 {
                        return Ok(());
                    }
                }
                drop(inner);
                context.deregister_table(groups.clone())?;
                if let Some(fresh) = fresh
                    && fresh.bytes <= cache_limit
                {
                    while memo.len() >= 8 || cached_bytes.saturating_add(fresh.bytes) > cache_limit
                    {
                        cached_bytes -= memo.pop_front().expect("pop CALL cache").bytes;
                    }
                    let key_bytes = fresh
                        .keys
                        .iter()
                        .map(|key| key.len().saturating_add(64))
                        .sum();
                    if fresh.budget.try_grow(key_bytes).is_ok() {
                        cached_bytes += fresh.bytes;
                        memo.push_back(fresh);
                    }
                }
                offset += rows;
                tokio::task::yield_now().await;
            }
        }
        Ok(())
    }
}
