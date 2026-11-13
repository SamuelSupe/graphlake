mod batch;
pub(crate) use batch::Batch;
mod lookup;
pub(crate) use lookup::Lookup;
pub(super) use lookup::scalar_projection;

use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};

use anyhow::{Context, Result, ensure};
use datafusion::{
    arrow::{
        array::{RecordBatch, UInt32Array, UInt64Array},
        compute::take,
        datatypes::{DataType, Field, Schema, SchemaRef},
        row::{RowConverter, SortField},
    },
    catalog::TableProvider,
    common::{Column, TableReference},
    datasource::MemTable,
    execution::memory_pool::MemoryReservation,
    prelude::{SessionContext, col},
};
use futures::TryStreamExt;

use crate::{
    Catalog,
    engine::{QueryScope, graph_frame},
    source::snapshot::Buffer,
};

/// A returning CALL with outer imports. The outer input and every subquery share the
/// query's source snapshots.
pub(crate) struct Apply {
    pub(crate) table: String,
    pub(crate) seed: String,
    pub(crate) outer: String,
    pub(crate) inner: String,
    pub(crate) nested: Vec<Apply>,
    pub(crate) lookups: Vec<Lookup>,
    pub(crate) limit: Option<usize>,
    pub(crate) mode: Mode,
}

pub(crate) enum Mode {
    Row {
        imports: Vec<String>,
        sequence: String,
    },
    Batch(Batch),
}

pub(crate) async fn pin_tables(
    context: &SessionContext,
    labels: &BTreeSet<String>,
    scope: &QueryScope,
) -> Result<Vec<MemoryReservation>> {
    let mut reservations = Vec::new();
    for label in labels {
        let reference = TableReference::bare(label.as_str());
        let mut stream = context
            .table(reference.clone())
            .await?
            .execute_stream()
            .await?;
        let mut buffer = crate::source::snapshot::Buffer::new(
            &scope.runtime.memory_pool,
            Some(&scope.runtime.disk_manager),
        );
        while let Some(batch) = stream.try_next().await? {
            scope.check()?;
            buffer.push(batch)?;
        }
        let (table, budget) = buffer.finish(stream.schema(), &scope.reads)?;
        context.deregister_table(reference.clone())?;
        context.register_table(reference, table)?;
        reservations.push(budget);
    }
    Ok(reservations)
}

impl Apply {
    pub(super) fn has_lookup(&self) -> bool {
        !self.lookups.is_empty() || self.nested.iter().any(Self::has_lookup)
    }

    pub(crate) fn order(&self) -> &[String] {
        match &self.mode {
            Mode::Row { sequence, .. } => std::slice::from_ref(sequence),
            Mode::Batch(batch) => &batch.order,
        }
    }

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
                "graph label collides with internal CALL name"
            );
        }
        let outer = graph_frame(context, &self.outer).await?;
        let outer_schema = Arc::clone(outer.schema().inner());
        let seed = TableReference::bare(self.seed.as_str());
        replace(context, seed.clone(), Arc::clone(&outer_schema), Vec::new())?;
        let _nested_schema = self
            .prepare_inner(catalog, context, scope, true, false)
            .await?;
        let groups = if let Mode::Batch(batch) = &self.mode {
            let frame = graph_frame(context, &batch.groups).await?;
            let schema = Arc::clone(frame.schema().inner());
            replace(
                context,
                TableReference::bare(batch.table.as_str()),
                Arc::clone(&schema),
                Vec::new(),
            )?;
            Some((batch, schema))
        } else {
            None
        };
        let inner = graph_frame(context, &self.inner).await?;
        self.remove_nested(context)?;
        let schema = match &self.mode {
            Mode::Batch(_) => Arc::clone(inner.schema().inner()),
            Mode::Row { sequence, .. } => {
                let fields = outer_schema
                    .fields()
                    .iter()
                    .chain(inner.schema().fields())
                    .cloned()
                    .chain([Arc::new(Field::new(sequence, DataType::UInt64, false))])
                    .collect::<Vec<_>>();
                Arc::new(Schema::new(fields))
            }
        };
        let output = TableReference::bare(self.table.as_str());
        let buffer = || {
            Buffer::new(
                &scope.runtime.memory_pool,
                Some(&scope.runtime.disk_manager),
            )
        };
        let mut results = buffer();
        if let Some((batch, schema)) = groups.as_ref()
            && !explain_only
        {
            self.apply_batches(
                catalog,
                context,
                scope,
                outer,
                &mut results,
                (batch, schema),
            )
            .await?;
        } else if !explain_only
            && self.limit != Some(0)
            && let Mode::Row { imports, .. } = &self.mode
        {
            let indices = imports
                .iter()
                .map(|name| outer_schema.index_of(name))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let converter = RowConverter::new(
                indices
                    .iter()
                    .map(|&i| SortField::new(outer_schema.field(i).data_type().clone()))
                    .collect(),
            )?;
            let inner_schema = Arc::clone(inner.schema().inner());
            let cache_limit = (scope.options.memory_bytes / 8).min(8 * 1024 * 1024);
            let mut memo = HashMap::new();
            let mut oversized: Option<(Vec<u8>, Arc<dyn TableProvider>, MemoryReservation)> = None;
            let mut cached_bytes = 0usize;
            let mut remaining = self.limit;
            let mut sequence = 0u64;
            let mut outer = outer.execute_stream().await?;
            'outer: while let Some(batch) = outer.try_next().await? {
                let keys = converter.convert_columns(
                    &indices
                        .iter()
                        .map(|&i| Arc::clone(batch.column(i)))
                        .collect::<Vec<_>>(),
                )?;
                for (index, key) in keys.iter().enumerate() {
                    scope.check()?;
                    let table = if let Some((_, table, _)) = oversized
                        .as_ref()
                        .filter(|(saved, _, _)| saved.as_slice() == key.as_ref())
                    {
                        Arc::clone(table)
                    } else if let Some((table, _)) = memo.get(key.as_ref()) {
                        oversized = None;
                        Arc::clone(table)
                    } else {
                        oversized = None;
                        replace(
                            context,
                            seed.clone(),
                            Arc::clone(&outer_schema),
                            vec![batch.slice(index, 1)],
                        )?;
                        let _nested_budget = self
                            .prepare_inner(
                                catalog,
                                context,
                                scope,
                                false,
                                remaining.is_some_and(|n| n <= 64),
                            )
                            .await?;
                        let mut inner = graph_frame(context, &self.inner)
                            .await?
                            .execute_stream()
                            .await?;
                        let mut saved = buffer();
                        let key_bytes = key.as_ref().len().saturating_add(64);
                        let mut bytes = key_bytes;
                        let mut budget_rows = remaining;
                        while let Some(result) = inner.try_next().await? {
                            scope.check()?;
                            let result = if let Some(left) = &mut budget_rows {
                                let rows = (*left).min(result.num_rows());
                                *left -= rows;
                                result.slice(0, rows)
                            } else {
                                result
                            };
                            bytes = bytes.saturating_add(result.get_array_memory_size());
                            saved.push(result)?;
                            if budget_rows == Some(0) {
                                break;
                            }
                        }
                        drop(inner);
                        self.remove_nested(context)?;
                        let (table, budget) =
                            saved.finish(Arc::clone(&inner_schema), &scope.reads)?;
                        if bytes <= cache_limit {
                            if memo.len() >= 128 || cached_bytes.saturating_add(bytes) > cache_limit
                            {
                                memo.clear();
                                cached_bytes = 0;
                            }
                            if budget.try_grow(key_bytes).is_ok() {
                                memo.insert(key.as_ref().to_vec(), (Arc::clone(&table), budget));
                                cached_bytes += bytes;
                            } else {
                                oversized =
                                    Some((key.as_ref().to_vec(), Arc::clone(&table), budget));
                            }
                        } else {
                            oversized = Some((key.as_ref().to_vec(), Arc::clone(&table), budget));
                        }
                        table
                    };
                    let mut inner = context.read_table(table)?.execute_stream().await?;
                    while let Some(result) = inner.try_next().await? {
                        scope.check()?;
                        let rows =
                            remaining.map_or(result.num_rows(), |left| left.min(result.num_rows()));
                        let result = result.slice(0, rows);
                        let indexes = UInt32Array::from_value(0, result.num_rows());
                        let mut columns = batch
                            .columns()
                            .iter()
                            .map(|column| take(&column.slice(index, 1), &indexes, None))
                            .collect::<std::result::Result<Vec<_>, _>>()?;
                        columns.extend_from_slice(result.columns());
                        let end = sequence
                            .checked_add(rows as u64)
                            .context("CALL output sequence overflow")?;
                        columns.push(Arc::new(UInt64Array::from_iter_values(sequence..end)));
                        sequence = end;
                        let output = RecordBatch::try_new(Arc::clone(&schema), columns)?;
                        results.push(output)?;
                        if let Some(left) = &mut remaining {
                            *left -= rows;
                            if *left == 0 {
                                break 'outer;
                            }
                        }
                    }
                    tokio::task::yield_now().await;
                }
            }
        }
        context.deregister_table(seed)?;
        if let Some((batch, _)) = groups {
            context.deregister_table(TableReference::bare(batch.table.as_str()))?;
        }
        let (provider, budget) = if explain_only {
            results.finish_schema(schema)?
        } else {
            let order = self
                .order()
                .iter()
                .map(|name| col(Column::from_name(name)).sort(true, false))
                .collect();
            results.finish_ordered(schema, &scope.reads, order)?
        };
        context.register_table(output, provider)?;
        Ok(budget)
    }

    async fn prepare_inner(
        &self,
        catalog: &Catalog,
        context: &SessionContext,
        scope: &QueryScope,
        explain_only: bool,
        bounded: bool,
    ) -> Result<Vec<MemoryReservation>> {
        let mut budgets = Vec::new();
        let mut lookups = self.lookups.iter().peekable();
        for (index, child) in self.nested.iter().enumerate() {
            // Later MATCH seeds can depend on the output of a nested CALL.
            while lookups.peek().is_some_and(|lookup| {
                !self.nested[index..]
                    .iter()
                    .any(|call| lookup.reads(&call.table))
            }) {
                budgets.extend(
                    lookups
                        .next()
                        .expect("pending lookup")
                        .register(context, scope, explain_only, bounded)
                        .await?,
                );
            }
            budgets.push(Box::pin(child.register(catalog, context, scope, explain_only)).await?);
        }
        for lookup in lookups {
            budgets.extend(
                lookup
                    .register(context, scope, explain_only, bounded)
                    .await?,
            );
        }
        Ok(budgets)
    }

    fn remove_nested(&self, context: &SessionContext) -> Result<()> {
        for lookup in &self.lookups {
            lookup.remove(context)?;
        }
        for child in &self.nested {
            context.deregister_table(TableReference::bare(child.table.as_str()))?;
        }
        Ok(())
    }
}

fn replace(
    context: &SessionContext,
    name: TableReference,
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
) -> Result<()> {
    context.deregister_table(name.clone())?;
    context.register_table(name, Arc::new(MemTable::try_new(schema, vec![batches])?))?;
    Ok(())
}
