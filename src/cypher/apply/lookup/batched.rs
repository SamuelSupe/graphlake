use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::{Session, TableProvider},
    common::{Column, DataFusionError, Result as DataFusionResult, ScalarValue},
    dataframe::DataFrame,
    datasource::source_as_provider,
    execution::{
        SendableRecordBatchStream, TaskContext, context::SessionState, memory_pool::MemoryConsumer,
    },
    logical_expr::{Expr, LogicalPlan, TableProviderFilterPushDown, TableType},
    physical_plan::{
        ExecutionPlan,
        stream::RecordBatchStreamAdapter,
        streaming::{PartitionStream, StreamingTableExec},
    },
    prelude::{SessionContext, col},
};
use futures::TryStreamExt;

use super::{Keys, MAX_NEIGHBORS, parquet, restrict};
use crate::{engine::QueryScope, source::parquet::ParquetTable};

pub(super) fn view(
    context: &SessionContext,
    scope: &QueryScope,
    base: DataFrame,
    keys: DataFrame,
    columns: &[String],
) -> Result<DataFrame> {
    let schema = Arc::clone(base.schema().inner());
    let cap = if columns.len() == 1 && parquet_only(base.logical_plan()) {
        // File scans benefit from covering several row groups at once. The
        // byte budget still limits keys, independently of the SQL batch cap.
        MAX_NEIGHBORS * 16
    } else {
        MAX_NEIGHBORS
    };
    Ok(context.read_table(Arc::new(Batched {
        schema,
        base: base.into_unoptimized_plan(),
        keys: keys.into_unoptimized_plan(),
        columns: columns.to_vec(),
        cap,
        budget: (scope.options.memory_bytes / 16).clamp(1, 4 * 1024 * 1024),
        memory: scope.options.memory_bytes,
    }))?)
}

#[derive(Debug, Clone)]
struct Batched {
    schema: SchemaRef,
    base: LogicalPlan,
    keys: LogicalPlan,
    columns: Vec<String>,
    budget: usize,
    cap: usize,
    memory: usize,
}

#[async_trait]
impl TableProvider for Batched {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let state = state
            .as_any()
            .downcast_ref::<SessionState>()
            .ok_or_else(|| DataFusionError::Internal("missing frontier session state".into()))?;
        let schema = projection.map_or_else(
            || Ok(Arc::clone(&self.schema)),
            |indices| self.schema.project(indices).map(Arc::new),
        )?;
        let mut base = DataFrame::new(state.clone(), self.base.clone());
        for filter in filters {
            base = base.filter(filter.clone())?;
        }
        let partition = Partition {
            schema: Arc::clone(&schema),
            base,
            keys: DataFrame::new(state.clone(), self.keys.clone()),
            columns: self.columns.clone(),
            budget: self.budget,
            cap: self.cap,
            memory: self.memory,
        };
        Ok(Arc::new(StreamingTableExec::try_new(
            schema,
            vec![Arc::new(partition)],
            None,
            [],
            false,
            limit,
        )?))
    }
}

struct Partition {
    schema: SchemaRef,
    base: DataFrame,
    keys: DataFrame,
    columns: Vec<String>,
    budget: usize,
    cap: usize,
    memory: usize,
}

impl std::fmt::Debug for Partition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("GraphLake batched key lookup")
    }
}

impl PartitionStream for Partition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, task: Arc<TaskContext>) -> SendableRecordBatchStream {
        let Self {
            schema,
            base,
            keys,
            columns,
            budget,
            cap,
            memory,
        } = self;
        let output = Arc::clone(schema);
        let (base, keys, columns, budget, cap, memory_bytes) = (
            base.clone(),
            keys.clone(),
            columns.clone(),
            *budget,
            *cap,
            *memory,
        );
        let stream = async_stream::try_stream! {
            let mut keys = keys.execute_stream().await?;
            let mut chunk = Keys::new();
            let memory = MemoryConsumer::new("GraphLake frontier key batch").register(task.memory_pool());
            while let Some(batch) = keys.try_next().await? {
                for row in 0..batch.num_rows() {
                    let key = batch.columns().iter()
                        .map(|column| ScalarValue::try_from_array(column, row))
                        .collect::<DataFusionResult<Vec<_>>>()?;
                    let size = size_of::<Vec<ScalarValue>>() + key.iter().map(ScalarValue::size).sum::<usize>();
                    if !chunk.is_empty()
                        && (chunk.len() == cap || memory.size().saturating_add(size) > budget)
                    {
                        let mut rows = scan(&base, &output, &columns, &chunk, memory_bytes).await?;
                        while let Some(row) = rows.try_next().await? {
                            yield row;
                        }
                        chunk.clear();
                        memory.free();
                    }
                    memory.try_grow(size)?;
                    chunk.insert(key);
                }
            }
            if !chunk.is_empty() {
                let mut rows = scan(&base, &output, &columns, &chunk, memory_bytes).await?;
                while let Some(row) = rows.try_next().await? {
                    yield row;
                }
            }
        };
        Box::pin(RecordBatchStreamAdapter::new(Arc::clone(schema), stream))
    }
}

async fn scan(
    base: &DataFrame,
    output: &SchemaRef,
    columns: &[String],
    keys: &Keys,
    memory: usize,
) -> DataFusionResult<SendableRecordBatchStream> {
    let predicate = parquet::predicate(base.logical_plan(), columns, keys, memory)
        .unwrap_or_else(|| restrict(columns, keys));
    base.clone()
        .filter(predicate)?
        .select(
            output
                .fields()
                .iter()
                .map(|field| col(Column::from_name(field.name()))),
        )?
        .execute_stream()
        .await
}

fn parquet_only(plan: &LogicalPlan) -> bool {
    if let LogicalPlan::TableScan(scan) = plan {
        return source_as_provider(&scan.source).is_ok_and(|provider| {
            provider.downcast_ref::<ParquetTable>().is_some()
                || provider
                    .get_logical_plan()
                    .is_some_and(|plan| parquet_only(&plan))
        });
    }
    let inputs = plan.inputs();
    !inputs.is_empty() && inputs.into_iter().all(parquet_only)
}
