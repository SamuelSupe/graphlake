use std::sync::Arc;

use anyhow::Result;
use datafusion::{
    arrow::{array::RecordBatch, datatypes::SchemaRef},
    catalog::streaming::StreamingTable,
    common::{DataFusionError, TableReference},
    dataframe::DataFrame,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_plan::{
        ExecutionPlan, execute_stream, execution_plan::reset_plan_states,
        stream::RecordBatchStreamAdapter, streaming::PartitionStream,
    },
    prelude::SessionContext,
};

struct Seed(RecordBatch);

#[derive(Debug)]
struct SeedScan(SchemaRef);

impl PartitionStream for SeedScan {
    fn schema(&self) -> &SchemaRef {
        &self.0
    }

    fn execute(&self, context: Arc<TaskContext>) -> SendableRecordBatchStream {
        let batch = context
            .session_config()
            .get_extension::<Seed>()
            .map(|seed| seed.0.clone())
            .ok_or_else(|| DataFusionError::Execution("missing correlated CALL seed".into()));
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.0),
            futures::stream::iter([batch]),
        ))
    }
}

pub(super) fn register_seed(
    context: &SessionContext,
    name: TableReference,
    schema: SchemaRef,
) -> Result<()> {
    let partition = Arc::new(SeedScan(Arc::clone(&schema)));
    context.register_table(
        name,
        Arc::new(StreamingTable::try_new(schema, vec![partition])?),
    )?;
    Ok(())
}

pub(super) struct Plan {
    physical: Arc<dyn ExecutionPlan>,
    state: datafusion::execution::SessionState,
}

impl Plan {
    pub(super) async fn new(context: &SessionContext, frame: &DataFrame) -> Result<Self> {
        let mut state = context.state();
        // Dynamic filters share state with their original producer and cannot
        // be rebound by DataFusion's physical plan reset.
        state
            .config_mut()
            .options_mut()
            .optimizer
            .enable_dynamic_filter_pushdown = false;
        let frame = DataFrame::new(state.clone(), frame.logical_plan().clone());
        let physical = frame.create_physical_plan().await?;
        Ok(Self { physical, state })
    }

    pub(super) fn execute(&self, seed: RecordBatch) -> Result<SendableRecordBatchStream> {
        // Each task owns its seed, even when cancellation leaves upstream work
        // unwinding while the next key starts.
        let config = self
            .state
            .config()
            .clone()
            .with_extension(Arc::new(Seed(seed)));
        let task = TaskContext::from(&self.state).with_session_config(config);
        Ok(execute_stream(
            reset_plan_states(Arc::clone(&self.physical))?,
            Arc::new(task),
        )?)
    }
}
