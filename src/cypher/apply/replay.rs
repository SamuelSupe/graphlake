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

struct Seed(RecordBatch, TableReference);

pub(crate) fn seed_batch(context: &TaskContext, name: &str) -> Option<RecordBatch> {
    let seed = context.session_config().get_extension::<Seed>()?;
    (seed.1 == TableReference::bare(name)).then(|| seed.0.clone())
}

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

pub(super) fn with_seed(
    context: TaskContext,
    name: TableReference,
    seed: RecordBatch,
) -> Arc<TaskContext> {
    let config = context
        .session_config()
        .clone()
        .with_extension(Arc::new(Seed(seed, name)));
    Arc::new(context.with_session_config(config))
}

pub(super) struct Plan {
    physical: Arc<dyn ExecutionPlan>,
    state: datafusion::execution::SessionState,
    seed: TableReference,
}

impl Plan {
    pub(super) async fn new(
        context: &SessionContext,
        frame: &DataFrame,
        seed: TableReference,
    ) -> Result<Self> {
        let mut state = context.state();
        // Dynamic filters share state with their original producer and cannot
        // be rebound by DataFusion's physical plan reset.
        state.config_mut().options_mut().set(
            "datafusion.optimizer.enable_dynamic_filter_pushdown",
            "false",
        )?;
        let frame = DataFrame::new(state.clone(), frame.logical_plan().clone());
        let physical = frame.create_physical_plan().await?;
        Ok(Self {
            physical,
            state,
            seed,
        })
    }

    pub(super) fn execute(&self, seed: RecordBatch) -> Result<SendableRecordBatchStream> {
        self.execute_with_plan(seed).map(|(stream, _)| stream)
    }

    pub(super) fn execute_with_plan(
        &self,
        seed: RecordBatch,
    ) -> Result<(SendableRecordBatchStream, Arc<dyn ExecutionPlan>)> {
        // Each task owns its seed, even when cancellation leaves upstream work
        // unwinding while the next key starts.
        let task = with_seed(TaskContext::from(&self.state), self.seed.clone(), seed);
        let plan = reset_plan_states(self.physical.clone())?;
        let stream = execute_stream(plan.clone(), task)?;
        Ok((stream, plan))
    }
}
