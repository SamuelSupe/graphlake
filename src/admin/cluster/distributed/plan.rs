use std::{collections::BTreeMap, fmt, sync::Arc};

use anyhow::{Context, Result, ensure};
use base64::{
    Engine as _,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use datafusion::{
    arrow::ipc::{reader::StreamReader, writer::StreamWriter},
    common::{DataFusionError, tree_node::TreeNodeRecursion},
    datasource::physical_plan::{FileGroup, FileScanConfig, FileScanConfigBuilder, ParquetSource},
    execution::TaskContext,
    physical_expr::{EquivalenceProperties, PhysicalExpr},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
        PlanProperties, SendableRecordBatchStream,
        aggregates::{AggregateExec, AggregateMode},
        coalesce_partitions::CoalescePartitionsExec,
        empty::EmptyExec,
        execution_plan::{
            Boundedness, ChildrenPropertiesMode, EmissionType, ReplaceChildrenOptions,
        },
        joins::{HashJoinExec, PartitionMode, SortMergeJoinExec},
        repartition::RepartitionExec,
        sorts::sort_preserving_merge::SortPreservingMergeExec,
    },
};
use datafusion_datasource::source::DataSourceExec;
use datafusion_proto::{
    bytes::{
        physical_plan_from_bytes_with_extension_codec, physical_plan_to_bytes_with_extension_codec,
    },
    physical_plan::{PhysicalExtensionCodec, PhysicalProtoConverterExtension},
};
use serde::{Deserialize, Serialize};

use super::{Distributed, MAX_STAGES, Output, Task, shuffle};
use crate::{
    engine::QueryScope,
    source::parquet::{ParquetTable, ScanObject},
};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(super) struct Input {
    schema: String,
    files: Vec<Vec<(uuid::Uuid, usize, u64)>>,
}

#[derive(Debug)]
struct Read {
    stage: usize,
    input: Option<Input>,
    cluster: Option<DistributedRead>,
    properties: Arc<PlanProperties>,
}

#[derive(Clone)]
struct DistributedRead {
    cluster: Distributed,
    pool: Arc<dyn datafusion::execution::memory_pool::MemoryPool>,
}

impl fmt::Debug for DistributedRead {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.write_str("shuffle source")
    }
}

impl Read {
    fn placeholder(stage: usize, input: &dyn ExecutionPlan, partitions: Partitioning) -> Arc<Self> {
        Arc::new(Self {
            stage,
            input: None,
            cluster: None,
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(input.schema()),
                partitions,
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
        })
    }

    fn descriptor(&self, outputs: &[Vec<Output>]) -> Result<Input> {
        let count = self.properties.output_partitioning().partition_count();
        let mut files = vec![Vec::new(); count];
        for output in &outputs[self.stage] {
            ensure!(
                output.buckets.len() == count,
                "invalid shuffle partition count"
            );
            for (bucket, bytes) in output.buckets.iter().enumerate() {
                if *bytes > 0 {
                    files[bucket].push((output.task, bucket, *bytes));
                }
            }
        }
        let schema = StreamWriter::try_new(Vec::new(), &self.schema())?.into_inner()?;
        Ok(Input {
            schema: STANDARD.encode(schema),
            files,
        })
    }

    fn from_input(stage: usize, input: Input, cluster: &DistributedRead) -> Result<Self> {
        let schema =
            StreamReader::try_new(std::io::Cursor::new(STANDARD.decode(&input.schema)?), None)?
                .schema();
        Ok(Self {
            stage,
            cluster: Some(cluster.clone()),
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(input.files.len()),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
            input: Some(input),
        })
    }
}

impl DisplayAs for Read {
    fn fmt_as(&self, _: DisplayFormatType, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "GraphLakeShuffleRead: stage={}, partitions={}",
            self.stage,
            self.properties.output_partitioning().partition_count()
        )
    }
}

impl ExecutionPlan for Read {
    fn name(&self) -> &str {
        "GraphLakeShuffleRead"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> datafusion::common::Result<TreeNodeRecursion>,
    ) -> datafusion::common::Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn replace_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
        _: ReplaceChildrenOptions,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }
    fn execute(
        &self,
        partition: usize,
        _: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let input = self
            .input
            .as_ref()
            .context("unbound shuffle input")
            .map_err(external)?;
        let cluster = self.cluster.as_ref().expect("bound shuffle input");
        let files = input
            .files
            .get(partition)
            .context("invalid shuffle read partition")
            .map_err(external)?;
        Ok(shuffle::read(
            cluster.cluster.clone(),
            Arc::clone(&cluster.pool),
            self.schema(),
            files.clone(),
        ))
    }
}

pub(super) fn external(error: impl Into<anyhow::Error>) -> DataFusionError {
    DataFusionError::External(error.into().into())
}

#[derive(Debug)]
struct Codec {
    inputs: Vec<Input>,
    cluster: Option<DistributedRead>,
}

impl PhysicalExtensionCodec for Codec {
    fn try_encode_udf(
        &self,
        udf: &datafusion::logical_expr::ScalarUDF,
        bytes: &mut Vec<u8>,
    ) -> datafusion::common::Result<()> {
        use datafusion::logical_expr::TypeSignature;
        if udf.name().starts_with(crate::engine::KEY_VALIDATOR) {
            let TypeSignature::Exact(types) = &udf.signature().type_signature else {
                return Err(external(anyhow::anyhow!("invalid key validator types")));
            };
            let schema = datafusion::arrow::datatypes::Schema::new(
                types
                    .iter()
                    .enumerate()
                    .map(|(index, ty)| {
                        datafusion::arrow::datatypes::Field::new(
                            index.to_string(),
                            ty.clone(),
                            true,
                        )
                    })
                    .collect::<Vec<_>>(),
            );
            *bytes = StreamWriter::try_new(Vec::new(), &schema)?.into_inner()?;
        }
        Ok(())
    }

    fn try_decode_udf(
        &self,
        name: &str,
        bytes: &[u8],
    ) -> datafusion::common::Result<Arc<datafusion::logical_expr::ScalarUDF>> {
        let error = name
            .strip_prefix(crate::engine::KEY_VALIDATOR)
            .ok_or_else(|| {
                DataFusionError::NotImplemented(format!(
                    "unregistered stage scalar function {name}"
                ))
            })?;
        let error = String::from_utf8(URL_SAFE_NO_PAD.decode(error).map_err(external)?)
            .map_err(external)?;
        let schema = StreamReader::try_new(std::io::Cursor::new(bytes), None)?.schema();
        let types = schema
            .fields()
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        Ok(Arc::new(crate::engine::key_validator(types, error)))
    }
    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        bytes: &mut Vec<u8>,
        _: &dyn PhysicalProtoConverterExtension,
    ) -> datafusion::common::Result<()> {
        let read = node.downcast_ref::<Read>().ok_or_else(|| {
            DataFusionError::NotImplemented("unserializable cluster operator".into())
        })?;
        bytes.extend_from_slice(&(read.stage as u32).to_le_bytes());
        Ok(())
    }

    fn try_decode(
        &self,
        buf: &[u8],
        _: &[Arc<dyn ExecutionPlan>],
        _: &TaskContext,
        _: &dyn PhysicalProtoConverterExtension,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let stage = u32::from_le_bytes(
            buf.try_into()
                .map_err(|_| external(anyhow::anyhow!("invalid shuffle node")))?,
        ) as usize;
        let input = self
            .inputs
            .get(stage)
            .context("missing shuffle descriptor")
            .map_err(external)?;
        let cluster = self
            .cluster
            .as_ref()
            .context("missing shuffle store")
            .map_err(external)?;
        Ok(Arc::new(
            Read::from_input(stage, input.clone(), cluster).map_err(external)?,
        ))
    }
}

pub(super) struct Stage {
    pub id: usize,
    pub input: Arc<dyn ExecutionPlan>,
    pub exchange: Option<RepartitionShape>,
    pub parquet: Option<Vec<ScanObject>>,
    pub join: bool,
    pub aggregate: bool,
    bytes: Option<String>,
    exchange_bytes: Option<String>,
}

pub(super) struct RepartitionShape(Partitioning);
impl RepartitionShape {
    pub fn partitioning(&self) -> &Partitioning {
        &self.0
    }
}

impl Stage {
    pub(super) fn remote(&self) -> bool {
        self.bytes
            .as_ref()
            .is_some_and(|bytes| bytes.len() <= super::MAX_PLAN as usize / 2)
    }
    pub(super) fn task(
        &self,
        partition: usize,
        outputs: &[Vec<Output>],
        graph: bool,
        scope: &QueryScope,
        spill_bytes: u64,
    ) -> Result<Option<Task>> {
        let Some(bytes) = &self.bytes else {
            return Ok(None);
        };
        if bytes.len() > super::MAX_PLAN as usize / 2 {
            return Ok(None);
        }
        let mut inputs = BTreeMap::new();
        descriptors(self.input.as_ref(), outputs, &mut inputs)?;
        // Empty entries are never referenced by the plan.
        let mut descriptors = vec![
            Input {
                schema: String::new(),
                files: Vec::new()
            };
            outputs.len()
        ];
        for (stage, input) in inputs {
            descriptors[stage] = input;
        }
        Ok(Some(Task {
            plan: bytes.clone(),
            exchange: self.exchange_bytes.clone(),
            inputs: descriptors,
            parquet: self.parquet.clone(),
            partition,
            graph,
            memory_bytes: scope.options.memory_bytes,
            spill_bytes,
        }))
    }
}

fn descriptors(
    plan: &dyn ExecutionPlan,
    outputs: &[Vec<Output>],
    inputs: &mut BTreeMap<usize, Input>,
) -> Result<()> {
    if let Some(read) = plan.downcast_ref::<Read>() {
        if !inputs.contains_key(&read.stage) {
            inputs.insert(read.stage, read.descriptor(outputs)?);
        }
    }
    for child in plan.children() {
        descriptors(child.as_ref(), outputs, inputs)?;
    }
    Ok(())
}

pub(super) fn build(
    root: Arc<dyn ExecutionPlan>,
    partitions: usize,
    has_staged_input: bool,
) -> Result<Option<(Arc<dyn ExecutionPlan>, Vec<Stage>)>> {
    // Some operators (for example null-aware anti joins) require CollectLeft
    // regardless of optimizer thresholds.
    if contains(root.as_ref(), |plan| {
        plan.downcast_ref::<HashJoinExec>()
            .is_some_and(|join| join.partition_mode() == &PartitionMode::CollectLeft)
    }) {
        return Ok(None);
    }
    let mut stages = Vec::new();
    let Ok(rewritten) = split(root, &mut stages, partitions) else {
        return Ok(None);
    };
    let parallel_scan = stages.iter().any(|stage| {
        stage.parquet.is_some() && stage.input.output_partitioning().partition_count() > 1
    });
    if !(has_staged_input || parallel_scan) || stages.len() > MAX_STAGES {
        return Ok(None);
    }
    Ok(Some((rewritten, stages)))
}

fn split(
    root: Arc<dyn ExecutionPlan>,
    stages: &mut Vec<Stage>,
    partitions: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    let codec = Codec {
        inputs: Vec::new(),
        cluster: None,
    };
    if let Some((scan, objects)) = partition_scan(&root, partitions)? {
        if let Ok(bytes) = physical_plan_to_bytes_with_extension_codec(Arc::clone(&scan), &codec) {
            return Ok(push(
                stages,
                scan,
                None,
                Some(objects),
                Some(STANDARD.encode(bytes)),
                None,
            ));
        }
    }
    let children = root.children();
    if children.is_empty() {
        return Ok(root);
    }
    let required = root.input_distribution_requirements();
    let children = children
        .into_iter()
        .zip(required.per_child_distributions())
        .map(|(child, distribution)| {
            let child = split(Arc::clone(child), stages, partitions)?;
            Ok(
                if matches!(
                    distribution,
                    datafusion::physical_plan::Distribution::SinglePartition
                ) && child.output_partitioning().partition_count() > 1
                {
                    Arc::new(CoalescePartitionsExec::new(child))
                } else {
                    child
                },
            )
        })
        .collect::<Result<Vec<_>>>()?;
    let rewritten = Arc::clone(&root).replace_children(
        children,
        ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
    )?;
    if let Some(exchange) = rewritten.downcast_ref::<RepartitionExec>() {
        let input = exchange.input();
        if matches!(exchange.partitioning(), Partitioning::RoundRobinBatch(count)
            if *count == input.output_partitioning().partition_count())
            && contains(input.as_ref(), |plan| plan.is::<Read>())
            && !exchange.preserve_order()
        {
            return Ok(Arc::clone(input));
        }
        if !exchange.preserve_order()
            && matches!(
                exchange.partitioning(),
                Partitioning::Hash(..) | Partitioning::RoundRobinBatch(..)
            )
        {
            let input = Arc::clone(exchange.input());
            let empty = Arc::new(
                EmptyExec::new(input.schema())
                    .with_partitions(input.output_partitioning().partition_count()),
            );
            let template = RepartitionExec::try_new(empty, exchange.partitioning().clone())?;
            if let Ok(spec) =
                physical_plan_to_bytes_with_extension_codec(Arc::new(template), &codec)
            {
                let (input, parquet, bytes) = if let Some(read) = input.downcast_ref::<Read>()
                    && read.stage + 1 == stages.len()
                    && stages[read.stage].parquet.is_some()
                {
                    let scan = stages.pop().context("missing scan stage")?;
                    (scan.input, scan.parquet, scan.bytes)
                } else {
                    let bytes = encode_stage(&input, &codec);
                    (input, None, bytes)
                };
                return Ok(push(
                    stages,
                    input,
                    Some(RepartitionShape(exchange.partitioning().clone())),
                    parquet,
                    bytes,
                    Some(STANDARD.encode(spec)),
                ));
            }
        }
    }
    if rewritten.is::<CoalescePartitionsExec>() || rewritten.is::<SortPreservingMergeExec>() {
        let input = rewritten.children()[0];
        if input.output_partitioning().partition_count() > 1
            && !input.is::<Read>()
            && let Some(bytes) = encode_stage(input, &codec)
        {
            let placeholder = push(stages, Arc::clone(input), None, None, Some(bytes), None);
            return Ok(rewritten.replace_children(
                vec![placeholder],
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )?);
        }
    }
    Ok(rewritten)
}

fn encode_stage(plan: &Arc<dyn ExecutionPlan>, codec: &Codec) -> Option<String> {
    // DataFusion's default scan codecs do not carry the engine's reader factories.
    if contains(plan.as_ref(), |plan| plan.is::<DataSourceExec>()) {
        return None;
    }
    physical_plan_to_bytes_with_extension_codec(Arc::clone(plan), codec)
        .ok()
        .map(|bytes| STANDARD.encode(bytes))
}

fn push(
    stages: &mut Vec<Stage>,
    input: Arc<dyn ExecutionPlan>,
    exchange: Option<RepartitionShape>,
    parquet: Option<Vec<ScanObject>>,
    bytes: Option<String>,
    exchange_bytes: Option<String>,
) -> Arc<dyn ExecutionPlan> {
    let id = stages.len();
    let partitioning = exchange.as_ref().map_or_else(
        || input.output_partitioning().clone(),
        |exchange| exchange.0.clone(),
    );
    let placeholder = Read::placeholder(id, input.as_ref(), partitioning);
    let join = contains(input.as_ref(), |plan| {
        plan.is::<HashJoinExec>() || plan.is::<SortMergeJoinExec>()
    });
    let aggregate = contains(input.as_ref(), |plan| {
        plan.downcast_ref::<AggregateExec>()
            .is_some_and(|aggregate| aggregate.mode() == &AggregateMode::Partial)
    });
    stages.push(Stage {
        id,
        input,
        exchange,
        parquet,
        join,
        aggregate,
        bytes,
        exchange_bytes,
    });
    placeholder
}

fn contains(
    plan: &dyn ExecutionPlan,
    predicate: impl Fn(&dyn ExecutionPlan) -> bool + Copy,
) -> bool {
    predicate(plan)
        || plan
            .children()
            .iter()
            .any(|child| contains(child.as_ref(), predicate))
}

fn partition_scan(
    plan: &Arc<dyn ExecutionPlan>,
    partitions: usize,
) -> Result<Option<(Arc<dyn ExecutionPlan>, Vec<ScanObject>)>> {
    let Some((config, _)) = plan
        .downcast_ref::<DataSourceExec>()
        .and_then(|exec| exec.downcast_to_file_source::<ParquetSource>())
    else {
        return Ok(None);
    };
    if config.limit.is_some() || !config.output_ordering.is_empty() {
        return Ok(None);
    }
    let files: Vec<_> = config
        .file_groups
        .iter()
        .flat_map(|group| group.files())
        .collect();
    if files.is_empty()
        || files
            .iter()
            .any(|file| file.extension::<ScanObject>().is_none() || file.range.is_some())
    {
        return Ok(None);
    }
    let mut objects = Vec::with_capacity(files.len());
    let mut parts = Vec::new();
    for file in files {
        let object = file.extension::<ScanObject>().expect("checked scan object");
        let mut object = object.clone();
        let groups = std::mem::take(&mut object.row_groups);
        if groups.is_empty() || groups.windows(2).any(|range| range[0] >= range[1]) {
            parts.push((file.object_meta.size, file.clone()));
        } else {
            for (index, start) in groups.iter().enumerate() {
                let end = groups
                    .get(index + 1)
                    .copied()
                    .unwrap_or(i64::try_from(object.size)?);
                parts.push(((end - start) as u64, file.clone().with_range(*start, end)));
            }
        }
        objects.push(object);
    }
    let target = parts.len().min(partitions);
    let mut groups = (0..target).map(|_| (0_u64, Vec::new())).collect::<Vec<_>>();
    parts.sort_by_key(|(bytes, _)| std::cmp::Reverse(*bytes));
    for (bytes, file) in parts {
        let (used, group) = groups
            .iter_mut()
            .min_by_key(|(used, _)| *used)
            .expect("scan groups");
        *used += bytes;
        group.push(file);
    }
    let mut config = FileScanConfigBuilder::from(config.clone())
        .with_preserve_order(true)
        .with_file_groups(
            groups
                .into_iter()
                .map(|(_, files)| FileGroup::new(files))
                .collect(),
        )
        .build();
    config.output_ordering.clear();
    Ok(Some((DataSourceExec::from_data_source(config), objects)))
}

pub(super) fn bind(
    root: Arc<dyn ExecutionPlan>,
    outputs: &[Vec<Output>],
    cluster: &Distributed,
    scope: &QueryScope,
) -> Result<Arc<dyn ExecutionPlan>> {
    if let Some(read) = root.downcast_ref::<Read>() {
        return Ok(Arc::new(Read {
            stage: read.stage,
            input: Some(read.descriptor(outputs)?),
            cluster: Some(DistributedRead {
                cluster: cluster.clone(),
                pool: Arc::clone(&scope.runtime.memory_pool),
            }),
            properties: Arc::clone(&read.properties),
        }));
    }
    if root.children().is_empty() {
        return Ok(root);
    }
    let children = root
        .children()
        .into_iter()
        .map(|child| bind(Arc::clone(child), outputs, cluster, scope))
        .collect::<Result<Vec<_>>>()?;
    Ok(root.replace_children(
        children,
        ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
    )?)
}

pub(super) async fn decode(
    task: Task,
    ctx: &TaskContext,
    scope: &QueryScope,
    cluster: &Distributed,
) -> Result<(Arc<dyn ExecutionPlan>, Option<RepartitionShape>)> {
    let codec = Codec {
        inputs: task.inputs,
        cluster: Some(DistributedRead {
            cluster: cluster.clone(),
            pool: Arc::clone(&scope.runtime.memory_pool),
        }),
    };
    let mut input =
        physical_plan_from_bytes_with_extension_codec(&STANDARD.decode(task.plan)?, ctx, &codec)?;
    if let Some(objects) = task.parquet {
        let scan = input
            .downcast_ref::<DataSourceExec>()
            .and_then(|exec| exec.downcast_to_file_source::<ParquetSource>())
            .context("remote Parquet plan has no scan")?;
        let config: &FileScanConfig = scan.0;
        let files = config
            .file_groups
            .get(task.partition)
            .context("missing Parquet partition")?;
        let chosen = files
            .files()
            .iter()
            .map(|file| file.object_meta.location.as_ref())
            .collect::<std::collections::BTreeSet<_>>();
        let objects = objects
            .into_iter()
            .filter(|object| chosen.contains(object.path.as_str()))
            .collect::<Vec<_>>();
        ensure!(
            objects.len() == chosen.len(),
            "missing pinned Parquet objects"
        );
        let source = scan
            .1
            .clone()
            .with_parquet_file_reader_factory(ParquetTable::pinned_scan(&objects, scope).await?);
        input = DataSourceExec::from_data_source(
            FileScanConfigBuilder::from(config.clone())
                .with_preserve_order(true)
                .with_source(Arc::new(source))
                .build(),
        );
    }
    let exchange = task
        .exchange
        .map(|bytes| {
            let plan = physical_plan_from_bytes_with_extension_codec(
                &STANDARD.decode(bytes)?,
                ctx,
                &codec,
            )?;
            let exchange = plan
                .downcast_ref::<RepartitionExec>()
                .context("invalid shuffle exchange")?;
            Ok::<_, anyhow::Error>(RepartitionShape(exchange.partitioning().clone()))
        })
        .transpose()?;
    Ok((input, exchange))
}
