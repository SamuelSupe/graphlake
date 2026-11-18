use std::{collections::BTreeMap, sync::Arc};

use anyhow::{Result, ensure};
use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::{Field, Schema, SchemaRef},
    catalog::{Session, TableProvider},
    common::TableReference,
    logical_expr::{Expr, TableType},
    physical_expr::expressions::Column,
    physical_plan::{ExecutionPlan, execution_plan::reset_plan_states, projection::ProjectionExec},
    prelude::SessionContext,
};

use crate::{
    Catalog,
    catalog::{equal_keys, ident, qualified, union_all},
    cypher::string,
    engine::{Prepared, QueryScope, graph_frame},
};

use super::{Direction, Input, Mode, TraversalExec, node_plan, remote::Remote, scan_plans};

#[derive(Clone, Debug)]
pub(crate) struct Expansion {
    pub table: String,
    pub edge: String,
    pub mixed: Option<MixedExpansion>,
    pub direction: Direction,
    pub shortest: Option<crate::cypher::paths::Shortest>,
    pub start_node: String,
    pub track_vertices: bool,
    pub min: usize,
    pub max: usize,
    pub filters: Vec<String>,
    pub starts: Vec<String>,
    pub anchored: bool,
    pub start_variable: Option<String>,
    pub start_seed: Option<StartSeed>,
    #[cfg(feature = "admin")]
    pub finishes: BTreeMap<String, Vec<String>>,
}

#[derive(Clone, Debug)]
pub(crate) struct MixedExpansion {
    pub edges: BTreeMap<String, Vec<String>>,
    pub starts: BTreeMap<String, Vec<String>>,
}

#[derive(Clone, Debug)]
pub(crate) struct StartSeed {
    pub table: String,
    pub keys: Vec<String>,
    // Heterogeneous topologies prefix the two typed key regions with a label tag.
    pub tag: Option<(bool, usize)>,
}

impl Expansion {
    pub(crate) fn imported_keys(
        &self,
        context: &datafusion::execution::TaskContext,
        tagged: bool,
    ) -> Result<Option<datafusion::arrow::array::RecordBatch>> {
        use anyhow::Context;
        use datafusion::arrow::{
            array::{Array, BooleanArray, RecordBatch, StructArray},
            compute::filter,
        };
        let (Some(start), Some(variable)) = (&self.start_seed, &self.start_variable) else {
            return Ok(None);
        };
        let Some(seed) = crate::cypher::apply::replay::seed_batch(context, &start.table) else {
            return Ok(None);
        };
        let entity = seed
            .column_by_name(variable)
            .and_then(|column| column.as_any().downcast_ref::<StructArray>())
            .context("missing imported traversal vertex")?;
        let (properties, keys) = if tagged {
            (entity, vec!["label", "key"])
        } else {
            let properties = entity
                .column_by_name("properties")
                .and_then(|column| column.as_any().downcast_ref::<StructArray>())
                .context("missing imported traversal key fields")?;
            (properties, start.keys.iter().map(String::as_str).collect())
        };
        let mut fields = Vec::new();
        let mut arrays = Vec::new();
        for key in keys {
            let index = properties
                .fields()
                .find(key)
                .map(|(i, _)| i)
                .context("missing imported traversal key")?;
            fields.push(properties.fields()[index].clone());
            arrays.push(properties.column(index).clone());
        }
        if let Some(nulls) = entity.nulls().filter(|nulls| nulls.null_count() > 0) {
            let valid = BooleanArray::new(nulls.inner().clone(), None);
            arrays = arrays
                .iter()
                .map(|array| filter(array, &valid))
                .collect::<Result<_, _>>()?;
        }
        Ok(Some(RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            arrays,
        )?))
    }

    pub(crate) async fn register(
        self,
        catalog: &Catalog,
        context: &SessionContext,
        scope: &Arc<QueryScope>,
        prepared: &Prepared,
        #[cfg(feature = "admin")] csr: Option<&crate::admin::cluster::csr::CypherCsr>,
    ) -> Result<()> {
        ensure!(
            !catalog.nodes.contains_key(&self.table) && !catalog.edges.contains_key(&self.table),
            "graph type {} collides with an internal expansion name",
            self.table
        );
        let direction = self.direction;
        let mut inputs = if let Some(mixed) = &self.mixed {
            self.mixed_inputs(context, catalog, mixed).await?
        } else {
            scan_plans(context, catalog, &self.edge, direction, &self.filters).await?
        };
        if self.mixed.is_none()
            && (!self.starts.is_empty()
                || catalog.edges[&self.edge].from.node != catalog.edges[&self.edge].to.node)
        {
            let edge = &catalog.edges[&self.edge];
            inputs.starts = Some(if edge.from.node == edge.to.node {
                node_plan(
                    context,
                    &self.start_node,
                    &catalog.nodes[&self.start_node].key,
                    &self.starts,
                )
                .await?
            } else {
                super::heterogeneous::starts(context, catalog, edge, &self.start_node, &self.starts)
                    .await?
            });
        }
        let name = self.table.clone();
        #[cfg(feature = "admin")]
        if let Some(csr) = csr {
            if csr.indexed()
                || (self.filters.is_empty()
                    && self
                        .mixed
                        .as_ref()
                        .is_none_or(|mixed| mixed.edges.values().all(Vec::is_empty)))
            {
                let starts = if let Some(starts) = &inputs.starts {
                    starts.clone()
                } else {
                    node_plan(
                        context,
                        &self.start_node,
                        &catalog.nodes[&self.start_node].key,
                        &self.starts,
                    )
                    .await?
                };
                let plan = TraversalExec::new(
                    Input::Live(inputs),
                    Mode::Expand(self.clone()),
                    Arc::clone(&scope.visits_by_depth),
                );
                let mut finishes = BTreeMap::new();
                for (label, filters) in self
                    .finishes
                    .iter()
                    .filter(|(_, filters)| !filters.is_empty())
                {
                    finishes.insert(
                        label.clone(),
                        node_plan(context, label, &catalog.nodes[label].key, filters).await?,
                    );
                }
                let mut typed_starts = BTreeMap::new();
                if let Some(mixed) = &self.mixed {
                    for (label, filters) in &mixed.starts {
                        typed_starts.insert(
                            label.clone(),
                            node_plan(context, label, &catalog.nodes[label].key, filters).await?,
                        );
                    }
                }
                context.register_table(
                    TableReference::bare(name),
                    csr.expansion(
                        scope,
                        catalog,
                        self,
                        plan.schema(),
                        starts,
                        typed_starts,
                        finishes,
                    )?,
                )?;
                return Ok(());
            }
        }
        let remote = if self.mixed.is_none()
            && self.anchored
            && self.filters.is_empty()
            && self.shortest.is_none()
            && catalog.edges[&self.edge].from.node == catalog.edges[&self.edge].to.node
        {
            Remote::open(catalog, prepared, scope, &self.edge, direction, false).await?
        } else {
            None
        };
        let mut plan = TraversalExec::new(
            Input::Live(inputs),
            Mode::Expand(self),
            Arc::clone(&scope.visits_by_depth),
        );
        plan.remote = remote;
        context.register_table(
            TableReference::bare(name),
            Arc::new(ExpansionTable(Arc::new(plan))),
        )?;
        Ok(())
    }

    async fn mixed_inputs(
        &self,
        context: &SessionContext,
        catalog: &Catalog,
        mixed: &MixedExpansion,
    ) -> Result<super::Inputs> {
        let vertex = |name: &str, filters: &[String]| {
            let key = text_key("src", &catalog.nodes[name].key);
            format!(
                "SELECT {} AS label, {key} AS key FROM {} AS src{}",
                string(name),
                ident(name),
                super::where_clause(filters)
            )
        };
        let nodes = mixed
            .edges
            .keys()
            .flat_map(|name| {
                let edge = &catalog.edges[name];
                [&edge.from.node, &edge.to.node]
            })
            .chain(mixed.starts.keys())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .map(|name| vertex(name, &[]))
            .collect::<Vec<_>>();
        let nodes = graph_frame(context, &union_all(&nodes))
            .await?
            .create_physical_plan()
            .await?;
        let starts_sql = mixed
            .starts
            .iter()
            .map(|(label, filters)| vertex(label, filters))
            .collect::<Vec<_>>();
        let starts = graph_frame(context, &union_all(&starts_sql))
            .await?
            .create_physical_plan()
            .await?;
        let mut reads = Vec::new();
        for (name, filters) in &mixed.edges {
            let edge = &catalog.edges[name];
            let (from, to) = if self.direction == Direction::Incoming {
                (&edge.to, &edge.from)
            } else {
                (&edge.from, &edge.to)
            };
            let from_key = &catalog.nodes[&from.node].key;
            let to_key = &catalog.nodes[&to.node].key;
            reads.push(format!(
                "SELECT {} AS from_label, {} AS from_key, {} AS to_label, {} AS to_key, {} AS label, {} AS key FROM {} AS src JOIN {} AS a ON {} JOIN {} AS b ON {}{}",
                string(&from.node), text_key("a", from_key),
                string(&to.node), text_key("b", to_key),
                string(name), text_key("src", &edge.identity()), ident(name),
                ident(&from.node), equal_keys("a", from_key, "src", &from.columns),
                ident(&to.node), equal_keys("b", to_key, "src", &to.columns),
                super::where_clause(filters)
            ));
        }
        let sql = if reads.is_empty() {
            "SELECT label AS from_label, key AS from_key, label AS to_label, key AS to_key, label, key FROM (SELECT '' AS label, make_array('') AS key) AS empty WHERE FALSE".into()
        } else {
            union_all(&reads)
        };
        let edges = graph_frame(context, &sql)
            .await?
            .create_physical_plan()
            .await?;
        let identity = Schema::new(vec![
            Field::new("label", edges.schema().field(4).data_type().clone(), false),
            Field::new("key", edges.schema().field(5).data_type().clone(), false),
        ]);
        Ok(super::Inputs {
            nodes,
            edges,
            identity: Arc::new(identity),
            undirected: self.direction == Direction::Undirected,
            starts: Some(starts),
        })
    }
}

pub(crate) fn text_key(alias: &str, fields: &[String]) -> String {
    let fields = fields
        .iter()
        .map(|field| format!("__graphlake_key_text({})", qualified(alias, field)))
        .collect::<Vec<_>>();
    format!("make_array({})", fields.join(", "))
}

#[derive(Debug)]
struct ExpansionTable(Arc<TraversalExec>);

#[async_trait]
impl TableProvider for ExpansionTable {
    fn schema(&self) -> SchemaRef {
        self.0.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _: &dyn Session,
        projection: Option<&Vec<usize>>,
        _: &[Expr],
        _: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        // Separate scans of an expansion cannot share the state of its
        // physical input operators, including repartition channels.
        let plan = reset_plan_states(self.0.clone())?;
        if let Some(indices) = projection {
            let schema = plan.schema();
            let exprs = indices.iter().map(|&index| {
                let name = schema.field(index).name();
                let column: Arc<dyn datafusion::physical_expr::PhysicalExpr> =
                    Arc::new(Column::new(name, index));
                (column, name.clone())
            });
            Ok(Arc::new(ProjectionExec::try_new(exprs, plan)?))
        } else {
            Ok(plan)
        }
    }
}
