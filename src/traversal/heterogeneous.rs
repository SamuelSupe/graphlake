use std::sync::Arc;

use anyhow::Result;
use datafusion::{
    arrow::datatypes::{DataType, Field, Schema},
    common::Column,
    dataframe::DataFrame,
    logical_expr::{Expr, lit},
    physical_plan::ExecutionPlan,
    prelude::SessionContext,
};

use crate::{Catalog, Edge, ScalarValue, catalog::ident};

use super::{Direction, Inputs, where_clause};

/// The tag and the two typed key regions let adjacent labels use different key types.
struct Layout {
    from: Vec<DataType>,
    to: Vec<DataType>,
}

impl Layout {
    async fn new(context: &SessionContext, catalog: &Catalog, edge: &Edge) -> Result<Self> {
        let fields = |frame: DataFrame, label: &str| -> Result<Vec<DataType>> {
            catalog.nodes[label]
                .key
                .iter()
                .map(|column| {
                    Ok(frame
                        .schema()
                        .field_with_unqualified_name(column)?
                        .data_type()
                        .clone())
                })
                .collect()
        };
        Ok(Self {
            from: fields(
                context
                    .table(datafusion::common::TableReference::bare(
                        edge.from.node.as_str(),
                    ))
                    .await?,
                &edge.from.node,
            )?,
            to: fields(
                context
                    .table(datafusion::common::TableReference::bare(
                        edge.to.node.as_str(),
                    ))
                    .await?,
                &edge.to.node,
            )?,
        })
    }

    fn width(&self) -> usize {
        1 + self.from.len() + self.to.len()
    }

    fn encode(&self, second: bool, key: &[String], offset: usize) -> Result<Vec<Expr>> {
        let mut expressions = vec![lit(i32::from(second)).alias(format!("c{offset}"))];
        for (tag, types) in [(false, &self.from), (true, &self.to)] {
            for (index, ty) in types.iter().enumerate() {
                let expression = if tag == second {
                    Expr::Column(Column::from_name(&key[index]))
                } else {
                    lit(ScalarValue::try_from(ty)?)
                };
                expressions.push(expression.alias(format!("c{}", offset + expressions.len())));
            }
        }
        Ok(expressions)
    }

    async fn vertices(
        &self,
        context: &SessionContext,
        catalog: &Catalog,
        label: &str,
        second: bool,
        filters: &[String],
    ) -> Result<DataFrame> {
        Ok(context
            .sql(&format!(
                "SELECT * FROM {} AS src{}",
                ident(label),
                where_clause(filters)
            ))
            .await?
            .select(self.encode(second, &catalog.nodes[label].key, 0)?)?)
    }
}

pub(super) async fn starts(
    context: &SessionContext,
    catalog: &Catalog,
    edge: &Edge,
    label: &str,
    filters: &[String],
) -> Result<Arc<dyn ExecutionPlan>> {
    let layout = Layout::new(context, catalog, edge).await?;
    Ok(layout
        .vertices(context, catalog, label, label == edge.to.node, filters)
        .await?
        .create_physical_plan()
        .await?)
}

pub(super) async fn scan(
    context: &SessionContext,
    catalog: &Catalog,
    edge_name: &str,
    direction: Direction,
    filters: &[String],
) -> Result<Inputs> {
    let edge = &catalog.edges[edge_name];
    let layout = Layout::new(context, catalog, edge).await?;
    let forward = layout
        .vertices(context, catalog, &edge.from.node, false, &[])
        .await?;
    let reverse = layout
        .vertices(context, catalog, &edge.to.node, true, &[])
        .await?;
    let nodes = forward.union(reverse)?.create_physical_plan().await?;
    let (second, from, to) = if matches!(direction, Direction::Incoming) {
        (true, &edge.to.columns, &edge.from.columns)
    } else {
        (false, &edge.from.columns, &edge.to.columns)
    };
    let mut projection = layout.encode(second, from, 0)?;
    projection.extend(layout.encode(!second, to, layout.width())?);
    let keys = edge.identity();
    projection.extend(keys.iter().enumerate().map(|(index, column)| {
        Expr::Column(Column::from_name(column)).alias(format!("c{}", 2 * layout.width() + index))
    }));
    let edges = context
        .sql(&format!(
            "SELECT * FROM {} AS src{}",
            ident(edge_name),
            where_clause(filters)
        ))
        .await?
        .select(projection)?
        .create_physical_plan()
        .await?;
    let fields = keys
        .into_iter()
        .zip(edges.schema().fields().iter().skip(2 * layout.width()))
        .map(|(name, field)| Field::new(name, field.data_type().clone(), false))
        .collect::<Vec<_>>();
    Ok(Inputs {
        nodes,
        edges,
        identity: Arc::new(Schema::new(fields)),
        undirected: matches!(direction, Direction::Undirected),
        starts: None,
    })
}
