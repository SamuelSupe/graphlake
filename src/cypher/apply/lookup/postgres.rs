use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use anyhow::Result;
use datafusion::{
    common::{
        TableReference,
        tree_node::{Transformed, TreeNode},
    },
    dataframe::DataFrame,
    datasource::{ViewTable, provider_as_source, source_as_provider},
    logical_expr::LogicalPlan,
    prelude::SessionContext,
};

use super::{Keys, Lookup, restrict};
use crate::{engine::QueryScope, source::postgres::PgTable};

impl Lookup {
    pub(super) async fn postgres_filters(
        &self,
        context: &SessionContext,
        scope: &QueryScope,
        starts: &Keys,
    ) -> Result<Option<BTreeMap<String, String>>> {
        let mut tables = BTreeMap::new();
        let labels = self
            .steps
            .iter()
            .flat_map(|step| [&step.edge, &step.to.node])
            .collect::<BTreeSet<_>>();
        for label in labels {
            let frame = context.table(TableReference::bare(label.as_str())).await?;
            let Some(table) = base_table(frame.logical_plan()) else {
                return Ok(None);
            };
            if !scope.options.graph.require_consistent_snapshot
                && !table.is_local_snapshot().await?
            {
                return Ok(None);
            }
            tables.insert(label.as_str(), table);
        }
        // The anchor contributes only literal keys, so it may be in another
        // snapshot. Every relation in the remote semi-join must share one.
        let Some(first) = tables.values().next() else {
            return Ok(None);
        };
        if tables.values().any(|table| !first.shares_snapshot(table)) {
            return Ok(None);
        }
        Ok(self.remote_candidates(&tables, starts))
    }

    fn remote_candidates(
        &self,
        tables: &BTreeMap<&str, PgTable>,
        starts: &Keys,
    ) -> Option<BTreeMap<String, String>> {
        let mut filters = BTreeMap::new();

        // The remote frontier is a superset. MATCH still checks graph filters,
        // endpoint visibility, and relationship uniqueness on the modeled views.
        let mut from = String::new();
        let mut previous = Vec::new();
        for (index, step) in self.steps.iter().enumerate() {
            let edge = &tables[step.edge.as_str()];
            let alias = format!("e{index}");
            let filter = if index == 0 {
                let seed = edge.filter_sql(&restrict(&step.from.columns, starts))?;
                let fields = step
                    .to
                    .columns
                    .iter()
                    .map(|name| crate::catalog::ident(name))
                    .collect::<Vec<_>>()
                    .join(", ");
                from = format!(
                    "FROM (SELECT {fields} FROM {} WHERE {seed}) AS {alias}",
                    edge.relation()
                );
                seed
            } else {
                let filter = membership(edge, &step.from.columns, &previous, &from)?;
                let sources = operands(edge, Some(&alias), &step.from.columns)?;
                from.push_str(&format!(
                    " JOIN {} AS {alias} ON ({}) = ({})",
                    edge.relation(),
                    previous.join(", "),
                    sources.join(", ")
                ));
                filter
            };
            filters.insert(step.edge_alias.clone(), filter);
            previous = operands(edge, Some(&alias), &step.to.columns)?;
            let node = &tables[step.to.node.as_str()];
            filters.insert(
                step.to_alias.clone(),
                membership(node, &step.keys, &previous, &from)?,
            );
        }
        Some(filters)
    }
}

fn operands(table: &PgTable, alias: Option<&str>, columns: &[String]) -> Option<Vec<String>> {
    columns
        .iter()
        .map(|column| table.key_sql(alias, column))
        .collect()
}

fn membership(table: &PgTable, keys: &[String], values: &[String], from: &str) -> Option<String> {
    Some(format!(
        "({}) IN (SELECT {} {from})",
        operands(table, None, keys)?.join(", "),
        values.join(", ")
    ))
}

fn base_table(plan: &LogicalPlan) -> Option<PgTable> {
    if let LogicalPlan::TableScan(scan) = plan {
        let provider = source_as_provider(&scan.source).ok()?;
        return provider.downcast_ref::<PgTable>().cloned().or_else(|| {
            provider
                .get_logical_plan()
                .and_then(|plan| base_table(&plan))
        });
    }
    // Direct mappings have a single lineage. A materialized or combined view
    // needs its normal execution path and cannot use a raw PostgreSQL frontier.
    let inputs = plan.inputs();
    let [input] = inputs.as_slice() else {
        return None;
    };
    base_table(input)
}

pub(super) async fn restrict_view(
    context: &SessionContext,
    frame: DataFrame,
    filter: &str,
) -> Result<DataFrame> {
    fn rewrite(plan: LogicalPlan, filter: &str) -> datafusion::common::Result<LogicalPlan> {
        plan.transform_up(|plan| {
            let LogicalPlan::TableScan(mut scan) = plan else {
                return Ok(Transformed::no(plan));
            };
            let provider = source_as_provider(&scan.source)?;
            scan.source = if let Some(table) = provider.downcast_ref::<PgTable>() {
                provider_as_source(Arc::new(table.restricted(filter.to_owned())))
            } else if let Some(plan) = provider.get_logical_plan() {
                let plan = rewrite(plan.into_owned(), filter)?;
                provider_as_source(Arc::new(ViewTable::new(plan, None)))
            } else {
                return Ok(Transformed::no(LogicalPlan::TableScan(scan)));
            };
            Ok(Transformed::yes(LogicalPlan::TableScan(scan)))
        })
        .map(|plan| plan.data)
    }
    let plan = rewrite(frame.into_unoptimized_plan(), filter)?;
    Ok(context.execute_logical_plan(plan).await?)
}
