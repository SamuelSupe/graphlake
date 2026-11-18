use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};
use std::sync::Arc;

use anyhow::Result;
use datafusion::{
    arrow::datatypes::DataType,
    common::{Column, ScalarValue, TableReference},
    dataframe::DataFrame,
    execution::memory_pool::MemoryReservation,
    logical_expr::Expr,
    prelude::{SessionContext, col, lit},
};
use futures::TryStreamExt;
use opencypher::{Direction, ExprKind, LabelExpr, Match, Properties, Visitor};

use crate::{
    Catalog, Source,
    catalog::{ident, qualified},
    cypher::{
        clauses::{Scope, Value},
        entities,
    },
    engine::{QueryScope, graph_frame},
    source::snapshot::Buffer,
};

mod batched;
mod parquet;
mod postgres;

const MAX_NEIGHBORS: usize = 4096;

#[derive(Clone, Copy)]
pub(super) enum LookupMode {
    Schema,
    Batch { bounded: bool },
    Set,
}

/// Keep complete entity records when an expression can observe their full
/// contents. Named property reads in a scalar query only need those fields.
pub(in crate::cypher) fn scalar_projection(
    catalog: &Catalog,
    query: &opencypher::Query,
) -> Option<Catalog> {
    #[derive(Default)]
    struct Fields {
        names: BTreeSet<String>,
        full: bool,
    }
    impl Fields {
        fn properties(&mut self, props: Option<&Properties>) {
            if let Some(Properties::Map(entries)) = props {
                self.names
                    .extend(entries.iter().map(|(name, _)| name.0.clone()));
            }
        }
    }
    impl Visitor for Fields {
        fn visit_node_pattern(&mut self, node: &opencypher::NodePattern) {
            self.properties(node.properties.as_ref());
            opencypher::walk_node_pattern(self, node);
        }

        fn visit_relationship_pattern(&mut self, rel: &opencypher::RelationshipPattern) {
            self.properties(rel.properties.as_ref());
            opencypher::walk_relationship_pattern(self, rel);
        }

        fn visit_expr(&mut self, expr: &opencypher::Expr) {
            match &expr.kind {
                ExprKind::Property { key, .. } => {
                    self.names.insert(key.0.clone());
                }
                ExprKind::FunctionCall { name, args, .. }
                    if matches!(
                        name.0.to_ascii_lowercase().as_str(),
                        "id" | "elementid"
                            | "labels"
                            | "type"
                            | "count"
                            | "sum"
                            | "avg"
                            | "range"
                            | crate::cypher::collect::COUNT_SIZE
                    ) || matches!(name.0.to_ascii_lowercase().as_str(), "min" | "max")
                        && matches!(args.as_slice(), [arg] if matches!(arg.kind, ExprKind::Property { .. })) =>
                    {}
                ExprKind::List(items)
                    if items.iter().all(|item| {
                        matches!(
                            item.kind,
                            ExprKind::Null
                                | ExprKind::Boolean(_)
                                | ExprKind::Integer(_)
                                | ExprKind::Float(_)
                                | ExprKind::String(_)
                        )
                    }) => {}
                ExprKind::Variable(_)
                | ExprKind::Null
                | ExprKind::Boolean(_)
                | ExprKind::Integer(_)
                | ExprKind::Float(_)
                | ExprKind::String(_)
                | ExprKind::Unary { .. }
                | ExprKind::Binary { .. }
                | ExprKind::Comparison { .. }
                | ExprKind::IsNull(_)
                | ExprKind::IsNotNull(_)
                | ExprKind::CountStar => {}
                _ => self.full = true,
            }
            if !self.full {
                opencypher::walk_expr(self, expr);
            }
        }
    }
    let mut fields = Fields::default();
    fields.visit_query(query);
    let omit = |name: &String| !fields.names.contains(name);
    if fields.full
        || (!catalog
            .nodes
            .values()
            .any(|node| node.properties.iter().any(omit))
            && !catalog
                .edges
                .values()
                .any(|edge| edge.properties.iter().any(omit)))
    {
        return None;
    }
    let mut result = catalog.clone();
    for node in result.nodes.values_mut() {
        node.properties.retain(|name| fields.names.contains(name));
    }
    for edge in result.edges.values_mut() {
        edge.properties.retain(|name| fields.names.contains(name));
    }
    Some(result)
}

/// Restrict a MATCH to candidates reachable from the imported keys. The
/// private aliases still read the modeled views, including graph-wide filters.
pub(crate) struct Lookup {
    seed_sql: String,
    node: String,
    node_alias: String,
    anchor_variable: Option<String>,
    columns: Vec<String>,
    steps: Vec<Step>,
    aliases: BTreeMap<String, VecDeque<String>>,
    remote_frontier: bool,
}

struct Step {
    edge: String,
    edge_alias: String,
    from: crate::Endpoint,
    to: crate::Endpoint,
    to_alias: String,
    keys: Vec<String>,
}

impl Lookup {
    pub(in crate::cypher) fn aliases(&self) -> &BTreeMap<String, VecDeque<String>> {
        &self.aliases
    }

    pub(in crate::cypher) fn relation_order(&self) -> BTreeMap<String, usize> {
        // Build the join from the bound end, with the narrow edge before
        // the target entity. This keeps the wide target on the probe side.
        std::iter::once(&self.node_alias)
            .chain(
                self.steps
                    .iter()
                    .flat_map(|step| [&step.edge_alias, &step.to_alias]),
            )
            .enumerate()
            .map(|(index, alias)| (alias.clone(), index))
            .collect()
    }

    pub(in crate::cypher) fn anchor_variable(&self) -> Option<&str> {
        self.anchor_variable.as_deref()
    }

    pub(in crate::cypher) fn plan(
        catalog: &Catalog,
        pattern: &Match,
        current: &Scope,
        seed: &str,
        input: Option<&str>,
        imports: Option<&[String]>,
        outer: &Scope,
    ) -> Option<Self> {
        let [part] = pattern.patterns.as_slice() else {
            return None;
        };
        let path = &part.element;
        let imported = |name: &str| {
            if imports.is_some_and(|names| !names.iter().any(|item| item == name)) {
                return None;
            }
            current.columns.get(name)?;
            outer.columns.get(name)
        };
        let entity = |variable: &str, label: &str| {
            let (Some(Value::Entity(outer)), Some(Value::Entity(bound))) =
                (imported(variable), current.columns.get(variable))
            else {
                return None;
            };
            (outer.node
                && !outer.dynamic
                && !bound.dynamic
                && outer.label == label
                && bound.label == label)
                .then(|| {
                    let value = qualified("seed", variable);
                    catalog.nodes[label]
                        .key
                        .iter()
                        .map(|key| (key.clone(), entities::property(&value, key)))
                        .collect::<Vec<_>>()
                })
        };
        let (node, fields, mut steps, reversed) = if path.chain.is_empty() {
            let variable = &path.start.variable.as_ref()?.0;
            let label = match &path.start.labels {
                Some(LabelExpr::Leaf(label)) => label.clone(),
                None => match imported(variable)? {
                    Value::Entity(binding) if binding.node && !binding.dynamic => {
                        binding.label.clone()
                    }
                    _ => return None,
                },
                _ => return None,
            };
            let node = catalog.nodes.get(&label)?;
            let fields = entity(variable, &label).or_else(|| {
                let mut fields = BTreeMap::new();
                key_equalities(
                    pattern.where_clause.as_ref()?,
                    variable,
                    &node.key,
                    &imported,
                    &mut fields,
                );
                (!fields.is_empty()).then(|| fields.into_iter().collect())
            })?;
            (label, fields, Vec::new(), false)
        } else {
            let mut steps = Vec::<Step>::new();
            for (rel, _) in &path.chain {
                let Some(LabelExpr::Leaf(label)) = &rel.types else {
                    return None;
                };
                let edge = catalog.edges.get(label)?;
                if rel.range.is_some() || edge.key.is_empty() {
                    return None;
                }
                let (from, to) = match rel.direction {
                    Direction::Outgoing => (&edge.from, &edge.to),
                    Direction::Incoming => (&edge.to, &edge.from),
                    _ => return None,
                };
                if steps.last().is_some_and(|step| step.to.node != from.node) {
                    return None;
                }
                steps.push(Step {
                    edge: label.clone(),
                    edge_alias: String::new(),
                    from: from.clone(),
                    to: to.clone(),
                    to_alias: String::new(),
                    keys: catalog.nodes[&to.node].key.clone(),
                });
            }
            let first = &steps.first()?.from.node;
            let last = &steps.last()?.to.node;
            if let Some(fields) = path
                .start
                .variable
                .as_ref()
                .and_then(|var| entity(&var.0, first))
            {
                (first.clone(), fields, steps, false)
            } else {
                let fields = entity(&path.chain.last()?.1.variable.as_ref()?.0, last)?;
                let label = last.clone();
                steps.reverse();
                for step in &mut steps {
                    std::mem::swap(&mut step.from, &mut step.to);
                    step.keys.clone_from(&catalog.nodes[&step.to.node].key);
                }
                (label, fields, steps, true)
            }
        };

        let postgres = |label: &str| {
            let mapping = catalog.mapping(label);
            if !mapping.inputs.is_empty() {
                return None;
            }
            match catalog.source(mapping.source).ok()? {
                (name, _, Source::Postgres { .. }) => Some(name),
                _ => None,
            }
        };
        let remote_frontier = steps
            .first()
            .and_then(|step| postgres(&step.edge))
            .is_some_and(|source| {
                steps.iter().all(|step| {
                    postgres(&step.edge) == Some(source) && postgres(&step.to.node) == Some(source)
                })
            });
        let alias_for = |index| {
            let mut alias = format!("{seed}_lookup_{}_{index}", pattern.span.start);
            while catalog.nodes.contains_key(&alias) || catalog.edges.contains_key(&alias) {
                alias.push('_');
            }
            alias
        };
        let node_alias = alias_for(0);
        for (index, step) in steps.iter_mut().enumerate() {
            step.edge_alias = alias_for(index * 2 + 1);
            step.to_alias = alias_for(index * 2 + 2);
        }
        let mut nodes = std::iter::once((&node, &node_alias))
            .chain(steps.iter().map(|step| (&step.to.node, &step.to_alias)))
            .collect::<Vec<_>>();
        let mut edges = steps.iter().collect::<Vec<_>>();
        if reversed {
            nodes.reverse();
            edges.reverse();
        }
        let mut aliases = BTreeMap::<String, VecDeque<String>>::new();
        let mut seen = BTreeSet::new();
        for (pattern, (label, alias)) in std::iter::once(&path.start)
            .chain(path.chain.iter().map(|(_, node)| node))
            .zip(nodes)
        {
            if pattern
                .variable
                .as_ref()
                .is_none_or(|variable| seen.insert(&variable.0))
            {
                aliases
                    .entry(label.clone())
                    .or_default()
                    .push_back(alias.clone());
            }
        }
        for step in edges {
            aliases
                .entry(step.edge.clone())
                .or_default()
                .push_back(step.edge_alias.clone());
        }
        let select = fields
            .iter()
            .map(|(name, value)| format!("{value} AS {}", ident(name)))
            .collect::<Vec<_>>()
            .join(", ");
        let anchor_variable = path.chain.last().and_then(|(_, end)| {
            let bound = if reversed { end } else { &path.start };
            bound.variable.as_ref().map(|name| name.0.clone())
        });
        let source = input.map_or_else(|| ident(seed), |sql| format!("({sql})"));
        Some(Self {
            seed_sql: format!("SELECT {select} FROM {source} AS seed"),
            node,
            node_alias,
            anchor_variable,
            columns: fields.into_iter().map(|(name, _)| name).collect(),
            steps,
            aliases,
            remote_frontier,
        })
    }

    pub(super) fn reads(&self, table: &str) -> bool {
        self.seed_sql.contains(&ident(table))
    }

    pub(super) async fn register(
        &self,
        context: &SessionContext,
        scope: &QueryScope,
        mode: LookupMode,
    ) -> Result<Vec<MemoryReservation>> {
        let mut budgets = Vec::new();
        let mut frames = match mode {
            LookupMode::Schema => BTreeMap::new(),
            _ => self.frames(context, scope, mode, &mut budgets).await?,
        };
        for (label, aliases) in &self.aliases {
            let base = context.table(TableReference::bare(label.as_str())).await?;
            for alias in aliases {
                let frame = frames.remove(alias).unwrap_or_else(|| base.clone());
                let alias = TableReference::bare(alias.as_str());
                context.deregister_table(alias.clone())?;
                context.register_table(alias, frame.into_view())?;
            }
        }
        Ok(budgets)
    }

    pub(super) fn remove(&self, context: &SessionContext) -> Result<()> {
        for name in self.aliases.values().flatten() {
            context.deregister_table(TableReference::bare(name.as_str()))?;
        }
        Ok(())
    }

    async fn frames(
        &self,
        context: &SessionContext,
        scope: &QueryScope,
        mode: LookupMode,
        budgets: &mut Vec<MemoryReservation>,
    ) -> Result<BTreeMap<String, DataFrame>> {
        let seed = graph_frame(context, &self.seed_sql).await?;
        let start = context
            .table(TableReference::bare(self.node.as_str()))
            .await?;
        if !seed
            .schema()
            .fields()
            .iter()
            .zip(&self.columns)
            .all(|(field, name)| {
                supported(field.data_type())
                    && start
                        .schema()
                        .field_with_unqualified_name(name)
                        .is_ok_and(|target| target.data_type() == field.data_type())
            })
        {
            return Ok(BTreeMap::new());
        }
        let set_join = matches!(mode, LookupMode::Set);
        let mut frontier = match keys(seed.clone(), scope, MAX_NEIGHBORS).await? {
            Some(keys) => Frontier::Inline(keys),
            // Large CALLs already have the modeled equality join. A batched
            // key view can reread the source for each key batch.
            None if set_join => return Ok(BTreeMap::new()),
            None => Frontier::spillable(context, scope, seed, budgets).await?,
        };
        let remote = if let Frontier::Inline(starts) = &frontier
            && self.remote_frontier
        {
            Some(starts.clone())
        } else {
            None
        };
        let mut frames = BTreeMap::from([(
            self.node_alias.clone(),
            frontier.restrict(context, scope, start, &self.columns)?,
        )]);
        if matches!(mode, LookupMode::Batch { bounded: true })
            && let Some(starts) = &remote
            && let Some(filters) = self.postgres_filters(context, scope, starts).await?
        {
            return self.remote_frames(context, frames, filters).await;
        }
        for step in &self.steps {
            let edge = context
                .table(TableReference::bare(step.edge.as_str()))
                .await?;
            let edge = frontier.restrict(context, scope, edge, &step.from.columns)?;
            let targets = edge.clone().select(
                step.to
                    .columns
                    .iter()
                    .map(|key| col(Column::from_name(key))),
            )?;
            if !targets
                .schema()
                .fields()
                .iter()
                .all(|field| supported(field.data_type()))
            {
                frames.insert(step.edge_alias.clone(), edge);
                break;
            }
            let cap = if step.keys.len() == 1 {
                MAX_NEIGHBORS * 8
            } else {
                MAX_NEIGHBORS
            };
            let inline = if let Some(starts) = &remote {
                let small = keys(targets.clone(), scope, MAX_NEIGHBORS).await?;
                if small.is_none()
                    && let Some(filters) = self.postgres_filters(context, scope, starts).await?
                {
                    return self.remote_frames(context, frames, filters).await;
                }
                match small {
                    Some(keys) => Some(keys),
                    None => keys(targets.clone(), scope, cap).await?,
                }
            } else {
                keys(targets.clone(), scope, cap).await?
            };
            frontier = match inline {
                Some(keys) => Frontier::Inline(keys),
                None => Frontier::spillable(context, scope, targets, budgets).await?,
            };
            let target = context
                .table(TableReference::bare(step.to.node.as_str()))
                .await?;
            frames.insert(step.edge_alias.clone(), edge);
            frames.insert(
                step.to_alias.clone(),
                frontier.restrict(context, scope, target, &step.keys)?,
            );
        }
        Ok(frames)
    }

    async fn remote_frames(
        &self,
        context: &SessionContext,
        mut frames: BTreeMap<String, DataFrame>,
        filters: BTreeMap<String, String>,
    ) -> Result<BTreeMap<String, DataFrame>> {
        for (label, aliases) in &self.aliases {
            for alias in aliases {
                if let Some(filter) = filters.get(alias) {
                    let base = context.table(TableReference::bare(label.as_str())).await?;
                    frames.insert(
                        alias.clone(),
                        postgres::restrict_view(context, base, filter).await?,
                    );
                }
            }
        }
        Ok(frames)
    }
}

enum Frontier {
    Inline(Keys),
    Batched(DataFrame),
}

impl Frontier {
    async fn spillable(
        context: &SessionContext,
        scope: &QueryScope,
        frame: DataFrame,
        budgets: &mut Vec<MemoryReservation>,
    ) -> Result<Self> {
        let columns = frame.schema().columns();
        let order = columns
            .iter()
            .map(|column| col(column.clone()).sort(true, false))
            .collect();
        // Keys are globally unique before batching, so a modeled row can only
        // be read by one batch even when the frontier has duplicate edges.
        let frame = frame
            .filter(
                columns
                    .into_iter()
                    .map(|c| col(c).is_not_null())
                    .reduce(Expr::and)
                    .expect("nonempty frontier key"),
            )?
            .distinct()?
            .sort(order)?;
        let physical = frame.create_physical_plan().await?;
        let mut stream =
            datafusion::physical_plan::execute_stream(Arc::clone(&physical), context.task_ctx())?;
        let mut buffer = Buffer::new(
            &scope.runtime.memory_pool,
            Some(&scope.runtime.disk_manager),
        );
        while let Some(batch) = stream.try_next().await? {
            scope.check()?;
            buffer.push(batch)?;
        }
        scope.record_spill(physical.as_ref());
        let (table, budget) = buffer.finish(stream.schema(), &scope.reads)?;
        budgets.push(budget);
        Ok(Self::Batched(context.read_table(table)?))
    }

    fn restrict(
        &self,
        context: &SessionContext,
        scope: &QueryScope,
        frame: DataFrame,
        columns: &[String],
    ) -> Result<DataFrame> {
        match self {
            Self::Inline(keys) => {
                let predicate = parquet::predicate(
                    frame.logical_plan(),
                    columns,
                    keys,
                    scope.options.memory_bytes,
                )
                .unwrap_or_else(|| restrict(columns, keys));
                Ok(frame.filter(predicate)?)
            }
            Self::Batched(keys) => batched::view(context, scope, frame, keys.clone(), columns),
        }
    }
}

fn supported(ty: &DataType) -> bool {
    matches!(
        ty,
        DataType::Boolean | DataType::Int16 | DataType::Int32 | DataType::Int64 | DataType::Utf8
    )
}

fn key_equalities<'a>(
    expr: &opencypher::Expr,
    node: &str,
    keys: &[String],
    imported: &impl Fn(&str) -> Option<&'a Value>,
    fields: &mut BTreeMap<String, String>,
) {
    match &expr.kind {
        ExprKind::Binary {
            op: opencypher::BinaryOp::And,
            lhs,
            rhs,
        } => {
            key_equalities(lhs, node, keys, imported, fields);
            key_equalities(rhs, node, keys, imported, fields);
        }
        ExprKind::Comparison { first, rest } => {
            let [(opencypher::ComparisonOp::Eq, second)] = rest.as_slice() else {
                return;
            };
            for (candidate, value) in [(first.as_ref(), second), (second, first.as_ref())] {
                let ExprKind::Property { base, key } = &candidate.kind else {
                    continue;
                };
                if !matches!(&base.kind, ExprKind::Variable(name) if name.0 == node)
                    || !keys.contains(&key.0)
                {
                    continue;
                }
                if let ExprKind::Variable(name) = &value.kind
                    && matches!(imported(&name.0), Some(Value::Scalar))
                {
                    fields.insert(key.0.clone(), qualified("seed", &name.0));
                }
            }
        }
        _ => {}
    }
}

type Keys = HashSet<Vec<ScalarValue>>;

async fn keys(frame: DataFrame, scope: &QueryScope, cap: usize) -> Result<Option<Keys>> {
    let limit = cap * 4;
    let mut stream = frame.limit(0, Some(limit + 1))?.execute_stream().await?;
    let mut result = HashSet::new();
    let mut seen = 0;
    let mut bytes = 0usize;
    let budget = (scope.options.memory_bytes / 16).min(4 * 1024 * 1024);
    while let Some(batch) = stream.try_next().await? {
        scope.check()?;
        seen += batch.num_rows();
        if seen > limit {
            return Ok(None);
        }
        for i in 0..batch.num_rows() {
            if batch.columns().iter().any(|column| column.is_null(i)) {
                continue;
            }
            let key = batch
                .columns()
                .iter()
                .map(|column| ScalarValue::try_from_array(column, i))
                .collect::<datafusion::common::Result<Vec<_>>>()?;
            if !result.contains(&key) {
                bytes = bytes.saturating_add(key.iter().map(ScalarValue::size).sum::<usize>());
                if bytes > budget || result.len() == cap {
                    return Ok(None);
                }
                result.insert(key);
            }
        }
    }
    Ok(Some(result))
}

fn restrict(columns: &[String], keys: &Keys) -> Expr {
    if keys.is_empty() {
        return lit(false);
    }
    if let [column] = columns {
        let column = col(Column::from_name(column));
        let membership = || {
            column
                .clone()
                .in_list(keys.iter().map(|key| lit(key[0].clone())).collect(), false)
        };
        let range = |lo: &ScalarValue, hi: &ScalarValue| {
            if lo == hi {
                column.clone().eq(lit(lo.clone()))
            } else {
                column
                    .clone()
                    .gt_eq(lit(lo.clone()))
                    .and(column.clone().lt_eq(lit(hi.clone())))
            }
        };
        if keys.len() >= 64
            && let Some(ranges) = integer_ranges(keys)
        {
            if ranges.len() <= keys.len() / 4 {
                return disjunction(ranges.into_iter().map(|(lo, hi)| range(lo, hi)).collect());
            }
            // Parquet can prune row groups by the sorted batch's envelope even
            // when it cannot simplify a large, sparse IN predicate.
            return range(ranges[0].0, ranges[ranges.len() - 1].1).and(membership());
        }
        return membership();
    }
    let terms = keys
        .iter()
        .map(|key| {
            columns
                .iter()
                .zip(key)
                .map(|(column, value)| col(Column::from_name(column)).eq(lit(value.clone())))
                .reduce(Expr::and)
                .expect("nonempty business key")
        })
        .collect::<Vec<_>>();
    disjunction(terms)
}

fn integer_ranges(keys: &Keys) -> Option<Vec<(&ScalarValue, &ScalarValue)>> {
    let mut numbers = keys
        .iter()
        .map(|key| {
            let value = &key[0];
            let number = match value {
                ScalarValue::Int16(Some(value)) => i64::from(*value),
                ScalarValue::Int32(Some(value)) => i64::from(*value),
                ScalarValue::Int64(Some(value)) => *value,
                _ => return None,
            };
            Some((number, value))
        })
        .collect::<Option<Vec<_>>>()?;
    numbers.sort_unstable_by_key(|(number, _)| *number);
    let mut result = Vec::new();
    let mut low = numbers[0].1;
    for pair in numbers.windows(2) {
        if pair[0].0.checked_add(1) != Some(pair[1].0) {
            result.push((low, pair[0].1));
            low = pair[1].1;
        }
    }
    result.push((low, numbers.last()?.1));
    Some(result)
}

fn disjunction(mut terms: Vec<Expr>) -> Expr {
    while terms.len() > 1 {
        terms = terms
            .chunks(2)
            .map(|pair| match pair {
                [left, right] => left.clone().or(right.clone()),
                [last] => last.clone(),
                _ => unreachable!(),
            })
            .collect();
    }
    terms.pop().expect("nonempty key set")
}
