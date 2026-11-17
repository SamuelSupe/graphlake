mod batch_call;
mod existence;
mod path_page;
mod row_expression;
pub(super) use existence::{exists, has_identifier, lambda_values, replace_identifiers};
pub(super) use row_expression::{RowExpression, pattern_list, row_subquery};
use std::collections::BTreeMap;

use anyhow::{Context, Result, bail, ensure};
use opencypher::{CallKind, Clause, Expr, ExprKind, Query, SingleQuery};

use super::{
    Aliases, Binding, MatchPlan, MatchTypes, OrderKey, Plan, bind_pattern,
    entities::{self, IdentityList, PathTypes},
    paths::Extensions,
    procedures,
};
use crate::{
    Catalog,
    catalog::{ident, qualified},
};

#[derive(Clone, Default)]
pub(super) struct Scope {
    pub(super) columns: BTreeMap<String, Value>,
}

#[derive(Clone)]
pub(super) enum Value {
    Entity(Binding),
    EntityList(Binding),
    Path(PathTypes),
    Identities(IdentityList),
    NestedList(Box<Value>),
    Scalar,
    Null,
}

#[cfg(feature = "admin")]
impl Scope {
    pub(super) fn graph_columns(
        &self,
    ) -> BTreeMap<String, crate::engine::query_graph::GraphColumn> {
        self.columns
            .iter()
            .filter_map(|(name, value)| Some((name.clone(), value.graph_column()?)))
            .collect()
    }
}

impl Value {
    #[cfg(feature = "admin")]
    fn graph_column(&self) -> Option<crate::engine::query_graph::GraphColumn> {
        use crate::engine::query_graph::GraphColumn;
        let binding_column = |binding: &Binding| GraphColumn::Entity {
            label: (!binding.dynamic).then(|| binding.label.clone()),
            node: binding.node,
        };
        Some(match self {
            Self::Entity(binding) => binding_column(binding),
            Self::EntityList(binding) => GraphColumn::List(Box::new(binding_column(binding))),
            Self::NestedList(value) => GraphColumn::List(Box::new(value.graph_column()?)),
            Self::Path(path) => {
                let label = |list: &IdentityList| {
                    if list.tagged {
                        None
                    } else {
                        list.labels.first().cloned()
                    }
                };
                GraphColumn::Path {
                    labels: Some((label(&path.nodes), label(&path.edges))),
                }
            }
            _ => return None,
        })
    }

    pub(super) fn merge(&mut self, other: &Self) -> bool {
        match (&mut *self, other) {
            (_, Self::Null) => true,
            (Self::Null, other) => {
                *self = other.clone();
                true
            }
            (Self::Entity(left), Self::Entity(right))
            | (Self::EntityList(left), Self::EntityList(right))
                if left.node == right.node =>
            {
                *left = entities::merged(&[left.clone(), right.clone()]);
                true
            }
            (Self::Path(left), Self::Path(right)) => {
                left.nodes.merge(&right.nodes);
                left.edges.merge(&right.edges);
                true
            }
            (Self::Identities(left), Self::Identities(right)) if left.node == right.node => {
                left.merge(right);
                true
            }
            (Self::NestedList(left), Self::NestedList(right)) => left.merge(right),
            (Self::Identities(_) | Self::NestedList(_), Self::Scalar)
            | (Self::Scalar, Self::Identities(_) | Self::NestedList(_)) => {
                *self = Self::Scalar;
                true
            }
            (Self::Scalar, Self::Scalar) => true,
            _ => false,
        }
    }

    pub(super) fn shape(
        &self,
        catalog: &Catalog,
        source: &Self,
        value: &str,
        depth: usize,
    ) -> String {
        let item = format!("__graphlake_shape_{depth}");
        match (self, source) {
            (Self::Entity(target), Self::Entity(source)) => entities::shape(source, target, value),
            (Self::EntityList(target), Self::EntityList(source)) if target.dynamic => {
                let shape = entities::shape(source, target, &item);
                format!("array_transform({value}, {item} -> {shape})")
            }
            (Self::Path(target), Self::Path(source)) => {
                let list = |key: &str, before: &IdentityList, after: &IdentityList| {
                    let list = format!("get_field({value}, '{key}')");
                    if after.tagged {
                        before.tagged_list(catalog, &list)
                    } else {
                        list
                    }
                };
                let nodes = list("nodes", &source.nodes, &target.nodes);
                let edges = list("relationships", &source.edges, &target.edges);
                format!(
                    "CASE WHEN {value} IS NOT NULL THEN named_struct('length', get_field({value}, 'length'), 'nodes', {nodes}, 'relationships', {edges}) END"
                )
            }
            (Self::Identities(target), Self::Identities(source)) if target.tagged => {
                source.tagged_list(catalog, value)
            }
            (Self::NestedList(target), Self::NestedList(source)) => {
                let shape = target.shape(catalog, source, &item, depth + 1);
                if shape == item {
                    value.to_owned()
                } else {
                    format!("array_transform({value}, {item} -> {shape})")
                }
            }
            (Self::Entity(_) | Self::EntityList(_), Self::Null) => "NULL".into(),
            _ => value.to_owned(),
        }
    }

    pub(super) fn projected(plan: &MatchPlan, expr: &Expr) -> Self {
        Self::inferred(plan, expr, &Aliases::new())
    }

    pub(super) fn inferred(plan: &MatchPlan, expr: &Expr, aliases: &Aliases) -> Self {
        if !super::expression_step() {
            return Self::Scalar;
        }
        if matches!(
            expr.kind,
            ExprKind::ListComprehension { .. } | ExprKind::PatternComprehension { .. }
        ) {
            return match plan.list_item(expr, aliases) {
                Self::Entity(binding) => Self::EntityList(binding),
                value if value.is_nested_item() => Self::NestedList(Box::new(value)),
                _ => Self::Scalar,
            };
        }
        if let ExprKind::Variable(name) = &expr.kind
            && let Some((_, value)) = aliases.get(&name.0)
        {
            return value.clone();
        }
        if let ExprKind::Index { base, .. } = &expr.kind
            && let Some(item) = Self::nested_item(plan, base, aliases)
        {
            return item;
        }
        if let Some(binding) = plan.entity_type(expr, aliases) {
            return Self::Entity(binding);
        }
        if let Some(shape) = entities::identity_list(plan, expr, aliases) {
            return Self::Identities(shape);
        }
        if let Some(item) = Self::nested_item(plan, expr, aliases) {
            return Self::NestedList(Box::new(item));
        }
        if let Some(binding) = plan.entity_list(expr, aliases) {
            return Self::EntityList(binding);
        }
        if let ExprKind::Variable(name) = &expr.kind
            && let Some(shape) = plan.path_types.get(&name.0)
        {
            return Self::Path(shape.clone());
        }
        if matches!(expr.kind, ExprKind::Null)
            || matches!(&expr.kind, ExprKind::Variable(name) if plan.nulls.contains(&name.0))
        {
            Self::Null
        } else {
            Self::Scalar
        }
    }

    fn is_nested_item(&self) -> bool {
        matches!(
            self,
            Self::Path(_) | Self::Identities(_) | Self::EntityList(_) | Self::NestedList(_)
        )
    }

    pub(super) fn nested_item(plan: &MatchPlan, expr: &Expr, aliases: &Aliases) -> Option<Self> {
        if !super::expression_step() {
            return None;
        }
        match &expr.kind {
            ExprKind::Variable(name) => match aliases.get(&name.0) {
                Some((_, Self::NestedList(item))) => Some(*item.clone()),
                Some(_) => None,
                None => plan.nested_lists.get(&name.0).cloned(),
            },
            ExprKind::Slice { base, .. } => Self::nested_item(plan, base, aliases),
            ExprKind::PatternComprehension { .. } | ExprKind::ListComprehension { .. } => {
                let item = plan.list_item(expr, aliases);
                item.is_nested_item().then_some(item)
            }
            ExprKind::List(items) => {
                let value =
                    Self::common(items.iter().map(|item| Self::inferred(plan, item, aliases)))?;
                value.is_nested_item().then_some(value)
            }
            ExprKind::Case { whens, default, .. } => Self::common(
                whens
                    .iter()
                    .map(|(_, value)| value)
                    .chain(default.as_deref())
                    .filter(|value| !empty_list(value))
                    .map(|value| Self::nested_item(plan, value, aliases))
                    .collect::<Option<Vec<_>>>()?
                    .into_iter(),
            )
            .filter(Self::is_nested_item),
            ExprKind::FunctionCall {
                name,
                args,
                distinct: false,
            } if name.0.eq_ignore_ascii_case("coalesce") => Self::common(
                args.iter()
                    .filter(|value| !empty_list(value))
                    .map(|value| Self::nested_item(plan, value, aliases))
                    .collect::<Option<Vec<_>>>()?
                    .into_iter(),
            )
            .filter(Self::is_nested_item),
            ExprKind::FunctionCall { name, args, .. } if name.0.eq_ignore_ascii_case("collect") => {
                let [item] = args.as_slice() else { return None };
                let item = Self::inferred(plan, item, aliases);
                item.is_nested_item().then_some(item)
            }
            ExprKind::Index { base, .. } => match Self::nested_item(plan, base, aliases)? {
                Self::NestedList(item) => Some(*item),
                _ => None,
            },
            _ => None,
        }
    }

    fn common(values: impl Iterator<Item = Self>) -> Option<Self> {
        let mut kind = Self::Null;
        for value in values {
            if !kind.merge(&value) {
                return None;
            }
        }
        Some(kind)
    }
}

pub(super) fn empty_list(expr: &Expr) -> bool {
    matches!(expr.kind, ExprKind::Null)
        || matches!(&expr.kind, ExprKind::List(items) if items.is_empty())
}

impl Scope {
    pub(super) fn from_plan(plan: &MatchPlan) -> Self {
        let mut columns = BTreeMap::new();
        columns.extend(
            plan.bindings
                .iter()
                .map(|(name, binding)| (name.clone(), Value::Entity(binding.clone()))),
        );
        columns.extend(
            plan.path_types
                .iter()
                .map(|(name, shape)| (name.clone(), Value::Path(shape.clone()))),
        );
        columns.extend(
            plan.list_types
                .iter()
                .map(|(name, shape)| (name.clone(), Value::Identities(shape.clone()))),
        );
        columns.extend(plan.scalars.keys().cloned().map(|name| {
            let value = if plan.nulls.contains(&name) {
                Value::Null
            } else {
                Value::Scalar
            };
            (name, value)
        }));
        columns.extend(
            plan.entity_lists
                .iter()
                .map(|(name, binding)| (name.clone(), Value::EntityList(binding.clone()))),
        );
        columns.extend(
            plan.nested_lists
                .iter()
                .map(|(name, item)| (name.clone(), Value::NestedList(Box::new(item.clone())))),
        );
        Self { columns }
    }

    pub(super) fn bind(&self, alias: &str, plan: &mut MatchPlan) {
        for (name, kind) in &self.columns {
            let value = qualified(alias, name);
            match kind {
                Value::Entity(entity) => {
                    let mut entity = entity.clone();
                    entity.stored = Some(value);
                    plan.bindings.insert(name.clone(), entity);
                }
                Value::Path(shape) => {
                    plan.lengths
                        .insert(name.clone(), format!("get_field({value}, 'length')"));
                    plan.paths
                        .insert(name.clone(), format!("get_field({value}, 'relationships')"));
                    plan.path_nodes
                        .insert(name.clone(), format!("get_field({value}, 'nodes')"));
                    plan.path_types.insert(name.clone(), shape.clone());
                }
                Value::Identities(shape) => {
                    plan.lengths
                        .insert(name.clone(), format!("array_length({value})"));
                    plan.lists.insert(name.clone(), value);
                    plan.list_types.insert(name.clone(), shape.clone());
                }
                Value::Scalar | Value::Null => {
                    plan.scalars.insert(name.clone(), value);
                    if matches!(kind, Value::Null) {
                        plan.nulls.insert(name.clone());
                    }
                }
                Value::EntityList(binding) => {
                    plan.scalars.insert(name.clone(), value);
                    plan.entity_lists.insert(name.clone(), binding.clone());
                }
                Value::NestedList(item) => {
                    plan.scalars.insert(name.clone(), value);
                    plan.nested_lists.insert(name.clone(), *item.clone());
                }
            }
        }
    }
}

struct Stage {
    sql: String,
    scope: Scope,
    output: Vec<String>,
    order: Vec<OrderKey>,
}

pub(super) struct ExpressionJoin {
    pub(super) alias: String,
    pub(super) seed: String,
    pub(super) captures: Vec<(String, String)>,
    pub(super) query: String,
    pub(super) expanded: Option<(String, String)>,
    pub(super) calls: Option<Vec<super::apply::Apply>>,
}

impl ExpressionJoin {
    pub(super) fn sql(&mut self, from: &str, calls: &mut Vec<super::apply::Apply>) -> String {
        let captures = self
            .captures
            .iter()
            .map(|(name, value)| {
                let key = format!("__graphlake_cypher_capture_key({value})");
                (name, value, key)
            })
            .collect::<Vec<_>>();
        let keys = if self.captures.is_empty() {
            "TRUE".to_owned()
        } else {
            captures
                .iter()
                .map(|(name, _, key)| {
                    format!(
                        "(__graphlake_cypher_capture_key({}) IS NOT DISTINCT FROM {key})",
                        qualified(&self.alias, name)
                    )
                })
                .collect::<Vec<_>>()
                .join(" AND ")
        };
        let seed_sql = if self.captures.is_empty() {
            "SELECT 1 AS __graphlake_seed".to_owned()
        } else {
            let keys = captures
                .iter()
                .map(|(_, _, key)| key.as_str())
                .collect::<Vec<_>>();
            let projection = captures
                .iter()
                .map(|(name, value, _)| format!("first_value({value}) AS {}", ident(name)))
                .collect::<Vec<_>>();
            format!(
                "SELECT {}{from} GROUP BY {}",
                projection.join(", "),
                keys.join(", ")
            )
        };
        if let Some(nested) = self.calls.as_mut() {
            // Nested CALL registers its own inputs and can read the captured
            // rows through a query-local table during that registration.
            if !nested.is_empty() {
                calls.push(super::apply::Apply {
                    table: self.alias.clone(),
                    seed: self.seed.clone(),
                    outer: seed_sql,
                    inner: self.query.clone(),
                    nested: std::mem::take(nested),
                    lookups: Vec::new(),
                    limit: None,
                    mode: super::apply::Mode::Relation {
                        expanded: self.expanded.clone(),
                    },
                });
            }
            return format!(" LEFT JOIN {} ON {keys}", ident(&self.alias));
        }
        let alias = ident(&self.alias);
        if self.captures.is_empty() {
            return format!(" LEFT JOIN ({}) AS {alias} ON TRUE", self.query);
        }
        let cte = self
            .expanded
            .as_ref()
            .map_or_else(String::new, |(name, sql)| {
                format!(", {} AS ({sql})", ident(name))
            });
        format!(
            " LEFT JOIN (WITH {} AS ({seed_sql}){cte} {}) AS {alias} ON {keys}",
            ident(&self.seed),
            self.query,
        )
    }
}

impl Stage {
    fn input(&self) -> String {
        format!(" FROM ({}) AS l", self.sql)
    }

    fn bind(&self, plan: &mut MatchPlan) {
        self.scope.bind("l", plan);
        plan.input_order.clone_from(&self.order);
    }

    fn sort(&mut self) {
        let order = self
            .order
            .iter()
            .map(|key| key.term(Some("l")))
            .collect::<Vec<_>>();
        self.sql = format!("SELECT l.*{} ORDER BY {}", self.input(), order.join(", "));
    }

    fn discard_order_columns(&mut self) {
        if self.order.is_empty() {
            return;
        }
        let visible = self
            .scope
            .columns
            .keys()
            .map(|name| qualified("l", name))
            .collect::<Vec<_>>();
        let order = self
            .order
            .drain(..)
            .map(|key| key.term(Some("l")))
            .collect::<Vec<_>>();
        self.sql = format!(
            "SELECT {}{} ORDER BY {}",
            visible.join(", "),
            self.input(),
            order.join(", ")
        );
    }
}

#[derive(Clone, Copy)]
struct Imports<'a> {
    outer: &'a Scope,
    seed: &'a str,
    explicit: Option<&'a [String]>,
}

impl Imports<'_> {
    fn selected(self, names: &[String]) -> Result<Scope> {
        let columns = names
            .iter()
            .map(|name| {
                let kind = self
                    .outer
                    .columns
                    .get(name)
                    .with_context(|| format!("unknown outer variable {name}"))?;
                Ok((name.clone(), kind.clone()))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        Ok(Scope { columns })
    }

    fn stage(self, names: &[String]) -> Result<Stage> {
        let scope = self.selected(names)?;
        let projection = names
            .iter()
            .map(|name| qualified("seed", name))
            .collect::<Vec<_>>();
        Ok(Stage {
            sql: format!(
                "SELECT {} FROM {} AS seed",
                projection.join(", "),
                ident(self.seed)
            ),
            scope,
            output: Vec::new(),
            order: Vec::new(),
        })
    }

    fn restore(self, sql: String, scope: &mut Scope) -> Result<String> {
        let Some(names) = self.explicit else {
            return Ok(sql);
        };
        let mut extra = Vec::new();
        for (name, kind) in self.selected(names)?.columns {
            if let std::collections::btree_map::Entry::Vacant(entry) = scope.columns.entry(name) {
                extra.push(qualified("seed", entry.key()));
                entry.insert(kind);
            }
        }
        if extra.is_empty() {
            Ok(sql)
        } else {
            Ok(format!(
                "SELECT result.*, {} FROM ({sql}) AS result CROSS JOIN {} AS seed",
                extra.join(", "),
                ident(self.seed)
            ))
        }
    }
}

fn legacy_imports(query: &SingleQuery, outer: &Scope) -> bool {
    matches!(query.clauses.first(), Some(Clause::With(with))
        if with.body.items.iter().any(|item| matches!(&item.expr.kind,
            ExprKind::Variable(name) if outer.columns.contains_key(&name.0))))
}

fn query_imports(query: &Query, outer: &Scope) -> bool {
    std::iter::once(&query.initial)
        .chain(query.unions.iter().map(|branch| &branch.query))
        .any(|query| legacy_imports(query, outer))
}

pub(super) fn plan(catalog: &Catalog, query: &Query, extensions: &mut Extensions) -> Result<Plan> {
    let mut plan = Plan {
        indexed_csr: extensions.indexed_csr,
        ..Default::default()
    };
    let saved = extensions.clone();
    let stage = branches(catalog, query, extensions, &mut plan, None, true)?;
    if plan.calls.iter().any(super::apply::Apply::has_lookup)
        && plan.expansions.is_empty()
        && plan.procedures.is_empty()
        && stage
            .scope
            .columns
            .values()
            .all(|kind| matches!(kind, Value::Scalar | Value::Null))
        && let Some(narrow) = super::apply::scalar_projection(catalog, query)
    {
        let mut projected = Plan {
            indexed_csr: extensions.indexed_csr,
            ..Default::default()
        };
        *extensions = saved;
        projected.sql = branches(&narrow, query, extensions, &mut projected, None, true)?.sql;
        return Ok(projected);
    }
    #[cfg(feature = "admin")]
    {
        plan.graph_output.columns = stage.scope.graph_columns();
    }
    plan.sql = stage.sql;
    Ok(plan)
}

fn branches(
    catalog: &Catalog,
    query: &Query,
    extensions: &mut Extensions,
    plan: &mut Plan,
    imported: Option<Imports<'_>>,
    public: bool,
) -> Result<Stage> {
    let left = single(
        catalog,
        &query.initial,
        extensions,
        plan,
        imported,
        public && query.unions.is_empty(),
    )?;
    if query.unions.is_empty() {
        return Ok(left);
    }
    let mut stages = vec![left];
    for branch in &query.unions {
        stages.push(single(
            catalog,
            &branch.query,
            extensions,
            plan,
            imported,
            false,
        )?);
    }
    combine_union(catalog, query, stages, None, public)
}

fn combine_union(
    catalog: &Catalog,
    query: &Query,
    stages: Vec<Stage>,
    partition: Option<&str>,
    public: bool,
) -> Result<Stage> {
    let mut stages = stages.into_iter();
    let mut left = stages.next().expect("UNION has an initial branch");
    left.order.clear();
    let mut inputs = vec![(std::mem::take(&mut left.sql), left.scope.clone())];
    let mut last_distinct = None;
    for (index, (branch, right)) in query.unions.iter().zip(stages).enumerate() {
        ensure!(
            left.scope.columns.keys().eq(right.scope.columns.keys()),
            "UNION branches must return the same column names"
        );
        for (name, kind) in &mut left.scope.columns {
            let other = &right.scope.columns[name];
            ensure!(
                kind.merge(other),
                "UNION column {name} has incompatible graph types"
            );
        }
        inputs.push((right.sql, right.scope));
        if !branch.all {
            last_distinct = Some(index + 1);
        }
    }

    // Keep each scalar branch in a separate SQL column until its real Arrow
    // type is known. SQL UNION only sees a value and nulls in each column.
    let sources = (0..inputs.len())
        .map(|index| {
            let mut name = format!("__graphlake_union_input_{}_{index}", query.span.start);
            while catalog.nodes.contains_key(&name) || catalog.edges.contains_key(&name) {
                name.push('_');
            }
            ident(&name)
        })
        .collect::<Vec<_>>();
    let partition = partition.map(ident).into_iter().collect::<Vec<_>>();
    let mut projections = vec![partition.clone(); inputs.len()];
    let mut output = partition.clone();
    let mut keys = partition.clone();
    for (column, name) in left.output.iter().enumerate() {
        let field = ident(name);
        if matches!(left.scope.columns[name], Value::Scalar | Value::Null) {
            let slots = (0..inputs.len())
                .map(|branch| format!("__graphlake_union_{column}_{branch}"))
                .collect::<Vec<_>>();
            for (branch, projection) in projections.iter_mut().enumerate() {
                for (slot, name) in slots.iter().enumerate() {
                    let value = if slot == branch {
                        field.clone()
                    } else {
                        // A nested UNION can refine the source type after initial
                        // SQL planning. Bind the inactive slot to the same source.
                        format!(
                            "CASE WHEN FALSE THEN (SELECT {field} FROM {} LIMIT 0) END",
                            sources[slot]
                        )
                    };
                    projection.push(format!("{value} AS {name}"));
                }
            }
            output.push(format!(
                "__graphlake_cypher_property({}) AS {field}",
                slots.join(", ")
            ));
            keys.push(format!("__graphlake_cypher_equivalence({field})"));
        } else {
            let slot = format!("__graphlake_union_{column}");
            for (projection, (_, scope)) in projections.iter_mut().zip(&inputs) {
                let value =
                    left.scope.columns[name].shape(catalog, &scope.columns[name], &field, 0);
                projection.push(format!("{value} AS {slot}"));
            }
            output.push(format!("{slot} AS {field}"));
            keys.push(format!("__graphlake_cypher_equivalence({field})"));
        }
    }
    // Choose private column names even when a user returns the usual prefix.
    let internal = |stem: &str| {
        let mut name = stem.to_owned();
        while left.scope.columns.contains_key(&name) {
            name.push('_');
        }
        ident(&name)
    };
    let branch = internal("__graphlake_union_branch");
    let rank = internal("__graphlake_union_rank");
    let ctes = sources
        .iter()
        .zip(inputs)
        .map(|(name, (sql, _))| format!("{name} AS ({sql})"))
        .collect::<Vec<_>>()
        .join(", ");
    let inputs = sources
        .iter()
        .zip(projections)
        .enumerate()
        .map(|(index, (source, mut projection))| {
            if last_distinct.is_some() {
                projection.push(format!("{index} AS {branch}"));
            }
            format!("SELECT {} FROM {source}", projection.join(", "))
        })
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    if let Some(last_distinct) = last_distinct {
        output.push(branch.clone());
        let combined = format!("SELECT {} FROM ({inputs}) AS combined", output.join(", "));
        // A later UNION removes duplicates from its whole prefix. UNION ALL
        // branches after the last UNION retain all their rows.
        let visible = partition
            .iter()
            .cloned()
            .chain(left.output.iter().map(|name| ident(name)))
            .collect::<Vec<_>>();
        left.sql = format!(
            "SELECT {} FROM (SELECT result.*, row_number() OVER
             (PARTITION BY {} ORDER BY {branch} ROWS UNBOUNDED PRECEDING) AS {rank}
             FROM ({combined}) AS result) AS ranked WHERE {branch} > {last_distinct} OR {rank} = 1",
            visible.join(", "),
            keys.join(", "),
        );
    } else {
        left.sql = format!("SELECT {} FROM ({inputs}) AS combined", output.join(", "));
    }
    if public {
        let visible = super::public_projection(left.output.iter().map(String::as_str));
        left.sql = format!("SELECT {visible} FROM ({}) AS result", left.sql);
    }
    left.sql = format!("WITH {ctes} {}", left.sql);
    Ok(left)
}

fn single(
    catalog: &Catalog,
    query: &SingleQuery,
    extensions: &mut Extensions,
    plan: &mut Plan,
    imported: Option<Imports<'_>>,
    public: bool,
) -> Result<Stage> {
    let grouped = (plan.indexed_csr && imported.is_none())
        .then(|| path_page::Grouped::describe(query))
        .flatten();
    let query = grouped.as_ref().map_or(query, |grouped| &grouped.query);
    let (body, ret) = match query.clauses.as_slice() {
        [body @ .., Clause::Return(ret)] => (body, Some(ret)),
        body @ [.., Clause::Call(call)] if matches!(&call.kind, CallKind::Procedure(_)) => {
            (body, None)
        }
        _ => bail!("a read-only Cypher branch must end with RETURN or a procedure call"),
    };
    let mut stage = imported
        .and_then(|imports| imports.explicit.map(|names| imports.stage(names)))
        .transpose()?;
    let mut output_limited = false;
    let mut path_page = None;
    let limit = ret.and_then(|ret| {
        let mut aggregate = super::HasAggregate(false);
        for item in &ret.body.items {
            opencypher::Visitor::visit_expr(&mut aggregate, &item.expr);
        }
        if ret.distinct || !ret.body.order_by.is_empty() || aggregate.0 {
            return None;
        }
        let ExprKind::Integer(limit) = ret.body.limit.as_ref()?.kind else {
            return None;
        };
        let skip = match &ret.body.skip {
            Some(Expr {
                kind: ExprKind::Integer(skip),
                ..
            }) => usize::try_from(*skip).ok()?,
            None => 0,
            _ => return None,
        };
        let limit = usize::try_from(limit).ok()?;
        Some(if limit == 0 {
            0
        } else {
            skip.saturating_add(limit)
        })
    });

    for (index, clause) in body.iter().enumerate() {
        let mut scope = MatchPlan::new(catalog);
        scope.call_imports = extensions.call_imports.clone();
        scope.partitioned_csr = extensions.partitioned_csr;
        scope.indexed_csr = plan.indexed_csr;
        if let Some(stage) = &stage {
            stage.bind(&mut scope);
        }
        match clause {
            Clause::Match(pattern) => {
                let anchor = imported.map(|imports| {
                    let first = usize::from(imports.explicit.is_none());
                    (imports, index > first)
                });
                stage = Some(join_match(
                    catalog, pattern, stage, None, extensions, plan, anchor,
                )?);
            }
            Clause::With(with)
                if index == 0
                    && imported.is_some_and(|imports| {
                        imports.explicit.is_none() && {
                            with.body.items.iter().any(|item| {
                                matches!(&item.expr.kind,
                    ExprKind::Variable(var) if imports.outer.columns.contains_key(&var.0))
                            })
                        }
                    }) =>
            {
                ensure!(
                    !with.distinct
                        && !with.body.star
                        && with.where_clause.is_none()
                        && with.body.order_by.is_empty()
                        && with.body.limit.is_none()
                        && with.body.skip.is_none(),
                    "importing WITH requires only outer variables"
                );
                let imports = imported.expect("importing WITH");
                let names = with
                    .body
                    .items
                    .iter()
                    .map(|item| {
                        let ExprKind::Variable(name) = &item.expr.kind else {
                            bail!("importing WITH requires an outer variable");
                        };
                        ensure!(
                            item.alias.is_none(),
                            "importing WITH does not rename variables"
                        );
                        Ok(name.0.clone())
                    })
                    .collect::<Result<Vec<_>>>()?;
                stage = Some(imports.stage(&names)?);
            }
            Clause::With(with) => {
                if let Some(names) = imported.and_then(|imports| imports.explicit) {
                    for item in &with.body.items {
                        if let Some(alias) = &item.alias
                            && names.contains(&alias.0)
                        {
                            ensure!(
                                matches!(&item.expr.kind, ExprKind::Variable(name) if name == alias),
                                "WITH cannot overwrite imported variable {}",
                                alias.0
                            );
                        }
                    }
                }
                let replay = path_page
                    .filter(|(start, end)| *start < index && index <= *end)
                    .map(|(_, end)| {
                        let mut replay = with.clone();
                        if end == index {
                            replay.body.order_by.clear();
                            replay.body.skip = None;
                            replay.body.limit = None;
                        } else {
                            replay.where_clause = None;
                        }
                        replay
                    });
                let with = replay.as_ref().unwrap_or(with);
                let mut body = with.body.clone();
                let skip = body.skip.take();
                let limit = body.limit.take();
                let mut next = project_with(
                    catalog,
                    &scope,
                    with,
                    &body,
                    stage.as_ref().map_or_else(String::new, Stage::input),
                    |next| {
                        if let Some(imports) = imported {
                            next.sql =
                                imports.restore(std::mem::take(&mut next.sql), &mut next.scope)?;
                        }
                        if !next.order.is_empty()
                            && (with.where_clause.is_some() || skip.is_some() || limit.is_some())
                        {
                            next.sort();
                        }
                        for (clause, expression) in [("LIMIT", limit), ("OFFSET", skip)] {
                            if let Some(expr) = expression {
                                let value = batch_call::page(Some(&expr), 0, clause)?;
                                next.sql.push_str(&format!(" {clause} {value}"));
                            }
                        }
                        Ok(())
                    },
                    imported
                        .and_then(|imports| imports.explicit)
                        .unwrap_or_default(),
                    plan,
                )?;
                if let Some(grouped) = grouped.as_ref().filter(|grouped| grouped.page == index) {
                    path_page::restore(&mut next, catalog, &grouped.edges);
                }
                stage = Some(next);
            }
            Clause::Unwind(unwind) => {
                let page = plan
                    .indexed_csr
                    .then(|| path_page::describe(&body[index + 1..], ret))
                    .flatten();
                let end = page.as_ref().map(|page| index + page.distance());
                let projection = grouped.as_ref().filter(|grouped| grouped.unwind == index);
                let (next, paged) = unwind_stage(
                    catalog,
                    unwind,
                    stage,
                    scope,
                    plan,
                    page,
                    projection.map(|grouped| &grouped.fields),
                )?;
                stage = Some(next);
                if paged {
                    path_page = end.map(|end| (index, end));
                }
            }
            Clause::Call(call) if let CallKind::Procedure(procedure) = &call.kind => {
                stage = Some(call_procedure(catalog, procedure, stage, extensions, plan)?);
            }
            Clause::Call(call) => {
                let call_limit = (index + 1 == body.len()).then_some(limit).flatten();
                let (next, limited) =
                    call_subquery(catalog, call, stage, extensions, plan, call_limit)?;
                stage = Some(next);
                output_limited = limited;
            }
            _ => bail!("unsupported Cypher clause in read-only query"),
        }
    }
    let Some(ret) = ret else {
        return stage.context("procedure did not produce a relation");
    };
    let mut scope = MatchPlan::new(catalog);
    scope.call_imports = extensions.call_imports.clone();
    scope.partitioned_csr = extensions.partitioned_csr;
    scope.indexed_csr = plan.indexed_csr;
    if let Some(stage) = &stage {
        stage.bind(&mut scope);
    }
    let unwind_paged = path_page.is_some_and(|(_, end)| end == body.len());
    let body = if output_limited || unwind_paged {
        let mut body = ret.body.clone();
        body.limit = None;
        if unwind_paged {
            body.skip = None;
            body.order_by.clear();
        }
        Some(body)
    } else {
        None
    };
    let (sql, columns, _) = scope.project(
        body.as_ref().unwrap_or(&ret.body),
        ret.distinct,
        stage.as_ref().map_or_else(String::new, Stage::input),
        &[],
        false,
        public,
    )?;
    scope.collect_expressions(plan);
    let mut output = if ret.body.star {
        Scope::from_plan(&scope).columns.into_keys().collect()
    } else {
        Vec::new()
    };
    output.extend(ret.body.items.iter().map(|item| {
        item.alias
            .as_ref()
            .map(|name| name.0.clone())
            .unwrap_or_else(|| crate::cypher::expression_name(&item.expr))
    }));
    Ok(Stage {
        sql,
        scope: columns,
        output,
        order: Vec::new(),
    })
}

fn call_subquery(
    catalog: &Catalog,
    call: &opencypher::Call,
    previous: Option<Stage>,
    extensions: &mut Extensions,
    plan: &mut Plan,
    call_limit: Option<usize>,
) -> Result<(Stage, bool)> {
    let CallKind::Subquery(query) = &call.kind else {
        bail!("only read-only CALL subqueries are supported");
    };
    let explicit = extensions.call_imports.get(&call.span.start).cloned();
    let seed = format!("__graphlake_call_seed_{}", call.span.start);
    let imports = match explicit.as_deref() {
        Some([]) => None,
        Some(names) => {
            let outer = &previous
                .as_ref()
                .context("CALL has no outer variables to import")?
                .scope;
            Some(Imports {
                outer,
                seed: &seed,
                explicit: Some(names),
            })
        }
        None => previous.as_ref().and_then(|stage| {
            query_imports(query, &stage.scope).then_some(Imports {
                outer: &stage.scope,
                seed: &seed,
                explicit: None,
            })
        }),
    };
    if let (Some(imports), Some(left)) = (imports, previous.as_ref())
        && let Some(result) =
            batch_call::plan(catalog, query, imports, left, extensions, plan, call_limit)?
    {
        return Ok((result, call_limit.is_some_and(|limit| limit != 0)));
    }
    let prior_calls = plan.calls.len();
    let prior_lookups = plan.call_lookups.len();
    let import_names = imports.map(|imports| {
        imports.explicit.map_or_else(
            || {
                std::iter::once(&query.initial)
                    .chain(query.unions.iter().map(|branch| &branch.query))
                    .filter_map(|query| match query.clauses.first() {
                        Some(Clause::With(with)) => Some(&with.body.items),
                        _ => None,
                    })
                    .flatten()
                    .filter_map(|item| match &item.expr.kind {
                        ExprKind::Variable(var) if imports.outer.columns.contains_key(&var.0) => {
                            Some(var.0.clone())
                        }
                        _ => None,
                    })
                    .collect::<std::collections::BTreeSet<_>>()
                    .into_iter()
                    .collect()
            },
            |names| names.to_vec(),
        )
    });
    let mut limited = false;
    let mut right = branches(catalog, query, extensions, plan, imports, false)?;
    if let Some(mut left) = previous {
        for name in right.scope.columns.keys() {
            ensure!(
                !left.scope.columns.contains_key(name),
                "CALL result {name} is already bound"
            );
        }
        left.discard_order_columns();
        right.scope.columns.extend(left.scope.columns);
        if let Some(imports) = import_names {
            let table = format!("__graphlake_call_{}", call.span.start);
            let mut sequence = format!("{table}_sequence");
            while right.scope.columns.contains_key(&sequence) {
                sequence.push('_');
            }
            let nested = plan.calls.split_off(prior_calls);
            let lookups = plan.call_lookups.split_off(prior_lookups);
            plan.calls.push(super::apply::Apply {
                table: table.clone(),
                seed,
                outer: left.sql,
                inner: right.sql,
                nested,
                lookups,
                limit: call_limit,
                mode: super::apply::Mode::Row {
                    imports,
                    sequence: sequence.clone(),
                },
            });
            limited = call_limit.is_some_and(|limit| limit != 0);
            right.sql = format!("SELECT * FROM {}", ident(&table));
            right.order = vec![OrderKey {
                column: sequence,
                descending: false,
                native: true,
            }];
        } else {
            right.sql = format!(
                "SELECT outer_row.*, subquery.* FROM ({}) AS outer_row CROSS JOIN ({}) AS subquery",
                left.sql, right.sql
            );
        }
    }
    Ok((right, limited))
}

fn project_with(
    catalog: &Catalog,
    input: &MatchPlan,
    with: &opencypher::With,
    body: &opencypher::ReturnBody,
    from: String,
    restore_and_page: impl FnOnce(&mut Stage) -> Result<()>,
    imports: &[String],
    plan: &mut Plan,
) -> Result<Stage> {
    let grouped = with.distinct
        || body
            .items
            .iter()
            .any(|item| batch_call::has_aggregate(&item.expr));
    let mut body = body.clone();
    let mut hidden = Vec::new();
    if !grouped
        && !body.star
        && let Some(predicate) = &with.where_clause
    {
        let projected = body
            .items
            .iter()
            .map(|item| {
                item.alias
                    .as_ref()
                    .map_or_else(|| super::expression_name(&item.expr), |v| v.0.clone())
            })
            .collect::<std::collections::BTreeSet<_>>();
        // A plain WITH's WHERE can refer to input variables that were not projected.
        let referenced = super::page::referenced_names(predicate);
        for name in Scope::from_plan(input).columns.into_keys() {
            if !projected.contains(&name) && referenced.contains(&name) {
                let variable = opencypher::Variable(name.clone());
                body.items.push(opencypher::ReturnItem {
                    expr: Expr {
                        kind: ExprKind::Variable(variable.clone()),
                        span: with.span,
                    },
                    alias: Some(variable),
                    span: with.span,
                });
                if !imports.contains(&name) {
                    hidden.push(name);
                }
            }
        }
    }
    let (sql, scope, order) = input.project(&body, with.distinct, from, &[], true, false)?;
    input.collect_expressions(plan);
    let mut next = Stage {
        sql,
        scope,
        output: Vec::new(),
        order,
    };
    restore_and_page(&mut next)?;
    if let Some(predicate) = &with.where_clause {
        let mut output = MatchPlan::new(catalog);
        output.call_imports = input.call_imports.clone();
        output.indexed_csr = plan.indexed_csr;
        output.partitioned_csr = input.partitioned_csr;
        next.scope.bind("projected", &mut output);
        let predicate = output.expr(predicate)?;
        let from = output.join_expressions(&format!(" FROM ({}) AS projected", next.sql));
        for name in hidden {
            next.scope.columns.remove(&name);
        }
        next.sql = format!("SELECT projected.*{from} WHERE {predicate}");
        output.collect_expressions(plan);
    }
    Ok(next)
}

fn unwind_stage(
    catalog: &Catalog,
    unwind: &opencypher::Unwind,
    mut stage: Option<Stage>,
    scope: MatchPlan,
    plan: &mut Plan,
    page: Option<path_page::Page<'_>>,
    projection: Option<&std::collections::BTreeSet<String>>,
) -> Result<(Stage, bool)> {
    let list = if matches!(unwind.expr.kind, ExprKind::Null) {
        "make_array()".into()
    } else {
        scope.expr(&unwind.expr)?
    };
    let mut columns = Scope::from_plan(&scope);
    ensure!(
        !columns.columns.contains_key(&unwind.variable.0),
        "UNWIND variable is already bound"
    );
    let keep = if let Some(stage) = &mut stage {
        stage.discard_order_columns();
        "l.*, "
    } else {
        ""
    };
    let from = scope.join_expressions(&stage.as_ref().map_or_else(String::new, Stage::input));
    scope.collect_expressions(plan);
    let (sql, value, order) = if let Some(shape) =
        entities::identity_list(&scope, &unwind.expr, &Aliases::new())
        && !shape.labels.is_empty()
    {
        // A direct path list references entities from this immutable snapshot. General
        // list expressions still use the normal UNWIND and its missing-entity check.
        let page = page.filter(|_| plan.indexed_csr && !shape.node
            && matches!(&unwind.expr.kind, ExprKind::FunctionCall { name, args, distinct: false }
                if name.0.eq_ignore_ascii_case("relationships")
                && matches!(args.as_slice(), [Expr { kind: ExprKind::Variable(var), .. }] if scope.path_types.contains_key(&var.0))));
        unwind_identities(
            catalog,
            &columns,
            &unwind.variable.0,
            &list,
            &from,
            &shape,
            plan,
            page,
            projection,
        )?
    } else {
        (
            format!(
                "SELECT {keep}unnest({list}) AS {}{from} WHERE {list} IS NOT NULL",
                ident(&unwind.variable.0)
            ),
            Value::nested_item(&scope, &unwind.expr, &Aliases::new())
                .or_else(|| {
                    scope
                        .entity_list(&unwind.expr, &Aliases::new())
                        .map(Value::Entity)
                })
                .unwrap_or(Value::Scalar),
            Vec::new(),
        )
    };
    let paged = !order.is_empty();
    columns.columns.insert(unwind.variable.0.clone(), value);
    Ok((
        Stage {
            sql,
            scope: columns,
            output: Vec::new(),
            order,
        },
        paged,
    ))
}

fn unwind_identities(
    catalog: &Catalog,
    scope: &Scope,
    name: &str,
    list: &str,
    from: &str,
    shape: &IdentityList,
    plan: &mut Plan,
    page: Option<path_page::Page<'_>>,
    projection: Option<&std::collections::BTreeSet<String>>,
) -> Result<(String, Value, Vec<OrderKey>)> {
    let bindings = shape.bindings(catalog);
    let result = entities::merged(&bindings);
    plan.labels.extend(shape.labels.iter().cloned());
    let seed = scope
        .columns
        .keys()
        .map(|name| format!("{}, ", qualified("l", name)))
        .collect::<String>();
    let prior = scope
        .columns
        .keys()
        .map(|name| format!("{}, ", qualified("u", name)))
        .collect::<String>();
    let mut item_column = "__graphlake_unwind_edge".to_owned();
    while scope.columns.contains_key(&item_column) {
        item_column.push('_');
    }
    let item = qualified("u", &item_column);
    let column = ident(&item_column);
    let input = format!("SELECT {seed}unnest({list}) AS {column}{from} WHERE {list} IS NOT NULL");
    if plan.indexed_csr {
        let id = shape
            .item_id(catalog, &item)
            .expect("identity list has a label");
        let payload =
            IdentityList::indexed_lookup(&bindings, &result, &format!("make_array({id})"));
        let alias = ident(name);
        if let Some(fields) = projection.filter(|_| !shape.node) {
            let (bindings, result) = path_page::project_relationship(&bindings, &result, fields);
            let value = path_page::relationship(
                catalog,
                shape,
                &bindings,
                &result,
                &item,
                &id,
                fields.is_empty(),
            );
            let sql = format!(
                "SELECT {prior}{value} AS {alias} FROM ({input}) AS u WHERE {item} IS NOT NULL"
            );
            return Ok((sql, Value::Entity(result), Vec::new()));
        }
        if let Some(page) = page
            && let Some((selected, order)) = page.select(
                catalog, scope, name, shape, &bindings, &result, &input, &item, &id,
            )?
        {
            let keys = order
                .iter()
                .map(|key| qualified("u", &key.column))
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "SELECT {prior}array_element({payload}, 1) AS {alias}, {keys} FROM ({selected}) AS u"
            );
            return Ok((sql, Value::Entity(result), order));
        }
        let sql = format!(
            "SELECT * FROM (SELECT {prior}array_element({payload}, 1) AS {alias} FROM ({input}) AS u) AS csr_unwind WHERE {alias} IS NOT NULL"
        );
        return Ok((sql, Value::Entity(result), Vec::new()));
    }
    if shape.tagged {
        let lookup = IdentityList::lookup(&bindings, &result);
        let id = shape
            .item_id(catalog, &item)
            .expect("identity list has a label");
        let sql = format!(
            "SELECT {prior}items.payload AS {} FROM ({input}) AS u JOIN ({lookup}) AS items ON items.__graphlake_lookup_id = {id}",
            ident(name)
        );
        return Ok((sql, Value::Entity(result), Vec::new()));
    }
    let branches = bindings
        .iter()
        .map(|binding| {
            let predicate = entities::match_key(binding, &item);
            let value = entities::shape(binding, &result, &binding.value());
            format!(
                "SELECT {prior}{value} AS {} FROM ({input}) AS u JOIN {} AS e ON {predicate}",
                ident(name),
                ident(&binding.label)
            )
        })
        .collect::<Vec<_>>();
    Ok((
        branches.join(" UNION ALL "),
        Value::Entity(result),
        Vec::new(),
    ))
}

fn call_procedure(
    catalog: &Catalog,
    call: &opencypher::ProcedureCall,
    mut previous: Option<Stage>,
    extensions: &Extensions,
    plan: &mut Plan,
) -> Result<Stage> {
    let (relation, fields) = procedures::relation(catalog, call, plan)?;
    let yielded = match &call.yields {
        Some(yield_) if !yield_.star => yield_
            .items
            .iter()
            .map(|item| {
                let (_, field) = fields
                    .iter()
                    .find(|(name, _)| *name == item.field.0)
                    .with_context(|| format!("unknown YIELD field {}", item.field.0))?;
                let name = item.alias.as_ref().unwrap_or(&item.field).0.clone();
                Ok((name, *field))
            })
            .collect::<Result<Vec<_>>>()?,
        _ => fields
            .iter()
            .map(|(name, column)| ((*name).to_owned(), *column))
            .collect(),
    };
    let mut scope = previous
        .as_ref()
        .map(|stage| stage.scope.clone())
        .unwrap_or_default();
    let mut output = scope.columns.keys().cloned().collect::<Vec<_>>();
    let mut selected = output
        .iter()
        .map(|name| qualified("l", name))
        .collect::<Vec<_>>();
    for (name, field) in yielded {
        ensure!(
            !scope.columns.contains_key(&name),
            "procedure output {name} is already bound"
        );
        selected.push(format!("{} AS {}", qualified("r", field), ident(&name)));
        scope.columns.insert(name.clone(), Value::Scalar);
        output.push(name);
    }
    let from = if let Some(left) = &mut previous {
        left.discard_order_columns();
        format!("{} CROSS JOIN ({relation}) AS r", left.input())
    } else {
        format!(" FROM ({relation}) AS r")
    };
    let mut sql = format!("SELECT {}{from}", selected.join(", "));
    if let Some(predicate) = call
        .yields
        .as_ref()
        .and_then(|yield_| yield_.where_clause.as_ref())
    {
        let mut bindings = MatchPlan::new(catalog);
        bindings.call_imports = extensions.call_imports.clone();
        scope.bind("p", &mut bindings);
        let predicate = bindings.expr(predicate)?;
        let from = bindings.join_expressions(&format!(" FROM ({sql}) AS p"));
        sql = format!("SELECT p.*{from} WHERE {predicate}");
        bindings.collect_expressions(plan);
    }
    Ok(Stage {
        sql,
        scope,
        output,
        order: Vec::new(),
    })
}

fn join_match(
    catalog: &Catalog,
    pattern: &opencypher::Match,
    left: Option<Stage>,
    outer: Option<&Scope>,
    extensions: &mut Extensions,
    plan: &mut Plan,
    anchor: Option<(Imports<'_>, bool)>,
) -> Result<Stage> {
    let left = left.unwrap_or_else(|| Stage {
        sql: "SELECT 1 AS __graphlake_seed".into(),
        scope: Scope::default(),
        output: Vec::new(),
        order: Vec::new(),
    });
    let prior = outer.map(|outer| {
        let mut prior = outer.clone();
        prior.columns.extend(left.scope.columns.clone());
        prior
    });
    let base = outer.map_or(plan.expansion_base, |_| {
        1_000_000 + pattern.span.start * 512
    }) + plan.expansions.len();
    let lookup = anchor
        .filter(|_| {
            pattern
                .patterns
                .iter()
                .all(|part| !extensions.shortest.contains_key(&part.element.span.start))
        })
        .and_then(|(anchor, from_stage)| {
            super::apply::Lookup::plan(
                catalog,
                pattern,
                &left.scope,
                anchor.seed,
                if from_stage { Some(&left.sql) } else { None },
                if from_stage { None } else { anchor.explicit },
                if from_stage {
                    &left.scope
                } else {
                    anchor.outer
                },
            )
        });
    let first_expansion = plan.expansions.len();
    let (right_sql, right_columns, consume_predicate) = match_input(
        catalog,
        pattern,
        prior.as_ref().unwrap_or(&left.scope),
        extensions,
        plan,
        base,
        lookup.as_ref(),
    )?;
    if let Some((imports, false)) = anchor {
        for expansion in &mut plan.expansions[first_expansion..] {
            if expansion.mixed.is_none()
                && catalog.edges[&expansion.edge].from.node == catalog.edges[&expansion.edge].to.node
                && expansion.starts.is_empty()
                && expansion.start_variable.as_ref().is_some_and(|name| {
                    matches!(left.scope.columns.get(name), Some(Value::Entity(binding))
                        if binding.node && !binding.dynamic && binding.label == expansion.start_node)
                        && imports.explicit.is_none_or(|names| names.contains(name))
                })
            {
                expansion.start_seed = Some(imports.seed.to_owned());
            }
        }
    }
    let mut bound_anchor = None;
    if let Some(lookup) = lookup
        && lookup
            .aliases()
            .values()
            .flatten()
            .any(|alias| right_sql.contains(&ident(alias)))
    {
        bound_anchor = lookup.anchor_variable().map(str::to_owned);
        plan.call_lookups.push(lookup);
    }

    let mut join = Vec::new();
    let mut scope = left.scope.clone();
    let mut project = left
        .scope
        .columns
        .keys()
        .map(|name| qualified("l", name))
        .collect::<Vec<_>>();
    for (name, kind) in &right_columns.columns {
        if let Some(previous) = left.scope.columns.get(name) {
            let (Value::Entity(left), Value::Entity(right)) = (previous, kind) else {
                bail!("pattern variable {name} is already bound");
            };
            ensure!(
                left.node == right.node
                    && (left.dynamic || right.dynamic || left.label == right.label),
                "incompatible pattern variable {name}"
            );
            if left.dynamic || right.dynamic {
                let (left_label, left_key) = entities::identity(&qualified("l", name));
                let (right_label, right_key) = entities::identity(&qualified("r", name));
                join.push(format!(
                    "{left_label} = {right_label} AND {left_key} = {right_key}"
                ));
            } else {
                join.extend(left.keys.iter().map(|key| {
                    format!(
                        "{} = {}",
                        entities::property(&qualified("l", name), key),
                        entities::property(&qualified("r", name), key)
                    )
                }));
            }
        } else {
            scope.columns.insert(name.clone(), kind.clone());
            project.push(qualified("r", name));
        }
    }
    let mut filter = None;
    let new_predicate_scope = || {
        let mut scope = MatchPlan::new(catalog);
        scope.call_imports = extensions.call_imports.clone();
        scope.indexed_csr = plan.indexed_csr;
        scope.partitioned_csr = extensions.partitioned_csr;
        scope
    };
    let mut predicate_scope = new_predicate_scope();
    let mut optional_filter = None;
    if !consume_predicate && let Some(where_clause) = &pattern.where_clause {
        let mut exists = HasExists(false);
        opencypher::Visitor::visit_expr(&mut exists, where_clause);
        if pattern.optional && exists.0 {
            optional_filter = Some(where_clause);
        } else {
            left.scope.bind("l", &mut predicate_scope);
            if let Some(anchor) = &bound_anchor {
                let mut matched = right_columns.clone();
                matched.columns.remove(anchor);
                matched.bind("r", &mut predicate_scope);
            } else {
                right_columns.bind("r", &mut predicate_scope);
            }
            let expression = predicate_scope.expr(where_clause)?;
            let uses_joins = !predicate_scope.expression_joins.borrow().is_empty();
            if pattern.optional && uses_joins {
                optional_filter = Some(where_clause);
                predicate_scope = new_predicate_scope();
            } else if exists.0 || uses_joins {
                filter = Some(expression);
            } else {
                join.push(expression);
            }
        }
    }
    if join.is_empty() {
        join.push("TRUE".into());
    }
    let join = join.join(" AND ");
    let sql = if let Some(predicate) = optional_filter {
        // Correlated predicates must be evaluated on the matched candidates.
        // Left joining those candidates by input row keeps one null row for
        // unmatched inputs, including duplicate rows from an earlier UNWIND.
        let mut row_name = format!("__graphlake_optional_row_{}", pattern.span.start);
        while scope.columns.contains_key(&row_name) {
            row_name.push('_');
        }
        let row = ident(&row_name);
        let table = format!("__graphlake_optional_input_{}", pattern.span.start);
        plan.calls.push(super::apply::Apply {
            table: table.clone(),
            seed: String::new(),
            outer: format!("SELECT l.*, row_number() OVER () AS {row}{}", left.input()),
            inner: String::new(),
            nested: Vec::new(),
            lookups: Vec::new(),
            limit: None,
            mode: super::apply::Mode::Materialize,
        });
        // Both branches must read the same numbered rows, even when the
        // upstream partitions have no deterministic order.
        let table = ident(&table);
        let right_fields = right_columns
            .columns
            .keys()
            .filter(|name| !left.scope.columns.contains_key(*name))
            .map(|name| format!(", {}", qualified("c", name)))
            .collect::<String>();
        if project.is_empty() {
            project.push("1 AS __graphlake_seed".into());
        }
        let mut candidates = scope.clone();
        let mut fields = vec![qualified("l", &row_name)];
        for name in candidates.columns.keys() {
            let right =
                right_columns.columns.contains_key(name) && bound_anchor.as_ref() != Some(name);
            fields.push(qualified(if right { "r" } else { "l" }, name));
        }
        candidates.columns.extend(
            right_columns
                .columns
                .iter()
                .filter(|(name, _)| bound_anchor.as_ref() != Some(*name))
                .map(|(name, kind)| (name.clone(), kind.clone())),
        );
        candidates.bind("c", &mut predicate_scope);
        let filter = predicate_scope.expr(predicate)?;
        let candidate_sql = format!(
            "SELECT {} FROM {table} AS l JOIN ({right_sql}) AS r ON {join}",
            fields.join(", "),
        );
        let from = if predicate_scope
            .expression_joins
            .borrow()
            .iter()
            .any(|expression| !expression.captures.is_empty())
        {
            // The predicate joins read candidates separately to form their
            // distinct capture keys. Reuse one join result for both reads.
            let candidate_table = format!("__graphlake_optional_candidates_{}", pattern.span.start);
            plan.calls.push(super::apply::Apply {
                table: candidate_table.clone(),
                seed: String::new(),
                outer: candidate_sql,
                inner: String::new(),
                nested: Vec::new(),
                lookups: Vec::new(),
                limit: None,
                mode: super::apply::Mode::Materialize,
            });
            format!(" FROM {} AS c", ident(&candidate_table))
        } else {
            format!(" FROM ({candidate_sql}) AS c")
        };
        let from = predicate_scope.join_expressions(&from);
        format!(
            "SELECT {projection} FROM {table} AS l
             LEFT JOIN (
               SELECT c.{row}{right_fields}{from} WHERE {filter}
             ) AS r ON l.{row} = r.{row}",
            projection = project.join(", "),
        )
    } else {
        if project.is_empty() {
            project.push("1 AS __graphlake_seed".into());
        }
        let join_type = if pattern.optional {
            "LEFT JOIN"
        } else {
            "JOIN"
        };
        let from = predicate_scope.join_expressions(&format!(
            "{} {join_type} ({right_sql}) AS r ON {join}",
            left.input()
        ));
        let filter = filter.map_or_else(String::new, |expression| format!(" WHERE {expression}"));
        format!("SELECT {}{from}{filter}", project.join(", "))
    };
    predicate_scope.collect_expressions(plan);
    Ok(Stage {
        sql,
        scope,
        output: Vec::new(),
        order: Vec::new(),
    })
}

pub(super) struct HasExists(pub(super) bool);

impl opencypher::Visitor for HasExists {
    fn visit_expr(&mut self, expr: &Expr) {
        if matches!(
            expr.kind,
            ExprKind::Exists(_) | ExprKind::PatternComprehension { .. }
        ) {
            self.0 = true;
        } else {
            opencypher::walk_expr(self, expr);
        }
    }
}

fn typed_match_input(
    catalog: &Catalog,
    pattern: &opencypher::Match,
    prior: &Scope,
    extensions: &mut Extensions,
    plan: &mut Plan,
    (match_types, base): (MatchTypes, usize),
    lookup: Option<&super::apply::Lookup>,
) -> Result<(String, Scope, bool)> {
    let mut right = MatchPlan::new(catalog);
    right.call_imports = extensions.call_imports.clone();
    right.partitioned_csr = extensions.partitioned_csr;
    right.indexed_csr = extensions.indexed_csr;
    if let Some(lookup) = lookup {
        right.relation_overrides = lookup.aliases().clone();
        right.relation_order = lookup.relation_order();
    }
    bind_pattern(
        catalog,
        pattern,
        &mut right,
        prior,
        &mut extensions.shortest,
        base,
        match_types,
    )?;
    if let Some(anchor) = lookup.and_then(super::apply::Lookup::anchor_variable)
        && let Some(binding) = right.bindings.get_mut(anchor)
    {
        // The left row already owns the bound entity. Carry its keys through
        // this join without cloning its payload onto every candidate edge.
        binding.properties.clone_from(&binding.keys);
    }
    for node in pattern.patterns.iter().flat_map(|part| {
        std::iter::once(&part.element.start).chain(part.element.chain.iter().map(|(_, node)| node))
    }) {
        if let Some((name, fields)) = extensions.page_fields.get(&node.span.start)
            && let Some(binding) = right.bindings.get_mut(name)
        {
            binding
                .properties
                .retain(|name| binding.keys.contains(name) || fields.contains(name));
        }
    }
    render_match_input(right, pattern, plan)
}

fn render_match_input(
    mut right: MatchPlan,
    pattern: &opencypher::Match,
    plan: &mut Plan,
) -> Result<(String, Scope, bool)> {
    let right_columns = Scope::from_plan(&right);
    let mut selected = Vec::new();
    for name in right_columns.columns.keys() {
        selected.push(format!("{} AS {}", right.variable(name)?, ident(name)));
    }
    if selected.is_empty() {
        selected.push("1 AS __graphlake_match".to_owned());
    }
    let consume_predicate = right
        .shortest_ranking
        .as_ref()
        .is_some_and(|rank| rank.consume_predicate);
    if consume_predicate && let Some(predicate) = &pattern.where_clause {
        right.predicates.push(right.expr(predicate)?);
    }
    if let Some(rank) = &right.shortest_ranking {
        let function = if rank.all { "rank" } else { "row_number" };
        selected.push(format!(
            "{function}() OVER (PARTITION BY {} ORDER BY {}) AS __graphlake_path_rank",
            rank.endpoints.join(", "),
            rank.hops
        ));
    }
    let mut right_sql = format!(
        "SELECT {}{}{}",
        selected.join(", "),
        right.join_expressions(&right.from()),
        if right.predicates.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", right.predicates.join(" AND "))
        }
    );
    if right.shortest_ranking.is_some() {
        right_sql =
            format!("SELECT * FROM ({right_sql}) AS ranked WHERE __graphlake_path_rank = 1");
    }
    right.collect_expressions(plan);
    plan.labels.extend(right.labels);
    plan.expansions.extend(right.expansions);

    Ok((right_sql, right_columns, consume_predicate))
}

fn match_input(
    catalog: &Catalog,
    pattern: &opencypher::Match,
    prior: &Scope,
    extensions: &mut Extensions,
    plan: &mut Plan,
    base: usize,
    lookup: Option<&super::apply::Lookup>,
) -> Result<(String, Scope, bool)> {
    if !super::alternatives::has_untyped(catalog, pattern) {
        return typed_match_input(
            catalog,
            pattern,
            prior,
            extensions,
            plan,
            (MatchTypes::Explicit, base),
            lookup,
        );
    }
    let domains = super::alternatives::Domains::new(catalog, pattern, prior)?;
    if let Some(typed) = domains.single(catalog, pattern)? {
        return typed_match_input(
            catalog,
            &typed,
            prior,
            extensions,
            plan,
            (MatchTypes::Inferred, base),
            lookup,
        );
    }
    let mut right =
        super::wildcard::bind(catalog, pattern, &domains, &mut extensions.shortest, base)?;
    right.call_imports = extensions.call_imports.clone();
    render_match_input(right, pattern, plan)
}
