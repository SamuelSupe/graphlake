mod alternatives;
pub(crate) mod apply;
mod clauses;
mod collect;
mod entities;
mod page;
mod parameters;
pub(crate) mod paths;
mod pattern;
pub(crate) mod procedures;
mod wildcard;

use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
};

use anyhow::{Context, Result, anyhow, bail, ensure};
use opencypher::{
    BinaryOp, Clause, Expr, ExprKind, LabelExpr, MapProjectionItem, NodePattern, Properties,
    QuantifierKind, ReturnBody, UnaryOp, Visitor, walk_expr,
};

pub(crate) use parameters::bind_parameters;
use pattern::{MatchTypes, bind_pattern, edge_record};

use crate::{
    catalog::{Catalog, Edge, columns, equal_keys, ident, qualified},
    traversal::{Expansion, PATH},
};

#[derive(Default)]
pub(crate) struct Plan {
    pub sql: String,
    pub labels: BTreeSet<String>,
    pub expansions: Vec<Expansion>,
    pub calls: Vec<apply::Apply>,
    pub(crate) call_lookups: Vec<apply::Lookup>,
    pub procedures: Vec<procedures::Procedure>,
    pub schema_procedures: Vec<procedures::SchemaProcedure>,
    pub repeats: Vec<crate::gremlin::repeat::Repeat>,
    pub(crate) indexed_csr: bool,
    pub(crate) has_row_expressions: bool,
    pub(crate) has_row_expression_lambda: bool,
    pub(crate) expansion_base: usize,
    #[cfg(feature = "admin")]
    pub graph_output: crate::engine::query_graph::GraphOutput,
}

const MAX_EXPRESSION_SQL_BYTES: usize = 256 * 1024;

pub(crate) fn plan(
    catalog: &Catalog,
    query: &str,
    partitioned_csr: bool,
    indexed_csr: bool,
) -> Result<Plan> {
    check_expression_complexity(query, "Cypher statement")?;
    let wrapped = paths::rewrite_count_subqueries(query)?;
    let (text, mut extensions) = paths::parse_wrappers(&wrapped)?;
    extensions.partitioned_csr = partitioned_csr;
    extensions.indexed_csr = indexed_csr;
    let mut ast = opencypher::parse(&text).map_err(|e| anyhow!("invalid Cypher: {e}"))?;
    paths::validate_call_scopes(&ast, &extensions)?;
    collect::fold_sizes(&mut ast);
    page::defer_properties(catalog, &mut ast, &mut extensions);
    let mut exists = clauses::HasExists(false);
    opencypher::Visitor::visit_query(&mut exists, &ast);
    let mut result = if let [Clause::Match(pattern), Clause::Return(ret)] =
        ast.initial.clauses.as_slice()
        && ast.unions.is_empty()
        && extensions.shortest.is_empty()
        && extensions.call_imports.is_empty()
        && !exists.0
        && !pattern.optional
        && !ret.body.star
        && pattern.patterns.iter().all(|part| {
            part.variable.is_none()
                && part
                    .element
                    .chain
                    .iter()
                    .all(|(edge, _)| edge.range.is_none())
        })
        && !alternatives::has_untyped(catalog, pattern)
    {
        let mut builder = MatchPlan::new(catalog);
        builder.partitioned_csr = partitioned_csr;
        builder.indexed_csr = indexed_csr;
        bind_pattern(
            catalog,
            pattern,
            &mut builder,
            &clauses::Scope::default(),
            &mut extensions.shortest,
            0,
            MatchTypes::Explicit,
        )?;
        if let Some(expr) = &pattern.where_clause {
            builder.predicates.push(builder.expr(expr)?);
        }
        let (sql, _output, _) = builder.project(
            &ret.body,
            ret.distinct,
            builder.from(),
            &builder.predicates,
            false,
            true,
        )?;
        let mut plan = Plan {
            sql,
            labels: std::mem::take(&mut builder.labels),
            expansions: std::mem::take(&mut builder.expansions),
            ..Default::default()
        };
        #[cfg(feature = "admin")]
        {
            plan.graph_output.columns = _output.graph_columns();
        }
        builder.collect_expressions(&mut plan);
        plan
    } else {
        clauses::plan(catalog, &ast, &mut extensions)?
    };
    ensure!(
        extensions.shortest.is_empty(),
        "shortest path function must wrap a MATCH path pattern"
    );
    ensure!(
        result.sql.len() <= 1024 * 1024,
        "Cypher plan exceeds the generated SQL size limit"
    );
    result.has_row_expressions = exists.0;
    result.has_row_expression_lambda = result.sql.contains("__graphlake_row_expression_")
        && result.sql.contains("array_transform(");
    Ok(result)
}

#[derive(Default)]
struct MatchPlan<'a> {
    catalog: Option<&'a Catalog>,
    partitioned_csr: bool,
    indexed_csr: bool,
    expression_labels: RefCell<BTreeSet<String>>,
    expression_expansions: RefCell<Vec<Expansion>>,
    expression_calls: RefCell<Vec<apply::Apply>>,
    expression_joins: RefCell<Vec<clauses::ExpressionJoin>>,
    call_imports: std::rc::Rc<BTreeMap<usize, Vec<String>>>,
    lambda_inputs: RefCell<Vec<(String, String)>>,
    labels: BTreeSet<String>,
    bindings: BTreeMap<String, Binding>,
    relations: Vec<(String, usize)>,
    relation_overrides: BTreeMap<String, VecDeque<String>>,
    relation_order: BTreeMap<String, usize>,
    predicates: Vec<String>,
    edges: HashMap<String, Vec<(String, bool)>>,
    mixed_paths: Vec<String>,
    lengths: HashMap<String, String>,
    lists: BTreeMap<String, String>,
    paths: BTreeMap<String, String>,
    path_nodes: BTreeMap<String, String>,
    path_types: BTreeMap<String, entities::PathTypes>,
    list_types: BTreeMap<String, entities::IdentityList>,
    entity_lists: BTreeMap<String, Binding>,
    nested_lists: BTreeMap<String, clauses::Value>,
    scalars: BTreeMap<String, String>,
    nulls: BTreeSet<String>,
    input_order: Vec<OrderKey>,
    partition_column: Option<String>,
    shortest_ranking: Option<ShortestRanking>,
    expansions: Vec<Expansion>,
}

#[derive(Clone)]
struct OrderKey {
    column: String,
    descending: bool,
    native: bool,
}

impl OrderKey {
    fn term(&self, alias: Option<&str>) -> String {
        let column = alias.map_or_else(
            || ident(&self.column),
            |alias| qualified(alias, &self.column),
        );
        if self.native {
            format!("{column} ASC NULLS LAST")
        } else {
            sort_term(&column, self.descending)
        }
    }
}

// Bound recursive descent in the expression parser and subsequent AST walks.
pub(crate) fn check_expression_complexity(input: &str, context: &str) -> Result<()> {
    let input = input.as_bytes();
    let mut cost = 0;
    let mut cursor = 0;
    let mut word = false;
    while cursor < input.len() {
        let ch = input[cursor];
        if let Some(end) = paths::skip_literal(input, cursor)? {
            cost += usize::from(matches!(ch, b'\'' | b'"' | b'`'));
            cursor = end;
            word = false;
        } else {
            let next_word = ch.is_ascii_alphanumeric() || ch == b'_' || !ch.is_ascii();
            cost += usize::from(
                next_word && !word
                    || matches!(
                        ch,
                        b'(' | b'[' | b'{' | b'+' | b'-' | b'^' | b'/' | b'*' | b'%' | b'.'
                    ),
            );
            word = next_word;
            cursor += 1;
        }
        ensure!(
            cost <= 384,
            "{context} exceeds the supported expression complexity"
        );
    }
    Ok(())
}

fn bounded_expression(sql: String) -> Result<String> {
    ensure!(
        sql.len() <= MAX_EXPRESSION_SQL_BYTES,
        "Cypher expression exceeds the generated SQL size limit"
    );
    Ok(sql)
}

fn sort_term(expression: &str, descending: bool) -> String {
    let direction = if descending {
        "DESC NULLS FIRST"
    } else {
        "ASC NULLS LAST"
    };
    format!("__graphlake_cypher_order({expression}) {direction}")
}

struct ShortestRanking {
    endpoints: Vec<String>,
    hops: String,
    all: bool,
    consume_predicate: bool,
}

#[derive(Clone)]
struct Binding {
    alias: String,
    label: String,
    properties: Vec<String>,
    keys: Vec<String>,
    stored: Option<String>,
    node: bool,
    dynamic: bool,
    lanes: BTreeMap<String, Vec<String>>,
}

type Aliases = BTreeMap<String, (String, clauses::Value)>;

impl<'a> MatchPlan<'a> {
    fn new(catalog: &'a Catalog) -> Self {
        Self {
            catalog: Some(catalog),
            ..Default::default()
        }
    }

    fn collect_expressions(&self, plan: &mut Plan) {
        plan.labels.append(&mut self.expression_labels.borrow_mut());
        plan.expansions
            .append(&mut self.expression_expansions.borrow_mut());
        plan.calls.append(&mut self.expression_calls.borrow_mut());
    }

    fn join_expressions(&self, from: &str) -> String {
        let mut joins = self.expression_joins.borrow_mut();
        let from = if from.is_empty() && !joins.is_empty() {
            " FROM (SELECT 1) AS __graphlake_expression_input"
        } else {
            from
        };
        let mut result = from.to_owned();
        for index in 0..joins.len() {
            let depends_on_join = joins[..index].iter().any(|previous| {
                let alias = format!("{}.", ident(&previous.alias));
                joins[index]
                    .captures
                    .iter()
                    .any(|(_, value)| value.contains(&alias))
            });
            let sql = joins[index].sql(
                if depends_on_join { &result } else { from },
                &mut self.expression_calls.borrow_mut(),
            );
            result.push_str(&sql);
        }
        result
    }

    fn lambda_expr(
        &self,
        expr: &Expr,
        aliases: &Aliases,
        item: &str,
        input: &str,
    ) -> Result<String> {
        self.lambda_inputs
            .borrow_mut()
            .push((ident(item), input.to_owned()));
        let result = self.expr_with_aliases(expr, aliases);
        self.lambda_inputs.borrow_mut().pop();
        result
    }

    fn relation(&mut self, label: &str) -> String {
        self.labels.insert(label.to_owned());
        let source = self
            .relation_overrides
            .get_mut(label)
            .and_then(VecDeque::pop_front)
            .unwrap_or_else(|| label.to_owned());
        let alias = format!("g{}", self.relations.len());
        let order = self
            .relation_order
            .get(&source)
            .copied()
            .unwrap_or(usize::MAX);
        self.relations
            .push((format!("{} AS {}", ident(&source), ident(&alias)), order));
        alias
    }

    fn add_relation(&mut self, label: &str) -> String {
        let alias = format!("g{}", self.relations.len());
        self.relations
            .push((format!("{} AS {}", ident(label), ident(&alias)), usize::MAX));
        alias
    }

    fn add_subquery(&mut self, sql: &str) -> String {
        let alias = format!("g{}", self.relations.len());
        self.relations
            .push((format!("({sql}) AS {}", ident(&alias)), usize::MAX));
        alias
    }

    fn node(&mut self, catalog: &Catalog, pattern: &NodePattern, label: &str) -> Result<String> {
        let node = catalog
            .nodes
            .get(label)
            .with_context(|| format!("unknown node type {label}"))?;
        let variable = pattern.variable.as_ref().map(|var| var.0.as_str());
        if let Some(variable) = variable {
            ensure!(
                !self.lengths.contains_key(variable),
                "repeated variable {variable}"
            );
        }
        let alias = if let Some(binding) = variable.and_then(|name| self.bindings.get(name)) {
            ensure!(
                binding.node && binding.label == label,
                "incompatible binding for node {}",
                variable.unwrap()
            );
            binding.alias.clone()
        } else {
            let alias = self.relation(label);
            if let Some(variable) = variable {
                self.bindings.insert(
                    variable.to_owned(),
                    Binding {
                        alias: alias.clone(),
                        label: label.to_owned(),
                        keys: node.key.clone(),
                        stored: None,
                        node: true,
                        dynamic: false,
                        lanes: Default::default(),
                        properties: columns([&node.key, &node.properties]),
                    },
                );
            }
            alias
        };
        self.predicates.extend(property_terms(
            &alias,
            &columns([&node.key, &node.properties]),
            pattern.properties.as_ref(),
        )?);
        Ok(alias)
    }

    fn edge(&mut self, label: &str, edge: &Edge, variable: Option<&str>) -> Result<String> {
        let alias = self.relation(label);
        let identity = edge.identity();
        self.edge_uniqueness(label, edge, &alias, false);
        if let Some(variable) = variable {
            ensure!(
                !self.bindings.contains_key(variable) && !self.lengths.contains_key(variable),
                "repeated relationship binding {variable}"
            );
            self.bindings.insert(
                variable.to_owned(),
                Binding {
                    alias: alias.clone(),
                    label: label.to_owned(),
                    keys: identity.clone(),
                    stored: None,
                    node: false,
                    dynamic: false,
                    lanes: Default::default(),
                    properties: columns([&identity, &edge.properties]),
                },
            );
        }
        Ok(alias)
    }

    fn edge_uniqueness(&mut self, label: &str, edge: &Edge, alias: &str, ranged: bool) {
        let identity = edge.identity();
        let used = self.edges.entry(label.to_owned()).or_default();
        for (other, other_ranged) in used.iter() {
            let same = match (ranged, *other_ranged) {
                (false, false) => equal_keys(other, &identity, alias, &identity),
                (true, true) => format!(
                    "array_has_any({}, {})",
                    qualified(other, PATH),
                    qualified(alias, PATH)
                ),
                (true, false) => {
                    format!(
                        "array_has({}, {})",
                        qualified(alias, PATH),
                        edge_record(&identity, other)
                    )
                }
                (false, true) => {
                    format!(
                        "array_has({}, {})",
                        qualified(other, PATH),
                        edge_record(&identity, alias)
                    )
                }
            };
            self.predicates.push(format!("NOT ({same})"));
        }
        used.push((alias.into(), ranged));
    }

    fn path_uniqueness(&mut self, edges: &str) {
        for other in &self.mixed_paths {
            self.predicates
                .push(format!("NOT array_has_any({edges}, {other})"));
        }
        self.mixed_paths.push(edges.to_owned());
    }

    fn bind_length(&mut self, variable: &str, length: String) -> Result<()> {
        ensure!(
            !self.bindings.contains_key(variable) && !self.lengths.contains_key(variable),
            "repeated path or relationship binding {variable}"
        );
        self.lengths.insert(variable.into(), length);
        Ok(())
    }

    fn bind_list(
        &mut self,
        variable: &str,
        list: String,
        length: String,
        label: &str,
    ) -> Result<()> {
        self.bind_length(variable, length)?;
        self.lists.insert(variable.into(), list);
        self.list_types
            .insert(variable.into(), entities::IdentityList::single(label));
        Ok(())
    }

    fn entity_type(&self, expr: &Expr, aliases: &Aliases) -> Option<Binding> {
        match &expr.kind {
            ExprKind::Variable(name) => match aliases.get(&name.0) {
                Some((_, clauses::Value::Entity(binding))) => Some(binding.clone()),
                Some(_) => None,
                None => self.bindings.get(&name.0).cloned(),
            },
            ExprKind::Index { base, .. } => self.entity_list(base, aliases),
            ExprKind::Case { whens, default, .. } => self.common_entity(
                whens
                    .iter()
                    .map(|(_, value)| value)
                    .chain(default.as_deref()),
                aliases,
            ),
            ExprKind::FunctionCall {
                name,
                distinct: false,
                args,
            } if name.0.eq_ignore_ascii_case("coalesce") => {
                self.common_entity(args.iter(), aliases)
            }
            _ => None,
        }
    }

    fn entity_list(&self, expr: &Expr, aliases: &Aliases) -> Option<Binding> {
        if let Some(shape) = entities::identity_list(self, expr, aliases)
            && !shape.labels.is_empty()
        {
            return Some(entities::merged(&shape.bindings(self.catalog?)));
        }
        match &expr.kind {
            ExprKind::Variable(name) => match aliases.get(&name.0) {
                Some((_, clauses::Value::EntityList(binding))) => Some(binding.clone()),
                Some(_) => None,
                None => self.entity_lists.get(&name.0).cloned(),
            },
            ExprKind::Index { base, .. } => match clauses::Value::nested_item(self, base, aliases)?
            {
                clauses::Value::EntityList(binding) => Some(binding),
                _ => None,
            },
            ExprKind::FunctionCall { name, args, .. } if name.0.eq_ignore_ascii_case("collect") => {
                let [item] = args.as_slice() else { return None };
                self.entity_type(item, aliases)
            }
            ExprKind::List(items) => self.common_entity(items.iter(), aliases),
            ExprKind::PatternComprehension { .. } => {
                match clauses::pattern_list(self.catalog?, self, aliases, expr, false)
                    .ok()?
                    .1
                {
                    clauses::Value::Entity(binding) => Some(binding),
                    _ => None,
                }
            }
            ExprKind::Slice { base, .. } => self.entity_list(base, aliases),
            ExprKind::Case { whens, default, .. } => Self::merge_entities(
                whens
                    .iter()
                    .map(|(_, value)| value)
                    .chain(default.as_deref())
                    .filter(|value| !clauses::empty_list(value))
                    .map(|value| self.entity_list(value, aliases)),
            ),
            ExprKind::FunctionCall {
                name,
                args,
                distinct: false,
            } if name.0.eq_ignore_ascii_case("coalesce") => Self::merge_entities(
                args.iter()
                    .filter(|value| !clauses::empty_list(value))
                    .map(|value| self.entity_list(value, aliases)),
            ),
            ExprKind::ListComprehension {
                variable,
                list,
                projection,
                ..
            } => {
                let Some(projection) = projection else {
                    return self.entity_list(list, aliases);
                };
                let mut scope = aliases.clone();
                scope.insert(
                    variable.0.clone(),
                    (String::new(), self.list_item(list, aliases)),
                );
                self.entity_type(projection, &scope)
            }
            _ => None,
        }
    }

    fn common_entity<'e>(
        &self,
        items: impl Iterator<Item = &'e Expr>,
        aliases: &Aliases,
    ) -> Option<Binding> {
        Self::merge_entities(
            items
                .filter(|item| !matches!(item.kind, ExprKind::Null))
                .map(|item| self.entity_type(item, aliases)),
        )
    }

    fn merge_entities(items: impl Iterator<Item = Option<Binding>>) -> Option<Binding> {
        let bindings = items.collect::<Option<Vec<_>>>()?;
        let first = bindings.first()?;
        bindings
            .iter()
            .all(|binding| binding.node == first.node)
            .then(|| entities::merged(&bindings))
    }

    fn list_item(&self, expr: &Expr, aliases: &Aliases) -> clauses::Value {
        clauses::Value::nested_item(self, expr, aliases)
            .or_else(|| self.entity_list(expr, aliases).map(clauses::Value::Entity))
            .unwrap_or(clauses::Value::Scalar)
    }

    fn shape_value(
        &self,
        expr: &Expr,
        aliases: &Aliases,
        target: &clauses::Value,
        value: String,
    ) -> String {
        if let Some(catalog) = self.catalog {
            target.shape(
                catalog,
                &clauses::Value::inferred(self, expr, aliases),
                &value,
                0,
            )
        } else {
            value
        }
    }

    fn expr(&self, expression: &Expr) -> Result<String> {
        self.expr_with_aliases(expression, &BTreeMap::new())
    }

    fn expr_with_aliases(&self, expression: &Expr, aliases: &Aliases) -> Result<String> {
        bounded_expression(self.render_expression(expression, aliases)?)
    }

    fn render_expression(&self, expression: &Expr, aliases: &Aliases) -> Result<String> {
        let render = |expr: &Expr| self.expr_with_aliases(expr, aliases);
        match &expression.kind {
            ExprKind::Property { base, key } => {
                if let Some(mut binding) = self.entity_type(base, aliases) {
                    if !matches!(&base.kind, ExprKind::Variable(name) if !aliases.contains_key(&name.0))
                    {
                        binding.stored = Some(render(base)?);
                    }
                    ensure!(
                        binding.properties.contains(&key.0),
                        "unknown property {}",
                        key.0
                    );
                    return Ok(binding.field(&key.0));
                }
                Ok(format!(
                    "__graphlake_cypher_map_get({}, {})",
                    render(base)?,
                    string(&key.0)
                ))
            }
            ExprKind::Variable(variable) => match aliases.get(&variable.0) {
                Some((expression, _)) => Ok(expression.clone()),
                None => self.variable(&variable.0),
            },
            ExprKind::List(items) => {
                let target = self.list_item(expression, aliases);
                let items = items
                    .iter()
                    .map(|item| Ok(self.shape_value(item, aliases, &target, render(item)?)))
                    .collect::<Result<Vec<_>>>()?;
                if items.is_empty() {
                    Ok("make_array()".into())
                } else {
                    Ok(format!("__graphlake_cypher_list({})", items.join(", ")))
                }
            }
            ExprKind::Case {
                operand,
                whens,
                default,
            } => {
                let mut sql = "CASE".to_owned();
                let operand = operand.as_deref().map(render).transpose()?;
                let count = whens.len() + usize::from(default.is_some());
                let target = clauses::Value::inferred(self, expression, aliases);
                let render_value = |value: &Expr| -> Result<String> {
                    Ok(self.shape_value(value, aliases, &target, render(value)?))
                };
                let mut values = Vec::with_capacity(count);
                for (index, (test, value)) in whens.iter().enumerate() {
                    let test = render(test)?;
                    let test = if let Some(operand) = &operand {
                        cypher_compare(operand, "=", &test)
                    } else {
                        test
                    };
                    values.push(render_value(value)?);
                    sql.push_str(&format!(
                        " WHEN {test} THEN {}",
                        variant_arm(count, index, &values[index])
                    ));
                }
                if let Some(default) = default {
                    values.push(render_value(default)?);
                    sql.push_str(&format!(
                        " ELSE {}",
                        variant_arm(count, whens.len(), &values[whens.len()])
                    ));
                }
                Ok(typed_case(&sql, &values))
            }
            ExprKind::HasLabels { base, labels } => {
                let binding = self
                    .entity_type(base, aliases)
                    .context("label predicate requires a node")?;
                ensure!(binding.node, "label predicate requires a node");
                let value = render(base)?;
                let (label, _) = entities::identity(&value);
                Ok(format!(
                    "CASE WHEN {value} IS NOT NULL THEN {} END",
                    label_test(&label, labels)?
                ))
            }
            ExprKind::Exists(body) => clauses::exists(
                self.catalog
                    .context("EXISTS requires a graph planning context")?,
                self,
                aliases,
                body,
                expression.span,
            ),
            ExprKind::PatternComprehension { .. } => Ok(clauses::pattern_list(
                self.catalog
                    .context("pattern comprehension requires a graph planning context")?,
                self,
                aliases,
                expression,
                true,
            )?
            .0),
            ExprKind::Quantifier {
                kind: kind @ (QuantifierKind::Any | QuantifierKind::All | QuantifierKind::None),
                variable,
                list,
                predicate,
            } => {
                if matches!(list.kind, ExprKind::Null) {
                    return Ok("NULL".into());
                }
                let item_kind = self.list_item(list, aliases);
                let list = self.entity_list_input(list, aliases)?;
                let item = format!("__graphlake_item_{}", expression.span.start);
                let mut scope = aliases.clone();
                scope.insert(variable.0.clone(), (ident(&item), item_kind));
                let test = if let Some(predicate) = predicate {
                    self.lambda_expr(predicate, &scope, &item, &list)?
                } else {
                    "TRUE".into()
                };
                let expected = if *kind == QuantifierKind::All {
                    "FALSE"
                } else {
                    "TRUE"
                };
                let negate = if *kind == QuantifierKind::Any {
                    ""
                } else {
                    "NOT "
                };
                Ok(format!(
                    "({negate}__graphlake_cypher_in({expected}, array_transform({list}, {item} -> ({test}) AND TRUE)))"
                ))
            }
            ExprKind::MapProjection { base, items } => {
                let value = render(base)?;
                if matches!(base.kind, ExprKind::Null) {
                    return Ok(value);
                }
                let binding = self.entity_type(base, aliases).map(|mut binding| {
                    binding.stored = Some(value.clone());
                    binding
                });
                let base = binding
                    .as_ref()
                    .map_or_else(|| value.clone(), Binding::property_map);
                let mut fields = BTreeMap::new();
                let mut all = false;
                for item in items {
                    match item {
                        MapProjectionItem::All => all = true,
                        MapProjectionItem::Property(key) => {
                            let property = if let Some(binding) = &binding {
                                if binding.properties.contains(&key.0) {
                                    binding.field(&key.0)
                                } else {
                                    "NULL".into()
                                }
                            } else {
                                format!("__graphlake_cypher_map_get({base}, {})", string(&key.0))
                            };
                            fields.insert(key.0.clone(), property);
                        }
                        MapProjectionItem::Literal(key, expression) => {
                            fields.insert(key.0.clone(), render(expression)?);
                        }
                        MapProjectionItem::Variable(variable) => {
                            let expr =
                                Expr::new(ExprKind::Variable(variable.clone()), expression.span);
                            fields.insert(variable.0.clone(), render(&expr)?);
                        }
                        _ => bail!("unsupported Cypher map projection item"),
                    }
                }
                let empty = "array_slice(make_array(CAST(NULL AS VARCHAR)), 1, 0)";
                let fields = if fields.is_empty() {
                    format!("map({empty}, {empty})")
                } else {
                    let fields = fields
                        .into_iter()
                        .flat_map(|(key, value)| [string(&key), value])
                        .collect::<Vec<_>>()
                        .join(", ");
                    format!("named_struct({fields})")
                };
                let map = if all {
                    format!("__graphlake_cypher_map_merge({base}, {fields})")
                } else {
                    fields
                };
                Ok(format!("CASE WHEN {value} IS NOT NULL THEN {map} END"))
            }
            ExprKind::Index { base, index } => {
                if matches!(base.kind, ExprKind::Null) {
                    return Ok("NULL".into());
                }
                let list = self.entity_list_input(base, aliases)?;
                let index = format!("__graphlake_cypher_index({})", render(index)?);
                let len = format!("CAST(array_length({list}) AS BIGINT)");
                Ok(format!(
                    "CASE WHEN {index} >= -{len} AND {index} < {len}
                     THEN array_element({list}, CASE WHEN {index} < 0 THEN {index} ELSE {index} + 1 END) END"
                ))
            }
            ExprKind::ListComprehension {
                variable,
                list,
                predicate,
                projection,
            } => {
                if matches!(list.kind, ExprKind::Null) {
                    return Ok("NULL".into());
                }
                let kind = self.list_item(list, aliases);
                let mut list = self.entity_list_input(list, aliases)?;
                let item = format!("__graphlake_item_{}", expression.span.start);
                let mut scope = aliases.clone();
                scope.insert(variable.0.clone(), (ident(&item), kind));
                if let Some(predicate) = predicate {
                    let test = self.lambda_expr(predicate, &scope, &item, &list)?;
                    list = format!("array_filter({list}, {item} -> coalesce({test}, FALSE))");
                }
                if let Some(projection) = projection {
                    let value = self.lambda_expr(projection, &scope, &item, &list)?;
                    list = format!("array_transform({list}, {item} -> {value})");
                }
                Ok(list)
            }
            ExprKind::Slice { base, from, to } => {
                if matches!(base.kind, ExprKind::Null) {
                    return Ok("NULL".into());
                }
                self.slice_list(&render(base)?, from.as_deref(), to.as_deref(), aliases)
            }
            ExprKind::Map(items) => {
                if items.is_empty() {
                    let empty = "array_slice(make_array(CAST(NULL AS VARCHAR)), 1, 0)";
                    return Ok(format!("map({empty}, {empty})"));
                }
                let fields = items
                    .iter()
                    .map(|(name, value)| Ok(format!("{}, {}", string(&name.0), render(value)?)))
                    .collect::<Result<Vec<_>>>()?;
                Ok(format!("named_struct({})", fields.join(", ")))
            }
            ExprKind::Comparison { first, rest } => {
                let mut left = self.comparison_value(first, aliases)?;
                let mut terms = Vec::new();
                for (op, operand) in rest {
                    let right = self.comparison_value(operand, aliases)?;
                    terms.push(cypher_compare(&left, op.as_str(), &right));
                    left = right;
                }
                Ok(format!("({})", terms.join(" AND ")))
            }
            ExprKind::Binary { op, lhs, rhs } => {
                let left = render(lhs)?;
                match op {
                    BinaryOp::And | BinaryOp::Or => {
                        Ok(format!("({left} {} {})", op.as_str(), render(rhs)?))
                    }
                    BinaryOp::Add
                    | BinaryOp::Sub
                    | BinaryOp::Mul
                    | BinaryOp::Div
                    | BinaryOp::Mod => {
                        let function = match op {
                            BinaryOp::Add => "add",
                            BinaryOp::Sub => "subtract",
                            BinaryOp::Mul => "multiply",
                            BinaryOp::Div => "divide",
                            _ => "remainder",
                        };
                        Ok(format!(
                            "__graphlake_cypher_{function}({left}, {})",
                            render(rhs)?
                        ))
                    }
                    BinaryOp::StartsWith | BinaryOp::EndsWith | BinaryOp::Contains => {
                        let function = match op {
                            BinaryOp::StartsWith => "starts_with",
                            BinaryOp::EndsWith => "ends_with",
                            _ => "contains",
                        };
                        Ok(format!("{function}({left}, {})", render(rhs)?))
                    }
                    BinaryOp::In => {
                        let left = self.comparison_value(lhs, aliases)?;
                        if let ExprKind::List(items) = &rhs.kind {
                            if items.is_empty() {
                                return Ok("FALSE".into());
                            }
                            let items = items
                                .iter()
                                .map(|item| {
                                    Ok(format!(
                                        "__graphlake_cypher_equals({left}, {})",
                                        self.comparison_value(item, aliases)?
                                    ))
                                })
                                .collect::<Result<Vec<_>>>()?;
                            return Ok(format!("({})", items.join(" OR ")));
                        }
                        Ok(format!(
                            "__graphlake_cypher_in({left}, {})",
                            self.comparison_value(rhs, aliases)?
                        ))
                    }
                    _ => bail!("unsupported Cypher operator {}", op.as_str()),
                }
            }
            ExprKind::Unary { op, operand } => {
                Ok(format!("({} {})", op.as_str(), render(operand)?))
            }
            ExprKind::IsNull(expr) => Ok(format!("({} IS NULL)", render(expr)?)),
            ExprKind::IsNotNull(expr) => Ok(format!("({} IS NOT NULL)", render(expr)?)),
            ExprKind::CountStar => Ok("COUNT(*)".into()),
            ExprKind::FunctionCall {
                name,
                distinct,
                args,
            } => {
                let function = name.0.to_ascii_lowercase();
                if function == paths::COUNT_SUBQUERY && !distinct {
                    let [
                        Expr {
                            kind: ExprKind::Exists(body),
                            ..
                        },
                    ] = args.as_slice()
                    else {
                        bail!("COUNT requires a braced subquery");
                    };
                    return clauses::row_subquery(
                        self.catalog
                            .context("COUNT requires a graph planning context")?,
                        self,
                        aliases,
                        body,
                        expression.span,
                        clauses::RowExpression::Count,
                    );
                }
                if matches!(function.as_str(), "date" | "datetime") && !distinct {
                    return Ok(match (function.as_str(), args.as_slice()) {
                        ("date", []) => "current_date()".into(),
                        ("datetime", []) => "now()".into(),
                        ("date", [arg]) => format!("CAST({} AS DATE)", render(arg)?),
                        ("datetime", [arg]) => {
                            format!("CAST({} AS TIMESTAMP WITH TIME ZONE)", render(arg)?)
                        }
                        _ => bail!("{function}() expects zero or one argument"),
                    });
                }
                if function == "reduce" && !distinct {
                    let [acc, initial, item, list, body] = args.as_slice() else {
                        bail!("reduce() expects accumulator = initial, item IN list | expression");
                    };
                    let (ExprKind::Variable(acc), ExprKind::Variable(item)) =
                        (&acc.kind, &item.kind)
                    else {
                        bail!("reduce() expects accumulator and item variable names");
                    };
                    ensure!(
                        acc != item,
                        "reduce() requires distinct accumulator and item names"
                    );
                    if matches!(list.kind, ExprKind::Null) {
                        return Ok("NULL".into());
                    }
                    let item_kind = self.list_item(list, aliases);
                    let initial_kind = clauses::Value::inferred(self, initial, aliases);
                    let initial = render(initial)?;
                    if clauses::empty_list(list) {
                        return Ok(initial);
                    }
                    let list = self.entity_list_input(list, aliases)?;
                    let acc_param = format!("__graphlake_acc_{}", expression.span.start);
                    let item_param = format!("__graphlake_item_{}", expression.span.start);
                    let mut scope = aliases.clone();
                    scope.insert(acc.0.clone(), (ident(&acc_param), initial_kind));
                    scope.insert(item.0.clone(), (ident(&item_param), item_kind));
                    let mut exists = clauses::HasExists(false);
                    exists.visit_expr(body);
                    ensure!(
                        !exists.0,
                        "reduce() does not support EXISTS inside the accumulator expression"
                    );
                    let body = self.lambda_expr(body, &scope, &item_param, &list)?;
                    return Ok(format!(
                        "__graphlake_cypher_reduce({list}, {initial}, ({acc_param}, {item_param}) -> {body})"
                    ));
                }
                if function == "range" && !distinct {
                    ensure!(
                        (2..=3).contains(&args.len()),
                        "range() expects start, end and optional step"
                    );
                    let args = args
                        .iter()
                        .map(|arg| Ok(format!("__graphlake_cypher_index({})", render(arg)?)))
                        .collect::<Result<Vec<_>>>()?;
                    return Ok(format!("__graphlake_cypher_range({})", args.join(", ")));
                }
                if function == "labels"
                    && !distinct
                    && let [arg] = args.as_slice()
                {
                    if matches!(arg.kind, ExprKind::Null) {
                        return Ok("NULL".into());
                    }
                    ensure!(
                        self.entity_type(arg, aliases)
                            .is_some_and(|binding| binding.node),
                        "labels() expects a node"
                    );
                    let value = render(arg)?;
                    let (label, _) = entities::identity(&value);
                    return Ok(format!(
                        "CASE WHEN {value} IS NOT NULL THEN make_array({label}) END"
                    ));
                }
                if function == "keys"
                    && !distinct
                    && let [arg] = args.as_slice()
                {
                    let value = render(arg)?;
                    if let Some(mut binding) = self.entity_type(arg, aliases) {
                        binding.stored = Some(value);
                        return Ok(format!(
                            "__graphlake_cypher_keys({}, TRUE)",
                            binding.property_map()
                        ));
                    }
                    return Ok(format!("__graphlake_cypher_keys({value}, FALSE)"));
                }
                if function == "properties"
                    && !distinct
                    && let [arg] = args.as_slice()
                {
                    if let Some(mut binding) = self.entity_type(arg, aliases) {
                        binding.stored = Some(render(arg)?);
                        return Ok(binding.property_map());
                    }
                    return Ok(format!("__graphlake_cypher_properties({})", render(arg)?));
                }
                if matches!(function.as_str(), "id" | "elementid")
                    && !distinct
                    && let [arg] = args.as_slice()
                {
                    if matches!(arg.kind, ExprKind::Null) {
                        return Ok("NULL".into());
                    }
                    self.entity_type(arg, aliases)
                        .with_context(|| format!("{function} requires a graph element"))?;
                    let value = render(arg)?;
                    let (label, key) = entities::identity(&value);
                    return Ok(format!(
                        "CASE WHEN {value} IS NOT NULL THEN {} END",
                        element_id(&label, &key)
                    ));
                }
                if function == "type"
                    && !distinct
                    && let [arg] = args.as_slice()
                    && self
                        .entity_type(arg, aliases)
                        .is_some_and(|binding| !binding.node)
                {
                    return Ok(entities::identity(&render(arg)?).0);
                }
                if matches!(function.as_str(), "nodes" | "relationships")
                    && let [arg] = args.as_slice()
                {
                    let values = if function == "nodes" {
                        &self.path_nodes
                    } else {
                        &self.paths
                    };
                    if let ExprKind::Variable(variable) = &arg.kind
                        && !aliases.contains_key(&variable.0)
                        && let Some(value) = values.get(&variable.0)
                    {
                        return Ok(value.clone());
                    }
                    ensure!(
                        matches!(
                            clauses::Value::inferred(self, arg, aliases),
                            clauses::Value::Path(_)
                        ),
                        "{function} requires a path"
                    );
                    return Ok(format!("get_field({}, '{function}')", render(arg)?));
                }
                if matches!(function.as_str(), "size" | "length")
                    && !distinct
                    && let [arg] = args.as_slice()
                    && let ExprKind::Variable(variable) = &arg.kind
                    && !aliases.contains_key(&variable.0)
                    && let Some(length) = self.lengths.get(&variable.0)
                    && (function == "length" || self.lists.contains_key(&variable.0))
                {
                    return Ok(format!("({length})"));
                }
                if function == "length"
                    && !distinct
                    && let [arg] = args.as_slice()
                    && matches!(
                        clauses::Value::inferred(self, arg, aliases),
                        clauses::Value::Path(_)
                    )
                {
                    return Ok(format!("get_field({}, 'length')", render(arg)?));
                }
                if function == "size"
                    && !distinct
                    && let [arg] = args.as_slice()
                {
                    if let Some(count) = collect::size_argument(arg) {
                        return render(&count);
                    }
                    return Ok(format!("array_length({})", render(arg)?));
                }
                if function == collect::COUNT_SIZE
                    && let [arg] = args.as_slice()
                {
                    let arg = render(arg)?;
                    let count = if *distinct {
                        format!(
                            "count(DISTINCT CASE WHEN {arg} IS NOT NULL THEN __graphlake_cypher_equivalence({arg}) END)"
                        )
                    } else {
                        format!("count({arg})")
                    };
                    return Ok(format!("arrow_cast({count}, 'UInt64')"));
                }
                if function == "coalesce" && !distinct && !args.is_empty() {
                    let mut sql = "CASE".to_owned();
                    let target = clauses::Value::inferred(self, expression, aliases);
                    let mut values = Vec::with_capacity(args.len());
                    for (index, arg) in args.iter().enumerate() {
                        let value = self.shape_value(arg, aliases, &target, render(arg)?);
                        let arm = variant_arm(args.len(), index, &value);
                        if index + 1 == args.len() {
                            if index == 0 {
                                return Ok(value);
                            }
                            sql.push_str(&format!(" ELSE {arm}"));
                        } else {
                            sql.push_str(&format!(" WHEN {value} IS NOT NULL THEN {arm}"));
                        }
                        values.push(value);
                    }
                    return Ok(typed_case(&sql, &values));
                }
                ensure!(
                    matches!(
                        function.as_str(),
                        "count" | "sum" | "avg" | "min" | "max" | "collect"
                    ),
                    "unsupported Cypher function {}",
                    name.0
                );
                let [arg] = args.as_slice() else {
                    bail!("{function} requires one argument");
                };
                let arg = if function == "count"
                    && !distinct
                    && let ExprKind::Variable(variable) = &arg.kind
                    && let Some(binding) = self.bindings.get(&variable.0)
                {
                    if binding.dynamic {
                        entities::identity(&binding.value()).0
                    } else {
                        binding.field(&binding.keys[0])
                    }
                } else {
                    render(arg)?
                };
                if function == "collect" {
                    let order = if self.input_order.is_empty() {
                        String::new()
                    } else {
                        let terms = self
                            .input_order
                            .iter()
                            .map(|key| key.term(Some("l")))
                            .collect::<Vec<_>>();
                        format!(" ORDER BY {}", terms.join(", "))
                    };
                    let name = if *distinct {
                        "__graphlake_collect_distinct"
                    } else {
                        "__graphlake_collect"
                    };
                    return Ok(format!(
                        "coalesce({name}({arg}{order}) FILTER (WHERE {arg} IS NOT NULL), make_array())"
                    ));
                }
                if matches!(function.as_str(), "min" | "max") {
                    return Ok(extremum(
                        &arg,
                        &format!("__graphlake_cypher_order({arg})"),
                        function == "max",
                    ));
                }
                if *distinct && matches!(function.as_str(), "sum" | "avg") {
                    let array =
                        format!("array_agg(DISTINCT {arg}) FILTER (WHERE {arg} IS NOT NULL)");
                    let aggregate = format!("__graphlake_cypher_{function}_distinct({array})");
                    return Ok(if function == "sum" {
                        format!("coalesce({aggregate}, 0)")
                    } else {
                        aggregate
                    });
                }
                let arg = if matches!(function.as_str(), "sum" | "avg") {
                    format!("__graphlake_numeric_input({arg})")
                } else {
                    arg
                };
                let arg = if function == "count" && *distinct {
                    format!(
                        "CASE WHEN {arg} IS NOT NULL THEN __graphlake_cypher_equivalence({arg}) END"
                    )
                } else {
                    arg
                };
                let aggregate_function = if function == "sum" {
                    "__graphlake_cypher_sum"
                } else {
                    &function
                };
                let aggregate = format!(
                    "{aggregate_function}({}{arg})",
                    if *distinct { "DISTINCT " } else { "" }
                );
                if function == "sum" {
                    Ok(format!("coalesce({aggregate}, 0)"))
                } else {
                    Ok(aggregate)
                }
            }
            _ => literal(expression),
        }
    }

    fn from(&self) -> String {
        if self.relations.is_empty() {
            String::new()
        } else {
            let mut relations = self.relations.iter().collect::<Vec<_>>();
            if !self.relation_order.is_empty() {
                relations.sort_by_key(|(_, order)| *order);
            }
            let terms = relations
                .into_iter()
                .map(|(sql, _)| sql.as_str())
                .collect::<Vec<_>>();
            format!(" FROM {}", terms.join(" CROSS JOIN "))
        }
    }

    fn slice_list(
        &self,
        list: &str,
        from: Option<&Expr>,
        to: Option<&Expr>,
        aliases: &Aliases,
    ) -> Result<String> {
        let len = format!("CAST(array_length({list}) AS BIGINT)");
        let bound = |expr: &Expr| -> Result<(String, String)> {
            let value = self.expr_with_aliases(expr, aliases)?;
            let index = format!("CAST({value} AS BIGINT)");
            let clamped = format!(
                "CASE WHEN {index} < 0 THEN greatest(0, {len} + {index}) ELSE least({len}, {index}) END"
            );
            Ok((clamped, format!("{value} IS NULL")))
        };
        let mut nulls = vec![format!("{list} IS NULL")];
        let start = if let Some(from) = from {
            let (start, null) = bound(from)?;
            nulls.push(null);
            start
        } else {
            "0".into()
        };
        let end = if let Some(to) = to {
            let (end, null) = bound(to)?;
            nulls.push(null);
            end
        } else {
            len
        };
        bounded_expression(format!(
            "CASE WHEN {} THEN NULL ELSE array_slice({list}, ({start}) + 1, {end}) END",
            nulls.join(" OR ")
        ))
    }

    fn variable(&self, variable: &str) -> Result<String> {
        if let Some(binding) = self.bindings.get(variable) {
            return Ok(binding.value());
        }
        if let Some(edges) = self.paths.get(variable) {
            let length = &self.lengths[variable];
            let nodes = &self.path_nodes[variable];
            return Ok(format!(
                "CASE WHEN {length} IS NULL THEN NULL ELSE named_struct('length', {length}, 'nodes', {nodes}, 'relationships', {edges}) END"
            ));
        }
        self.scalars
            .get(variable)
            .or_else(|| self.lists.get(variable))
            .cloned()
            .with_context(|| format!("unknown variable {variable}"))
    }

    fn project(
        &self,
        body: &ReturnBody,
        distinct: bool,
        from: String,
        predicates: &[String],
        keep_order: bool,
        public: bool,
    ) -> Result<(String, clauses::Scope, Vec<OrderKey>)> {
        let mut scope = clauses::Scope::default();
        let mut grouping = Vec::new();
        let mut projection = Vec::new();
        let mut aggregate_aliases = BTreeSet::new();
        let mut output_names = BTreeSet::new();
        let mut aliases = BTreeMap::new();
        let sort_requested = !body.order_by.is_empty();
        if body.star {
            let visible = clauses::Scope::from_plan(self);
            for name in visible.columns.keys() {
                let expr = self.variable(name)?;
                output_names.insert(name.clone());
                projection.push((name.clone(), expr.clone()));
                if sort_requested {
                    aliases.insert(name.clone(), (expr, visible.columns[name].clone()));
                }
            }
            scope = visible;
        }
        for item in &body.items {
            let name = item
                .alias
                .as_ref()
                .map(|v| v.0.clone())
                .unwrap_or_else(|| expression_name(&item.expr));
            ensure!(
                output_names.insert(name.clone()),
                "duplicate RETURN column {name}"
            );
            let expr = self.expr(&item.expr)?;
            scope
                .columns
                .insert(name.clone(), clauses::Value::projected(self, &item.expr));
            let mut aggregate = HasAggregate(false);
            aggregate.visit_expr(&item.expr);
            if aggregate.0 {
                aggregate_aliases.insert(name.clone());
            }
            projection.push((name.clone(), expr.clone()));
            if sort_requested {
                aliases.insert(name.clone(), (expr, scope.columns[&name].clone()));
            }
        }
        let aggregated = !aggregate_aliases.is_empty();
        let visible = projection
            .iter()
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>();
        let mut projection = projection
            .into_iter()
            .map(|(name, expression)| {
                let value = if aggregated && self.partition_column.as_ref() == Some(&name) {
                    grouping.push(expression.clone());
                    expression
                } else if aggregated && !aggregate_aliases.contains(&name) {
                    grouping.push(format!("__graphlake_cypher_equivalence({expression})"));
                    format!("first_value({expression})")
                } else {
                    expression
                };
                format!("{value} AS {}", ident(&name))
            })
            .collect::<Vec<_>>();
        ensure!(
            !projection.is_empty(),
            "projection needs at least one expression"
        );
        // A nested SQL sort can be removed. Project the sort keys so the next
        // Cypher clause can express its ordering where SQL consumes it.
        let mut order = Vec::new();
        let mut hidden = Vec::new();
        let mut projected_order = None;
        if sort_requested {
            let mut projected = MatchPlan {
                catalog: self.catalog,
                call_imports: self.call_imports.clone(),
                indexed_csr: self.indexed_csr,
                partitioned_csr: self.partitioned_csr,
                ..Default::default()
            };
            scope.bind("d", &mut projected);
            for (index, item) in body.order_by.iter().enumerate() {
                let column = order_column(index, &mut output_names);
                let mut aggregate = HasAggregate(false);
                aggregate.visit_expr(&item.expr);
                let spelling = expression_name(&item.expr);
                let projected_expression = if !aggregate.0 {
                    projected.expr(&item.expr).ok()
                } else {
                    None
                };
                let projected_expression = projected_expression.or_else(|| {
                    body.items
                        .iter()
                        .find(|item| expression_name(&item.expr) == spelling)
                        .map(|item| {
                            qualified(
                                "d",
                                item.alias
                                    .as_ref()
                                    .map_or(spelling.as_str(), |name| name.0.as_str()),
                            )
                        })
                });
                if let Some(expression) = projected_expression {
                    hidden.push(format!("{expression} AS {}", ident(&column)));
                } else {
                    ensure!(
                        !distinct,
                        "ORDER BY after DISTINCT requires projected values"
                    );
                    let value = self.expr_with_aliases(&item.expr, &aliases)?;
                    let expression = if aggregated && !aggregate.0 {
                        format!("first_value({value})")
                    } else {
                        value
                    };
                    projection.push(format!("{expression} AS {}", ident(&column)));
                }
                order.push(OrderKey {
                    column,
                    descending: item.descending,
                    native: false,
                });
            }
            self.expression_labels
                .borrow_mut()
                .append(&mut projected.expression_labels.borrow_mut());
            self.expression_expansions
                .borrow_mut()
                .append(&mut projected.expression_expansions.borrow_mut());
            projected_order = Some(projected);
        } else if !aggregated && !distinct {
            for (index, key) in self.input_order.iter().enumerate() {
                let column = order_column(index, &mut output_names);
                projection.push(format!(
                    "{} AS {}",
                    qualified("l", &key.column),
                    ident(&column)
                ));
                order.push(OrderKey {
                    column,
                    descending: key.descending,
                    native: key.native,
                });
            }
        }
        let from = self.join_expressions(&from);
        let mut sql = format!("SELECT {}{from}", projection.join(", "));
        if !predicates.is_empty() {
            sql.push_str(&format!(" WHERE {}", predicates.join(" AND ")));
        }
        if aggregated && !grouping.is_empty() {
            sql.push_str(&format!(" GROUP BY {}", grouping.join(", ")));
        }
        if distinct {
            let keys = visible
                .iter()
                .map(|name| {
                    let value = qualified("q", name);
                    if self.partition_column.as_ref() == Some(name) {
                        value
                    } else {
                        format!("__graphlake_cypher_equivalence({value})")
                    }
                })
                .collect::<Vec<_>>();
            sql = format!(
                "SELECT DISTINCT ON ({}) q.* FROM ({sql}) AS q",
                keys.join(", ")
            );
        }
        if !hidden.is_empty() {
            let projected = projected_order.expect("projected ordering");
            let projected_from = projected.join_expressions(&format!(" FROM ({sql}) AS d"));
            self.expression_calls
                .borrow_mut()
                .append(&mut projected.expression_calls.borrow_mut());
            sql = format!("SELECT d.*, {}{projected_from}", hidden.join(", "));
        }
        if public || !keep_order && !order.is_empty() {
            let fields = if public {
                public_projection(visible.iter().map(String::as_str))
            } else {
                visible
                    .iter()
                    .map(|name| ident(name))
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            sql = format!("SELECT {fields} FROM ({sql}) AS result");
        }
        if !order.is_empty() && self.partition_column.is_none() {
            let terms = order.iter().map(|key| key.term(None)).collect::<Vec<_>>();
            sql.push_str(&format!(" ORDER BY {}", terms.join(", ")));
        }
        for (clause, expression) in [("LIMIT", &body.limit), ("OFFSET", &body.skip)] {
            if let Some(expr) = expression {
                let ExprKind::Integer(value) = expr.kind else {
                    bail!("{clause} requires an integer literal");
                };
                ensure!(value >= 0, "{clause} requires a nonnegative integer");
                sql.push_str(&format!(" {clause} {value}"));
            }
        }
        Ok((sql, scope, order))
    }
}

fn order_column(index: usize, used: &mut BTreeSet<String>) -> String {
    let mut column = format!("__graphlake_cypher_order_{index}");
    while !used.insert(column.clone()) {
        column.push('_');
    }
    column
}

fn public_projection<'a>(names: impl Iterator<Item = &'a str>) -> String {
    names
        .map(|name| {
            let field = ident(name);
            format!("__graphlake_cypher_public({field}) AS {field}")
        })
        .collect::<Vec<_>>()
        .join(", ")
}

impl Binding {
    fn field(&self, field: &str) -> String {
        match &self.stored {
            Some(value) if self.dynamic => {
                let slots = self
                    .lanes
                    .iter()
                    .filter(|(_, properties)| properties.iter().any(|name| name == field))
                    .map(|(label, _)| {
                        format!(
                            "get_field(get_field(get_field({value}, 'lanes'), {}), {})",
                            string(label),
                            string(field)
                        )
                    })
                    .collect::<Vec<_>>();
                match slots.as_slice() {
                    [] => "NULL".into(),
                    [slot] => slot.clone(),
                    _ => format!("__graphlake_cypher_property({})", slots.join(", ")),
                }
            }
            Some(value) => entities::property(value, field),
            None => qualified(&self.alias, field),
        }
    }

    fn property_map(&self) -> String {
        if !self.dynamic
            && let Some(value) = &self.stored
        {
            return format!("get_field({value}, 'properties')");
        }
        let fields = self
            .properties
            .iter()
            .flat_map(|field| [string(field), self.field(field)])
            .collect::<Vec<_>>()
            .join(", ");
        let fields = if fields.is_empty() {
            "'_empty', NULL"
        } else {
            &fields
        };
        let map = format!("named_struct({fields})");
        if let Some(value) = &self.stored {
            format!("CASE WHEN {value} IS NOT NULL THEN {map} END")
        } else {
            map
        }
    }

    fn value(&self) -> String {
        if let Some(value) = &self.stored {
            return value.clone();
        }
        let kind = if self.node { "node" } else { "relationship" };
        let label = string(&self.label);
        let key = crate::traversal::text_key(&self.alias, &self.keys);
        let properties = self.property_map();
        format!(
            "__graphlake_cypher_entity(named_struct('kind', '{kind}', 'label', {label}, 'key', {key}, 'properties', {properties}))"
        )
    }
}

fn label_test(value: &str, labels: &LabelExpr) -> Result<String> {
    Ok(match labels {
        LabelExpr::Leaf(name) => format!("({value} = {})", string(name)),
        LabelExpr::Wildcard => "TRUE".into(),
        LabelExpr::Disjunction(left, right) => format!(
            "({} OR {})",
            label_test(value, left)?,
            label_test(value, right)?
        ),
        LabelExpr::Conjunction(left, right) => format!(
            "({} AND {})",
            label_test(value, left)?,
            label_test(value, right)?
        ),
        LabelExpr::Negation(inner) => format!("NOT ({})", label_test(value, inner)?),
        _ => bail!("unsupported label predicate"),
    })
}

pub(crate) fn literal(expr: &Expr) -> Result<String> {
    match &expr.kind {
        ExprKind::Null => Ok("NULL".into()),
        ExprKind::Boolean(value) => Ok(if *value { "TRUE" } else { "FALSE" }.into()),
        ExprKind::Integer(value) => Ok(value.to_string()),
        ExprKind::Float(value) if value.is_finite() => Ok(format!("{value:e}")),
        ExprKind::String(value) => Ok(string(value)),
        ExprKind::Unary {
            op: UnaryOp::Minus,
            operand,
        } => Ok(format!("(- {})", literal(operand)?)),
        _ => bail!("unsupported Cypher expression at {:?}", expr.span),
    }
}

struct HasAggregate(bool);

impl Visitor for HasAggregate {
    fn visit_expr(&mut self, expr: &Expr) {
        if matches!(
            expr.kind,
            ExprKind::Exists(_) | ExprKind::PatternComprehension { .. }
        ) {
            return;
        }
        if matches!(&expr.kind, ExprKind::CountStar)
            || matches!(&expr.kind, ExprKind::FunctionCall { name, .. }
            if matches!(name.0.to_ascii_lowercase().as_str(), "count" | "sum" | "avg" | "min" | "max" | "collect" | collect::COUNT_SIZE))
        {
            self.0 = true;
        } else {
            walk_expr(self, expr);
        }
    }
}

fn expression_name(expr: &Expr) -> String {
    if let ExprKind::FunctionCall { name, args, .. } = &expr.kind
        && name.0 == paths::COUNT_SUBQUERY
        && let [
            Expr {
                kind: ExprKind::Exists(body),
                ..
            },
        ] = args.as_slice()
    {
        let rendered = body.to_string();
        return format!("COUNT{}", &rendered["EXISTS".len()..]);
    }
    expr.to_string()
}

pub(crate) fn string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

pub(crate) fn element_id(label: &str, key: &str) -> String {
    format!("__graphlake_element_id({label}, {key})")
}

pub(crate) fn escape_id_component(value: &str, output: &mut String) {
    for ch in value.chars() {
        match ch {
            '%' => output.push_str("%25"),
            ',' => output.push_str("%2C"),
            '[' => output.push_str("%5B"),
            ']' => output.push_str("%5D"),
            _ => output.push(ch),
        }
    }
}

#[cfg(feature = "admin")]
pub(crate) fn format_element_id(label: &str, key: &[String]) -> String {
    let mut id = String::new();
    escape_id_component(label, &mut id);
    id.push('[');
    for (i, value) in key.iter().enumerate() {
        if i > 0 {
            id.push(',');
        }
        escape_id_component(value, &mut id);
    }
    id.push(']');
    id
}

pub(crate) fn extremum(value: &str, key: &str, maximum: bool) -> String {
    let direction = if maximum { "DESC" } else { "ASC" };
    format!("first_value({value} ORDER BY {key} {direction}) FILTER (WHERE {value} IS NOT NULL)")
}

fn typed_case(sql: &str, values: &[String]) -> String {
    // CASE simplification can remove inactive branches before a surrounding
    // lambda is planned. Keep their types in a record of typed nulls.
    let types = values
        .iter()
        .enumerate()
        .map(|(index, value)| format!("'branch_{index}', CASE WHEN FALSE THEN {value} END"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("__graphlake_cypher_case({sql} END, named_struct({types}))")
}

fn variant_arm(count: usize, chosen: usize, value: &str) -> String {
    let fields = (0..count)
        .map(|index| {
            let slot = if index == chosen { value } else { "NULL" };
            format!("'branch_{index}', {slot}")
        })
        .collect::<Vec<_>>();
    format!("named_struct({})", fields.join(", "))
}

fn cypher_compare(left: &str, operator: &str, right: &str) -> String {
    format!(
        "__graphlake_cypher_compare({left}, {right}, {})",
        string(operator)
    )
}

fn start_terms(
    expression: &Expr,
    variable: &str,
    allowed: &[String],
    terms: &mut Vec<String>,
    bound_keys: &mut BTreeSet<String>,
) {
    if let ExprKind::Binary {
        op: BinaryOp::And,
        lhs,
        rhs,
    } = &expression.kind
    {
        start_terms(lhs, variable, allowed, terms, bound_keys);
        start_terms(rhs, variable, allowed, terms, bound_keys);
        return;
    }
    let ExprKind::Comparison { first, rest } = &expression.kind else {
        return;
    };
    let [(operator, value)] = rest.as_slice() else {
        return;
    };
    if let ExprKind::Property { base, key } = &first.kind
        && matches!(&base.kind, ExprKind::Variable(var) if var.0 == variable)
        && allowed.contains(&key.0)
        && let Ok(value) = literal(value)
    {
        if operator.as_str() == "=" {
            bound_keys.insert(key.0.clone());
        }
        terms.push(cypher_compare(
            &qualified("src", &key.0),
            operator.as_str(),
            &value,
        ));
    }
}

fn property_terms(
    alias: &str,
    allowed: &[String],
    props: Option<&Properties>,
) -> Result<Vec<String>> {
    match props {
        None => Ok(Vec::new()),
        Some(Properties::Map(values)) => values
            .iter()
            .map(|(key, value)| {
                ensure!(
                    allowed.contains(&key.0),
                    "unknown inline property {}",
                    key.0
                );
                Ok(cypher_compare(
                    &qualified(alias, &key.0),
                    "=",
                    &literal(value)?,
                ))
            })
            .collect(),
        _ => bail!("inline properties require literal values"),
    }
}
