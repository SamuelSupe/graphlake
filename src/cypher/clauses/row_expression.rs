use anyhow::{Context, Result, bail, ensure};
use opencypher::{
    Clause, ExistsExpr, Expr, ExprKind, Match, PatternPart, Query, Return, ReturnBody, ReturnItem,
    SingleQuery, Span, Variable, Visitor,
};

use super::{
    ExpressionJoin, Scope, Stage, Value, batch_call, branches, call_subquery, combine_union, ident,
    join_match, qualified, unwind_stage,
};
use crate::{
    Catalog,
    cypher::{Aliases, MatchPlan, Plan, paths::Extensions},
};

#[derive(Clone, Copy)]
pub(in crate::cypher) enum RowExpression {
    Exists,
    Count,
    List,
}

pub(super) struct References<'a> {
    pub(super) names: std::collections::BTreeSet<String>,
    pub(super) call_imports: &'a std::collections::BTreeMap<usize, Vec<String>>,
}

impl Visitor for References<'_> {
    fn visit_clause(&mut self, clause: &Clause) {
        match clause {
            Clause::With(with) => self.names.extend(
                with.body
                    .items
                    .iter()
                    .filter_map(|item| item.alias.as_ref())
                    .map(|name| name.0.clone()),
            ),
            Clause::Return(ret) => self.names.extend(
                ret.body
                    .items
                    .iter()
                    .filter_map(|item| item.alias.as_ref())
                    .map(|name| name.0.clone()),
            ),
            Clause::Unwind(unwind) => {
                self.names.insert(unwind.variable.0.clone());
            }
            Clause::Call(call) => {
                if let Some(imports) = self.call_imports.get(&call.span.start) {
                    self.names.extend(imports.iter().cloned());
                }
                if let opencypher::CallKind::Subquery(query) = &call.kind {
                    for branch in std::iter::once(&query.initial)
                        .chain(query.unions.iter().map(|union| &union.query))
                    {
                        if !self.call_imports.contains_key(&call.span.start)
                            && let Some(Clause::With(with)) = branch.clauses.first()
                        {
                            for item in &with.body.items {
                                if let ExprKind::Variable(name) = &item.expr.kind {
                                    self.names.insert(name.0.clone());
                                }
                            }
                        }
                        if let Some(Clause::Return(ret)) = branch.clauses.last() {
                            self.names.extend(ret.body.items.iter().map(|item| {
                                item.alias.as_ref().map_or_else(
                                    || crate::cypher::expression_name(&item.expr),
                                    |name| name.0.clone(),
                                )
                            }));
                        }
                    }
                    return;
                }
            }
            _ => {}
        }
        opencypher::walk_clause(self, clause);
    }

    fn visit_pattern_part(&mut self, part: &PatternPart) {
        self.names
            .extend(part.variable.iter().map(|name| name.0.clone()));
        opencypher::walk_pattern_part(self, part);
    }

    fn visit_expr(&mut self, expr: &Expr) {
        if let ExprKind::Variable(name) = &expr.kind {
            self.names.insert(name.0.clone());
        }
        opencypher::walk_expr(self, expr);
    }

    fn visit_node_pattern(&mut self, node: &opencypher::NodePattern) {
        self.names
            .extend(node.variable.iter().map(|name| name.0.clone()));
        opencypher::walk_node_pattern(self, node);
    }

    fn visit_relationship_pattern(&mut self, rel: &opencypher::RelationshipPattern) {
        self.names
            .extend(rel.variable.iter().map(|name| name.0.clone()));
        opencypher::walk_relationship_pattern(self, rel);
    }
}

pub(in crate::cypher) fn row_subquery(
    catalog: &Catalog,
    outer: &MatchPlan<'_>,
    aliases: &Aliases,
    body: &ExistsExpr,
    span: Span,
    mode: RowExpression,
) -> Result<String> {
    let query = match body {
        ExistsExpr::Pattern {
            patterns,
            where_clause,
        } => Query {
            initial: SingleQuery {
                clauses: vec![Clause::Match(Match {
                    optional: false,
                    patterns: patterns.clone(),
                    where_clause: where_clause.clone(),
                    span,
                })],
                span,
            },
            unions: Vec::new(),
            span,
        },
        ExistsExpr::Subquery(query) => query.clone(),
        _ => bail!("unsupported braced subquery"),
    };
    compile(catalog, outer, aliases, query, span, mode, true).map(|(sql, _)| sql)
}

pub(in crate::cypher) fn pattern_list(
    catalog: &Catalog,
    outer: &MatchPlan<'_>,
    aliases: &Aliases,
    expression: &Expr,
    emit: bool,
) -> Result<(String, Value)> {
    let ExprKind::PatternComprehension {
        variable,
        pattern,
        predicate,
        projection,
    } = &expression.kind
    else {
        unreachable!("pattern comprehension")
    };
    let span = expression.span;
    let query = Query {
        initial: SingleQuery {
            clauses: vec![
                Clause::Match(Match {
                    optional: false,
                    patterns: vec![PatternPart {
                        variable: variable.clone(),
                        element: *pattern.clone(),
                        span,
                    }],
                    where_clause: predicate.as_deref().cloned(),
                    span,
                }),
                output(
                    projection.as_ref().clone(),
                    "__graphlake_pattern_value",
                    span,
                ),
            ],
            span,
        },
        unions: Vec::new(),
        span,
    };
    compile(
        catalog,
        outer,
        aliases,
        query,
        span,
        RowExpression::List,
        emit,
    )
}

fn output(expr: Expr, name: &str, span: Span) -> Clause {
    Clause::Return(Return {
        distinct: false,
        body: ReturnBody {
            items: vec![ReturnItem {
                expr,
                alias: Some(Variable(name.to_owned())),
                span,
            }],
            ..Default::default()
        },
        span,
    })
}

fn compile(
    catalog: &Catalog,
    outer: &MatchPlan<'_>,
    aliases: &Aliases,
    mut query: Query,
    span: Span,
    mode: RowExpression,
    emit: bool,
) -> Result<(String, Value)> {
    let mut referenced = References {
        names: Default::default(),
        call_imports: &outer.call_imports,
    };
    referenced.visit_query(&query);
    let mut imported = Scope::from_plan(outer);
    imported.columns.extend(
        aliases
            .iter()
            .map(|(name, (_, kind))| (name.clone(), kind.clone())),
    );
    imported
        .columns
        .retain(|name, _| referenced.names.contains(name));
    let captures = imported
        .columns
        .keys()
        .map(|name| {
            let value = if let Some((value, _)) = aliases.get(name) {
                value.clone()
            } else {
                outer.variable(name)?
            };
            Ok((name.clone(), value))
        })
        .collect::<Result<Vec<_>>>()?;
    let frames = outer.lambda_inputs.borrow();
    let (lambda, mut captures): (Vec<_>, Vec<_>) = captures
        .into_iter()
        .partition(|(_, value)| frames.iter().any(|(item, _)| item == value));
    let result = format!(
        "__graphlake_row_expression_{}_{}",
        span.start,
        outer.expression_joins.borrow().len()
    );
    let text = format!("{query:?}");
    let internal = |suffix: &str| {
        let mut name = format!("{result}_{suffix}");
        while text.contains(&name)
            || imported.columns.contains_key(&name)
            || catalog.nodes.contains_key(&name)
            || catalog.edges.contains_key(&name)
        {
            name.push('_');
        }
        name
    };
    let seed = internal("seed");
    let key = internal("key");
    let value = internal("value");
    let implicit = internal("row");
    let seed_table = ident(&seed);
    let mut input_table = seed_table.clone();
    let mut lambda_projection = None;
    if !lambda.is_empty() {
        let list = internal("list");
        let tuple = internal("tuple");
        let fields = lambda
            .iter()
            .enumerate()
            .flat_map(|(index, (_, value))| [format!("'item_{index}'"), value.clone()])
            .collect::<Vec<_>>();
        let current = format!("named_struct({})", fields.join(", "));
        let items = lambda.iter().map(|(_, value)| value.as_str());
        captures.push((
            list.clone(),
            super::lambda_values(&frames, items, &current, &result),
        ));
        let fields = lambda
            .iter()
            .enumerate()
            .map(|(index, (name, _))| {
                format!(
                    "get_field({}, 'item_{index}') AS {}",
                    ident(&tuple),
                    ident(name)
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        let expanded_name = internal("expanded");
        input_table = ident(&expanded_name);
        let expanded = format!(
            "SELECT DISTINCT t.*, {fields} FROM (SELECT s.*, unnest({}) AS {} FROM {seed_table} AS s) AS t",
            ident(&list),
            ident(&tuple)
        );
        lambda_projection = Some((tuple, current, (expanded_name, expanded)));
    }
    drop(frames);
    let key_name = ident(&key);
    let value_name = ident(&value);
    let input_key = format!(
        "__graphlake_cypher_capture_key({})",
        batch_call::import_key(&imported, "s")
    );
    let mut extensions = Extensions {
        partitioned_csr: outer.partitioned_csr,
        indexed_csr: outer.indexed_csr,
        call_imports: outer.call_imports.clone(),
        ..Default::default()
    };
    let mut plan = Plan {
        indexed_csr: outer.indexed_csr,
        expansion_base: 1_000_000 + span.start * 512,
        ..Default::default()
    };
    if !query.unions.is_empty() {
        let explicit = matches!(query.initial.clauses.last(), Some(Clause::Return(_)));
        ensure!(
            query.unions.iter().all(|branch| {
                matches!(branch.query.clauses.last(), Some(Clause::Return(_))) == explicit
            }) && (explicit || query.unions.iter().all(|branch| branch.all)),
            "subquery UNION must return compatible columns"
        );
    }
    for branch in std::iter::once(&mut query.initial)
        .chain(query.unions.iter_mut().map(|branch| &mut branch.query))
    {
        if !matches!(branch.clauses.last(), Some(Clause::Return(_))) {
            branch.clauses.push(output(
                Expr::new(ExprKind::Integer(1), span),
                &implicit,
                span,
            ));
        }
    }
    let mut stage = if captures.is_empty() {
        branches(catalog, &query, &mut extensions, &mut plan, None, false)?
    } else {
        let names = imported.columns.keys().cloned().collect::<Vec<_>>();
        let mut columns = imported.clone();
        columns.columns.insert(key.clone(), Value::Scalar);
        let seed_sql = format!("SELECT s.*, {input_key} AS {key_name} FROM {input_table} AS s");
        let mut stages = Vec::new();
        for branch in
            std::iter::once(&query.initial).chain(query.unions.iter().map(|branch| &branch.query))
        {
            let mut stage = Stage {
                sql: seed_sql.clone(),
                scope: columns.clone(),
                output: Vec::new(),
                order: Vec::new(),
            };
            for clause in &branch.clauses {
                stage = match clause {
                    Clause::Match(pattern) => join_match(
                        catalog,
                        pattern,
                        Some(stage),
                        None,
                        &mut extensions,
                        &mut plan,
                        None,
                    )?,
                    Clause::Unwind(unwind) => {
                        let mut scope = MatchPlan::new(catalog);
                        scope.call_imports = extensions.call_imports.clone();
                        stage.bind(&mut scope);
                        unwind_stage(catalog, unwind, Some(stage), scope, &mut plan, None, None)?.0
                    }
                    Clause::With(with) => batch_call::project_with(
                        catalog,
                        with,
                        stage,
                        Some(&names),
                        &key,
                        &seed_sql,
                        false,
                        &extensions,
                        &mut plan,
                    )?,
                    Clause::Call(call) => {
                        call_subquery(catalog, call, Some(stage), &mut extensions, &mut plan, None)?
                            .0
                    }
                    Clause::Return(ret) => {
                        let body = ret.body.clone();
                        for item in &body.items {
                            if let Some(alias) = &item.alias {
                                ensure!(
                                    !imported.columns.contains_key(&alias.0),
                                    "subquery cannot overwrite outer variable {}",
                                    alias.0
                                );
                            }
                        }
                        let output = returned_names(&stage, &body, &key);
                        let project = opencypher::With {
                            distinct: ret.distinct,
                            body,
                            where_clause: None,
                            span: ret.span,
                        };
                        let mut next = batch_call::project_with(
                            catalog,
                            &project,
                            stage,
                            Some(&names),
                            &key,
                            &seed_sql,
                            false,
                            &extensions,
                            &mut plan,
                        )?;
                        next.output = output;
                        let selected = next
                            .output
                            .iter()
                            .map(|name| qualified("l", name))
                            .chain([qualified("l", &key)])
                            .collect::<Vec<_>>();
                        next.sql = format!("SELECT {}{}", selected.join(", "), next.input());
                        next.scope
                            .columns
                            .retain(|name, _| name == &key || next.output.contains(name));
                        next
                    }
                    _ => bail!(
                        "correlated row subquery supports MATCH, WITH, UNWIND, CALL and RETURN"
                    ),
                };
            }
            stages.push(stage);
        }
        if query.unions.is_empty() {
            stages.remove(0)
        } else {
            combine_union(catalog, &query, stages, Some(&key), false)?
        }
    };
    ensure!(
        plan.procedures.is_empty() && plan.schema_procedures.is_empty(),
        "row subquery does not support materialized procedures"
    );
    if emit {
        outer
            .expression_labels
            .borrow_mut()
            .append(&mut plan.labels);
        outer
            .expression_expansions
            .borrow_mut()
            .append(&mut plan.expansions);
    }
    let (aggregate, fallback, kind) = match mode {
        RowExpression::Exists => ("TRUE".to_owned(), "FALSE", Value::Scalar),
        RowExpression::Count => ("count(*)".to_owned(), "0", Value::Scalar),
        RowExpression::List => {
            let item = stage
                .output
                .first()
                .context("pattern comprehension has no projection")?;
            (
                format!("array_agg({})", ident(item)),
                "make_array()",
                stage.scope.columns[item].clone(),
            )
        }
    };
    if captures.is_empty() {
        let sql = if matches!(mode, RowExpression::Exists) || !plan.calls.is_empty() {
            let query = if matches!(mode, RowExpression::Exists) {
                format!(
                    "SELECT TRUE AS {value_name} FROM ({}) AS rows LIMIT 1",
                    stage.sql
                )
            } else {
                format!(
                    "SELECT {aggregate} AS {value_name} FROM ({}) AS rows",
                    stage.sql
                )
            };
            if emit {
                outer.expression_joins.borrow_mut().push(ExpressionJoin {
                    alias: result.clone(),
                    seed,
                    captures,
                    query,
                    expanded: None,
                    calls: (!plan.calls.is_empty()).then_some(plan.calls),
                });
            }
            let field = qualified(&result, &value);
            if matches!(mode, RowExpression::Exists) {
                format!("{field} IS NOT NULL")
            } else {
                format!("coalesce({field}, {fallback})")
            }
        } else {
            format!(
                "coalesce((SELECT {aggregate} FROM ({}) AS rows), {fallback})",
                stage.sql
            )
        };
        return Ok((sql, kind));
    }
    let distinct = if matches!(mode, RowExpression::Exists) {
        "DISTINCT "
    } else {
        ""
    };
    let group = if matches!(mode, RowExpression::Exists) {
        String::new()
    } else {
        format!(" GROUP BY {key_name}")
    };
    let mut fields = captures
        .iter()
        .map(|(name, _)| qualified("s", name))
        .collect::<Vec<_>>();
    if let Some((tuple, _, _)) = &lambda_projection {
        fields.push(qualified("s", tuple));
    }
    let (join, projected) = if lambda_projection.is_some() {
        (
            "LEFT JOIN",
            format!("coalesce(q.{value_name}, {fallback}) AS {value_name}"),
        )
    } else {
        ("JOIN", format!("q.{value_name}"))
    };
    stage.sql = format!(
        "SELECT {}, {projected} FROM {input_table} AS s {join} (SELECT {distinct}{key_name}, {aggregate} AS {value_name} FROM ({}) AS rows{group}) AS q ON q.{key_name} = {input_key}",
        fields.join(", "),
        stage.sql
    );
    let field = qualified(&result, &value);
    let (sql, expanded) = if let Some((tuple, current, expanded)) = lambda_projection {
        let keys = ident(&internal("keys"));
        let tuple_key = ident(&internal("tuple_key"));
        let fields = captures
            .iter()
            .map(|(name, _)| ident(name))
            .collect::<Vec<_>>()
            .join(", ");
        stage.sql = format!(
            "SELECT {fields}, array_agg({tuple_key} ORDER BY {tuple_key}) AS {keys}, array_agg({value_name} ORDER BY {tuple_key}) AS {value_name} FROM (SELECT q.*, __graphlake_cypher_capture_key({}) AS {tuple_key} FROM ({}) AS q) AS keyed GROUP BY {fields}",
            ident(&tuple),
            stage.sql,
        );
        let keys = format!("{}.{keys}", ident(&result));
        // Defaults are filled before building the lookup so the lambda only
        // indexes the arrays. A CASE here can reindex its captures incorrectly.
        let found = format!(
            "array_element({field}, CAST(array_position({keys}, __graphlake_cypher_capture_key({current})) AS BIGINT))"
        );
        (found, Some(expanded))
    } else {
        (format!("coalesce({field}, {fallback})"), None)
    };
    if emit {
        outer.expression_joins.borrow_mut().push(ExpressionJoin {
            alias: result,
            seed,
            captures,
            query: stage.sql,
            expanded,
            calls: (!plan.calls.is_empty()).then_some(plan.calls),
        });
    }
    Ok((sql, kind))
}

fn returned_names(stage: &Stage, body: &ReturnBody, key: &str) -> Vec<String> {
    let mut output = if body.star {
        stage
            .scope
            .columns
            .keys()
            .filter(|name| name.as_str() != key)
            .cloned()
            .collect()
    } else {
        Vec::new()
    };
    output.extend(body.items.iter().map(|item| {
        item.alias
            .as_ref()
            .map(|name| name.0.clone())
            .unwrap_or_else(|| crate::cypher::expression_name(&item.expr))
    }));
    output
}
