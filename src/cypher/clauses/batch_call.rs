mod union;

use anyhow::{Result, ensure};
use opencypher::{CallKind, Clause, Expr, ExprKind, Query, ReturnItem, SingleQuery, Visitor};

use super::{
    Imports, Scope, Stage, Value, branches, combine_union, entities, ident, join_match, qualified,
    query_imports,
};
use crate::{
    Catalog,
    cypher::{
        Aliases, HasAggregate, MatchPlan, OrderKey, Plan, element_id, paths::Extensions, sort_term,
    },
};

/// Evaluate compatible correlated clauses once for each distinct set of imports.
pub(super) fn plan(
    catalog: &Catalog,
    query: &Query,
    imports: Imports<'_>,
    outer: &Stage,
    extensions: &mut Extensions,
    plan: &mut Plan,
    outer_limit: Option<usize>,
) -> Result<Option<Stage>> {
    let saved = extensions.clone();
    let labels = plan.labels.clone();
    let calls = plan.calls.len();
    let lookups = plan.call_lookups.len();
    let expansions = plan.expansions.len();
    let procedures = plan.procedures.len();
    let schema_procedures = plan.schema_procedures.len();
    let result = if query.unions.is_empty() {
        single(
            catalog,
            query,
            imports,
            outer,
            extensions,
            plan,
            outer_limit,
        )
    } else {
        union::plan(
            catalog,
            query,
            imports,
            outer,
            extensions,
            plan,
            outer_limit,
        )
    }?;
    if result.is_none() {
        *extensions = saved;
        plan.labels = labels;
        plan.calls.truncate(calls);
        plan.call_lookups.truncate(lookups);
        plan.expansions.truncate(expansions);
        plan.procedures.truncate(procedures);
        plan.schema_procedures.truncate(schema_procedures);
    }
    Ok(result)
}

fn single(
    catalog: &Catalog,
    query: &Query,
    imports: Imports<'_>,
    outer: &Stage,
    extensions: &mut Extensions,
    state: &mut Plan,
    outer_limit: Option<usize>,
) -> Result<Option<Stage>> {
    let clauses = query.initial.clauses.as_slice();
    let imported_names;
    let (names, clauses) = if let Some(names) = imports.explicit {
        (names, clauses)
    } else {
        let Some(names) = legacy_names(&query.initial) else {
            return Ok(None);
        };
        imported_names = names;
        (imported_names.as_slice(), &clauses[1..])
    };
    let [body @ .., Clause::Return(ret)] = clauses else {
        return Ok(None);
    };
    if names.is_empty()
        || ret.distinct
        || ret.body.star
        || !body.iter().all(|clause| match clause {
            Clause::Match(_) => true,
            Clause::With(_) | Clause::Unwind(_) => true,
            Clause::Call(call) => matches!(call.kind, CallKind::Subquery(_)),
            _ => false,
        })
    {
        return Ok(None);
    }
    let selected = imports.selected(names)?;
    let aggregate = ret
        .body
        .items
        .iter()
        .map(|item| has_aggregate(&item.expr))
        .collect::<Vec<_>>();
    let aggregated = aggregate.contains(&true);
    let global = aggregated && aggregate.iter().all(|value| *value);
    let ordered = !ret.body.order_by.is_empty();
    let paged = ret.body.skip.is_some() || ret.body.limit.is_some();
    let skip = page(ret.body.skip.as_ref(), 0, "SKIP")?;
    let limit = page(ret.body.limit.as_ref(), i64::MAX, "LIMIT")?;
    let outputs = ret
        .body
        .items
        .iter()
        .map(|item| {
            item.alias
                .as_ref()
                .map(|name| name.0.clone())
                .unwrap_or_else(|| crate::cypher::expression_name(&item.expr))
        })
        .collect::<Vec<_>>();
    let projected_order = if aggregated {
        let Some(columns) =
            ret.body
                .order_by
                .iter()
                .map(|sort| {
                    let spelling = crate::cypher::expression_name(&sort.expr);
                    outputs
                        .iter()
                        .position(|name| name == &spelling)
                        .or_else(|| {
                            ret.body.items.iter().position(|item| {
                                crate::cypher::expression_name(&item.expr) == spelling
                            })
                        })
                })
                .collect::<Option<Vec<_>>>()
        else {
            return Ok(None);
        };
        columns
    } else {
        Vec::new()
    };
    if !aggregated
        && ret
            .body
            .order_by
            .iter()
            .any(|item| has_aggregate(&item.expr))
    {
        return Ok(None);
    }

    let defaults = if global {
        if skip != 0 || limit == 0 {
            return Ok(None);
        }
        let Some(defaults) = ret
            .body
            .items
            .iter()
            .map(|item| match &item.expr.kind {
                ExprKind::CountStar => Some(Some("0")),
                ExprKind::FunctionCall { name, .. } => match name.0.to_ascii_lowercase().as_str() {
                    "count" | "sum" | crate::cypher::collect::COUNT_SIZE => Some(Some("0")),
                    "avg" | "min" | "max" => Some(None),
                    "collect" => Some(Some("make_array()")),
                    _ => None,
                },
                _ => None,
            })
            .collect::<Option<Vec<_>>>()
        else {
            return Ok(None);
        };
        defaults
    } else {
        Vec::new()
    };

    let internal = internal_names(catalog, query, outer, "");
    let seed = internal("outer");
    let input = ident(&seed);
    let key = internal("partition");
    let rank = internal("rank");
    let sequence = internal("sequence");
    let key_expr = |alias| import_key(&selected, alias);
    let projection = selected
        .columns
        .keys()
        .map(|name| qualified("seed", name))
        .collect::<Vec<_>>()
        .join(", ");
    let mut seed_scope = selected.clone();
    seed_scope.columns.insert(key.clone(), Value::Scalar);
    let mut right = Stage {
        sql: format!(
            "SELECT DISTINCT {projection}, {} AS {} FROM {input} AS seed",
            key_expr("seed"),
            ident(&key)
        ),
        scope: seed_scope,
        output: Vec::new(),
        order: Vec::new(),
    };
    let seed_stage = right.sql.clone();
    let lookup_start = state.call_lookups.len();
    let nested_start = state.calls.len();
    let single_key_only = outer_limit.is_some_and(|limit| limit <= crate::cypher::apply::SINGLE_KEY_PAGE)
        && body.iter().any(|clause| matches!(clause, Clause::With(with)
            if with.body.order_by.is_empty() && (with.body.skip.is_some() || with.body.limit.is_some())));
    for (index, clause) in body.iter().enumerate() {
        right = match clause {
            Clause::Match(pattern) => join_match(
                catalog,
                pattern,
                Some(right),
                None,
                extensions,
                state,
                Some((
                    Imports {
                        outer: &selected,
                        seed: &seed,
                        explicit: Some(names),
                    },
                    index != 0,
                )),
            )?,
            Clause::With(with) => project_with(
                catalog,
                with,
                right,
                imports.explicit,
                &key,
                &seed_stage,
                single_key_only,
                extensions,
                state,
            )?,
            Clause::Unwind(unwind) => {
                let mut bindings = MatchPlan::new(catalog);
                bindings.call_imports = extensions.call_imports.clone();
                right.bind(&mut bindings);
                super::unwind_stage(catalog, unwind, Some(right), bindings, state, None, None)?.0
            }
            Clause::Call(call) if let CallKind::Subquery(query) = &call.kind => {
                let explicit = extensions.call_imports.get(&call.span.start).cloned();
                let correlated = explicit.as_ref().map_or_else(
                    || query_imports(query, &right.scope),
                    |names| !names.is_empty(),
                );
                if !correlated {
                    let child = branches(catalog, query, extensions, state, None, false)?;
                    for (name, kind) in child.scope.columns {
                        ensure!(
                            !right.scope.columns.contains_key(&name),
                            "CALL result {name} is already bound"
                        );
                        right.scope.columns.insert(name, kind);
                    }
                    right.sql = format!(
                        "SELECT l.*, r.* FROM ({}) AS l CROSS JOIN ({}) AS r",
                        right.sql, child.sql
                    );
                    right.order.clear();
                    continue;
                }
                let imports = Imports {
                    outer: &right.scope,
                    seed: "",
                    explicit: explicit.as_deref(),
                };
                if let Some(needed) = extensions
                    .page_outputs
                    .get_mut(&(query.span.start, query.span.end))
                {
                    needed.insert(key.clone());
                }
                let Some(next) = plan(catalog, query, imports, &right, extensions, state, None)?
                else {
                    return Ok(None);
                };
                next
            }
            _ => unreachable!("batchable CALL clause"),
        };
    }

    let mut bindings = MatchPlan::new(catalog);
    bindings.call_imports = extensions.call_imports.clone();
    right.bind(&mut bindings);
    let mut fields = vec![qualified("l", &key)];
    let mut group = vec![qualified("l", &key)];
    let mut scope = outer.scope.clone();
    if let Some(needed) = extensions
        .page_outputs
        .get(&(query.span.start, query.span.end))
    {
        // The page only carries the outer columns observed by its sort or
        // final projection. Imported entities are still available to MATCH.
        scope.columns.retain(|name, _| needed.contains(name));
    }
    let mut projection = scope
        .columns
        .keys()
        .map(|name| qualified("l", name))
        .collect::<Vec<_>>();
    let mut aliases = Aliases::new();
    for (index, item) in ret.body.items.iter().enumerate() {
        let name = &outputs[index];
        ensure!(
            !outer.scope.columns.contains_key(name),
            "CALL result {name} is already bound"
        );
        let value = bindings.expr(&item.expr)?;
        let kind = Value::projected(&bindings, &item.expr);
        if aggregated && !aggregate[index] {
            group.push(format!("__graphlake_cypher_equivalence({value})"));
            fields.push(format!("first_value({value}) AS {}", ident(name)));
        } else {
            fields.push(format!("{value} AS {}", ident(name)));
        }
        aliases.insert(name.clone(), (value, kind.clone()));
        scope.columns.insert(name.clone(), kind);
        let value = qualified("r", name);
        projection.push(if let Some(Some(default)) = defaults.get(index) {
            format!("coalesce({value}, {default}) AS {}", ident(name))
        } else {
            value
        });
    }

    let window = (ordered || paged || !aggregated && !right.order.is_empty()) && !global;
    let mut order = Vec::new();
    let mut outer_sql = outer.sql.clone();
    let sequenced = window || !outer.order.is_empty();
    if sequenced {
        let ordering = outer
            .order
            .iter()
            .map(|key| key.term(Some("o")))
            .collect::<Vec<_>>();
        let over = order_clause(&ordering);
        outer_sql = format!(
            "SELECT o.*, row_number() OVER ({over}) AS {} FROM ({outer_sql}) AS o",
            ident(&sequence)
        );
        projection.push(qualified("l", &sequence));
        order.push(OrderKey {
            column: sequence.clone(),
            descending: false,
            native: true,
        });
        if window {
            projection.push(qualified("r", &rank));
            order.push(OrderKey {
                column: rank.clone(),
                descending: false,
                native: true,
            });
        }
    }

    let right = if aggregated {
        let group = format!(" GROUP BY {}", group.join(", "));
        let from = bindings.join_expressions(&right.input());
        let grouped = format!("SELECT {}{from}{group}", fields.join(", "));
        if window {
            let ordering = projected_order
                .iter()
                .zip(&ret.body.order_by)
                .map(|(&index, sort)| sort_term(&qualified("g", &outputs[index]), sort.descending))
                .collect::<Vec<_>>();
            format!(
                "SELECT g.*, {} FROM ({grouped}) AS g",
                rank_column("g", &key, &rank, &ordering)
            )
        } else {
            grouped
        }
    } else {
        if window {
            let mut ordering = ret
                .body
                .order_by
                .iter()
                .map(|sort| {
                    Ok(sort_term(
                        &bindings.expr_with_aliases(&sort.expr, &aliases)?,
                        sort.descending,
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            if ordering.is_empty() {
                ordering.extend(right.order.iter().map(|item| item.term(Some("l"))));
            }
            fields.push(rank_column("l", &key, &rank, &ordering));
        }
        let from = bindings.join_expressions(&right.input());
        format!("SELECT {}{from}", fields.join(", "))
    };
    bindings.collect_expressions(state);
    let right = if window && paged {
        format!(
            "SELECT * FROM ({right}) AS ranked WHERE {0} > {skip} AND {0} <= {1}",
            ident(&rank),
            skip.saturating_add(limit)
        )
    } else {
        right
    };
    let join = if global { "LEFT JOIN" } else { "JOIN" };
    let join_sql = |right: &str| {
        format!(
            "SELECT {} FROM {input} AS l {join} {right} AS r ON ({} IS NOT DISTINCT FROM ({}))",
            projection.join(", "),
            qualified("r", &key),
            key_expr("l")
        )
    };
    let lookups = state.call_lookups.split_off(lookup_start);
    let nested = state.calls.split_off(nested_start);
    let sql = if outer_limit.is_some()
        || !outer.order.is_empty()
        || !lookups.is_empty()
        || !nested.is_empty()
    {
        let groups = internal("groups");
        let mut sql = join_sql(&ident(&groups));
        if sequenced {
            sql.push_str(&format!(
                " ORDER BY {}",
                order
                    .iter()
                    .map(|key| key.term(None))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
            if !outer.order.is_empty() {
                outer_sql.push_str(&format!(" ORDER BY {}", ident(&sequence)));
            }
        }
        let table = internal("result");
        let uses_traversal_seed = state.expansions.iter().any(|expansion| {
            expansion
                .start_seed
                .as_ref()
                .is_some_and(|input| input.table == seed)
        });
        state.calls.push(crate::cypher::apply::Apply {
            table: table.clone(),
            seed,
            outer: outer_sql,
            inner: sql,
            nested,
            lookups,
            limit: outer_limit,
            mode: crate::cypher::apply::Mode::Batch(crate::cypher::apply::Batch {
                order: order.iter().map(|key| key.column.clone()).collect(),
                keys: format!("SELECT {} FROM {input} AS s", key_expr("s")),
                groups: right,
                group_key: key,
                group_rank: window.then_some((rank, skip as u64)),
                single_key_only,
                push_group_limit: !aggregated
                    && body.iter().all(|clause| match clause {
                        Clause::With(with) => {
                            !with.distinct
                                && !with.body.items.iter().any(|item| has_aggregate(&item.expr))
                        }
                        Clause::Call(_) => false,
                        _ => true,
                    }),
                table: groups,
                uses_traversal_seed,
            }),
        });
        format!("SELECT * FROM {}", ident(&table))
    } else {
        format!(
            "WITH {input} AS ({outer_sql}) {}",
            join_sql(&format!("({right})"))
        )
    };
    Ok(Some(Stage {
        sql,
        scope,
        output: outputs,
        order,
    }))
}

fn plain_with(with: &opencypher::With) -> bool {
    !with.distinct
        && !with.body.star
        && with.body.order_by.is_empty()
        && with.body.skip.is_none()
        && with.body.limit.is_none()
        && !with.body.items.iter().any(|item| has_aggregate(&item.expr))
}

fn internal_names(
    catalog: &Catalog,
    query: &Query,
    outer: &Stage,
    prefix: &str,
) -> impl Fn(&str) -> String {
    let text = format!("{query:?}");
    move |stem| {
        let mut name = format!("__graphlake_call_{prefix}{stem}_{}", query.span.start);
        while text.contains(&name)
            || outer.scope.columns.contains_key(&name)
            || outer.order.iter().any(|key| key.column == name)
            || catalog.nodes.contains_key(&name)
            || catalog.edges.contains_key(&name)
        {
            name.push('_');
        }
        name
    }
}

pub(super) fn import_key(scope: &Scope, alias: &str) -> String {
    let fields = scope
        .columns
        .iter()
        .enumerate()
        .flat_map(|(index, (name, kind))| {
            let column = qualified(alias, name);
            let value = if matches!(kind, Value::Entity(_)) {
                let (label, key) = entities::identity(&column);
                element_id(&label, &key)
            } else {
                column
            };
            [format!("'k{index}'"), value]
        })
        .collect::<Vec<_>>();
    format!("named_struct({})", fields.join(", "))
}

fn legacy_names(query: &SingleQuery) -> Option<Vec<String>> {
    let Some(Clause::With(with)) = query.clauses.first() else {
        return None;
    };
    if !plain_with(with) || with.where_clause.is_some() {
        return None;
    }
    with.body
        .items
        .iter()
        .map(|item| match (&item.alias, &item.expr.kind) {
            (None, ExprKind::Variable(name)) => Some(name.0.clone()),
            _ => None,
        })
        .collect()
}

pub(super) fn has_aggregate(expr: &Expr) -> bool {
    let mut aggregate = HasAggregate(false);
    aggregate.visit_expr(expr);
    aggregate.0
}

pub(super) fn page(expr: Option<&Expr>, default: i64, clause: &str) -> Result<i64> {
    let Some(expr) = expr else { return Ok(default) };
    let ExprKind::Integer(value) = expr.kind else {
        anyhow::bail!("{clause} requires an integer literal");
    };
    ensure!(value >= 0, "{clause} requires a nonnegative integer");
    Ok(value)
}

fn order_clause(order: &[String]) -> String {
    if order.is_empty() {
        String::new()
    } else {
        format!("ORDER BY {} ROWS UNBOUNDED PRECEDING", order.join(", "))
    }
}

fn rank_column(alias: &str, key: &str, rank: &str, order: &[String]) -> String {
    format!(
        "row_number() OVER (PARTITION BY {} {}) AS {}",
        qualified(alias, key),
        order_clause(order),
        ident(rank)
    )
}

pub(super) fn project_with(
    catalog: &Catalog,
    with: &opencypher::With,
    input: Stage,
    imports: Option<&[String]>,
    key: &str,
    seed: &str,
    single_seed: bool,
    extensions: &Extensions,
    plan: &mut Plan,
) -> Result<Stage> {
    let mut bindings = MatchPlan::new(catalog);
    bindings.call_imports = extensions.call_imports.clone();
    input.bind(&mut bindings);
    let mut body = with.body.clone();
    let paged = body.skip.is_some() || body.limit.is_some();
    let skip = page(body.skip.take().as_ref(), 0, "SKIP")?;
    let limit = page(body.limit.take().as_ref(), i64::MAX, "LIMIT")?;
    let global = !body.star
        && !body.items.is_empty()
        && body.items.iter().all(|item| has_aggregate(&item.expr));
    let imports = imports.unwrap_or_default();
    let empty = if global {
        // Adding the batch key turns a global aggregate into a grouped one.
        // The scalar projection over an empty input supplies Cypher's empty
        // aggregate result for keys that vanished in MATCH or UNWIND.
        let mut empty_body = body.clone();
        empty_body.order_by.clear();
        let mut empty_bindings = MatchPlan::new(catalog);
        empty_bindings.call_imports = extensions.call_imports.clone();
        input.bind(&mut empty_bindings);
        let result = empty_bindings
            .project(
                &empty_body,
                false,
                format!(
                    " FROM (SELECT * FROM ({}) AS empty_input LIMIT 0) AS l",
                    input.sql
                ),
                &[],
                true,
                false,
            )?
            .0;
        empty_bindings.collect_expressions(plan);
        Some(result)
    } else {
        None
    };
    bindings.partition_column = Some(key.to_owned());
    for name in imports
        .iter()
        .map(String::as_str)
        .chain(std::iter::once(key))
    {
        if let Some(item) = body.items.iter().find(|item| {
            item.alias.as_ref().map_or_else(
                || matches!(&item.expr.kind, ExprKind::Variable(var) if var.0 == name),
                |alias| alias.0 == name,
            )
        }) {
            ensure!(
                matches!(&item.expr.kind, ExprKind::Variable(var) if var.0 == name),
                "WITH cannot overwrite imported variable {name}"
            );
            continue;
        }
        if !body.star && (!global || name == key) {
            let variable = opencypher::Variable(name.to_owned());
            body.items.push(ReturnItem {
                expr: Expr {
                    kind: ExprKind::Variable(variable.clone()),
                    span: with.span,
                },
                alias: Some(variable),
                span: with.span,
            });
        }
    }
    super::project_with(
        catalog,
        &bindings,
        with,
        &body,
        input.input(),
        |next| {
            if let Some(empty) = empty {
                for name in imports {
                    next.scope
                        .columns
                        .insert(name.clone(), input.scope.columns[name].clone());
                }
                let mut fields = next
                    .scope
                    .columns
                    .keys()
                    .map(|name| {
                        if name == key || imports.contains(name) {
                            qualified("s", name)
                        } else {
                            format!(
                                "CASE WHEN {} IS NULL THEN {} ELSE {} END AS {}",
                                qualified("g", key),
                                qualified("e", name),
                                qualified("g", name),
                                ident(name)
                            )
                        }
                    })
                    .collect::<Vec<_>>();
                fields.extend(next.order.iter().map(|item| qualified("g", &item.column)));
                next.sql = format!(
                    "SELECT {} FROM ({seed}) AS s CROSS JOIN ({empty}) AS e LEFT JOIN ({}) AS g
             ON {} IS NOT DISTINCT FROM {}",
                    fields.join(", "),
                    next.sql,
                    qualified("s", key),
                    qualified("g", key)
                );
            }
            if paged {
                if single_seed && next.order.is_empty() {
                    next.sql = format!("SELECT l.*{} LIMIT {limit} OFFSET {skip}", next.input());
                    return Ok(());
                }
                let mut rank = format!("{key}_with_rank_{}", with.span.start);
                while next.scope.columns.contains_key(&rank) {
                    rank.push('_');
                }
                let terms = next
                    .order
                    .iter()
                    .map(|order| order.term(Some("l")))
                    .collect::<Vec<_>>();
                next.sql = format!(
                    "SELECT l.*, {}{}",
                    rank_column("l", key, &rank, &terms),
                    next.input()
                );
                next.sql = format!(
                    "SELECT * FROM ({}) AS p WHERE {rank} > {skip} AND {rank} <= {}",
                    next.sql,
                    skip.saturating_add(limit),
                    rank = ident(&rank)
                );
                next.order = vec![OrderKey {
                    column: rank,
                    descending: false,
                    native: true,
                }];
            }
            Ok(())
        },
        imports,
        plan,
    )
}
