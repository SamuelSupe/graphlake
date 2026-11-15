use anyhow::{Context, Result, bail, ensure};
use opencypher::{Clause, ExistsExpr, Match, Span};

use super::{
    ExpressionJoin, Scope, Value, entities, ident, join_match, match_input, qualified,
    row_expression::References,
};
use crate::{
    Catalog,
    cypher::{Aliases, MatchPlan, Plan, element_id, paths::Extensions},
};

pub(in crate::cypher) fn exists(
    catalog: &Catalog,
    outer: &MatchPlan<'_>,
    aliases: &Aliases,
    body: &ExistsExpr,
    span: Span,
) -> Result<String> {
    if let ExistsExpr::Subquery(query) = body
        && (!query.unions.is_empty()
            || query
                .initial
                .clauses
                .iter()
                .any(|clause| !matches!(clause, Clause::Match(pattern) if !pattern.optional)))
    {
        return super::row_subquery(
            catalog,
            outer,
            aliases,
            body,
            span,
            super::RowExpression::Exists,
        );
    }
    let patterns = match body {
        ExistsExpr::Pattern {
            patterns,
            where_clause,
        } => vec![Match {
            optional: false,
            patterns: patterns.clone(),
            where_clause: where_clause.clone(),
            span,
        }],
        ExistsExpr::Subquery(query) if query.unions.is_empty() => {
            ensure!(!query.initial.clauses.is_empty(), "EXISTS requires MATCH");
            query
                .initial
                .clauses
                .iter()
                .map(|clause| {
                    let Clause::Match(pattern) = clause else {
                        bail!("EXISTS expects MATCH clauses with optional WHERE predicates");
                    };
                    ensure!(!pattern.optional, "EXISTS requires mandatory MATCH clauses");
                    Ok(pattern.clone())
                })
                .collect::<Result<Vec<_>>>()?
        }

        _ => bail!("EXISTS expects a MATCH pattern"),
    };
    let mut prior = Scope::from_plan(outer);
    for (name, (_, kind)) in aliases {
        prior.columns.insert(name.clone(), kind.clone());
    }
    let mut plan = Plan {
        indexed_csr: outer.indexed_csr,
        ..Default::default()
    };
    let mut extensions = Extensions {
        partitioned_csr: outer.partitioned_csr,
        indexed_csr: outer.indexed_csr,
        call_imports: outer.call_imports.clone(),
        ..Default::default()
    };
    let predicates = patterns
        .iter()
        .filter_map(|pattern| pattern.where_clause.as_ref())
        .collect::<Vec<_>>();
    let (sql, columns) = if let [pattern] = patterns.as_slice() {
        let (sql, columns, _) = match_input(
            catalog,
            pattern,
            &prior,
            &mut extensions,
            &mut plan,
            1_000_000 + span.start * 512,
            None,
        )?;
        (sql, columns)
    } else {
        let mut stage = None;
        for mut pattern in patterns.iter().cloned() {
            // Mandatory MATCH predicates can be evaluated after all joins, where
            // both the outer values and every candidate binding are available.
            pattern.where_clause = None;
            stage = Some(join_match(
                catalog,
                &pattern,
                stage,
                Some(&prior),
                &mut extensions,
                &mut plan,
                None,
            )?);
        }
        let stage = stage.context("EXISTS requires MATCH")?;
        (stage.sql, stage.scope)
    };
    let alias = format!("__graphlake_exists_{}", span.start);
    let mut scope = MatchPlan::new(catalog);
    scope.bindings.clone_from(&outer.bindings);
    scope.scalars.clone_from(&outer.scalars);
    columns.bind(&alias, &mut scope);
    let mut terms = Vec::new();
    let mut keys = Vec::new();
    let mut outer_keys = Vec::new();
    for (name, kind) in &columns.columns {
        let Some(previous) = prior.columns.get(name) else {
            continue;
        };
        let (Value::Entity(before), Value::Entity(after)) = (previous, kind) else {
            bail!("EXISTS variable {name} is already bound to a non-entity");
        };
        ensure!(
            before.node == after.node,
            "incompatible EXISTS variable {name}"
        );
        let previous = if let Some((value, _)) = aliases.get(name) {
            value.clone()
        } else {
            outer.bindings[name].value()
        };
        let current = scope.bindings[name].value();
        let (left_label, left_key) = entities::identity(&previous);
        let (right_label, right_key) = entities::identity(&current);
        let right = element_id(&right_label, &right_key);
        let left = element_id(&left_label, &left_key);
        let mut field = format!("__graphlake_exists_key_{}_{}", span.start, keys.len());
        while columns.columns.contains_key(&field) || prior.columns.contains_key(&field) {
            field.push('_');
        }
        outer_keys.push(left);
        keys.push((field, right));
    }
    let matched_keys = keys
        .iter()
        .map(|(field, value)| format!(", {value} AS {}", ident(field)))
        .collect::<Vec<_>>()
        .join("");
    let sql = format!(
        "SELECT {alias}.*{matched_keys} FROM ({sql}) AS {alias}",
        alias = ident(&alias)
    );
    let mut correlated_predicates = Vec::new();
    let mut matched = MatchPlan::new(catalog);
    matched.call_imports = outer.call_imports.clone();
    columns.bind(&alias, &mut matched);
    for predicate in predicates {
        if let Ok(expression) = matched.expr(predicate) {
            terms.push(expression);
        } else {
            correlated_predicates.push(predicate);
        }
    }
    let relation = ident(&alias);
    let candidate_from = matched.join_expressions(&format!(" FROM ({sql}) AS {relation}"));
    matched.collect_expressions(&mut plan);
    let mut vars = References {
        names: Default::default(),
        call_imports: &outer.call_imports,
    };
    for expr in &correlated_predicates {
        opencypher::Visitor::visit_expr(&mut vars, expr);
    }
    let frames = outer.lambda_inputs.borrow();
    let referenced = aliases
        .iter()
        .filter(|(name, (value, _))| {
            frames.iter().any(|(item, _)| item == value)
                && (outer_keys.iter().any(|key| has_identifier(key, value))
                    || vars.names.contains(*name) && !columns.columns.contains_key(*name))
        })
        .collect::<Vec<_>>();
    let filter = if terms.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", terms.join(" AND "))
    };
    let predicate = if !correlated_predicates.is_empty()
        || !referenced.is_empty()
        || !frames.is_empty() && !keys.is_empty()
    {
        let mut captured = Scope::default();
        let mut captures = keys
            .iter()
            .zip(&outer_keys)
            .filter(|(_, value)| {
                !referenced
                    .iter()
                    .any(|(_, (item, _))| has_identifier(value, item))
            })
            .map(|((name, _), value)| (name.clone(), value.clone()))
            .collect::<Vec<_>>();
        for name in vars.names {
            if columns.columns.contains_key(&name)
                || referenced.iter().any(|(item, _)| *item == &name)
            {
                continue;
            }
            if let Some((value, kind)) = aliases.get(&name) {
                captured.columns.insert(name.clone(), kind.clone());
                captures.push((name, value.clone()));
            } else if let Some(kind) = prior.columns.get(&name) {
                captures.push((name.clone(), outer.variable(&name)?));
                captured.columns.insert(name, kind.clone());
            }
        }

        let result = format!(
            "__graphlake_exists_result_{}_{}",
            span.start,
            outer.expression_joins.borrow().len()
        );
        let mut seed = format!("{result}_seed");
        while catalog.nodes.contains_key(&seed) || catalog.edges.contains_key(&seed) {
            seed.push('_');
        }
        let internal = |stem: &str| {
            let mut name = stem.to_owned();
            while prior.columns.contains_key(&name)
                || columns.columns.contains_key(&name)
                || captures.iter().any(|(key, _)| key == &name)
            {
                name.push('_');
            }
            name
        };
        let list_field = internal("__graphlake_lambda_input");
        let tuple_field = internal("__graphlake_lambda_tuple");
        let found = internal("__graphlake_found");
        let independent = referenced
            .iter()
            .map(|(_, (value, _))| {
                let index = frames.iter().position(|(item, _)| item == value)?;
                let list = &frames[index].1;
                frames[..index]
                    .iter()
                    .all(|(item, _)| !has_identifier(list, item))
                    .then_some(list)
            })
            .collect::<Option<Vec<_>>>();
        let mut inputs = Vec::new();
        let tuple = if referenced.is_empty() {
            None
        } else {
            let fields = referenced
                .iter()
                .enumerate()
                .flat_map(|(i, (_, (value, _)))| [format!("'item_{i}'"), value.clone()])
                .collect::<Vec<_>>();
            let tuple = format!("named_struct({})", fields.join(", "));
            if let Some(lists) = independent {
                inputs = lists
                    .iter()
                    .enumerate()
                    .map(|(i, _)| internal(&format!("{list_field}_{i}")))
                    .collect();
                captures.extend(inputs.iter().cloned().zip(lists.into_iter().cloned()));
            } else {
                let items = referenced.iter().map(|(_, (value, _))| value.as_str());
                captures.push((
                    list_field.clone(),
                    lambda_values(&frames, items, &tuple, &result),
                ));
            }
            for (name, (_, kind)) in &referenced {
                captured.columns.insert((*name).clone(), kind.clone());
            }
            Some(tuple)
        };
        ensure!(
            !captures.is_empty(),
            "EXISTS predicate references an unknown variable"
        );
        // The attached join can recursively compile another EXISTS. Release the
        // lexical frame borrow before rebinding the candidate predicates.
        drop(frames);
        let mut bindings = MatchPlan::new(catalog);
        bindings.call_imports = outer.call_imports.clone();
        captured.bind(&seed, &mut bindings);
        columns.bind(&alias, &mut bindings);
        let mut tests = keys
            .iter()
            .zip(&outer_keys)
            .map(|((key, _), value)| {
                let value = if let Some((name, (item, _))) = referenced
                    .iter()
                    .find(|(_, (item, _))| has_identifier(value, item))
                {
                    replace_identifiers(value, &[(item, qualified(&seed, name))])
                } else {
                    qualified(&seed, key)
                };
                format!("{value} = {}", qualified(&alias, key))
            })
            .collect::<Vec<_>>();
        for expr in &correlated_predicates {
            tests.push(bindings.expr(expr)?);
        }
        let candidate = format!("SELECT {relation}.*{candidate_from}{filter}");
        let seed_table = ident(&seed);
        let seed_from = if !inputs.is_empty() {
            let mut expanded = format!("SELECT * FROM {seed_table}");
            for (field, (name, _)) in inputs.iter().zip(&referenced) {
                expanded = format!(
                    "SELECT t.*, unnest({}) AS {} FROM ({expanded}) AS t",
                    qualified("t", field),
                    ident(name)
                );
            }
            let fields = referenced
                .iter()
                .enumerate()
                .flat_map(|(i, (name, _))| [format!("'item_{i}'"), qualified("t", name)])
                .collect::<Vec<_>>();
            format!(
                "(SELECT t.*, named_struct({}) AS {} FROM ({expanded}) AS t) AS {seed_table}",
                fields.join(", "),
                ident(&tuple_field)
            )
        } else if tuple.is_some() {
            let fields = referenced
                .iter()
                .enumerate()
                .map(|(i, (name, _))| {
                    format!(
                        "get_field({}, 'item_{i}') AS {}",
                        ident(&tuple_field),
                        ident(name)
                    )
                })
                .collect::<Vec<_>>();
            format!(
                "(SELECT t.*, {} FROM (SELECT {seed_table}.*, unnest({}) AS {} FROM {seed_table}) AS t) AS {seed_table}",
                fields.join(", "),
                ident(&list_field),
                ident(&tuple_field)
            )
        } else {
            seed_table.clone()
        };
        let matched = if bindings.expression_joins.borrow().is_empty() {
            format!(
                "SELECT {seed_table}.* FROM {seed_from} LEFT SEMI JOIN ({candidate}) AS {relation} ON {}",
                tests.join(" AND ")
            )
        } else {
            let from = bindings.join_expressions(&format!(
                " FROM {seed_from} CROSS JOIN ({candidate}) AS {relation}"
            ));
            format!(
                "SELECT DISTINCT {seed_table}.*{from} WHERE {}",
                tests.join(" AND ")
            )
        };
        bindings.collect_expressions(&mut plan);
        let result_field = qualified(&result, &found);
        let (query, predicate) = if let Some(tuple) = tuple {
            let columns = captures
                .iter()
                .map(|(name, _)| qualified("q", name))
                .collect::<Vec<_>>()
                .join(", ");
            (
                format!(
                    "SELECT {columns}, array_agg({}) AS {} FROM ({matched}) AS q GROUP BY {columns}",
                    qualified("q", &tuple_field),
                    ident(&found)
                ),
                format!("coalesce(array_has({result_field}, {tuple}), FALSE)"),
            )
        } else {
            (
                format!(
                    "SELECT q.*, TRUE AS {} FROM ({matched}) AS q",
                    ident(&found)
                ),
                format!("{result_field} IS NOT NULL"),
            )
        };
        outer.expression_joins.borrow_mut().push(ExpressionJoin {
            alias: result,
            seed,
            captures,
            query,
            expanded: None,
            calls: (!plan.calls.is_empty()).then(|| std::mem::take(&mut plan.calls)),
        });
        predicate
    } else {
        let mut terms = terms;
        terms.extend(
            keys.iter()
                .zip(&outer_keys)
                .map(|((field, _), left)| format!("{} = {left}", qualified(&alias, field))),
        );
        let filter = if terms.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", terms.join(" AND "))
        };
        if keys.is_empty() {
            let field = "__graphlake_found";
            let result = format!(
                "__graphlake_exists_result_{}_{}",
                span.start,
                outer.expression_joins.borrow().len()
            );
            outer.expression_joins.borrow_mut().push(ExpressionJoin {
                alias: result.clone(),
                seed: String::new(),
                captures: Vec::new(),
                query: format!("SELECT TRUE AS {field}{candidate_from}{filter} LIMIT 1"),
                expanded: None,
                calls: Default::default(),
            });
            format!("{} IS NOT NULL", qualified(&result, field))
        } else {
            format!("(coalesce((SELECT count(*){candidate_from}{filter}), 0) > 0)")
        }
    };
    outer
        .expression_labels
        .borrow_mut()
        .append(&mut plan.labels);
    outer
        .expression_expansions
        .borrow_mut()
        .append(&mut plan.expansions);
    outer.expression_calls.borrow_mut().append(&mut plan.calls);
    Ok(predicate)
}

pub(in crate::cypher) fn lambda_values<'a>(
    frames: &'a [(String, String)],
    items: impl Iterator<Item = &'a str>,
    tuple: &str,
    prefix: &str,
) -> String {
    let mut needed = items.collect::<std::collections::BTreeSet<_>>();
    let names = frames
        .iter()
        .enumerate()
        .map(|(i, (item, _))| (item, ident(&format!("{prefix}_item_{i}"))))
        .collect::<Vec<_>>();
    let names = names
        .iter()
        .map(|(from, to)| (from.as_str(), to.clone()))
        .collect::<Vec<_>>();
    let rebind = |sql: &str| replace_identifiers(sql, &names);
    let mut tuples: Option<String> = None;
    // Each list is evaluated in its original lexical scope. Only expand
    // referenced values and the frames needed to compute their lists.
    for (index, (item, list)) in frames.iter().enumerate().rev() {
        if !needed.remove(item.as_str()) {
            continue;
        }
        needed.extend(
            frames[..index]
                .iter()
                .filter(|(previous, _)| has_identifier(list, previous))
                .map(|(previous, _)| previous.as_str()),
        );
        let list = rebind(list);
        let item = &names[index].1;
        tuples = Some(if let Some(inner) = tuples {
            format!("flatten(array_transform({list}, {item} -> coalesce({inner}, make_array())))")
        } else {
            format!("array_transform({list}, {item} -> {})", rebind(tuple))
        });
    }
    tuples.expect("referenced lambda frames")
}

// The generated SQL quotes lambda variables as identifiers. A user string
// containing the same spelling is neither a capture nor a rename target.
fn quoted_identifiers(sql: &str) -> impl Iterator<Item = (usize, usize)> + '_ {
    let bytes = sql.as_bytes();
    let mut offset = 0;
    std::iter::from_fn(move || {
        while let Some(&quote) = bytes.get(offset) {
            let start = offset;
            offset += 1;
            if !matches!(quote, b'\'' | b'"') {
                continue;
            }
            while let Some(&byte) = bytes.get(offset) {
                offset += 1;
                if byte == quote {
                    if bytes.get(offset) == Some(&quote) {
                        offset += 1;
                    } else {
                        break;
                    }
                }
            }
            if quote == b'"' {
                return Some((start, offset));
            }
        }
        None
    })
}

pub(in crate::cypher) fn has_identifier(sql: &str, name: &str) -> bool {
    quoted_identifiers(sql).any(|(start, end)| &sql[start..end] == name)
}

pub(in crate::cypher) fn replace_identifiers(sql: &str, names: &[(&str, String)]) -> String {
    let mut result = String::with_capacity(sql.len());
    let mut previous = 0;
    for (start, end) in quoted_identifiers(sql) {
        if let Some((_, value)) = names.iter().find(|(name, _)| *name == &sql[start..end]) {
            result.push_str(&sql[previous..start]);
            result.push_str(value);
            previous = end;
        }
    }
    result.push_str(&sql[previous..]);
    result
}
