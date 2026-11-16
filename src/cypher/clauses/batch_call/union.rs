use anyhow::{Result, ensure};
use opencypher::Query;

use super::super::{legacy_imports, single as scalar_branch};
use super::{
    Imports, Stage, Value, combine_union, ident, import_key, internal_names, legacy_names,
    order_clause, qualified, single,
};
use crate::{
    Catalog,
    cypher::{OrderKey, Plan, paths::Extensions},
};

pub(super) fn plan(
    catalog: &Catalog,
    query: &Query,
    imports: Imports<'_>,
    outer: &Stage,
    extensions: &mut Extensions,
    state: &mut Plan,
    outer_limit: Option<usize>,
) -> Result<Option<Stage>> {
    let branches = std::iter::once(&query.initial)
        .chain(query.unions.iter().map(|branch| &branch.query))
        .collect::<Vec<_>>();
    let imported;
    let names = if let Some(names) = imports.explicit {
        names
    } else {
        let Some(names) = branches
            .iter()
            .filter(|branch| legacy_imports(branch, imports.outer))
            .map(|branch| legacy_names(branch))
            .collect::<Option<Vec<_>>>()
        else {
            return Ok(None);
        };
        imported = names
            .into_iter()
            .flatten()
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        &imported
    };
    if names.is_empty() {
        return Ok(None);
    }
    let selected = imports.selected(names)?;
    let internal = internal_names(catalog, query, outer, "union_");
    let seed = internal("outer");
    let key = internal("key");
    let key_field = ident(&key);
    let input = ident(&seed);
    let seed_fields = selected
        .columns
        .keys()
        .map(|name| qualified("s", name))
        .collect::<Vec<_>>()
        .join(", ");
    let mut scope = selected.clone();
    scope.columns.insert(key.clone(), Value::Scalar);
    let distinct = Stage {
        sql: format!(
            "SELECT DISTINCT {seed_fields}, {} AS {key_field} FROM {input} AS s",
            import_key(&selected, "s")
        ),
        scope,
        output: Vec::new(),
        order: Vec::new(),
    };

    let nested_start = state.calls.len();
    let mut stages = Vec::new();
    for branch in branches {
        if imports.explicit.is_none() && !legacy_imports(branch, imports.outer) {
            let mut stage = scalar_branch(catalog, branch, extensions, state, None, false)?;
            stage.sql = format!(
                "SELECT r.*, k.{key_field} FROM ({}) AS k CROSS JOIN ({}) AS r",
                distinct.sql, stage.sql
            );
            stage.scope.columns.insert(key.clone(), Value::Scalar);
            stages.push(stage);
            continue;
        }
        let branch = Query {
            initial: branch.clone(),
            unions: Vec::new(),
            span: branch.span,
        };
        let Some(mut stage) = single(
            catalog, &branch, imports, &distinct, extensions, state, None,
        )?
        else {
            return Ok(None);
        };
        stage
            .scope
            .columns
            .retain(|name, _| name == &key || stage.output.contains(name));
        stages.push(stage);
    }
    let mut right = combine_union(catalog, query, stages, Some(&key), false)?;
    right.scope.columns.remove(&key);
    let mut scope = outer.scope.clone();
    if let Some(needed) = extensions
        .page_outputs
        .get(&(query.span.start, query.span.end))
    {
        scope.columns.retain(|name, _| needed.contains(name));
    }
    let mut fields = scope
        .columns
        .keys()
        .map(|name| qualified("l", name))
        .collect::<Vec<_>>();
    for (name, kind) in right.scope.columns {
        ensure!(
            !outer.scope.columns.contains_key(&name),
            "CALL result {name} is already bound"
        );
        scope.columns.insert(name, kind);
    }
    fields.extend(right.output.iter().map(|name| qualified("r", name)));
    let sequence = internal("sequence");
    let mut outer_sql = outer.sql.clone();
    let mut order = Vec::new();
    if !outer.order.is_empty() {
        let terms = outer
            .order
            .iter()
            .map(|key| key.term(Some("o")))
            .collect::<Vec<_>>();
        outer_sql = format!(
            "SELECT o.*, row_number() OVER ({}) AS {} FROM ({outer_sql}) AS o ORDER BY {}",
            order_clause(&terms),
            ident(&sequence),
            ident(&sequence)
        );
        fields.push(qualified("l", &sequence));
        order.push(OrderKey {
            column: sequence.clone(),
            descending: false,
            native: true,
        });
    }
    let join_sql = |right: &str| {
        format!(
            "SELECT {} FROM {input} AS l JOIN {right} AS r ON {} IS NOT DISTINCT FROM ({})",
            fields.join(", "),
            qualified("r", &key),
            import_key(&selected, "l")
        )
    };
    let nested = state.calls.split_off(nested_start);
    let sql = if outer_limit.is_some() || !nested.is_empty() {
        let groups = internal("groups");
        let mut sql = join_sql(&ident(&groups));
        if !order.is_empty() {
            sql.push_str(&format!(" ORDER BY {}", ident(&sequence)));
        }
        let table = internal("result");
        state.calls.push(crate::cypher::apply::Apply {
            table: table.clone(),
            seed,
            outer: outer_sql,
            inner: sql,
            nested,
            lookups: Vec::new(),
            limit: outer_limit,
            mode: crate::cypher::apply::Mode::Batch(crate::cypher::apply::Batch {
                order: order.iter().map(|key| key.column.clone()).collect(),
                keys: format!("SELECT {} FROM {input} AS s", import_key(&selected, "s")),
                groups: right.sql,
                group_key: key,
                group_rank: None,
                single_key_only: false,
                push_group_limit: false,
                table: groups,
            }),
        });
        format!("SELECT * FROM {}", ident(&table))
    } else {
        format!(
            "WITH {input} AS ({outer_sql}) {}",
            join_sql(&format!("({})", right.sql))
        )
    };
    Ok(Some(Stage {
        sql,
        scope,
        output: right.output,
        order,
    }))
}
