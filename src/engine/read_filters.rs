use std::{collections::BTreeMap, ops::ControlFlow};

use datafusion::{
    arrow::datatypes::Schema,
    sql::sqlparser::ast::{
        BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Query, SetExpr,
        Statement, TableFactor, UnaryOperator, Value, Visit, Visitor,
    },
};

use crate::{Catalog, Comparison, Predicate, ScalarValue};

/// Restrict eager source reads only when all occurrences of a table need the same rows.
pub(super) fn query_filter(
    statement: &Statement,
    catalog: &Catalog,
) -> Option<(String, Vec<Predicate>)> {
    let Statement::Query(root) = statement else {
        return None;
    };
    let mut query = root.as_ref();
    let mut depth = 1;
    let (select, from) = loop {
        let SetExpr::Select(select) = query.body.as_ref() else {
            return None;
        };
        let [from] = select.from.as_slice() else {
            return None;
        };
        if query.with.is_some() || !from.joins.is_empty() {
            return None;
        }
        if let TableFactor::Derived {
            lateral: false,
            subquery,
            ..
        } = &from.relation
        {
            depth += 1;
            query = subquery;
        } else {
            break (select, from);
        }
    };
    let TableFactor::Table {
        name,
        alias,
        args: None,
        version: None,
        sample: None,
        ..
    } = &from.relation
    else {
        return None;
    };
    let [part] = name.0.as_slice() else {
        return None;
    };
    let name = part.as_ident()?;
    let label = if name.quote_style.is_none() {
        name.value.to_ascii_lowercase()
    } else {
        name.value.clone()
    };
    if alias
        .as_ref()
        .is_some_and(|alias| !alias.columns.is_empty())
        || !catalog.nodes.contains_key(&label)
    {
        return None;
    }
    // A projection can wrap the filtered scan, but any extra subquery
    // might read the same table without its predicate.
    let mut count = CountQueries(depth);
    if root.visit(&mut count).is_break() {
        return None;
    }
    let qualifier = alias
        .as_ref()
        .map_or(name.value.as_str(), |alias| alias.name.value.as_str());
    let mut filters = Vec::new();
    extract(select.selection.as_ref()?, qualifier, &mut filters);
    // Restrict complete key groups so the local uniqueness check still sees every row for a key.
    filters.retain(|filter| catalog.nodes[&label].key.contains(&filter.column));
    Some((label, filters))
}

struct CountQueries(usize);

impl Visitor for CountQueries {
    type Break = ();

    fn pre_visit_query(&mut self, _: &Query) -> ControlFlow<()> {
        if self.0 == 0 {
            ControlFlow::Break(())
        } else {
            self.0 -= 1;
            ControlFlow::Continue(())
        }
    }
}

fn extract(expr: &Expr, qualifier: &str, filters: &mut Vec<Predicate>) {
    let (column, op, value) = match expr {
        Expr::Nested(inner) => return extract(inner, qualifier, filters),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            extract(left, qualifier, filters);
            return extract(right, qualifier, filters);
        }
        Expr::BinaryOp { left, op, right } => {
            let op = match op {
                BinaryOperator::Eq => Comparison::Eq,
                BinaryOperator::NotEq => Comparison::Ne,
                BinaryOperator::Lt => Comparison::Lt,
                BinaryOperator::LtEq => Comparison::Le,
                BinaryOperator::Gt => Comparison::Gt,
                BinaryOperator::GtEq => Comparison::Ge,
                _ => return,
            };
            let pair = if let Some(column) = column(left, qualifier) {
                literal(right).map(|value| (column, value))
            } else if op == Comparison::Eq {
                column(right, qualifier).zip(literal(left))
            } else {
                None
            };
            let Some((column, value)) = pair else { return };
            (column, op, value)
        }
        Expr::Function(function) if function.name.to_string() == "__graphlake_cypher_compare" => {
            let FunctionArguments::List(args) = &function.args else {
                return;
            };
            let [
                FunctionArg::Unnamed(FunctionArgExpr::Expr(left)),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(right)),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(op))),
            ] = args.args.as_slice()
            else {
                return;
            };
            let Value::SingleQuotedString(op) = &op.value else {
                return;
            };
            let op = match op.as_str() {
                "=" => Comparison::Eq,
                "<>" => Comparison::Ne,
                "<" => Comparison::Lt,
                "<=" => Comparison::Le,
                ">" => Comparison::Gt,
                ">=" => Comparison::Ge,
                _ => return,
            };
            let Some(column) = column(left, qualifier) else {
                return;
            };
            let Some(value) = literal(right) else { return };
            (column, op, value)
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            let Some(column) = column(inner, qualifier) else {
                return;
            };
            let op = if matches!(expr, Expr::IsNull(_)) {
                Comparison::IsNull
            } else {
                Comparison::IsNotNull
            };
            (column, op, serde_json::Value::Null)
        }
        _ => return,
    };
    filters.push(Predicate { column, op, value });
}

fn column(expr: &Expr, qualifier: &str) -> Option<String> {
    let name = match expr {
        Expr::Identifier(name) => name,
        Expr::CompoundIdentifier(parts) => {
            let [prefix, name] = parts.as_slice() else {
                return None;
            };
            if prefix.value != qualifier {
                return None;
            }
            name
        }
        _ => return None,
    };
    Some(if name.quote_style.is_none() {
        name.value.to_ascii_lowercase()
    } else {
        name.value.clone()
    })
}

fn integer(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Value(value) => match &value.value {
            Value::Number(text, _) => text.parse().ok(),
            _ => None,
        },
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => integer(expr)?.checked_neg(),
        _ => None,
    }
}

fn literal(expr: &Expr) -> Option<serde_json::Value> {
    if let Expr::Value(value) = expr {
        match &value.value {
            Value::SingleQuotedString(text) => return Some(text.clone().into()),
            Value::Boolean(value) => return Some((*value).into()),
            _ => {}
        }
    }
    integer(expr).map(Into::into)
}

pub(super) fn for_labels<'a>(
    catalog: &'a Catalog,
    labels: &std::collections::BTreeSet<String>,
    graph: &crate::GraphOptions,
    query_filter: Option<&(String, Vec<Predicate>)>,
) -> BTreeMap<&'a str, Vec<Predicate>> {
    let mut filters = BTreeMap::<_, Vec<Predicate>>::new();
    for label in labels {
        let mapping = catalog.mapping(label);
        let graph_filters = graph
            .logical_partition
            .iter()
            .filter(|_| !mapping.unpartitioned);
        let query_filters = query_filter
            .filter(|(name, _)| name == label)
            .into_iter()
            .flat_map(|(_, filters)| filters);
        let logical_filters = graph_filters.chain(query_filters);
        for input in std::iter::once(None)
            .filter(|_| mapping.inputs.is_empty())
            .chain(mapping.inputs.iter().map(Some))
        {
            let reference = input.map_or(mapping.source, |input| input.table.as_str());
            let current = input
                .into_iter()
                .flat_map(|input| input.filters.iter().cloned())
                .chain(logical_filters.clone().filter_map(|predicate| {
                    let mut predicate = predicate.clone();
                    if let Some(input) = input {
                        if input.casts.contains_key(&predicate.column) {
                            return None;
                        }
                        if let Some(column) = input.columns.get(&predicate.column) {
                            predicate.column.clone_from(column);
                        }
                    }
                    Some(predicate)
                }))
                .collect::<Vec<_>>();
            filters
                .entry(reference)
                .and_modify(|common| common.retain(|p| current.contains(p)))
                .or_insert(current);
        }
    }
    filters
}

pub(crate) fn where_clause(
    filters: &[Predicate],
    schema: &Schema,
    ident: impl Fn(&str) -> String,
) -> String {
    let terms = filters
        .iter()
        .filter_map(|predicate| {
            let kind = schema.field_with_name(&predicate.column).ok()?.data_type();
            let column = ident(&predicate.column);
            let operator = match predicate.op {
                Comparison::IsNull => return Some(format!("{column} IS NULL")),
                Comparison::IsNotNull => return Some(format!("{column} IS NOT NULL")),
                Comparison::Eq => "=",
                Comparison::Ne => "<>",
                Comparison::Lt => "<",
                Comparison::Le => "<=",
                Comparison::Gt => ">",
                Comparison::Ge => ">=",
            };
            if !kind.is_integer() {
                return None;
            }
            let value = predicate.value.as_i64()?.to_string();
            ScalarValue::try_from_string(value.clone(), kind).ok()?;
            Some(format!("{column} {operator} {value}"))
        })
        .collect::<Vec<_>>();
    if terms.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", terms.join(" AND "))
    }
}
