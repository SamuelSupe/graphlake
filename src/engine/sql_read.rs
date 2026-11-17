use std::{collections::BTreeSet, ops::ControlFlow};

use datafusion::sql::sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, Ident, JoinConstraint,
    JoinOperator, LimitClause, OrderByKind, Query, Select, SelectFlavor, SelectItem, SetExpr,
    Statement, TableFactor, Value, Visit, Visitor,
};

use crate::{Catalog, Predicate};

pub(crate) struct Read {
    pub(super) columns: Option<BTreeSet<String>>,
    pub(super) native: Option<(String, Pushdown)>,
    pub(super) single: Option<String>,
    pub(super) limited: bool,
}

#[derive(Clone)]
pub(crate) struct Pushdown {
    pub(crate) operation: Native,
    pub(crate) filters: Vec<Predicate>,
}

#[derive(Clone)]
pub(crate) enum Native {
    Filter,
    Top {
        order: Vec<(String, bool)>,
        limit: u64,
    },
    Aggregate {
        function: Aggregate,
        column: Option<String>,
    },
}

#[derive(Clone, Copy)]
pub(crate) enum Aggregate {
    Count,
    Min,
    Max,
}

pub(super) fn hint(statement: &Statement, catalog: &Catalog) -> Option<Read> {
    let Statement::Query(query) = statement else {
        return None;
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    if !matches!(select.flavor, SelectFlavor::Standard)
        || select.projection.is_empty()
        || query.with.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
        || select.into.is_some()
        || select.top.is_some()
        || select.exclude.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !select.connect_by.is_empty()
        || select.value_table_mode.is_some()
    {
        return None;
    }
    for from in &select.from {
        table(&from.relation)?;
        for join in &from.joins {
            table(&join.relation)?;
            if !matches!(
                &join.join_operator,
                JoinOperator::Join(JoinConstraint::On(_))
                    | JoinOperator::Inner(JoinConstraint::On(_))
                    | JoinOperator::Left(JoinConstraint::On(_))
                    | JoinOperator::LeftOuter(JoinConstraint::On(_))
                    | JoinOperator::Right(JoinConstraint::On(_))
                    | JoinOperator::RightOuter(JoinConstraint::On(_))
                    | JoinOperator::FullOuter(JoinConstraint::On(_))
                    | JoinOperator::CrossJoin(JoinConstraint::None)
            ) {
                return None;
            }
        }
    }

    let mut visitor = Columns::default();
    if query.visit(&mut visitor).is_break() {
        return None;
    }
    let columns = select
        .projection
        .iter()
        .all(|item| {
            matches!(
                item,
                SelectItem::UnnamedExpr(_) | SelectItem::ExprWithAlias { .. }
            )
        })
        .then_some(visitor.names);
    let native = native(query, select, catalog);
    // A blocking sort or grouping can exhaust the pool while the producer
    // still needs space for its next batch. Keep the spillable snapshot there.
    let single = match select.from.as_slice() {
        [from]
            if from.joins.is_empty()
                && query.order_by.is_none()
                && select.distinct.is_none()
                && select.having.is_none()
                && select.qualify.is_none()
                && matches!(&select.group_by, GroupByExpr::Expressions(e, m) if e.is_empty() && m.is_empty()) =>
        {
            table(&from.relation)
                .map(|(label, _)| label)
                .filter(|label| direct_node(catalog, label))
        }
        _ => None,
    };
    Some(Read {
        columns,
        native,
        single,
        limited: query.limit_clause.is_some(),
    })
}

fn direct_node(catalog: &Catalog, label: &str) -> bool {
    catalog
        .nodes
        .get(label)
        .is_some_and(|node| node.inputs.is_empty() && node.scd2.is_none() && !node.key.is_empty())
}

/// The graph compilers wrap a direct scan in projections. Only an unordered
/// outer LIMIT can cross these wrappers, and only if no wrapper filters rows.
pub(super) fn graph_hint(statement: &Statement, catalog: &Catalog) -> Option<Read> {
    let Statement::Query(root) = statement else {
        return None;
    };
    let mut query = root.as_ref();
    let mut limit = None;
    loop {
        let SetExpr::Select(select) = query.body.as_ref() else {
            return None;
        };
        let [from] = select.from.as_slice() else {
            return None;
        };
        if query.with.is_some()
            || query.fetch.is_some()
            || query.order_by.is_some()
            || !from.joins.is_empty()
            || select.distinct.is_some()
            || select.having.is_some()
            || select.qualify.is_some()
            || !matches!(&select.group_by, GroupByExpr::Expressions(e, m) if e.is_empty() && m.is_empty())
            || !select.projection.iter().all(graph_projection)
        {
            return None;
        }
        if let Some(clause) = &query.limit_clause {
            if !std::ptr::eq(query, root.as_ref()) {
                return None;
            }
            let LimitClause::LimitOffset {
                limit: Some(Expr::Value(value)),
                offset: None,
                limit_by,
            } = clause
            else {
                return None;
            };
            let Value::Number(value, _) = &value.value else {
                return None;
            };
            if !limit_by.is_empty() {
                return None;
            }
            limit = Some(value.parse::<i64>().ok().filter(|limit| *limit > 0)? as u64);
        }
        if let TableFactor::Derived {
            lateral: false,
            subquery,
            ..
        } = &from.relation
        {
            if select.selection.is_some() {
                return None;
            }
            query = subquery;
            continue;
        }
        let (label, qualifier) = table(&from.relation)?;
        let node = catalog.nodes.get(&label)?;
        if !node.inputs.is_empty() || node.scd2.is_some() || node.key.is_empty() {
            return None;
        }
        let mut filters = Vec::new();
        if select
            .selection
            .as_ref()
            .is_some_and(|expr| !super::read_filters::exact(expr, &qualifier, &mut filters))
        {
            return None;
        }
        let operation = match limit {
            Some(limit) => Some(Native::Top {
                limit,
                order: Vec::new(),
            }),
            None if !filters.is_empty() => Some(Native::Filter),
            None => None,
        };
        return Some(Read {
            columns: None,
            native: operation.map(|operation| (label.clone(), Pushdown { operation, filters })),
            single: Some(label),
            limited: limit.is_some(),
        });
    }
}

fn graph_projection(item: &SelectItem) -> bool {
    let expr = match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => return true,
        _ => return false,
    };
    match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) | Expr::Value(_) => true,
        Expr::Function(function)
            if function_name(function).as_deref() == Some("__graphlake_cypher_public") =>
        {
            matches!(&function.args, FunctionArguments::List(args)
                if matches!(args.args.as_slice(), [FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(_)))]))
        }
        _ => false,
    }
}

fn name(ident: &Ident) -> String {
    if ident.quote_style.is_none() {
        ident.value.to_ascii_lowercase()
    } else {
        ident.value.clone()
    }
}

fn table(factor: &TableFactor) -> Option<(String, String)> {
    let TableFactor::Table {
        name: object,
        alias,
        args: None,
        version: None,
        sample: None,
        ..
    } = factor
    else {
        return None;
    };
    let [part] = object.0.as_slice() else {
        return None;
    };
    let table = name(part.as_ident()?);
    if alias
        .as_ref()
        .is_some_and(|alias| !alias.columns.is_empty())
    {
        return None;
    }
    let qualifier = alias
        .as_ref()
        .map_or_else(|| table.clone(), |alias| name(&alias.name));
    Some((table, qualifier))
}

#[derive(Default)]
struct Columns {
    names: BTreeSet<String>,
    visited_query: bool,
}

impl Visitor for Columns {
    type Break = ();

    fn pre_visit_query(&mut self, _: &Query) -> ControlFlow<()> {
        if std::mem::replace(&mut self.visited_query, true) {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    }

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
        match expr {
            Expr::Identifier(column) => {
                self.names.insert(name(column));
            }
            // A compound identifier can also dereference a struct column.
            Expr::CompoundIdentifier(parts) => self.names.extend(parts.iter().map(name)),
            Expr::Wildcard(_) | Expr::QualifiedWildcard(..) => return ControlFlow::Break(()),
            Expr::Function(f) => {
                for args in [&f.args, &f.parameters] {
                    if let FunctionArguments::List(args) = args
                        && args.args.iter().any(|arg| {
                            !matches!(arg, FunctionArg::Unnamed(FunctionArgExpr::Expr(_)))
                        })
                        && !(function_name(f) == Some("count".into())
                            && matches!(
                                args.args.as_slice(),
                                [FunctionArg::Unnamed(FunctionArgExpr::Wildcard)]
                            ))
                    {
                        return ControlFlow::Break(());
                    }
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

fn function_name(function: &datafusion::sql::sqlparser::ast::Function) -> Option<String> {
    let [part] = function.name.0.as_slice() else {
        return None;
    };
    let ident = part.as_ident()?;
    ident
        .quote_style
        .is_none()
        .then(|| ident.value.to_ascii_lowercase())
}

fn column(expr: &Expr, qualifier: &str) -> Option<String> {
    match expr {
        Expr::Identifier(column) => Some(name(column)),
        Expr::CompoundIdentifier(parts) if parts.len() == 2 && name(&parts[0]) == qualifier => {
            Some(name(&parts[1]))
        }
        _ => None,
    }
}

fn native(query: &Query, select: &Select, catalog: &Catalog) -> Option<(String, Pushdown)> {
    let [from] = select.from.as_slice() else {
        return None;
    };
    if !from.joins.is_empty()
        || select.having.is_some()
        || select.qualify.is_some()
        || select.distinct.is_some()
        || select.select_modifiers.is_some()
        || !select.named_window.is_empty()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || !matches!(&select.group_by, GroupByExpr::Expressions(exprs, modifiers)
            if exprs.is_empty() && modifiers.is_empty())
    {
        return None;
    }
    let (label, qualifier) = table(&from.relation)?;
    let node = catalog.nodes.get(&label)?;
    let mut filters = Vec::new();
    if select
        .selection
        .as_ref()
        .is_some_and(|expr| !super::read_filters::exact(expr, &qualifier, &mut filters))
    {
        return None;
    }
    if !node.inputs.is_empty() || node.scd2.is_some() || node.key.is_empty() {
        return None;
    }
    let operation = if let Some(LimitClause::LimitOffset {
        limit: Some(Expr::Value(limit)),
        offset: None,
        limit_by,
    }) = &query.limit_clause
    {
        let Value::Number(limit, _) = &limit.value else {
            return None;
        };
        let limit = limit
            .parse::<u64>()
            .ok()
            .filter(|limit| i64::try_from(*limit).is_ok())?;
        if limit == 0
            || !limit_by.is_empty()
            || !select.projection.iter().all(|item| match item {
                SelectItem::UnnamedExpr(expr) => column(expr, &qualifier).is_some(),
                SelectItem::ExprWithAlias { expr, .. } if query.order_by.is_none() => {
                    column(expr, &qualifier).is_some()
                }
                SelectItem::Wildcard(options) => options == &Default::default(),
                _ => false,
            })
        {
            return None;
        }
        let Some(ordering) = query.order_by.as_ref() else {
            return Some((
                label,
                Pushdown {
                    operation: Native::Top {
                        limit,
                        order: Vec::new(),
                    },
                    filters,
                },
            ));
        };
        let OrderByKind::Expressions(terms) = &ordering.kind else {
            return None;
        };
        if ordering.interpolate.is_some() {
            return None;
        }
        let order = terms
            .iter()
            .map(|term| {
                if term.with_fill.is_some() {
                    return None;
                }
                Some((
                    column(&term.expr, &qualifier)?,
                    term.options.asc.unwrap_or(true),
                ))
            })
            .collect::<Option<Vec<_>>>()?;
        if order.len() != node.key.len()
            || !node
                .key
                .iter()
                .all(|key| order.iter().any(|(column, _)| column == key))
        {
            return None;
        }
        Native::Top { limit, order }
    } else {
        if query.limit_clause.is_some() || query.order_by.is_some() {
            return None;
        }
        let [
            SelectItem::UnnamedExpr(Expr::Function(function))
            | SelectItem::ExprWithAlias {
                expr: Expr::Function(function),
                ..
            },
        ] = select.projection.as_slice()
        else {
            return (!filters.is_empty()).then_some((
                label,
                Pushdown {
                    operation: Native::Filter,
                    filters,
                },
            ));
        };
        if function.over.is_some()
            || function.filter.is_some()
            || function.null_treatment.is_some()
            || !function.within_group.is_empty()
            || !matches!(function.parameters, FunctionArguments::None)
        {
            return None;
        }
        let function_kind = match function_name(function)?.as_str() {
            "count" => Aggregate::Count,
            "min" => Aggregate::Min,
            "max" => Aggregate::Max,
            _ => return None,
        };
        let FunctionArguments::List(args) = &function.args else {
            return None;
        };
        if args.duplicate_treatment.is_some() || !args.clauses.is_empty() {
            return None;
        }
        let column = match args.args.as_slice() {
            [FunctionArg::Unnamed(FunctionArgExpr::Wildcard)]
                if matches!(function_kind, Aggregate::Count) =>
            {
                None
            }
            [FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))] => {
                let column = column(expr, &qualifier)?;
                if !node.key.contains(&column) && !node.properties.contains(&column) {
                    return None;
                }
                Some(column)
            }
            _ => return None,
        };
        Native::Aggregate {
            function: function_kind,
            column,
        }
    };
    Some((label, Pushdown { operation, filters }))
}
