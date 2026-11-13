use opencypher::{
    CallKind, Clause, Expr, ExprKind, Query, ReturnBody, ReturnItem, SingleQuery, Span, Variable,
    Visitor, With, walk_expr,
};

pub(super) const COUNT_SIZE: &str = "__graphlake_collect_size";

pub(super) fn size_argument(expr: &Expr) -> Option<Expr> {
    let ExprKind::FunctionCall {
        name,
        args,
        distinct,
    } = &expr.kind
    else {
        return None;
    };
    (name.0.eq_ignore_ascii_case("collect") && args.len() == 1).then(|| Expr {
        kind: ExprKind::FunctionCall {
            name: opencypher::FunctionName(COUNT_SIZE.into()),
            args: args.clone(),
            distinct: *distinct,
        },
        span: expr.span,
    })
}

pub(super) fn fold_sizes(query: &mut Query) {
    fold_branch(&mut query.initial);
    for branch in &mut query.unions {
        fold_branch(&mut branch.query);
    }
}

fn fold_branch(query: &mut SingleQuery) {
    for clause in &mut query.clauses {
        if let Clause::Call(call) = clause
            && let CallKind::Subquery(query) = &mut call.kind
        {
            fold_sizes(query);
        }
    }
    let text = format!("{query:?}");
    let input_name = |span: Span| {
        let mut name = format!("__graphlake_collect_item_{}", span.start);
        while text.contains(&name) {
            name.push('_');
        }
        Variable(name)
    };
    if let Some(Clause::Return(ret)) = query.clauses.last_mut()
        && ret.body.order_by.is_empty()
        && !ret.body.star
        && let [item] = ret.body.items.as_mut_slice()
        && let ExprKind::FunctionCall {
            name,
            args,
            distinct: false,
        } = &item.expr.kind
        && name.0.eq_ignore_ascii_case("size")
        && let [arg] = args.as_slice()
        && let Some(mut count) = size_argument(arg)
        && let Some(input) = dedup_argument(&mut count, input_name(ret.span), ret.span)
    {
        item.alias
            .get_or_insert_with(|| Variable(item.expr.to_string()));
        item.expr = count;
        query.clauses.insert(query.clauses.len() - 1, input);
        return;
    }
    let [.., Clause::With(with), Clause::Return(ret)] = query.clauses.as_mut_slice() else {
        return;
    };
    if with.distinct || ret.body.star {
        return;
    }
    let one_collect = !with.body.star && with.body.items.len() == 1;
    let mut dedup = None;
    for item in &mut with.body.items {
        let Some(alias) = &item.alias else { continue };
        let Some(mut count) = size_argument(&item.expr) else {
            continue;
        };
        let name = &alias.0;
        if with
            .where_clause
            .iter()
            .chain(with.body.order_by.iter().map(|sort| &sort.expr))
            .chain(ret.body.order_by.iter().map(|sort| &sort.expr))
            .any(|expr| references(expr, name))
            || ret
                .body
                .items
                .iter()
                .any(|item| !is_size_of(&item.expr, name) && references(&item.expr, name))
            || !ret
                .body
                .items
                .iter()
                .any(|item| is_size_of(&item.expr, name))
        {
            continue;
        }
        if one_collect {
            dedup = dedup_argument(&mut count, input_name(with.span), with.span);
        }
        item.expr = count;
        for item in &mut ret.body.items {
            if is_size_of(&item.expr, name) {
                if item.alias.is_none() {
                    item.alias = Some(Variable(item.expr.to_string()));
                }
                item.expr.kind = ExprKind::Variable(alias.clone());
            }
        }
    }
    if let Some(input) = dedup {
        query.clauses.insert(query.clauses.len() - 2, input);
    }
}

fn dedup_argument(count: &mut Expr, name: Variable, span: Span) -> Option<Clause> {
    let ExprKind::FunctionCall { args, distinct, .. } = &mut count.kind else {
        return None;
    };
    if !*distinct {
        return None;
    }
    *distinct = false;
    let arg = std::mem::replace(
        &mut args[0],
        Expr {
            kind: ExprKind::Variable(name.clone()),
            span,
        },
    );
    // Expose deduplication as a spillable relational stage instead of
    // keeping all distinct values inside a single count accumulator.
    Some(Clause::With(With {
        distinct: true,
        body: ReturnBody {
            star: false,
            items: vec![ReturnItem {
                expr: arg,
                alias: Some(name),
                span,
            }],
            order_by: Vec::new(),
            skip: None,
            limit: None,
        },
        where_clause: None,
        span,
    }))
}

fn is_size_of(expr: &Expr, name: &str) -> bool {
    matches!(&expr.kind, ExprKind::FunctionCall { name: function, args, distinct: false }
        if function.0.eq_ignore_ascii_case("size")
            && matches!(args.as_slice(), [arg] if matches!(&arg.kind, ExprKind::Variable(var) if var.0 == name)))
}

fn references(expr: &Expr, name: &str) -> bool {
    struct Find<'a>(&'a str, bool);
    impl Visitor for Find<'_> {
        fn visit_expr(&mut self, expr: &Expr) {
            if matches!(&expr.kind, ExprKind::Variable(var) if var.0 == self.0) {
                self.1 = true;
            } else if !self.1 {
                walk_expr(self, expr);
            }
        }
    }
    let mut find = Find(name, false);
    find.visit_expr(expr);
    find.1
}
