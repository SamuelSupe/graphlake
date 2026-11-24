use super::output_name;
use crate::cypher::clauses::batch_call::has_aggregate;
use opencypher::{
    Clause, Expr, ExprKind, Return, ReturnBody, ReturnItem, SingleQuery, Variable, Visitor, With,
    walk_expr,
};
use std::collections::BTreeSet;

pub(in crate::cypher::clauses) struct Grouped {
    pub query: SingleQuery,
    pub unwind: usize,
    pub page: usize,
    pub fields: BTreeSet<String>,
    pub edges: BTreeSet<String>,
}

impl Grouped {
    pub fn describe(query: &SingleQuery) -> Option<Self> {
        let [body @ .., Clause::Return(ret)] = query.clauses.as_slice() else {
            return None;
        };
        let unwind = body.iter().rposition(|clause| {
            matches!(clause, Clause::Unwind(unwind)
                if matches!(&unwind.expr.kind, ExprKind::FunctionCall { name, args, distinct: false }
                    if name.0.eq_ignore_ascii_case("relationships")
                    && matches!(args.as_slice(), [Expr { kind: ExprKind::Variable(_), .. }])))
        })?;
        let Clause::Unwind(unwind_clause) = &body[unwind] else {
            unreachable!();
        };
        let mut edges = BTreeSet::from([unwind_clause.variable.0.clone()]);
        let mut fields = BTreeSet::new();
        let mut grouped = false;
        for i in unwind + 1..=body.len() {
            let (mut with, terminal) = if i == body.len() {
                (
                    With {
                        body: ret.body.clone(),
                        distinct: ret.distinct,
                        where_clause: None,
                        span: ret.span,
                    },
                    true,
                )
            } else if let Clause::With(with) = &body[i] {
                (with.clone(), false)
            } else {
                return None;
            };
            let grouping =
                with.distinct || with.body.items.iter().any(|item| has_aggregate(&item.expr));
            grouped |= grouping;
            let paging = with.body.limit.is_some();
            if paging && (!grouped || with.body.order_by.is_empty()) {
                return None;
            }
            let original = with.body.items.clone();
            if original
                .iter()
                .map(output_name)
                .collect::<BTreeSet<_>>()
                .len()
                != original.len()
            {
                return None;
            }
            let mut next = projected_edges(&with.body, &edges);
            let mut restored = Vec::new();
            let mut injected = false;
            if paging && !with.body.star {
                // An existing group key must determine the relationship before
                // any properties can be moved outside a grouping projection.
                if next.is_empty()
                    && terminal
                    && edges.len() == 1
                    && (!grouping || original.iter().any(|item| is_identity(&item.expr, &edges)))
                {
                    if let Some(edge) = edges.first() {
                        let mut name = format!("__graphlake_page_edge_{}", with.span.start);
                        let text = format!("{query:?}");
                        while text.contains(&name) {
                            name.push('_');
                        }
                        next.insert(name.clone());
                        with.body.items.push(variable(edge, &name, with.span));
                        injected = true;
                    }
                }
                if next.len() == 1
                    && edges.len() == 1
                    && let Some(edge) = next.first()
                {
                    with.body.items.retain(|item| {
                        let ExprKind::Property { base, key } = &item.expr.kind else {
                            return true;
                        };
                        if !is_edge(base, &edges) {
                            return true;
                        }
                        let name = output_name(item);
                        let mut used = Used {
                            name: &name,
                            key: &key.0,
                            found: false,
                        };
                        for sort in &with.body.order_by {
                            used.visit_expr(&sort.expr);
                        }
                        if let Some(predicate) = &with.where_clause {
                            used.visit_expr(predicate);
                        }
                        if used.found {
                            return true;
                        }
                        let mut item = item.clone();
                        item.expr.kind = ExprKind::Property {
                            base: Box::new(variable(edge, edge, item.span).expr),
                            key: key.clone(),
                        };
                        item.alias = Some(Variable(name));
                        restored.push(item);
                        false
                    });
                }
                if injected && restored.is_empty() {
                    with.body.items.pop();
                    next.clear();
                }
            }
            let mut read = Read {
                edges: &edges,
                fields: &mut fields,
                full: false,
            };
            for item in &with.body.items {
                if !is_edge(&item.expr, &edges) {
                    read.visit_expr(&item.expr);
                }
            }
            if read.full {
                return None;
            }
            // Ordering first tries projected names and can then use input names.
            // Grouped WHERE only sees outputs.
            let mut lookup = edges.clone();
            lookup.extend(next.iter().cloned());
            let mut read = Read {
                edges: &lookup,
                fields: &mut fields,
                full: false,
            };
            for sort in &with.body.order_by {
                read.visit_expr(&sort.expr);
            }
            if read.full {
                return None;
            }
            if let Some(predicate) = &with.where_clause {
                let mut read = Read {
                    edges: if grouping { &next } else { &lookup },
                    fields: &mut fields,
                    full: false,
                };
                read.visit_expr(predicate);
                if read.full {
                    return None;
                }
            }
            edges = next;
            if !paging {
                continue;
            }
            let span = with.span;
            let mut rewritten = query.clone();
            rewritten.clauses[i] = Clause::With(with);
            if terminal {
                if ret.body.star && injected {
                    return None;
                }
                let items = if ret.body.star {
                    // `WITH *, ...` already carries every original non-restored item in scope;
                    // only emit items that were removed from `with.body.items` into `restored`.
                    restored
                } else {
                    original
                        .iter()
                        .map(|item| {
                            let name = output_name(item);
                            restored
                                .iter()
                                .find(|item| {
                                    item.alias.as_ref().is_some_and(|alias| alias.0 == name)
                                })
                                .cloned()
                                .unwrap_or_else(|| variable(&name, &name, item.span))
                        })
                        .collect()
                };
                rewritten.clauses.push(Clause::Return(Return {
                    distinct: false,
                    body: ReturnBody {
                        star: ret.body.star,
                        items,
                        ..Default::default()
                    },
                    span,
                }));
            } else if !restored.is_empty() {
                rewritten.clauses.insert(
                    i + 1,
                    Clause::With(With {
                        distinct: false,
                        body: ReturnBody {
                            star: true,
                            items: restored,
                            ..Default::default()
                        },
                        where_clause: None,
                        span,
                    }),
                );
            }
            return Some(Self {
                query: rewritten,
                unwind,
                page: i,
                fields,
                edges,
            });
        }
        None
    }
}

fn projected_edges(body: &ReturnBody, edges: &BTreeSet<String>) -> BTreeSet<String> {
    let mut projected = if body.star {
        edges.clone()
    } else {
        BTreeSet::new()
    };
    for item in &body.items {
        let name = output_name(item);
        projected.remove(&name);
        if is_edge(&item.expr, edges) {
            projected.insert(name);
        }
    }
    projected
}

fn variable(input: &str, output: &str, span: opencypher::Span) -> ReturnItem {
    ReturnItem {
        expr: Expr {
            kind: ExprKind::Variable(Variable(input.to_owned())),
            span,
        },
        alias: Some(Variable(output.to_owned())),
        span,
    }
}

fn is_edge(expr: &Expr, edges: &BTreeSet<String>) -> bool {
    matches!(&expr.kind, ExprKind::Variable(var) if edges.contains(&var.0))
}

fn is_identity(expr: &Expr, edges: &BTreeSet<String>) -> bool {
    matches!(&expr.kind, ExprKind::FunctionCall { name, args, distinct: false }
        if matches!(name.0.to_ascii_lowercase().as_str(), "id" | "elementid")
            && matches!(args.as_slice(), [arg] if is_edge(arg, edges)))
}

struct Read<'a> {
    edges: &'a BTreeSet<String>,
    fields: &'a mut BTreeSet<String>,
    full: bool,
}

impl Visitor for Read<'_> {
    fn visit_expr(&mut self, expr: &Expr) {
        match &expr.kind {
            ExprKind::Exists(_) | ExprKind::PatternComprehension { .. } => self.full = true,
            ExprKind::Property { base, key } if is_edge(base, self.edges) => {
                self.fields.insert(key.0.clone());
            }
            ExprKind::FunctionCall { name, args, .. }
                if matches!(
                    name.0.to_ascii_lowercase().as_str(),
                    "id" | "elementid" | "type" | "count"
                ) && matches!(args.as_slice(), [arg] if is_edge(arg, self.edges)) => {}
            _ if is_edge(expr, self.edges) => self.full = true,
            _ => walk_expr(self, expr),
        }
    }
}

struct Used<'a> {
    name: &'a str,
    key: &'a str,
    found: bool,
}

impl Visitor for Used<'_> {
    fn visit_expr(&mut self, expr: &Expr) {
        match &expr.kind {
            ExprKind::Variable(var) if var.0 == self.name => self.found = true,
            ExprKind::Property { key, .. } if key.0 == self.key => self.found = true,
            _ => walk_expr(self, expr),
        }
    }
}
