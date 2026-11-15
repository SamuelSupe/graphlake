use std::collections::{BTreeMap, BTreeSet};

use opencypher::{
    BinaryOp, Call, CallKind, Clause, ComparisonOp, Direction, Expr, ExprKind, LabelExpr, Match,
    NodePattern, PatternElement, PatternPart, Properties, PropertyKey, Query, Return, ReturnBody,
    ReturnItem, SingleQuery, SortItem, Span, Variable, Visitor, With, walk_expr,
};

use super::{HasAggregate, paths::Extensions};
use crate::Catalog;

type Fields = BTreeMap<String, Payload>;

#[derive(Clone, PartialEq, Eq)]
enum Payload {
    Field(String),
    Entity,
}
type Keys = BTreeMap<String, Variable>;

/// Only properties whose values can be recovered after pagination travel as
/// business keys. Every branch of a UNION ALL must agree on the deferred field.
pub(super) fn defer_properties(catalog: &Catalog, query: &mut Query, extensions: &mut Extensions) {
    if !query.unions.is_empty() {
        return;
    }
    let text = format!("{query:?}");
    let span = Span::empty(query.span.end + 1);
    let clauses = &mut query.initial.clauses;
    let [prefix @ .., Clause::Call(call), Clause::Return(ret)] = clauses.as_mut_slice() else {
        return;
    };
    if prefix.is_empty()
        || !projectable(ret)
        || matches!(&ret.body.limit, Some(limit) if matches!(limit.kind, ExprKind::Integer(0)))
    {
        return;
    }
    let Some(mut plan) = describe(catalog, call, extensions) else {
        return;
    };
    let outer_page =
        has_page(&ret.body) && (ret.body.skip.is_some() || !ret.body.order_by.is_empty());
    if !(outer_page || plan.paged) {
        return;
    }
    let mut order = ret.body.order_by.clone();
    if outer_page {
        for sort in &mut order {
            if let ExprKind::Variable(var) = &sort.expr.kind {
                if let Some(item) = ret
                    .body
                    .items
                    .iter()
                    .find(|item| item.alias.as_ref() == Some(var))
                {
                    sort.expr = item.expr.clone();
                }
            } else if ret
                .body
                .items
                .iter()
                .filter_map(|item| item.alias.as_ref())
                .any(|alias| references(&sort.expr, &alias.0))
            {
                return;
            }
        }
        plan.fields
            .retain(|name, _| !order.iter().any(|sort| references(&sort.expr, name)));
    }
    if plan.fields.is_empty() {
        return;
    }
    let internal = |stem: &str, index: usize| {
        let mut name = format!("__graphlake_page_{stem}_{index}_{}", call.span.start);
        while text.contains(&name)
            || catalog.nodes.contains_key(&name)
            || catalog.edges.contains_key(&name)
        {
            name.push('_');
        }
        Variable(name)
    };
    let keys = catalog.nodes[&plan.label]
        .key
        .iter()
        .enumerate()
        .map(|(index, key)| (key.clone(), internal("key", index)))
        .collect::<Keys>();
    if keys.is_empty() {
        return;
    }
    let fetch = fetch_page(
        &plan.label,
        internal("node", 0),
        &keys,
        &plan.fields,
        plan.nullable,
        span,
    );
    let fields = plan.fields.clone();
    let needed = output_dependencies(ret, &order, &fields);
    plan.rewrite(call, &keys, &fields, needed, span, extensions);
    std::rc::Rc::make_mut(&mut extensions.call_imports)
        .insert(span.start, keys.values().map(|key| key.0.clone()).collect());
    let page = outer_page.then(|| {
        Clause::With(With {
            distinct: false,
            body: ReturnBody {
                star: true,
                order_by: order,
                skip: ret.body.skip.take(),
                limit: ret.body.limit.take(),
                ..Default::default()
            },
            where_clause: None,
            span,
        })
    });
    let insert = clauses.len() - 1;
    clauses.splice(insert..insert, page.into_iter().chain([fetch]));
}

fn output_dependencies(ret: &Return, order: &[SortItem], fields: &Fields) -> BTreeSet<String> {
    let mut needed = Variables(fields.keys().cloned().collect());
    for item in &ret.body.items {
        needed.visit_expr(&item.expr);
    }
    visit_order(&mut needed, order);
    needed.0
}

struct QueryPlan {
    label: String,
    fields: Fields,
    paged: bool,
    nullable: bool,
    branches: Vec<Branch>,
}

enum Branch {
    Match {
        start: usize,
        source: String,
        target: Variable,
        required: BTreeSet<String>,
        nullable: bool,
    },
    Call {
        index: usize,
        plan: Box<QueryPlan>,
    },
}

impl Branch {
    fn nullable(&self) -> bool {
        match self {
            Self::Match { nullable, .. } => *nullable,
            Self::Call { plan, .. } => plan.nullable,
        }
    }
}

fn describe(catalog: &Catalog, call: &Call, extensions: &Extensions) -> Option<QueryPlan> {
    let CallKind::Subquery(query) = &call.kind else {
        return None;
    };
    let imports = extensions.call_imports.get(&call.span.start);
    if imports.is_some_and(Vec::is_empty) || query.unions.iter().any(|branch| !branch.all) {
        return None;
    }
    let mut branches = std::iter::once(&query.initial)
        .chain(query.unions.iter().map(|branch| &branch.query))
        .map(|branch| describe_branch(catalog, branch, extensions, imports.map(Vec::as_slice)));
    let (branch, label, mut fields, mut paged) = branches.next()??;
    let mut nullable = branch.nullable();
    let mut result = vec![branch];
    for branch in branches {
        let (branch, other_label, other_fields, other_paged) = branch?;
        if label != other_label {
            return None;
        }
        fields.retain(|name, field| other_fields.get(name) == Some(field));
        paged &= other_paged;
        nullable |= branch.nullable();
        result.push(branch);
    }
    (!fields.is_empty()).then_some(QueryPlan {
        label,
        fields,
        paged,
        nullable,
        branches: result,
    })
}

fn describe_branch(
    catalog: &Catalog,
    query: &SingleQuery,
    extensions: &Extensions,
    imports: Option<&[String]>,
) -> Option<(Branch, String, Fields, bool)> {
    let [body @ .., Clause::Return(ret)] = query.clauses.as_slice() else {
        return None;
    };
    if !projectable(ret) {
        return None;
    }
    let ret_paged = has_page(&ret.body) && !ret.body.order_by.is_empty();
    let offset = usize::from(imports.is_none());
    if imports.is_none() {
        let Some(Clause::With(with)) = body.first() else {
            return None;
        };
        if with.distinct
            || with.body.star
            || with.where_clause.is_some()
            || !with.body.order_by.is_empty()
            || with.body.skip.is_some()
            || with.body.limit.is_some()
            || with.body.items.is_empty()
            || !with
                .body
                .items
                .iter()
                .all(|item| item.alias.is_none() && matches!(item.expr.kind, ExprKind::Variable(_)))
        {
            return None;
        }
    }
    let body = &body[offset..];
    if let [prefix @ .., Clause::Call(call)] = body {
        let plan = describe(catalog, call, extensions)?;
        let mut fields = Fields::new();
        for item in &ret.body.items {
            if let ExprKind::Variable(name) = &item.expr.kind
                && let Some(field) = plan.fields.get(&name.0)
                && ret
                    .body
                    .items
                    .iter()
                    .filter(|other| references(&other.expr, &name.0))
                    .count()
                    == 1
                && !ret.body.order_by.iter().any(|sort| {
                    references(&sort.expr, &name.0) || references(&sort.expr, &output_name(item))
                })
            {
                fields.insert(output_name(item), field.clone());
            }
        }
        let label = plan.label.clone();
        let paged = ret_paged || plan.paged;
        return Some((
            Branch::Call {
                index: prefix.len() + offset,
                plan: Box::new(plan),
            },
            label,
            fields,
            paged,
        ));
    }
    let [Clause::Match(pattern), middle @ ..] = body else {
        return None;
    };
    let (label, target) = endpoint(catalog, pattern, extensions)?;
    let initial = target.variable.as_ref()?;
    if imports.is_some_and(|names| names.contains(&initial.0)) {
        return None;
    }
    let mut active = initial.clone();
    let mut required = read_properties(&active, BTreeSet::new());
    if let Some(Properties::Map(fields)) = &target.properties {
        required
            .fields
            .extend(fields.iter().map(|(name, _)| name.0.clone()));
    }
    for part in &pattern.patterns {
        required.visit_pattern_part(part);
    }
    if let Some(predicate) = &pattern.where_clause {
        required.visit_expr(predicate);
    }
    let mut paged = ret_paged;
    for clause in middle {
        let Clause::With(with) = clause else {
            return None;
        };
        let mut next = with.body.star.then(|| active.clone());
        for item in &with.body.items {
            if matches!(&item.expr.kind, ExprKind::Variable(var) if var == &active) {
                if next
                    .replace(item.alias.clone().unwrap_or_else(|| active.clone()))
                    .is_some()
                {
                    return None;
                }
            } else {
                required.visit_expr(&item.expr);
            }
        }
        visit_order(&mut required, &with.body.order_by);
        if let Some(predicate) = &with.where_clause {
            required.visit_expr(predicate);
        }
        if required.full {
            return None;
        }
        let fields = required.fields;
        active = next?;
        required = read_properties(&active, fields);
        visit_order(&mut required, &with.body.order_by);
        if let Some(predicate) = &with.where_clause {
            required.visit_expr(predicate);
        }
        paged |= has_page(&with.body) && !with.body.order_by.is_empty();
    }
    for item in &ret.body.items {
        if property_of(&item.expr, &active).is_none() && !is_entity(&item.expr, &active) {
            required.visit_expr(&item.expr);
        }
    }
    visit_order(&mut required, &ret.body.order_by);
    if required.full {
        return None;
    }
    let node = &catalog.nodes[label];
    let fields = ret
        .body
        .items
        .iter()
        .filter_map(|item| {
            let name = output_name(item);
            if ret
                .body
                .order_by
                .iter()
                .any(|sort| references(&sort.expr, &name))
            {
                return None;
            }
            if is_entity(&item.expr, &active) {
                return Some((name, Payload::Entity));
            }
            let field = property_of(&item.expr, &active)?;
            (node.properties.contains(&field.0)
                && !node.key.contains(&field.0)
                && !required.fields.contains(&field.0))
            .then(|| (name, Payload::Field(field.0.clone())))
        })
        .collect();
    let required = required.fields;
    Some((
        Branch::Match {
            start: target.span.start,
            source: initial.0.clone(),
            target: active,
            required,
            nullable: pattern.optional,
        },
        label.to_owned(),
        fields,
        paged,
    ))
}

impl QueryPlan {
    fn rewrite(
        self,
        call: &mut Call,
        keys: &Keys,
        fields: &Fields,
        needed: BTreeSet<String>,
        span: Span,
        extensions: &mut Extensions,
    ) {
        let CallKind::Subquery(query) = &mut call.kind else {
            unreachable!("described CALL")
        };
        extensions
            .page_outputs
            .insert((query.span.start, query.span.end), needed);
        for (branch, plan) in std::iter::once(&mut query.initial)
            .chain(query.unions.iter_mut().map(|branch| &mut branch.query))
            .zip(self.branches)
        {
            let [body @ .., Clause::Return(ret)] = branch.clauses.as_mut_slice() else {
                unreachable!("described RETURN");
            };
            let target = match plan {
                Branch::Match {
                    start,
                    source,
                    target,
                    mut required,
                    ..
                } => {
                    let mut full = false;
                    for item in &ret.body.items {
                        if fields.contains_key(&output_name(item)) {
                            continue;
                        }
                        if let Some(field) = property_of(&item.expr, &target) {
                            required.insert(field.0.clone());
                        }
                        full |= is_entity(&item.expr, &target);
                    }
                    if !full {
                        extensions.page_fields.insert(start, (source, required));
                    }
                    Some(target)
                }
                Branch::Call { index, plan } => {
                    let Clause::Call(nested) = &mut body[index] else {
                        unreachable!("described nested CALL")
                    };
                    let nested_fields = ret
                        .body
                        .items
                        .iter()
                        .filter_map(|item| {
                            if let ExprKind::Variable(name) = &item.expr.kind
                                && let Some(field) = fields.get(&output_name(item))
                            {
                                Some((name.0.clone(), field.clone()))
                            } else {
                                None
                            }
                        })
                        .collect();
                    let needed = output_dependencies(ret, &ret.body.order_by, &nested_fields);
                    plan.rewrite(nested, keys, &nested_fields, needed, span, extensions);
                    None
                }
            };
            ret.body
                .items
                .retain(|item| !fields.contains_key(&output_name(item)));
            ret.body
                .items
                .extend(keys.iter().map(|(field, key)| ReturnItem {
                    expr: target.as_ref().map_or_else(
                        || variable(key.clone(), span),
                        |target| property(target, field, span),
                    ),
                    alias: Some(key.clone()),
                    span,
                }));
        }
    }
}

fn projectable(ret: &Return) -> bool {
    !ret.distinct
        && !ret.body.star
        && !aggregated(&ret.body)
        && ret
            .body
            .items
            .iter()
            .map(output_name)
            .collect::<BTreeSet<_>>()
            .len()
            == ret.body.items.len()
}

fn output_name(item: &ReturnItem) -> String {
    item.alias
        .as_ref()
        .map_or_else(|| item.expr.to_string(), |name| name.0.clone())
}

fn read_properties(variable: &Variable, fields: BTreeSet<String>) -> PropertiesRead<'_> {
    PropertiesRead {
        variable: &variable.0,
        fields,
        full: false,
        identity_only: true,
    }
}

fn fetch_page(
    label: &str,
    read: Variable,
    keys: &Keys,
    deferred: &Fields,
    nullable: bool,
    span: Span,
) -> Clause {
    let predicates = keys
        .iter()
        .map(|(field, key)| Expr {
            kind: ExprKind::Comparison {
                first: Box::new(property(&read, field, span)),
                rest: vec![(ComparisonOp::Eq, variable(key.clone(), span))],
            },
            span,
        })
        .reduce(|lhs, rhs| Expr {
            kind: ExprKind::Binary {
                op: BinaryOp::And,
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
            },
            span,
        });
    Clause::Call(Call {
        kind: CallKind::Subquery(Box::new(Query {
            initial: SingleQuery {
                clauses: vec![
                    Clause::Match(Match {
                        optional: nullable,
                        patterns: vec![PatternPart {
                            variable: None,
                            element: PatternElement {
                                start: NodePattern {
                                    variable: Some(read.clone()),
                                    labels: Some(LabelExpr::Leaf(label.to_owned())),
                                    properties: None,
                                    span,
                                },
                                chain: Vec::new(),
                                span,
                            },
                            span,
                        }],
                        where_clause: predicates,
                        span,
                    }),
                    Clause::Return(Return {
                        distinct: false,
                        body: ReturnBody {
                            items: deferred
                                .iter()
                                .map(|(name, field)| ReturnItem {
                                    expr: match field {
                                        Payload::Field(field) => property(&read, field, span),
                                        Payload::Entity => variable(read.clone(), span),
                                    },
                                    alias: Some(Variable(name.clone())),
                                    span,
                                })
                                .collect(),
                            ..Default::default()
                        },
                        span,
                    }),
                ],
                span,
            },
            unions: Vec::new(),
            span,
        })),
        span,
    })
}

fn endpoint<'a>(
    catalog: &'a Catalog,
    pattern: &'a Match,
    extensions: &Extensions,
) -> Option<(&'a str, &'a NodePattern)> {
    let [part] = pattern.patterns.as_slice() else {
        return None;
    };
    let (last, target) = part.element.chain.last()?;
    let name = target.variable.as_ref()?;
    if part.variable.is_some()
        || extensions.shortest.contains_key(&part.element.span.start)
        || part
            .element
            .chain
            .iter()
            .any(|(rel, _)| rel.range.is_some() || rel.variable.as_ref() == Some(name))
        || std::iter::once(&part.element.start)
            .chain(part.element.chain.iter().map(|(_, node)| node))
            .filter(|node| node.variable.as_ref() == Some(name))
            .count()
            != 1
    {
        return None;
    }
    let Some(LabelExpr::Leaf(edge)) = &last.types else {
        return None;
    };
    let edge = catalog.edges.get(edge)?;
    let label = match last.direction {
        Direction::Outgoing => &edge.to.node,
        Direction::Incoming => &edge.from.node,
        _ => return None,
    };
    if target
        .labels
        .as_ref()
        .is_some_and(|expr| !matches!(expr, LabelExpr::Leaf(name) if name == label))
    {
        return None;
    }
    Some((label, target))
}

fn has_page(body: &ReturnBody) -> bool {
    body.skip
        .as_ref()
        .is_none_or(|expr| matches!(expr.kind, ExprKind::Integer(n) if n >= 0))
        && matches!(&body.limit, Some(expr) if matches!(expr.kind, ExprKind::Integer(n) if n > 0))
}

fn aggregated(body: &ReturnBody) -> bool {
    let mut found = HasAggregate(false);
    for item in &body.items {
        found.visit_expr(&item.expr);
    }
    visit_order(&mut found, &body.order_by);
    found.0
}

fn visit_order(visitor: &mut impl Visitor, order: &[SortItem]) {
    for sort in order {
        visitor.visit_expr(&sort.expr);
    }
}

fn is_entity(expr: &Expr, target: &Variable) -> bool {
    matches!(&expr.kind, ExprKind::Variable(name) if name == target)
}

fn property_of<'a>(expr: &'a Expr, variable: &Variable) -> Option<&'a PropertyKey> {
    match &expr.kind {
        ExprKind::Property { base, key } if matches!(&base.kind, ExprKind::Variable(name) if name == variable) => {
            Some(key)
        }
        _ => None,
    }
}

fn variable(name: Variable, span: Span) -> Expr {
    Expr {
        kind: ExprKind::Variable(name),
        span,
    }
}

fn property(node: &Variable, field: &str, span: Span) -> Expr {
    Expr {
        kind: ExprKind::Property {
            base: Box::new(variable(node.clone(), span)),
            key: PropertyKey(field.to_owned()),
        },
        span,
    }
}

struct PropertiesRead<'a> {
    variable: &'a str,
    fields: BTreeSet<String>,
    full: bool,
    identity_only: bool,
}

impl Visitor for PropertiesRead<'_> {
    fn visit_expr(&mut self, expr: &Expr) {
        match &expr.kind {
            ExprKind::Property { base, key } if matches!(&base.kind, ExprKind::Variable(var) if var.0 == self.variable) =>
            {
                self.fields.insert(key.0.clone());
            }
            ExprKind::FunctionCall { name, args, .. }
                if self.identity_only
                    && matches!(
                        name.0.to_ascii_lowercase().as_str(),
                        "id" | "elementid" | "labels" | "count"
                    )
                    && matches!(args.as_slice(), [arg] if matches!(&arg.kind, ExprKind::Variable(var) if var.0 == self.variable)) =>
                {}
            ExprKind::Variable(var) if var.0 == self.variable => self.full = true,
            _ => walk_expr(self, expr),
        }
    }
}

fn references(expr: &Expr, name: &str) -> bool {
    let mut visitor = PropertiesRead {
        variable: name,
        fields: BTreeSet::new(),
        full: false,
        identity_only: false,
    };
    visitor.visit_expr(expr);
    visitor.full || !visitor.fields.is_empty()
}

pub(super) fn referenced_names(expr: &Expr) -> BTreeSet<String> {
    let mut visitor = Variables(BTreeSet::new());
    visitor.visit_expr(expr);
    visitor.0
}

struct Variables(BTreeSet<String>);

impl Visitor for Variables {
    fn visit_expr(&mut self, expr: &Expr) {
        if let ExprKind::Variable(var) = &expr.kind {
            self.0.insert(var.0.clone());
        }
        walk_expr(self, expr);
    }

    fn visit_node_pattern(&mut self, node: &NodePattern) {
        if let Some(var) = &node.variable {
            self.0.insert(var.0.clone());
        }
        opencypher::walk_node_pattern(self, node);
    }

    fn visit_relationship_pattern(&mut self, rel: &opencypher::RelationshipPattern) {
        if let Some(var) = &rel.variable {
            self.0.insert(var.0.clone());
        }
        opencypher::walk_relationship_pattern(self, rel);
    }
}
