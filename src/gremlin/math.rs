use anyhow::{Result, bail, ensure};
use opencypher::{BinaryOp, Expr, ExprKind, UnaryOp, Visitor, walk_expr};

use super::{Traversers, parse::parse_expr, string, text, values::by_traversal, variants::Kind};

impl Traversers<'_> {
    pub(super) fn math(
        &mut self,
        formula: &str,
        by: Vec<Option<&str>>,
        depth: usize,
    ) -> Result<()> {
        let formula = text(formula)?;
        let expression = parse_expr(&formula)?;
        let mut variables = Variables(Vec::new());
        variables.visit_expr(&expression);
        ensure!(
            by.len() <= variables.0.len(),
            "math() has more by() modulators than variables"
        );
        let scalar = variables.0 == ["_"];
        if scalar && let Some(Some(modulator)) = by.first() {
            if let Some(traversal) = by_traversal(modulator) {
                self.branch("map", &[&traversal], depth)?;
            } else {
                self.modulate(Some(modulator), depth)?;
            }
        } else if !scalar && !variables.0.is_empty() {
            let fields = variables
                .0
                .iter()
                .map(serde_json::to_string)
                .collect::<std::result::Result<Vec<_>, _>>()?;
            self.record_values(
                &fields.iter().map(String::as_str).collect::<Vec<_>>(),
                &by,
                true,
                true,
                depth,
            )?;
        }
        if scalar {
            ensure!(
                self.kinds()
                    .iter()
                    .all(|kind| !matches!(kind, Kind::Element(_))),
                "math() requires numeric values"
            );
        }
        let value = render(&expression, &|name| {
            let value = self.variant_value("v");
            if scalar {
                value
            } else {
                format!("__graphlake_cypher_map_get({value}, {})", string(name))
            }
        })?;
        self.scalar(&value, true, Kind::Double);
        self.variants.clear();
        Ok(())
    }
}

struct Variables(Vec<String>);

impl Visitor for Variables {
    fn visit_expr(&mut self, expr: &Expr) {
        if let ExprKind::Variable(name) = &expr.kind {
            if !self.0.contains(&name.0) {
                self.0.push(name.0.clone());
            }
        } else {
            walk_expr(self, expr);
        }
    }
}

fn render(expr: &Expr, variable: &impl Fn(&str) -> String) -> Result<String> {
    let sub = |expr| render(expr, variable);
    Ok(match &expr.kind {
        ExprKind::Integer(_) | ExprKind::Float(_) => {
            format!("CAST({} AS DOUBLE)", crate::cypher::literal(expr)?)
        }
        ExprKind::Variable(name) => format!(
            "__graphlake_gremlin_number(__graphlake_numeric_input({}))",
            variable(&name.0)
        ),
        ExprKind::Unary {
            op: op @ (UnaryOp::Plus | UnaryOp::Minus),
            operand,
        } => {
            format!("({}{})", op.as_str(), sub(operand)?)
        }
        ExprKind::Binary {
            op: BinaryOp::Pow,
            lhs,
            rhs,
        } => {
            format!("power({}, {})", sub(lhs)?, sub(rhs)?)
        }
        ExprKind::Binary {
            op: op @ (BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod),
            lhs,
            rhs,
        } => format!("({} {} {})", sub(lhs)?, op.as_str(), sub(rhs)?),
        ExprKind::FunctionCall {
            name,
            distinct: false,
            args,
        } => {
            let name = name.0.to_ascii_lowercase();
            ensure!(
                matches!(
                    name.as_str(),
                    "abs"
                        | "acos"
                        | "asin"
                        | "atan"
                        | "cbrt"
                        | "ceil"
                        | "cos"
                        | "cosh"
                        | "exp"
                        | "floor"
                        | "ln"
                        | "log10"
                        | "log2"
                        | "signum"
                        | "sin"
                        | "sinh"
                        | "sqrt"
                        | "tan"
                        | "tanh"
                ),
                "unsupported math() function {name}"
            );
            let [arg] = args.as_slice() else {
                bail!("math() function {name} expects one argument");
            };
            format!("{name}({})", sub(arg)?)
        }
        _ => bail!("unsupported math() expression"),
    })
}
