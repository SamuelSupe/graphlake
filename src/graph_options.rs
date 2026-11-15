use anyhow::{Context, Result, bail, ensure};
use opencypher::{Expr, ExprKind, parse_expr};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{Comparison, Predicate};

/// Business filters and snapshot requirements for a query, traversal, or refresh.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GraphOptions {
    #[serde(default)]
    pub logical_partition: Vec<Predicate>,
    pub snapshot_time: Option<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub require_consistent_snapshot: bool,
}

impl GraphOptions {
    /// Parse graph options from a JSON object and validate its predicates.
    pub fn from_json(input: &str) -> Result<Self> {
        let options: Self = serde_json::from_str(input)?;
        options.validate()?;
        Ok(options)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        for predicate in &self.logical_partition {
            predicate.validate()?;
        }
        if let Some(value) = &self.snapshot_time {
            ensure!(!value.is_empty(), "snapshot_time cannot be empty");
        }
        Ok(())
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.logical_partition.is_empty() && self.snapshot_time.is_none()
    }
}

pub(crate) fn using<'a>(text: &'a str, options: &mut GraphOptions) -> Result<&'a str> {
    let Some((keyword, _)) = text
        .trim_start()
        .split_once(|ch: char| ch.is_whitespace() || ch == '{')
    else {
        return Ok(text);
    };
    if !keyword.eq_ignore_ascii_case("using") {
        return Ok(text);
    }
    let rest = text.trim_start()[keyword.len()..].trim_start();
    ensure!(rest.starts_with('{'), "expected USING {{...}}");
    let end = map_end(rest)?;
    ensure!(end <= 8 * 1024, "USING options exceed 8 KiB");
    crate::cypher::check_expression_complexity(&rest[..end], "USING options")?;
    let ExprKind::Map(items) = parse_expr(&rest[..end])?.kind else {
        bail!("expected USING options map");
    };
    for (key, value) in items {
        match key.0.as_str() {
            "logicalPartition" => {
                ensure!(
                    options.logical_partition.is_empty(),
                    "logical partition already set"
                );
                options.logical_partition = partition(value)?;
            }
            "SnapShotTime" => {
                ensure!(options.snapshot_time.is_none(), "snapshot time already set");
                options.snapshot_time = Some(match value.kind {
                    ExprKind::String(value) => value,
                    ExprKind::Integer(value) => value.to_string(),
                    _ => bail!("SnapShotTime requires a date, timestamp or 'current'"),
                });
            }
            "requireConsistentSnapshot" => match value.kind {
                ExprKind::Boolean(true) => options.require_consistent_snapshot = true,
                ExprKind::Boolean(false) => {}
                _ => bail!("requireConsistentSnapshot requires a boolean"),
            },
            _ => bail!("unsupported USING option {}", key.0),
        }
    }
    Ok(rest[end..].trim_start())
}

fn map_end(text: &str) -> Result<usize> {
    let mut depth = 0_u32;
    let mut quote = None;
    let mut escaped = false;
    for (index, ch) in text.char_indices() {
        if escaped {
            escaped = false;
        } else if let Some(delimiter) = quote {
            if ch == '\\' && delimiter != '`' {
                escaped = true;
            } else if ch == delimiter {
                quote = None;
            }
        } else {
            match ch {
                '\'' | '"' | '`' => quote = Some(ch),
                '{' | '[' | '(' => {
                    depth += 1;
                    ensure!(depth <= 32, "USING nesting exceeds 32");
                }
                '}' | ']' | ')' => {
                    depth -= 1;
                    if depth == 0 {
                        return Ok(index + 1);
                    }
                }
                _ => {}
            }
        }
    }
    bail!("unterminated USING options map")
}

fn partition(value: Expr) -> Result<Vec<Predicate>> {
    match value.kind {
        ExprKind::Map(items) => items
            .into_iter()
            .map(|(key, value)| {
                Ok(Predicate {
                    column: key.0,
                    op: Comparison::Eq,
                    value: scalar(&value)?,
                })
            })
            .collect(),
        ExprKind::List(terms) => terms
            .into_iter()
            .map(|term| {
                let (column, op, value) = match term.kind {
                    ExprKind::Comparison { first, rest } => {
                        let [(operator, value)] = rest.as_slice() else {
                            bail!("partition conditions require one comparison");
                        };
                        let op = match operator.as_str() {
                            "=" => Comparison::Eq,
                            "<>" => Comparison::Ne,
                            "<" => Comparison::Lt,
                            "<=" => Comparison::Le,
                            ">" => Comparison::Gt,
                            ">=" => Comparison::Ge,
                            _ => bail!("unsupported partition comparison"),
                        };
                        (first, op, scalar(value)?)
                    }
                    ExprKind::IsNull(value) => (value, Comparison::IsNull, Value::Null),
                    ExprKind::IsNotNull(value) => (value, Comparison::IsNotNull, Value::Null),
                    _ => bail!("expected a logical partition comparison"),
                };
                let ExprKind::Variable(column) = column.kind else {
                    bail!("partition conditions require a field name");
                };
                Ok(Predicate {
                    column: column.0,
                    op,
                    value,
                })
            })
            .collect(),
        _ => bail!("logicalPartition requires a map or list of comparisons"),
    }
}

fn scalar(value: &Expr) -> Result<Value> {
    match &value.kind {
        ExprKind::Boolean(value) => Ok((*value).into()),
        ExprKind::Integer(value) => Ok((*value).into()),
        ExprKind::Float(value) => Ok(Value::Number(
            serde_json::Number::from_f64(*value).context("invalid numeric partition value")?,
        )),
        ExprKind::String(value) => Ok(value.clone().into()),
        ExprKind::Unary {
            op: opencypher::UnaryOp::Minus,
            operand,
        } => {
            let value = scalar(operand)?;
            if let Some(value) = value.as_i64() {
                return Ok(value
                    .checked_neg()
                    .context("integer partition value out of range")?
                    .into());
            }
            Ok(Value::Number(
                serde_json::Number::from_f64(
                    -value
                        .as_f64()
                        .context("expected a numeric partition value")?,
                )
                .context("invalid numeric partition value")?,
            ))
        }
        _ => bail!("expected a scalar partition value"),
    }
}
