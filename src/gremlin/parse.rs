use anyhow::{Context, Result, bail, ensure};
use opencypher::{ExprKind, parse_expr};

use crate::cypher::paths::skip_literal;

#[derive(Clone)]
pub(super) struct Step<'a> {
    pub(super) name: &'a str,
    pub(super) args: Vec<&'a str>,
}

pub(super) fn text(arg: &str) -> Result<String> {
    let ExprKind::String(value) = parse_expr(arg)?.kind else {
        bail!("Gremlin expects a string argument");
    };
    Ok(value)
}

pub(super) fn integer(arg: &str) -> Result<u64> {
    let ExprKind::Integer(value) = parse_expr(arg)?.kind else {
        bail!("Gremlin expects a nonnegative integer");
    };
    u64::try_from(value).context("Gremlin expects a nonnegative integer")
}

pub(super) fn range_bounds(low: &str, high: &str) -> Result<(u64, Option<u64>)> {
    let low = integer(low)?;
    let high = (high != "-1").then(|| integer(high)).transpose()?;
    ensure!(
        high.is_none_or(|high| high >= low),
        "range() requires low <= high"
    );
    Ok((low, high))
}

pub(super) fn parse_chain<'a>(input: &'a str, prefix: &str) -> Result<Vec<Step<'a>>> {
    let mut rest = input
        .trim()
        .strip_prefix(prefix)
        .context("invalid Gremlin traversal source")?
        .trim_start();
    let mut steps = Vec::new();
    while !rest.is_empty() {
        rest = rest
            .strip_prefix('.')
            .context("expected Gremlin step")?
            .trim_start();
        let end = rest
            .find(|ch: char| !ch.is_ascii_alphanumeric() && ch != '_')
            .context("expected step arguments")?;
        let name = &rest[..end];
        rest = rest[end..]
            .trim_start()
            .strip_prefix('(')
            .context("expected step arguments")?;
        let (args, tail) = arguments(rest)?;
        steps.push(Step { name, args });
        ensure!(steps.len() <= 256, "Gremlin exceeds 256 steps");
        rest = tail.trim_start();
    }
    Ok(steps)
}

fn arguments(input: &str) -> Result<(Vec<&str>, &str)> {
    let bytes = input.as_bytes();
    let mut cursor = 0;
    let mut start = 0;
    let mut depth = 0;
    let mut args = Vec::new();
    while cursor < bytes.len() {
        if let Some(end) = skip_literal(bytes, cursor)? {
            cursor = end;
            continue;
        }
        match bytes[cursor] {
            b'(' | b'[' | b'{' => {
                depth += 1;
                ensure!(depth <= 64, "Gremlin argument nesting exceeds 64");
            }
            b')' if depth == 0 => {
                let value = input[start..cursor].trim();
                if !value.is_empty() {
                    args.push(value);
                } else {
                    ensure!(args.is_empty(), "empty Gremlin argument");
                }
                return Ok((args, &input[cursor + 1..]));
            }
            b')' | b']' | b'}' => {
                ensure!(depth > 0, "unbalanced Gremlin arguments");
                depth -= 1;
            }
            b',' if depth == 0 => {
                let value = input[start..cursor].trim();
                ensure!(!value.is_empty(), "empty Gremlin argument");
                args.push(value);
                start = cursor + 1;
            }
            _ => {}
        }
        cursor += 1;
    }
    bail!("unterminated Gremlin arguments")
}
