use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Result, ensure};
use opencypher::{ExprKind, RangeLiteral, parse_expr};

#[derive(Clone, Copy, Debug)]
pub(crate) enum Shortest {
    One,
    All,
}

pub(super) fn bounds(range: Option<RangeLiteral>) -> Result<(usize, usize)> {
    let Some(range) = range else {
        return Ok((1, 1));
    };
    let min = range.min.unwrap_or(1);
    let max = if range.has_range {
        range.max
    } else {
        range.min
    };
    ensure!(
        (0..=256).contains(&min),
        "invalid shortest path lower bound"
    );
    if let Some(max) = max {
        ensure!((min..=256).contains(&max), "invalid shortest path range");
        Ok((min as usize, max as usize))
    } else {
        ensure!(
            min <= 1,
            "shortest paths with a lower bound above one require an upper bound"
        );
        Ok((min as usize, usize::MAX))
    }
}

#[derive(Clone, Default)]
pub(super) struct Extensions {
    pub(super) partitioned_csr: bool,
    pub(super) indexed_csr: bool,
    pub(super) shortest: BTreeMap<usize, Shortest>,
    pub(super) call_imports: std::rc::Rc<BTreeMap<usize, Vec<String>>>,
    pub(super) page_fields: BTreeMap<usize, (String, BTreeSet<String>)>,
    pub(super) page_outputs: BTreeMap<(usize, usize), BTreeSet<String>>,
}

#[derive(Default)]
struct ScopedCalls(BTreeSet<usize>);

impl opencypher::Visitor for ScopedCalls {
    fn visit_clause(&mut self, clause: &opencypher::Clause) {
        if let opencypher::Clause::Call(call) = clause
            && matches!(call.kind, opencypher::CallKind::Subquery(_))
        {
            self.0.insert(call.span.start);
        }
        opencypher::walk_clause(self, clause);
    }
}

pub(super) fn validate_call_scopes(
    query: &opencypher::Query,
    extensions: &Extensions,
) -> Result<()> {
    let mut calls = ScopedCalls::default();
    opencypher::Visitor::visit_query(&mut calls, query);
    ensure!(
        extensions
            .call_imports
            .keys()
            .all(|span| calls.0.contains(span)),
        "CALL scope must precede a subquery"
    );
    Ok(())
}

pub(super) const COUNT_SUBQUERY: &str = "__graphlake_count_subquery";

// The parser already understands both forms of EXISTS. Use its AST for the
// same braced grammar, wrapped in an internal function to retain the result mode.
pub(super) fn rewrite_count_subqueries(query: &str) -> Result<String> {
    let bytes = query.as_bytes();
    let mut output = String::with_capacity(query.len());
    let mut copied = 0;
    let mut cursor = 0;
    let mut braces = Vec::new();
    let mut previous = None;
    while cursor < bytes.len() {
        if let Some(end) = skip_literal(bytes, cursor)? {
            if matches!(bytes[cursor], b'\'' | b'"' | b'`') {
                previous = Some(bytes[cursor]);
            }
            cursor = end;
            continue;
        }
        let byte = bytes[cursor];
        if bytes
            .get(cursor..cursor + 5)
            .is_some_and(|name| name.eq_ignore_ascii_case(b"count"))
            && (cursor == 0 || !word_byte(bytes[cursor - 1]))
            && !matches!(previous, Some(b'.' | b'$'))
            && let open = next_token(bytes, cursor + 5)?
            && bytes.get(open) == Some(&b'{')
            && starts_subquery(bytes, next_token(bytes, open + 1)?)?
        {
            output.push_str(&query[copied..cursor]);
            output.push_str(COUNT_SUBQUERY);
            output.push_str("(EXISTS");
            copied = cursor + 5;
            cursor = open + 1;
            previous = Some(b'{');
            braces.push(true);
            continue;
        }
        if byte == b'{' {
            braces.push(false);
        } else if byte == b'}' && braces.pop() == Some(true) {
            output.push_str(&query[copied..=cursor]);
            output.push(')');
            copied = cursor + 1;
        }
        if !byte.is_ascii_whitespace() {
            previous = Some(byte);
        }
        cursor += 1;
    }
    output.push_str(&query[copied..]);
    Ok(output)
}

fn starts_subquery(bytes: &[u8], start: usize) -> Result<bool> {
    if bytes.get(start) == Some(&b'(') {
        return Ok(true);
    }
    let end = if bytes.get(start) == Some(&b'`') {
        skip_literal(bytes, start)?.expect("quoted identifier")
    } else {
        start
            + bytes[start..]
                .iter()
                .take_while(|&&byte| word_byte(byte))
                .count()
    };
    let next = next_token(bytes, end)?;
    match bytes.get(next) {
        Some(b':') => return Ok(false),
        Some(b'=') if end > start => {
            return Ok(bytes.get(next_token(bytes, next + 1)?) == Some(&b'('));
        }
        _ => {}
    }
    Ok([
        b"match".as_slice(),
        b"optional",
        b"with",
        b"unwind",
        b"return",
        b"call",
        b"create",
        b"merge",
        b"delete",
        b"set",
        b"remove",
        b"foreach",
        b"load",
    ]
    .iter()
    .any(|word| bytes[start..end].eq_ignore_ascii_case(word)))
}

/// Adapt syntax absent from the parser while preserving its source spans.
pub(super) fn parse_wrappers(query: &str) -> Result<(String, Extensions)> {
    let bytes = query.as_bytes();
    let mut text = bytes.to_vec();
    let mut extensions = Extensions::default();
    let mut cursor = 0;
    let mut previous = None;
    while cursor < bytes.len() {
        if let Some(end) = skip_literal(bytes, cursor)? {
            if matches!(bytes[cursor], b'\'' | b'"' | b'`') {
                previous = Some(bytes[cursor]);
            }
            cursor = end;
            continue;
        }
        let start = cursor;
        let qualified = previous == Some(b'.');
        if !bytes[start].is_ascii_whitespace() {
            previous = Some(bytes[start]);
        }
        if bytes[cursor].is_ascii_alphabetic() || bytes[cursor] == b'_' {
            cursor += 1;
            while bytes
                .get(cursor)
                .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
            {
                cursor += 1;
            }
        } else {
            cursor += 1;
            continue;
        }
        let name = &bytes[start..cursor];
        let open = next_token(bytes, cursor)?;
        if qualified || bytes.get(open) != Some(&b'(') {
            continue;
        }
        if name.eq_ignore_ascii_case(b"call") {
            let end = closing_paren(bytes, open)?;
            let brace = next_token(bytes, end)?;
            if bytes.get(brace) == Some(&b'{') {
                let variables = imports(&query[open + 1..end - 1])?;
                text[open..end].fill(b' ');
                std::rc::Rc::make_mut(&mut extensions.call_imports).insert(start, variables);
                cursor = end;
            }
            continue;
        }
        if name.eq_ignore_ascii_case(b"reduce") {
            rewrite_reduce(bytes, &mut text, open)?;
            continue;
        }
        let kind = if name.eq_ignore_ascii_case(b"shortestPath") {
            Shortest::One
        } else if name.eq_ignore_ascii_case(b"allShortestPaths") {
            Shortest::All
        } else {
            continue;
        };
        let pattern = next_token(bytes, open + 1)?;
        ensure!(
            bytes.get(pattern) == Some(&b'('),
            "shortest path expects a graph pattern"
        );
        let end = closing_paren(bytes, open)?;
        text[start..=open].fill(b' ');
        text[end - 1] = b' ';
        extensions.shortest.insert(pattern, kind);
        cursor = end;
    }
    Ok((String::from_utf8(text)?, extensions))
}

// Preserve spans and let the regular expression parser parse all five operands.
fn rewrite_reduce(bytes: &[u8], text: &mut [u8], open: usize) -> Result<()> {
    let mut cursor = open + 1;
    let mut depth = 0;
    let mut separators = 0;
    while cursor < bytes.len() {
        if let Some(end) = skip_literal(bytes, cursor)? {
            cursor = end;
            continue;
        }
        let byte = bytes[cursor];
        if depth == 0 {
            match (separators, byte) {
                (0, b'=') | (3, b'|') => {
                    text[cursor] = b',';
                    separators += 1;
                }
                (1, b',') => separators += 1,
                (2, b'i' | b'I')
                    if bytes
                        .get(cursor + 1)
                        .is_some_and(|b| b.eq_ignore_ascii_case(&b'n'))
                        && !word_byte(bytes[cursor - 1])
                        && !bytes.get(cursor + 2).is_some_and(|b| word_byte(*b)) =>
                {
                    text[cursor..cursor + 2].copy_from_slice(b", ");
                    separators += 1;
                }
                (_, b')') => break,
                _ => {}
            }
        }
        match byte {
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' => depth -= 1,
            _ => {}
        }
        cursor += 1;
    }
    ensure!(
        separators == 4,
        "reduce() expects accumulator = initial, item IN list | expression"
    );
    Ok(())
}

fn word_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_' || !byte.is_ascii()
}

fn imports(text: &str) -> Result<Vec<String>> {
    let ExprKind::List(items) = parse_expr(&format!("[{text}]"))?.kind else {
        anyhow::bail!("expected CALL import list");
    };
    let mut names = std::collections::BTreeSet::new();
    items
        .into_iter()
        .map(|item| {
            let ExprKind::Variable(name) = item.kind else {
                anyhow::bail!("CALL scope requires explicit variable names");
            };
            ensure!(
                names.insert(name.0.clone()),
                "duplicate CALL import {}",
                name.0
            );
            Ok(name.0)
        })
        .collect()
}

fn closing_paren(bytes: &[u8], open: usize) -> Result<usize> {
    let mut depth = 1;
    let mut end = open + 1;
    while depth != 0 && end < bytes.len() {
        if let Some(next) = skip_literal(bytes, end)? {
            end = next;
            continue;
        }
        match bytes[end] {
            b'(' => depth += 1,
            b')' => depth -= 1,
            _ => {}
        }
        end += 1;
    }
    ensure!(depth == 0, "unclosed Cypher parentheses");
    Ok(end)
}

fn next_token(bytes: &[u8], mut cursor: usize) -> Result<usize> {
    while let Some(byte) = bytes.get(cursor) {
        if byte.is_ascii_whitespace() {
            cursor += 1;
        } else if matches!(bytes.get(cursor..cursor + 2), Some(b"//" | b"/*")) {
            cursor = skip_literal(bytes, cursor)?.expect("comment prefix");
        } else {
            break;
        }
    }
    Ok(cursor)
}

pub(crate) fn skip_literal(bytes: &[u8], start: usize) -> Result<Option<usize>> {
    let quote = bytes[start];
    if matches!(quote, b'\'' | b'"' | b'`') {
        let mut index = start + 1;
        while index < bytes.len() {
            let byte = bytes[index];
            index += 1;
            if byte == b'\\' && quote != b'`' {
                index += 1;
            } else if byte == quote {
                if quote == b'`' && bytes.get(index) == Some(&quote) {
                    index += 1;
                } else {
                    return Ok(Some(index));
                }
            }
        }
        anyhow::bail!("unterminated Cypher literal");
    }
    if bytes[start..].starts_with(b"//") {
        return Ok(Some(
            start
                + bytes[start..]
                    .iter()
                    .position(|byte| *byte == b'\n')
                    .unwrap_or(bytes.len() - start),
        ));
    }
    if bytes[start..].starts_with(b"/*") {
        let end = bytes[start + 2..].windows(2).position(|pair| pair == b"*/");
        return Ok(Some(
            start + 4 + end.ok_or_else(|| anyhow::anyhow!("unterminated Cypher comment"))?,
        ));
    }
    Ok(None)
}
