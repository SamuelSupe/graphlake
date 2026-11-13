use std::collections::BTreeMap;

use anyhow::{Context, Result, ensure};
use serde_json::Value;

use super::paths::skip_literal;

pub(crate) fn bind_parameters(text: &str, values: &BTreeMap<String, Value>) -> Result<String> {
    let bytes = text.as_bytes();
    let mut result = String::with_capacity(text.len());
    let mut copied = 0;
    let mut cursor = 0;
    while cursor < bytes.len() {
        if let Some(end) = skip_literal(bytes, cursor)? {
            cursor = end;
            continue;
        }
        if bytes[cursor] != b'$' {
            cursor += 1;
            continue;
        }
        let start = cursor + 1;
        let numeric = bytes.get(start).is_some_and(u8::is_ascii_digit);
        let end = start
            + bytes[start..]
                .iter()
                .take_while(|byte| {
                    if numeric {
                        byte.is_ascii_digit()
                    } else {
                        byte.is_ascii_alphanumeric() || **byte == b'_'
                    }
                })
                .count();
        ensure!(end > start, "invalid Cypher parameter");
        let name = &text[start..end];
        let value = values
            .get(name)
            .with_context(|| format!("missing Cypher parameter ${name}"))?;
        result.push_str(&text[copied..cursor]);
        // Serialize a literal in the Cypher grammar. Parameter values never supply tokens.
        result.push_str(&literal(value, 0)?);
        ensure!(result.len() <= 64 * 1024, "bound Cypher exceeds 64 KiB");
        copied = end;
        cursor = end;
    }
    result.push_str(&text[copied..]);
    ensure!(result.len() <= 64 * 1024, "bound Cypher exceeds 64 KiB");
    Ok(result)
}

fn literal(value: &Value, depth: usize) -> Result<String> {
    ensure!(depth <= 32, "Cypher parameter nesting exceeds 32");
    match value {
        Value::Array(items) => {
            let items = items
                .iter()
                .map(|item| literal(item, depth + 1))
                .collect::<Result<Vec<_>>>()?;
            Ok(format!("[{}]", items.join(", ")))
        }
        Value::Object(items) => {
            let fields = items
                .iter()
                .map(|(key, value)| {
                    Ok(format!(
                        "`{}`: {}",
                        key.replace('`', "``"),
                        literal(value, depth + 1)?
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(format!("{{{}}}", fields.join(", ")))
        }
        Value::Number(number) => {
            ensure!(
                number.is_i64() || number.is_f64(),
                "Cypher integer parameters require signed 64-bit values"
            );
            Ok(number.to_string())
        }
        _ => Ok(serde_json::to_string(value)?),
    }
}
