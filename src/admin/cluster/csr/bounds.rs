use std::collections::BTreeMap;

use anyhow::Result;
use bigdecimal::BigDecimal;
use datafusion::{
    arrow::{
        array::{ArrayRef, AsArray, RecordBatch},
        compute::{max, min},
        datatypes::*,
    },
    common::ExprSchema,
    logical_expr::{Expr, Operator},
};

pub(super) type Ranges = BTreeMap<String, Option<[BigDecimal; 2]>>;

pub(super) fn supported(ty: &DataType) -> bool {
    ty.is_integer()
        || matches!(
            ty,
            DataType::Decimal32(..)
                | DataType::Decimal64(..)
                | DataType::Decimal128(..)
                | DataType::Decimal256(..)
        )
}

pub(super) fn merge(ranges: &mut Ranges, batch: &RecordBatch, columns: &[usize]) -> Result<()> {
    for &column in columns {
        let name = batch.schema_ref().field(column).name();
        let range = ranges.entry(name.clone()).or_default();
        if let Some([lo, hi]) = extrema(batch.column(column))? {
            if let Some([min, max]) = range {
                if lo < *min {
                    *min = lo;
                }
                if hi > *max {
                    *max = hi;
                }
            } else {
                *range = Some([lo, hi]);
            }
        }
    }
    Ok(())
}

pub(super) fn finish(ranges: &mut Ranges) -> BTreeMap<String, Option<[String; 2]>> {
    std::mem::take(ranges)
        .into_iter()
        .map(|(name, range)| (name, range.map(|[lo, hi]| [lo.to_string(), hi.to_string()])))
        .collect()
}

fn extrema(array: &ArrayRef) -> Result<Option<[BigDecimal; 2]>> {
    macro_rules! primitive {
        ($ty:ty, $convert:expr) => {{
            let a = array.as_primitive::<$ty>();
            min(a)
                .zip(max(a))
                .map(|(lo, hi)| [$convert(lo), $convert(hi)])
        }};
    }
    Ok(match array.data_type() {
        DataType::Int8 => primitive!(Int8Type, BigDecimal::from),
        DataType::Int16 => primitive!(Int16Type, BigDecimal::from),
        DataType::Int32 => primitive!(Int32Type, BigDecimal::from),
        DataType::Int64 => primitive!(Int64Type, BigDecimal::from),
        DataType::UInt8 => primitive!(UInt8Type, BigDecimal::from),
        DataType::UInt16 => primitive!(UInt16Type, BigDecimal::from),
        DataType::UInt32 => primitive!(UInt32Type, BigDecimal::from),
        DataType::UInt64 => primitive!(UInt64Type, BigDecimal::from),
        DataType::Decimal32(_, s) => {
            primitive!(Decimal32Type, |n| BigDecimal::from((n, i64::from(*s))))
        }
        DataType::Decimal64(_, s) => {
            primitive!(Decimal64Type, |n| BigDecimal::from((n, i64::from(*s))))
        }
        DataType::Decimal128(_, s) => {
            primitive!(Decimal128Type, |n| BigDecimal::from((n, i64::from(*s))))
        }
        DataType::Decimal256(_, s) => primitive!(Decimal256Type, |n: i256| BigDecimal::from((
            bigdecimal::num_bigint::BigInt::from_signed_bytes_le(&n.to_le_bytes()),
            i64::from(*s)
        ))),
        _ => anyhow::bail!("unsupported CSR numeric statistics"),
    })
}

// Unknown expressions, types, and older blocks use the original exact filter.
// Float literals use the same conversion as mixed numeric Cypher equality.
pub(super) fn equalities<'a>(expr: &'a Expr, out: &mut Vec<(&'a str, BigDecimal)>) {
    if let Expr::BinaryExpr(binary) = expr
        && binary.op == Operator::And
    {
        equalities(&binary.left, out);
        equalities(&binary.right, out);
    }
    if let Expr::ScalarFunction(function) = expr
        && function.name() == "__graphlake_cypher_compare"
        && let [Expr::Column(column), value, Expr::Literal(operator, _)] = function.args.as_slice()
        && operator.try_as_str().flatten() == Some("=")
    {
        let number = match value {
            Expr::Literal(value, _) => crate::scalar::finite_decimal(value),
            Expr::Negative(inner) if let Expr::Literal(value, _) = inner.as_ref() => {
                crate::scalar::finite_decimal(value).map(|value| -value)
            }
            _ => None,
        };
        if let Some(number) = number {
            out.push((&column.name, number));
        }
    }
}

pub(super) fn impossible(expr: &Expr, schema: &datafusion::common::DFSchema) -> bool {
    if let Expr::BinaryExpr(binary) = expr
        && binary.op == Operator::And
    {
        return impossible(&binary.left, schema) || impossible(&binary.right, schema);
    }
    if let Expr::ScalarFunction(function) = expr
        && function.name() == "__graphlake_cypher_compare"
        && let [
            Expr::Column(column),
            Expr::Literal(value, _),
            Expr::Literal(operator, _),
        ] = function.args.as_slice()
        && operator.try_as_str().flatten() == Some("=")
    {
        return value.is_null()
            || schema.field_from_column(column).is_ok_and(|field| {
                (supported(field.data_type()) && value.try_as_str().flatten().is_some())
                    || (matches!(
                        field.data_type(),
                        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
                    ) && value.data_type().is_numeric())
            });
    }
    false
}

pub(super) fn excludes(block: &super::Block, equalities: &[(&str, BigDecimal)]) -> Result<bool> {
    for (column, value) in equalities {
        match block.numbers.get(*column) {
            None => continue,
            Some(None) => return Ok(true),
            Some(Some([lo, hi])) => {
                let (lo, hi) = (lo.parse::<BigDecimal>()?, hi.parse::<BigDecimal>()?);
                if value < &lo || value > &hi {
                    return Ok(true);
                }
            }
        }
    }
    Ok(false)
}
