use bigdecimal::{BigDecimal, num_bigint::BigInt};

use crate::ScalarValue;

pub(crate) fn finite_decimal(value: &ScalarValue) -> Option<BigDecimal> {
    use ScalarValue::*;
    Some(match value {
        Int8(Some(n)) => BigDecimal::from(*n),
        Int16(Some(n)) => BigDecimal::from(*n),
        Int32(Some(n)) => BigDecimal::from(*n),
        Int64(Some(n)) => BigDecimal::from(*n),
        UInt8(Some(n)) => BigDecimal::from(*n),
        UInt16(Some(n)) => BigDecimal::from(*n),
        UInt32(Some(n)) => BigDecimal::from(*n),
        UInt64(Some(n)) => BigDecimal::from(*n),
        Float16(Some(n)) => BigDecimal::try_from(n.to_f32()).ok()?,
        Float32(Some(n)) => BigDecimal::try_from(*n).ok()?,
        Float64(Some(n)) => BigDecimal::try_from(*n).ok()?,
        Decimal32(Some(n), _, scale) => BigDecimal::from((*n, i64::from(*scale))),
        Decimal64(Some(n), _, scale) => BigDecimal::from((*n, i64::from(*scale))),
        Decimal128(Some(n), _, scale) => BigDecimal::from((*n, i64::from(*scale))),
        Decimal256(Some(n), _, scale) => BigDecimal::from((
            BigInt::from_signed_bytes_le(&n.to_le_bytes()),
            i64::from(*scale),
        )),
        Dictionary(_, value) => return finite_decimal(value),
        _ => return None,
    })
}
