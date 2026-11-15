use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Array, ArrayRef, ListArray, make_array},
        buffer::{NullBuffer, OffsetBuffer},
        compute::{
            CastOptions, cast, cast_with_options,
            kernels::{concat_elements::concat_elements_dyn, numeric},
        },
        datatypes::DataType,
    },
    common::Result,
    functions_nested::concat::array_concat_inner,
    logical_expr::{
        ColumnarValue, Operator, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
        Volatility, type_coercion::binary::BinaryTypeCoercer,
    },
    prelude::SessionContext,
};

use super::{common_value_type, flatten_value, is_variant, merge_values, text_type, value_types};

pub(super) fn register(context: &SessionContext) {
    for (name, op) in [
        ("__graphlake_cypher_add", Operator::Plus),
        ("__graphlake_cypher_subtract", Operator::Minus),
        ("__graphlake_cypher_multiply", Operator::Multiply),
        ("__graphlake_cypher_divide", Operator::Divide),
        ("__graphlake_cypher_remainder", Operator::Modulo),
    ] {
        context.register_udf(ScalarUDF::from(Arithmetic {
            name,
            op,
            signature: Signature::any(2, Volatility::Immutable),
        }));
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Arithmetic {
    name: &'static str,
    op: Operator,
    signature: Signature,
}

fn concat_type(a: &DataType, b: &DataType) -> Option<DataType> {
    if text_type(a) && (text_type(b) || b.is_null()) || text_type(b) && a.is_null() {
        return Some(DataType::Utf8View);
    }
    let element = |ty: &DataType| match ty {
        DataType::List(field) => field.data_type().clone(),
        _ => ty.clone(),
    };
    if matches!(a, DataType::List(_)) || matches!(b, DataType::List(_)) {
        let left = element(a);
        let right = element(b);
        return Some(DataType::new_list(
            common_value_type([&left, &right].into_iter()),
            true,
        ));
    }
    None
}

fn result_type(a: &DataType, op: Operator, b: &DataType) -> Result<DataType> {
    if op == Operator::Plus
        && let Some(ty) = concat_type(a, b)
    {
        return Ok(ty);
    }
    let (a, b) = integer_lanes(a, b);
    BinaryTypeCoercer::new(a, &op, b).get_result_type()
}

fn integer_lanes<'a>(a: &'a DataType, b: &'a DataType) -> (&'a DataType, &'a DataType) {
    fn narrow(ty: &DataType) -> bool {
        matches!(
            ty,
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
        )
    }
    if (a.is_integer() || a.is_null()) && (b.is_integer() || b.is_null()) {
        (
            if narrow(a) { &DataType::Int64 } else { a },
            if narrow(b) { &DataType::Int64 } else { b },
        )
    } else {
        (a, b)
    }
}

fn concatenate(
    a: ArrayRef,
    b: ArrayRef,
    ty: &DataType,
    nulls: Option<NullBuffer>,
) -> Result<ArrayRef> {
    if let DataType::List(item) = ty {
        let rows = a.len();
        let list = |value: ArrayRef| -> Result<ArrayRef> {
            if matches!(value.data_type(), DataType::List(_)) {
                merge_values(vec![value], ty, rows)
            } else {
                let values = merge_values(vec![value], item.data_type(), rows)?;
                Ok(Arc::new(ListArray::try_new(
                    Arc::clone(item),
                    OffsetBuffer::from_lengths(std::iter::repeat_n(1, rows)),
                    values,
                    None,
                )?))
            }
        };
        let result = array_concat_inner(&[list(a)?, list(b)?])?;
        return Ok(make_array(
            result.to_data().into_builder().nulls(nulls).build()?,
        ));
    }
    Ok(concat_elements_dyn(
        cast(&a, ty)?.as_ref(),
        cast(&b, ty)?.as_ref(),
    )?)
}

impl ScalarUDFImpl for Arithmetic {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        if !args
            .iter()
            .any(|ty| matches!(ty, DataType::Struct(fields) if is_variant(fields)))
        {
            return result_type(&args[0], self.op, &args[1]);
        }
        let mut left = Vec::new();
        let mut right = Vec::new();
        value_types(&args[0], &mut left);
        value_types(&args[1], &mut right);
        let types = left
            .iter()
            .flat_map(|a| right.iter().filter_map(|b| result_type(a, self.op, b).ok()))
            .collect::<Vec<_>>();
        Ok(common_value_type(types.iter()))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let mut left = Vec::new();
        let mut right = Vec::new();
        flatten_value(args.args[0].to_array(args.number_rows)?, &mut left);
        flatten_value(args.args[1].to_array(args.number_rows)?, &mut right);
        let mut values = Vec::new();
        for a in &left {
            for b in &right {
                let nulls = NullBuffer::union(a.nulls(), b.nulls());
                if nulls
                    .as_ref()
                    .is_some_and(|mask| mask.null_count() == args.number_rows)
                {
                    continue;
                }
                if self.op == Operator::Plus
                    && let Some(ty) = concat_type(a.data_type(), b.data_type())
                {
                    values.push(concatenate(a.clone(), b.clone(), &ty, nulls)?);
                    continue;
                }
                let (left, right) = integer_lanes(a.data_type(), b.data_type());
                let coercer = BinaryTypeCoercer::new(left, &self.op, right);
                let (left_type, right_type) = coercer.get_input_types()?;
                let result_type = coercer.get_result_type()?;
                // Only the rows where both lanes are active belong to this type pair.
                let a = make_array(a.to_data().into_builder().nulls(nulls.clone()).build()?);
                let b = make_array(b.to_data().into_builder().nulls(nulls).build()?);
                let checked = CastOptions {
                    safe: false,
                    ..Default::default()
                };
                let a = cast_with_options(&a, &left_type, &checked)?;
                let b = cast_with_options(&b, &right_type, &checked)?;
                let value = match self.op {
                    Operator::Plus => numeric::add(&a, &b)?,
                    Operator::Minus => numeric::sub(&a, &b)?,
                    Operator::Multiply => numeric::mul(&a, &b)?,
                    Operator::Divide => numeric::div(&a, &b)?,
                    Operator::Modulo => numeric::rem(&a, &b)?,
                    _ => unreachable!("registered arithmetic operator"),
                };
                values.push(cast_with_options(&value, &result_type, &checked)?);
            }
        }
        Ok(ColumnarValue::Array(merge_values(
            values,
            args.return_field.data_type(),
            args.number_rows,
        )?))
    }
}
