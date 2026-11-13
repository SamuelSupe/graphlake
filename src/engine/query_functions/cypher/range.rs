use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Array, Int64Array},
        datatypes::{DataType, Field},
    },
    common::{Result, exec_err, plan_err},
    execution::memory_pool::MemoryLimit,
    functions_nested::range::gen_series_udf,
    logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    },
    prelude::SessionContext,
};

pub(super) fn register(context: &SessionContext) {
    let bytes = match context.runtime_env().memory_pool.memory_limit() {
        MemoryLimit::Finite(bytes) => bytes,
        _ => usize::MAX,
    };
    context.register_udf(ScalarUDF::from(Range {
        signature: Signature::variadic_any(Volatility::Immutable),
        bytes,
    }));
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Range {
    signature: Signature,
    bytes: usize,
}

impl ScalarUDFImpl for Range {
    fn name(&self) -> &str {
        "__graphlake_cypher_range"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        if !(2..=3).contains(&args.len()) || args.iter().any(|ty| *ty != DataType::Int64) {
            return plan_err!("range() expects two or three integer arguments");
        }
        Ok(DataType::List(Arc::new(Field::new_list_field(
            DataType::Int64,
            true,
        ))))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = args
            .args
            .iter()
            .map(|arg| arg.to_array(args.number_rows))
            .collect::<Result<Vec<_>>>()?;
        let integers = arrays
            .iter()
            .map(|array| {
                array
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("range integer input")
            })
            .collect::<Vec<_>>();
        let mut items = 0_u128;
        for row in 0..args.number_rows {
            if integers.iter().any(|arg| arg.is_null(row)) {
                continue;
            }
            let start = integers[0].value(row);
            let end = integers[1].value(row);
            let step = integers.get(2).map_or(1, |arg| arg.value(row));
            if step == 0 {
                return exec_err!("range() step must be nonzero");
            }
            if (step > 0 && start <= end) || (step < 0 && start >= end) {
                items += u128::from(start.abs_diff(end) / step.unsigned_abs()) + 1;
                if items > i32::MAX as u128 || items * 8 > self.bytes as u128 {
                    return exec_err!(
                        "range() exceeds the query memory budget or Arrow list capacity"
                    );
                }
            }
        }
        gen_series_udf().invoke_with_args(args)
    }
}
