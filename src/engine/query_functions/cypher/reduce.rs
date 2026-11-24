use std::{
    hash::{Hash, Hasher},
    sync::Arc,
};

use datafusion::{
    arrow::{
        array::{Array, ArrayRef, AsArray, new_empty_array, new_null_array},
        compute::{CastOptions, cast_with_options, concat},
        datatypes::{DataType, Field, FieldRef},
    },
    common::{Result, exec_err, plan_err},
    execution::{memory_pool::MemoryConsumer, runtime_env::RuntimeEnv},
    logical_expr::{
        ColumnarValue, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs, HigherOrderSignature,
        HigherOrderUDF, HigherOrderUDFImpl, LambdaParametersProgress, ValueOrLambda, Volatility,
    },
    prelude::SessionContext,
};

use super::{common_value_type, flatten_value, merge_values, value_types};
use crate::engine::query::allocation::slice_bytes;

pub(in crate::engine) fn register(
    context: &SessionContext,
    scope: &crate::engine::query::QueryScope,
) {
    let mut signature = HigherOrderSignature::exact(
        vec![
            ValueOrLambda::Value(()),
            ValueOrLambda::Value(()),
            ValueOrLambda::Lambda(()),
        ],
        Volatility::Volatile,
    );
    signature.lambda_parameters_max_iterations = 8;
    context.register_higher_order_function(Arc::new(HigherOrderUDF::from(Reduce {
        signature,
        runtime: Arc::clone(&scope.runtime),
        memory: scope.options.memory_bytes,
        cancelled: scope.options.cancellation.clone(),
        deadline: scope.deadline,
    })));
}

#[derive(Debug)]
struct Reduce {
    signature: HigherOrderSignature,
    runtime: Arc<RuntimeEnv>,
    memory: usize,
    cancelled: crate::CancellationToken,
    deadline: tokio::time::Instant,
}

impl PartialEq for Reduce {
    fn eq(&self, other: &Self) -> bool {
        self.signature == other.signature
            && Arc::ptr_eq(&self.runtime, &other.runtime)
            && self.memory == other.memory
            && self.deadline == other.deadline
    }
}

impl Eq for Reduce {}

impl Hash for Reduce {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.signature.hash(state);
        Arc::as_ptr(&self.runtime).hash(state);
        self.memory.hash(state);
        self.deadline.hash(state);
    }
}

fn accumulator(initial: &DataType, body: &DataType) -> DataType {
    // Keep an exact decimal accumulator at its maximum precision so repeated
    // decimal addition does not widen the type on every planning pass.
    let mut types = Vec::new();
    value_types(initial, &mut types);
    value_types(body, &mut types);
    for ty in &mut types {
        if let DataType::Decimal128(precision, _) = ty {
            *precision = 38;
        }
    }
    common_value_type(types.iter())
}

impl HigherOrderUDFImpl for Reduce {
    fn name(&self) -> &str {
        "__graphlake_cypher_reduce"
    }

    fn signature(&self) -> &HigherOrderSignature {
        &self.signature
    }

    fn coerce_value_types(&self, args: &[DataType]) -> Result<Vec<DataType>> {
        Ok(args.to_vec())
    }

    fn lambda_parameters(
        &self,
        step: usize,
        args: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
    ) -> Result<LambdaParametersProgress> {
        let [
            ValueOrLambda::Value(list),
            ValueOrLambda::Value(initial),
            ValueOrLambda::Lambda(body),
        ] = args
        else {
            return plan_err!("invalid reduce() arguments");
        };
        let element = match list.data_type() {
            DataType::List(element)
            | DataType::LargeList(element)
            | DataType::FixedSizeList(element, _) => Arc::clone(element),
            DataType::Null => Arc::new(Field::new("", DataType::Null, true)),
            _ => return plan_err!("reduce() expects a list"),
        };
        let data_type = body.as_ref().map_or_else(
            || initial.data_type().clone(),
            |body| accumulator(initial.data_type(), body.data_type()),
        );
        let params = vec![Arc::new(Field::new("", data_type, true)), element];
        // The second pass types the lambda with its own output as a possible
        // input, e.g. an integer initial value followed by a floating sum.
        Ok(if step == 0 && body.is_none() {
            LambdaParametersProgress::Partial(vec![Some(params)])
        } else {
            LambdaParametersProgress::Complete(vec![params])
        })
    }

    fn return_field_from_args(&self, args: HigherOrderReturnFieldArgs) -> Result<FieldRef> {
        let [
            ValueOrLambda::Value(_),
            ValueOrLambda::Value(initial),
            ValueOrLambda::Lambda(body),
        ] = args.arg_fields
        else {
            return plan_err!("invalid reduce() arguments");
        };
        Ok(Arc::new(Field::new(
            "",
            accumulator(initial.data_type(), body.data_type()),
            true,
        )))
    }

    fn invoke_with_args(&self, args: HigherOrderFunctionArgs) -> Result<ColumnarValue> {
        let [
            ValueOrLambda::Value(lists),
            ValueOrLambda::Value(initial),
            ValueOrLambda::Lambda(body),
        ] = args.args.as_slice()
        else {
            return exec_err!("invalid reduce() arguments");
        };
        let lists_scalar = matches!(lists, ColumnarValue::Scalar(_));
        let lists = match lists {
            ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(1)?,
            ColumnarValue::Array(array) => Arc::clone(array),
        };
        let ty = args.return_type();
        if lists.data_type().is_null()
            || (lists_scalar && lists.is_null(0))
            || (!lists_scalar && lists.null_count() == args.number_rows)
        {
            return Ok(ColumnarValue::Array(new_null_array(ty, args.number_rows)));
        }
        let elements = match lists.data_type() {
            DataType::List(_) => |list: &dyn Array, row| list.as_list::<i32>().value(row),
            DataType::LargeList(_) => |list: &dyn Array, row| list.as_list::<i64>().value(row),
            DataType::FixedSizeList(_, _) => {
                |list: &dyn Array, row| list.as_fixed_size_list().value(row)
            }
            _ => return exec_err!("reduce() expects a list"),
        };
        let initial_scalar = matches!(initial, ColumnarValue::Scalar(_));
        let initial = match initial {
            ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(1)?,
            ColumnarValue::Array(array) => Arc::clone(array),
        };
        let reservation =
            MemoryConsumer::new("GraphLake reduce()").register(&self.runtime.memory_pool);
        let mut output = Vec::with_capacity(args.number_rows);
        let mut bytes = 0usize;
        for row in 0..args.number_rows {
            if self.cancelled.is_cancelled() {
                return exec_err!("query cancelled");
            }
            if tokio::time::Instant::now() >= self.deadline {
                return exec_err!("query deadline exceeded");
            }
            let list_row = if lists_scalar { 0 } else { row };
            let init_row = if initial_scalar { 0 } else { row };
            let mut value = new_null_array(ty, 1);
            if lists.is_valid(list_row) {
                let items = elements(lists.as_ref(), list_row);
                value = fit(initial.slice(init_row, 1), ty)?;
                for index in 0..items.len() {
                    if self.cancelled.is_cancelled() {
                        return exec_err!("query cancelled");
                    }
                    if tokio::time::Instant::now() >= self.deadline {
                        return exec_err!("query deadline exceeded");
                    }
                    let update = body
                        .evaluate(
                            &[&|| Ok(Arc::clone(&value)), &|| Ok(items.slice(index, 1))],
                            |columns| Ok(columns.iter().map(|col| col.slice(row, 1)).collect()),
                        )?
                        .into_array(1)?;
                    value = fit(update, ty)?;
                    let current = bytes.saturating_add(value_bytes(&value)?);
                    if current > self.memory || reservation.try_resize(current).is_err() {
                        return exec_err!("reduce() exceeds the query memory budget");
                    }
                }
            }
            bytes = bytes.saturating_add(value_bytes(&value)?);
            if bytes > self.memory || reservation.try_resize(bytes).is_err() {
                return exec_err!("reduce() exceeds the query memory budget");
            }
            output.push(value);
        }
        let array = if output.is_empty() {
            new_empty_array(ty)
        } else {
            let peak = bytes.saturating_mul(2);
            if peak > self.memory || reservation.try_resize(peak).is_err() {
                return exec_err!("reduce() exceeds the query memory budget");
            }
            concat(
                &output
                    .iter()
                    .map(|array| array.as_ref())
                    .collect::<Vec<_>>(),
            )?
        };
        Ok(ColumnarValue::Array(array))
    }
}

fn value_bytes(value: &ArrayRef) -> Result<usize> {
    slice_bytes(value.as_ref())
}

fn fit(value: ArrayRef, ty: &DataType) -> Result<ArrayRef> {
    if value.data_type() == ty {
        return Ok(value);
    }
    let mut lanes = Vec::new();
    flatten_value(Arc::clone(&value), &mut lanes);
    for lane in &mut lanes {
        if let DataType::Decimal128(precision, scale) = lane.data_type()
            && *precision != 38
        {
            *lane = cast_with_options(
                lane,
                &DataType::Decimal128(38, *scale),
                &CastOptions {
                    safe: false,
                    ..Default::default()
                },
            )?;
        }
    }
    let result = merge_values(lanes, ty, 1)?;
    if !value.data_type().is_null() && value.is_valid(0) && result.is_null(0) {
        return exec_err!(
            "reduce() cannot keep an accumulator of type {} as {ty}",
            value.data_type()
        );
    }
    Ok(result)
}
