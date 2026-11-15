use datafusion::{
    arrow::{
        array::{Array, ArrayRef, AsArray, BooleanArray, PrimitiveArray},
        compute::{CastOptions, cast_with_options, sum_checked},
        datatypes::{
            ArrowNativeTypeOp, ArrowNumericType, DataType, Decimal128Type, Decimal256Type,
            DecimalType, Float64Type, Int64Type, UInt64Type,
        },
    },
    common::{Result, ScalarValue, exec_err},
    logical_expr::{
        Accumulator, AggregateUDF, AggregateUDFImpl, EmitTo, GroupsAccumulator, Signature,
        Volatility, function::AccumulatorArgs,
    },
    prelude::SessionContext,
};

pub(super) fn register(context: &SessionContext) {
    context.register_udaf(AggregateUDF::from(Sum(Signature::any(
        1,
        Volatility::Immutable,
    ))));
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Sum(Signature);

impl AggregateUDFImpl for Sum {
    fn name(&self) -> &str {
        "__graphlake_cypher_sum"
    }

    fn signature(&self) -> &Signature {
        &self.0
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        Ok(super::super::numeric_type(args))
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        accumulator(args.return_field.data_type())
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        !args.is_distinct
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        let ty = args.return_field.data_type().clone();
        fn groups<T: ArrowNumericType>(ty: DataType) -> Box<dyn GroupsAccumulator> {
            Box::new(GroupedSum::<T> {
                ty,
                values: Vec::new(),
            })
        }
        Ok(match ty {
            DataType::Int64 => groups::<Int64Type>(ty),
            DataType::UInt64 => groups::<UInt64Type>(ty),
            DataType::Float64 => groups::<Float64Type>(ty),
            DataType::Decimal128(..) => groups::<Decimal128Type>(ty),
            DataType::Decimal256(..) => groups::<Decimal256Type>(ty),
            _ => return exec_err!("unsupported Cypher sum type {ty}"),
        })
    }
}

struct GroupedSum<T: ArrowNumericType> {
    ty: DataType,
    values: Vec<Option<T::Native>>,
}

impl<T: ArrowNumericType> GroupedSum<T> {
    fn drain(&mut self, emit: EmitTo) -> ArrayRef {
        let values = emit.take_needed(&mut self.values);
        let array: PrimitiveArray<T> = values.into_iter().collect();
        std::sync::Arc::new(array.with_data_type(self.ty.clone()))
    }
}

impl<T: ArrowNumericType> GroupsAccumulator for GroupedSum<T> {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        groups: &[usize],
        filter: Option<&BooleanArray>,
        total: usize,
    ) -> Result<()> {
        let values = checked_cast(&values[0], &self.ty)?;
        self.values.resize(total, None);
        for (row, (&group, value)) in groups
            .iter()
            .zip(values.as_primitive::<T>().iter())
            .enumerate()
        {
            if let Some(value) = value
                && filter.is_none_or(|f| f.is_valid(row) && f.value(row))
            {
                let sum = &mut self.values[group];
                *sum = Some(match *sum {
                    Some(previous) => previous.add_checked(value)?,
                    None => value,
                });
            }
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef], groups: &[usize], total: usize) -> Result<()> {
        self.update_batch(states, groups, None, total)
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        let values = checked_cast(&values[0], &self.ty)?;
        if let Some(filter) = filter {
            let array: PrimitiveArray<T> = values
                .as_primitive::<T>()
                .iter()
                .enumerate()
                .map(|(row, value)| {
                    if filter.is_valid(row) && filter.value(row) {
                        value
                    } else {
                        None
                    }
                })
                .collect();
            Ok(vec![std::sync::Arc::new(
                array.with_data_type(self.ty.clone()),
            )])
        } else {
            Ok(vec![values])
        }
    }

    fn evaluate(&mut self, emit: EmitTo) -> Result<ArrayRef> {
        let array = self.drain(emit);
        match self.ty {
            DataType::Decimal128(precision, _) => array
                .as_primitive::<Decimal128Type>()
                .validate_decimal_precision(precision)?,
            DataType::Decimal256(precision, _) => array
                .as_primitive::<Decimal256Type>()
                .validate_decimal_precision(precision)?,
            _ => {}
        }
        Ok(array)
    }

    fn state(&mut self, emit: EmitTo) -> Result<Vec<ArrayRef>> {
        Ok(vec![self.drain(emit)])
    }

    fn size(&self) -> usize {
        size_of::<Self>() + self.values.capacity() * size_of::<Option<T::Native>>()
    }
}

fn checked_cast(values: &ArrayRef, ty: &DataType) -> Result<ArrayRef> {
    Ok(cast_with_options(
        values,
        ty,
        &CastOptions {
            safe: false,
            ..Default::default()
        },
    )?)
}

pub(super) fn accumulator(ty: &DataType) -> Result<Box<dyn Accumulator>> {
    Ok(Box::new(CheckedSum(ScalarValue::try_from(ty)?)))
}

#[derive(Debug)]
struct CheckedSum(ScalarValue);

impl Accumulator for CheckedSum {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.0.clone()])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        fn batch_sum<T: ArrowNumericType>(values: &ArrayRef, ty: &DataType) -> Result<ScalarValue> {
            ScalarValue::new_primitive::<T>(sum_checked(values.as_primitive::<T>())?, ty)
        }

        let ty = self.0.data_type();
        let values = checked_cast(&values[0], &ty)?;
        let values = &values;
        let total = match &ty {
            DataType::Int64 => batch_sum::<Int64Type>(values, &ty)?,
            DataType::UInt64 => batch_sum::<UInt64Type>(values, &ty)?,
            DataType::Float64 => batch_sum::<Float64Type>(values, &ty)?,
            DataType::Decimal128(..) => batch_sum::<Decimal128Type>(values, &ty)?,
            DataType::Decimal256(..) => batch_sum::<Decimal256Type>(values, &ty)?,
            _ => return exec_err!("unsupported Cypher sum type {ty}"),
        };
        if self.0.is_null() {
            self.0 = total;
        } else if !total.is_null() {
            self.0 = self.0.add_checked(total)?;
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        match &self.0 {
            ScalarValue::Decimal128(Some(value), precision, scale) => {
                Decimal128Type::validate_decimal_precision(*value, *precision, *scale)?;
            }
            ScalarValue::Decimal256(Some(value), precision, scale) => {
                Decimal256Type::validate_decimal_precision(*value, *precision, *scale)?;
            }
            _ => {}
        }
        Ok(self.0.clone())
    }

    fn size(&self) -> usize {
        size_of::<Self>()
    }
}
