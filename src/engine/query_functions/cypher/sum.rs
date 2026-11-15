use datafusion::{
    arrow::{
        array::{ArrayRef, AsArray},
        compute::{CastOptions, cast_with_options, sum_checked},
        datatypes::{
            ArrowNumericType, DataType, Decimal128Type, Decimal256Type, DecimalType, Float64Type,
            Int64Type, UInt64Type,
        },
    },
    common::{Result, ScalarValue, exec_err},
    logical_expr::{
        Accumulator, AggregateUDF, AggregateUDFImpl, Signature, Volatility,
        function::AccumulatorArgs,
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
        let values = cast_with_options(
            &values[0],
            &ty,
            &CastOptions {
                safe: false,
                ..Default::default()
            },
        )?;
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
