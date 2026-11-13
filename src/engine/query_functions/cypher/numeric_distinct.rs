use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Array, ListArray, new_null_array},
        datatypes::{DataType, Field, Schema},
    },
    common::{Result, ScalarValue, plan_err},
    functions_aggregate::{average::avg_udaf, sum::sum_udaf},
    logical_expr::{
        AggregateUDF, ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
        Volatility, function::AccumulatorArgs,
    },
    physical_expr::expressions::Column,
    prelude::SessionContext,
};

use super::{
    super::{numeric_array, numeric_type},
    distinct_list,
};

pub(super) fn register(context: &SessionContext) {
    for average in [false, true] {
        context.register_udf(ScalarUDF::from(NumericDistinct {
            average,
            signature: Signature::any(1, Volatility::Immutable),
        }));
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct NumericDistinct {
    average: bool,
    signature: Signature,
}

impl NumericDistinct {
    fn aggregate(&self) -> Arc<AggregateUDF> {
        if self.average { avg_udaf() } else { sum_udaf() }
    }

    fn input_type(&self, ty: &DataType) -> Result<DataType> {
        let DataType::List(field) = ty else {
            return plan_err!("DISTINCT numeric aggregate requires a list of values");
        };
        let ty = numeric_type(std::slice::from_ref(field.data_type()));
        Ok(if self.average && ty.is_integer() {
            DataType::Float64
        } else {
            ty
        })
    }
}

impl ScalarUDFImpl for NumericDistinct {
    fn name(&self) -> &str {
        if self.average {
            "__graphlake_cypher_avg_distinct"
        } else {
            "__graphlake_cypher_sum_distinct"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        self.aggregate().return_type(&[self.input_type(&args[0])?])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let array = args.args[0].to_array(args.number_rows)?;
        let ty = self.input_type(array.data_type())?;
        // Deduplicate original values before a promotion can round integers to
        // the same float. DataFusion still performs the typed sum or average.
        let array = distinct_list(array)?;
        let list = array
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("distinct list");
        let aggregate = self.aggregate();
        let field = Arc::new(Field::new("value", ty.clone(), true));
        let fields = [field];
        let schema = Schema::new(fields.to_vec());
        let exprs = [Arc::new(Column::new("value", 0)) as _];
        if list.is_empty() {
            return Ok(ColumnarValue::Array(new_null_array(
                args.return_field.data_type(),
                0,
            )));
        }
        let result = ScalarValue::iter_to_array(
            (0..list.len())
                .map(|index| {
                    if list.is_null(index) {
                        return ScalarValue::try_from(args.return_field.data_type());
                    }
                    let items = list.value(index);
                    let rows = items.len();
                    let values = numeric_array(vec![items], &ty, rows)?;
                    let mut accumulator = aggregate.accumulator(AccumulatorArgs {
                        return_field: args.return_field.clone(),
                        schema: &schema,
                        ignore_nulls: true,
                        order_bys: &[],
                        is_reversed: false,
                        name: aggregate.name(),
                        is_distinct: false,
                        exprs: &exprs,
                        expr_fields: &fields,
                    })?;
                    accumulator.update_batch(&[values])?;
                    accumulator.evaluate()
                })
                .collect::<Result<Vec<_>>>()?,
        )?;
        Ok(ColumnarValue::Array(result))
    }
}
