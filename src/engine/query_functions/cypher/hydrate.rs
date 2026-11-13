use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Array, ListArray, StringArray, StructArray, UInt32Builder},
        buffer::OffsetBuffer,
        compute::{cast, take},
        datatypes::{DataType, Field},
    },
    common::{Result, exec_err, plan_err},
    logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    },
    prelude::SessionContext,
};

pub(super) fn register(context: &SessionContext) {
    context.register_udf(ScalarUDF::from(Hydrate(Signature::any(
        2,
        Volatility::Immutable,
    ))));
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Hydrate(Signature);

impl ScalarUDFImpl for Hydrate {
    fn name(&self) -> &str {
        "__graphlake_cypher_hydrate"
    }

    fn signature(&self) -> &Signature {
        &self.0
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        if let DataType::List(ids) = &args[0]
            && super::text_type(ids.data_type())
            && let DataType::Struct(fields) = &args[1]
            && let [keys, values] = fields.as_ref()
            && keys.name() == "ids"
            && values.name() == "values"
            && matches!(keys.data_type(), DataType::List(keys) if super::text_type(keys.data_type()))
            && let DataType::List(payload) = values.data_type()
        {
            return Ok(DataType::List(Arc::new(Field::new_list_field(
                payload.data_type().clone(),
                true,
            ))));
        }
        plan_err!("invalid path lookup layout")
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let identities = args.args[0].to_array(args.number_rows)?;
        let identities = identities
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("path identities");
        let scalar = matches!(&args.args[1], ColumnarValue::Scalar(_));
        let lookup = args.args[1].to_array(if scalar { 1 } else { args.number_rows })?;
        let lookup = lookup
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("path lookup");
        let ids = lookup
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("lookup ids");
        let values = lookup
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("lookup values");
        let requested = cast(identities.values(), &DataType::Utf8)?;
        let requested = requested
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("text ids");
        let keys = cast(ids.values(), &DataType::Utf8)?;
        let keys = keys
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("lookup text ids");
        let mut indices = UInt32Builder::new();
        let mut lengths = Vec::with_capacity(identities.len());
        for (row, range) in identities.value_offsets().windows(2).enumerate() {
            if identities.is_null(row) {
                lengths.push(0);
                continue;
            }
            let lookup_row = if scalar { 0 } else { row };
            let start = ids.value_offsets()[lookup_row] as usize;
            let end = ids.value_offsets()[lookup_row + 1] as usize;
            let value_start = values.value_offsets()[lookup_row] as usize;
            if end - start != values.value_length(lookup_row) as usize {
                return exec_err!("path lookup keys and payloads differ in length");
            }
            for index in range[0] as usize..range[1] as usize {
                let found = if requested.is_valid(index) && ids.is_valid(lookup_row) {
                    let id = requested.value(index);
                    // The lookup's IDs and payloads are aggregated in the same ID order.
                    let mut lo = start;
                    let mut hi = end;
                    while lo < hi {
                        let mid = lo + (hi - lo) / 2;
                        if keys.value(mid) < id {
                            lo = mid + 1;
                        } else {
                            hi = mid;
                        }
                    }
                    (lo < end && keys.value(lo) == id).then_some((value_start + lo - start) as u32)
                } else {
                    None
                };
                indices.append_option(found);
            }
            lengths.push((range[1] - range[0]) as usize);
        }
        let values = take(values.values().as_ref(), &indices.finish(), None)?;
        let DataType::List(field) = args.return_field.data_type() else {
            unreachable!("path list type")
        };
        Ok(ColumnarValue::Array(Arc::new(ListArray::try_new(
            field.clone(),
            OffsetBuffer::from_lengths(lengths),
            values,
            identities.nulls().cloned(),
        )?)))
    }
}
