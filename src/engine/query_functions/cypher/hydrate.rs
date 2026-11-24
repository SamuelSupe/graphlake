use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Array, ListArray, StringArray, StructArray, UInt32Builder},
        buffer::OffsetBuffer,
        compute::{cast, take},
        datatypes::{DataType, Field},
    },
    common::{Result, exec_err, plan_err},
    execution::memory_pool::MemoryConsumer,
    logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    },
    prelude::SessionContext,
};

use super::ScopeGuard;
use crate::engine::query::allocation::slice_bytes;

pub(super) fn register(context: &SessionContext, guard: Option<ScopeGuard>) {
    context.register_udf(ScalarUDF::from(Hydrate {
        signature: Signature::any(2, Volatility::Immutable),
        guard,
    }));
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Hydrate {
    signature: Signature,
    guard: Option<ScopeGuard>,
}

impl ScalarUDFImpl for Hydrate {
    fn name(&self) -> &str {
        "__graphlake_cypher_hydrate"
    }

    fn signature(&self) -> &Signature {
        &self.signature
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
        let _broadcast = super::reserve_scalar_broadcast(
            &args.args[..1],
            args.number_rows,
            "__graphlake_cypher_hydrate broadcast",
            self.guard.as_ref(),
        )?;
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
        let _indices_mem = if let Some(guard) = &self.guard {
            guard.check()?;
            let bytes = requested.len().saturating_mul(std::mem::size_of::<u32>());
            if bytes > 0 {
                let reservation = MemoryConsumer::new("__graphlake_cypher_hydrate indices")
                    .register(&guard.runtime.memory_pool);
                reservation.try_grow(bytes)?;
                Some(reservation)
            } else {
                None
            }
        } else {
            None
        };
        let mut indices = UInt32Builder::new();
        let mut lengths = Vec::with_capacity(identities.len());
        let mut total_items = 0usize;
        let mut matched_items = 0usize;
        let mut visited = 0usize;
        for (row, range) in identities.value_offsets().windows(2).enumerate() {
            if identities.is_null(row) {
                lengths.push(0);
                continue;
            }
            let row_len = (range[1] - range[0]) as usize;
            total_items = total_items
                .checked_add(row_len)
                .filter(|&total| total <= i32::MAX as usize)
                .ok_or_else(|| {
                    datafusion::common::exec_datafusion_err!(
                        "path hydration exceeds the Arrow list offset range"
                    )
                })?;
            let lookup_row = if scalar { 0 } else { row };
            if !lookup.is_valid(lookup_row)
                || !ids.is_valid(lookup_row)
                || !values.is_valid(lookup_row)
            {
                for _ in 0..row_len {
                    visited += 1;
                    if visited.is_multiple_of(256)
                        && let Some(guard) = &self.guard
                    {
                        guard.check()?;
                    }
                    indices.append_null();
                }
                lengths.push(row_len);
                continue;
            }
            let start = ids.value_offsets()[lookup_row] as usize;
            let end = ids.value_offsets()[lookup_row + 1] as usize;
            let value_start = values.value_offsets()[lookup_row] as usize;
            if end - start != values.value_length(lookup_row) as usize {
                return exec_err!("path lookup keys and payloads differ in length");
            }
            for index in range[0] as usize..range[1] as usize {
                visited += 1;
                if visited.is_multiple_of(256)
                    && let Some(guard) = &self.guard
                {
                    guard.check()?;
                }
                let found = if requested.is_valid(index) {
                    let id = requested.value(index);
                    // The lookup's IDs and payloads are aggregated in the same ID order
                    // (SQL ORDER BY sorts NULL keys after non-NULL keys).
                    let mut lo = start;
                    let mut hi = end;
                    while lo < hi {
                        let mid = lo + (hi - lo) / 2;
                        if keys.is_valid(mid) && keys.value(mid) < id {
                            lo = mid + 1;
                        } else {
                            hi = mid;
                        }
                    }
                    if lo < end && keys.is_valid(lo) && keys.value(lo) == id {
                        let target = value_start + lo - start;
                        let target = u32::try_from(target).map_err(|_| {
                            datafusion::common::exec_datafusion_err!(
                                "path lookup payload index exceeds u32 range"
                            )
                        })?;
                        matched_items += 1;
                        Some(target)
                    } else {
                        None
                    }
                } else {
                    None
                };
                indices.append_option(found);
            }
            lengths.push(row_len);
        }
        let _take_mem = if let Some(guard) = &self.guard {
            guard.check()?;
            let payload_len = values.values().len();
            let payload_bytes = slice_bytes(values.values().as_ref())?;
            let avg_row = if payload_len > 0 {
                payload_bytes.div_ceil(payload_len)
            } else {
                0
            };
            let estimated = matched_items
                .saturating_mul(avg_row)
                .saturating_add(total_items.saturating_mul(std::mem::size_of::<u32>()));
            if estimated > 0 {
                let reservation = MemoryConsumer::new("__graphlake_cypher_hydrate output")
                    .register(&guard.runtime.memory_pool);
                reservation.try_grow(estimated)?;
                Some(reservation)
            } else {
                None
            }
        } else {
            None
        };
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

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::buffer::NullBuffer;
    use datafusion::arrow::datatypes::{Fields, Int64Type};

    #[test]
    fn hydrate_handles_null_lookup_struct_and_null_keys() -> Result<()> {
        let udf = Hydrate {
            signature: Signature::any(2, Volatility::Immutable),
            guard: None,
        };
        let requested_values = Arc::new(StringArray::from(vec![Some("a"), Some("z"), Some("b")]));
        let requested = Arc::new(ListArray::try_new(
            Arc::new(Field::new_list_field(DataType::Utf8, true)),
            OffsetBuffer::from_lengths([2, 1]),
            requested_values,
            None,
        )?);
        // Row 0 has a valid lookup with sorted keys ["a", "b", NULL] (NULL sorted last by SQL).
        // Row 1 is a NULL lookup struct with mismatched underlying list lengths (must not error).
        let ids_list = Arc::new(ListArray::try_new(
            Arc::new(Field::new_list_field(DataType::Utf8, true)),
            OffsetBuffer::from_lengths([3, 1]),
            Arc::new(StringArray::from(vec![
                Some("a"),
                Some("b"),
                None,
                Some("x"),
            ])),
            None,
        )?);
        let vals_list = Arc::new(ListArray::try_new(
            Arc::new(Field::new_list_field(DataType::Int64, true)),
            OffsetBuffer::from_lengths([3, 0]),
            Arc::new(Int64Array::from(vec![Some(10), Some(20), Some(30)])),
            None,
        )?);
        let lookup_fields = Fields::from(vec![
            Field::new("ids", ids_list.data_type().clone(), true),
            Field::new("values", vals_list.data_type().clone(), true),
        ]);
        let lookup = Arc::new(StructArray::try_new(
            lookup_fields.clone(),
            vec![ids_list, vals_list],
            Some(NullBuffer::from(vec![true, false])),
        )?);
        let ret_type = udf.return_type(&[
            requested.data_type().clone(),
            DataType::Struct(lookup_fields),
        ])?;
        let out = udf
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Array(requested.clone()),
                    ColumnarValue::Array(lookup.clone()),
                ],
                arg_fields: vec![
                    Arc::new(Field::new("a", requested.data_type().clone(), true)),
                    Arc::new(Field::new("b", lookup.data_type().clone(), true)),
                ],
                number_rows: 2,
                return_field: Arc::new(Field::new("out", ret_type, true)),
                config_options: Arc::new(Default::default()),
            })?
            .into_array(2)?;
        let out = out.as_any().downcast_ref::<ListArray>().unwrap();
        let expected = ListArray::from_iter_primitive::<Int64Type, _, _>([
            Some(vec![Some(10), None]),
            Some(vec![None]),
        ]);
        assert_eq!(out, &expected);
        Ok(())
    }
}
