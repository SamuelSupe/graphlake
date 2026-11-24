use std::{collections::BTreeMap, sync::Arc};

use datafusion::{
    arrow::{
        array::{
            Array, ArrayBuilder, ArrayRef, ListArray, MapArray, MutableArrayData,
            StringViewBuilder, StructArray, make_array,
        },
        buffer::{NullBuffer, OffsetBuffer},
        datatypes::{DataType, Field, FieldRef, Fields},
    },
    common::{Result, ScalarValue, exec_err, internal_err, plan_err},
    execution::memory_pool::MemoryConsumer,
    logical_expr::{
        ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
        Volatility,
    },
    prelude::SessionContext,
};

use super::{ScopeGuard, common_value_type, flatten_value, merge_values, unpack, value_types};

pub(super) fn register(context: &SessionContext, guard: Option<ScopeGuard>) {
    for operation in ["get", "merge", "keys"] {
        context.register_udf(ScalarUDF::from(MapProjection {
            operation,
            signature: Signature::any(2, Volatility::Immutable),
            guard: guard.clone(),
        }));
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct MapProjection {
    operation: &'static str,
    signature: Signature,
    guard: Option<ScopeGuard>,
}

fn map_fields(ty: &DataType) -> Result<&[FieldRef]> {
    match ty {
        DataType::Struct(fields) => Ok(fields),
        DataType::Map(..) | DataType::Null => Ok(&[]),
        _ => plan_err!("Cypher expects a map, got {ty}"),
    }
}

fn property_name(value: &ScalarValue) -> Result<&str> {
    match value {
        ScalarValue::Utf8(Some(key))
        | ScalarValue::Utf8View(Some(key))
        | ScalarValue::LargeUtf8(Some(key)) => Ok(key),
        _ => plan_err!("Cypher map projection needs a string property name"),
    }
}

fn merged_fields(base: &DataType, overrides: &DataType) -> Result<DataType> {
    let fields: BTreeMap<_, _> = map_fields(base)?
        .iter()
        .chain(map_fields(overrides)?)
        .map(|field| (field.name(), field))
        .collect();
    Ok(if fields.is_empty() {
        overrides.clone()
    } else {
        DataType::Struct(
            fields
                .into_values()
                .map(|field| field.as_ref().clone().with_nullable(true))
                .collect::<Fields>(),
        )
    })
}

impl ScalarUDFImpl for MapProjection {
    fn name(&self) -> &str {
        match self.operation {
            "get" => "__graphlake_cypher_map_get",
            "merge" => "__graphlake_cypher_map_merge",
            _ => "__graphlake_cypher_keys",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        internal_err!("map projection uses argument fields")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let mut maps = Vec::new();
        value_types(args.arg_fields[0].data_type(), &mut maps);
        let ty = match self.operation {
            "merge" => {
                let overrides = args.arg_fields[1].data_type();
                if maps.is_empty() {
                    overrides.clone()
                } else {
                    let layouts = maps
                        .iter()
                        .map(|base| merged_fields(base, overrides))
                        .collect::<Result<Vec<_>>>()?;
                    common_value_type(layouts.iter())
                }
            }
            "get" => {
                let Some(key) = args.scalar_arguments[1] else {
                    return plan_err!("Cypher map projection needs a literal property name");
                };
                let key = property_name(key)?;
                let mut types = Vec::new();
                for ty in &maps {
                    if let DataType::Map(entries, _) = ty {
                        let DataType::Struct(fields) = entries.data_type() else {
                            return plan_err!("invalid map entry type");
                        };
                        types.push(fields[1].data_type());
                    } else if let Some(field) =
                        map_fields(ty)?.iter().find(|field| field.name() == key)
                    {
                        types.push(field.data_type());
                    }
                }
                common_value_type(types.into_iter())
            }
            _ => {
                for ty in &maps {
                    map_fields(ty)?;
                }
                DataType::List(Arc::new(Field::new_list_field(DataType::Utf8View, true)))
            }
        };
        Ok(Arc::new(Field::new(self.name(), ty, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let broadcast_slice = if self.operation == "merge" {
            &args.args[..2]
        } else {
            &args.args[..1]
        };
        let _broadcast = super::reserve_scalar_broadcast(
            broadcast_slice,
            args.number_rows,
            self.name(),
            self.guard.as_ref(),
        )?;
        let base = args.args[0].to_array(args.number_rows)?;
        let ty = args.return_field.data_type();
        if self.operation == "keys" {
            let omit_null = matches!(
                &args.args[1],
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(true)))
            );
            return Ok(ColumnarValue::Array(keys(
                base,
                ty,
                omit_null,
                self.guard.as_ref(),
            )?));
        }
        let mut maps = Vec::new();
        flatten_value(base, &mut maps);
        let mut results = Vec::new();
        if self.operation == "get" {
            let ColumnarValue::Scalar(key) = &args.args[1] else {
                return exec_err!("Cypher map projection needs a literal property name");
            };
            let key = property_name(key)?;
            for map in maps {
                if let Some(guard) = &self.guard {
                    guard.check()?;
                }
                if let Some(record) = map.as_any().downcast_ref::<StructArray>()
                    && let Some((index, _)) = record.fields().find(key)
                {
                    results.push(record.flatten().1[index].clone());
                } else if let Some(map) = map.as_any().downcast_ref::<MapArray>() {
                    results.push(get_map_value(map, key, self.guard.as_ref())?);
                }
            }
        } else {
            let overrides = args.args[1].to_array(args.number_rows)?;
            for map in maps {
                if let Some(guard) = &self.guard {
                    guard.check()?;
                }
                results.push(merge_map(&map, &overrides)?);
            }
        }
        Ok(ColumnarValue::Array(merge_values(
            results,
            ty,
            args.number_rows,
        )?))
    }
}

fn merge_map(base: &ArrayRef, overrides: &ArrayRef) -> Result<ArrayRef> {
    if let Some(map) = base.as_any().downcast_ref::<MapArray>()
        && !map.entries().is_empty()
    {
        return exec_err!("Cypher .* requires a map with known fields");
    }
    let DataType::Struct(fields) = merged_fields(base.data_type(), overrides.data_type())? else {
        return Ok(make_array(
            overrides
                .to_data()
                .into_builder()
                .nulls(base.nulls().cloned())
                .build()?,
        ));
    };
    let mut values = BTreeMap::new();
    for array in [base, overrides] {
        if let Some(record) = array.as_any().downcast_ref::<StructArray>() {
            let (fields, columns) = record.flatten();
            values.extend(
                fields
                    .iter()
                    .zip(columns)
                    .map(|(field, array)| (field.name().clone(), array)),
            );
        }
    }
    let columns = fields
        .iter()
        .map(|field| values.remove(field.name()).expect("projected field"))
        .collect();
    Ok(Arc::new(StructArray::try_new(
        fields,
        columns,
        base.nulls().cloned(),
    )?))
}

fn get_map_value(map: &MapArray, key: &str, guard: Option<&ScopeGuard>) -> Result<ArrayRef> {
    let data = map.values().to_data();
    let mut values = MutableArrayData::new(vec![&data], true, map.len());
    for (row, offset) in map.value_offsets().windows(2).enumerate() {
        if row.is_multiple_of(256)
            && let Some(guard) = guard
        {
            guard.check()?;
        }
        let mut found = None;
        if map.is_valid(row) {
            for index in offset[0] as usize..offset[1] as usize {
                let name = ScalarValue::try_from_array(map.keys(), index)?;
                if property_name(&name)? == key {
                    found = Some(index);
                    break;
                }
            }
        }
        if let Some(index) = found {
            values.try_extend(0, index, index + 1)?;
        } else {
            values.try_extend_nulls(1)?;
        }
    }
    Ok(make_array(values.freeze()))
}

fn keys(
    input: ArrayRef,
    ty: &DataType,
    omit_null: bool,
    guard: Option<&ScopeGuard>,
) -> Result<ArrayRef> {
    let DataType::List(field) = ty else {
        return internal_err!("expected keys list");
    };
    let reservation = guard.map(|guard| {
        MemoryConsumer::new("__graphlake_cypher_keys output").register(&guard.runtime.memory_pool)
    });
    let mut keys = StringViewBuilder::new();
    let mut lengths = Vec::with_capacity(input.len());
    let mut valid = Vec::with_capacity(input.len());
    let mut total_items = 0usize;
    for row in 0..input.len() {
        if row.is_multiple_of(256)
            && let Some(guard) = guard
        {
            guard.check()?;
        }
        let value = unpack(ScalarValue::try_from_array(&input, row)?)?;
        let start = keys.len();
        valid.push(!value.is_null());
        if let ScalarValue::Struct(record) = &value
            && !value.is_null()
        {
            for (field, column) in record.fields().iter().zip(record.columns()) {
                if !omit_null || !ScalarValue::try_from_array(column, 0)?.is_null() {
                    total_items = total_items
                        .checked_add(1)
                        .filter(|&n| n <= i32::MAX as usize)
                        .ok_or_else(|| {
                            datafusion::common::exec_datafusion_err!(
                                "Cypher keys() exceeds Arrow list offset range"
                            )
                        })?;
                    if let Some(reservation) = &reservation {
                        reservation.try_grow(field.name().len().saturating_add(16))?;
                    }
                    keys.append_value(field.name());
                }
            }
        } else if let ScalarValue::Map(map) = &value
            && !value.is_null()
        {
            let offset = map.value_offsets();
            for index in offset[0] as usize..offset[1] as usize {
                if omit_null && map.values().is_null(index) {
                    continue;
                }
                let key_scalar = ScalarValue::try_from_array(map.keys(), index)?;
                let key = property_name(&key_scalar)?;
                total_items = total_items
                    .checked_add(1)
                    .filter(|&n| n <= i32::MAX as usize)
                    .ok_or_else(|| {
                        datafusion::common::exec_datafusion_err!(
                            "Cypher keys() exceeds Arrow list offset range"
                        )
                    })?;
                if let Some(reservation) = &reservation {
                    reservation.try_grow(key.len().saturating_add(16))?;
                }
                keys.append_value(key);
            }
        }
        lengths.push(keys.len() - start);
    }
    Ok(Arc::new(ListArray::try_new(
        field.clone(),
        OffsetBuffer::from_lengths(lengths),
        Arc::new(keys.finish()),
        Some(NullBuffer::from(valid)),
    )?))
}
