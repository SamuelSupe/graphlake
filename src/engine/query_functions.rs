pub(super) mod cypher;

use std::sync::Arc;

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};

use datafusion::{
    arrow::row::{RowConverter, SortField},
    arrow::{
        array::{
            Array, ArrayRef, AsArray, ListArray, MapArray, StringArray, StringBuilder,
            StringViewBuilder, StructArray,
        },
        buffer::OffsetBuffer,
        compute::{CastOptions, can_cast_types, cast, cast_with_options},
        datatypes::{DataType, Field, Fields, Int64Type},
        util::display::{ArrayFormatter, FormatOptions},
    },
    common::{Result, ScalarValue, exec_err, plan_err},
    logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    },
    prelude::SessionContext,
};

#[cfg(feature = "admin")]
pub(crate) fn register(context: &SessionContext) {
    register_inner(context, None);
}

pub(in crate::engine) fn register_scoped(
    context: &SessionContext,
    scope: &crate::engine::query::QueryScope,
) {
    register_inner(context, Some(scope));
}

fn register_inner(context: &SessionContext, scope: Option<&crate::engine::query::QueryScope>) {
    context.register_udf(ScalarUDF::from(Items(Signature::any(
        1,
        Volatility::Immutable,
    ))));
    context.register_udf(ScalarUDF::from(ElementId(Signature::any(
        2,
        Volatility::Immutable,
    ))));
    context.register_udf(ScalarUDF::from(GroupMap(Signature::any(
        1,
        Volatility::Immutable,
    ))));
    context.register_udf(ScalarUDF::from(TraverserText(Signature::any(
        1,
        Volatility::Immutable,
    ))));
    context.register_udf(ScalarUDF::from(Properties(Signature::any(
        1,
        Volatility::Immutable,
    ))));
    cypher::register_inner(context, scope);
    context.register_udf(ScalarUDF::from(QueryValue {
        name: "__graphlake_numeric_input",
        signature: Signature::variadic_any(Volatility::Immutable),
    }));
    for name in [
        "__graphlake_gremlin_number",
        "__graphlake_gremlin_repeat_depth",
        "__graphlake_gremlin_sum_zero",
        "__graphlake_gremlin_key",
        "__graphlake_cypher_index",
        "__graphlake_key_text",
    ] {
        context.register_udf(ScalarUDF::from(QueryValue {
            name,
            signature: Signature::any(1, Volatility::Immutable),
        }));
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Items(Signature);

impl ScalarUDFImpl for Items {
    fn name(&self) -> &str {
        "__graphlake_gremlin_items"
    }

    fn signature(&self) -> &Signature {
        &self.0
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        Ok(match &args[0] {
            DataType::List(field)
            | DataType::ListView(field)
            | DataType::LargeList(field)
            | DataType::LargeListView(field)
            | DataType::FixedSizeList(field, _)
            | DataType::Map(field, _) => DataType::List(field.clone()),
            scalar => DataType::new_list(scalar.clone(), true),
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let input = args.args[0].to_array(args.number_rows)?;
        let data_type = self.return_type(&[input.data_type().clone()])?;
        let DataType::List(field) = &data_type else {
            unreachable!()
        };
        let list = match input.data_type() {
            DataType::List(_) => input,
            DataType::ListView(_)
            | DataType::LargeList(_)
            | DataType::LargeListView(_)
            | DataType::FixedSizeList(_, _) => cast(&input, &data_type)?,
            DataType::Map(..) => {
                let map = input
                    .as_any()
                    .downcast_ref::<MapArray>()
                    .expect("Arrow map");
                Arc::new(ListArray::try_new(
                    field.clone(),
                    map.offsets().clone(),
                    Arc::new(map.entries().clone()),
                    map.nulls().cloned(),
                )?)
            }
            _ => Arc::new(ListArray::try_new(
                field.clone(),
                OffsetBuffer::from_lengths(std::iter::repeat_n(1, input.len())),
                input,
                None,
            )?),
        };
        Ok(ColumnarValue::Array(list))
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct ElementId(Signature);

impl ScalarUDFImpl for ElementId {
    fn name(&self) -> &str {
        "__graphlake_element_id"
    }

    fn signature(&self) -> &Signature {
        &self.0
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let label = cast(&args.args[0].to_array(args.number_rows)?, &DataType::Utf8)?;
        let key = cast(
            &args.args[1].to_array(args.number_rows)?,
            &DataType::new_list(DataType::Utf8, true),
        )?;
        let labels = label
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Arrow text");
        let keys = key
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("Arrow list");
        let mut output = StringBuilder::new();
        let mut id = String::new();
        for (label, key) in labels.iter().zip(keys.iter()) {
            let (Some(label), Some(key)) = (label, key) else {
                output.append_null();
                continue;
            };
            let key = key
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Arrow text");
            if key.null_count() > 0 {
                output.append_null();
                continue;
            }
            id.clear();
            crate::cypher::escape_id_component(label, &mut id);
            id.push('[');
            for (index, part) in key.iter().flatten().enumerate() {
                if index > 0 {
                    id.push(',');
                }
                crate::cypher::escape_id_component(part, &mut id);
            }
            id.push(']');
            output.append_value(&id);
        }
        Ok(ColumnarValue::Array(Arc::new(output.finish())))
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct GroupMap(Signature);

impl ScalarUDFImpl for GroupMap {
    fn name(&self) -> &str {
        "__graphlake_gremlin_group_map"
    }

    fn signature(&self) -> &Signature {
        &self.0
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        let DataType::List(item) = &args[0] else {
            return plan_err!("group entries must be a list");
        };
        let DataType::Struct(fields) = item.data_type() else {
            return plan_err!("group entries must contain key and value");
        };
        let [key, value] = fields.as_ref() else {
            return plan_err!("group entries must contain key and value");
        };
        let fields = Fields::from(vec![
            Field::new("key", key.data_type().clone(), false),
            Field::new("value", value.data_type().clone(), true),
        ]);
        Ok(DataType::Map(
            Arc::new(Field::new("entries", DataType::Struct(fields), false)),
            false,
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let input = args.args[0].to_array(args.number_rows)?;
        let map = self.return_type(&[input.data_type().clone()])?;
        let DataType::Map(field, _) = map else {
            unreachable!()
        };
        let DataType::Struct(fields) = field.data_type() else {
            unreachable!()
        };
        let list = input
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("Arrow list");
        let entries = list
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("Arrow struct");
        if entries.null_count() > 0 || entries.column(0).null_count() > 0 {
            return exec_err!("group entries must have non-null keys");
        }
        let entries = StructArray::try_new(fields.clone(), entries.columns().to_vec(), None)?;
        Ok(ColumnarValue::Array(Arc::new(MapArray::try_new(
            field,
            list.offsets().clone(),
            entries,
            list.nulls().cloned(),
            false,
        )?)))
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct TraverserText(Signature);

impl ScalarUDFImpl for TraverserText {
    fn name(&self) -> &str {
        "__graphlake_gremlin_text"
    }

    fn signature(&self) -> &Signature {
        &self.0
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8View)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let input = args.args[0].to_array(args.number_rows)?;
        if can_cast_types(input.data_type(), &DataType::Utf8View) {
            return Ok(ColumnarValue::Array(cast(&input, &DataType::Utf8View)?));
        }
        let formatter = ArrayFormatter::try_new(input.as_ref(), &FormatOptions::default())?;
        let mut output = StringViewBuilder::new();
        for index in 0..input.len() {
            if input.is_null(index) {
                output.append_null();
            } else {
                output.append_value(formatter.value(index).try_to_string()?);
            }
        }
        Ok(ColumnarValue::Array(Arc::new(output.finish())))
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Properties(Signature);

impl ScalarUDFImpl for Properties {
    fn name(&self) -> &str {
        "__graphlake_cypher_properties"
    }

    fn signature(&self) -> &Signature {
        &self.0
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        match &args[0] {
            ty @ (DataType::Struct(_) | DataType::Map(..) | DataType::Null) => Ok(ty.clone()),
            _ => plan_err!("properties() requires a graph element or map"),
        }
    }

    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(args.args.remove(0))
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct QueryValue {
    name: &'static str,
    signature: Signature,
}

impl ScalarUDFImpl for QueryValue {
    fn name(&self) -> &str {
        self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        match self.name {
            "__graphlake_key_text" => Ok(DataType::Utf8),
            "__graphlake_gremlin_number" if args[0].is_numeric() || args[0].is_null() => {
                Ok(DataType::Float64)
            }
            "__graphlake_gremlin_number" => {
                plan_err!("mean() requires numeric values, got {}", args[0])
            }
            "__graphlake_gremlin_key" => Ok(DataType::Utf8View),
            "__graphlake_gremlin_repeat_depth" if args[0] == DataType::Int64 => Ok(DataType::Int64),
            "__graphlake_gremlin_repeat_depth" => plan_err!("repeat depth must be an integer"),
            "__graphlake_numeric_input" => Ok(numeric_type(args)),
            "__graphlake_gremlin_sum_zero" if args[0].is_numeric() => Ok(args[0].clone()),
            "__graphlake_gremlin_sum_zero" => plan_err!("sum() requires numeric values"),
            "__graphlake_cypher_index" if args[0].is_integer() || args[0].is_null() => {
                Ok(DataType::Int64)
            }
            "__graphlake_cypher_index" => {
                plan_err!("Cypher list index must be an integer, got {}", args[0])
            }
            _ => unreachable!("registered query value function"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if self.name == "__graphlake_numeric_input" {
            let inputs = args
                .args
                .iter()
                .map(|input| input.to_array(args.number_rows))
                .collect::<Result<Vec<_>>>()?;
            return Ok(ColumnarValue::Array(numeric_array(
                inputs,
                args.return_field.data_type(),
                args.number_rows,
            )?));
        }
        let input = args.args[0].to_array(args.number_rows)?;
        let value = match self.name {
            "__graphlake_gremlin_number" => cast(&input, &DataType::Float64)?,
            "__graphlake_gremlin_sum_zero" => {
                let ty = input.data_type().clone();
                let zero = ScalarValue::Int64(Some(0))
                    .cast_to(&ty)?
                    .to_array_of_size(input.len())?;
                cypher::merge_lanes(&[input, zero], &ty, args.number_rows)?
            }
            "__graphlake_cypher_index" => cast(&input, &DataType::Int64)?,
            "__graphlake_gremlin_key" => group_key(&input)?,
            "__graphlake_key_text" => key_text(&input)?,
            "__graphlake_gremlin_repeat_depth" => {
                if input
                    .as_primitive::<Int64Type>()
                    .iter()
                    .flatten()
                    .any(|round| round > crate::gremlin::MAX_REPEAT_ROUNDS as i64)
                {
                    return exec_err!("repeat() is limited to 256 iterations");
                }
                input
            }
            _ => unreachable!("registered query value function"),
        };
        Ok(ColumnarValue::Array(value))
    }
}

pub(crate) fn key_text(input: &ArrayRef) -> Result<ArrayRef> {
    if !matches!(
        input.data_type(),
        DataType::Binary
            | DataType::BinaryView
            | DataType::LargeBinary
            | DataType::FixedSizeBinary(_)
    ) {
        return Ok(cast(input, &DataType::Utf8)?);
    }
    let formatter = ArrayFormatter::try_new(input.as_ref(), &FormatOptions::default())?;
    let mut output = StringBuilder::new();
    for row in 0..input.len() {
        if input.is_null(row) {
            output.append_null();
        } else {
            output.append_value(formatter.value(row).try_to_string()?);
        }
    }
    Ok(Arc::new(output.finish()))
}

fn group_key(input: &ArrayRef) -> Result<ArrayRef> {
    let ty = input.data_type();
    let format = ArrayFormatter::try_new(input.as_ref(), &FormatOptions::default())?;
    let nested = if ty.is_nested() {
        Some(
            RowConverter::new(vec![SortField::new(ty.clone())])?
                .convert_columns(std::slice::from_ref(input))?,
        )
    } else {
        None
    };
    let nulls = input.logical_nulls();
    let mut output = StringViewBuilder::new();
    // Arrow JSON requires string map keys. Escape string keys in the reserved
    // namespace and prefix other keys with their Arrow type before grouping.
    for index in 0..input.len() {
        if nulls.as_ref().is_some_and(|nulls| nulls.is_null(index)) {
            output.append_null();
            continue;
        }
        let value = if let Some(rows) = &nested {
            URL_SAFE_NO_PAD.encode(rows.row(index).as_ref())
        } else {
            format.value(index).try_to_string()?
        };
        if !cypher::text_type(ty) {
            output.append_value(format!("@{ty}:{value}"));
        } else if value.starts_with('@') {
            output.append_value(format!("@{value}"));
        } else {
            output.append_value(value);
        }
    }
    Ok(Arc::new(output.finish()))
}

fn numeric_array(inputs: Vec<ArrayRef>, ty: &DataType, rows: usize) -> Result<ArrayRef> {
    let mut slots = Vec::new();
    for input in inputs {
        cypher::flatten_value(input, &mut slots);
    }
    let mut values = Vec::new();
    for slot in slots {
        if slot.data_type().is_numeric() {
            values.push(cast_with_options(
                &slot,
                ty,
                &CastOptions {
                    safe: false,
                    ..Default::default()
                },
            )?);
        } else if slot.null_count() != slot.len() {
            return exec_err!(
                "numeric operation requires numeric values, got {}",
                slot.data_type()
            );
        }
    }
    cypher::merge_lanes(&values, ty, rows)
}

fn numeric_type(args: &[DataType]) -> DataType {
    let mut types = Vec::new();
    for ty in args {
        cypher::value_types(ty, &mut types);
    }
    types.retain(DataType::is_numeric);
    let args = &types;
    if args.iter().any(DataType::is_floating) {
        return DataType::Float64;
    }
    let signed = args.iter().any(DataType::is_signed_integer);
    let unsigned = args.iter().any(DataType::is_unsigned_integer);
    let scale = args
        .iter()
        .filter_map(|ty| match ty {
            DataType::Decimal32(_, scale)
            | DataType::Decimal64(_, scale)
            | DataType::Decimal128(_, scale)
            | DataType::Decimal256(_, scale) => Some(*scale),
            _ => None,
        })
        .max();
    if let Some(scale) = scale {
        let scale = if signed || unsigned {
            scale.max(0)
        } else {
            scale
        };
        if args.iter().any(|ty| matches!(ty, DataType::Decimal256(..))) || scale > 38 {
            return DataType::Decimal256(76, scale);
        }
        return DataType::Decimal128(38, scale);
    }
    match (signed, unsigned) {
        (true, true) => DataType::Decimal128(38, 0),
        (false, true) => DataType::UInt64,
        _ => DataType::Int64,
    }
}
