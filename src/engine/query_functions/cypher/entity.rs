use std::{collections::BTreeMap, sync::Arc};

use datafusion::{
    arrow::{
        array::{Array, ArrayRef, ListArray, StructArray},
        datatypes::{DataType, Field, Fields},
    },
    common::{Result, ScalarValue, plan_err},
    logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    },
    prelude::SessionContext,
};

use super::{ScopeGuard, common_value_type, merge_values, text_type};

const ENTITY: &str = "graphlake.cypher.entity";

pub(super) fn register(context: &SessionContext, guard: Option<ScopeGuard>) {
    for public in [false, true] {
        context.register_udf(ScalarUDF::from(Entity {
            public,
            signature: Signature::any(1, Volatility::Immutable),
            guard: guard.clone(),
        }));
    }
}

fn is_entity(fields: &Fields) -> bool {
    fields
        .first()
        .is_some_and(|field| field.metadata().contains_key(ENTITY))
}

pub(super) fn identity(value: &ScalarValue) -> Result<Option<[ScalarValue; 3]>> {
    let ScalarValue::Struct(record) = value else {
        return Ok(None);
    };
    if !is_entity(record.fields()) {
        return Ok(None);
    }
    Ok(Some([
        ScalarValue::try_from_array(record.column(0), 0)?,
        ScalarValue::try_from_array(record.column(1), 0)?,
        ScalarValue::try_from_array(record.column(2), 0)?,
    ]))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Entity {
    public: bool,
    signature: Signature,
    guard: Option<ScopeGuard>,
}

impl ScalarUDFImpl for Entity {
    fn name(&self) -> &str {
        if self.public {
            "__graphlake_cypher_public"
        } else {
            "__graphlake_cypher_entity"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        if self.public {
            return Ok(public_type(&args[0]));
        }
        let DataType::Struct(fields) = &args[0] else {
            return plan_err!("Cypher entity requires a record");
        };
        let [kind, label, key, payload] = fields.as_ref() else {
            return plan_err!("Cypher entity requires a kind, label, key and payload");
        };
        if kind.name() != "kind"
            || !text_type(kind.data_type())
            || label.name() != "label"
            || !text_type(label.data_type())
            || key.name() != "key"
            || !matches!(key.data_type(), DataType::List(field) if text_type(field.data_type()))
            || !matches!(payload.data_type(), DataType::Struct(fields) if
                payload.name() == "properties" || payload.name() == "lanes" &&
                fields.iter().all(|field| matches!(field.data_type(), DataType::Struct(_))))
        {
            return plan_err!("Cypher entity has an invalid record layout");
        }
        let kind = fields[0]
            .as_ref()
            .clone()
            .with_metadata([(ENTITY.into(), "1".into())].into());
        Ok(DataType::Struct(
            std::iter::once(Arc::new(kind))
                .chain(fields.iter().skip(1).cloned())
                .collect(),
        ))
    }

    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let _broadcast = super::reserve_scalar_broadcast(
            &args.args[..1],
            args.number_rows,
            self.name(),
            self.guard.as_ref(),
        )?;
        let input = args.args.remove(0);
        let target = args.return_field.data_type();
        if input.data_type() == *target {
            return Ok(input);
        }
        let array = input.to_array(args.number_rows)?;
        if self.public {
            return Ok(ColumnarValue::Array(public_array(array, target)?));
        }
        let record = array
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("entity record");
        let DataType::Struct(fields) = target else {
            unreachable!("entity type")
        };
        Ok(ColumnarValue::Array(Arc::new(StructArray::try_new(
            fields.clone(),
            record.columns().to_vec(),
            record.nulls().cloned(),
        )?)))
    }
}

fn public_type(ty: &DataType) -> DataType {
    match ty {
        DataType::Struct(fields) if is_entity(fields) => {
            let payload = &fields[3];
            if payload.name() == "properties" {
                public_type(payload.data_type())
            } else {
                let DataType::Struct(lanes) = payload.data_type() else {
                    unreachable!("entity lanes")
                };
                let mut properties = BTreeMap::<&str, Vec<&DataType>>::new();
                for lane in lanes {
                    let DataType::Struct(fields) = lane.data_type() else {
                        unreachable!("property lane")
                    };
                    for field in fields {
                        properties
                            .entry(field.name())
                            .or_default()
                            .push(field.data_type());
                    }
                }
                let properties = properties
                    .into_iter()
                    .map(|(name, types)| {
                        Field::new(name, common_value_type(types.into_iter()), true)
                    })
                    .collect();
                DataType::Struct(
                    [
                        fields[1].clone(),
                        fields[2].clone(),
                        Arc::new(Field::new("properties", DataType::Struct(properties), true)),
                    ]
                    .into(),
                )
            }
        }
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|field| {
                    field
                        .as_ref()
                        .clone()
                        .with_data_type(public_type(field.data_type()))
                })
                .collect(),
        ),
        DataType::List(field) => DataType::List(Arc::new(
            field
                .as_ref()
                .clone()
                .with_data_type(public_type(field.data_type())),
        )),
        _ => ty.clone(),
    }
}

fn public_array(array: ArrayRef, ty: &DataType) -> Result<ArrayRef> {
    if array.data_type() == ty {
        return Ok(array);
    }
    if let Some(record) = array.as_any().downcast_ref::<StructArray>() {
        if is_entity(record.fields()) {
            let (_, columns) = record.flatten();
            let nulls = record.nulls().cloned();
            if record.fields()[3].name() == "properties" {
                return public_array(columns[3].clone(), ty);
            }
            let DataType::Struct(fields) = ty else {
                unreachable!("public entity")
            };
            let DataType::Struct(properties) = fields[2].data_type() else {
                unreachable!("properties type")
            };
            let lanes = columns[3]
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("entity lanes");
            let (_, lanes) = lanes.flatten();
            let lanes = lanes
                .iter()
                .map(|lane| {
                    lane.as_any()
                        .downcast_ref::<StructArray>()
                        .expect("property lane")
                        .flatten()
                })
                .collect::<Vec<_>>();
            let values = properties
                .iter()
                .map(|field| {
                    let arrays = lanes
                        .iter()
                        .filter_map(|(fields, arrays)| {
                            fields
                                .find(field.name())
                                .map(|(index, _)| arrays[index].clone())
                        })
                        .collect();
                    merge_values(arrays, field.data_type(), record.len())
                })
                .collect::<Result<Vec<_>>>()?;
            let payload = StructArray::try_new(properties.clone(), values, nulls.clone())?;
            return Ok(Arc::new(StructArray::try_new(
                fields.clone(),
                vec![columns[1].clone(), columns[2].clone(), Arc::new(payload)],
                nulls,
            )?));
        }
        let DataType::Struct(fields) = ty else {
            unreachable!("public record")
        };
        let columns = record
            .columns()
            .iter()
            .zip(fields)
            .map(|(column, field)| public_array(column.clone(), field.data_type()))
            .collect::<Result<Vec<_>>>()?;
        return Ok(Arc::new(StructArray::try_new(
            fields.clone(),
            columns,
            record.nulls().cloned(),
        )?));
    }
    let list = array
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("public list");
    let DataType::List(field) = ty else {
        unreachable!("public list type")
    };
    let values = public_array(list.values().clone(), field.data_type())?;
    Ok(Arc::new(ListArray::try_new(
        field.clone(),
        list.offsets().clone(),
        values,
        list.nulls().cloned(),
    )?))
}
