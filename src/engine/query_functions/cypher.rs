mod arithmetic;
pub(in crate::engine) mod collect;
mod entity;
mod hydrate;
mod map;
mod numeric_distinct;
mod range;
pub(in crate::engine) mod reduce;
mod sum;

use std::{
    cmp::Ordering,
    collections::HashSet,
    hash::{Hash, Hasher},
    sync::Arc,
};

use datafusion::{
    arrow::{
        array::{
            Array, ArrayRef, BinaryBuilder, BooleanBuilder, ListArray, MutableArrayData,
            StructArray, make_array, new_null_array,
        },
        buffer::{NullBuffer, OffsetBuffer},
        compute::cast,
        datatypes::{DataType, Field, Fields},
        row::{RowConverter, SortField},
    },
    common::{Result, ScalarValue, exec_err},
    execution::{
        memory_pool::{MemoryConsumer, MemoryReservation},
        runtime_env::RuntimeEnv,
    },
    logical_expr::{
        BinaryExpr, ColumnarValue, Expr, Operator, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
        Signature, Volatility, expr_fn,
        simplify::{ExprSimplifyResult, SimplifyContext},
        type_coercion::binary::BinaryTypeCoercer,
    },
    prelude::SessionContext,
};

#[derive(Clone, Debug)]
pub(super) struct ScopeGuard {
    pub(super) runtime: Arc<RuntimeEnv>,
    pub(super) cancelled: crate::CancellationToken,
    pub(super) deadline: tokio::time::Instant,
}

impl ScopeGuard {
    fn from_scope(scope: &crate::engine::query::QueryScope) -> Self {
        Self {
            runtime: Arc::clone(&scope.runtime),
            cancelled: scope.options.cancellation.clone(),
            deadline: scope.deadline,
        }
    }

    pub(super) fn check(&self) -> Result<()> {
        if self.cancelled.is_cancelled() {
            return exec_err!("query cancelled");
        }
        if tokio::time::Instant::now() >= self.deadline {
            return exec_err!("query deadline exceeded");
        }
        Ok(())
    }
}

impl PartialEq for ScopeGuard {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.runtime, &other.runtime) && self.deadline == other.deadline
    }
}

impl Eq for ScopeGuard {}

impl Hash for ScopeGuard {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.runtime).hash(state);
        self.deadline.hash(state);
    }
}

pub(super) fn reserve_scalar_broadcast(
    args: &[ColumnarValue],
    number_rows: usize,
    label: &str,
    guard: Option<&ScopeGuard>,
) -> Result<Option<MemoryReservation>> {
    let Some(guard) = guard else {
        return Ok(None);
    };
    guard.check()?;
    if number_rows <= 1 {
        return Ok(None);
    }
    let mut bytes = 0usize;
    for arg in args {
        if let ColumnarValue::Scalar(scalar) = arg {
            let one = crate::engine::query::allocation::slice_bytes(
                scalar.to_array_of_size(1)?.as_ref(),
            )?;
            bytes = bytes.saturating_add(one.saturating_mul(number_rows));
        }
    }
    if bytes == 0 {
        return Ok(None);
    }
    let reservation = MemoryConsumer::new(label).register(&guard.runtime.memory_pool);
    reservation.try_grow(bytes)?;
    Ok(Some(reservation))
}

pub(super) fn register_inner(
    context: &SessionContext,
    scope: Option<&crate::engine::query::QueryScope>,
) {
    let guard = scope.map(ScopeGuard::from_scope);
    for (name, signature) in [
        (
            "__graphlake_cypher_property",
            Signature::variadic_any(Volatility::Immutable),
        ),
        (
            "__graphlake_cypher_case",
            Signature::any(2, Volatility::Immutable),
        ),
    ] {
        context.register_udf(ScalarUDF::from(Property {
            name,
            signature,
            guard: guard.clone(),
        }));
    }
    context.register_udf(ScalarUDF::from(CypherList {
        signature: Signature::variadic_any(Volatility::Immutable),
        guard: guard.clone(),
    }));
    context.register_udf(ScalarUDF::from(Size(Signature::any(
        1,
        Volatility::Immutable,
    ))));
    entity::register(context, guard.clone());
    hydrate::register(context, guard.clone());
    arithmetic::register(context, guard.clone());
    map::register(context, guard.clone());
    numeric_distinct::register(context, guard.clone());
    sum::register(context);
    range::register(context);
    for mode in [
        KeyMode::Equivalence,
        KeyMode::Order,
        KeyMode::GremlinOrder,
        KeyMode::Capture,
    ] {
        context.register_udf(ScalarUDF::from(ValueKey {
            mode,
            signature: Signature::any(1, Volatility::Immutable),
            guard: guard.clone(),
        }));
    }
    for (name, arity) in [
        ("__graphlake_cypher_equals", 2),
        ("__graphlake_cypher_in", 2),
        ("__graphlake_cypher_compare", 3),
    ] {
        context.register_udf(ScalarUDF::from(Predicate {
            name,
            signature: Signature::any(arity, Volatility::Immutable),
            guard: guard.clone(),
        }));
    }
}

const VARIANT: &str = "graphlake.cypher.variant";

#[derive(Debug, PartialEq, Eq, Hash)]
struct Property {
    name: &'static str,
    signature: Signature,
    guard: Option<ScopeGuard>,
}

fn normalized(ty: &DataType) -> DataType {
    match ty {
        DataType::Dictionary(_, value) => normalized(value),
        ty if text_type(ty) => DataType::Utf8View,
        ty => ty.clone(),
    }
}

pub(super) fn value_types(ty: &DataType, types: &mut Vec<DataType>) {
    if let DataType::Struct(fields) = ty
        && is_variant(fields)
    {
        for field in fields {
            value_types(field.data_type(), types);
        }
    } else if !ty.is_null() {
        types.push(normalized(ty));
    }
}

fn common_value_type<'a>(inputs: impl Iterator<Item = &'a DataType>) -> DataType {
    let mut types = Vec::new();
    for ty in inputs {
        value_types(ty, &mut types);
    }
    let items = types
        .iter()
        .filter_map(|ty| match ty {
            DataType::List(item) => Some(item.data_type()),
            _ => None,
        })
        .collect::<Vec<_>>();
    if items.len() > 1 {
        let list = DataType::new_list(common_value_type(items.into_iter()), true);
        types.retain(|ty| !matches!(ty, DataType::List(_)));
        types.push(list);
    }
    types.sort_by_cached_key(|ty| (ty.to_string(), ty.clone()));
    types.dedup();
    match types.as_slice() {
        [] => DataType::Null,
        [ty] => ty.clone(),
        types => {
            // Each slot has a native Arrow type. Slots have a stable order even
            // when a CASE or a later UNION adds types to an existing value.
            let fields = types
                .iter()
                .enumerate()
                .map(|(index, ty)| {
                    let name = if types[..index]
                        .iter()
                        .any(|previous| previous.to_string() == ty.to_string())
                    {
                        format!("{ty}_{index}")
                    } else {
                        ty.to_string()
                    };
                    Field::new(name, ty.clone(), true)
                        .with_metadata([(VARIANT.into(), "1".into())].into())
                })
                .collect::<Fields>();
            DataType::Struct(fields)
        }
    }
}

pub(super) fn flatten_value(array: ArrayRef, lanes: &mut Vec<ArrayRef>) {
    if let Some(record) = array.as_any().downcast_ref::<StructArray>()
        && is_variant(record.fields())
    {
        for child in record.flatten().1 {
            flatten_value(child, lanes);
        }
    } else if !array.data_type().is_null() {
        lanes.push(array);
    }
}

fn merge_values(arrays: Vec<ArrayRef>, ty: &DataType, rows: usize) -> Result<ArrayRef> {
    let mut lanes = Vec::new();
    for array in arrays {
        flatten_value(array, &mut lanes);
    }
    if let DataType::Struct(fields) = ty
        && is_variant(fields)
    {
        let columns = fields
            .iter()
            .map(|field| merge_lanes(&lanes, field.data_type(), rows))
            .collect::<Result<Vec<_>>>()?;
        let present = (0..rows)
            .map(|index| columns.iter().any(|column| column.is_valid(index)))
            .collect::<Vec<_>>();
        Ok(Arc::new(StructArray::try_new(
            fields.clone(),
            columns,
            Some(NullBuffer::from(present)),
        )?))
    } else {
        merge_lanes(&lanes, ty, rows)
    }
}

impl ScalarUDFImpl for Property {
    fn name(&self) -> &str {
        self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        if self.name == "__graphlake_cypher_case" {
            if !matches!(&args[0], DataType::Struct(_) | DataType::Null) {
                return datafusion::common::plan_err!("CASE value must be a record");
            }
            let DataType::Struct(fields) = &args[1] else {
                return exec_err!("CASE branches must be a record");
            };
            return Ok(common_value_type(
                fields.iter().map(|field| field.data_type()),
            ));
        }
        Ok(common_value_type(args.iter()))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let _broadcast = reserve_scalar_broadcast(
            if self.name == "__graphlake_cypher_case" {
                &args.args[..1]
            } else {
                &args.args
            },
            args.number_rows,
            self.name,
            self.guard.as_ref(),
        )?;
        if self.name == "__graphlake_cypher_case" {
            if args.args[0].data_type().is_null() {
                return Ok(ColumnarValue::Array(new_null_array(
                    args.return_field.data_type(),
                    args.number_rows,
                )));
            }
            let input = args.args[0].to_array(args.number_rows)?;
            let Some(record) = input.as_any().downcast_ref::<StructArray>() else {
                return exec_err!("CASE value must be a record");
            };
            let _output_mem = if let Some(guard) = &self.guard {
                guard.check()?;
                let bytes = crate::engine::query::allocation::slice_bytes(input.as_ref())?;
                if bytes > 0 {
                    let reservation = MemoryConsumer::new("__graphlake_cypher_case output")
                        .register(&guard.runtime.memory_pool);
                    reservation.try_grow(bytes)?;
                    Some(reservation)
                } else {
                    None
                }
            } else {
                None
            };
            let arrays = record.flatten().1;
            return Ok(ColumnarValue::Array(merge_values(
                arrays,
                args.return_field.data_type(),
                args.number_rows,
            )?));
        }
        let arrays = args
            .args
            .iter()
            .map(|arg| arg.to_array(args.number_rows))
            .collect::<Result<Vec<_>>>()?;
        let _output_mem = if let Some(guard) = &self.guard {
            guard.check()?;
            let mut bytes = 0usize;
            for array in &arrays {
                bytes = bytes.saturating_add(crate::engine::query::allocation::slice_bytes(
                    array.as_ref(),
                )?);
            }
            if bytes > 0 {
                let reservation = MemoryConsumer::new("__graphlake_cypher_property output")
                    .register(&guard.runtime.memory_pool);
                reservation.try_grow(bytes)?;
                Some(reservation)
            } else {
                None
            }
        } else {
            None
        };
        Ok(ColumnarValue::Array(merge_values(
            arrays,
            args.return_field.data_type(),
            args.number_rows,
        )?))
    }
}

pub(super) fn merge_lanes(arrays: &[ArrayRef], ty: &DataType, rows: usize) -> Result<ArrayRef> {
    let arrays = arrays
        .iter()
        .filter_map(|a| {
            if a.data_type() == ty {
                return Some(Ok(Arc::clone(a)));
            }
            if let (DataType::List(item), Some(list)) = (ty, a.as_any().downcast_ref::<ListArray>())
            {
                return Some(
                    merge_values(
                        vec![Arc::clone(list.values())],
                        item.data_type(),
                        list.values().len(),
                    )
                    .and_then(|values| {
                        Ok(Arc::new(ListArray::try_new(
                            Arc::clone(item),
                            list.offsets().clone(),
                            values,
                            list.nulls().cloned(),
                        )?) as ArrayRef)
                    }),
                );
            }
            (!a.data_type().is_null() && normalized(a.data_type()) == *ty).then(|| Ok(cast(a, ty)?))
        })
        .collect::<Result<Vec<_>>>()?;
    if arrays.is_empty() {
        return Ok(new_null_array(ty, rows));
    }
    if arrays.len() == 1 {
        return Ok(arrays.into_iter().next().expect("single property lane"));
    }
    let data = arrays
        .iter()
        .map(|array| array.to_data())
        .collect::<Vec<_>>();
    let mut merged = MutableArrayData::new(data.iter().collect(), true, rows);
    for index in 0..rows {
        if let Some(lane) = arrays.iter().position(|array| array.is_valid(index)) {
            merged.try_extend(lane, index, index + 1)?;
        } else {
            merged.try_extend_nulls(1)?;
        }
    }
    Ok(make_array(merged.freeze()))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherList {
    signature: Signature,
    guard: Option<ScopeGuard>,
}

impl ScalarUDFImpl for CypherList {
    fn name(&self) -> &str {
        "__graphlake_cypher_list"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        Ok(DataType::new_list(common_value_type(args.iter()), true))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let _broadcast = reserve_scalar_broadcast(
            &args.args,
            args.number_rows,
            "__graphlake_cypher_list broadcast",
            self.guard.as_ref(),
        )?;
        let arrays = args
            .args
            .iter()
            .map(|arg| arg.to_array(args.number_rows))
            .collect::<Result<Vec<_>>>()?;
        let _output_mem = if let Some(guard) = &self.guard {
            guard.check()?;
            let mut bytes = 0usize;
            for array in &arrays {
                bytes = bytes.saturating_add(crate::engine::query::allocation::slice_bytes(
                    array.as_ref(),
                )?);
            }
            if bytes > 0 {
                let reservation = MemoryConsumer::new("__graphlake_cypher_list output")
                    .register(&guard.runtime.memory_pool);
                reservation.try_grow(bytes)?;
                Some(reservation)
            } else {
                None
            }
        } else {
            None
        };
        let types = arrays
            .iter()
            .map(|array| array.data_type().clone())
            .collect::<Vec<_>>();
        let width = arrays.len();
        let rows = args.number_rows;
        if width
            .checked_mul(rows)
            .is_none_or(|len| len > i32::MAX as usize)
        {
            return exec_err!("Cypher list exceeds the Arrow offset range");
        }
        let DataType::List(item) = self.return_type(&types)? else {
            unreachable!()
        };
        let arrays = arrays
            .into_iter()
            .map(|array| merge_values(vec![array], item.data_type(), rows))
            .collect::<Result<Vec<_>>>()?;
        let values = interleave_list(&arrays, item.data_type(), rows, self.guard.as_ref())?;
        let offsets = OffsetBuffer::from_lengths(std::iter::repeat_n(width, rows));
        Ok(ColumnarValue::Array(Arc::new(ListArray::try_new(
            item, offsets, values, None,
        )?)))
    }
}

fn interleave_list(
    arrays: &[ArrayRef],
    ty: &DataType,
    rows: usize,
    guard: Option<&ScopeGuard>,
) -> Result<ArrayRef> {
    if arrays.is_empty() {
        return Ok(new_null_array(ty, 0));
    }
    let data = arrays
        .iter()
        .map(|array| array.to_data())
        .collect::<Vec<_>>();
    let mut values = MutableArrayData::new(data.iter().collect(), true, rows * arrays.len());
    for row in 0..rows {
        if row.is_multiple_of(256)
            && let Some(guard) = guard
        {
            guard.check()?;
        }
        for lane in 0..arrays.len() {
            values.try_extend(lane, row, row + 1)?;
        }
    }
    Ok(make_array(values.freeze()))
}

pub(in crate::engine) fn is_variant(fields: &Fields) -> bool {
    fields
        .first()
        .is_some_and(|field| field.metadata().contains_key(VARIANT))
}

#[cfg(test)]
mod memory_tests {
    use super::*;
    use datafusion::arrow::{array::Int64Array, datatypes::Int64Type};

    async fn scope(bytes: usize) -> anyhow::Result<crate::engine::query::QueryScope> {
        let admission = Arc::new(crate::engine::query::Admission::new(
            crate::EngineLimits::default(),
        )?);
        crate::engine::query::QueryScope::new(
            crate::QueryOptions {
                memory_bytes: bytes,
                ..Default::default()
            },
            &admission,
        )
        .await
    }

    #[tokio::test]
    async fn distinct_numeric_groups_reuse_set_budget() -> anyhow::Result<()> {
        let scope = scope(128 * 1024).await?;
        let guard = ScopeGuard::from_scope(&scope);
        let input = ListArray::from_iter_primitive::<Int64Type, _, _>(
            (0..128).map(|_| Some((0..64).map(Some))),
        );
        let result = distinct_list(Arc::new(input.clone()), Some(&guard))?;
        assert_eq!(result.as_any().downcast_ref::<ListArray>().unwrap(), &input);
        Ok(())
    }

    #[tokio::test]
    async fn scalar_in_duplicate_values_only_reserve_unique_keys() -> anyhow::Result<()> {
        use datafusion::arrow::array::BooleanArray;
        let scope = scope(4096).await?;
        let predicate = Predicate {
            name: "__graphlake_cypher_in",
            signature: Signature::any(2, Volatility::Immutable),
            guard: Some(ScopeGuard::from_scope(&scope)),
        };
        let list =
            ListArray::from_iter_primitive::<Int64Type, _, _>([Some((0..8192).map(|_| Some(7)))]);
        let input = Arc::new(Int64Array::from(vec![7, 8])) as ArrayRef;
        let result = predicate
            .invoke_scalar_in(&input, &ScalarValue::List(Arc::new(list)), 2)?
            .into_array(2)?;
        assert_eq!(
            result.as_any().downcast_ref::<BooleanArray>().unwrap(),
            &BooleanArray::from(vec![true, false])
        );
        Ok(())
    }

    #[tokio::test]
    async fn cypher_udf_edge_cases_and_dictionary_text_do_not_panic() -> anyhow::Result<()> {
        let scope = scope(4 * 1024 * 1024).await?;
        let context = scope.session();
        register_inner(&context, Some(&scope));

        let case_null = context
            .sql("SELECT __graphlake_cypher_case(NULL, named_struct('a', 1)) AS v")
            .await?
            .collect()
            .await?;
        assert_eq!(case_null.len(), 1);
        assert!(case_null[0].column(0).is_null(0));

        let bad_cmp = async {
            context
                .sql("SELECT __graphlake_cypher_compare(1, 2, 'bad') AS v")
                .await?
                .collect()
                .await
        }
        .await;
        assert!(bad_cmp.is_err(), "invalid compare operator must return Err");

        let dict_ops = context
            .sql(
                "SELECT \
                 __graphlake_cypher_add(arrow_cast('hi', 'Dictionary(Int32, Utf8)'), '_x') AS joined, \
                 __graphlake_cypher_size(arrow_cast('hello', 'Dictionary(Int32, Utf8)')) AS len",
            )
            .await?
            .collect()
            .await?;
        assert_eq!(dict_ops.len(), 1);
        assert_eq!(
            ScalarValue::try_from_array(dict_ops[0].column(0), 0)?,
            ScalarValue::Utf8View(Some("hi_x".into()))
        );
        assert_eq!(
            ScalarValue::try_from_array(dict_ops[0].column(1), 0)?,
            ScalarValue::UInt64(Some(5))
        );
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Predicate {
    name: &'static str,
    signature: Signature,
    guard: Option<ScopeGuard>,
}

impl ScalarUDFImpl for Predicate {
    fn name(&self) -> &str {
        self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        let operator = match (self.name, args.as_slice()) {
            ("__graphlake_cypher_equals", [_, _]) => Some(Operator::Eq),
            (
                "__graphlake_cypher_compare",
                [
                    _,
                    _,
                    Expr::Literal(ScalarValue::Utf8(Some(op)) | ScalarValue::Utf8View(Some(op)), _),
                ],
            ) => sql_operator(op),
            _ => None,
        };
        if let Some(operator) = operator {
            let left = info.get_data_type(&args[0])?;
            let right = info.get_data_type(&args[1])?;
            if (plain_text_type(&left) && plain_text_type(&right))
                || (left == DataType::Boolean && right == DataType::Boolean)
            {
                return Ok(ExprSimplifyResult::Simplified(Expr::BinaryExpr(
                    BinaryExpr::new(
                        Box::new(args[0].clone()),
                        operator,
                        Box::new(args[1].clone()),
                    ),
                )));
            }
            // Prefer narrowing an exact literal for predicate pushdown.
            // Otherwise widen both integers to DataFusion's lossless common type.
            if left.is_integer() && right.is_integer() {
                let operands = if left == right {
                    Some((args[0].clone(), args[1].clone()))
                } else if let Some(value) = integer_literal_as(&args[1], &left)? {
                    Some((args[0].clone(), value))
                } else if let Some(value) = integer_literal_as(&args[0], &right)? {
                    Some((value, args[1].clone()))
                } else if left.is_signed_integer() == right.is_signed_integer() {
                    let (lhs, rhs) =
                        BinaryTypeCoercer::new(&left, &operator, &right).get_input_types()?;
                    let widen = |expr: &Expr, actual: DataType, target: DataType| {
                        if actual == target {
                            expr.clone()
                        } else {
                            expr_fn::cast(expr.clone(), target)
                        }
                    };
                    Some((widen(&args[0], left, lhs), widen(&args[1], right, rhs)))
                } else {
                    None
                };
                if let Some((left, right)) = operands {
                    return Ok(ExprSimplifyResult::Simplified(Expr::BinaryExpr(
                        BinaryExpr::new(Box::new(left), operator, Box::new(right)),
                    )));
                }
            }
        }
        Ok(ExprSimplifyResult::Original(args))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let _left_broadcast = reserve_scalar_broadcast(
            &args.args[..1],
            args.number_rows,
            self.name,
            self.guard.as_ref(),
        )?;
        let input = args.args[0].to_array(args.number_rows)?;
        if self.name == "__graphlake_cypher_in"
            && let ColumnarValue::Scalar(ref right_scalar) = args.args[1]
        {
            return self.invoke_scalar_in(&input, right_scalar, args.number_rows);
        }
        let _right_broadcast = reserve_scalar_broadcast(
            &args.args[1..2],
            args.number_rows,
            self.name,
            self.guard.as_ref(),
        )?;
        let right = args.args[1].to_array(args.number_rows)?;
        let scalar_op = if self.name == "__graphlake_cypher_compare"
            && let ColumnarValue::Scalar(scalar) = &args.args[2]
        {
            let ScalarValue::Utf8(Some(op)) = scalar.cast_to(&DataType::Utf8)? else {
                return exec_err!("missing comparison operator");
            };
            Some(op)
        } else {
            None
        };
        let op = if self.name == "__graphlake_cypher_compare" && scalar_op.is_none() {
            Some(args.args[2].to_array(args.number_rows)?)
        } else {
            None
        };
        let mut output = BooleanBuilder::with_capacity(args.number_rows);
        for row in 0..args.number_rows {
            if row.is_multiple_of(256)
                && let Some(guard) = &self.guard
            {
                guard.check()?;
            }
            let left = unpack(ScalarValue::try_from_array(&input, row)?)?;
            let right = unpack(ScalarValue::try_from_array(&right, row)?)?;
            let result = match self.name {
                "__graphlake_cypher_in" => membership(&left, &right, self.guard.as_ref())?,
                "__graphlake_cypher_equals" => equal(&left, &right)?,
                _ => {
                    let row_op;
                    let op = if let Some(op) = scalar_op.as_deref() {
                        op
                    } else {
                        let op_array = op.as_ref().expect("comparison operator column");
                        let value =
                            ScalarValue::try_from_array(op_array, row)?.cast_to(&DataType::Utf8)?;
                        let ScalarValue::Utf8(Some(text)) = value else {
                            return exec_err!("missing comparison operator");
                        };
                        row_op = text;
                        row_op.as_str()
                    };
                    match op {
                        "=" => equal(&left, &right)?,
                        "<>" => equal(&left, &right)?.map(|same| !same),
                        _ if left.is_null() || right.is_null() => None,
                        _ if float(&left).is_some_and(f64::is_nan)
                            || float(&right).is_some_and(f64::is_nan) =>
                        {
                            Some(false)
                        }
                        _ => ordering(&left, &right)?
                            .map(|order| match op {
                                "<" => Ok(order.is_lt()),
                                "<=" => Ok(!order.is_gt()),
                                ">" => Ok(order.is_gt()),
                                ">=" => Ok(!order.is_lt()),
                                other => exec_err!("unsupported comparison operator {other}"),
                            })
                            .transpose()?,
                    }
                }
            };
            output.append_option(result);
        }
        Ok(ColumnarValue::Array(Arc::new(output.finish())))
    }
}

impl Predicate {
    fn invoke_scalar_in(
        &self,
        input: &ArrayRef,
        right_scalar: &ScalarValue,
        number_rows: usize,
    ) -> Result<ColumnarValue> {
        let right = unpack(right_scalar.clone())?;
        let mut output = BooleanBuilder::with_capacity(number_rows);
        if right.is_null() {
            for _ in 0..number_rows {
                output.append_null();
            }
            return Ok(ColumnarValue::Array(Arc::new(output.finish())));
        }
        let Some(items) = list_items(&right) else {
            return exec_err!("Cypher IN expects a list");
        };
        if items.is_empty() {
            for _ in 0..number_rows {
                output.append_value(false);
            }
            return Ok(ColumnarValue::Array(Arc::new(output.finish())));
        }
        let item_type = match items.data_type() {
            DataType::Dictionary(_, value) => value.as_ref(),
            other => other,
        };
        let flat_rhs = !item_type.is_nested();
        if flat_rhs && number_rows > 1 {
            let reservation = self.guard.as_ref().map(|guard| {
                MemoryConsumer::new("__graphlake_cypher_in set")
                    .register(&guard.runtime.memory_pool)
            });
            let mut seen = HashSet::new();
            let mut has_null = false;
            for index in 0..items.len() {
                if index.is_multiple_of(256)
                    && let Some(guard) = &self.guard
                {
                    guard.check()?;
                }
                let item = unpack(ScalarValue::try_from_array(&items, index)?)?;
                if item.is_null() {
                    has_null = true;
                    continue;
                }
                if float(&item).is_some_and(f64::is_nan) {
                    continue;
                }
                let mut key = Vec::new();
                encode_equivalence(item, &mut key)?;
                if !seen.contains(&key) {
                    if let Some(reservation) = &reservation {
                        reservation.try_grow(key.capacity().saturating_add(32))?;
                    }
                    seen.insert(key);
                }
            }
            let mut key = Vec::new();
            for row in 0..number_rows {
                if row.is_multiple_of(256)
                    && let Some(guard) = &self.guard
                {
                    guard.check()?;
                }
                let left = unpack(ScalarValue::try_from_array(input, row)?)?;
                let result = if left.is_null() {
                    None
                } else if float(&left).is_some_and(f64::is_nan) || left.data_type().is_nested() {
                    if has_null { None } else { Some(false) }
                } else {
                    key.clear();
                    encode_equivalence(left, &mut key)?;
                    if seen.contains(&key) {
                        Some(true)
                    } else if has_null {
                        None
                    } else {
                        Some(false)
                    }
                };
                output.append_option(result);
            }
            return Ok(ColumnarValue::Array(Arc::new(output.finish())));
        }
        for row in 0..number_rows {
            if row.is_multiple_of(256)
                && let Some(guard) = &self.guard
            {
                guard.check()?;
            }
            let left = unpack(ScalarValue::try_from_array(input, row)?)?;
            output.append_option(membership(&left, &right, self.guard.as_ref())?);
        }
        Ok(ColumnarValue::Array(Arc::new(output.finish())))
    }
}

fn unpack(value: ScalarValue) -> Result<ScalarValue> {
    if let ScalarValue::Dictionary(_, inner) = value {
        return unpack(*inner);
    }
    if let ScalarValue::Struct(values) = &value
        && is_variant(values.fields())
    {
        return values
            .columns()
            .iter()
            .find(|array| array.is_valid(0) && values.is_valid(0))
            .map_or(Ok(ScalarValue::Null), |array| {
                unpack(ScalarValue::try_from_array(array, 0)?)
            });
    }
    Ok(value)
}

fn list_items(value: &ScalarValue) -> Option<ArrayRef> {
    match value {
        ScalarValue::List(array) => Some(array.value(0)),
        ScalarValue::LargeList(array) => Some(array.value(0)),
        ScalarValue::FixedSizeList(array) => Some(array.value(0)),
        ScalarValue::ListView(array) => Some(array.value(0)),
        ScalarValue::LargeListView(array) => Some(array.value(0)),
        _ => None,
    }
}

fn membership(
    left: &ScalarValue,
    right: &ScalarValue,
    guard: Option<&ScopeGuard>,
) -> Result<Option<bool>> {
    if right.is_null() {
        return Ok(None);
    }
    let Some(items) = list_items(right) else {
        return exec_err!("Cypher IN expects a list");
    };
    let mut found = Some(false);
    for index in 0..items.len() {
        if index.is_multiple_of(256)
            && let Some(guard) = guard
        {
            guard.check()?;
        }
        match equal(left, &unpack(ScalarValue::try_from_array(&items, index)?)?)? {
            Some(true) => return Ok(Some(true)),
            None => found = None,
            Some(false) => {}
        }
    }
    Ok(found)
}

fn plain_text_type(value: &DataType) -> bool {
    matches!(
        value,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

pub(super) fn text_type(value: &DataType) -> bool {
    match value {
        DataType::Dictionary(_, inner) => text_type(inner),
        other => plain_text_type(other),
    }
}

fn integer_literal_as(expr: &Expr, ty: &DataType) -> Result<Option<Expr>> {
    let Expr::Literal(value, metadata) = expr else {
        return Ok(None);
    };
    let Ok(converted) = value.cast_to(ty) else {
        return Ok(None);
    };
    Ok((converted.cast_to(&value.data_type())? == *value)
        .then(|| Expr::Literal(converted, metadata.clone())))
}

fn sql_operator(op: &str) -> Option<Operator> {
    Some(match op {
        "=" => Operator::Eq,
        "<>" => Operator::NotEq,
        "<" => Operator::Lt,
        "<=" => Operator::LtEq,
        ">" => Operator::Gt,
        ">=" => Operator::GtEq,
        _ => return None,
    })
}

fn float(value: &ScalarValue) -> Option<f64> {
    match value {
        ScalarValue::Float16(value) => value.map(|value| f64::from(value.to_f32())),
        ScalarValue::Float32(value) => value.map(f64::from),
        ScalarValue::Float64(value) => *value,
        _ => None,
    }
}

fn numeric_order(left: &ScalarValue, right: &ScalarValue) -> Option<Ordering> {
    let a = float(left);
    let b = float(right);
    if let (Some(a), Some(b)) = (a, b) {
        return a.partial_cmp(&b);
    }
    if let Some(a) = a.filter(|n| !n.is_finite()) {
        return a.partial_cmp(&0.0);
    }
    if let Some(b) = b.filter(|n| !n.is_finite()) {
        return 0.0f64.partial_cmp(&b);
    }
    if left.data_type() == right.data_type() {
        return left.partial_cmp(right);
    }
    Some(crate::scalar::finite_decimal(left)?.cmp(&crate::scalar::finite_decimal(right)?))
}

fn ordering(left: &ScalarValue, right: &ScalarValue) -> Result<Option<Ordering>> {
    if let (Some(left), Some(right)) = (list_items(left), list_items(right)) {
        for index in 0..left.len().min(right.len()) {
            let a = unpack(ScalarValue::try_from_array(&left, index)?)?;
            let b = unpack(ScalarValue::try_from_array(&right, index)?)?;
            if a.is_null() || b.is_null() {
                return Ok(None);
            }
            let result = ordering(&a, &b)?;
            if result != Some(Ordering::Equal) {
                return Ok(result);
            }
        }
        return Ok(Some(left.len().cmp(&right.len())));
    }
    let left_id = entity::identity(left)?;
    let right_id = entity::identity(right)?;
    if left_id.is_some() || right_id.is_some() {
        return match (left_id, right_id) {
            (Some([left_kind, ..]), Some([right_kind, ..]))
                if equal(&left_kind, &right_kind)? == Some(true) =>
            {
                let mut a = Vec::new();
                let mut b = Vec::new();
                encode_order(left.clone(), &mut a, false)?;
                encode_order(right.clone(), &mut b, false)?;
                Ok(Some(a.cmp(&b)))
            }
            _ => Ok(None),
        };
    }
    let a = left.data_type();
    let b = right.data_type();
    Ok(if a.is_numeric() && b.is_numeric() {
        numeric_order(left, right)
    } else if text_type(&a) && text_type(&b) {
        left.cast_to(&DataType::Utf8)?
            .partial_cmp(&right.cast_to(&DataType::Utf8)?)
    } else if a == b {
        left.partial_cmp(right)
    } else {
        None
    })
}

fn equal(left: &ScalarValue, right: &ScalarValue) -> Result<Option<bool>> {
    if left.is_null() || right.is_null() {
        return Ok(None);
    }
    let left_id = entity::identity(left)?;
    let right_id = entity::identity(right)?;
    if left_id.is_some() || right_id.is_some() {
        return match (left_id, right_id) {
            (Some(left), Some(right)) => equal_items(left.into_iter().zip(right).map(Ok)),
            _ => Ok(Some(false)),
        };
    }
    if let (Some(left), Some(right)) = (list_items(left), list_items(right)) {
        if left.len() != right.len() {
            return Ok(Some(false));
        }
        return equal_items((0..left.len()).map(|index| {
            Ok((
                ScalarValue::try_from_array(&left, index)?,
                ScalarValue::try_from_array(&right, index)?,
            ))
        }));
    }
    if let (ScalarValue::Struct(left), ScalarValue::Struct(right)) = (left, right) {
        if left.num_columns() != right.num_columns()
            || left
                .fields()
                .iter()
                .any(|field| right.column_by_name(field.name()).is_none())
        {
            return Ok(Some(false));
        }
        return equal_items(
            left.fields()
                .iter()
                .zip(left.columns())
                .map(|(field, column)| {
                    let other = right.column_by_name(field.name()).expect("same map keys");
                    Ok((
                        ScalarValue::try_from_array(column, 0)?,
                        ScalarValue::try_from_array(other, 0)?,
                    ))
                }),
        );
    }
    if left.data_type() == right.data_type() && !left.data_type().is_numeric() {
        return Ok(Some(left == right));
    }
    Ok(Some(ordering(left, right)? == Some(Ordering::Equal)))
}

fn equal_items(
    items: impl Iterator<Item = Result<(ScalarValue, ScalarValue)>>,
) -> Result<Option<bool>> {
    let mut result = Some(true);
    for item in items {
        let (a, b) = item?;
        match equal(&unpack(a)?, &unpack(b)?)? {
            Some(false) => return Ok(Some(false)),
            None => result = None,
            Some(true) => {}
        }
    }
    Ok(result)
}

#[derive(Debug, PartialEq, Eq, Hash)]
enum KeyMode {
    Equivalence,
    Order,
    GremlinOrder,
    Capture,
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct ValueKey {
    mode: KeyMode,
    signature: Signature,
    guard: Option<ScopeGuard>,
}

impl ValueKey {
    fn encodes(&self, ty: &DataType) -> bool {
        self.mode == KeyMode::GremlinOrder || needs_equivalence_key(ty)
    }
}

impl ScalarUDFImpl for ValueKey {
    fn name(&self) -> &str {
        match self.mode {
            KeyMode::Equivalence => "__graphlake_cypher_equivalence",
            KeyMode::Order => "__graphlake_cypher_order",
            KeyMode::GremlinOrder => "__graphlake_gremlin_order",
            KeyMode::Capture => "__graphlake_cypher_capture_key",
        }
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        let ty = &args[0];
        Ok(if self.encodes(ty) {
            DataType::Binary
        } else {
            ty.clone()
        })
    }

    fn simplify(&self, mut args: Vec<Expr>, info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        if !self.encodes(&info.get_data_type(&args[0])?) {
            return Ok(ExprSimplifyResult::Simplified(args.remove(0)));
        }
        Ok(ExprSimplifyResult::Original(args))
    }

    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let _broadcast = reserve_scalar_broadcast(
            &args.args[..1],
            args.number_rows,
            self.name(),
            self.guard.as_ref(),
        )?;
        let input = args.args.remove(0);
        if !self.encodes(&input.data_type()) {
            return Ok(input);
        }
        let input = input.to_array(args.number_rows)?;
        let reservation = self
            .guard
            .as_ref()
            .map(|guard| MemoryConsumer::new(self.name()).register(&guard.runtime.memory_pool));
        if self.mode == KeyMode::Capture {
            // A reusable CALL input must preserve values that compare equal
            // but behave differently in expressions, such as 1 and 1.0.
            let converter = RowConverter::new(vec![SortField::new(input.data_type().clone())])?;
            let rows = converter.convert_columns(&[input])?;
            if let Some(reservation) = &reservation {
                reservation.try_grow(rows.size())?;
            }
            let mut keys = BinaryBuilder::new();
            for (index, row) in rows.iter().enumerate() {
                if index.is_multiple_of(256)
                    && let Some(guard) = &self.guard
                {
                    guard.check()?;
                }
                let bytes = row.as_ref();
                if let Some(reservation) = &reservation {
                    reservation.try_grow(bytes.len().saturating_add(4))?;
                }
                keys.append_value(bytes);
            }
            return Ok(ColumnarValue::Array(Arc::new(keys.finish())));
        }
        // Vectorized batch path for fallback scalar types (Date, Timestamp, Binary, etc.)
        // to avoid per-row RowConverter::new + ty.to_string() + to_array() allocations.
        let ty = input.data_type();
        let is_fallback_scalar = !ty.is_null()
            && !ty.is_numeric()
            && !text_type(ty)
            && *ty != DataType::Boolean
            && !ty.is_nested()
            && !matches!(ty, DataType::Dictionary(..));
        if is_fallback_scalar {
            let ty_name = ty.to_string();
            let converter = RowConverter::new(vec![SortField::new(ty.clone())])?;
            let rows = converter.convert_columns(&[Arc::clone(&input)])?;
            if let Some(reservation) = &reservation {
                reservation.try_grow(rows.size())?;
            }
            let mut keys = BinaryBuilder::with_capacity(input.len(), input.len() * 16);
            let mut key = Vec::new();
            let order_mode = matches!(self.mode, KeyMode::Order | KeyMode::GremlinOrder);
            let gremlin = self.mode == KeyMode::GremlinOrder;
            let order_tag = if gremlin {
                match ty {
                    DataType::Date32 | DataType::Date64 | DataType::Timestamp(..) => 4,
                    _ => 12,
                }
            } else {
                9
            };
            for index in 0..input.len() {
                if index.is_multiple_of(256)
                    && let Some(guard) = &self.guard
                {
                    guard.check()?;
                }
                if input.is_null(index) {
                    if order_mode {
                        keys.append_null();
                    } else {
                        keys.append_value([0u8]);
                    }
                    continue;
                }
                key.clear();
                if order_mode {
                    key.push(order_tag);
                    encode_order_bytes(ty_name.as_bytes(), &mut key);
                    encode_order_bytes(rows.row(index).as_ref(), &mut key);
                } else {
                    key.push(5);
                    encode_bytes(ty_name.as_bytes(), &mut key);
                    encode_bytes(rows.row(index).as_ref(), &mut key);
                }
                if let Some(reservation) = &reservation {
                    reservation.try_grow(key.len().saturating_add(4))?;
                }
                keys.append_value(&key);
            }
            return Ok(ColumnarValue::Array(Arc::new(keys.finish())));
        }
        let mut keys = BinaryBuilder::new();
        let mut key = Vec::new();
        for index in 0..input.len() {
            if index.is_multiple_of(256)
                && let Some(guard) = &self.guard
            {
                guard.check()?;
            }
            key.clear();
            let value = ScalarValue::try_from_array(&input, index)?;
            if matches!(self.mode, KeyMode::Order | KeyMode::GremlinOrder) {
                let value = unpack(value)?;
                if value.is_null() {
                    keys.append_null();
                    continue;
                }
                encode_order(value, &mut key, self.mode == KeyMode::GremlinOrder)?;
            } else {
                encode_equivalence(value, &mut key)?;
            }
            if let Some(reservation) = &reservation {
                reservation.try_grow(key.len().saturating_add(4))?;
            }
            keys.append_value(&key);
        }
        Ok(ColumnarValue::Array(Arc::new(keys.finish())))
    }
}

fn distinct_list(input: ArrayRef, guard: Option<&ScopeGuard>) -> Result<ArrayRef> {
    let Some(list) = input.as_any().downcast_ref::<ListArray>() else {
        return exec_err!("collect(DISTINCT) requires a list");
    };
    let DataType::List(field) = list.data_type() else {
        unreachable!("collect list type")
    };
    let reservation = guard.map(|guard| {
        MemoryConsumer::new("GraphLake DISTINCT numeric aggregate")
            .register(&guard.runtime.memory_pool)
    });
    if let Some(reservation) = &reservation {
        let estimated = crate::engine::query::allocation::slice_bytes(list.values().as_ref())?;
        reservation.try_grow(estimated)?;
    }
    let data = list.values().to_data();
    let mut values = MutableArrayData::new(vec![&data], true, list.values().len());
    let mut lengths = Vec::with_capacity(list.len());
    let mut visited = 0usize;
    for window in list.value_offsets().windows(2) {
        let set_memory = reservation
            .as_ref()
            .map(|reservation| reservation.new_empty());
        let mut seen = HashSet::new();
        for index in window[0] as usize..window[1] as usize {
            visited += 1;
            if visited.is_multiple_of(256)
                && let Some(guard) = guard
            {
                guard.check()?;
            }
            let mut key = Vec::new();
            encode_equivalence(ScalarValue::try_from_array(list.values(), index)?, &mut key)?;
            if !seen.contains(&key) {
                if let Some(memory) = &set_memory {
                    memory.try_grow(key.capacity().saturating_add(32))?;
                }
                seen.insert(key);
                values.try_extend(0, index, index + 1)?;
            }
        }
        lengths.push(seen.len());
    }
    Ok(Arc::new(ListArray::try_new(
        field.clone(),
        OffsetBuffer::from_lengths(lengths),
        make_array(values.freeze()),
        list.nulls().cloned(),
    )?))
}

fn needs_equivalence_key(ty: &DataType) -> bool {
    ty.is_nested() || ty.is_floating() || matches!(ty, DataType::Dictionary(..))
}

fn encode_equivalence(value: ScalarValue, key: &mut Vec<u8>) -> Result<()> {
    let value = unpack(value)?;
    if value.is_null() {
        key.push(0);
    } else if let Some(identity) = entity::identity(&value)? {
        key.push(8);
        for part in identity {
            encode_equivalence(part, key)?;
        }
    } else if value.data_type().is_numeric() {
        key.push(1);
        let decimal = crate::scalar::finite_decimal(&value).map(|n| n.normalized().to_string());
        let number =
            decimal.unwrap_or_else(|| float(&value).expect("non-finite number").to_string());
        encode_bytes(number.as_bytes(), key);
    } else if let ScalarValue::Utf8(Some(text))
    | ScalarValue::Utf8View(Some(text))
    | ScalarValue::LargeUtf8(Some(text)) = &value
    {
        key.push(2);
        encode_bytes(text.as_bytes(), key);
    } else if let ScalarValue::Boolean(Some(value)) = &value {
        key.push(if *value { 6 } else { 7 });
    } else if let Some(items) = list_items(&value) {
        key.push(3);
        key.extend_from_slice(&items.len().to_le_bytes());
        for index in 0..items.len() {
            encode_equivalence(ScalarValue::try_from_array(&items, index)?, key)?;
        }
    } else if let ScalarValue::Struct(record) = value {
        key.push(4);
        let mut fields = record
            .fields()
            .iter()
            .zip(record.columns())
            .collect::<Vec<_>>();
        fields.sort_unstable_by_key(|(field, _)| field.name());
        key.extend_from_slice(&fields.len().to_le_bytes());
        for (field, column) in fields {
            encode_bytes(field.name().as_bytes(), key);
            encode_equivalence(ScalarValue::try_from_array(column, 0)?, key)?;
        }
    } else {
        key.push(5);
        let ty = value.data_type();
        encode_bytes(ty.to_string().as_bytes(), key);
        let converter = RowConverter::new(vec![SortField::new(ty)])?;
        let rows = converter.convert_columns(&[value.to_array()?])?;
        encode_bytes(rows.row(0).as_ref(), key);
    }
    Ok(())
}

fn encode_bytes(bytes: &[u8], key: &mut Vec<u8>) {
    key.extend_from_slice(&bytes.len().to_le_bytes());
    key.extend_from_slice(bytes);
}

// Cypher orders incomparable types for sorting, while comparison operators
// still return null for values without an ordering relation.
fn encode_order(value: ScalarValue, key: &mut Vec<u8>, gremlin: bool) -> Result<()> {
    let value = unpack(value)?;
    if value.is_null() {
        key.push(if gremlin { 1 } else { 255 });
    } else if let Some([kind, label, id]) = entity::identity(&value)? {
        key.push(
            if matches!(kind, ScalarValue::Utf8(Some(name))
                | ScalarValue::Utf8View(Some(name))
                | ScalarValue::LargeUtf8(Some(name)) if name == "node")
            {
                2
            } else {
                3
            },
        );
        encode_order(label, key, gremlin)?;
        encode_order(id, key, gremlin)?;
    } else if let Some(items) = list_items(&value) {
        key.push(if gremlin { 9 } else { 4 });
        for index in 0..items.len() {
            encode_order(ScalarValue::try_from_array(&items, index)?, key, gremlin)?;
        }
        key.push(0);
    } else if let ScalarValue::Struct(record) = &value {
        key.push(if gremlin { 11 } else { 1 });
        let mut fields = record
            .fields()
            .iter()
            .zip(record.columns())
            .collect::<Vec<_>>();
        fields.sort_unstable_by_key(|(field, _)| field.name());
        for (field, column) in fields {
            key.push(1);
            encode_order_bytes(field.name().as_bytes(), key);
            encode_order(ScalarValue::try_from_array(column, 0)?, key, gremlin)?;
        }
        key.push(0);
    } else if let ScalarValue::Utf8(Some(text))
    | ScalarValue::Utf8View(Some(text))
    | ScalarValue::LargeUtf8(Some(text)) = &value
    {
        key.push(if gremlin { 5 } else { 6 });
        encode_order_bytes(text.as_bytes(), key);
    } else if let ScalarValue::Boolean(Some(value)) = value {
        key.extend_from_slice(&[if gremlin { 2 } else { 7 }, u8::from(value)]);
    } else if value.data_type().is_numeric() {
        key.push(if gremlin { 3 } else { 8 });
        if let Some(int_val) = integer_i128(&value) {
            encode_order_i128(int_val, key);
        } else if let Some(decimal) = crate::scalar::finite_decimal(&value) {
            let (coefficient, scale) = decimal.normalized().as_bigint_and_exponent();
            let digits = coefficient.to_string();
            if digits == "0" {
                key.push(2);
            } else {
                let (negative, digits) = match digits.strip_prefix('-') {
                    Some(digits) => (true, digits),
                    None => (false, digits.as_str()),
                };
                key.push(if negative { 1 } else { 3 });
                // Sort magnitude by scientific exponent and then significand.
                let exponent = (digits.len() as i64 - scale) as u64 ^ (1 << 63);
                let bytes = exponent
                    .to_be_bytes()
                    .into_iter()
                    .chain(digits.bytes())
                    .chain(std::iter::once(0));
                key.extend(bytes.map(|byte| if negative { !byte } else { byte }));
            }
        } else {
            let number = float(&value).expect("non-finite number");
            key.push(if number.is_nan() {
                5
            } else if number.is_sign_positive() {
                4
            } else {
                0
            });
        }
    } else {
        let ty = value.data_type();
        key.push(if gremlin {
            match ty {
                DataType::Date32 | DataType::Date64 | DataType::Timestamp(..) => 4,
                _ => 12,
            }
        } else {
            9
        });
        encode_order_bytes(ty.to_string().as_bytes(), key);
        let converter = RowConverter::new(vec![SortField::new(ty)])?;
        let rows = converter.convert_columns(&[value.to_array()?])?;
        encode_order_bytes(rows.row(0).as_ref(), key);
    }
    Ok(())
}

fn integer_i128(value: &ScalarValue) -> Option<i128> {
    match value {
        ScalarValue::Int8(Some(v)) => Some(i128::from(*v)),
        ScalarValue::Int16(Some(v)) => Some(i128::from(*v)),
        ScalarValue::Int32(Some(v)) => Some(i128::from(*v)),
        ScalarValue::Int64(Some(v)) => Some(i128::from(*v)),
        ScalarValue::UInt8(Some(v)) => Some(i128::from(*v)),
        ScalarValue::UInt16(Some(v)) => Some(i128::from(*v)),
        ScalarValue::UInt32(Some(v)) => Some(i128::from(*v)),
        ScalarValue::UInt64(Some(v)) => Some(i128::from(*v)),
        _ => None,
    }
}

fn encode_order_i128(value: i128, key: &mut Vec<u8>) {
    if value == 0 {
        key.push(2);
        return;
    }
    let negative = value < 0;
    let mut mag = value.unsigned_abs();
    let mut buf = [0u8; 40];
    let mut pos = buf.len();
    while mag > 0 {
        pos -= 1;
        buf[pos] = b'0' + (mag % 10) as u8;
        mag /= 10;
    }
    let total_digits = (buf.len() - pos) as i64;
    let mut end = buf.len();
    while end > pos && buf[end - 1] == b'0' {
        end -= 1;
    }
    let digits = &buf[pos..end];
    key.push(if negative { 1 } else { 3 });
    let exponent = (total_digits as u64) ^ (1 << 63);
    let bytes = exponent
        .to_be_bytes()
        .into_iter()
        .chain(digits.iter().copied())
        .chain(std::iter::once(0));
    key.extend(bytes.map(|byte| if negative { !byte } else { byte }));
}

fn encode_order_bytes(bytes: &[u8], key: &mut Vec<u8>) {
    for byte in bytes {
        key.push(*byte);
        if *byte == 0 {
            key.push(255);
        }
    }
    key.extend_from_slice(&[0, 0]);
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Size(Signature);

impl ScalarUDFImpl for Size {
    fn name(&self) -> &str {
        "__graphlake_cypher_size"
    }

    fn signature(&self) -> &Signature {
        &self.0
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        if text_type(&args[0])
            || matches!(
                args[0],
                DataType::Null
                    | DataType::List(_)
                    | DataType::LargeList(_)
                    | DataType::FixedSizeList(_, _)
            )
        {
            Ok(DataType::UInt64)
        } else {
            datafusion::common::plan_err!("size() requires a string or list")
        }
    }

    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let mut kind = args.args[0].data_type();
        let function = if kind.is_null() {
            return args.args[0].cast_to(&DataType::UInt64, None);
        } else if text_type(&kind) {
            if matches!(kind, DataType::Dictionary(..)) {
                args.args[0] = args.args[0].cast_to(&DataType::Utf8View, None)?;
                args.arg_fields = vec![Arc::new(Field::new(
                    args.arg_fields[0].name(),
                    DataType::Utf8View,
                    args.arg_fields[0].is_nullable(),
                ))];
                kind = DataType::Utf8View;
            }
            datafusion::functions::unicode::character_length()
        } else {
            datafusion::functions_nested::length::array_length_udf()
        };
        args.return_field = Arc::new(Field::new("length", function.return_type(&[kind])?, true));
        function
            .invoke_with_args(args)?
            .cast_to(&DataType::UInt64, None)
    }
}
