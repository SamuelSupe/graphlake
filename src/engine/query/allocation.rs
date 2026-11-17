use std::{
    hash::{Hash, Hasher},
    sync::Arc,
};

use datafusion::{
    arrow::{
        array::{Array, AsArray},
        datatypes::{DataType, FieldRef, Int64Type, IntervalMonthDayNanoType},
    },
    common::{Result, ScalarValue, exec_err},
    execution::{memory_pool::MemoryConsumer, runtime_env::RuntimeEnv},
    logical_expr::{
        ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
        Volatility,
    },
    prelude::SessionContext,
};
use tokio::time::Instant;

use super::QueryScope;

pub(super) const BUILT_INS: &[&str] = &[
    "repeat",
    "lpad",
    "rpad",
    "replace",
    "regexp_replace",
    "concat",
    "concat_ws",
    "array_repeat",
    "array_resize",
    "range",
    "generate_series",
];

pub(super) fn register(context: &SessionContext, scope: &QueryScope, names: &[&str]) {
    let state = context.state();
    for &name in names {
        if let Some(function) = state.scalar_functions().get(name) {
            let mut signature = function.signature().clone();
            // DataFusion does not retain a memory reservation with a folded literal.
            signature.volatility = Volatility::Volatile;
            context.register_udf(ScalarUDF::from(CheckedFunction {
                function: Arc::clone(function),
                signature,
                runtime: Arc::clone(&scope.runtime),
                cancelled: scope.options.cancellation.clone(),
                deadline: scope.deadline,
            }));
        }
    }
}

#[derive(Debug)]
struct CheckedFunction {
    function: Arc<ScalarUDF>,
    signature: Signature,
    runtime: Arc<RuntimeEnv>,
    cancelled: crate::CancellationToken,
    deadline: Instant,
}

impl PartialEq for CheckedFunction {
    fn eq(&self, other: &Self) -> bool {
        self.function == other.function
            && self.deadline == other.deadline
            && Arc::ptr_eq(&self.runtime, &other.runtime)
    }
}

impl Eq for CheckedFunction {}

impl Hash for CheckedFunction {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.function.hash(state);
        self.deadline.hash(state);
        Arc::as_ptr(&self.runtime).hash(state);
    }
}

impl CheckedFunction {
    fn check(&self) -> Result<()> {
        if self.cancelled.is_cancelled() {
            return exec_err!("query cancelled");
        }
        if Instant::now() >= self.deadline {
            return exec_err!("query deadline exceeded");
        }
        Ok(())
    }
}

impl ScalarUDFImpl for CheckedFunction {
    fn name(&self) -> &str {
        self.function.name()
    }

    fn aliases(&self) -> &[String] {
        self.function.aliases()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        self.function.return_type(types)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        self.function.return_field_from_args(args)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        self.check()?;
        let rows = args.number_rows;
        let scalar = args
            .args
            .iter()
            .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
        let mut bytes = 0usize;
        for row in 0..if scalar { 1 } else { rows } {
            if row.is_multiple_of(256) {
                self.check()?;
            }
            bytes = bytes.saturating_add(row_bytes(self.name(), &args.args, row)?);
        }
        if scalar {
            bytes = bytes.saturating_mul(rows.max(1));
        }
        // Include scratch space and Arrow's output builders.
        let memory = MemoryConsumer::new(format!("{} scalar allocation", self.name()))
            .register(&self.runtime.memory_pool);
        memory.try_grow(bytes.saturating_mul(3))?;
        let result = if bytes >= 256 * 1024 {
            crate::blocking(|| self.function.invoke_with_args(args))
        } else {
            self.function.invoke_with_args(args)
        }?;
        self.check()?;
        Ok(result)
    }
}

fn text(value: &ColumnarValue, row: usize) -> Option<&str> {
    match value {
        ColumnarValue::Scalar(
            ScalarValue::Utf8(text) | ScalarValue::Utf8View(text) | ScalarValue::LargeUtf8(text),
        ) => text.as_deref(),
        ColumnarValue::Array(array) if array.is_valid(row) => match array.data_type() {
            DataType::Utf8 => Some(array.as_string::<i32>().value(row)),
            DataType::LargeUtf8 => Some(array.as_string::<i64>().value(row)),
            DataType::Utf8View => Some(array.as_string_view().value(row)),
            _ => None,
        },
        _ => None,
    }
}

fn integer(value: &ColumnarValue, row: usize) -> Option<i64> {
    match value {
        ColumnarValue::Scalar(ScalarValue::Int64(value)) => *value,
        ColumnarValue::Array(array)
            if array.data_type() == &DataType::Int64 && array.is_valid(row) =>
        {
            Some(array.as_primitive::<Int64Type>().value(row))
        }
        _ => None,
    }
}

fn value_bytes(value: &ColumnarValue, row: usize) -> usize {
    if let Some(value) = text(value, row) {
        return value.len().saturating_add(16);
    }
    match value {
        ColumnarValue::Scalar(value) => value.size(),
        ColumnarValue::Array(array) => array
            .data_type()
            .primitive_width()
            .unwrap_or_else(|| array.get_buffer_memory_size()),
    }
}

fn scalar(value: &ColumnarValue, row: usize) -> Result<ScalarValue> {
    match value {
        ColumnarValue::Scalar(value) => Ok(value.clone()),
        ColumnarValue::Array(array) => ScalarValue::try_from_array(array, row),
    }
}

const DAY: i128 = 86_400_000_000_000;

fn time_coordinate(value: &ScalarValue) -> Option<i128> {
    match value {
        ScalarValue::Date32(Some(v)) => Some(i128::from(*v) * DAY),
        ScalarValue::Date64(Some(v)) | ScalarValue::TimestampMillisecond(Some(v), _) => {
            Some(i128::from(*v) * 1_000_000)
        }
        ScalarValue::TimestampSecond(Some(v), _) => Some(i128::from(*v) * 1_000_000_000),
        ScalarValue::TimestampMicrosecond(Some(v), _) => Some(i128::from(*v) * 1_000),
        ScalarValue::TimestampNanosecond(Some(v), _) => Some(i128::from(*v)),
        _ => None,
    }
}

fn range_bytes(args: &[ColumnarValue], row: usize, inclusive: bool) -> Result<usize> {
    let (distance, step) = if args[0].data_type() == DataType::Int64 {
        let start = if args.len() == 1 {
            Some(0)
        } else {
            integer(&args[0], row)
        };
        let stop = integer(&args[usize::from(args.len() > 1)], row);
        let step = args.get(2).map_or(Some(1), |step| integer(step, row));
        let (Some(start), Some(stop), Some(step)) = (start, stop, step) else {
            return Ok(0);
        };
        (
            (i128::from(stop) - i128::from(start)) * i128::from(step.signum()),
            i128::from(step).abs(),
        )
    } else {
        if args.len() != 3 {
            return Ok(0);
        }
        let start = scalar(&args[0], row)?;
        let stop = scalar(&args[1], row)?;
        let (Some(begin), Some(end)) = (time_coordinate(&start), time_coordinate(&stop)) else {
            return Ok(0);
        };
        let ScalarValue::IntervalMonthDayNano(Some(interval)) = scalar(&args[2], row)? else {
            return Ok(0);
        };
        let (months, days, nanos) = IntervalMonthDayNanoType::to_parts(interval);
        let fixed = i128::from(days) * DAY + i128::from(nanos);
        let mut short = i128::from(months) * 28 * DAY + fixed;
        let mut long = i128::from(months) * 31 * DAY + fixed;
        let date = matches!(start.data_type(), DataType::Date32 | DataType::Date64);
        if date {
            short = short.div_euclid(DAY) * DAY;
            long = long.div_euclid(DAY) * DAY;
        }
        let descending_date = date && (months < 0 || days < 0);
        if short == 0 || short.signum() != long.signum() || (date && (short < 0) != descending_date)
        {
            return exec_err!("range step must advance monotonically");
        }
        let step = short.abs().min(long.abs());
        (
            (end - begin) * short.signum() + if date { DAY } else { 0 },
            step,
        )
    };
    if step == 0 || distance < 0 {
        return Ok(0);
    }
    let count = if inclusive {
        distance / step + 1
    } else {
        (distance + step - 1) / step
    };
    Ok(usize::try_from(count)
        .unwrap_or(usize::MAX)
        .saturating_mul(8))
}

fn row_bytes(name: &str, args: &[ColumnarValue], row: usize) -> Result<usize> {
    let string = |index| args.get(index).and_then(|arg| text(arg, row));
    let len = |index| string(index).map_or(0, str::len);
    let count =
        || usize::try_from(integer(&args[1], row).unwrap_or_default().max(0)).unwrap_or(usize::MAX);
    let bytes = match name {
        "repeat" => len(0).saturating_mul(count()),
        "lpad" | "rpad" => {
            let fill = if args.len() == 2 {
                " "
            } else {
                string(2).unwrap_or_default()
            };
            let width = fill.chars().map(char::len_utf8).max().unwrap_or(0);
            count().saturating_mul(width).saturating_add(len(0))
        }
        "replace" => {
            let increase = len(2).saturating_sub(len(1));
            let occurrences = if len(1) == 0 {
                len(0).saturating_add(1)
            } else {
                len(0) / len(1)
            };
            len(0).saturating_add(occurrences.saturating_mul(increase))
        }
        "regexp_replace" => {
            let input = len(0);
            let replacement = string(2).unwrap_or_default();
            // Each capture reference can copy at most the input across disjoint matches.
            input
                .saturating_add(input.saturating_add(1).saturating_mul(replacement.len()))
                .saturating_add(
                    input.saturating_mul(replacement.bytes().filter(|&b| b == b'$').count()),
                )
        }
        "concat" | "concat_ws" => {
            let separator = if name == "concat_ws" { len(0) } else { 0 };
            args.iter().enumerate().fold(0usize, |total, (i, _)| {
                total.saturating_add(len(i)).saturating_add(separator)
            })
        }
        "array_repeat" => count().saturating_mul(value_bytes(&args[0], row).saturating_add(8)),
        "array_resize" => {
            let fill = args
                .get(2)
                .map_or(8, |value| value_bytes(value, row).saturating_add(8));
            value_bytes(&args[0], row).saturating_add(count().saturating_mul(fill))
        }
        "range" | "generate_series" | "__graphlake_cypher_range" => {
            range_bytes(args, row, name != "range")?
        }
        _ => unreachable!("only registered expanding functions use the allocation guard"),
    };
    // Builders may broadcast scalar inputs into a batch before allocating output.
    let input = args
        .iter()
        .filter(|arg| matches!(arg, ColumnarValue::Scalar(_)))
        .fold(0usize, |total, arg| {
            total.saturating_add(text(arg, row).map_or(0, str::len))
        });
    Ok(bytes.saturating_add(input).saturating_add(16))
}
