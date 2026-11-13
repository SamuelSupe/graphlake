use std::sync::Arc;

use anyhow::{Context, Result, bail, ensure};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use datafusion::arrow::{
    array::{
        ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
        Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, RecordBatch, StringArray,
        Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
        TimestampNanosecondArray, UInt8Array,
    },
    datatypes::{DataType, SchemaRef, TimeUnit},
};

pub(super) fn numeric(precision: u8, scale: i8) -> Result<DataType> {
    ensure!(
        (1..=38).contains(&precision) && scale >= 0 && scale as u8 <= precision,
        "warehouse fixed-point value exceeds Decimal128 precision or scale"
    );
    Ok(DataType::Decimal128(precision, scale))
}

#[derive(Clone, Copy)]
pub(super) enum Encoding {
    Snowflake,
    BigQuery,
    Trino,
    Rfc3339,
}

pub(super) fn batch(
    schema: SchemaRef,
    rows: &[Vec<Option<String>>],
    encoding: Encoding,
) -> Result<RecordBatch> {
    ensure!(
        rows.iter().all(|row| row.len() == schema.fields().len()),
        "warehouse result row width mismatch"
    );
    let arrays = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| -> Result<ArrayRef> {
            let values = || rows.iter().map(|row| row[index].as_deref());
            macro_rules! converted {
                ($array:ty, $convert:expr) => {{
                    let cells = values()
                        .map(|value| value.map($convert).transpose())
                        .collect::<Result<Vec<_>, _>>()
                        .with_context(|| format!("decoding warehouse column {}", field.name()))?;
                    Arc::new(<$array>::from(cells))
                }};
            }
            let array: ArrayRef = match field.data_type() {
                DataType::Int8 => converted!(Int8Array, str::parse::<i8>),
                DataType::UInt8 => converted!(UInt8Array, str::parse::<u8>),
                DataType::Int16 => converted!(Int16Array, str::parse::<i16>),
                DataType::Int32 => converted!(Int32Array, str::parse::<i32>),
                DataType::Int64 => converted!(Int64Array, str::parse::<i64>),
                DataType::Float32 => converted!(Float32Array, str::parse::<f32>),
                DataType::Float64 => converted!(Float64Array, str::parse::<f64>),
                DataType::Boolean => converted!(BooleanArray, boolean),
                DataType::Utf8 => Arc::new(StringArray::from_iter(values())),
                DataType::Binary => {
                    let cells = values()
                        .map(|value| {
                            value
                                .map(|value| {
                                    if !matches!(encoding, Encoding::Snowflake) {
                                        Ok(STANDARD.decode(value)?)
                                    } else {
                                        hex(value)
                                    }
                                })
                                .transpose()
                        })
                        .collect::<Result<Vec<_>>>()?;
                    Arc::new(BinaryArray::from_iter(cells.iter().map(Option::as_deref)))
                }
                DataType::Date32 => {
                    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("Unix epoch");
                    converted!(Date32Array, |value| NaiveDate::parse_from_str(
                        value, "%Y-%m-%d"
                    )
                    .map(|date| (date - epoch).num_days() as i32))
                }
                DataType::Time64(TimeUnit::Microsecond) => {
                    converted!(Time64MicrosecondArray, |value| time_nanos(value)
                        .map(|n| n / 1_000))
                }
                DataType::Time64(TimeUnit::Nanosecond) => {
                    converted!(Time64NanosecondArray, time_nanos)
                }
                DataType::Timestamp(TimeUnit::Microsecond, timezone) => {
                    let cells = values()
                        .map(|value| {
                            value
                                .map(|value| {
                                    if matches!(encoding, Encoding::BigQuery) && timezone.is_some()
                                    {
                                        Ok(value.parse::<i64>()?)
                                    } else {
                                        Ok(timestamp(value, timezone.is_some(), encoding)?
                                            .timestamp_micros())
                                    }
                                })
                                .transpose()
                        })
                        .collect::<Result<Vec<_>>>()?;
                    Arc::new(
                        TimestampMicrosecondArray::from(cells).with_timezone_opt(timezone.clone()),
                    )
                }
                DataType::Timestamp(TimeUnit::Nanosecond, timezone) => {
                    let cells = values()
                        .map(|value| {
                            value
                                .map(|value| {
                                    timestamp(value, timezone.is_some(), encoding)?
                                        .timestamp_nanos_opt()
                                        .context(
                                            "warehouse timestamp exceeds Arrow nanosecond range",
                                        )
                                })
                                .transpose()
                        })
                        .collect::<Result<Vec<_>>>()?;
                    Arc::new(
                        TimestampNanosecondArray::from(cells).with_timezone_opt(timezone.clone()),
                    )
                }
                DataType::Decimal128(precision, scale) => {
                    let cells = values()
                        .map(|value| {
                            value
                                .map(|value| crate::source::decimal128(value, *scale))
                                .transpose()
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let array = Decimal128Array::from(cells)
                        .with_precision_and_scale(*precision, *scale)?;
                    array.validate_decimal_precision(*precision)?;
                    Arc::new(array)
                }
                ty => bail!("unsupported warehouse Arrow type {ty}"),
            };
            Ok(array)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(schema, arrays)?)
}

fn timestamp(value: &str, zoned: bool, encoding: Encoding) -> Result<DateTime<chrono::Utc>> {
    if zoned && matches!(encoding, Encoding::Trino) {
        let (value, zone) = value
            .rsplit_once(' ')
            .context("Trino timestamp has no time zone")?;
        let timestamp = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f")?;
        if let Ok(zone) = zone.parse::<chrono_tz::Tz>() {
            use chrono::TimeZone;
            return Ok(zone
                .from_local_datetime(&timestamp)
                .single()
                .context("ambiguous Trino timestamp time zone")?
                .with_timezone(&chrono::Utc));
        }
        return Ok(
            DateTime::parse_from_rfc3339(&format!("{}{}", value.replace(' ', "T"), zone))?
                .with_timezone(&chrono::Utc),
        );
    }
    if zoned {
        Ok(DateTime::parse_from_rfc3339(value)?.with_timezone(&chrono::Utc))
    } else {
        Ok(
            NaiveDateTime::parse_from_str(&value.replace(' ', "T"), "%Y-%m-%dT%H:%M:%S%.f")?
                .and_utc(),
        )
    }
}

fn time_nanos(value: &str) -> Result<i64> {
    let time = NaiveTime::parse_from_str(value, "%H:%M:%S%.f")?;
    Ok(i64::from(time.num_seconds_from_midnight()) * 1_000_000_000 + i64::from(time.nanosecond()))
}

fn boolean(value: &str) -> Result<bool> {
    if value.eq_ignore_ascii_case("true") || value == "1" {
        Ok(true)
    } else if value.eq_ignore_ascii_case("false") || value == "0" {
        Ok(false)
    } else {
        bail!("invalid warehouse boolean")
    }
}

fn hex(value: &str) -> Result<Vec<u8>> {
    ensure!(
        value.is_ascii() && value.len().is_multiple_of(2),
        "invalid warehouse hex value"
    );
    (0..value.len())
        .step_by(2)
        .map(|index| Ok(u8::from_str_radix(&value[index..index + 2], 16)?))
        .collect()
}
