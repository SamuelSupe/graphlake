use std::sync::{Arc, LazyLock};

use datafusion::arrow::{
    array::{Array, AsArray},
    datatypes::{DataType, FieldRef, Float16Type, Float32Type, Float64Type},
    error::ArrowError,
    json::{
        WriterBuilder,
        writer::{Encoder, EncoderFactory, EncoderOptions, NullableEncoder, make_encoder},
    },
};

pub fn json_writer() -> WriterBuilder {
    WriterBuilder::new().with_encoder_factory(Arc::new(FloatFactory))
}

#[derive(Debug)]
struct FloatFactory;

impl EncoderFactory for FloatFactory {
    fn make_default_encoder<'a>(
        &self,
        field: &'a FieldRef,
        array: &'a dyn Array,
        _: &'a EncoderOptions,
    ) -> Result<Option<NullableEncoder<'a>>, ArrowError> {
        if !array.data_type().is_floating() {
            return Ok(None);
        }
        static DEFAULT: LazyLock<EncoderOptions> = LazyLock::new(EncoderOptions::default);
        let encoder = FloatEncoder {
            array,
            finite: make_encoder(field, array, &DEFAULT)?,
        };
        Ok(Some(NullableEncoder::new(
            Box::new(encoder),
            array.logical_nulls(),
        )))
    }
}

struct FloatEncoder<'a> {
    array: &'a dyn Array,
    finite: NullableEncoder<'a>,
}

impl Encoder for FloatEncoder<'_> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let value = match self.array.data_type() {
            DataType::Float16 => f64::from(self.array.as_primitive::<Float16Type>().value(idx)),
            DataType::Float32 => f64::from(self.array.as_primitive::<Float32Type>().value(idx)),
            DataType::Float64 => self.array.as_primitive::<Float64Type>().value(idx),
            _ => unreachable!("float encoder"),
        };
        let special: &[u8] = if value.is_nan() {
            b"\"NaN\""
        } else if value == f64::INFINITY {
            b"\"Infinity\""
        } else if value == f64::NEG_INFINITY {
            b"\"-Infinity\""
        } else {
            return self.finite.encode(idx, out);
        };
        out.extend_from_slice(special);
    }
}
