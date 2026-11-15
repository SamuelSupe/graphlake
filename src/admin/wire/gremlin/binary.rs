use std::io::{Cursor, Read};

use serde_json::{Map, Value, json};
use uuid::Uuid;

use super::{Request, typed};

pub(super) const MIME: &[u8] = b"application/vnd.graphbinary-v1.0";
const VERSION: u8 = 0x81;
const NULL: [u8; 2] = [0xfe, 1];

pub(super) fn request(bytes: &[u8]) -> Option<Request> {
    let mut input = Input(Cursor::new(bytes));
    if input.byte()? != VERSION {
        return None;
    }
    let id = Uuid::from_bytes(input.fixed()?);
    let op = input.string()?;
    let processor = input.string()?;
    let args = input.map(0)?;
    (input.0.position() as usize == bytes.len()).then_some(Request {
        id,
        op,
        processor,
        args,
        binary: true,
    })
}

struct Input<'a>(Cursor<&'a [u8]>);

impl Input<'_> {
    fn fixed<const N: usize>(&mut self) -> Option<[u8; N]> {
        let mut fixed = [0; N];
        self.0.read_exact(&mut fixed).ok()?;
        Some(fixed)
    }

    fn byte(&mut self) -> Option<u8> {
        Some(self.fixed::<1>()?[0])
    }

    fn size(&mut self) -> Option<usize> {
        let size = usize::try_from(i32::from_be_bytes(self.fixed()?)).ok()?;
        (size
            <= self
                .0
                .get_ref()
                .len()
                .saturating_sub(self.0.position() as usize))
        .then_some(size)
    }

    fn string(&mut self) -> Option<String> {
        let size = self.size()?;
        let mut bytes = vec![0; size];
        self.0.read_exact(&mut bytes).ok()?;
        String::from_utf8(bytes).ok()
    }

    fn map(&mut self, depth: usize) -> Option<Map<String, Value>> {
        let mut map = Map::new();
        for _ in 0..self.size()? {
            let key = self.value(depth + 1)?.as_str()?.to_owned();
            if map.insert(key, self.value(depth + 1)?).is_some() {
                return None;
            }
        }
        Some(map)
    }

    fn list(&mut self, depth: usize) -> Option<Vec<Value>> {
        let size = self.size()?;
        (0..size).map(|_| self.value(depth + 1)).collect()
    }

    fn instructions(&mut self, depth: usize) -> Option<Vec<Value>> {
        let size = self.size()?;
        if size > 256 {
            return None;
        }
        (0..size)
            .map(|_| {
                let name = Value::String(self.string()?);
                let args = self.list(depth)?;
                Some(Value::Array(std::iter::once(name).chain(args).collect()))
            })
            .collect()
    }

    fn value(&mut self, depth: usize) -> Option<Value> {
        if depth > 16 {
            return None;
        }
        let ty = self.byte()?;
        match self.byte()? {
            1 => return Some(Value::Null),
            0 => {}
            _ => return None,
        }
        Some(match ty {
            0x01 => json!(i32::from_be_bytes(self.fixed()?)),
            0x02 => json!(i64::from_be_bytes(self.fixed()?)),
            0x03 => json!(self.string()?),
            0x07 => serde_json::Number::from_f64(f64::from_be_bytes(self.fixed()?))?.into(),
            0x08 => {
                serde_json::Number::from_f64(f64::from(f32::from_be_bytes(self.fixed()?)))?.into()
            }
            0x09 | 0x0b => Value::Array(self.list(depth)?),
            0x0a => Value::Object(self.map(depth)?),
            0x15 => {
                let steps = self.instructions(depth)?;
                let source = self.instructions(depth)?;
                typed("g:Bytecode", json!({ "step": steps, "source": source }))
            }
            0x1a => typed("g:Order", self.value(depth + 1)?),
            0x1e | 0x28 => {
                let predicate = self.string()?;
                let mut args = self.list(depth)?;
                let value = if args.len() == 1 {
                    args.pop()?
                } else {
                    Value::Array(args)
                };
                typed(
                    if ty == 0x1e { "g:P" } else { "g:TextP" },
                    json!({ "predicate": predicate, "value": value }),
                )
            }
            0x24 => json!(i8::from_be_bytes(self.fixed()?)),
            0x26 => json!(i16::from_be_bytes(self.fixed()?)),
            0x27 => match self.byte()? {
                0 => Value::Bool(false),
                1 => Value::Bool(true),
                _ => return None,
            },
            _ => return None,
        })
    }
}

pub(super) fn response(
    id: Uuid,
    code: u16,
    message: &str,
    rows: Vec<Value>,
    attributes: Value,
) -> Vec<u8> {
    let mut output = vec![VERSION, 0];
    output.extend(id.as_bytes());
    output.extend(i32::from(code).to_be_bytes());
    output.push(0);
    string(&mut output, message);
    let attributes = attributes.as_object().expect("Gremlin status attributes");
    size(&mut output, attributes.len());
    for (name, value) in attributes {
        output.extend([0x03, 0]);
        string(&mut output, name);
        write(&mut output, value);
    }
    size(&mut output, 0); // result metadata
    list(&mut output, 0x09, rows.iter());
    output
}

fn size(out: &mut Vec<u8>, value: usize) {
    out.extend(
        i32::try_from(value)
            .expect("bounded Gremlin response")
            .to_be_bytes(),
    );
}

fn string(out: &mut Vec<u8>, value: &str) {
    size(out, value.len());
    out.extend(value.as_bytes());
}

fn list<'a>(out: &mut Vec<u8>, ty: u8, values: impl ExactSizeIterator<Item = &'a Value>) {
    out.extend([ty, 0]);
    size(out, values.len());
    for value in values {
        write(out, value);
    }
}

fn write(out: &mut Vec<u8>, value: &Value) {
    match value {
        Value::Null => out.extend(NULL),
        Value::Bool(value) => out.extend([0x27, 0, (*value).into()]),
        Value::String(value) => {
            out.extend([0x03, 0]);
            string(out, value);
        }
        Value::Object(data) => {
            let value = &data["@value"];
            match data["@type"].as_str().expect("typed GraphSON response") {
                "g:Int64" => {
                    out.extend([0x02, 0]);
                    out.extend(value.as_i64().unwrap().to_be_bytes());
                }
                "g:Double" => {
                    let value = value.as_f64().unwrap_or_else(|| match value.as_str() {
                        Some("Infinity") => f64::INFINITY,
                        Some("-Infinity") => f64::NEG_INFINITY,
                        _ => f64::NAN,
                    });
                    out.extend([0x07, 0]);
                    out.extend(value.to_be_bytes());
                }
                "g:List" => list(out, 0x09, value.as_array().unwrap().iter()),
                "g:Set" => list(out, 0x0b, value.as_array().unwrap().iter()),
                "g:Map" => {
                    out.extend([0x0a, 0]);
                    let entries = value.as_array().unwrap();
                    size(out, entries.len() / 2);
                    for entry in entries {
                        write(out, entry);
                    }
                }
                "g:Traverser" => {
                    out.extend([0x21, 0]);
                    out.extend(value["bulk"]["@value"].as_i64().unwrap().to_be_bytes());
                    write(out, &value["value"]);
                }
                "g:Vertex" => {
                    out.extend([0x11, 0]);
                    write(out, &value["id"]);
                    string(out, value["label"].as_str().unwrap_or_default());
                    let properties = value["properties"].as_object().unwrap();
                    let count = properties
                        .values()
                        .map(|v| v.as_array().unwrap().len())
                        .sum();
                    out.extend([0x09, 0]);
                    size(out, count);
                    for prop in properties.values().flat_map(|v| v.as_array().unwrap()) {
                        write(out, prop);
                    }
                }
                "g:Edge" => {
                    out.extend([0x0d, 0]);
                    write(out, &value["id"]);
                    string(out, value["label"].as_str().unwrap());
                    write(out, &value["inV"]);
                    string(out, value["inVLabel"].as_str().unwrap());
                    write(out, &value["outV"]);
                    string(out, value["outVLabel"].as_str().unwrap());
                    out.extend(NULL);
                    list(out, 0x09, value["properties"].as_object().unwrap().values());
                }
                "g:VertexProperty" => {
                    out.extend([0x12, 0]);
                    write(out, &value["id"]);
                    string(out, value["label"].as_str().unwrap());
                    write(out, &value["value"]);
                    out.extend(NULL);
                    out.extend(NULL);
                }
                "g:Property" => {
                    out.extend([0x0f, 0]);
                    string(out, value["key"].as_str().unwrap());
                    write(out, &value["value"]);
                    out.extend(NULL);
                }
                "g:Path" => {
                    out.extend([0x0e, 0]);
                    write(out, &value["labels"]);
                    write(out, &value["objects"]);
                }
                _ => unreachable!("unsupported GraphSON result"),
            }
        }
        _ => unreachable!("unexpected untyped GraphSON result"),
    }
}
