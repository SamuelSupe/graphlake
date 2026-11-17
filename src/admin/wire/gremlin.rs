mod binary;

use std::{sync::Arc, time::Duration};

use axum::{
    extract::{
        State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use boltr::types::BoltValue;
use serde::Deserialize;
use serde_json::{Value, json};
use uuid::Uuid;

use super::super::{App, Error};
use super::{Rows, Target};
use crate::Language;

const MIME: &[u8] = b"application/vnd.gremlin-v3.0+json";

pub(in crate::admin) async fn upgrade(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    socket: WebSocketUpgrade,
) -> Response {
    let Some(access) = &app.wire else {
        return Error::not_found().into_response();
    };
    // Native drivers do not send Origin. Browser scripts cannot open this endpoint.
    if headers.contains_key(header::ORIGIN) {
        return StatusCode::FORBIDDEN.into_response();
    }
    let Ok(pending) = Arc::clone(&access.pending_sockets).try_acquire_owned() else {
        return StatusCode::TOO_MANY_REQUESTS.into_response();
    };
    socket
        .max_message_size(256 * 1024)
        .max_frame_size(256 * 1024)
        .on_upgrade(move |socket| async move {
            tokio::select! {
                _ = app.shutdown.cancelled() => {},
                _ = serve(&app, socket, pending) => {},
            }
        })
}

#[derive(Deserialize)]
struct Request {
    #[serde(rename = "requestId", deserialize_with = "request_id")]
    id: Uuid,
    #[serde(default)]
    processor: String,
    op: String,
    args: serde_json::Map<String, Value>,
    #[serde(skip)]
    binary: bool,
}

fn request_id<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Uuid, D::Error> {
    let value = Value::deserialize(d)?;
    value
        .as_str()
        .or_else(|| value.get("@value").and_then(Value::as_str))
        .and_then(|value| Uuid::parse_str(value).ok())
        .ok_or_else(|| serde::de::Error::custom("Invalid Gremlin request ID"))
}

async fn receive(socket: &mut Input, wait: Duration) -> Option<Request> {
    use futures::StreamExt;
    tokio::time::timeout(wait, async {
        loop {
            match socket.next().await?.ok()? {
                Message::Binary(bytes) => {
                    let (&len, content) = bytes.split_first()?;
                    let mime = content.get(..usize::from(len))?;
                    let body = content.get(usize::from(len)..)?;
                    if mime == binary::MIME {
                        return binary::request(body);
                    }
                    return (mime == MIME)
                        .then(|| serde_json::from_slice(body).ok())
                        .flatten();
                }
                Message::Text(text) => return serde_json::from_str(text.as_str()).ok(),
                Message::Close(_) => return None,
                _ => {}
            }
        }
    })
    .await
    .ok()
    .flatten()
}

async fn serve(app: &Arc<App>, socket: WebSocket, pending: tokio::sync::OwnedSemaphorePermit) {
    use futures::StreamExt;
    let (mut output, mut input) = socket.split();
    let mut credential: Option<super::super::access::Credential> = None;
    let mut queued = std::collections::VecDeque::new();
    let mut pending = Some(pending);
    let mut _authenticated_slot = None;
    let auth_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let request = if let Some(request) = queued.pop_front() {
            request
        } else {
            let mut live = credential
                .as_ref()
                .map(|credential| credential.live.clone());
            let Some(request) = (tokio::select! {
                biased;
                () = super::super::access::revoked(&mut live) => return,
                request = receive(&mut input, Duration::from_secs(if credential.is_some() { 60 } else { 5 })) => request,
            }) else {
                return;
            };
            request
        };
        if credential.is_none() {
            if !matches!(
                tokio::time::timeout_at(
                    auth_deadline,
                    send(
                        &mut output,
                        request.id,
                        407,
                        "SASL PLAIN authentication required",
                        Vec::new(),
                        json!({}),
                        request.binary,
                    )
                )
                .await,
                Ok(true)
            ) {
                return;
            }
            credential = receive(
                &mut input,
                auth_deadline.saturating_duration_since(tokio::time::Instant::now()),
            )
            .await
            .and_then(|auth| {
                if auth.op != "authentication" || auth.binary != request.binary {
                    return None;
                }
                let sasl = auth.args.get("sasl")?.as_str()?;
                if sasl.len() > 512 {
                    return None;
                }
                let plain = String::from_utf8(STANDARD.decode(sasl).ok()?).ok()?;
                let mut parts = plain.split('\0');
                match (parts.next(), parts.next(), parts.next(), parts.next()) {
                    (Some(""), Some(user), Some(token), None) => {
                        super::super::access::authenticate(&app, user, token, false)
                    }
                    _ => None,
                }
            });
            app.access.record(
                credential.as_ref().map(|credential| &credential.principal),
                "gremlin",
                "login",
                None,
                if credential.is_some() { "ok" } else { "error" },
                if credential.is_some() { 200 } else { 401 },
            );
            if credential.is_none() {
                send(
                    &mut output,
                    request.id,
                    401,
                    "Invalid driver credentials",
                    Vec::new(),
                    json!({}),
                    request.binary,
                )
                .await;
                return;
            }
        }
        if pending.take().is_some() {
            let access = app.wire.as_ref().expect("Gremlin configured");
            let Ok(slot) = access.sockets.try_acquire() else {
                send_error(&mut output, &request, Error::busy()).await;
                return;
            };
            _authenticated_slot = Some(slot);
        }
        if let Some(ha) = &app.ha
            && (ha.recovery_pending() || !ha.leader.eligible())
        {
            return;
        }
        let credential = credential.as_ref().unwrap();
        let mut live = credential.live.clone();
        if !app.access.refresh() || live.has_changed().is_err() {
            return;
        }
        let query = stream(app, credential.principal.clone(), &mut output, &request);
        tokio::pin!(query);
        loop {
            tokio::select! {
                biased;
                _ = live.changed() => return,
                result = &mut query => {
                    if !result { return; }
                    break;
                }
                incoming = receive(&mut input, Duration::from_secs(65)) => {
                    let Some(request) = incoming else { return };
                    if queued.len() == 8 { return; }
                    queued.push_back(request);
                }
            }
        }
    }
}

async fn stream(
    app: &Arc<App>,
    principal: super::super::access::Principal,
    output: &mut Output,
    request: &Request,
) -> bool {
    let mut result = match run(app, principal, request).await {
        Ok(rows) => rows,
        Err(error) => return send_error(output, request, error).await,
    };
    let mut batch = Vec::new();
    let mut bytes = 0;
    let mut partial = false;
    loop {
        let values = match result.next().await {
            Ok(Some(values)) => values,
            Ok(None) => {
                let code = if batch.is_empty() && !partial {
                    204
                } else {
                    200
                };
                let sent = send(
                    output,
                    request.id,
                    code,
                    "",
                    batch,
                    json!({"model_revision": result.revision}),
                    request.binary,
                )
                .await;
                if sent {
                    result.finish("ok", 200);
                }
                return sent;
            }
            Err(error) => return send_error(output, request, error).await,
        };
        let value = if values.len() == 1
            && (result.result.graph_output.row_entity.is_some() || result.columns == ["value"])
        {
            graphson(values.into_iter().next().unwrap())
        } else {
            graphson(BoltValue::Dict(
                result.columns.iter().cloned().zip(values).collect(),
            ))
        };
        let value = if request.op == "bytecode" {
            typed(
                "g:Traverser",
                json!({"bulk":typed("g:Int64", json!(1)),"value":value}),
            )
        } else {
            value
        };
        let size = serde_json::to_vec(&value)
            .expect("serializable GraphSON")
            .len()
            + 1;
        if size > 2 * super::ROW_BYTES {
            result.finish("error", 413);
            return send_error(output, request, super::result_limit()).await;
        }
        if batch.len() >= 128 || bytes + size > 2 * super::ROW_BYTES {
            if !send(
                output,
                request.id,
                206,
                "",
                std::mem::take(&mut batch),
                json!({}),
                request.binary,
            )
            .await
            {
                return false;
            }
            bytes = 0;
            partial = true;
        }
        bytes += size;
        batch.push(value);
    }
}

async fn send_error(output: &mut Output, request: &Request, error: Error) -> bool {
    let code = match error.code {
        "timeout" => 598,
        "busy" | "draining" => 429,
        "model_forbidden" => 403,
        "not_found" => 404,
        "invalid_request" => 499,
        _ => 597,
    };
    send(
        output,
        request.id,
        code,
        &error.message,
        Vec::new(),
        json!({}),
        request.binary,
    )
    .await
}

type Output = futures::stream::SplitSink<WebSocket, Message>;
type Input = futures::stream::SplitStream<WebSocket>;

async fn send(
    socket: &mut Output,
    id: Uuid,
    code: u16,
    message: &str,
    data: Vec<Value>,
    attributes: Value,
    binary: bool,
) -> bool {
    use futures::SinkExt;
    let body = if binary {
        binary::response(id, code, message, data, attributes)
    } else {
        serde_json::to_vec(&json!({
            "requestId": id, "status": {"code":code,"message":message,"attributes":attributes},
            "result": {"data": typed("g:List", Value::Array(data)), "meta":{}}
        }))
        .expect("serializable GraphSON")
    };
    matches!(
        tokio::time::timeout(
            Duration::from_secs(5),
            socket.send(Message::Binary(body.into()))
        )
        .await,
        Ok(Ok(()))
    )
}

async fn run(
    app: &Arc<App>,
    principal: super::super::access::Principal,
    request: &Request,
) -> Result<Rows, Error> {
    let invalid = |message| Error::invalid("invalid_request", message);
    let configured = &app.wire.as_ref().expect("Gremlin configured").model;
    let default = || {
        super::super::access::default_model(app, &principal, configured)
            .unwrap_or_else(|| configured.clone())
    };
    let model = match request.args.get("aliases") {
        None => default(),
        Some(value) => {
            let aliases = value
                .as_object()
                .filter(|aliases| aliases.len() == 1)
                .ok_or_else(|| invalid("Expected one traversal source alias"))?;
            match aliases.get("g").and_then(Value::as_str) {
                Some("g") => default(),
                Some(model) => model.to_owned(),
                None => return Err(invalid("Expected g traversal source alias")),
            }
        }
    };
    let target = Target {
        principal,
        model,
        protocol: "gremlin",
    };
    let source = request
        .args
        .get("gremlin")
        .ok_or_else(|| invalid("Missing Gremlin traversal"))?;
    let statement = match (request.processor.as_str(), request.op.as_str()) {
        ("", "eval") => {
            if request
                .args
                .get("bindings")
                .is_some_and(|v| v != &json!({}))
            {
                return Err(invalid("Use literal arguments or traversal bytecode"));
            }
            source
                .as_str()
                .ok_or_else(|| invalid("Gremlin script must be a string"))?
                .to_owned()
        }
        ("traversal", "bytecode") => traversal(source, "g", 0)?,
        _ => return Err(invalid("Unsupported Gremlin processor or operation")),
    };
    let timeout = request
        .args
        .get("evaluationTimeout")
        .map(|value| {
            untyped(value)
                .as_u64()
                .filter(|ms| *ms > 0)
                .ok_or_else(|| invalid("Invalid evaluationTimeout"))
        })
        .transpose()?;
    super::execute(
        app,
        target,
        Language::Gremlin,
        &statement,
        Default::default(),
        timeout.map(Duration::from_millis),
        None,
    )
    .await
}

fn untyped(value: &Value) -> &Value {
    value.get("@value").unwrap_or(value)
}

fn traversal(value: &Value, root: &str, depth: usize) -> Result<String, Error> {
    let invalid = || {
        Error::invalid(
            "invalid_request",
            "Unsupported Gremlin bytecode argument or traversal source",
        )
    };
    if depth > 16 || value.get("@type").and_then(Value::as_str) != Some("g:Bytecode") {
        return Err(invalid());
    }
    let data = untyped(value);
    if data
        .get("source")
        .is_some_and(|v| !v.as_array().is_some_and(Vec::is_empty))
    {
        return Err(invalid());
    }
    let mut text = root.to_owned();
    if let Some(steps) = data.get("step") {
        for step in steps
            .as_array()
            .filter(|steps| steps.len() <= 256)
            .ok_or_else(invalid)?
        {
            let (name, args) = step
                .as_array()
                .and_then(|step| step.split_first())
                .ok_or_else(invalid)?;
            let name = name
                .as_str()
                .filter(|name| identifier(name))
                .ok_or_else(invalid)?;
            text.push('.');
            text.push_str(name);
            text.push('(');
            for (index, arg) in args.iter().enumerate() {
                if index > 0 {
                    text.push(',');
                }
                text.push_str(&argument(arg, depth + 1)?);
                if text.len() > 64 * 1024 {
                    return Err(invalid());
                }
            }
            text.push(')');
        }
    }
    Ok(text)
}

fn identifier(name: &str) -> bool {
    !name.is_empty() && name.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_')
}

fn argument(value: &Value, depth: usize) -> Result<String, Error> {
    let invalid = || Error::invalid("invalid_request", "Unsupported Gremlin bytecode argument");
    if depth > 16 {
        return Err(invalid());
    }
    let args = |values: &[Value]| -> Result<String, Error> {
        let mut text = String::new();
        for (index, arg) in values.iter().enumerate() {
            if index > 0 {
                text.push(',');
            }
            text.push_str(&argument(arg, depth + 1)?);
            if text.len() > 64 * 1024 {
                return Err(invalid());
            }
        }
        Ok(text)
    };
    let raw = untyped(value);
    match value.get("@type").and_then(Value::as_str) {
        Some("g:Bytecode") => traversal(value, "__", depth),
        Some("g:P" | "g:TextP") => {
            let name = raw
                .get("predicate")
                .and_then(Value::as_str)
                .filter(|s| identifier(s))
                .ok_or_else(invalid)?;
            let value = raw.get("value").ok_or_else(invalid)?;
            let value = if let Some(values) = untyped(value).as_array() {
                args(values)?
            } else {
                argument(value, depth + 1)?
            };
            Ok(format!("{name}({value})"))
        }
        Some("g:Order") if matches!(raw.as_str(), Some("asc" | "desc")) => {
            Ok(raw.as_str().unwrap().into())
        }
        Some("g:Int32" | "g:Int64" | "g:Float" | "g:Double") if raw.is_number() => {
            Ok(raw.to_string())
        }
        Some("g:List") | None if raw.is_array() => {
            Ok(format!("[{}]", args(raw.as_array().unwrap())?))
        }
        None if !raw.is_object() => Ok(raw.to_string()),
        _ => Err(invalid()),
    }
}

fn typed(name: &str, value: Value) -> Value {
    json!({"@type":name,"@value":value})
}

fn vertex_label(id: &str) -> String {
    id.split_once('[')
        .map_or(id, |(label, _)| label)
        .replace("%5B", "[")
        .replace("%5D", "]")
        .replace("%2C", ",")
        .replace("%25", "%")
}

fn graphson(value: BoltValue) -> Value {
    match value {
        BoltValue::Null => Value::Null,
        BoltValue::Boolean(value) => json!(value),
        BoltValue::String(value) => json!(value),
        BoltValue::Integer(value) => typed("g:Int64", json!(value)),
        BoltValue::Float(value) => typed(
            "g:Double",
            if value.is_finite() {
                json!(value)
            } else if value.is_nan() {
                json!("NaN")
            } else if value.is_sign_positive() {
                json!("Infinity")
            } else {
                json!("-Infinity")
            },
        ),
        BoltValue::Bytes(value) => json!(STANDARD.encode(value)),
        BoltValue::List(value) => typed(
            "g:List",
            Value::Array(value.into_iter().map(graphson).collect()),
        ),
        BoltValue::Dict(value) => typed(
            "g:Map",
            Value::Array(
                value
                    .into_iter()
                    .flat_map(|(key, value)| [json!(key), graphson(value)])
                    .collect(),
            ),
        ),
        BoltValue::Node(node) => typed(
            "g:Vertex",
            json!({
                "id": node.element_id, "label": node.labels.first(), "properties": node.properties.into_iter()
                    .map(|(key, value)| {
                        let property = typed("g:VertexProperty", json!({
                            "id": format!("{}:{key}", node.element_id), "label": key, "value": graphson(value),
                        }));
                        (key, json!([property]))
                    }).collect::<serde_json::Map<String, Value>>()
            }),
        ),
        BoltValue::Relationship(edge) => typed(
            "g:Edge",
            json!({
                "id":edge.element_id, "label":edge.rel_type, "outV":edge.start_element_id, "inV":edge.end_element_id,
            "outVLabel":vertex_label(&edge.start_element_id), "inVLabel":vertex_label(&edge.end_element_id),
                "properties": edge.properties.into_iter().map(|(key, value)| {
                    let property = typed("g:Property", json!({"key":key,"value":graphson(value)}));
                    (key, property)
                }).collect::<serde_json::Map<String, Value>>()
            }),
        ),
        BoltValue::UnboundRelationship(mut path) => {
            let BoltValue::List(objects) = path
                .properties
                .remove("objects")
                .expect("Gremlin path objects")
            else {
                unreachable!()
            };
            let labels = vec![typed("g:Set", json!([])); objects.len()];
            typed(
                "g:Path",
                json!({
                    "labels":typed("g:List", json!(labels)), "objects":graphson(BoltValue::List(objects)),
                }),
            )
        }
        _ => unreachable!("unsupported driver value"),
    }
}
