pub(super) mod bolt;
mod entities;
pub(super) mod gremlin;

use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Result, ensure};
use axum::http::StatusCode;
use boltr::{
    packstream::encode::encode_value,
    types::{BoltDict, BoltValue},
};
use futures::TryStreamExt;
use opentelemetry::{Context, trace::FutureExt as _};
use serde_json::Value;

use super::{ApiResult, App, Error, access::Principal, models::Models, query, telemetry};
use crate::{
    Engine, Language, QueryResult, ScalarValue,
    arrow::{
        array::{
            Array, ArrayRef, BinaryArray, BinaryViewArray, FixedSizeBinaryArray, LargeBinaryArray,
            LargeStringArray, RecordBatch, StringArray, StringViewArray, StructArray,
        },
        datatypes::{Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type, DecimalType},
    },
    engine::ReadTransaction,
};

const ROW_BYTES: usize = 1024 * 1024;

pub(super) struct Access {
    pub(super) model: String,
    sockets: tokio::sync::Semaphore,
    pending_sockets: Arc<tokio::sync::Semaphore>,
}

impl Access {
    pub(super) fn load(models: &Models, model: String) -> Result<Self> {
        ensure!(
            models.has_driver_model(&model).unwrap_or(false),
            "--driver-model must name a model with version history"
        );
        Ok(Self {
            model,
            sockets: tokio::sync::Semaphore::new(16),
            pending_sockets: Arc::new(tokio::sync::Semaphore::new(16)),
        })
    }
}

#[derive(Clone)]
struct Target {
    principal: Principal,
    model: String,
    protocol: &'static str,
}

impl Target {
    async fn engine(&self, app: &Arc<App>) -> ApiResult<(String, Engine)> {
        super::access::check_query(app, &self.principal, &self.model)?;
        let target = self.clone();
        let owner = app.clone();
        let (revision, _, engine) = app
            .read_models(move |files| {
                files.query_engine(&owner, &target.model, None, &target.principal)
            })
            .await?;
        if engine.catalog.freshness.policy != crate::FreshnessPolicy::default() {
            return Err(super::freshness::conflict(
                "此模型的严格查询策略需通过查询 API 或 MCP 执行",
            ));
        }
        Ok((revision, engine))
    }

    fn audit(&self, app: &App, action: &str, outcome: &str, code: u16) {
        app.access.record(
            Some(&self.principal),
            self.protocol,
            action,
            Some(&self.model),
            outcome,
            code,
        );
    }
}

type DriverTransaction<'a> = (
    &'a str,
    &'a Engine,
    &'a ReadTransaction,
    Option<&'a Arc<super::local_tables::managed::Selection>>,
);

struct Rows {
    app: Arc<App>,
    target: Target,
    language: Language,
    active: query::Active,
    engine: Engine,
    result: QueryResult,
    _managed: Option<Arc<super::local_tables::managed::Selection>>,
    batch: Option<RecordBatch>,
    offset: usize,
    finished: bool,
    columns: Vec<String>,
    revision: String,
    started: Instant,
    deadline: Instant,
    span: Option<Context>,
    leadership: Option<crate::CancellationToken>,
}

async fn execute(
    app: &Arc<App>,
    target: Target,
    language: Language,
    statement: &str,
    parameters: BTreeMap<String, Value>,
    timeout: Option<Duration>,
    transaction: Option<DriverTransaction<'_>>,
) -> ApiResult<Rows> {
    let started = Instant::now();
    let span = telemetry::start_span("query", target.protocol, None);
    let mut abandoned = PendingQuery {
        app,
        target: &target,
        language,
        pending: true,
        started,
        span: span.as_ref(),
    };
    let leadership = app
        .ha
        .as_ref()
        .map(|ha| ha.leader.admission())
        .transpose()?;
    let query = execute_inner(
        app,
        &target,
        language,
        statement,
        parameters,
        timeout,
        transaction,
        started,
    );
    let result = tokio::select! {
        biased;
        _ = super::await_leadership_pause(leadership.as_ref()) => Err(super::cluster::control::interrupted()),
        result = async {
            if let Some(span) = &span {
                query.with_context(span.clone()).await
            } else {
                query.await
            }
        } => result,
    };
    abandoned.pending = false;
    drop(abandoned);
    if let Err(error) = &result {
        app.metrics
            .query_error(super::ErrorCode::class(error.code).0);
        finish_query(
            app,
            &target,
            language,
            started,
            span.as_ref(),
            "error",
            error.status.as_u16(),
        );
    }
    result.map(|mut rows| {
        rows.span = span;
        rows.leadership = leadership;
        rows
    })
}

struct PendingQuery<'a> {
    app: &'a App,
    target: &'a Target,
    language: Language,
    pending: bool,
    started: Instant,
    span: Option<&'a Context>,
}

impl Drop for PendingQuery<'_> {
    fn drop(&mut self) {
        if self.pending {
            finish_query(
                self.app,
                self.target,
                self.language,
                self.started,
                self.span,
                "abandoned",
                499,
            );
        }
    }
}

fn finish_query(
    app: &App,
    target: &Target,
    language: Language,
    started: Instant,
    span: Option<&Context>,
    outcome: &str,
    status: u16,
) {
    app.metrics.driver(language, status == 200);
    app.metrics
        .observe(target.protocol, "query", status, started.elapsed());
    let _current = span.map(|span| span.clone().attach());
    target.audit(app, "query", outcome, status);
    if let Some(span) = span {
        telemetry::finish_span(span, status);
    }
}

async fn execute_inner(
    app: &Arc<App>,
    target: &Target,
    language: Language,
    statement: &str,
    parameters: BTreeMap<String, Value>,
    timeout: Option<Duration>,
    transaction: Option<DriverTransaction<'_>>,
    started: Instant,
) -> ApiResult<Rows> {
    super::access::check_query(app, &target.principal, &target.model)?;
    if statement.trim().is_empty() || statement.len() > 64 * 1024 {
        return Err(Error::invalid(
            "invalid_query",
            "Query must contain 1 to 65536 bytes",
        ));
    }
    let mut options = app.options.clone();
    if let Some(timeout) = timeout {
        if timeout.is_zero() {
            return Err(Error::new(
                StatusCode::GATEWAY_TIMEOUT,
                "timeout",
                "Read transaction expired",
            ));
        }
        options.timeout = options.timeout.min(timeout);
    }
    let deadline = started + options.timeout;
    options.parameters = parameters;
    let active = query::admit(
        app,
        uuid::Uuid::new_v4(),
        &target.model,
        &target.principal,
        &mut options,
    )
    .await?;
    let (revision, engine, pinned, managed) =
        if let Some((revision, engine, pinned, managed)) = transaction {
            app.published_model_revision(&target.model).await?;
            (
                revision.to_owned(),
                engine.clone(),
                Some(pinned),
                managed.cloned(),
            )
        } else {
            let (revision, mut engine) = target.engine(app).await?;
            let managed = super::local_tables::managed::resolve(
                app,
                &mut engine,
                &target.model,
                options.timeout.as_secs() + 120,
            )
            .await?
            .map(Arc::new);
            (revision, engine, None, managed)
        };
    if let Some(term) = super::access::partition(app, &target.principal, &target.model)? {
        super::users::apply_partition(&engine.catalog, &mut options.graph, &term)?;
    }
    let result = Box::pin(engine.driver_query(language, statement, options, pinned))
        .await
        .map_err(query::query_error)?;
    let columns = result
        .batches
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    Ok(Rows {
        app: app.clone(),
        target: target.clone(),
        language,
        active,
        engine,
        result,
        _managed: managed,
        batch: None,
        offset: 0,
        finished: false,
        columns,
        revision,
        started,
        deadline,
        span: None,
        leadership: None,
    })
}

impl Rows {
    fn finish(&mut self, outcome: &str, status: u16) {
        if !self.finished {
            self.finished = true;
            let _current = self.span.as_ref().map(|span| span.clone().attach());
            super::ops::observe_query_io(&self.result);
            finish_query(
                &self.app,
                &self.target,
                self.language,
                self.started,
                self.span.as_ref(),
                outcome,
                status,
            );
        }
    }

    async fn next(&mut self) -> ApiResult<Option<Vec<BoltValue>>> {
        let leadership = self.leadership.clone();
        let result = tokio::select! {
            biased;
            _ = super::await_leadership_pause(leadership.as_ref()) => Err(super::cluster::control::interrupted()),
            result = self.next_inner() => result,
        };
        match &result {
            Ok(None) if matches!(self.language, Language::Cypher) => self.finish("ok", 200),
            Err(error) => self.finish("error", error.status.as_u16()),
            _ => {}
        }
        result
    }

    async fn next_inner(&mut self) -> ApiResult<Option<Vec<BoltValue>>> {
        if self.finished {
            return Ok(None);
        }
        if Instant::now() >= self.deadline {
            return Err(Error::new(
                StatusCode::GATEWAY_TIMEOUT,
                "timeout",
                "Query deadline exceeded",
            ));
        }
        if self.active.token.is_cancelled() {
            return Err(Error::new(
                StatusCode::CONFLICT,
                "cancelled",
                "Query cancelled",
            ));
        }
        loop {
            if let Some(batch) = &self.batch
                && self.offset < batch.num_rows()
            {
                break;
            }
            self.batch = self
                .result
                .batches
                .try_next()
                .await
                .map_err(|error| query::query_error(error.into()))?;
            self.offset = 0;
            if self.batch.is_none() {
                return Ok(None);
            }
        }
        let batch = self.batch.as_ref().unwrap();
        let mut budget = ROW_BYTES;
        let values = if let Some((label, node)) = self.result.graph_output.row_entity.clone() {
            vec![
                entities::fields(
                    &self.engine.catalog,
                    &mut self.result,
                    &label,
                    node,
                    self.offset,
                    |name| batch.column_by_name(name),
                    &mut budget,
                )
                .await?,
            ]
        } else {
            let mut values = Vec::with_capacity(batch.num_columns());
            for (name, column) in self.columns.iter().zip(batch.columns()) {
                let shape = self.result.graph_output.columns.get(name).cloned();
                let variant = self.result.graph_output.gremlin_variant;
                values.push(
                    entities::value(
                        &self.engine.catalog,
                        &mut self.result,
                        column,
                        self.offset,
                        shape.as_ref(),
                        variant,
                        &mut budget,
                    )
                    .await?,
                );
            }
            values
        };
        let mut encoded = bytes::BytesMut::new();
        for value in &values {
            encode_value(&mut encoded, value);
        }
        if encoded.len() > ROW_BYTES {
            return Err(result_limit());
        }
        self.offset += 1;
        Ok(Some(values))
    }
}

impl Drop for Rows {
    fn drop(&mut self) {
        self.finish("abandoned", 499);
    }
}

fn result_limit() -> Error {
    Error::new(
        StatusCode::PAYLOAD_TOO_LARGE,
        "result_limit",
        "Driver record exceeds the 1 MiB encoding budget",
    )
}

fn value_at(
    column: &ArrayRef,
    row: usize,
    depth: usize,
    gremlin_variant: bool,
    remaining: &mut usize,
) -> ApiResult<BoltValue> {
    if depth > 24 {
        return Err(result_limit());
    }
    // Account for the in-memory values before constructing nested records.
    *remaining = remaining
        .checked_sub(std::mem::size_of::<BoltValue>())
        .ok_or_else(result_limit)?;
    if column.is_null(row) {
        return Ok(BoltValue::Null);
    }
    let array = column.as_any();
    macro_rules! variable_width {
        ($($type:ty),+ => $variant:ident) => {
            $(if let Some(array) = array.downcast_ref::<$type>() {
                let value = array.value(row);
                if value.len() > *remaining {
                    return Err(result_limit());
                }
                return Ok(BoltValue::$variant(value.to_owned()));
            })+
        };
    }
    variable_width!(StringArray, StringViewArray, LargeStringArray => String);
    variable_width!(BinaryArray, BinaryViewArray, LargeBinaryArray, FixedSizeBinaryArray => Bytes);
    if let Some(record) = array.downcast_ref::<StructArray>()
        && (gremlin_variant || crate::engine::variant_fields(record.fields()))
    {
        return match record.columns().iter().find(|item| item.is_valid(row)) {
            Some(item) => value_at(item, row, depth + 1, false, remaining),
            None => Ok(BoltValue::Null),
        };
    }
    use ScalarValue::*;
    let value = ScalarValue::try_from_array(column, row)
        .map_err(|_| Error::invalid("result_type", "Unsupported driver result type"))?;
    if value.is_null() {
        return Ok(BoltValue::Null);
    }
    let mut list = |array: ArrayRef| -> ApiResult<BoltValue> {
        if array.len() > *remaining / std::mem::size_of::<BoltValue>() {
            return Err(result_limit());
        }
        Ok(BoltValue::List(
            (0..array.len())
                .map(|i| value_at(&array, i, depth + 1, false, remaining))
                .collect::<ApiResult<_>>()?,
        ))
    };
    Ok(match value {
        Boolean(Some(v)) => BoltValue::Boolean(v),
        Int8(Some(v)) => BoltValue::Integer(v.into()),
        Int16(Some(v)) => BoltValue::Integer(v.into()),
        Int32(Some(v)) => BoltValue::Integer(v.into()),
        Int64(Some(v)) => BoltValue::Integer(v),
        UInt8(Some(v)) => BoltValue::Integer(v.into()),
        UInt16(Some(v)) => BoltValue::Integer(v.into()),
        UInt32(Some(v)) => BoltValue::Integer(v.into()),
        UInt64(Some(v)) => {
            i64::try_from(v).map_or_else(|_| BoltValue::String(v.to_string()), BoltValue::Integer)
        }
        Float16(Some(v)) => BoltValue::Float(v.to_f64()),
        Float32(Some(v)) => BoltValue::Float(v.into()),
        Float64(Some(v)) => BoltValue::Float(v),
        Utf8(Some(v)) | Utf8View(Some(v)) | LargeUtf8(Some(v)) => BoltValue::String(v),
        Binary(Some(v))
        | BinaryView(Some(v))
        | LargeBinary(Some(v))
        | FixedSizeBinary(_, Some(v)) => BoltValue::Bytes(v),
        Decimal32(Some(v), p, s) => BoltValue::String(Decimal32Type::format_decimal(v, p, s)),
        Decimal64(Some(v), p, s) => BoltValue::String(Decimal64Type::format_decimal(v, p, s)),
        Decimal128(Some(v), p, s) => BoltValue::String(Decimal128Type::format_decimal(v, p, s)),
        Decimal256(Some(v), p, s) => BoltValue::String(Decimal256Type::format_decimal(v, p, s)),
        List(v) => list(v.value(0))?,
        LargeList(v) => list(v.value(0))?,
        FixedSizeList(v) => list(v.value(0))?,
        ListView(v) => list(v.value(0))?,
        LargeListView(v) => list(v.value(0))?,
        Struct(v) => BoltValue::Dict(
            v.fields()
                .iter()
                .zip(v.columns())
                .map(|(field, col)| {
                    Ok((
                        field.name().clone(),
                        value_at(col, 0, depth + 1, false, remaining)?,
                    ))
                })
                .collect::<ApiResult<BoltDict>>()?,
        ),
        Map(v) => {
            let entries = v.value(0);
            let mut fields = BoltDict::new();
            for index in 0..entries.len() {
                let key = value_at(entries.column(0), index, depth + 1, false, remaining)?;
                let BoltValue::String(key) = key else {
                    return Err(Error::invalid(
                        "result_type",
                        "Driver maps require string keys",
                    ));
                };
                fields.insert(
                    key,
                    value_at(entries.column(1), index, depth + 1, false, remaining)?,
                );
            }
            BoltValue::Dict(fields)
        }
        _ => {
            let formatter = crate::arrow::util::display::ArrayFormatter::try_new(
                column.as_ref(),
                &Default::default(),
            )
            .map_err(|_| Error::invalid("result_type", "Unsupported driver result type"))?;
            BoltValue::String(formatter.value(row).to_string())
        }
    })
}
