mod impact;
mod templates;

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::time::Instant;

use crate::{
    BusinessTask, Engine, Language, QueryOptions, SourceVersion,
    arrow::{
        array::{Array, Int64Array},
        datatypes::DataType,
        util::display::{ArrayFormatter, FormatOptions},
    },
};

use super::{
    ApiResult, App, Error,
    access::{self, Principal},
    freshness, local_tables, query, users,
};

pub(super) use impact::{
    ImpactInput, ImpactStamp, SaveTaskSuite, check_suite_revision, http_business_status,
    http_impact, http_read_suite, http_rollback_impact, http_save_suite, managed_suite_path,
    save_suite, suite_path, suite_revision, validate_impact,
};

const MAX_CELL_BYTES: usize = 1024;
const MAX_TASK_BYTES: usize = 768 * 1024;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RunTask {
    pub model: String,
    pub model_revision: String,
    pub task: BusinessTask,
    pub parameters: Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OffboardParameters {
    namespace: String,
    person_id: String,
    as_of: String,
    window_start: String,
    window_end: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AlertParameters {
    alert_id: String,
    window_start: String,
    window_end: String,
}

struct Parameters {
    values: BTreeMap<String, Value>,
    window_start: i64,
    window_end: i64,
    as_of: Option<i64>,
}

fn time(value: &str) -> Result<i64> {
    let parsed = chrono::DateTime::parse_from_rfc3339(value)?;
    ensure!(
        parsed.timestamp_subsec_nanos().is_multiple_of(1_000_000),
        "time precision must be milliseconds or coarser"
    );
    Ok(parsed.timestamp_millis())
}

fn identifier(value: String) -> Result<Value> {
    ensure!(
        !value.trim().is_empty() && value.len() <= 256 && !value.contains('\0'),
        "identifiers must contain 1 to 256 bytes"
    );
    Ok(Value::String(value))
}

impl Parameters {
    fn new(task: BusinessTask, value: Value) -> Result<Self> {
        let (mut values, window_start, window_end, as_of) = match task {
            BusinessTask::OffboardedAccountExposure => {
                let p = serde_json::from_value::<OffboardParameters>(value)
                    .context("expected namespace, person_id, as_of, window_start and window_end")?;
                (
                    BTreeMap::from([
                        ("namespace".into(), identifier(p.namespace)?),
                        ("person_id".into(), identifier(p.person_id)?),
                    ]),
                    time(&p.window_start)?,
                    time(&p.window_end)?,
                    Some(time(&p.as_of)?),
                )
            }
            BusinessTask::AlertIdentityDeviceContext => {
                let p = serde_json::from_value::<AlertParameters>(value)
                    .context("expected alert_id, window_start and window_end")?;
                (
                    BTreeMap::from([("alert_id".into(), identifier(p.alert_id)?)]),
                    time(&p.window_start)?,
                    time(&p.window_end)?,
                    None,
                )
            }
        };
        ensure!(
            window_start < window_end && window_end - window_start <= 31 * 86_400_000,
            "event window must be positive and at most 31 days"
        );
        ensure!(
            as_of.is_none_or(|point| point >= window_end),
            "window_end must be at or before as_of"
        );
        values.insert("window_start".into(), json!(window_start));
        values.insert("window_end".into(), json!(window_end));
        if let Some(as_of) = as_of {
            values.insert("as_of".into(), json!(as_of));
        }
        Ok(Self {
            values,
            window_start,
            window_end,
            as_of,
        })
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
struct Evidence {
    label: String,
    tenant_id: String,
    source_system: Option<String>,
    record_id: Option<String>,
    observed_at_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    source_version: Option<String>,
}

impl Evidence {
    fn complete(&self) -> bool {
        self.source_system
            .as_ref()
            .is_some_and(|v| !v.trim().is_empty())
            && self
                .record_id
                .as_ref()
                .is_some_and(|v| !v.trim().is_empty())
            && self.observed_at_ms.is_some()
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
struct Finding {
    kind: String,
    values: BTreeMap<String, Option<String>>,
    evidence: Vec<Evidence>,
}

type Row = BTreeMap<String, Option<String>>;

struct StepRows {
    rows: Vec<Row>,
    sources: Vec<SourceVersion>,
    truncated: bool,
}

#[derive(Serialize)]
struct TaskResult {
    scope: Value,
    subject: &'static str,
    anchor: Option<Evidence>,
    findings: Vec<Finding>,
    sources: BTreeMap<&'static str, Vec<SourceVersion>>,
    execution_status: &'static str,
    truncated: bool,
    limitations: Vec<&'static str>,
}

fn take_evidence(step: &templates::Step, row: &mut Row, tenant: &str) -> Result<Vec<Evidence>> {
    step.evidence
        .iter()
        .enumerate()
        .map(|(index, (_, label))| {
            let mut get = |column: &str| row.remove(&format!("ev{index}_{column}")).flatten();
            Ok(Evidence {
                label: (*label).into(),
                tenant_id: tenant.into(),
                record_id: get("record_id"),
                source_system: get("source_system"),
                observed_at_ms: get("observed_at_ms").map(|v| v.parse()).transpose()?,
                source_version: get("source_version"),
            })
        })
        .collect()
}

async fn read_step(
    engine: &Engine,
    task: BusinessTask,
    step: &templates::Step,
    mut options: QueryOptions,
    parameters: &Parameters,
    start: Instant,
    max_rows: usize,
) -> Result<StepRows> {
    options.timeout = options
        .timeout
        .checked_sub(start.elapsed())
        .filter(|v| !v.is_zero())
        .context("query deadline exceeded")?;
    options.graph.snapshot_time = step
        .snapshot
        .then(|| parameters.as_of.expect("grant step has as_of").to_string());
    options.parameters.clone_from(&parameters.values);
    let mut statement = format!("{} RETURN {}", step.pattern, step.fields);
    for (index, (alias, label)) in step.evidence.iter().enumerate() {
        for column in ["record_id", "source_system", "observed_at_ms"] {
            write!(statement, ", {alias}.{column} AS ev{index}_{column}")?;
        }
        if engine.catalog.task_has_source_version(task, label) {
            write!(
                statement,
                ", {alias}.source_version AS ev{index}_source_version"
            )?;
        }
    }
    // One extra row distinguishes an exact fit from an incomplete result.
    write!(statement, " LIMIT {}", max_rows + 1)?;
    let mut output = Box::pin(engine.query(Language::Cypher, &statement, options)).await?;
    let mut rows = Vec::new();
    let mut bytes = 0;
    let mut truncated = false;
    let format = FormatOptions::default();
    'batches: while let Some(batch) = output.batches.try_next().await? {
        let schema = batch.schema_ref();
        let formatters = batch
            .columns()
            .iter()
            .map(|column| ArrayFormatter::try_new(column.as_ref(), &format))
            .collect::<Result<Vec<_>, _>>()?;
        for row in 0..batch.num_rows() {
            if rows.len() >= max_rows {
                truncated = true;
                break 'batches;
            }
            let mut record = BTreeMap::new();
            for (i, field) in schema.fields().iter().enumerate() {
                let array = batch.column(i);
                let name = field.name();
                let numeric = name.ends_with("_ms");
                let value = if array.is_null(row) {
                    None
                } else if numeric {
                    Some(
                        array
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .context("business time field is not Int64")?
                            .value(row)
                            .to_string(),
                    )
                } else {
                    ensure!(
                        matches!(
                            field.data_type(),
                            DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8
                        ),
                        "business identifier or evidence field is not text"
                    );
                    Some(formatters[i].value(row).try_to_string()?)
                };
                if value.as_ref().is_some_and(|s| s.len() > MAX_CELL_BYTES) {
                    truncated = true;
                    break 'batches;
                }
                bytes += value.as_ref().map_or(0, String::len) + name.len();
                if bytes > MAX_TASK_BYTES {
                    truncated = true;
                    break 'batches;
                }
                record.insert(name.clone(), value);
            }
            rows.push(record);
        }
    }
    Ok(StepRows {
        rows,
        sources: output.versions,
        truncated,
    })
}

async fn execute(
    engine: &Engine,
    task: BusinessTask,
    mut parameters: Parameters,
    options: QueryOptions,
    max_rows: usize,
) -> Result<TaskResult> {
    let start = Instant::now();
    let scope = json!({"window_start_ms":parameters.window_start,"window_end_ms":parameters.window_end,"as_of_ms":parameters.as_of});
    let mut result = TaskResult {
        scope,
        subject: "not_found",
        anchor: None,
        findings: Vec::new(),
        sources: BTreeMap::new(),
        execution_status: "complete",
        truncated: false,
        limitations: match task {
            BusinessTask::OffboardedAccountExposure => vec![
                "Grants show recorded direct entitlements and one account-to-role path. Deny policies, conditions, account disablement and live access are outside this task.",
                "Access outcomes represent events in the returned source coverage. Missing logs or grants leave access unknown.",
            ],
            BusinessTask::AlertIdentityDeviceContext => vec![
                "Identity assignments apply at the alert time. Device activity and shared IPs are correlations.",
                "Source coverage, retention and freshness may be incomplete. Multiple assignments remain separate.",
            ],
        },
    };
    let anchor = templates::anchor(task);
    let mut read = read_step(
        engine,
        task,
        &anchor,
        options.clone(),
        &parameters,
        start,
        2,
    )
    .await?;
    result.sources.insert(anchor.kind, read.sources);
    ensure!(
        !read.truncated && read.rows.len() <= 1,
        "task subject is ambiguous or exceeds result limits"
    );
    let Some(mut row) = read.rows.pop() else {
        return Ok(result);
    };
    let tenant = row
        .remove("tenant_id")
        .flatten()
        .filter(|s| !s.trim().is_empty())
        .context("task subject has no tenant")?;
    result.anchor = take_evidence(&anchor, &mut row, &tenant)?.pop();
    result.subject = "found";
    result.scope["tenant_id"] = json!(tenant);
    parameters.values.insert("tenant".into(), json!(tenant));
    if let Some((key, value)) = row.into_iter().next() {
        let point = value.as_deref().map(str::parse::<i64>).transpose()?;
        result.scope[&key] = json!(point);
        match task {
            BusinessTask::OffboardedAccountExposure => {
                if point.is_none_or(|t| t > parameters.as_of.expect("offboarding as_of")) {
                    result.subject = "no_departure_at_as_of";
                } else {
                    parameters.values.insert("departed".into(), json!(point));
                }
            }
            BusinessTask::AlertIdentityDeviceContext => {
                let point = point.context("alert has no event time")?;
                ensure!(
                    point >= parameters.window_start && point < parameters.window_end,
                    "event window must include the alert time"
                );
                parameters.values.insert("alert_time".into(), json!(point));
            }
        }
    }
    if result.anchor.as_ref().is_none_or(|e| !e.complete()) {
        result.execution_status = "partial";
        result
            .limitations
            .push("The subject record is missing required evidence.");
    }
    if result.subject != "found" {
        return Ok(result);
    }
    let mut bytes = 0;
    let mut missing_evidence = false;
    for step in templates::steps(task) {
        let remaining = max_rows.saturating_sub(result.findings.len());
        let read = read_step(
            engine,
            task,
            step,
            options.clone(),
            &parameters,
            start,
            remaining,
        )
        .await
        .with_context(|| format!("executing task step {}", step.kind))?;
        result.sources.insert(step.kind, read.sources);
        for mut values in read.rows {
            let evidence = take_evidence(step, &mut values, &tenant)?;
            missing_evidence |= evidence.iter().any(|e| !e.complete());
            let kind = if step.kind == "access_after_departure" {
                match values.get("outcome").and_then(|v| v.as_deref()) {
                    Some("success") => "successful_access_after_departure",
                    Some("failure") => "failed_access_after_departure",
                    _ => "unknown_access_outcome",
                }
            } else {
                step.kind
            };
            let finding = Finding {
                kind: kind.into(),
                values,
                evidence,
            };
            bytes += serde_json::to_vec(&finding)?.len();
            if bytes > MAX_TASK_BYTES {
                result.truncated = true;
                break;
            }
            result.findings.push(finding);
        }
        result.truncated |= read.truncated;
        if bytes > MAX_TASK_BYTES {
            break;
        }
    }
    result.findings.sort();
    if result.truncated || missing_evidence {
        result.execution_status = "partial";
    }
    if missing_evidence {
        result
            .limitations
            .push("One or more findings are missing required source evidence.");
    }
    Ok(result)
}

pub(super) fn task_args(task: BusinessTask) -> Value {
    let mut properties = json!({
        "window_start":{"type":"string","format":"date-time"},
        "window_end":{"type":"string","format":"date-time"},
    });
    let mut required = vec!["window_start", "window_end"];
    let key = json!({"type":"string","minLength":1,"maxLength":256});
    match task {
        BusinessTask::OffboardedAccountExposure => {
            properties["namespace"] = key.clone();
            properties["person_id"] = key;
            properties["as_of"] = json!({"type":"string","format":"date-time"});
            required.extend(["namespace", "person_id", "as_of"]);
        }
        BusinessTask::AlertIdentityDeviceContext => {
            properties["alert_id"] = key;
            required.push("alert_id");
        }
    }
    json!({"type":"object","properties":properties,"required":required,"additionalProperties":false})
}

async fn published(
    app: &Arc<App>,
    principal: &Principal,
    model: &str,
    expected: Option<&str>,
) -> ApiResult<(String, Engine, Option<ImpactStamp>)> {
    access::check_query(app, principal, model)?;
    let revision = match expected {
        Some(revision) => revision.to_owned(),
        None => app.published_model_revision(model).await?,
    };
    let (_, _, engine) = app.query_model(model, &revision, principal).await?;
    let models = app.models.lock().unwrap().read_files();
    let stamp = models.task_stamp(model, &revision)?;
    Ok((revision, engine, stamp))
}

pub(super) async fn list(app: &Arc<App>, principal: &Principal, model: &str) -> ApiResult<Value> {
    let (revision, engine, stamp) = published(app, principal, model, None).await?;
    if let Some(term) = access::partition(app, principal, model)? {
        users::apply_partition(&engine.catalog, &mut Default::default(), &term)?;
    }
    let tasks = engine.catalog.tasks.iter().map(|task| {
        json!({"id":task, "parameters":task_args(*task), "validation":stamp.as_ref(),
            "description":match task {
                BusinessTask::OffboardedAccountExposure => "Find residual grants and post-departure access events with evidence.",
                BusinessTask::AlertIdentityDeviceContext => "Find direct alert context, identity assignments and device or IP correlations with evidence.",
            }})
    }).collect::<Vec<_>>();
    Ok(json!({"model_revision":revision,"tasks":tasks}))
}

pub(super) async fn run(
    app: &Arc<App>,
    principal: &Principal,
    args: RunTask,
    timeout: Duration,
    max_rows: usize,
) -> ApiResult<Value> {
    let (revision, mut engine, stamp) =
        published(app, principal, &args.model, Some(&args.model_revision)).await?;
    if !engine.catalog.tasks.contains(&args.task) {
        return Err(Error::not_found());
    }
    let stamp = stamp.ok_or_else(|| {
        Error::invalid("task_unvalidated", "发布版本需要使用当前业务场景重新校验")
    })?;
    let p = Parameters::new(args.task, args.parameters)
        .map_err(|error| Error::invalid("invalid_task_parameters", error.to_string()))?;
    let mut options = app.options.clone();
    options.timeout = options.timeout.min(timeout);
    let _active = query::admit(
        app,
        uuid::Uuid::new_v4(),
        &args.model,
        principal,
        &mut options,
    )
    .await?;
    let partition = access::partition(app, principal, &args.model)?;
    if let Some(term) = &partition {
        users::apply_partition(&engine.catalog, &mut options.graph, term)?;
    }
    access::check_query(app, principal, &args.model)?;
    let policy = freshness::policy(&engine.catalog, None);
    let strict = policy != crate::FreshnessPolicy::default();
    let observations = if strict {
        freshness::observe(app, &engine.catalog).await
    } else {
        Vec::new()
    };
    freshness::check(&policy, &observations, false)?;
    options.graph.require_consistent_snapshot |= policy.require_consistent_snapshot;
    let started = Instant::now();
    let _selection = tokio::time::timeout(
        options.timeout,
        local_tables::managed::resolve(
            app,
            &mut engine,
            &args.model,
            options.timeout.as_secs() + 30,
        ),
    )
    .await
    .map_err(|_| task_error(anyhow::anyhow!("task source resolution timed out")))??;
    options.timeout = options.timeout.saturating_sub(started.elapsed());
    let result = execute(&engine, args.task, p, options, max_rows)
        .await
        .map_err(task_error)?;
    if strict {
        if result
            .sources
            .values()
            .any(|versions| !freshness::versions_match(&observations, versions, false))
        {
            return Err(freshness::conflict("本地副本在查询固定输入期间更新"));
        }
        freshness::recheck(
            app,
            &engine.catalog,
            &args.model,
            &revision,
            &policy,
            &observations,
            false,
        )
        .await?;
    }
    access::check_query(app, principal, &args.model)?;
    if access::partition(app, principal, &args.model)? != partition {
        return Err(access::forbidden());
    }
    let coverage = coverage(&result.findings, args.task);
    let mut result = serde_json::to_value(result).map_err(|_| Error::storage())?;
    result["coverage"] = coverage;
    result["model_revision"] = json!(revision);
    result["task"] = json!(args.task);
    result["validation_report_id"] = json!(stamp.report_id);
    Ok(result)
}

fn coverage(findings: &[Finding], task: BusinessTask) -> Value {
    let mut counts = BTreeMap::new();
    let mut incomplete = 0;
    let mut observed = BTreeSet::new();
    let mut assigned = BTreeMap::<&str, BTreeSet<(&str, &str)>>::new();
    for finding in findings {
        *counts.entry(&finding.kind).or_insert(0) += 1;
        incomplete += usize::from(finding.evidence.iter().any(|item| !item.complete()));
        let get = |field: &str| finding.values.get(field).and_then(|value| value.as_deref());
        if let Some(account) = get("account_id") {
            if finding.kind == "direct_account" {
                observed.insert(account);
            }
            if finding.kind == "identity_assignment"
                && let (Some(namespace), Some(person)) = (get("namespace"), get("person_id"))
            {
                assigned
                    .entry(account)
                    .or_default()
                    .insert((namespace, person));
            }
        }
    }
    let mut coverage = json!({
        "finding_counts":counts, "incomplete_evidence_findings":incomplete,
        "source_retention":"unknown", "source_freshness":"unknown"
    });
    if task == BusinessTask::AlertIdentityDeviceContext {
        coverage["direct_accounts_without_identity"] = json!(
            observed
                .into_iter()
                .filter(|account| !assigned.contains_key(account))
                .collect::<Vec<_>>()
        );
        coverage["accounts_with_multiple_identities"] = json!(
            assigned
                .iter()
                .filter_map(|(account, candidates)| (candidates.len() > 1).then_some(account))
                .collect::<Vec<_>>()
        );
    }
    coverage
}

fn task_error(error: anyhow::Error) -> Error {
    for cause in error.chain() {
        let message = cause.to_string();
        if [
            "task subject is ambiguous",
            "task subject has no tenant",
            "alert has no event time",
            "event window must include the alert time",
        ]
        .iter()
        .any(|prefix| message.starts_with(prefix))
        {
            return Error::invalid("task_precondition", message);
        }
    }
    query::query_error(error)
}
