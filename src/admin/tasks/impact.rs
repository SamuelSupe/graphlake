use std::{
    collections::{BTreeMap, BTreeSet},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result};
use axum::{
    Extension, Json,
    extract::{Path as Route, State},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::time::Instant;

use crate::{
    BusinessTask, Catalog, Comparison, Engine, Predicate, QueryOptions, Source, TextColumn,
    TextTable,
};

use super::{Evidence, Finding, Parameters, TaskResult, execute};
use crate::admin::{
    ApiResult, App, Error,
    access::{self, Principal},
    models::{
        self, Models,
        versions::{Action, Activation, ImpactPreview},
    },
    users::{self, Role},
};

const MAX_CASES: usize = 16;
const MAX_CASE_ROWS: usize = 50;

pub(in crate::admin) fn suite_path(dir: &Path, model: &str) -> PathBuf {
    dir.join(format!(".tasks-{model}.json"))
}

pub(in crate::admin) fn managed_suite_path(dir: &Path, model: &str) -> PathBuf {
    dir.join(format!(".graphlake-{model}.tasks.json"))
}

pub(in crate::admin) fn suite_revision(models: &Models, model: &str) -> ApiResult<Option<String>> {
    Ok(read_suite(models, model)?.revision)
}

#[derive(Serialize)]
pub(in crate::admin) struct TaskSuite {
    revision: Option<String>,
    source: Option<&'static str>,
    content: Option<String>,
}

fn read_suite(models: &Models, model: &str) -> ApiResult<TaskSuite> {
    models.path(model)?;
    for (source, path) in [
        ("console", managed_suite_path(models.directory(), model)),
        ("deployment", suite_path(models.source_directory(), model)),
    ] {
        if let Some(content) = models::read_file(&path, super::super::CATALOG_LIMIT)? {
            return Ok(TaskSuite {
                revision: Some(models::revision(&content)),
                source: Some(source),
                content: Some(content),
            });
        }
    }
    Ok(TaskSuite {
        revision: None,
        source: None,
        content: None,
    })
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(in crate::admin) struct SaveTaskSuite {
    pub(in crate::admin) revision: Option<String>,
    pub(super) content: String,
}

pub(in crate::admin) fn check_suite_revision(
    models: &Models,
    model: &str,
    expected: Option<&str>,
) -> ApiResult<()> {
    if suite_revision(models, model)?.as_deref() == expected {
        Ok(())
    } else {
        Err(Error::new(
            axum::http::StatusCode::CONFLICT,
            "task_policy_changed",
            "固定业务场景已变化，请重新加载",
        ))
    }
}

pub(in crate::admin) fn save_suite(
    models: &Models,
    model: &str,
    request: SaveTaskSuite,
) -> ApiResult<models::Model> {
    let model_info = models.get(model)?;
    check_suite_revision(models, model, request.revision.as_deref())?;
    let suite = Suite::parse(&request.content)?;
    for case in &suite.cases {
        Parameters::new(case.task, case.parameters.clone()).map_err(|error| {
            Error::invalid(
                "invalid_task_suite",
                format!("场景 {} 的参数无效：{error}", case.id),
            )
        })?;
    }
    Fixture::new(suite.tables)
        .map_err(|_| Error::invalid("invalid_task_suite", "固定数据列定义或数据文件无效"))?;
    // Persist any implicit active version before the suite turns it into a governed model.
    models.adopt(model, &model_info)?;
    models::atomic_write(
        &managed_suite_path(models.directory(), model),
        request.content.as_bytes(),
        false,
    )?;
    models.get(model)
}

fn runner_revision() -> String {
    static REVISION: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
        let version = option_env!("GRAPHLAKE_BUILD_VERSION").unwrap_or(env!("CARGO_PKG_VERSION"));
        let source = concat!(
            include_str!("templates.rs"),
            include_str!("mod.rs"),
            include_str!("impact.rs"),
            include_str!("../../catalog/tasks.rs"),
            include_str!("../../engine.rs"),
            include_str!("../../engine/model.rs"),
            include_str!("../../cypher.rs"),
            include_str!("../../../Cargo.lock"),
        );
        models::revision(&format!("{version}\n{source}"))
    });
    REVISION.clone()
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub(in crate::admin) struct ImpactStamp {
    pub report_id: String,
    suite_revision: String,
    runner_revision: String,
}

impl ImpactStamp {
    pub(in crate::admin) fn current(&self, suite: Option<&str>) -> bool {
        suite == Some(self.suite_revision.as_str()) && self.runner_revision == runner_revision()
    }

    pub(in crate::admin) fn matches_suite(&self, suite: Option<&str>) -> bool {
        suite == Some(self.suite_revision.as_str())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Suite {
    tables: BTreeMap<String, Table>,
    cases: Vec<Case>,
}

impl Suite {
    fn parse(raw: &str) -> ApiResult<Self> {
        if raw.len() > super::super::CATALOG_LIMIT {
            return Err(Error::invalid("catalog_limit", "固定业务场景超过 1 MiB"));
        }
        let suite: Self = serde_json::from_str(raw).map_err(|error| {
            Error::invalid(
                "invalid_task_suite",
                format!("业务场景文件格式无效：{error}"),
            )
        })?;
        let tasks = suite.tasks();
        if suite.cases.is_empty()
            || suite.cases.len() > MAX_CASES
            || suite
                .cases
                .iter()
                .map(|case| &case.id)
                .collect::<BTreeSet<_>>()
                .len()
                != suite.cases.len()
            || !suite.cases.iter().all(|case| {
                models::valid_id(&case.id)
                    && !case.tenant_id.trim().is_empty()
                    && case.tenant_id.len() <= 256
                    && case.expected.findings.len() <= MAX_CASE_ROWS
            })
            || !tasks.iter().all(|task| {
                suite
                    .cases
                    .iter()
                    .any(|case| case.task == *task && !case.expected.findings.is_empty())
            })
        {
            return Err(Error::invalid(
                "invalid_task_suite",
                "业务场景需使用唯一标识，并为每个任务提供包含证据的正向用例",
            ));
        }
        Ok(suite)
    }

    fn tasks(&self) -> BTreeSet<BusinessTask> {
        self.cases.iter().map(|case| case.task).collect()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Table {
    columns: Vec<TextColumn>,
    rows: Vec<serde_json::Map<String, Value>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    task: BusinessTask,
    tenant_id: String,
    parameters: Value,
    expected: Expected,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Expected {
    subject: String,
    anchor: Option<Evidence>,
    findings: Vec<Finding>,
}

impl Expected {
    fn matches(&self, result: &TaskResult) -> bool {
        self.subject == result.subject
            && self.anchor == result.anchor
            && self.findings == result.findings
    }
}

struct Fixture {
    _dir: tempfile::TempDir,
    tables: BTreeMap<String, TextTable>,
}

impl Fixture {
    fn new(tables: BTreeMap<String, Table>) -> Result<Self> {
        let dir = tempfile::tempdir()?;
        let mut files = BTreeMap::new();
        for (index, (reference, table)) in tables.into_iter().enumerate() {
            let path = dir.path().join(format!("{index}.jsonl"));
            let mut file = BufWriter::new(std::fs::File::create(&path)?);
            for row in table.rows {
                serde_json::to_writer(&mut file, &row)?;
                file.write_all(b"\n")?;
            }
            file.flush()?;
            let table = TextTable {
                files: vec![path.to_string_lossy().into_owned()],
                columns: table.columns,
            };
            table.schema()?;
            files.insert(reference, table);
        }
        Ok(Self {
            _dir: dir,
            tables: files,
        })
    }

    fn bind(
        &self,
        mut catalog: Catalog,
        task: BusinessTask,
        admission: &Arc<crate::engine::Admission>,
    ) -> Result<Engine> {
        let labels = task
            .labels()
            .iter()
            .map(|label| label.name)
            .collect::<BTreeSet<_>>();
        catalog
            .nodes
            .retain(|name, _| labels.contains(name.as_str()));
        catalog
            .edges
            .retain(|name, _| labels.contains(name.as_str()));
        catalog.tasks.retain(|enabled| *enabled == task);
        let mut sources = BTreeMap::<String, BTreeMap<String, TextTable>>::new();
        let mut bind = |reference: &str| -> Result<()> {
            let (source, table) = reference
                .split_once('.')
                .context("invalid table reference")?;
            let fixture = self
                .tables
                .get(reference)
                .with_context(|| format!("fixture table missing: {reference}"))?;
            sources
                .entry(source.into())
                .or_default()
                .insert(table.into(), fixture.clone());
            Ok(())
        };
        for name in labels {
            let mapping = catalog.mapping(name);
            if mapping.inputs.is_empty() {
                bind(mapping.source)?;
            } else {
                for input in mapping.inputs {
                    bind(&input.table)?;
                }
            }
        }
        // Preserve the mappings and replace every source before building the fixture engine.
        catalog.sources = sources
            .into_iter()
            .map(|(alias, tables)| (alias, Source::Jsonl { tables }))
            .collect();
        catalog.export_locations.clear();
        Engine::with_admission(catalog, Arc::clone(admission))
    }
}

#[derive(Serialize)]
pub(in crate::admin) struct ImpactReport {
    status: &'static str,
    model: String,
    action: Action,
    revision: String,
    active_revision: Option<String>,
    suite_revision: Option<String>,
    runner_revision: String,
    cases: Vec<CaseResult>,
    mapping_changes: BTreeMap<String, Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    report_id: Option<String>,
}

impl ImpactReport {
    fn finish(mut self) -> ApiResult<Self> {
        let encoded = serde_json::to_string(&self).map_err(|_| Error::storage())?;
        if encoded.len() > 2 * super::super::CATALOG_LIMIT {
            return Err(Error::invalid(
                "impact_report_limit",
                "业务场景报告超出大小上限",
            ));
        }
        self.report_id = Some(models::revision(&encoded));
        Ok(self)
    }

    pub(in crate::admin) fn require_pass(
        self,
        preview: Option<&ImpactPreview>,
    ) -> ApiResult<Option<ImpactStamp>> {
        if let Some(preview) = preview
            && (preview.report_id != self.report_id
                || !matches!(self.status, "passed" | "not_required"))
        {
            return Err(Error::new(
                axum::http::StatusCode::CONFLICT,
                "impact_preview_changed",
                "业务影响与预览不一致，请重新预览",
            ));
        }
        match self.status {
            "not_required" => Ok(None),
            "passed" => Ok(Some(ImpactStamp {
                suite_revision: self.suite_revision.ok_or_else(Error::storage)?,
                runner_revision: self.runner_revision,
                report_id: self.report_id.ok_or_else(Error::storage)?,
            })),
            _ => {
                let issues = self
                    .cases
                    .iter()
                    .filter(|case| case.status != "passed")
                    .map(|case| format!("{} ({})", case.id, case.status))
                    .collect::<Vec<_>>()
                    .join(", ");
                Err(Error::invalid(
                    "business_impact_failed",
                    format!("业务场景未通过: {issues}"),
                ))
            }
        }
    }
}

#[derive(Serialize)]
struct CaseResult {
    id: String,
    task: BusinessTask,
    tenant_id: String,
    parameters: Value,
    status: &'static str,
    baseline_matches: Option<bool>,
    candidate_matches: Option<bool>,
    baseline_subject: Option<&'static str>,
    candidate_subject: Option<&'static str>,
    baseline_findings: Option<usize>,
    candidate_findings: Option<usize>,
    baseline_anchor: Option<Evidence>,
    candidate_anchor: Option<Evidence>,
    added: Vec<Finding>,
    removed: Vec<Finding>,
    changed: Vec<(Finding, Finding)>,
    #[serde(skip_serializing_if = "Option::is_none")]
    issue: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    expected_on_mismatch: Option<Expected>,
}

fn delta(before: Vec<Finding>, after: Vec<Finding>, result: &mut CaseResult) {
    // A stable subject and evidence record key distinguish distinct paths to the same resource.
    fn key(finding: &Finding) -> Vec<Option<String>> {
        let mut key = vec![Some(finding.kind.clone())];
        key.extend(
            finding
                .values
                .iter()
                .filter(|(name, _)| !name.ends_with("_ms") && name.as_str() != "outcome")
                .flat_map(|(name, value)| [Some(name.clone()), value.clone()]),
        );
        key.extend(finding.evidence.iter().flat_map(|item| {
            [
                Some(item.label.clone()),
                Some(item.tenant_id.clone()),
                item.source_system.clone(),
                item.record_id.clone(),
            ]
        }));
        key
    }
    let mut before_by_key = BTreeMap::<_, Vec<_>>::new();
    for row in before.into_iter().rev() {
        before_by_key.entry(key(&row)).or_default().push(row);
    }
    for row in after {
        let bucket = before_by_key.get_mut(&key(&row));
        let previous = bucket.and_then(|rows| {
            if let Some(index) = rows.iter().position(|existing| *existing == row) {
                Some(rows.remove(index))
            } else {
                rows.pop()
            }
        });
        if let Some(previous) = previous {
            if previous != row {
                result.changed.push((previous, row));
            }
        } else {
            result.added.push(row);
        }
    }
    result.removed.extend(before_by_key.into_values().flatten());
}

pub(in crate::admin) struct ImpactInput {
    report: ImpactReport,
    path: PathBuf,
    candidate: String,
    baseline: Option<String>,
    suite: Option<String>,
    protected: bool,
}

impl ImpactInput {
    pub(in crate::admin) fn new(
        models: &Models,
        model: &str,
        revision: &str,
        active_revision: Option<String>,
        candidate: String,
        action: Action,
    ) -> ApiResult<Self> {
        let path = models.source_path(model)?;
        let suite = read_suite(models, model)?;
        let baseline = active_revision
            .as_ref()
            .filter(|value| suite.content.is_some() && *value != revision)
            .map(|value| models.archived(model, value))
            .transpose()?;
        let protected = suite.content.is_none() && models.has_task_history(model)?;
        Ok(Self {
            report: ImpactReport {
                status: "not_required",
                model: model.into(),
                action,
                revision: revision.into(),
                active_revision,
                suite_revision: suite.revision,
                runner_revision: runner_revision(),
                cases: Vec::new(),
                mapping_changes: BTreeMap::new(),
                report_id: None,
            },
            path,
            candidate,
            baseline,
            suite: suite.content,
            protected,
        })
    }

    pub(in crate::admin) async fn run(
        mut self,
        mut options: QueryOptions,
        admission: &Arc<crate::engine::Admission>,
    ) -> ApiResult<ImpactReport> {
        options.memory_bytes = options.memory_bytes.min(64 * 1024 * 1024);
        options.spill_bytes = options.spill_bytes.min(64 * 1024 * 1024);
        let candidate = models::parse(&self.candidate, &self.path)?;
        let Some(raw) = self.suite else {
            if self.protected || !candidate.tasks.is_empty() {
                return Err(Error::invalid(
                    "missing_task_suite",
                    "启用业务任务的模型需要运维配置的固定场景",
                ));
            }
            return self.report.finish();
        };
        let suite = Suite::parse(&raw)?;
        let required = suite.tasks();
        if required != candidate.tasks {
            return Err(Error::invalid(
                "task_policy_mismatch",
                "候选模型的任务必须与运维配置的固定场景一致",
            ));
        }
        let tables = suite.tables;
        let fixture = tokio::task::spawn_blocking(move || Fixture::new(tables))
            .await
            .map_err(|_| Error::storage())?
            .map_err(|_| Error::invalid("invalid_task_suite", "固定数据列定义或数据文件无效"))?;
        let mut cases = suite.cases;
        let outcome = async {
            let before = self
                .baseline
                .as_deref()
                .map(|yaml| models::parse(yaml, &self.path))
                .transpose()?;
            let same_revision =
                self.report.active_revision.as_deref() == Some(&self.report.revision);
            if let Some(before) = &before {
                self.report.mapping_changes = mapping_changes(before, &candidate)?;
            }
            let start = Instant::now();
            let mut engines = BTreeMap::new();
            for task in required {
                let candidate = fixture
                    .bind(candidate.clone(), task, admission)
                    .map_err(CaseIssue::from);
                let baseline = before
                    .as_ref()
                    .filter(|catalog| catalog.tasks.contains(&task))
                    .map(|catalog| {
                        fixture
                            .bind(catalog.clone(), task, admission)
                            .map_err(CaseIssue::from)
                    });
                let equivalent = same_revision
                    || match (&baseline, &candidate) {
                        (Some(Ok(before)), Ok(after)) => same_execution_mapping(before, after)?,
                        _ => false,
                    };
                engines.insert(task, (baseline, candidate, equivalent));
            }
            self.report.status = "passed";
            for case in &mut cases {
                case.expected.findings.sort();
                let task = case.task;
                let mut result = CaseResult {
                    id: case.id.clone(),
                    task,
                    tenant_id: case.tenant_id.clone(),
                    parameters: case.parameters.clone(),
                    status: "passed",
                    baseline_matches: None,
                    candidate_matches: None,
                    baseline_subject: None,
                    candidate_subject: None,
                    baseline_findings: None,
                    candidate_findings: None,
                    baseline_anchor: None,
                    candidate_anchor: None,
                    added: Vec::new(),
                    removed: Vec::new(),
                    changed: Vec::new(),
                    issue: None,
                    expected_on_mismatch: None,
                };
                let (baseline, candidate, equivalent) = &engines[&task];
                let previous = match baseline {
                    Some(engine) if !equivalent => {
                        Some(run_case(engine, case, &options, start).await)
                    }
                    _ => None,
                };
                let proposed = run_case(candidate, case, &options, start).await;
                let previous = if *equivalent {
                    Some(&proposed)
                } else {
                    previous.as_ref()
                };
                if let Some(previous) = previous {
                    match previous {
                        Ok(output) => {
                            result.baseline_matches = Some(case.expected.matches(output));
                            result.baseline_subject = Some(output.subject);
                            result.baseline_findings = Some(output.findings.len());
                            result.baseline_anchor.clone_from(&output.anchor);
                        }
                        Err(issue) => issue.record(&mut result, "baseline"),
                    }
                }
                match &proposed {
                    Ok(output) => {
                        let matches = case.expected.matches(output);
                        result.candidate_matches = Some(matches);
                        result.candidate_subject = Some(output.subject);
                        result.candidate_findings = Some(output.findings.len());
                        result.candidate_anchor.clone_from(&output.anchor);
                        if !matches {
                            result.expected_on_mismatch = Some(case.expected.clone());
                            if result.status == "passed" {
                                result.status = "failed";
                            }
                        }
                    }
                    Err(issue) => issue.record(&mut result, "candidate"),
                }
                match result.status {
                    "passed" | "failed" => {
                        let before = previous
                            .and_then(|r| r.as_ref().ok())
                            .map_or(&[][..], |r| &r.findings);
                        let after = &proposed.as_ref().expect("complete candidate").findings;
                        if before != after {
                            delta(before.to_vec(), after.clone(), &mut result);
                        }
                        if result.status == "failed" && self.report.status == "passed" {
                            self.report.status = "failed";
                        }
                    }
                    _ => self.report.status = "inconclusive",
                }
                self.report.cases.push(result);
            }
            self.report.finish()
        }
        .await;
        let _ = tokio::task::spawn_blocking(move || drop(fixture)).await;
        outcome
    }
}

#[derive(Clone, Debug)]
struct CaseIssue {
    status: &'static str,
    reason: String,
}

impl From<anyhow::Error> for CaseIssue {
    fn from(error: anyhow::Error) -> Self {
        let message = error.to_string();
        if message.starts_with("fixture table missing:") {
            Self {
                status: "not_covered",
                reason: message,
            }
        } else {
            Self {
                status: "inconclusive",
                reason: super::task_error(error).message,
            }
        }
    }
}

impl CaseIssue {
    fn record(&self, result: &mut CaseResult, side: &str) {
        result.status = self.status;
        result
            .issue
            .get_or_insert_with(|| format!("{side}: {}", self.reason));
    }
}

async fn run_case(
    engine: &Result<Engine, CaseIssue>,
    case: &Case,
    options: &QueryOptions,
    start: Instant,
) -> Result<TaskResult, CaseIssue> {
    let engine = engine.as_ref().map_err(Clone::clone)?;
    let parameters = Parameters::new(case.task, case.parameters.clone())?;
    let mut options = options.clone();
    options.timeout = options
        .timeout
        .checked_sub(start.elapsed())
        .filter(|v| !v.is_zero())
        .context("suite deadline exceeded")?;
    let term = Predicate {
        column: "tenant_id".into(),
        op: Comparison::Eq,
        value: Value::String(case.tenant_id.clone()),
    };
    users::apply_partition(&engine.catalog, &mut options.graph, &term)
        .map_err(|e| anyhow::anyhow!(e.message))?;
    let output = execute(engine, case.task, parameters, options, MAX_CASE_ROWS).await?;
    if output.execution_status != "complete" {
        return Err(CaseIssue {
            status: "inconclusive",
            reason: "truncated result or incomplete evidence".into(),
        });
    }
    Ok(output)
}

fn same_execution_mapping(before: &Engine, after: &Engine) -> ApiResult<bool> {
    fn mapping(engine: &Engine) -> ApiResult<Vec<u8>> {
        let mut catalog = (*engine.catalog).clone();
        // The task result does not use the graph display name or model-level text.
        catalog.graph.clear();
        catalog.semantics = None;
        // Predicates convert numbers to text before typing them. JSON value
        // equality would collapse distinct literals such as -0.0 and 0.0.
        serde_json::to_vec(&catalog).map_err(|_| Error::storage())
    }
    Ok(mapping(before)? == mapping(after)?)
}

fn mapping_changes(before: &Catalog, after: &Catalog) -> ApiResult<BTreeMap<String, Vec<String>>> {
    let labels = before
        .tasks
        .iter()
        .chain(&after.tasks)
        .flat_map(|task| task.labels())
        .map(|label| label.name)
        .collect::<BTreeSet<_>>();
    let before = serde_json::to_value(before).map_err(|_| Error::storage())?;
    let after = serde_json::to_value(after).map_err(|_| Error::storage())?;
    let mut changes = BTreeMap::new();
    for section in ["nodes", "edges"] {
        for label in &labels {
            match (
                before[section][label].as_object(),
                after[section][label].as_object(),
            ) {
                (Some(a), Some(b)) => {
                    let fields = a
                        .keys()
                        .chain(b.keys())
                        .collect::<BTreeSet<_>>()
                        .into_iter()
                        .filter(|field| {
                            a.get(*field).map(Value::to_string)
                                != b.get(*field).map(Value::to_string)
                        })
                        .cloned()
                        .collect::<Vec<_>>();
                    if !fields.is_empty() {
                        changes.insert((*label).to_owned(), fields);
                    }
                }
                (Some(_), None) => {
                    changes.insert((*label).to_owned(), vec!["removed".into()]);
                }
                (None, Some(_)) => {
                    changes.insert((*label).to_owned(), vec!["added".into()]);
                }
                _ => {}
            }
        }
    }
    let fields = ["graph", "sources", "semantics", "tasks"]
        .into_iter()
        .filter(|field| before[*field] != after[*field])
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if !fields.is_empty() {
        changes.insert("model".into(), fields);
    }
    Ok(changes)
}

pub(in crate::admin) async fn http_impact(
    State(app): State<Arc<App>>,
    Route(model): Route<String>,
    Extension(principal): Extension<Principal>,
    Json(request): Json<Activation>,
) -> ApiResult<Json<ImpactReport>> {
    Ok(Json(
        validate_impact(&app, &principal, &model, request, app.options.timeout).await?,
    ))
}

pub(in crate::admin) async fn http_rollback_impact(
    State(app): State<Arc<App>>,
    Route(model): Route<String>,
    Extension(principal): Extension<Principal>,
    Json(request): Json<Activation>,
) -> ApiResult<Json<ImpactReport>> {
    users::check(&app, &principal, &model, Role::Publisher)?;
    let _permit = app.metadata_work.try_acquire().map_err(|_| Error::busy())?;
    let files = app.models.lock().unwrap().read_files();
    let model_id = model.clone();
    let (_, input) =
        tokio::task::spawn_blocking(move || files.rollback_impact(&model_id, &request))
            .await
            .map_err(|_| Error::storage())??;
    let result = input
        .run(
            impact_options(&app, app.options.timeout),
            &app.impact_admission,
        )
        .await?;
    users::check(&app, &principal, &model, Role::Publisher)?;
    Ok(Json(result))
}

#[derive(Serialize)]
pub(in crate::admin) struct BusinessStatus {
    required: bool,
    suite_revision: Option<String>,
    active: Option<&'static str>,
}

pub(in crate::admin) async fn http_business_status(
    State(app): State<Arc<App>>,
    Route(model): Route<String>,
    Extension(principal): Extension<Principal>,
) -> ApiResult<Json<BusinessStatus>> {
    check_preview_access(&app, &principal, &model)?;
    let files = app.models.lock().unwrap().read_files();
    let status = tokio::task::spawn_blocking(move || -> ApiResult<BusinessStatus> {
        let info = files.get(&model)?;
        let suite_revision = suite_revision(&files, &model)?;
        let required =
            files.requires_task_publish(&model, &info.yaml)? || files.has_task_history(&model)?;
        let active = match info.active_revision.as_deref() {
            Some(rev) if required => {
                let passed =
                    files.has_task_history(&model)? && files.task_stamp(&model, rev)?.is_some();
                Some(if passed { "passed" } else { "stale" })
            }
            _ => None,
        };
        Ok(BusinessStatus {
            required,
            suite_revision,
            active,
        })
    })
    .await
    .map_err(|_| Error::storage())??;
    Ok(Json(status))
}

fn check_preview_access(app: &App, principal: &Principal, model: &str) -> ApiResult<()> {
    users::check(app, principal, model, Role::Modeler)
        .or_else(|_| users::check(app, principal, model, Role::Publisher))
}

fn impact_options(app: &App, timeout: Duration) -> QueryOptions {
    let mut options = app.options.clone();
    options.timeout = options.timeout.min(timeout);
    if app.ha.is_some() {
        options.timeout = options.timeout.min(Duration::from_secs(8));
    }
    options
}

pub(in crate::admin) async fn validate_impact(
    app: &Arc<App>,
    principal: &Principal,
    model: &str,
    request: Activation,
    timeout: Duration,
) -> ApiResult<ImpactReport> {
    check_preview_access(app, principal, model)?;
    let _permit = app.metadata_work.try_acquire().map_err(|_| Error::busy())?;
    let files = app.models.lock().unwrap().read_files();
    let model_id = model.to_owned();
    let (_, input) = tokio::task::spawn_blocking(move || files.draft_impact(&model_id, &request))
        .await
        .map_err(|_| Error::storage())??;
    let report = input
        .run(impact_options(app, timeout), &app.impact_admission)
        .await?;
    check_preview_access(app, principal, model)?;
    Ok(report)
}

pub(in crate::admin) async fn http_read_suite(
    State(app): State<Arc<App>>,
    Route(model): Route<String>,
    Extension(principal): Extension<Principal>,
) -> ApiResult<Json<TaskSuite>> {
    if principal != Principal::Admin {
        return Err(access::forbidden());
    }
    let files = app.models.lock().unwrap().read_files();
    let suite = tokio::task::spawn_blocking(move || -> ApiResult<TaskSuite> {
        files.get(&model)?;
        read_suite(&files, &model)
    })
    .await
    .map_err(|_| Error::storage())??;
    Ok(Json(suite))
}

pub(in crate::admin) async fn http_save_suite(
    State(app): State<Arc<App>>,
    Route(model): Route<String>,
    Extension(principal): Extension<Principal>,
    headers: axum::http::HeaderMap,
    Json(request): Json<SaveTaskSuite>,
) -> ApiResult<axum::response::Response> {
    use axum::response::IntoResponse;
    if principal != Principal::Admin {
        return Err(access::forbidden());
    }
    if app.ha.is_some() {
        return super::super::ha::mutate(
            &app,
            model,
            &principal,
            &headers,
            super::super::ha::Mutation::SaveTaskSuite(request),
        )
        .await;
    }
    let _permit = app.metadata_work.try_acquire().map_err(|_| Error::busy())?;
    let _policy = app.users.writes.lock().await;
    let app_for_save = Arc::clone(&app);
    let info = tokio::task::spawn_blocking(move || {
        save_suite(&app_for_save.models.lock().unwrap(), &model, request)
    })
    .await
    .map_err(|_| Error::storage())??;
    Ok(models::model_response(info).into_response())
}
