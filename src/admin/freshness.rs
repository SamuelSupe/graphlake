use std::{collections::BTreeMap, sync::Arc};

use axum::http::StatusCode;
use datafusion::execution::memory_pool::UnboundedMemoryPool;
use serde::{Deserialize, Serialize};

use super::{ApiResult, App, Error, local_tables::store::now, models};
use crate::{Catalog, Source, SourceVersion, catalog::FreshnessPolicy, source};

#[derive(Serialize)]
pub(super) struct Observation {
    source: String,
    #[serde(skip)]
    location: String,
    generation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    result_generation: Option<String>,
    observed_at_ms: Option<u64>,
    observation_finished_at_ms: Option<u64>,
    reconciled_at_ms: Option<u64>,
    deletion_capture: &'static str,
    last_status: Option<String>,
}

#[derive(Clone, Deserialize, Serialize)]
pub(super) struct Proof {
    pub policy: FreshnessPolicy,
    pub generations: BTreeMap<String, Option<String>>,
}

impl Proof {
    pub(super) fn new(policy: FreshnessPolicy, inputs: &[Observation]) -> Option<Self> {
        (policy != FreshnessPolicy::default()).then(|| Self {
            policy,
            generations: generations(inputs),
        })
    }
    pub(super) fn verify(&self, versions: &[SourceVersion]) -> ApiResult<()> {
        if versions.iter().any(|v| {
            self.generations
                .get(&v.source)
                .is_some_and(|expected| expected.as_ref() != Some(&v.version))
        }) {
            return Err(conflict("本地副本在查询固定输入期间更新"));
        }
        Ok(())
    }
}

impl Observation {
    pub(super) fn used(&mut self, lookup: impl FnOnce(&str) -> Option<String>) {
        self.result_generation = lookup(&self.location);
    }
}

pub(super) fn generations(inputs: &[Observation]) -> BTreeMap<String, Option<String>> {
    inputs
        .iter()
        .map(|item| (item.location.clone(), item.generation.clone()))
        .collect()
}

pub(super) async fn observe(app: &App, catalog: &Catalog) -> Vec<Observation> {
    let definitions = {
        let store = app.local_tables.store.lock().unwrap();
        store
            .tables
            .values()
            .map(|t| (store.path(t), t.clone()))
            .collect::<BTreeMap<_, _>>()
    };
    let mut observations = Vec::new();
    for reference in models::physical_tables(catalog) {
        let Ok((_, relation, Source::Parquet { tables })) = catalog.source(&reference) else {
            continue;
        };
        for location in &tables[relation] {
            let (table, generation) =
                if let Some(id) = source::parquet_snapshot::logical_id(location) {
                    let current = match super::local_tables::managed::backend(app) {
                        Some(backend) => backend.inspect_record(id).await.ok().flatten(),
                        None => None,
                    };
                    match current {
                        Some((table, generation)) => (Some(table), generation),
                        None => (None, None),
                    }
                } else if let Some(path) = source::parquet_snapshot::managed_location(location) {
                    let version = source::parquet::object_version(
                        location,
                        None,
                        Arc::new(UnboundedMemoryPool::default()),
                        Arc::default(),
                    )
                    .await
                    .ok();
                    (definitions.get(&path).cloned(), version.map(|v| v.version))
                } else {
                    continue;
                };
            let table = table.as_ref();
            observations.push(Observation {
                source: reference.clone(),
                location: location.clone(),
                generation,
                result_generation: None,
                observed_at_ms: table.and_then(|t| t.observed_at_ms),
                observation_finished_at_ms: table.and_then(|t| t.observation_finished_at_ms),
                reconciled_at_ms: table.and_then(|t| t.reconciled_at_ms),
                deletion_capture: table.map_or("unknown", |t| {
                    if t.config.deleted_column.is_some() {
                        "marker_and_reconciliation"
                    } else {
                        "reconciliation"
                    }
                }),
                last_status: table
                    .and_then(|t| t.history.first())
                    .map(|r| r.status.clone()),
            });
        }
    }
    observations
}

pub(super) fn policy(catalog: &Catalog, request: Option<&FreshnessPolicy>) -> FreshnessPolicy {
    let base = &catalog.freshness.policy;
    let Some(req) = request else {
        return base.clone();
    };
    let min = |x: Option<u64>, y: Option<u64>| x.into_iter().chain(y).min();
    FreshnessPolicy {
        max_observation_age_seconds: min(
            base.max_observation_age_seconds,
            req.max_observation_age_seconds,
        ),
        max_reconciliation_age_seconds: min(
            base.max_reconciliation_age_seconds,
            req.max_reconciliation_age_seconds,
        ),
        require_current_csr: base.require_current_csr || req.require_current_csr,
        require_consistent_snapshot: base.require_consistent_snapshot
            || req.require_consistent_snapshot,
    }
}

pub(super) fn conflict(message: &str) -> Error {
    Error::new(StatusCode::CONFLICT, "freshness_required", message)
}

pub(super) fn check(policy: &FreshnessPolicy, inputs: &[Observation], csr: bool) -> ApiResult<()> {
    if policy.require_current_csr && !csr {
        return Err(conflict("查询需要已追平当前本地代次的 CSR"));
    }
    if policy.max_observation_age_seconds.is_some()
        || policy.max_reconciliation_age_seconds.is_some()
    {
        if inputs.is_empty() {
            return Err(conflict("模型没有可验证观察时间和对账时间的本地副本"));
        }
        let outside = |limit: Option<u64>, at: Option<u64>| {
            limit.is_some_and(|limit| {
                at.is_none_or(|at| at > now() || now().saturating_sub(at) > limit * 1000)
            })
        };
        for table in inputs {
            if table.generation.is_none()
                || outside(policy.max_observation_age_seconds, table.observed_at_ms)
            {
                return Err(conflict("本地副本观察时间缺失或已超限"));
            }
            if outside(
                policy.max_reconciliation_age_seconds,
                table.reconciled_at_ms,
            ) {
                return Err(conflict("本地副本全量对账时间缺失或已超限"));
            }
        }
    }
    Ok(())
}

pub(super) fn versions_match(
    inputs: &[Observation],
    versions: &[SourceVersion],
    all: bool,
) -> bool {
    inputs.iter().all(
        |item| match versions.iter().find(|v| v.source == item.location) {
            Some(v) => item.generation.as_ref() == Some(&v.version),
            None => !all,
        },
    )
}

pub(super) fn generations_match(before: &[Observation], after: &[Observation]) -> bool {
    before.len() == after.len()
        && before.iter().zip(after).all(|(a, b)| {
            a.location == b.location && a.generation.is_some() && a.generation == b.generation
        })
}

pub(super) async fn recheck(
    app: &App,
    catalog: &Catalog,
    model: &str,
    revision: &str,
    policy: &FreshnessPolicy,
    before: &[Observation],
    csr: bool,
) -> ApiResult<()> {
    if policy == &FreshnessPolicy::default() {
        return Ok(());
    }
    let current = observe(app, catalog).await;
    check(policy, &current, csr)?;
    if !generations_match(before, &current)
        || app.published_model_revision(model).await? != revision
    {
        return Err(conflict("查询期间模型或本地副本代次已更新"));
    }
    Ok(())
}
