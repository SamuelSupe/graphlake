mod archive;
mod files;
mod gc;
mod metadata;
mod sync;
pub(super) use sync::WorkspaceVersion;

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use anyhow::{Context, Result};
use uuid::Uuid;

use super::store::Table;
use crate::{
    Catalog, Engine, Source,
    admin::{
        ApiResult, App, Error,
        cluster::store::{Metadata, Objects},
        models,
    },
    source::parquet_snapshot::{self, RemoteSnapshot},
};

pub(in crate::admin) use archive::Archive;
pub(in crate::admin) use gc::watch;
pub(in crate::admin) use metadata::Pin;
use metadata::{Head, Record};

#[derive(Clone)]
pub(in crate::admin) struct Managed {
    meta: Metadata,
    objects: Objects,
    retention_ms: u64,
    grace_ms: u64,
}

impl Managed {
    pub fn new(
        meta: Metadata,
        objects: Objects,
        retention_seconds: u64,
        grace_seconds: u64,
    ) -> Self {
        Self {
            meta,
            objects,
            retention_ms: retention_seconds * 1000,
            grace_ms: grace_seconds * 1000,
        }
    }

    pub(in crate::admin) fn reader(meta: Metadata, objects: Objects) -> Self {
        Self::new(meta, objects, 86_400, 300)
    }

    pub(in crate::admin) async fn snapshots(
        &self,
        pins: &[Pin],
    ) -> Result<BTreeMap<Uuid, RemoteSnapshot>> {
        let mut snapshots = BTreeMap::new();
        for pin in pins {
            self.check_pin(pin).await?;
            snapshots.insert(pin.table, self.snapshot(pin.table, &pin.head).await?);
        }
        Ok(snapshots)
    }

    pub(in crate::admin) async fn pin(&self, ids: BTreeSet<Uuid>, ttl: u64) -> Result<Selection> {
        let mut selected = Selection {
            managed: self.clone(),
            lease: None,
            tables: Vec::new(),
        };
        if ids.is_empty() {
            return Ok(selected);
        }
        selected.lease = Some(self.grant(ttl).await?);
        let ticket = Uuid::new_v4();
        for id in ids {
            selected.tables.push(
                self.pin_current(id, ticket, selected.lease.as_deref().unwrap())
                    .await?,
            );
        }
        Ok(selected)
    }

    pub(super) async fn by_name(&self, model: &str, name: &str) -> Result<Option<Uuid>> {
        Ok(self
            .records()
            .await?
            .into_iter()
            .find(|(r, _)| r.table.model == model && r.table.config.name == name)
            .map(|(r, _)| r.table.data_id))
    }

    pub(in crate::admin) async fn pin_model(
        &self,
        ids: BTreeSet<Uuid>,
        model: &str,
        ttl: u64,
    ) -> ApiResult<Selection> {
        let selected = self.pin(ids, ttl).await.map_err(error)?;
        if selected
            .tables
            .iter()
            .any(|(table, _)| table.model != model)
        {
            return Err(crate::admin::access::forbidden());
        }
        Ok(selected)
    }

    pub(in crate::admin) async fn inspect_record(
        &self,
        id: Uuid,
    ) -> Result<Option<(Table, Option<String>)>> {
        Ok(self
            .record(id)
            .await?
            .map(|(r, _)| (r.table, r.head.map(|h| h.generation.to_string()))))
    }
}

pub(in crate::admin) struct Selection {
    managed: Managed,
    lease: Option<String>,
    pub tables: Vec<(Table, Option<Pin>)>,
}

impl Selection {
    pub fn pins(&self) -> Result<Vec<Pin>> {
        self.tables
            .iter()
            .map(|(table, pin)| {
                pin.clone().with_context(|| {
                    format!("managed local table {} is not loaded", table.config.name)
                })
            })
            .collect()
    }

    // Dispatch and its retries own the lease until etcd's TTL expires.
    pub fn into_dispatch(mut self) -> Result<Vec<Pin>> {
        let pins = self.pins()?;
        self.lease.take();
        Ok(pins)
    }
}

impl Drop for Selection {
    fn drop(&mut self) {
        if let Some(lease) = self.lease.take()
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            let managed = self.managed.clone();
            runtime.spawn(async move {
                managed.release(&lease).await;
            });
        }
    }
}

pub(in crate::admin) fn referenced(catalog: &Catalog) -> BTreeSet<Uuid> {
    let mut ids = BTreeSet::new();
    for reference in models::physical_tables(catalog) {
        if let Ok((_, relation, Source::Parquet { tables })) = catalog.source(&reference) {
            ids.extend(
                tables[relation]
                    .iter()
                    .filter_map(|location| parquet_snapshot::logical_id(location)),
            );
        }
    }
    ids
}

pub(in crate::admin) fn backend(app: &App) -> Option<&Managed> {
    app.cluster
        .as_ref()
        .and_then(|cluster| cluster.managed.as_ref())
}

pub(in crate::admin) async fn resolve(
    app: &App,
    engine: &mut Engine,
    model: &str,
    ttl: u64,
) -> ApiResult<Option<Selection>> {
    let ids = referenced(&engine.catalog);
    if ids.is_empty() {
        return Ok(None);
    }
    let managed = backend(app).ok_or_else(unavailable)?;
    let selected = managed.pin_model(ids, model, ttl).await?;
    engine.managed_parquet = Arc::new(
        managed
            .snapshots(&selected.pins().map_err(error)?)
            .await
            .map_err(error)?,
    );
    Ok(Some(selected))
}

pub(super) fn api_error(e: Error) -> anyhow::Error {
    anyhow::anyhow!(e.message)
}

pub(in crate::admin) fn unavailable() -> Error {
    Error::invalid(
        "managed_local_tables_required",
        "托管本地表需要启用 S3 和 Coordinator 故障接管",
    )
}

pub(in crate::admin) fn error(error: anyhow::Error) -> Error {
    eprintln!("GraphLake managed local table operation failed: {error}");
    Error::new(
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        "managed_local_table_unavailable",
        "托管本地表状态或固定代次暂不可用",
    )
}
