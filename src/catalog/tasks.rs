use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};

use super::{Catalog, ValidationSite, semantics::RelationKind};

/// Built-in investigations enabled by a graph model.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BusinessTask {
    OffboardedAccountExposure,
    AlertIdentityDeviceContext,
}

impl BusinessTask {
    pub const fn id(self) -> &'static str {
        match self {
            Self::OffboardedAccountExposure => "offboarded_account_exposure",
            Self::AlertIdentityDeviceContext => "alert_identity_device_context",
        }
    }

    pub(crate) fn labels(self) -> &'static [&'static Label] {
        match self {
            Self::OffboardedAccountExposure => &[
                &PERSON,
                &ACCOUNT,
                &RESOURCE,
                &ROLE,
                &IDENTIFIED,
                &GRANTED,
                &MEMBER,
                &ROLE_GRANT,
                &ACCESSED,
            ],
            Self::AlertIdentityDeviceContext => &[
                &ALERT,
                &ACCOUNT,
                &PERSON,
                &DEVICE,
                &IP,
                &IDENTIFIED,
                &ALERT_ACCOUNT,
                &ALERT_DEVICE,
                &USED_DEVICE,
                &USED_IP,
            ],
        }
    }
}

pub(crate) struct Label {
    pub name: &'static str,
    key: &'static [&'static str],
    text: &'static [&'static str],
    time: &'static [&'static str],
    edge: Option<(&'static Label, &'static Label, RelationKind)>,
    evidence: bool,
    grant_interval: bool,
}

const PERSON: Label = Label::node(
    "Person",
    &["tenant_id", "namespace", "person_id"],
    &[],
    &["departed_at_ms"],
    true,
);
const ACCOUNT: Label = Label::node("Account", &["tenant_id", "account_id"], &[], &[], false);
const RESOURCE: Label = Label::node("Resource", &["tenant_id", "resource_id"], &[], &[], false);
const ROLE: Label = Label::node("Role", &["tenant_id", "role_id"], &[], &[], false);
const ALERT: Label = Label::node(
    "Alert",
    &["tenant_id", "alert_id"],
    &[],
    &["happened_at_ms"],
    true,
);
const DEVICE: Label = Label::node("Device", &["tenant_id", "device_id"], &[], &[], false);
const IP: Label = Label::node("IP", &["tenant_id", "address"], &[], &[], false);
const INTERVAL: &[&str] = &["valid_from_ms", "valid_to_ms"];
const EVENT: &[&str] = &["happened_at_ms"];
const IDENTIFIED: Label = Label::edge(
    "IDENTIFIED_AS",
    &ACCOUNT,
    &PERSON,
    RelationKind::IdentityLink,
    &[],
    INTERVAL,
    false,
);
const GRANTED: Label = Label::edge(
    "GRANTED",
    &ACCOUNT,
    &RESOURCE,
    RelationKind::Entitlement,
    &["action"],
    INTERVAL,
    true,
);
const MEMBER: Label = Label::edge(
    "MEMBER_OF",
    &ACCOUNT,
    &ROLE,
    RelationKind::Entitlement,
    &[],
    INTERVAL,
    true,
);
const ROLE_GRANT: Label = Label::edge(
    "ROLE_GRANT",
    &ROLE,
    &RESOURCE,
    RelationKind::Entitlement,
    &["action"],
    INTERVAL,
    true,
);
const ACCESSED: Label = Label::edge(
    "ACCESSED",
    &ACCOUNT,
    &RESOURCE,
    RelationKind::ObservedUse,
    &["action", "outcome"],
    EVENT,
    false,
);
const ALERT_ACCOUNT: Label = Label::edge(
    "ALERT_ACCOUNT",
    &ALERT,
    &ACCOUNT,
    RelationKind::ObservedUse,
    &[],
    &[],
    false,
);
const ALERT_DEVICE: Label = Label::edge(
    "ALERT_DEVICE",
    &ALERT,
    &DEVICE,
    RelationKind::ObservedUse,
    &[],
    &[],
    false,
);
const USED_DEVICE: Label = Label::edge(
    "USED_DEVICE",
    &ACCOUNT,
    &DEVICE,
    RelationKind::Correlation,
    &[],
    EVENT,
    false,
);
const USED_IP: Label = Label::edge(
    "USED_IP",
    &ACCOUNT,
    &IP,
    RelationKind::Correlation,
    &[],
    EVENT,
    false,
);

impl Label {
    const fn node(
        name: &'static str,
        key: &'static [&'static str],
        text: &'static [&'static str],
        time: &'static [&'static str],
        evidence: bool,
    ) -> Self {
        Self {
            name,
            key,
            text,
            time,
            edge: None,
            evidence,
            grant_interval: false,
        }
    }

    const fn edge(
        name: &'static str,
        from: &'static Label,
        to: &'static Label,
        kind: RelationKind,
        text: &'static [&'static str],
        time: &'static [&'static str],
        grant_interval: bool,
    ) -> Self {
        Self {
            name,
            key: &["tenant_id", "record_id"],
            text,
            time,
            edge: Some((from, to, kind)),
            evidence: true,
            grant_interval,
        }
    }

    fn text_fields(&self) -> impl Iterator<Item = &'static str> + '_ {
        self.key.iter().chain(self.text).copied().chain(
            self.evidence
                .then_some(["record_id", "source_system"])
                .into_iter()
                .flatten(),
        )
    }

    fn time_fields(&self) -> impl Iterator<Item = &'static str> + '_ {
        self.time
            .iter()
            .copied()
            .chain(self.evidence.then_some("observed_at_ms"))
    }

    pub(super) fn validate(&self, catalog: &Catalog) -> Result<()> {
        let (key, properties, semantics, scd2, unpartitioned) =
            if let Some((from, to, kind)) = self.edge {
                let edge = catalog
                    .edges
                    .get(self.name)
                    .context("required task edge is missing")?;
                ensure!(
                    edge.from.node == from.name
                        && edge.to.node == to.name
                        && edge
                            .from
                            .columns
                            .first()
                            .is_some_and(|key| key == "tenant_id")
                        && edge
                            .to
                            .columns
                            .first()
                            .is_some_and(|key| key == "tenant_id"),
                    "task edge must map {} to {} within tenant_id",
                    from.name,
                    to.name
                );
                ensure!(
                    edge.semantics
                        .as_ref()
                        .and_then(|s| s.relation.as_ref())
                        .is_some_and(|r| r.kind == kind),
                    "task edge requires relation kind {kind:?}"
                );
                (
                    &edge.key,
                    &edge.properties,
                    edge.semantics.as_ref(),
                    edge.scd2.as_ref(),
                    edge.unpartitioned,
                )
            } else {
                let node = catalog
                    .nodes
                    .get(self.name)
                    .context("required task node is missing")?;
                (
                    &node.key,
                    &node.properties,
                    node.semantics.as_ref(),
                    node.scd2.as_ref(),
                    node.unpartitioned,
                )
            };
        ensure!(
            !unpartitioned && key == self.key,
            "task requires business key {:?}",
            self.key
        );
        for field in self.text_fields().chain(self.time_fields()) {
            ensure!(
                key.iter().chain(properties).any(|name| name == field),
                "task requires exposed field {field}"
            );
        }
        for field in self.time_fields() {
            let unit = semantics
                .and_then(|s| s.properties.get(field))
                .and_then(|property| property.unit.as_deref());
            ensure!(
                unit.is_none_or(|unit| unit == "ms")
                    && (field != "observed_at_ms" || unit == Some("ms")),
                "task time field {field} must use epoch milliseconds (unit ms)"
            );
        }
        if let Some(scd2) = scd2 {
            ensure!(
                self.grant_interval
                    && scd2.valid_from.as_deref() == Some("valid_from_ms")
                    && scd2.valid_to.as_deref() == Some("valid_to_ms")
                    && scd2.open_end.is_none(),
                "task requires unfiltered history or an SCD2 grant interval with a null open end"
            );
        }
        if self.evidence {
            let evidence = semantics
                .and_then(|s| s.evidence.as_ref())
                .context("task requires evidence metadata")?;
            ensure!(
                evidence.source_system == "source_system"
                    && evidence.observed_at == "observed_at_ms"
                    && evidence.record_key == ["tenant_id", "record_id"]
                    && evidence
                        .source_version
                        .as_deref()
                        .is_none_or(|name| name == "source_version"),
                "task evidence requires source_system, [tenant_id, record_id], observed_at_ms and optional source_version"
            );
        }
        Ok(())
    }

    #[cfg(feature = "admin")]
    pub(crate) fn validate_types(
        &self,
        catalog: &Catalog,
        schema: &crate::arrow::datatypes::Schema,
    ) -> Result<()> {
        use crate::arrow::datatypes::DataType;
        for field in self
            .text_fields()
            .chain(self.has_source_version(catalog).then_some("source_version"))
        {
            let ty = schema.field_with_name(field)?.data_type();
            ensure!(
                matches!(
                    ty,
                    DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8
                ),
                "task field {field} requires a string type, got {ty}"
            );
        }
        for field in self.time_fields() {
            let ty = schema.field_with_name(field)?.data_type();
            ensure!(
                *ty == DataType::Int64,
                "task field {field} requires Int64 epoch milliseconds, got {ty}"
            );
        }
        Ok(())
    }

    #[cfg(feature = "admin")]
    pub(crate) fn has_source_version(&self, catalog: &Catalog) -> bool {
        let semantics = if self.edge.is_some() {
            catalog.edges[self.name].semantics.as_ref()
        } else {
            catalog.nodes[self.name].semantics.as_ref()
        };
        semantics
            .and_then(|s| s.evidence.as_ref())
            .is_some_and(|e| e.source_version.is_some())
    }
}

impl Catalog {
    pub(super) fn validate_tasks(&self) -> Result<()> {
        for task in &self.tasks {
            for label in task.labels() {
                label
                    .validate(self)
                    .with_context(|| ValidationSite::field(label.name, "tasks", None))?;
            }
        }
        Ok(())
    }

    #[cfg(feature = "admin")]
    pub(crate) fn task_has_source_version(&self, task: BusinessTask, label: &str) -> bool {
        task.labels()
            .iter()
            .any(|item| item.name == label && item.has_source_version(self))
    }
}
