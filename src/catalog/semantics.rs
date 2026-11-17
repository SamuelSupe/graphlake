use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};

use super::ValidationSite;

/// Business context for consumers of a graph. These declarations do not alter query execution.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModelSemantics {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub limitations: Vec<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LabelSemantics {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub properties: BTreeMap<String, PropertySemantics>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub relation: Option<RelationSemantics>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temporal: Option<TemporalSemantics>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub evidence: Option<EvidenceSemantics>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PropertySemantics {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unit: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub value_meanings: BTreeMap<String, String>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RelationKind {
    #[default]
    Unspecified,
    IdentityLink,
    Correlation,
    Entitlement,
    ObservedUse,
    Custom,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RelationSemantics {
    #[serde(default)]
    pub kind: RelationKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub statement: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub does_not_imply: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TemporalSemantics {
    pub description: String,
}

/// Logical fields that allow a consumer to locate a source record and interpret its observation time.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EvidenceSemantics {
    pub source_system: String,
    pub record_key: Vec<String>,
    pub observed_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_version: Option<String>,
}

impl LabelSemantics {
    pub(super) fn validate(
        &self,
        label: &str,
        key: &[String],
        properties: &[String],
        scd2: bool,
        edge: bool,
    ) -> Result<()> {
        self.validate_references(key, properties, scd2, edge)
            .map_err(|error| error.context(ValidationSite::field(label, "semantics", None)))
    }

    fn validate_references(
        &self,
        key: &[String],
        properties: &[String],
        scd2: bool,
        edge: bool,
    ) -> Result<()> {
        for (name, property) in &self.properties {
            ensure!(
                key.contains(name) || properties.contains(name),
                "semantics.properties references non-exposed field {name:?}"
            );
            if let Some(unit) = &property.unit {
                ensure!(
                    !unit.trim().is_empty(),
                    "semantics unit for {name:?} is empty"
                );
            }
        }
        if let Some(relation) = &self.relation {
            ensure!(edge, "semantics.relation is only supported on edges");
            ensure!(
                relation.kind != RelationKind::Custom
                    || relation
                        .statement
                        .as_ref()
                        .is_some_and(|s| !s.trim().is_empty()),
                "semantics.relation.statement is required for a custom relation"
            );
        }
        if let Some(temporal) = &self.temporal {
            ensure!(scd2, "semantics.temporal requires an SCD2 mapping");
            ensure!(
                !temporal.description.trim().is_empty(),
                "semantics.temporal.description is empty"
            );
        }
        if let Some(evidence) = &self.evidence {
            ensure!(
                !evidence.record_key.is_empty()
                    && evidence.record_key.iter().collect::<BTreeSet<_>>().len()
                        == evidence.record_key.len(),
                "semantics.evidence.record_key must contain distinct fields"
            );
            for name in evidence
                .record_key
                .iter()
                .chain([&evidence.source_system, &evidence.observed_at])
                .chain(&evidence.source_version)
            {
                ensure!(
                    key.contains(name) || properties.contains(name),
                    "semantics.evidence references non-exposed field {name:?}"
                );
            }
        }
        Ok(())
    }

    #[cfg(feature = "admin")]
    pub(crate) fn validate_types(&self, schema: &crate::arrow::datatypes::Schema) -> Result<()> {
        use crate::arrow::datatypes::DataType;

        for (name, property) in &self.properties {
            if property.unit.is_some() {
                let data_type = schema.field_with_name(name)?.data_type();
                ensure!(
                    data_type.is_numeric() || matches!(data_type, DataType::Duration(_)),
                    "semantics unit for {name:?} requires a numeric or duration field, got {data_type}"
                );
            }
        }
        if let Some(evidence) = &self.evidence {
            let data_type = schema.field_with_name(&evidence.observed_at)?.data_type();
            let unit = self
                .properties
                .get(&evidence.observed_at)
                .and_then(|p| p.unit.as_deref());
            ensure!(
                matches!(
                    data_type,
                    DataType::Timestamp(..) | DataType::Date32 | DataType::Date64
                ) || data_type.is_integer() && matches!(unit, Some("s" | "ms" | "us" | "ns")),
                "semantics.evidence.observed_at field {:?} must be a date, timestamp or integer epoch with unit s, ms, us or ns, got {data_type}",
                evidence.observed_at
            );
        }
        Ok(())
    }
}
