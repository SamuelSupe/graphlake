use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};

pub(crate) const ROW_ID: &str = "__graphlake_edge_id";

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum ValidationSite {
    Table {
        table: String,
        column: Option<String>,
    },
    Column {
        column: String,
    },
    Field {
        label: String,
        field: &'static str,
        index: Option<usize>,
    },
    InputColumn {
        label: String,
        input: usize,
        column: String,
    },
}

impl ValidationSite {
    pub(crate) fn field(label: &str, field: &'static str, index: Option<usize>) -> Self {
        Self::Field {
            label: label.into(),
            field,
            index,
        }
    }

    pub(crate) fn for_mapping(error: anyhow::Error, label: &str) -> anyhow::Error {
        if error.downcast_ref::<Self>().is_some() {
            error
        } else {
            error.context(Self::field(label, "mapping", None))
        }
    }
}

#[derive(Debug)]
pub(crate) struct MappedTable(pub String);

impl std::fmt::Display for MappedTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "reading mapped table {}", self.0)
    }
}

impl std::fmt::Display for ValidationSite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Table { table, .. } => write!(f, "reading mapped table {table}"),
            Self::Column { column } => write!(f, "missing column {column}"),
            Self::Field { label, .. } | Self::InputColumn { label, .. } => {
                write!(f, "binding graph label {label}")
            }
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Catalog {
    pub graph: String,
    pub sources: BTreeMap<String, Source>,
    pub nodes: BTreeMap<String, Node>,
    #[serde(default)]
    pub edges: BTreeMap<String, Edge>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub export_locations: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Freshness::is_default")]
    pub freshness: Freshness,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Freshness {
    #[serde(default)]
    pub auto_refresh_csr: bool,
    #[serde(default)]
    pub policy: FreshnessPolicy,
}

impl Freshness {
    fn is_default(&self) -> bool {
        self == &Self::default()
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct FreshnessPolicy {
    pub max_observation_age_seconds: Option<u64>,
    pub max_reconciliation_age_seconds: Option<u64>,
    pub require_current_csr: bool,
    pub require_consistent_snapshot: bool,
}

impl FreshnessPolicy {
    pub(crate) fn validate(&self) -> Result<()> {
        for age in [
            self.max_observation_age_seconds,
            self.max_reconciliation_age_seconds,
        ]
        .into_iter()
        .flatten()
        {
            ensure!(
                (1..=2_592_000).contains(&age),
                "freshness age must be 1–2592000 seconds"
            );
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Source {
    Sqlite {
        path: String,
    },
    Postgres {
        dsn_env: String,
    },
    Duckdb {
        path: String,
    },
    Doris(MysqlConnection),
    Starrocks(MysqlConnection),
    Tidb(MysqlConnection),
    Oceanbase(MysqlConnection),
    Mariadb(MysqlConnection),
    SqlserverNative {
        host: String,
        port: u16,
        database: String,
        user: String,
        password_env: String,
        #[serde(default = "default_tls")]
        tls: bool,
        ca_file: Option<String>,
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        truncate_datetime2_to_microseconds: bool,
    },
    OracleNative {
        connect_string: String,
        user: String,
        password_env: String,
    },
    MongodbNative {
        dsn_env: String,
        #[serde(default = "default_schema_sample_size")]
        schema_sample_size: u32,
    },
    Databricks {
        uri: String,
        token_env: String,
        warehouse: String,
        catalog: String,
    },
    ObjectStorage {
        storage: StorageConfig,
        format: ObjectFormat,
        tables: BTreeMap<String, TextTable>,
    },

    Mysql {
        dsn_env: String,
        #[serde(default = "default_tls")]
        tls: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        ca_file: Option<String>,
    },
    Snowflake {
        uri: String,
        token_env: String,
        warehouse: String,
        role: Option<String>,
        as_of: Option<String>,
    },
    Bigquery {
        uri: Option<String>,
        token_env: String,
        billing_project: String,
        location: String,
        maximum_bytes_billed: u64,
        as_of: Option<String>,
    },
    Clickhouse {
        uri: String,
        user: String,
        password_env: Option<String>,
    },
    Trino {
        uri: String,
        user: String,
        token_env: Option<String>,
        password_env: Option<String>,
    },
    Mongodb(TrinoCatalog),
    Elasticsearch(TrinoCatalog),
    Redshift(TrinoCatalog),
    Sqlserver(TrinoCatalog),
    Oracle(TrinoCatalog),
    Spanner {
        database: String,
        uri: Option<String>,
    },
    SapHana {
        host: String,
        port: u16,
        user: String,
        password_env: String,
        #[serde(default = "default_tls")]
        tls: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        ca_file: Option<String>,
    },
    Parquet {
        tables: BTreeMap<String, Vec<String>>,
    },
    Csv {
        tables: BTreeMap<String, TextTable>,
    },
    Jsonl {
        tables: BTreeMap<String, TextTable>,
    },
    Delta {
        tables: BTreeMap<String, DeltaTable>,
    },
    Iceberg {
        uri: String,
        warehouse: Option<String>,
        token_env: Option<String>,
        #[serde(default)]
        snapshots: BTreeMap<String, i64>,
    },
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MysqlConnection {
    pub dsn_env: String,
    #[serde(default = "default_tls")]
    pub tls: bool,
    pub ca_file: Option<String>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectFormat {
    Parquet,
    Csv,
    Jsonl,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    pub provider: String,
    pub bucket: String,
    pub endpoint: Option<String>,
    pub region: Option<String>,
    pub access_key_env: Option<String>,
    pub secret_key_env: Option<String>,
    pub session_token_env: Option<String>,
    pub account: Option<String>,
    pub account_key_env: Option<String>,
    pub service_account_env: Option<String>,
}

fn default_schema_sample_size() -> u32 {
    1000
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TrinoCatalog {
    pub uri: String,
    pub catalog: String,
    pub user: String,
    pub token_env: Option<String>,
    pub password_env: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeltaTable {
    pub path: String,
    pub version: Option<u64>,
}

/// An explicit file list with an ordered schema; CSV headers must match this order.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TextTable {
    pub files: Vec<String>,
    #[serde(default)]
    pub columns: Vec<TextColumn>,
}

/// A text-file column using Arrow type notation, for example `Int64` or `Utf8`.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TextColumn {
    pub name: String,
    pub data_type: String,
    #[serde(default = "default_nullable")]
    pub nullable: bool,
}

fn default_nullable() -> bool {
    true
}

impl TextTable {
    pub(crate) fn schema(&self) -> Result<datafusion::arrow::datatypes::SchemaRef> {
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        ensure!(
            !self.files.is_empty() && self.files.iter().all(|file| !file.trim().is_empty()),
            "text table needs an explicit file list"
        );
        ensure!(
            !self.columns.is_empty(),
            "text table needs an explicit column schema"
        );
        let mut names = BTreeSet::new();
        let fields = self
            .columns
            .iter()
            .map(|column| {
                validate_name(&column.name)?;
                ensure!(
                    names.insert(&column.name),
                    "duplicate text column {}",
                    column.name
                );
                let kind: DataType = column
                    .data_type
                    .parse()
                    .context("invalid text column Arrow type")?;
                ensure!(
                    matches!(
                        kind,
                        DataType::Boolean
                            | DataType::Int8
                            | DataType::Int16
                            | DataType::Int32
                            | DataType::Int64
                            | DataType::UInt8
                            | DataType::UInt16
                            | DataType::UInt32
                            | DataType::UInt64
                            | DataType::Float32
                            | DataType::Float64
                            | DataType::Utf8
                            | DataType::Date32
                            | DataType::Timestamp(_, _)
                            | DataType::Decimal128(_, _)
                    ),
                    "unsupported text column type {}",
                    column.data_type
                );
                if let DataType::Decimal128(precision, scale) = kind {
                    ensure!(
                        precision > 0 && precision <= 38 && scale >= 0 && scale as u8 <= precision,
                        "text decimal requires 0 <= scale <= precision <= 38"
                    );
                }
                Ok(Field::new(&column.name, kind, column.nullable))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(std::sync::Arc::new(Schema::new(fields)))
    }
}

fn default_tls() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Node {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub source: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inputs: Vec<GraphInput>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scd2: Option<Scd2>,
    #[serde(default, skip_serializing_if = "ConflictPolicy::is_error")]
    pub on_conflict: ConflictPolicy,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub unpartitioned: bool,
    pub key: Vec<String>,
    #[serde(default)]
    pub properties: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Edge {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub source: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inputs: Vec<GraphInput>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scd2: Option<Scd2>,
    #[serde(default, skip_serializing_if = "ConflictPolicy::is_error")]
    pub on_conflict: ConflictPolicy,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub unpartitioned: bool,

    #[serde(default)]
    pub key: Vec<String>,
    pub from: Endpoint,
    pub to: Endpoint,
    #[serde(default)]
    pub properties: Vec<String>,
}

/// A physical input for a node or edge type. The column map goes from logical to physical names.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GraphInput {
    pub table: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub columns: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub casts: BTreeMap<String, String>,
    #[serde(default, rename = "where", skip_serializing_if = "Vec::is_empty")]
    pub filters: Vec<Predicate>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Predicate {
    pub column: String,
    pub op: Comparison,
    #[serde(default)]
    pub value: serde_json::Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Comparison {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    IsNull,
    IsNotNull,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Scd2 {
    pub valid_from: Option<String>,
    pub valid_to: Option<String>,
    pub current_flag: Option<String>,
    pub current_value: Option<serde_json::Value>,
    pub open_end: Option<serde_json::Value>,
}

impl Scd2 {
    pub(crate) fn columns(&self) -> impl Iterator<Item = &String> {
        [&self.valid_from, &self.valid_to, &self.current_flag]
            .into_iter()
            .flatten()
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConflictPolicy {
    #[default]
    Error,
    PreferFirst,
}

impl ConflictPolicy {
    fn is_error(&self) -> bool {
        *self == Self::Error
    }
}

pub(crate) struct Mapping<'a> {
    pub(crate) source: &'a str,
    pub(crate) inputs: &'a [GraphInput],
    pub(crate) scd2: Option<&'a Scd2>,
    pub(crate) on_conflict: ConflictPolicy,
    pub(crate) unpartitioned: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Endpoint {
    pub node: String,
    pub columns: Vec<String>,
}

impl Catalog {
    /// Load a graph mapping. Relative Parquet paths are resolved beside the mapping file.
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let input = std::fs::read_to_string(path)
            .with_context(|| format!("reading graph catalog {}", path.display()))?;
        Self::parse_at(&input, path)
    }

    pub(crate) fn parse_at(input: &str, path: &Path) -> Result<Self> {
        let catalog = Self::parse_paths(input, path)?;
        catalog.validate()?;
        Ok(catalog)
    }

    pub(crate) fn parse_paths(input: &str, path: &Path) -> Result<Self> {
        let mut catalog: Self = serde_saphyr::from_str(input).context("invalid graph catalog")?;
        let base = std::path::absolute(path)?
            .parent()
            .map(Path::to_owned)
            .unwrap_or_default();
        for source in catalog.sources.values_mut() {
            source.visit_paths(|path| resolve_path(path, &base))?;
        }
        for path in catalog.export_locations.values_mut() {
            resolve_path(path, &base)?;
        }
        Ok(catalog)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(!self.graph.trim().is_empty(), "graph name is required");
        self.freshness.policy.validate()?;
        ensure!(!self.nodes.is_empty(), "at least one node type is required");
        for (name, node) in &self.nodes {
            validate_name(name)?;
            ensure!(
                !self.edges.contains_key(name),
                "graph type {name} is ambiguous"
            );
            self.validate_mapping(
                name,
                self.mapping(name),
                &node.key,
                &node.properties,
                &node.properties,
            )
            .map_err(|error| ValidationSite::for_mapping(error, name))?;
            if node.key.is_empty() {
                return Err(anyhow::anyhow!("node {name} needs a business key")
                    .context(ValidationSite::field(name, "key", None)));
            }
            validate_columns_at(&node.key, name, "key")?;
            validate_columns_at(&node.properties, name, "properties")?;
        }
        for (name, edge) in &self.edges {
            validate_name(name)?;
            self.validate_mapping(
                name,
                self.mapping(name),
                &edge.key,
                &edge.source_columns(),
                &edge.properties,
            )
            .map_err(|error| ValidationSite::for_mapping(error, name))?;
            validate_columns_at(&edge.key, name, "key")?;
            validate_columns_at(&edge.properties, name, "properties")?;
            for input in &edge.inputs {
                ensure!(
                    input
                        .casts
                        .keys()
                        .all(|field| !edge.from.columns.contains(field)
                            && !edge.to.columns.contains(field)),
                    "edge {name} endpoint columns cannot be cast"
                );
            }
            for (side, endpoint) in [("from", &edge.from), ("to", &edge.to)] {
                let node = self
                    .nodes
                    .get(&endpoint.node)
                    .with_context(|| {
                        format!("edge {name} references unknown node {}", endpoint.node)
                    })
                    .with_context(|| ValidationSite::field(name, side, None))?;
                if endpoint.columns.len() != node.key.len() {
                    return Err(anyhow::anyhow!(
                        "edge {name} has the wrong key arity for {}",
                        endpoint.node
                    )
                    .context(ValidationSite::field(name, side, None)));
                }
                validate_columns_at(&endpoint.columns, name, side)?;
            }
        }
        Ok(())
    }

    pub(crate) fn mapping(&self, name: &str) -> Mapping<'_> {
        if let Some(node) = self.nodes.get(name) {
            return Mapping {
                source: &node.source,
                inputs: &node.inputs,
                scd2: node.scd2.as_ref(),
                on_conflict: node.on_conflict,
                unpartitioned: node.unpartitioned,
            };
        }
        let edge = &self.edges[name];
        Mapping {
            source: &edge.source,
            inputs: &edge.inputs,
            scd2: edge.scd2.as_ref(),
            on_conflict: edge.on_conflict,
            unpartitioned: edge.unpartitioned,
        }
    }

    fn validate_mapping(
        &self,
        name: &str,
        mapping: Mapping<'_>,
        keys: &[String],
        columns: &[String],
        properties: &[String],
    ) -> Result<()> {
        ensure!(
            mapping.source.is_empty() != mapping.inputs.is_empty(),
            "{name} requires either source or inputs"
        );
        if !mapping.source.is_empty() {
            self.source(mapping.source)
                .with_context(|| ValidationSite::field(name, "source", None))?;
        }
        ensure!(
            mapping.on_conflict != ConflictPolicy::PreferFirst
                || (mapping.inputs.len() > 1 && !keys.is_empty()),
            "{name} requires multiple inputs and a business key for prefer_first"
        );
        let temporal = mapping
            .scd2
            .into_iter()
            .flat_map(Scd2::columns)
            .cloned()
            .collect::<Vec<_>>();
        validate_columns(&temporal)?;
        if let Some(scd2) = mapping.scd2 {
            ensure!(
                scd2.valid_from.is_some() == scd2.valid_to.is_some()
                    && (scd2.valid_from.is_some() || scd2.current_flag.is_some()),
                "{name} SCD2 requires a validity interval or a current flag"
            );
            ensure!(
                scd2.open_end.is_none() || scd2.valid_to.is_some(),
                "{name} SCD2 open_end requires valid_to"
            );
            ensure!(
                scd2.current_value.is_none() || scd2.current_flag.is_some(),
                "{name} SCD2 current_value requires current_flag"
            );
            ensure!(!keys.is_empty(), "{name} SCD2 requires a business key");
        }
        for (index, input) in mapping.inputs.iter().enumerate() {
            self.source(&input.table)
                .with_context(|| ValidationSite::field(name, "source", Some(index)))?;
            for (logical, physical) in &input.columns {
                ensure!(
                    keys.contains(logical)
                        || columns.contains(logical)
                        || temporal.contains(logical),
                    "unknown logical column {name}.{logical}"
                );
                validate_name(physical).with_context(|| ValidationSite::InputColumn {
                    label: name.into(),
                    input: index,
                    column: logical.clone(),
                })?;
            }
            for (column, data_type) in &input.casts {
                ensure!(
                    properties.contains(column)
                        && !keys.contains(column)
                        && !temporal.contains(column)
                        && columns.contains(column),
                    "only non-key properties can be cast: {name}.{column}"
                );
                data_type
                    .parse::<datafusion::arrow::datatypes::DataType>()
                    .with_context(|| format!("invalid Arrow type for {name}.{column}"))?;
            }
            for predicate in &input.filters {
                predicate.validate()?;
            }
        }
        Ok(())
    }

    pub(crate) fn source<'a>(
        &'a self,
        reference: &'a str,
    ) -> Result<(&'a str, &'a str, &'a Source)> {
        let (name, table) = reference
            .split_once('.')
            .with_context(|| format!("expected source.table, got {reference}"))?;
        let source = self
            .sources
            .get(name)
            .with_context(|| format!("unknown source {name}"))?;
        match source {
            Source::Sqlite { path } => {
                ensure!(
                    !path.trim().is_empty() && !path.contains("://") && path != ":memory:",
                    "SQLite requires a local database file path"
                );
                validate_name(table)?;
            }
            Source::Duckdb { path } => {
                ensure!(
                    !path.trim().is_empty() && !path.contains("://") && path != ":memory:",
                    "DuckDB requires a local database file path"
                );
                qualified_table(table, 2)?;
            }
            Source::Doris(config)
            | Source::Starrocks(config)
            | Source::Tidb(config)
            | Source::Oceanbase(config)
            | Source::Mariadb(config) => {
                ensure!(
                    !config.dsn_env.trim().is_empty(),
                    "database source requires dsn_env"
                );
                ensure!(
                    config.tls || config.ca_file.is_none(),
                    "ca_file requires TLS"
                );
                qualified_table(table, 2)?;
            }
            Source::SqlserverNative {
                host,
                port,
                database,
                user,
                password_env,
                tls,
                ca_file,
                ..
            } => {
                url::Host::parse(host).context("invalid SQL Server host")?;
                ensure!(
                    *port > 0
                        && !database.trim().is_empty()
                        && !user.trim().is_empty()
                        && !password_env.trim().is_empty(),
                    "SQL Server requires database, user, port and password_env"
                );
                ensure!(*tls || ca_file.is_none(), "ca_file requires TLS");
                qualified_table(table, 2)?;
            }
            Source::OracleNative {
                connect_string,
                user,
                password_env,
            } => {
                ensure!(
                    !connect_string.trim().is_empty()
                        && !user.trim().is_empty()
                        && !password_env.trim().is_empty(),
                    "Oracle requires connect_string, user and password_env"
                );
                qualified_table(table, 2)?;
            }
            Source::MongodbNative {
                dsn_env,
                schema_sample_size,
            } => {
                ensure!(
                    !dsn_env.trim().is_empty() && (1..=10000).contains(schema_sample_size),
                    "MongoDB requires dsn_env and schema_sample_size between 1 and 10000"
                );
                let (database, collection) = table
                    .split_once('.')
                    .context("expected MongoDB database.collection")?;
                validate_name(database)?;
                validate_name(collection)?;
            }
            Source::Databricks {
                uri,
                token_env,
                warehouse,
                catalog,
            } => {
                warehouse_endpoint(uri)?;
                ensure!(
                    !token_env.trim().is_empty()
                        && !warehouse.trim().is_empty()
                        && !catalog.trim().is_empty(),
                    "Databricks requires token_env, warehouse and catalog"
                );
                qualified_table(table, 2)?;
            }
            Source::ObjectStorage {
                storage,
                format,
                tables,
            } => {
                crate::source::storage::validate(storage)?;
                let mapping = tables.get(table).context("unknown object-storage table")?;
                ensure!(
                    !mapping.files.is_empty(),
                    "object-storage table requires files"
                );
                for path in &mapping.files {
                    crate::source::storage::object_path(path)?;
                }
                if !matches!(format, ObjectFormat::Parquet) {
                    mapping.schema()?;
                }
            }
            Source::Postgres { dsn_env } | Source::Mysql { dsn_env, .. } => {
                ensure!(!dsn_env.is_empty(), "database source {name} needs dsn_env");
                let (schema, relation) = table
                    .split_once('.')
                    .with_context(|| format!("expected source.schema.table, got {reference}"))?;
                validate_name(schema)?;
                validate_name(relation)?;
                ensure!(
                    !relation.contains('.'),
                    "invalid database table reference {reference}"
                );
            }
            Source::Snowflake {
                uri,
                token_env,
                warehouse,
                as_of,
                ..
            } => {
                warehouse_table(table)?;
                warehouse_endpoint(uri)?;
                ensure!(
                    !token_env.trim().is_empty() && !warehouse.trim().is_empty(),
                    "Snowflake source {name} requires token_env and warehouse"
                );
                validate_timestamp(as_of.as_deref())?;
            }
            Source::Bigquery {
                uri,
                token_env,
                billing_project,
                location,
                maximum_bytes_billed,
                as_of,
            } => {
                warehouse_table(table)?;
                if let Some(uri) = uri {
                    warehouse_endpoint(uri)?;
                }
                ensure!(
                    !token_env.trim().is_empty()
                        && !location.trim().is_empty()
                        && *maximum_bytes_billed > 0,
                    "BigQuery source {name} requires token_env, location and a positive maximum_bytes_billed"
                );
                ensure!(
                    warehouse_identifier(billing_project),
                    "invalid BigQuery billing project"
                );
                validate_timestamp(as_of.as_deref())?;
            }
            Source::Clickhouse {
                uri,
                user,
                password_env,
            } => {
                qualified_table(table, 2)?;
                warehouse_endpoint(uri)?;
                validate_http_auth(user, None, password_env.as_deref())?;
            }
            Source::Trino {
                uri,
                user,
                token_env,
                password_env,
            } => {
                qualified_table(table, 3)?;
                warehouse_endpoint(uri)?;
                validate_http_auth(user, token_env.as_deref(), password_env.as_deref())?;
            }
            Source::Mongodb(config)
            | Source::Elasticsearch(config)
            | Source::Redshift(config)
            | Source::Sqlserver(config)
            | Source::Oracle(config) => {
                let (schema, table) = table
                    .split_once('.')
                    .with_context(|| format!("expected source.schema.table, got {reference}"))?;
                ensure!(warehouse_identifier(schema), "invalid Trino schema name");
                validate_name(table)?;
                ensure!(
                    warehouse_identifier(&config.catalog),
                    "invalid Trino catalog name"
                );
                warehouse_endpoint(&config.uri)?;
                validate_http_auth(
                    &config.user,
                    config.token_env.as_deref(),
                    config.password_env.as_deref(),
                )?;
            }
            Source::Spanner { database, uri } => {
                ensure!(
                    matches!(database.split('/').collect::<Vec<_>>().as_slice(),
                        ["projects", project, "instances", instance, "databases", database]
                            if [project, instance, database].into_iter().all(|name| warehouse_identifier(name))),
                    "Spanner database must use projects/PROJECT/instances/INSTANCE/databases/DATABASE"
                );
                ensure!(
                    (1..=2).contains(&table.split('.').count())
                        && table.split('.').all(warehouse_identifier),
                    "Spanner table must use table or schema.table"
                );
                if let Some(uri) = uri {
                    warehouse_endpoint(uri)?;
                }
            }
            Source::SapHana {
                host,
                port,
                user,
                password_env,
                tls,
                ca_file,
            } => {
                qualified_table(table, 2)?;
                let host = url::Host::parse(host).context("invalid SAP HANA host")?;
                ensure!(
                    *port > 0 && !user.trim().is_empty() && !password_env.trim().is_empty(),
                    "SAP HANA requires a port, user and password_env"
                );
                ensure!(
                    *tls || (host_is_loopback(&host) && ca_file.is_none()),
                    "SAP HANA requires TLS except for loopback connections"
                );
            }
            Source::Delta { tables } => {
                ensure!(
                    tables
                        .get(table)
                        .is_some_and(|mapping| !mapping.path.trim().is_empty()),
                    "Delta table {reference} requires a table path"
                );
            }
            Source::Iceberg { uri, .. } => {
                let url = url::Url::parse(uri).context("invalid Iceberg REST catalog URI")?;
                ensure!(
                    matches!(url.scheme(), "http" | "https")
                        && url.host_str().is_some()
                        && url.username().is_empty()
                        && url.password().is_none()
                        && url.query().is_none()
                        && url.fragment().is_none(),
                    "Iceberg REST catalog URI must be HTTP(S) without credentials, query or fragment"
                );
                ensure!(
                    table.contains('.') && table.split('.').all(|part| !part.trim().is_empty()),
                    "expected Iceberg source.namespace.table, got {reference}"
                );
            }
            Source::Parquet { tables } => {
                ensure!(
                    tables.get(table).is_some_and(
                        |files| !files.is_empty() && files.iter().all(|file| !file.is_empty())
                    ),
                    "Parquet table {reference} needs an explicit file list"
                );
            }
            Source::Csv { tables } | Source::Jsonl { tables } => {
                tables
                    .get(table)
                    .with_context(|| format!("unknown text table {reference}"))?
                    .schema()?;
            }
        }
        Ok((name, table, source))
    }
}

impl Edge {
    pub(crate) fn identity(&self) -> Vec<String> {
        if self.key.is_empty() {
            vec![ROW_ID.into()]
        } else {
            self.key.clone()
        }
    }

    pub(crate) fn source_columns(&self) -> Vec<String> {
        columns([
            &self.key,
            &self.from.columns,
            &self.to.columns,
            &self.properties,
        ])
    }
}

pub(crate) fn columns<'a>(lists: impl IntoIterator<Item = &'a Vec<String>>) -> Vec<String> {
    let mut seen = BTreeSet::new();
    lists
        .into_iter()
        .flatten()
        .filter(|name| seen.insert(*name))
        .cloned()
        .collect()
}

pub(crate) fn warehouse_endpoint(uri: &str) -> Result<url::Url> {
    let url = url::Url::parse(uri).context("invalid warehouse API endpoint")?;
    let loopback = url.host().is_some_and(|host| host_is_loopback(&host));
    ensure!(
        (url.scheme() == "https" || (url.scheme() == "http" && loopback))
            && url.host_str().is_some()
            && url.username().is_empty()
            && url.password().is_none()
            && url.path() == "/"
            && url.query().is_none()
            && url.fragment().is_none(),
        "warehouse API URI must be an HTTPS origin or a loopback HTTP origin"
    );
    Ok(url)
}

fn host_is_loopback<T: AsRef<str>>(host: &url::Host<T>) -> bool {
    match host {
        url::Host::Ipv4(ip) => ip.is_loopback(),
        url::Host::Ipv6(ip) => ip.is_loopback(),
        url::Host::Domain(name) => name.as_ref() == "localhost",
    }
}

pub(crate) fn warehouse_identifier(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'$'))
}

fn warehouse_table(table: &str) -> Result<()> {
    qualified_table(table, 3)
}

fn qualified_table(table: &str, width: usize) -> Result<()> {
    ensure!(
        table.split('.').count() == width && table.split('.').all(warehouse_identifier),
        "warehouse table must use {width} simple identifiers"
    );
    Ok(())
}

fn validate_http_auth(user: &str, token: Option<&str>, password: Option<&str>) -> Result<()> {
    ensure!(
        !user.trim().is_empty() && !user.contains(':'),
        "invalid connector user"
    );
    reqwest::header::HeaderValue::from_str(user).context("invalid connector user header")?;
    ensure!(
        token.is_none() || password.is_none(),
        "configure either token_env or password_env"
    );
    ensure!(
        token
            .into_iter()
            .chain(password)
            .all(|name| !name.trim().is_empty()),
        "credential environment variable cannot be empty"
    );
    Ok(())
}

fn validate_timestamp(value: Option<&str>) -> Result<()> {
    if let Some(value) = value {
        chrono::DateTime::parse_from_rfc3339(value).context("invalid warehouse as_of timestamp")?;
    }
    Ok(())
}

impl Source {
    pub(crate) fn visit_paths(
        &mut self,
        mut visit: impl FnMut(&mut String) -> Result<()>,
    ) -> Result<()> {
        match self {
            Source::Sqlite { path } | Source::Duckdb { path } => visit(path)?,
            Source::Parquet { tables } => {
                for file in tables.values_mut().flatten() {
                    visit(file)?;
                }
            }
            Source::Delta { tables } => {
                for table in tables.values_mut() {
                    visit(&mut table.path)?;
                }
            }
            Source::Csv { tables } | Source::Jsonl { tables } => {
                for table in tables.values_mut() {
                    for file in &mut table.files {
                        visit(file)?;
                    }
                }
            }
            Source::Mysql {
                ca_file: Some(path),
                ..
            }
            | Source::SqlserverNative {
                ca_file: Some(path),
                ..
            }
            | Source::SapHana {
                ca_file: Some(path),
                ..
            } => visit(path)?,
            Source::Doris(config)
            | Source::Starrocks(config)
            | Source::Tidb(config)
            | Source::Oceanbase(config)
            | Source::Mariadb(config) => {
                if let Some(path) = &mut config.ca_file {
                    visit(path)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
}

fn resolve_path(path: &mut String, base: &Path) -> Result<()> {
    if !path.trim().is_empty() && !path.contains("://") && Path::new(path).is_relative() {
        let resolved: PathBuf = base.join(&*path);
        *path = resolved
            .into_os_string()
            .into_string()
            .map_err(|_| anyhow::anyhow!("source path must be UTF-8"))?;
    }
    Ok(())
}

fn validate_name(name: &str) -> Result<()> {
    ensure!(
        !name.trim().is_empty() && !name.contains('\0'),
        "invalid identifier {name:?}"
    );
    Ok(())
}

fn validate_columns(names: &[String]) -> Result<()> {
    check_columns(names).map_err(|(_, error)| error)
}

fn validate_columns_at(names: &[String], label: &str, field: &'static str) -> Result<()> {
    check_columns(names)
        .map_err(|(index, error)| error.context(ValidationSite::field(label, field, Some(index))))
}

fn check_columns(names: &[String]) -> std::result::Result<(), (usize, anyhow::Error)> {
    let mut seen = BTreeSet::new();
    for (index, name) in names.iter().enumerate() {
        let result = (|| {
            validate_name(name)?;
            ensure!(
                !name.starts_with("__graphlake_"),
                "column name {name} uses the reserved __graphlake_ prefix"
            );
            ensure!(seen.insert(name), "duplicate column {name:?}");
            Ok(())
        })();
        result.map_err(|error| (index, error))?;
    }
    Ok(())
}

impl Predicate {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_name(&self.column)?;
        match self.op {
            Comparison::IsNull | Comparison::IsNotNull => {
                ensure!(self.value.is_null(), "NULL checks take no value")
            }
            _ => ensure!(
                self.value.is_boolean() || self.value.is_number() || self.value.is_string(),
                "comparison on {} requires a non-NULL scalar value",
                self.column
            ),
        }
        Ok(())
    }
}

pub(crate) fn ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

pub(crate) fn qualified(relation: &str, column: &str) -> String {
    format!("{}.{}", ident(relation), ident(column))
}

pub(crate) fn union_all(reads: &[String]) -> String {
    assert!(!reads.is_empty(), "UNION needs at least one input");
    if let [read] = reads {
        return read.clone();
    }
    // Balance the SQL AST even for graphs with hundreds of mapped types.
    let (left, right) = reads.split_at(reads.len() / 2);
    format!("({}) UNION ALL ({})", union_all(left), union_all(right))
}

pub(crate) fn equal_keys(
    left: &str,
    left_keys: &[String],
    right: &str,
    right_keys: &[String],
) -> String {
    left_keys
        .iter()
        .zip(right_keys)
        .map(|(l, r)| format!("{} = {}", qualified(left, l), qualified(right, r)))
        .collect::<Vec<_>>()
        .join(" AND ")
}
