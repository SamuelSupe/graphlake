use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use datafusion::{
    arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
    catalog::TableProvider,
    execution::{
        disk_manager::DiskManager,
        memory_pool::{MemoryPool, MemoryReservation},
    },
};
use hdbconnect_async::{
    ConnectParamsBuilder, Connection, ConnectionConfiguration, FieldMetadata, HdbValue, ResultSet,
    ServerCerts, TypeId,
};

use super::{ReadStats, collect_rows, credential, types::numeric};
use crate::{Predicate, Source, catalog::ident, engine::read_filters::where_clause};

pub(crate) struct Hana {
    connection: Connection,
    pool: Arc<dyn MemoryPool>,
    stats: Arc<ReadStats>,
}

impl Hana {
    #[cfg(feature = "admin")]
    pub(crate) async fn browse(&self, schema: &str) -> Result<Vec<String>> {
        use futures::TryStreamExt;
        let (_, result) = self.query(format!(
            "SELECT TABLE_NAME FROM SYS.TABLES WHERE SCHEMA_NAME = '{}' ORDER BY TABLE_NAME LIMIT 2001",
            schema.replace('\'', "''")), &[]).await?;
        let mut rows = std::pin::pin!(rows(result));
        let mut names = Vec::new();
        while let Some(row) = rows.try_next().await? {
            if let [Some(table)] = row.as_slice() {
                names.push(table.clone());
            }
        }
        Ok(names)
    }

    pub(crate) async fn open(
        source: &Source,
        timeout: Duration,
        batch_size: usize,
        pool: Arc<dyn MemoryPool>,
        stats: Arc<ReadStats>,
    ) -> Result<Self> {
        let Source::SapHana {
            host,
            port,
            user,
            password_env,
            tls,
            ca_file,
        } = source
        else {
            bail!("expected SAP HANA source");
        };
        let mut params = ConnectParamsBuilder::new()
            .with_hostname(host.trim_start_matches('[').trim_end_matches(']'))
            .with_port(*port)
            .with_dbuser(user)
            .with_password(credential(password_env)?);
        if *tls {
            let _ = rustls::crypto::ring::default_provider().install_default();
            params.tls_with(ServerCerts::RootCertificates);
            if let Some(path) = ca_file {
                let cert = std::fs::read_to_string(path).context("reading SAP HANA CA file")?;
                params.tls_with(ServerCerts::Direct(cert));
            }
        }
        let config = ConnectionConfiguration::default()
            .with_fetch_size(batch_size.clamp(1, 1024) as u32)
            .with_read_timeout(Some(timeout));
        stats.request();
        let connection = Connection::with_configuration(params, &config).await?;
        Ok(Self {
            connection,
            pool,
            stats,
        })
    }

    pub(crate) async fn table(
        &self,
        relation: &str,
        columns: &[String],
        filters: &[Predicate],
        batch_size: usize,
        disk: &Arc<DiskManager>,
        explain_only: bool,
    ) -> Result<(Arc<dyn TableProvider>, MemoryReservation, String)> {
        let relation = relation.split('.').map(ident).collect::<Vec<_>>().join(".");
        let selected = columns
            .iter()
            .map(|name| ident(name))
            .collect::<Vec<_>>()
            .join(", ");
        let selected = if explain_only && columns.is_empty() {
            "*".into()
        } else {
            selected
        };
        let mut sql = format!("SELECT {selected} FROM {relation}");
        let version = uuid::Uuid::new_v4().to_string();
        let expected = if explain_only || !filters.is_empty() {
            let (schema, result) = self.query(format!("{sql} WHERE 1 = 0"), columns).await?;
            let (table, budget) = collect_rows(
                rows(result),
                Arc::clone(&schema),
                batch_size,
                &self.pool,
                &self.stats,
                None,
            )
            .await?;
            if explain_only {
                return Ok((table, budget, version));
            }
            sql.push_str(&where_clause(filters, &schema, ident));
            Some(schema)
        } else {
            None
        };
        let (schema, result) = self.query(sql, columns).await?;
        ensure!(
            expected.is_none_or(|expected| expected == schema),
            "SAP HANA result schema changed"
        );
        let (table, budget) = collect_rows(
            rows(result),
            schema,
            batch_size,
            &self.pool,
            &self.stats,
            Some(disk),
        )
        .await?;
        Ok((table, budget, version))
    }

    async fn query(&self, sql: String, columns: &[String]) -> Result<(SchemaRef, ResultSet)> {
        self.stats.request();
        let result = self.connection.query(sql).await?;
        let metadata = result.metadata();
        ensure!(
            columns.is_empty()
                || metadata
                    .iter()
                    .map(FieldMetadata::displayname)
                    .eq(columns.iter().map(String::as_str)),
            "SAP HANA result columns differ from projection"
        );
        let fields = metadata.iter().map(field).collect::<Result<Vec<_>>>()?;
        Ok((Arc::new(Schema::new(fields)), result))
    }
}

fn rows(mut result: ResultSet) -> impl futures::Stream<Item = Result<Vec<Option<String>>>> {
    async_stream::try_stream! {
        while let Some(mut row) = result.next_row().await? {
            let cells = std::iter::from_fn(|| row.next_value()).map(cell).collect::<Result<Vec<_>>>()?;
            yield cells;
        }
    }
}

fn field(meta: &FieldMetadata) -> Result<Field> {
    let kind = match meta.type_id() {
        TypeId::TINYINT => DataType::UInt8,
        TypeId::SMALLINT => DataType::Int16,
        TypeId::INT => DataType::Int32,
        TypeId::BIGINT => DataType::Int64,
        TypeId::REAL => DataType::Float32,
        TypeId::DOUBLE => DataType::Float64,
        TypeId::DECIMAL | TypeId::FIXED8 | TypeId::FIXED12 | TypeId::FIXED16 => {
            numeric(meta.precision().try_into()?, meta.scale().try_into()?)?
        }
        TypeId::BOOLEAN => DataType::Boolean,
        TypeId::CHAR
        | TypeId::VARCHAR
        | TypeId::NCHAR
        | TypeId::NVARCHAR
        | TypeId::STRING
        | TypeId::NSTRING
        | TypeId::ALPHANUM => DataType::Utf8,
        TypeId::BINARY | TypeId::VARBINARY | TypeId::BSTRING => DataType::Binary,
        TypeId::DAYDATE => DataType::Date32,
        TypeId::SECONDTIME => DataType::Time64(TimeUnit::Microsecond),
        TypeId::SECONDDATE | TypeId::LONGDATE => DataType::Timestamp(TimeUnit::Microsecond, None),
        kind => bail!(
            "unsupported SAP HANA type {kind:?} for {}",
            meta.displayname()
        ),
    };
    ensure!(
        !meta.is_array_type(),
        "SAP HANA arrays require an explicit scalar projection"
    );
    Ok(Field::new(meta.displayname(), kind, meta.is_nullable()))
}

fn cell(value: HdbValue<'_>) -> Result<Option<String>> {
    let value = match value {
        HdbValue::NULL => return Ok(None),
        HdbValue::TINYINT(value) => value.to_string(),
        HdbValue::SMALLINT(value) => value.to_string(),
        HdbValue::INT(value) => value.to_string(),
        HdbValue::BIGINT(value) => value.to_string(),
        HdbValue::DECIMAL(value) => value.to_plain_string(),
        HdbValue::REAL(value) => value.to_string(),
        HdbValue::DOUBLE(value) => value.to_string(),
        HdbValue::BOOLEAN(value) => value.to_string(),
        HdbValue::STRING(value) => value,
        HdbValue::BINARY(value) => STANDARD.encode(value),
        HdbValue::LONGDATE(value) => value.to_string(),
        HdbValue::SECONDDATE(value) => value.to_string(),
        HdbValue::DAYDATE(value) => value.to_string(),
        HdbValue::SECONDTIME(value) => value.to_string(),
        _ => bail!("unsupported SAP HANA result value"),
    };
    Ok(Some(value))
}
