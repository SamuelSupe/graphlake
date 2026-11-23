mod frontier;

use std::{
    collections::BTreeMap,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail, ensure};
use async_trait::async_trait;
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, Utc};
use datafusion::{
    arrow::{
        array::{
            ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
            Float64Array, Int16Array, Int32Array, Int64Array, RecordBatch, RecordBatchOptions,
            StringArray, TimestampMicrosecondArray,
        },
        datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
    },
    catalog::{Session, TableProvider},
    common::{DataFusionError, ScalarValue},
    execution::{SendableRecordBatchStream, TaskContext, memory_pool::MemoryConsumer},
    logical_expr::{Expr, Operator, TableProviderFilterPushDown, TableType},
    physical_plan::{
        ExecutionPlan,
        stream::RecordBatchStreamAdapter,
        streaming::{PartitionStream, StreamingTableExec},
    },
};
use futures::TryStreamExt;
use tokio::{runtime::Handle, task::JoinHandle};
use tokio_postgres::{
    Client, Column, Config, Row,
    config::SslMode,
    types::{FromSql, Type},
};
use tokio_postgres_rustls::MakeRustlsConnect;

use super::{ReadStats, decimal128};
use crate::catalog::ident;

pub(crate) struct Snapshot {
    client: Client,
    driver: Option<JoinHandle<()>>,
    tls: MakeRustlsConnect,
    next_cursor: AtomicU64,
    interrupted: AtomicBool,
    stats: Arc<ReadStats>,
    pub(crate) version: String,
    #[cfg(feature = "admin")]
    pub(crate) exploration_snapshot: Option<String>,
}

struct ActiveRequest<'a> {
    interrupted: &'a AtomicBool,
    active: bool,
}

impl ActiveRequest<'_> {
    fn finish(&mut self) {
        self.active = false;
    }
}

impl Drop for ActiveRequest<'_> {
    fn drop(&mut self) {
        if self.active {
            self.interrupted.store(true, Ordering::Relaxed);
        }
    }
}

impl Snapshot {
    fn active_request(&self) -> ActiveRequest<'_> {
        ActiveRequest {
            interrupted: &self.interrupted,
            active: true,
        }
    }

    async fn request<F: std::future::Future>(&self, future: F) -> F::Output {
        let mut request = self.active_request();
        let output = future.await;
        request.finish();
        output
    }

    #[cfg(feature = "admin")]
    pub(crate) async fn recovery_target_empty(&self) -> Result<bool> {
        Ok(self
            .request(self.client.query_one(
                "SELECT NOT EXISTS (
                SELECT 1 FROM pg_namespace
                WHERE nspname NOT LIKE 'pg\\_%' AND nspname NOT IN ('public', 'information_schema')
                UNION ALL
                SELECT 1 FROM pg_depend
                WHERE refclassid = 'pg_namespace'::regclass AND refobjid = 'public'::regnamespace
                UNION ALL
                SELECT 1 FROM pg_extension WHERE extname <> 'plpgsql'
                UNION ALL
                SELECT 1 FROM pg_event_trigger
                UNION ALL
                SELECT 1 FROM pg_publication
                UNION ALL
                SELECT 1 FROM pg_foreign_server
            )",
                &[],
            ))
            .await?
            .try_get(0)?)
    }

    #[cfg(feature = "admin")]
    pub(crate) async fn relation_reuses_snapshot(&self, relation: &str) -> Result<bool> {
        let (schema, table) = relation
            .split_once('.')
            .context("expected PostgreSQL schema.table")?;
        self.relation_is_local(&format!("{}.{}", ident(schema), ident(table)), true)
            .await
    }

    async fn relation_is_local(&self, quoted_relation: &str, exclude_rls: bool) -> Result<bool> {
        self.stats.request();
        Ok(self.request(self.client.query_one(
            "WITH RECURSIVE relations(oid) AS (
                SELECT $1::text::regclass::oid
                UNION
                SELECT child.inhrelid FROM pg_inherits AS child
                JOIN relations AS parent ON child.inhparent = parent.oid
            )
            SELECT COALESCE(bool_and(tbl.relkind IN ('r', 'p', 'm') AND (NOT $2 OR NOT tbl.relrowsecurity)), false)
            FROM relations JOIN pg_class AS tbl USING (oid)",
            &[&quoted_relation, &exclude_rls],
        )).await?.try_get(0)?)
    }

    #[cfg(feature = "admin")]
    pub(crate) async fn recovery_digest(&self, relation: &str) -> Result<String> {
        use sha2::{Digest, Sha256};

        let (schema, table) = relation
            .split_once('.')
            .context("expected PostgreSQL schema.table")?;
        let relation = format!("{}.{}", ident(schema), ident(table));
        ensure!(
            self.relation_is_local(&relation, true).await?,
            "expected a local table without row-level security"
        );
        self.request(self.client.batch_execute(
            "SET LOCAL TimeZone = 'UTC'; SET LOCAL DateStyle = 'ISO, MDY';
             SET LOCAL IntervalStyle = 'postgres'; SET LOCAL extra_float_digits = 3;
             SET LOCAL bytea_output = 'hex'; SET LOCAL client_encoding = 'UTF8';",
        ))
        .await?;
        let columns = self
            .request(self.client.query(
                "SELECT attname::text, format_type(atttypid, atttypmod)
             FROM pg_attribute WHERE attrelid = $1::text::regclass AND attnum > 0
             AND NOT attisdropped ORDER BY attnum",
                &[&relation],
            ))
            .await?
            .iter()
            .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
            .collect::<Vec<_>>();
        // Ordering the canonical rows preserves duplicates and ignores heap order,
        // which can change during a logical restore. COPY streams the sorted rows.
        let sql = format!(
            r#"COPY (
            SELECT row_to_json(t)::text COLLATE "C" FROM {relation} AS t ORDER BY 1
        ) TO STDOUT"#
        );
        let mut request = self.active_request();
        let stream = self.client.copy_out(&sql).await?;
        futures::pin_mut!(stream);
        let mut hash = Sha256::new();
        hash.update(serde_json::to_vec(&columns)?);
        hash.update(b"\n");
        while let Some(chunk) = stream.try_next().await? {
            hash.update(chunk);
        }
        request.finish();
        Ok(hash
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect())
    }

    #[cfg(feature = "admin")]
    pub(crate) async fn relation_version(&self, relation: &str) -> Result<String> {
        use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
        use sha2::{Digest, Sha256};

        let (schema, table) = relation
            .split_once('.')
            .context("expected PostgreSQL schema.table")?;
        let relation = format!("{}.{}", ident(schema), ident(table));
        if self.exploration_snapshot.is_some()
            && let Some(version) = self.tracked_relation_version(&relation).await?
        {
            return Ok(format!(
                "tracked:{}",
                URL_SAFE_NO_PAD.encode(Sha256::digest(version))
            ));
        }
        self.stats.request();
        let columns = self
            .request(self.client.query(
                "SELECT attname::text, atttypid::text, atttypmod FROM pg_catalog.pg_attribute
             WHERE attrelid = $1::text::regclass AND attnum > 0 AND NOT attisdropped
               AND has_column_privilege(attrelid, attnum, 'SELECT') ORDER BY attnum",
                &[&relation],
            ))
            .await?;
        let fields = columns
            .iter()
            .map(|row| {
                (
                    row.get::<_, String>(0),
                    row.get::<_, String>(1),
                    row.get::<_, i32>(2),
                )
            })
            .collect::<Vec<_>>();
        ensure!(
            !fields.is_empty(),
            "no readable PostgreSQL columns in {relation}"
        );
        let projection = fields
            .iter()
            .map(|(name, _, _)| ident(name))
            .collect::<Vec<_>>()
            .join(", ");
        // Counts and two sums preserve multiplicity without sorting the table or transferring rows.
        // The projection covers readable columns even when different labels use different subsets.
        let sql = format!(
            "SELECT count(*)::text,
                    COALESCE(sum(hashtextextended(row_to_json(t)::text COLLATE \"C\", 0)::numeric), 0)::text,
                    COALESCE(sum(hashtextextended(row_to_json(t)::text COLLATE \"C\", 1)::numeric), 0)::text
             FROM (SELECT {projection} FROM {relation}) AS t"
        );
        self.stats.request();
        let row = self
            .request(self.client.query_one(&sql, &[]))
            .await
            .with_context(|| format!("fingerprinting PostgreSQL relation {relation}"))?;
        let summary = [row.get::<_, String>(0), row.get(1), row.get(2)];
        let fingerprint = Sha256::digest(serde_json::to_vec(&(fields, summary))?);
        Ok(URL_SAFE_NO_PAD.encode(fingerprint))
    }

    #[cfg(feature = "admin")]
    async fn tracked_relation_version(&self, relation: &str) -> Result<Option<String>> {
        self.stats.request();
        let eligible = self.request(self
            .client
            .query_one(
                "SELECT EXISTS (
                SELECT FROM pg_namespace n JOIN pg_class c ON c.relnamespace = n.oid
                WHERE n.nspname = 'graphlake_tracking' AND c.relname IN ('relation_versions', 'relation_changes')
                  AND has_schema_privilege(n.oid, 'USAGE') AND has_table_privilege(c.oid, 'SELECT')
                GROUP BY n.oid HAVING count(*) = 2
            )",
                &[],
            ))
            .await?
            .get::<_, bool>(0);
        if !eligible {
            return Ok(None);
        }
        self.stats.request();
        // Include catalog state so replacing, rewriting or changing the table,
        // disabling/reinstalling the trigger, and changing visible columns invalidate the token.
        Ok(self.request(self.client.query_opt(
            "SELECT jsonb_build_array(
                pg_postmaster_start_time(), inet_server_addr(), inet_server_port(),
                current_database(), current_user, c.oid, c.relfilenode,
                v.generation + (SELECT count(*) FROM graphlake_tracking.relation_changes ch WHERE ch.relation = c.oid),
                t.oid, t.xmin::text, p.oid, p.xmin::text,
                (SELECT jsonb_agg(jsonb_build_array(a.attname, a.atttypid, a.atttypmod, a.attcollation)
                    ORDER BY a.attnum) FROM pg_attribute a
                 WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
                    AND has_column_privilege(c.oid, a.attnum, 'SELECT'))
             )::text
             FROM pg_class c
             JOIN pg_am am ON am.oid = c.relam AND am.amname = 'heap'
             JOIN graphlake_tracking.relation_versions v ON v.relation = c.oid
             JOIN pg_trigger t ON t.tgrelid = c.oid
             JOIN pg_proc p ON p.oid = t.tgfoid
             WHERE c.oid = $1::text::regclass AND c.relkind = 'r' AND c.relpersistence = 'p'
                 AND NOT c.relrowsecurity
                 AND NOT EXISTS (SELECT FROM pg_inherits WHERE c.oid IN (inhrelid, inhparent))
                 AND t.tgname = 'graphlake_relation_version' AND t.tgenabled = 'A'
                 AND t.tgtype = 60 AND t.tgqual IS NULL AND t.tgnargs = 0
                 AND p.oid = to_regprocedure('graphlake_tracking.record_relation_change()')",
            &[&relation],
        )).await?.map(|row| row.get(0)))
    }

    #[cfg(feature = "admin")]
    pub(crate) async fn browse(&self, schema: Option<&str>) -> Result<Vec<String>> {
        let rows = if let Some(schema) = schema {
            self.request(self.client.query(
                "SELECT table_name::text FROM information_schema.tables WHERE table_schema = $1
                 ORDER BY table_name LIMIT 2001",
                &[&schema],
            ))
            .await?
        } else {
            self.request(self.client.query(
                "SELECT DISTINCT table_schema::text FROM information_schema.tables
                 WHERE table_schema NOT IN ('pg_catalog','information_schema')
                 ORDER BY 1 LIMIT 2001",
                &[],
            ))
            .await?
        };
        Ok(rows.iter().map(|row| row.get(0)).collect())
    }

    #[cfg(feature = "admin")]
    pub(crate) async fn describe(
        &self,
        schema: &str,
        table: &str,
    ) -> Result<Vec<(String, String, bool)>> {
        let sql = format!("SELECT * FROM {}.{} LIMIT 0", ident(schema), ident(table));
        let statement = self.request(self.client.prepare(&sql)).await?;
        Ok(statement
            .columns()
            .iter()
            .map(|column| {
                let ty = field(column).map_or_else(
                    |_| format!("{}（暂不支持）", column.type_().name()),
                    |field| field.data_type().to_string(),
                );
                (column.name().to_owned(), ty, true)
            })
            .collect())
    }

    pub(crate) async fn open(
        dsn_env: &str,
        timeout: Duration,
        stats: Arc<ReadStats>,
    ) -> Result<Self> {
        let dsn = std::env::var(dsn_env)
            .with_context(|| format!("missing PostgreSQL DSN in {dsn_env}"))?;
        let mut config = dsn
            .parse::<Config>()
            .map_err(|_| anyhow!("invalid PostgreSQL DSN in {dsn_env}"))?;
        if config.get_ssl_mode() == SslMode::Prefer {
            config.ssl_mode(SslMode::Require);
        }
        let tls = if config.get_ssl_mode() != SslMode::Disable {
            static VERIFIED_TLS: tokio::sync::OnceCell<MakeRustlsConnect> =
                tokio::sync::OnceCell::const_new();
            VERIFIED_TLS
                .get_or_try_init(|| async {
                    tokio::task::spawn_blocking(|| -> Result<MakeRustlsConnect> {
                        let mut roots = rustls::RootCertStore::empty();
                        roots.add_parsable_certificates(
                            rustls_native_certs::load_native_certs().certs,
                        );
                        ensure!(
                            !roots.is_empty(),
                            "no trusted PostgreSQL TLS roots in the system certificate store"
                        );
                        Ok(MakeRustlsConnect::new(
                            rustls::ClientConfig::builder_with_provider(
                                rustls::crypto::ring::default_provider().into(),
                            )
                            .with_safe_default_protocol_versions()?
                            .with_root_certificates(roots)
                            .with_no_client_auth(),
                        ))
                    })
                    .await
                    .context("loading PostgreSQL TLS roots")?
                })
                .await?
                .clone()
        } else {
            static PLAIN_TLS: OnceLock<MakeRustlsConnect> = OnceLock::new();
            if let Some(tls) = PLAIN_TLS.get() {
                tls.clone()
            } else {
                let built = MakeRustlsConnect::new(
                    rustls::ClientConfig::builder_with_provider(
                        rustls::crypto::ring::default_provider().into(),
                    )
                    .with_safe_default_protocol_versions()?
                    .with_root_certificates(rustls::RootCertStore::empty())
                    .with_no_client_auth(),
                );
                PLAIN_TLS.get_or_init(|| built).clone()
            }
        };
        let (client, connection) = tokio::time::timeout(timeout, config.connect(tls.clone()))
            .await
            .context("PostgreSQL connection timed out")?
            .context("opening PostgreSQL connection")?;
        let driver = tokio::spawn(async move {
            let _ = connection.await;
        });
        let mut snapshot = Self {
            client,
            driver: Some(driver),
            tls,
            stats,
            next_cursor: AtomicU64::new(0),
            interrupted: AtomicBool::new(false),
            version: String::new(),
            #[cfg(feature = "admin")]
            exploration_snapshot: None,
        };
        let timeout_ms = timeout.as_millis().clamp(1, i32::MAX as u128);
        snapshot.stats.request();
        snapshot
            .request(snapshot.client.batch_execute(&format!(
                "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
             SET LOCAL statement_timeout = {timeout_ms};
             SET LOCAL search_path = pg_catalog;"
            )))
            .await
            .context("starting PostgreSQL read-only snapshot")?;
        snapshot.stats.request();
        let row = snapshot.request(snapshot
            .client
            .query_one(
                if cfg!(feature = "admin") {
                    "SELECT pg_current_snapshot()::text, CASE WHEN NOT pg_is_in_recovery() THEN
                        jsonb_build_array(pg_current_snapshot()::text, pg_postmaster_start_time(),
                            inet_server_addr(), inet_server_port(), current_database(), current_user)::text END"
                } else {
                    "SELECT pg_current_snapshot()::text"
                },
                &[],
            ))
            .await
            .context("pinning PostgreSQL snapshot")?;
        snapshot.version = row.try_get(0)?;
        #[cfg(feature = "admin")]
        {
            snapshot.exploration_snapshot = row.try_get(1)?;
        }
        Ok(snapshot)
    }

    pub(crate) async fn table(
        self: &Arc<Self>,
        table: &str,
        columns: &[String],
    ) -> Result<PgTable> {
        let (schema, table) = table
            .split_once('.')
            .context("expected PostgreSQL schema.table")?;
        let relation = format!("{}.{}", ident(schema), ident(table));
        let selected = columns.iter().map(|name| ident(name)).collect::<Vec<_>>();
        let sql = format!("SELECT {} FROM {relation} LIMIT 0", selected.join(", "));
        self.stats.request();
        let stmt = self
            .request(self.client.prepare(&sql))
            .await
            .map_err(|error| {
                if let Some(db) = error.as_db_error()
                    && *db.code() == tokio_postgres::error::SqlState::UNDEFINED_COLUMN
                {
                    let error =
                        anyhow!("missing PostgreSQL column in {relation}: {}", db.message());
                    if let Some(tokio_postgres::error::ErrorPosition::Original(position)) =
                        db.position()
                    {
                        // PostgreSQL positions count characters from one, including quoted identifiers.
                        let mut start = 8;
                        for (column, ident) in columns.iter().zip(&selected) {
                            let end = start + ident.chars().count() as u32;
                            if (start..end).contains(position) {
                                return error.context(crate::catalog::ValidationSite::Column {
                                    column: column.clone(),
                                });
                            }
                            start = end + 2;
                        }
                    }
                    return error;
                }
                anyhow!(error)
            })
            .with_context(|| format!("describing PostgreSQL relation {relation}"))?;
        let fields = stmt
            .columns()
            .iter()
            .map(field)
            .collect::<Result<Vec<_>>>()?;
        Ok(PgTable {
            snapshot: Arc::clone(self),
            relation,
            schema: Arc::new(Schema::new(fields)),
            types: stmt
                .columns()
                .iter()
                .map(|col| col.type_().clone())
                .collect(),
            scan_filter: None,
            top: None,
        })
    }
}

impl Drop for Snapshot {
    fn drop(&mut self) {
        // Closing the socket alone may leave a backend computing until its next write.
        let interrupted = self.interrupted.swap(false, Ordering::AcqRel);
        if interrupted {
            if let Ok(handle) = Handle::try_current() {
                let token = self.client.cancel_token();
                let tls = self.tls.clone();
                handle.spawn(async move {
                    let _ =
                        tokio::time::timeout(Duration::from_secs(2), token.cancel_query(tls)).await;
                });
            }
            if let Some(driver) = self.driver.take() {
                driver.abort();
            }
        } else if let Some(driver) = self.driver.take() {
            if let Ok(handle) = Handle::try_current() {
                // Allow the driver to send PostgreSQL's Terminate message once `self.client`
                // drops immediately after this Drop impl returns, aborting only if it hangs.
                let abort = driver.abort_handle();
                handle.spawn(async move {
                    if tokio::time::timeout(Duration::from_millis(500), driver)
                        .await
                        .is_err()
                    {
                        abort.abort();
                    }
                });
            } else {
                driver.abort();
            }
        }
    }
}

impl std::fmt::Debug for Snapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgreSQL snapshot")
            .field("version", &self.version)
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub(crate) struct PgTable {
    snapshot: Arc<Snapshot>,
    relation: String,
    schema: SchemaRef,
    types: Vec<Type>,
    scan_filter: Option<String>,
    top: Option<(String, usize)>,
}

impl std::fmt::Debug for PgTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresScan")
            .field("relation", &self.relation)
            .finish_non_exhaustive()
    }
}

impl PgTable {
    #[cfg(feature = "admin")]
    pub(crate) fn watermark(&self, column: &str, checkpoint: &ScalarValue) -> Self {
        let Some(index) = self.schema.index_of(column).ok() else {
            return self.clone();
        };
        if self.schema.field(index).data_type() != &checkpoint.data_type() {
            return self.clone();
        }
        let bound = match (checkpoint, self.types[index].clone()) {
            (ScalarValue::Date32(Some(days)), Type::DATE) => {
                format!("DATE '1970-01-01' + {days}")
            }
            (
                ScalarValue::TimestampMicrosecond(Some(value), _),
                kind @ (Type::TIMESTAMP | Type::TIMESTAMPTZ),
            ) => {
                let Some(date) = DateTime::from_timestamp_micros(*value)
                    .filter(|date| (1..=9999).contains(&date.year()))
                else {
                    return self.clone();
                };
                if kind == Type::TIMESTAMPTZ {
                    format!(
                        "TIMESTAMPTZ '{}+00:00'",
                        date.format("%Y-%m-%d %H:%M:%S%.6f")
                    )
                } else {
                    format!("TIMESTAMP '{}'", date.format("%Y-%m-%d %H:%M:%S%.6f"))
                }
            }
            _ => return self.clone(),
        };
        let column = ident(column);
        // Keep negative infinity visible to the existing temporal decoder.
        self.restricted(format!("({column} >= {bound} OR {column} = '-infinity')"))
    }

    pub(crate) async fn pushdown(
        &mut self,
        keys: &[String],
        operation: &crate::engine::sql_read::Pushdown,
    ) -> Result<Option<RecordBatch>> {
        use crate::engine::sql_read::{Aggregate, Native};
        let integer = |name: &str| {
            self.schema
                .field_with_name(name)
                .is_ok_and(|field| field.data_type().is_integer())
        };
        if !keys.iter().all(|key| integer(key)) || !self.guarantees_unique_key(keys).await? {
            return Ok(None);
        }
        let Some(mut terms) = operation
            .filters
            .iter()
            .map(|filter| {
                crate::engine::read_filters::sql_term(filter, &self.schema, ident).or_else(|| {
                    let value = filter.value.as_str()?;
                    let ty = &self.types[self.schema.index_of(&filter.column).ok()?];
                    if !matches!(*ty, Type::TEXT | Type::VARCHAR)
                        || filter.op != crate::Comparison::Eq
                        || value.contains('\0')
                    {
                        return None;
                    }
                    Some(text_equality(&ident(&filter.column), &text_literal(value)))
                })
            })
            .collect::<Option<Vec<_>>>()
        else {
            return Ok(None);
        };
        terms.extend(keys.iter().map(|key| format!("{} IS NOT NULL", ident(key))));
        let condition = terms.join(" AND ");
        match &operation.operation {
            Native::Filter => {
                *self = self.restricted(condition);
                Ok(None)
            }
            Native::Top { order, limit } => {
                let Ok(limit) = usize::try_from(*limit) else {
                    return Ok(None);
                };
                let order = order
                    .iter()
                    .map(|(key, asc)| {
                        format!("{} {}", ident(key), if *asc { "ASC" } else { "DESC" })
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                *self = self.restricted(condition);
                self.top = Some((order, limit));
                Ok(None)
            }
            Native::Aggregate { function, column } => {
                let (function, data_type) = match function {
                    Aggregate::Count => ("COUNT", DataType::Int64),
                    Aggregate::Min | Aggregate::Max if column.as_deref().is_some_and(integer) => (
                        if matches!(function, Aggregate::Min) {
                            "MIN"
                        } else {
                            "MAX"
                        },
                        self.schema
                            .field_with_name(column.as_deref().unwrap())?
                            .data_type()
                            .clone(),
                    ),
                    _ => return Ok(None),
                };
                let input = column.as_deref().map(ident).unwrap_or_else(|| "*".into());
                self.snapshot.stats.request();
                let row = self
                    .snapshot
                    .request(self.snapshot.client.query_one(
                        &format!(
                            "SELECT {function}({input}) FROM {} WHERE {condition}",
                            self.relation
                        ),
                        &[],
                    ))
                    .await?;
                let schema = Arc::new(Schema::new(vec![Field::new("aggregate", data_type, true)]));
                let batch = to_batch(schema, &[row])?;
                self.snapshot.stats.batch(&batch);
                Ok(Some(batch))
            }
        }
    }
    #[cfg(feature = "admin")]
    pub(crate) async fn probe(&self) -> Result<()> {
        let columns = self
            .schema
            .fields()
            .iter()
            .map(|field| ident(field.name()))
            .collect::<Vec<_>>()
            .join(", ");
        self.snapshot.stats.request();
        self.snapshot
            .request(self.snapshot.client.query(
                &format!("SELECT {columns} FROM {} LIMIT 0", self.relation),
                &[],
            ))
            .await?;
        Ok(())
    }

    #[cfg(feature = "admin")]
    pub(crate) fn exploration_snapshot(&self) -> Option<&str> {
        self.snapshot.exploration_snapshot.as_deref()
    }

    pub(crate) fn relation(&self) -> &str {
        &self.relation
    }

    pub(crate) fn shares_snapshot(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.snapshot, &other.snapshot)
    }

    pub(crate) fn restricted(&self, filter: String) -> Self {
        let mut table = self.clone();
        table.scan_filter = Some(match &self.scan_filter {
            Some(prior) => format!("({prior}) AND ({filter})"),
            None => filter,
        });
        table
    }

    pub(crate) fn filter_sql(&self, expr: &Expr) -> Option<String> {
        predicate(expr, &self.schema, &self.types)
    }

    pub(crate) fn key_sql(&self, alias: Option<&str>, name: &str) -> Option<String> {
        let name_sql = alias.map_or_else(
            || ident(name),
            |alias| crate::catalog::qualified(alias, name),
        );
        key_operand(&name_sql, &self.types[self.schema.index_of(name).ok()?])
    }

    pub(crate) fn column_type(&self, name: &str) -> Option<&Type> {
        self.types.get(self.schema.index_of(name).ok()?)
    }

    pub(crate) async fn key_collations(&self) -> Result<BTreeMap<String, String>> {
        if !self
            .types
            .iter()
            .any(|ty| matches!(*ty, Type::TEXT | Type::VARCHAR))
        {
            return Ok(BTreeMap::new());
        }
        self.snapshot.stats.request();
        let rows = self
            .snapshot
            .request(self.snapshot.client.query(
                "SELECT a.attname::text, n.nspname::text, c.collname::text
             FROM pg_catalog.pg_attribute a
             JOIN pg_catalog.pg_collation c ON c.oid = a.attcollation
             JOIN pg_catalog.pg_namespace n ON n.oid = c.collnamespace
             WHERE a.attrelid = $1::text::regclass AND a.attnum > 0 AND NOT a.attisdropped",
                &[&self.relation],
            ))
            .await?;
        rows.iter()
            .map(|row| {
                let name: &str = row.try_get(0)?;
                Ok((
                    name.into(),
                    format!("{}.{}", ident(row.try_get(1)?), ident(row.try_get(2)?)),
                ))
            })
            .collect()
    }

    pub(crate) async fn verify_local_snapshot(&self) -> Result<()> {
        ensure!(
            self.is_local_snapshot().await?,
            "query requires one consistent snapshot, but PostgreSQL relation {} may read outside its transaction snapshot",
            self.relation
        );
        Ok(())
    }

    pub(crate) async fn is_local_snapshot(&self) -> Result<bool> {
        self.snapshot.relation_is_local(&self.relation, false).await
    }

    pub(crate) async fn guarantees_unique_key(&self, keys: &[String]) -> Result<bool> {
        self.snapshot.stats.request();
        Ok(self
            .snapshot
            .request(self.snapshot.client.query_one(
                "SELECT EXISTS (
                SELECT FROM pg_catalog.pg_index AS idx
                JOIN pg_catalog.pg_class AS tbl ON tbl.oid = idx.indrelid
                WHERE idx.indrelid = $1::text::regclass AND idx.indisunique AND idx.indisvalid
                    AND idx.indpred IS NULL AND idx.indexprs IS NULL
                    AND (tbl.relkind = 'p' OR NOT tbl.relhassubclass)
                    AND NOT EXISTS (
                        SELECT FROM unnest(idx.indkey) WITH ORDINALITY AS k(attnum, ord)
                        JOIN pg_catalog.pg_attribute AS a
                            ON a.attrelid = idx.indrelid AND a.attnum = k.attnum
                        WHERE k.ord <= idx.indnkeyatts AND a.attname::text <> ALL($2::text[])
                    )
            )",
                &[&self.relation, &keys],
            ))
            .await?
            .try_get(0)?)
    }
}

#[async_trait]
impl TableProvider for PgTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::common::Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|expr| {
                if predicate(expr, &self.schema, &self.types).is_some() {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        _: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let schema = match projection {
            Some(indices) => Arc::new(self.schema.project(indices)?),
            None => Arc::clone(&self.schema),
        };
        let limit = match (&self.top, limit) {
            (Some((_, top)), limit) => Some(limit.map_or(*top, |limit| limit.min(*top))),
            (None, limit) => limit,
        };
        let partition = Arc::new(PgPartition {
            snapshot: Arc::clone(&self.snapshot),
            relation: self.relation.clone(),
            schema: Arc::clone(&schema),
            limit,
            order: self
                .top
                .as_ref()
                .and_then(|(order, _)| (!order.is_empty()).then(|| order.clone())),
            filter: filters
                .iter()
                .map(|expr| {
                    predicate(expr, &self.schema, &self.types).ok_or_else(|| {
                        DataFusionError::Internal("unsupported PostgreSQL filter".into())
                    })
                })
                .chain(self.scan_filter.iter().map(|sql| Ok(format!("({sql})"))))
                .collect::<datafusion::common::Result<Vec<_>>>()?
                .join(" AND "),
        });
        Ok(Arc::new(StreamingTableExec::try_new(
            schema,
            vec![partition],
            None,
            vec![],
            false,
            limit,
        )?))
    }
}

#[derive(Debug)]
struct PgPartition {
    snapshot: Arc<Snapshot>,
    relation: String,
    schema: SchemaRef,
    limit: Option<usize>,
    order: Option<String>,
    filter: String,
}

impl PartitionStream for PgPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let snapshot = Arc::clone(&self.snapshot);
        let schema = Arc::clone(&self.schema);
        let relation = self.relation.clone();
        let limit = self.limit;
        let order = self
            .order
            .as_ref()
            .map(|order| format!(" ORDER BY {order}"))
            .unwrap_or_default();
        let filter = if self.filter.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", self.filter)
        };
        let batch_size = ctx.session_config().batch_size();
        let stream = async_stream::try_stream! {
            let cursor = format!("graphlake_{}", snapshot.next_cursor.fetch_add(1, Ordering::Relaxed));
            let mut columns = schema.fields().iter().map(|field| {
                let name = ident(field.name());
                match field.data_type() {
                    DataType::Decimal128(..) => format!("{name}::text"),
                    _ => name,
                }
            }).collect::<Vec<_>>().join(", ");
            if columns.is_empty() {
                columns = "1".into();
            }
            let limit_sql = limit.map(|value| format!(" LIMIT {value}")).unwrap_or_default();
            snapshot.stats.request();
            snapshot.request(snapshot.client.batch_execute(&format!(
                "DECLARE {cursor} NO SCROLL CURSOR FOR SELECT {columns} FROM {relation}{filter}{order}{limit_sql}"
            ))).await.map_err(external)?;
            // A Bolt transaction can keep the PostgreSQL snapshot alive after
            // DISCARD, LIMIT or a dropped result stream.
            let mut close = CursorCleanup { snapshot: Some(snapshot.clone()), name: cursor.clone() };
            let mut remaining = limit.unwrap_or(usize::MAX);
            // Keep the last page's buffers reserved while consumers build
            // their state. Otherwise a spillable aggregate can fill the pool
            // before the connector has room to fetch the page that triggers a spill.
            let raw = MemoryConsumer::new("GraphLake PostgreSQL fetch")
                .register(&ctx.runtime_env().memory_pool);
            let output = MemoryConsumer::new("GraphLake PostgreSQL batch")
                .register(&ctx.runtime_env().memory_pool);
            while remaining > 0 {
                let size = remaining.min(batch_size);
                snapshot.stats.request();
                let statement = format!("FETCH FORWARD {size} FROM {cursor}");
                let mut request = snapshot.active_request();
                let result = snapshot.client.query_raw(statement.as_str(), std::iter::empty::<&str>())
                    .await.map_err(external)?;
                futures::pin_mut!(result);
                let mut rows = Vec::new();
                let mut raw_bytes = 0usize;
                while let Some(row) = result.try_next().await.map_err(external)? {
                    raw_bytes = raw_bytes.saturating_add(row.raw_size_bytes().saturating_add(std::mem::size_of::<Row>()));
                    if raw_bytes > raw.size() {
                        raw.try_resize(raw_bytes)?;
                    }
                    rows.push(row);
                }
                request.finish();
                if rows.is_empty() {
                    break;
                }
                let batch = to_batch(Arc::clone(&schema), &rows).map_err(external)?;
                let bytes = batch.get_array_memory_size();
                if bytes > output.size() {
                    output.try_resize(bytes)?;
                }
                snapshot.stats.batch(&batch);
                let n = rows.len();
                drop(rows);
                yield batch;
                if n < size {
                    break;
                }
                remaining -= n;
            }
            snapshot.stats.request();
            snapshot.request(snapshot.client.batch_execute(&format!("CLOSE {cursor}"))).await.map_err(external)?;
            close.snapshot = None;
        };
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        ))
    }
}

struct CursorCleanup {
    snapshot: Option<Arc<Snapshot>>,
    name: String,
}

impl Drop for CursorCleanup {
    fn drop(&mut self) {
        let Some(snapshot) = self.snapshot.take() else {
            return;
        };
        let Ok(runtime) = Handle::try_current() else {
            return;
        };
        let shared = Arc::strong_count(&snapshot) > 1;
        if !shared {
            // We are the last owner: drop immediately so Snapshot::drop sends cancel_query
            // (if interrupted) without a 2-second CLOSE delay behind an uncancelled FETCH.
            drop(snapshot);
            return;
        }
        let name = std::mem::take(&mut self.name);
        let interrupted = snapshot.interrupted.swap(false, Ordering::AcqRel);
        let cancel = interrupted.then(|| (snapshot.client.cancel_token(), snapshot.tls.clone()));
        let weak = Arc::downgrade(&snapshot);
        drop(snapshot);
        runtime.spawn(async move {
            if let Some((token, tls)) = cancel {
                let _ = tokio::time::timeout(Duration::from_secs(2), token.cancel_query(tls)).await;
            }
            if let Some(snapshot) = weak.upgrade() {
                let _ = tokio::time::timeout(
                    Duration::from_secs(2),
                    snapshot.request(snapshot.client.batch_execute(&format!("CLOSE {name}"))),
                )
                .await;
            }
        });
    }
}

// PostgreSQL and Arrow share the ordering and NULL semantics of these scalar types.
fn key_operand(name: &str, ty: &Type) -> Option<String> {
    match *ty {
        Type::BOOL
        | Type::INT2
        | Type::INT4
        | Type::INT8
        | Type::DATE
        | Type::TIMESTAMP
        | Type::TIMESTAMPTZ
        | Type::NUMERIC => Some(name.to_owned()),
        Type::TEXT | Type::VARCHAR => Some(format!("({name} COLLATE \"C\")")),
        Type::UUID => Some(format!("({name}::text COLLATE \"C\")")),
        _ => None,
    }
}

fn format_decimal(value: i128, scale: i8) -> Option<String> {
    if scale < 0 {
        return None;
    }
    if scale == 0 {
        return Some(value.to_string());
    }
    let negative = value < 0;
    let digits = value.unsigned_abs().to_string();
    let scale = scale as usize;
    let prefix = if negative { "-" } else { "" };
    if digits.len() <= scale {
        Some(format!(
            "{prefix}0.{}{digits}",
            "0".repeat(scale - digits.len())
        ))
    } else {
        let split = digits.len() - scale;
        Some(format!("{prefix}{}.{}", &digits[..split], &digits[split..]))
    }
}

fn text_literal(value: &str) -> String {
    format!("E'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
}

fn text_equality(name: &str, literal: &str) -> String {
    format!("({name} = {literal} AND ({name} COLLATE \"C\") = {literal})")
}

fn predicate(expr: &Expr, schema: &Schema, types: &[Type]) -> Option<String> {
    let column = |expr: &Expr| match expr {
        Expr::Column(column) => schema.field_with_name(&column.name).ok(),
        _ => None,
    };
    let operand = |expr: &Expr| {
        if let Some(field) = column(expr) {
            let name = ident(field.name());
            return key_operand(&name, &types[schema.index_of(field.name()).ok()?]);
        }
        match expr {
            Expr::Literal(ScalarValue::Int16(Some(value)), _) => Some(value.to_string()),
            Expr::Literal(ScalarValue::Int32(Some(value)), _) => Some(value.to_string()),
            Expr::Literal(ScalarValue::Int64(Some(value)), _) => Some(value.to_string()),
            Expr::Literal(ScalarValue::Boolean(Some(value)), _) => Some(value.to_string()),
            Expr::Literal(ScalarValue::Date32(Some(days)), _) => {
                Some(format!("(DATE '1970-01-01' + {days})"))
            }
            Expr::Literal(ScalarValue::TimestampMicrosecond(Some(value), tz), _) => {
                let date = DateTime::from_timestamp_micros(*value)
                    .filter(|date| (1..=9999).contains(&date.year()))?;
                if tz.is_some() {
                    Some(format!(
                        "TIMESTAMPTZ '{}+00:00'",
                        date.format("%Y-%m-%d %H:%M:%S%.6f")
                    ))
                } else {
                    Some(format!(
                        "TIMESTAMP '{}'",
                        date.format("%Y-%m-%d %H:%M:%S%.6f")
                    ))
                }
            }
            Expr::Literal(ScalarValue::Decimal128(Some(value), _, scale), _) => {
                format_decimal(*value, *scale).map(|lit| format!("{lit}::numeric"))
            }
            Expr::Literal(
                ScalarValue::Utf8(Some(value))
                | ScalarValue::Utf8View(Some(value))
                | ScalarValue::LargeUtf8(Some(value)),
                _,
            ) if !value.contains('\0') => Some(text_literal(value)),
            _ => None,
        }
    };
    match expr {
        Expr::Column(_) if types[schema.index_of(column(expr)?.name()).ok()?] == Type::BOOL => {
            operand(expr)
        }
        Expr::Literal(ScalarValue::Boolean(Some(value)), _) => {
            Some(if *value { "TRUE" } else { "FALSE" }.into())
        }
        Expr::IsNull(value) => Some(format!("{} IS NULL", ident(column(value)?.name()))),
        Expr::IsNotNull(value) => Some(format!("{} IS NOT NULL", ident(column(value)?.name()))),
        Expr::InList(list) if !list.negated && !list.list.is_empty() => {
            let field = operand(&list.expr)?;
            let values = list.list.iter().map(operand).collect::<Option<Vec<_>>>()?;
            let exact = format!("({field} IN ({}))", values.join(", "));
            if let Some(column) = column(&list.expr)
                && let Some(strings) = list
                    .list
                    .iter()
                    .map(|item| match item {
                        Expr::Literal(
                            ScalarValue::Utf8(Some(value))
                            | ScalarValue::Utf8View(Some(value))
                            | ScalarValue::LargeUtf8(Some(value)),
                            _,
                        ) => Some(value),
                        _ => None,
                    })
                    .collect::<Option<Vec<_>>>()
            {
                let name = ident(column.name());
                match types[schema.index_of(column.name()).ok()?] {
                    Type::TEXT | Type::VARCHAR => {
                        return Some(format!("({name} IN ({}) AND {exact})", values.join(", ")));
                    }
                    Type::UUID
                        if strings.iter().all(|value| {
                            uuid::Uuid::parse_str(value).is_ok_and(|id| id.to_string() == **value)
                        }) =>
                    {
                        let values = values
                            .iter()
                            .map(|value| format!("{value}::uuid"))
                            .collect::<Vec<_>>();
                        return Some(format!("({name} IN ({}))", values.join(", ")));
                    }
                    _ => {}
                }
            }
            Some(exact)
        }
        Expr::BinaryExpr(binary) => {
            let op = binary.op;
            if op == Operator::Eq {
                let operands = [(&binary.left, &binary.right), (&binary.right, &binary.left)];
                for (field, literal) in operands {
                    let (
                        Some(field),
                        Expr::Literal(
                            ScalarValue::Utf8(Some(value))
                            | ScalarValue::Utf8View(Some(value))
                            | ScalarValue::LargeUtf8(Some(value)),
                            _,
                        ),
                    ) = (column(field), literal.as_ref())
                    else {
                        continue;
                    };
                    let name = ident(field.name());
                    let lit = operand(literal)?;
                    match types[schema.index_of(field.name()).ok()?] {
                        Type::UUID
                            if uuid::Uuid::parse_str(value)
                                .is_ok_and(|uuid| uuid.to_string() == *value) =>
                        {
                            return Some(format!("({name} = {lit}::uuid)"));
                        }
                        Type::TEXT | Type::VARCHAR => {
                            return Some(text_equality(&name, &lit));
                        }
                        _ => {}
                    }
                }
            }
            let (left, right) = match op {
                Operator::And | Operator::Or => (
                    predicate(&binary.left, schema, types)?,
                    predicate(&binary.right, schema, types)?,
                ),
                Operator::Eq
                | Operator::NotEq
                | Operator::Lt
                | Operator::LtEq
                | Operator::Gt
                | Operator::GtEq => (operand(&binary.left)?, operand(&binary.right)?),
                _ => return None,
            };
            Some(format!("({left} {op} {right})"))
        }
        _ => None,
    }
}

fn external(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> DataFusionError {
    DataFusionError::External(error.into())
}

fn field(col: &Column) -> Result<Field> {
    let ty = match *col.type_() {
        Type::BOOL => DataType::Boolean,
        Type::INT2 => DataType::Int16,
        Type::INT4 => DataType::Int32,
        Type::INT8 => DataType::Int64,
        Type::FLOAT4 => DataType::Float32,
        Type::FLOAT8 => DataType::Float64,
        Type::TEXT | Type::VARCHAR | Type::UUID => DataType::Utf8,
        Type::BYTEA => DataType::Binary,
        Type::DATE => DataType::Date32,
        Type::TIMESTAMP => DataType::Timestamp(TimeUnit::Microsecond, None),
        Type::TIMESTAMPTZ => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        Type::NUMERIC => {
            let typmod = col.type_modifier();
            ensure!(
                typmod >= 4,
                "PostgreSQL NUMERIC column {} requires declared precision and scale; for unrestricted values expose the column as text in a PostgreSQL view",
                col.name()
            );
            let precision = (typmod - 4) >> 16;
            let scale = (typmod - 4) & 0x7ff;
            ensure!(
                (1..=38).contains(&precision) && scale <= precision,
                "PostgreSQL NUMERIC column {} requires 0 <= scale <= precision <= 38",
                col.name()
            );
            DataType::Decimal128(precision as u8, scale as i8)
        }
        ref ty => bail!("unsupported PostgreSQL type {ty} for column {}", col.name()),
    };
    Ok(Field::new(col.name(), ty, true))
}

struct Temporal<T>(T);

impl<'a, T: FromSql<'a>> FromSql<'a> for Temporal<T> {
    fn accepts(ty: &Type) -> bool {
        T::accepts(ty)
    }

    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        // PostgreSQL uses reserved signed integer bounds for temporal infinities.
        let special = if *ty == Type::DATE {
            raw.try_into()
                .ok()
                .and_then(|bytes| match i32::from_be_bytes(bytes) {
                    i32::MAX => Some("infinity"),
                    i32::MIN => Some("-infinity"),
                    _ => None,
                })
        } else {
            raw.try_into()
                .ok()
                .and_then(|bytes| match i64::from_be_bytes(bytes) {
                    i64::MAX => Some("infinity"),
                    i64::MIN => Some("-infinity"),
                    _ => None,
                })
        };
        if let Some(value) = special {
            return Err(anyhow!("PostgreSQL {ty} {value} has no Arrow temporal representation; expose the column as text in a PostgreSQL view").into());
        }
        T::from_sql(ty, raw).map(Self)
    }
}

fn to_batch(schema: SchemaRef, rows: &[Row]) -> Result<RecordBatch> {
    let arrays = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| -> Result<ArrayRef> {
            macro_rules! collect {
                ($ty:ty, $array:ty) => {
                    Arc::new(<$array>::from(
                        rows.iter()
                            .map(|row| row.try_get::<_, Option<$ty>>(i))
                            .collect::<Result<Vec<_>, _>>()?,
                    ))
                };
            }
            let array: ArrayRef = match f.data_type() {
                DataType::Boolean => collect!(bool, BooleanArray),
                DataType::Int16 => collect!(i16, Int16Array),
                DataType::Int32 => collect!(i32, Int32Array),
                DataType::Int64 => collect!(i64, Int64Array),
                DataType::Float32 => collect!(f32, Float32Array),
                DataType::Float64 => collect!(f64, Float64Array),
                DataType::Utf8 if rows[0].columns()[i].type_() == &Type::UUID => {
                    Arc::new(StringArray::from(
                        rows.iter()
                            .map(|row| {
                                row.try_get::<_, Option<uuid::Uuid>>(i)
                                    .map(|v| v.map(|v| v.to_string()))
                            })
                            .collect::<Result<Vec<_>, _>>()?,
                    ))
                }
                DataType::Utf8 => collect!(&str, StringArray),
                DataType::Binary => collect!(&[u8], BinaryArray),
                DataType::Date32 => {
                    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("Unix epoch");
                    Arc::new(Date32Array::from(
                        rows.iter()
                            .map(|row| {
                                row.try_get::<_, Option<Temporal<NaiveDate>>>(i)
                                    .map(|v| v.map(|v| (v.0 - epoch).num_days() as i32))
                                    .with_context(|| format!("decoding PostgreSQL column {}", f.name()))
                            })
                            .collect::<Result<Vec<_>, _>>()?,
                    ))
                }
                DataType::Timestamp(_, timezone) => {
                    let values = rows
                        .iter()
                        .map(|row| match timezone {
                            Some(_) => row
                                .try_get::<_, Option<Temporal<DateTime<Utc>>>>(i)
                                .map(|v| v.map(|v| v.0.timestamp_micros())),
                            None => row
                                .try_get::<_, Option<Temporal<NaiveDateTime>>>(i)
                                .map(|v| v.map(|v| v.0.and_utc().timestamp_micros())),
                        }.with_context(|| format!("decoding PostgreSQL column {}", f.name())))
                        .collect::<Result<Vec<_>, _>>()?;
                    Arc::new(
                        TimestampMicrosecondArray::from(values).with_timezone_opt(timezone.clone()),
                    )
                }
                DataType::Decimal128(precision, scale) => {
                    let values = rows
                        .iter()
                        .map(|row| {
                            row.try_get::<_, Option<&str>>(i)?
                                .map(|v| {
                                    ensure!(v != "NaN",
                                        "PostgreSQL NUMERIC column {} contains NaN, which Decimal128 cannot represent; expose the column as text in a PostgreSQL view",
                                        f.name()
                                    );
                                    decimal128(v, *scale)
                                })
                                .transpose()
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let array = Decimal128Array::from(values)
                        .with_precision_and_scale(*precision, *scale)?;
                    array.validate_decimal_precision(*precision)?;
                    Arc::new(array)
                }
                ty => bail!("unsupported PostgreSQL Arrow type {ty}"),
            };
            Ok(array)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new_with_options(
        schema,
        arrays,
        &RecordBatchOptions::new().with_row_count(Some(rows.len())),
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::{col, lit};

    #[test]
    fn predicate_pushes_down_date_timestamp_and_decimal() {
        let schema = Schema::new(vec![
            Field::new("d", DataType::Date32, true),
            Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new(
                "tz",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
            Field::new("amt", DataType::Decimal128(10, 2), true),
        ]);
        let types = [
            Type::DATE,
            Type::TIMESTAMP,
            Type::TIMESTAMPTZ,
            Type::NUMERIC,
        ];

        let date_sql = predicate(
            &col("d").gt_eq(lit(ScalarValue::Date32(Some(19723)))),
            &schema,
            &types,
        )
        .unwrap();
        assert!(date_sql.contains("DATE '1970-01-01' + 19723"), "{date_sql}");

        let ts_sql = predicate(
            &col("ts").lt(lit(ScalarValue::TimestampMicrosecond(
                Some(1_704_067_200_000_000),
                None,
            ))),
            &schema,
            &types,
        )
        .unwrap();
        assert!(
            ts_sql.contains("TIMESTAMP '2024-01-01 00:00:00.000000'"),
            "{ts_sql}"
        );

        let tz_sql = predicate(
            &col("tz").eq(lit(ScalarValue::TimestampMicrosecond(
                Some(1_704_067_200_000_000),
                Some("UTC".into()),
            ))),
            &schema,
            &types,
        )
        .unwrap();
        assert!(
            tz_sql.contains("TIMESTAMPTZ '2024-01-01 00:00:00.000000+00:00'"),
            "{tz_sql}"
        );

        let dec_sql = predicate(
            &col("amt").eq(lit(ScalarValue::Decimal128(Some(-5), 10, 2))),
            &schema,
            &types,
        )
        .unwrap();
        assert!(dec_sql.contains("-0.05::numeric"), "{dec_sql}");
    }
}
