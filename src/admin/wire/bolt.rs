use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use boltr::{
    error::BoltError,
    packstream::{decode_value, encode_value},
    server::{
        AuthCredentials, AuthInfo, AuthValidator, BoltBackend, BoltRecord, BoltServer,
        RecordStream, ResultMetadata, ResultStream, SessionConfig, SessionHandle, SessionProperty,
        TransactionHandle,
    },
    types::{BoltDict, BoltValue},
};
use serde_json::{Value, json};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter},
    net::TcpListener,
};

use super::super::{App, Error, access::Principal};
use super::{Rows, Target};
use crate::{Engine, Language, engine::ReadTransaction};

pub(in crate::admin) async fn serve(app: Arc<App>, listener: TcpListener) -> Result<(), BoltError> {
    let backend = Backend {
        app: app.clone(),
        sessions: Arc::default(),
    };
    BoltServer::builder(backend.clone())
        .auth(backend)
        .max_sessions(16)
        .max_pending_sessions(16)
        .max_pending_sessions_per_peer(8)
        .max_unauthenticated_message_size(4096)
        .max_message_size(256 * 1024)
        .handshake_timeout(Duration::from_secs(5))
        .idle_timeout(Duration::from_secs(60))
        .shutdown(app.shutdown.clone().cancelled_owned())
        .serve_listener(listener)
        .await
}

#[derive(Clone)]
struct Backend {
    app: Arc<App>,
    sessions: Arc<Mutex<HashMap<SessionHandle, Session>>>,
}

struct Session {
    credential: Option<super::super::access::Credential>,
    model: String,
    transaction: Option<Arc<Transaction>>,
}

struct Transaction {
    handle: TransactionHandle,
    model: String,
    revision: String,
    engine: Engine,
    snapshots: ReadTransaction,
    managed: Option<Arc<crate::admin::local_tables::managed::Selection>>,
    deadline: Instant,
}

impl Session {
    fn target(&self, app: &App) -> Result<Target, BoltError> {
        let credential = self
            .credential
            .as_ref()
            .ok_or_else(|| BoltError::Authentication("Missing authentication".into()))?;
        if app.shutdown.is_cancelled()
            || app
                .ha
                .as_ref()
                .is_some_and(|ha| ha.recovery_pending() || !ha.leader.eligible())
            || !app.access.refresh()
            || credential.live.has_changed().is_err()
        {
            return Err(BoltError::Authentication(
                "Driver credentials revoked".into(),
            ));
        }
        let principal = credential.principal.clone();
        Ok(Target {
            principal,
            model: self.model.clone(),
            protocol: "bolt",
        })
    }
}

fn closed() -> BoltError {
    BoltError::Session("Bolt session is closed".into())
}

fn transaction_error() -> BoltError {
    BoltError::Transaction("No matching read transaction".into())
}

fn timeout(extra: &BoltDict) -> Result<Option<Duration>, BoltError> {
    match extra.get("tx_timeout") {
        None => Ok(None),
        Some(BoltValue::Integer(ms)) if *ms > 0 => Ok(Some(Duration::from_millis(*ms as u64))),
        _ => Err(BoltError::Protocol("Invalid tx_timeout".into())),
    }
}

fn mode(extra: &BoltDict) -> Result<(), BoltError> {
    match extra.get("mode") {
        // Existing drivers default to write routing even for statements that only read.
        None => Ok(()),
        Some(BoltValue::String(value)) if matches!(value.as_str(), "r" | "w") => Ok(()),
        _ => Err(BoltError::Protocol(
            "Invalid transaction access mode".into(),
        )),
    }
}

#[async_trait::async_trait]
impl AuthValidator for Backend {
    async fn session_allowed(&self) -> bool {
        match &self.app.ha {
            Some(ha) => !ha.recovery_pending() && ha.leader.eligible(),
            None => !self.app.shutdown.is_cancelled(),
        }
    }

    fn refresh_credentials(&self) {
        self.app.access.refresh();
    }

    async fn validate(&self, input: &AuthCredentials) -> Result<AuthInfo, BoltError> {
        if let Some(ha) = &self.app.ha {
            ha.leader.admission().map_err(query_error)?;
        }
        let name = input.principal.as_deref().unwrap_or_default();
        let credential = (input.scheme == "basic")
            .then(|| {
                super::super::access::authenticate(
                    &self.app,
                    name,
                    input.credentials.as_deref().unwrap_or_default(),
                    false,
                )
            })
            .flatten();
        self.app.access.record(
            credential.as_ref().map(|credential| &credential.principal),
            "bolt",
            "login",
            None,
            if credential.is_some() { "ok" } else { "error" },
            if credential.is_some() { 200 } else { 401 },
        );
        let credential = credential
            .ok_or_else(|| BoltError::Authentication("Invalid driver credentials".into()))?;
        Ok(AuthInfo {
            // The Bolt backend receives this identity out of band. Include the
            // authenticated grant version so LOGON can never pick up a newer grant.
            principal: serde_json::to_string(&credential.principal)
                .expect("serializable principal"),
            credential: Some(credential.live),
            ..Default::default()
        })
    }
}

#[async_trait::async_trait]
impl BoltBackend for Backend {
    async fn create_session(&self, _: &SessionConfig) -> Result<SessionHandle, BoltError> {
        let handle = SessionHandle(uuid::Uuid::new_v4().to_string());
        self.sessions.lock().unwrap().insert(
            handle.clone(),
            Session {
                credential: None,
                model: self
                    .app
                    .wire
                    .as_ref()
                    .expect("Bolt configured")
                    .model
                    .clone(),
                transaction: None,
            },
        );
        Ok(handle)
    }

    async fn set_session_auth(
        &self,
        handle: &SessionHandle,
        auth: AuthInfo,
    ) -> Result<(), BoltError> {
        let mut sessions = self.sessions.lock().unwrap();
        let session = sessions.get_mut(handle).ok_or_else(closed)?;
        let live = auth
            .credential
            .ok_or_else(|| BoltError::Authentication("Missing authentication".into()))?;
        let principal = serde_json::from_str(&auth.principal)
            .map_err(|_| BoltError::Authentication("Invalid authenticated identity".into()))?;
        session.model = self.default_model(Some(&principal));
        session.credential = Some(super::super::access::Credential { principal, live });
        Ok(())
    }

    async fn close_session(&self, handle: &SessionHandle) -> Result<(), BoltError> {
        let session = self.sessions.lock().unwrap().remove(handle);
        if let Some(session) = session
            && let Some(transaction) = session.transaction
            && let Some(credential) = session.credential
        {
            self.app.access.record(
                Some(&credential.principal),
                "bolt",
                "rollback",
                Some(&transaction.model),
                "abandoned",
                499,
            );
        }
        Ok(())
    }

    async fn configure_session(
        &self,
        handle: &SessionHandle,
        property: SessionProperty,
    ) -> Result<(), BoltError> {
        let SessionProperty::Database(model) = property;
        let mut sessions = self.sessions.lock().unwrap();
        let session = sessions.get_mut(handle).ok_or_else(closed)?;
        let target = session.target(&self.app)?;
        if session
            .transaction
            .as_ref()
            .is_some_and(|tx| tx.model != model)
        {
            return Err(transaction_error());
        }
        if !super::super::access::query_allowed(&self.app, &target.principal, &model) {
            self.app.access.record(
                Some(&target.principal),
                "bolt",
                "select_model",
                Some(&model),
                "error",
                403,
            );
            return Err(query_error(super::super::access::forbidden()));
        }
        session.model = model;
        Ok(())
    }

    async fn reset_session(&self, handle: &SessionHandle) -> Result<(), BoltError> {
        let mut sessions = self.sessions.lock().unwrap();
        let session = sessions.get_mut(handle).ok_or_else(closed)?;
        session.transaction = None;
        session.model = self.default_model(
            session
                .credential
                .as_ref()
                .map(|credential| &credential.principal),
        );
        Ok(())
    }

    async fn execute(
        &self,
        handle: &SessionHandle,
        query: &str,
        parameters: &BoltDict,
        extra: &BoltDict,
        transaction: Option<&TransactionHandle>,
    ) -> Result<ResultStream, BoltError> {
        let (target, pinned) = {
            let sessions = self.sessions.lock().unwrap();
            let session = sessions.get(handle).ok_or_else(closed)?;
            let transaction = match transaction {
                None => None,
                Some(handle) => Some(
                    session
                        .transaction
                        .clone()
                        .filter(|tx| &tx.handle == handle)
                        .ok_or_else(transaction_error)?,
                ),
            };
            (session.target(&self.app)?, transaction)
        };
        mode(extra)?;
        if extra.contains_key("imp_user") {
            return Err(query_error(super::super::access::forbidden()));
        }
        let params = parameters
            .iter()
            .map(|(key, value)| Ok((key.clone(), parameter(value, 0)?)))
            .collect::<Result<BTreeMap<_, _>, BoltError>>()?;
        let timeout = if let Some(tx) = &pinned {
            Some(tx.deadline.saturating_duration_since(Instant::now()))
        } else {
            timeout(extra)?
        };
        let result = super::execute(
            &self.app,
            target,
            Language::Cypher,
            query,
            params,
            timeout,
            pinned.as_ref().map(|tx| {
                (
                    tx.revision.as_str(),
                    &tx.engine,
                    &tx.snapshots,
                    tx.managed.as_ref(),
                )
            }),
        )
        .await
        .map_err(query_error)?;
        let metadata = ResultMetadata {
            columns: result.columns.clone(),
            extra: BoltDict::new(),
        };
        Ok(ResultStream {
            metadata,
            records: Box::new(result),
        })
    }

    async fn begin_transaction(
        &self,
        handle: &SessionHandle,
        extra: &BoltDict,
    ) -> Result<TransactionHandle, BoltError> {
        mode(extra)?;
        if extra.contains_key("imp_user") {
            return Err(query_error(super::super::access::forbidden()));
        }
        let deadline = Instant::now()
            + timeout(extra)?
                .unwrap_or(self.app.options.timeout)
                .min(self.app.options.timeout);
        let target = {
            let sessions = self.sessions.lock().unwrap();
            sessions.get(handle).ok_or_else(closed)?.target(&self.app)?
        };
        let (revision, mut engine) = target
            .engine(&self.app)
            .await
            .inspect_err(|error| target.audit(&self.app, "begin", "error", error.status.as_u16()))
            .map_err(query_error)?;
        let managed = crate::admin::local_tables::managed::resolve(
            &self.app,
            &mut engine,
            &target.model,
            deadline.saturating_duration_since(Instant::now()).as_secs() + 120,
        )
        .await
        .map_err(query_error)?
        .map(Arc::new);
        let mut sessions = self.sessions.lock().unwrap();
        let session = sessions.get_mut(handle).ok_or_else(closed)?;
        let handle = TransactionHandle(uuid::Uuid::new_v4().to_string());
        session.transaction = Some(Arc::new(Transaction {
            handle: handle.clone(),
            model: target.model.clone(),
            revision,
            engine,
            snapshots: ReadTransaction::default(),
            managed,
            deadline,
        }));
        drop(sessions);
        crate::blocking(|| target.audit(&self.app, "begin", "ok", 200));
        Ok(handle)
    }

    async fn commit(
        &self,
        handle: &SessionHandle,
        transaction: &TransactionHandle,
    ) -> Result<BoltDict, BoltError> {
        self.end(handle, transaction, true)?;
        Ok(BoltDict::new())
    }

    async fn rollback(
        &self,
        handle: &SessionHandle,
        transaction: &TransactionHandle,
    ) -> Result<(), BoltError> {
        self.end(handle, transaction, false)
    }

    async fn get_server_info(&self) -> Result<BoltDict, BoltError> {
        // Official Python drivers require a Neo4j-compatible server agent prefix.
        Ok(BoltDict::from([(
            "server".into(),
            BoltValue::String(format!("Neo4j/5.4 GraphLake/{}", env!("CARGO_PKG_VERSION"))),
        )]))
    }
}

impl Backend {
    fn default_model(&self, principal: Option<&Principal>) -> String {
        let configured = &self.app.wire.as_ref().expect("Bolt configured").model;
        principal
            .and_then(|principal| {
                super::super::access::default_model(&self.app, principal, configured)
            })
            .unwrap_or_else(|| configured.clone())
    }

    fn end(
        &self,
        handle: &SessionHandle,
        transaction: &TransactionHandle,
        commit: bool,
    ) -> Result<(), BoltError> {
        let (tx, target) = {
            let mut sessions = self.sessions.lock().unwrap();
            let session = sessions.get_mut(handle).ok_or_else(closed)?;
            if !session
                .transaction
                .as_ref()
                .is_some_and(|tx| &tx.handle == transaction)
            {
                return Err(transaction_error());
            }
            let target = session.target(&self.app)?;
            (
                session.transaction.take().ok_or_else(transaction_error)?,
                target,
            )
        };
        if commit && Instant::now() >= tx.deadline {
            crate::blocking(|| target.audit(&self.app, "commit", "error", 504));
            return Err(query_error(Error::new(
                axum::http::StatusCode::GATEWAY_TIMEOUT,
                "timeout",
                "Read transaction expired",
            )));
        }
        crate::blocking(|| {
            target.audit(
                &self.app,
                if commit { "commit" } else { "rollback" },
                "ok",
                200,
            )
        });
        Ok(())
    }
}

#[async_trait::async_trait]
impl RecordStream for Rows {
    async fn buffer_for_next_run(&mut self) -> Result<Option<Box<dyn RecordStream>>, BoltError> {
        let limit = self.app.options.spill_bytes.min(64 * 1024 * 1024);
        if limit == 0 || self.finished {
            return Ok(None);
        }
        // Only a second RUN needs to free the first stream's query slot.
        // The normal RUN/PULL sequence can stream directly from the engine.
        let deadline = self.deadline;
        let spool = tokio::time::timeout_at(
            deadline.into(),
            TransactionResult::capture(self, limit, deadline),
        )
        .await
        .map_err(|_| result_expired())??;
        Ok(Some(Box::new(spool)))
    }

    fn deadline(&self) -> Option<tokio::time::Instant> {
        Some(self.deadline.into())
    }

    async fn next(&mut self) -> Result<Option<BoltRecord>, BoltError> {
        Ok(Rows::next(self)
            .await
            .map_err(query_error)?
            .map(|values| BoltRecord { values }))
    }

    fn discard(&mut self) {
        self.finish("discarded", 200);
    }

    fn summary(&self) -> BoltDict {
        BoltDict::from([
            ("type".into(), BoltValue::String("r".into())),
            (
                "t_last".into(),
                BoltValue::Integer(self.result.metrics().elapsed_ms as i64),
            ),
            ("db".into(), BoltValue::String(self.target.model.clone())),
            (
                "model_revision".into(),
                BoltValue::String(self.revision.clone()),
            ),
        ])
    }
}

struct TransactionResult {
    data: BufReader<File>,
    _spill: tokio::sync::OwnedSemaphorePermit,
    rows: u64,
    summary: BoltDict,
    deadline: Instant,
}

fn result_expired() -> BoltError {
    query_error(Error::new(
        axum::http::StatusCode::GATEWAY_TIMEOUT,
        "timeout",
        "Read transaction expired",
    ))
}

impl TransactionResult {
    async fn capture(
        result: &mut Rows,
        mut remaining: u64,
        deadline: Instant,
    ) -> Result<Self, BoltError> {
        let spool = &result
            .app
            .wire
            .as_ref()
            .expect("Bolt configured")
            .bolt_spill;
        let spool = Arc::clone(spool);
        let mut spill = Arc::clone(&spool)
            .try_acquire_many_owned(0)
            .map_err(|_| query_error(super::result_limit()))?;
        let file = tokio::task::spawn_blocking(tempfile::tempfile)
            .await
            .map_err(|_| query_error(Error::storage()))??;
        let mut writer = BufWriter::new(File::from_std(file));
        let mut encoded = bytes::BytesMut::new();
        let mut rows = 0;
        while let Some(values) = result.next().await.map_err(query_error)? {
            encoded.clear();
            encode_value(&mut encoded, &BoltValue::List(values));
            let len = encoded.len() as u32;
            remaining = remaining
                .checked_sub(u64::from(len) + 4)
                .ok_or_else(|| query_error(super::result_limit()))?;
            spill.merge(
                Arc::clone(&spool)
                    .try_acquire_many_owned(len + 4)
                    .map_err(|_| query_error(super::result_limit()))?,
            );
            writer.write_u32(len).await?;
            writer.write_all(&encoded).await?;
            rows += 1;
        }
        writer.flush().await?;
        let mut data = writer.into_inner();
        data.rewind().await?;
        Ok(Self {
            data: BufReader::new(data),
            _spill: spill,
            rows,
            summary: result.summary(),
            deadline,
        })
    }
}

#[async_trait::async_trait]
impl RecordStream for TransactionResult {
    fn deadline(&self) -> Option<tokio::time::Instant> {
        Some(self.deadline.into())
    }

    async fn next(&mut self) -> Result<Option<BoltRecord>, BoltError> {
        if Instant::now() >= self.deadline {
            return Err(result_expired());
        }
        if self.rows == 0 {
            return Ok(None);
        }
        let len = self.data.read_u32().await? as usize;
        let mut bytes = vec![0; len];
        self.data.read_exact(&mut bytes).await?;
        let BoltValue::List(values) = decode_value(&mut bytes.as_slice())? else {
            return Err(BoltError::Protocol("Invalid buffered Bolt result".into()));
        };
        self.rows -= 1;
        Ok(Some(BoltRecord { values }))
    }

    fn discard(&mut self) {
        self.rows = 0;
    }

    fn summary(&self) -> BoltDict {
        self.summary.clone()
    }
}

fn query_error(error: Error) -> BoltError {
    let code = match error.code {
        "model_forbidden" => "Neo.ClientError.Security.Forbidden",
        "not_found" => "Neo.ClientError.Database.DatabaseNotFound",
        "timeout" => "Neo.TransientError.Transaction.TransactionTimedOut",
        "busy" | "draining" => "Neo.TransientError.General.DatabaseUnavailable",
        "result_limit" => "Neo.ClientError.Statement.ArgumentError",
        _ => "Neo.ClientError.Statement.ExecutionFailed",
    };
    BoltError::Query {
        code: code.into(),
        message: error.message,
    }
}

fn parameter(value: &BoltValue, depth: usize) -> Result<Value, BoltError> {
    let invalid = || {
        BoltError::Protocol(
            "Cypher parameters require finite scalars, lists or string-keyed maps".into(),
        )
    };
    if depth > 24 {
        return Err(invalid());
    }
    Ok(match value {
        BoltValue::Null => Value::Null,
        BoltValue::Boolean(value) => json!(value),
        BoltValue::Integer(value) => json!(value),
        BoltValue::Float(value) if value.is_finite() => json!(value),
        BoltValue::String(value) => json!(value),
        BoltValue::List(values) => Value::Array(
            values
                .iter()
                .map(|v| parameter(v, depth + 1))
                .collect::<Result<_, _>>()?,
        ),
        BoltValue::Dict(values) => Value::Object(
            values
                .iter()
                .map(|(k, v)| Ok((k.clone(), parameter(v, depth + 1)?)))
                .collect::<Result<_, BoltError>>()?,
        ),
        _ => return Err(invalid()),
    })
}
