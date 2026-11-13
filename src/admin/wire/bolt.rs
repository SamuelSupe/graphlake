use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use boltr::{
    error::BoltError,
    server::{
        AuthCredentials, AuthInfo, AuthValidator, BoltBackend, BoltRecord, BoltServer,
        RecordStream, ResultMetadata, ResultStream, SessionConfig, SessionHandle, SessionProperty,
        TransactionHandle,
    },
    types::{BoltDict, BoltValue},
};
use serde_json::{Value, json};
use tokio::net::TcpListener;

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
    principal: Option<Principal>,
    model: String,
    transaction: Option<Arc<Transaction>>,
}

struct Transaction {
    handle: TransactionHandle,
    model: String,
    revision: String,
    engine: Engine,
    snapshots: ReadTransaction,
    deadline: Instant,
}

impl Session {
    fn target(&self) -> Result<Target, BoltError> {
        let principal = self
            .principal
            .clone()
            .ok_or_else(|| BoltError::Authentication("Missing authentication".into()))?;
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
    async fn validate(&self, input: &AuthCredentials) -> Result<AuthInfo, BoltError> {
        let name = input.principal.as_deref().unwrap_or_default();
        let principal = (input.scheme == "basic")
            .then(|| {
                self.app.access.authenticate(
                    name,
                    input.credentials.as_deref().unwrap_or_default(),
                    false,
                )
            })
            .flatten();
        self.app.access.record(
            principal.as_ref(),
            "bolt",
            "login",
            None,
            if principal.is_some() { "ok" } else { "error" },
            if principal.is_some() { 200 } else { 401 },
        );
        if principal.is_none() {
            return Err(BoltError::Authentication(
                "Invalid driver credentials".into(),
            ));
        }
        Ok(AuthInfo {
            principal: name.into(),
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
                principal: None,
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
        let principal = Principal::Service(auth.principal);
        session.model = self.default_model(Some(&principal));
        session.principal = Some(principal);
        Ok(())
    }

    async fn close_session(&self, handle: &SessionHandle) -> Result<(), BoltError> {
        let session = self.sessions.lock().unwrap().remove(handle);
        if let Some(session) = session
            && session.transaction.is_some()
        {
            session
                .target()?
                .audit(&self.app, "rollback", "abandoned", 499);
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
        let target = session.target()?;
        if session
            .transaction
            .as_ref()
            .is_some_and(|tx| tx.model != model)
        {
            return Err(transaction_error());
        }
        if !self.app.access.permits(&target.principal, &model) {
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
        session.model = self.default_model(session.principal.as_ref());
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
            (session.target()?, transaction)
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
            pinned
                .as_ref()
                .map(|tx| (tx.revision.as_str(), &tx.engine, &tx.snapshots)),
        )
        .await
        .map_err(query_error)?;
        Ok(ResultStream {
            metadata: ResultMetadata {
                columns: result.columns.clone(),
                extra: BoltDict::new(),
            },
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
        let mut sessions = self.sessions.lock().unwrap();
        let session = sessions.get_mut(handle).ok_or_else(closed)?;
        let target = session.target()?;
        let (revision, engine) = target
            .engine(&self.app)
            .inspect_err(|error| target.audit(&self.app, "begin", "error", error.status.as_u16()))
            .map_err(query_error)?;
        let handle = TransactionHandle(uuid::Uuid::new_v4().to_string());
        session.transaction = Some(Arc::new(Transaction {
            handle: handle.clone(),
            model: target.model.clone(),
            revision,
            engine,
            snapshots: ReadTransaction::default(),
            deadline,
        }));
        target.audit(&self.app, "begin", "ok", 200);
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
            .and_then(|principal| self.app.access.default_model(principal, configured))
            .unwrap_or_else(|| configured.clone())
    }

    fn end(
        &self,
        handle: &SessionHandle,
        transaction: &TransactionHandle,
        commit: bool,
    ) -> Result<(), BoltError> {
        let mut sessions = self.sessions.lock().unwrap();
        let session = sessions.get_mut(handle).ok_or_else(closed)?;
        let tx = session
            .transaction
            .take()
            .filter(|tx| &tx.handle == transaction)
            .ok_or_else(transaction_error)?;
        let target = session.target()?;
        if commit && Instant::now() >= tx.deadline {
            target.audit(&self.app, "commit", "error", 504);
            return Err(query_error(Error::new(
                axum::http::StatusCode::GATEWAY_TIMEOUT,
                "timeout",
                "Read transaction expired",
            )));
        }
        target.audit(
            &self.app,
            if commit { "commit" } else { "rollback" },
            "ok",
            200,
        );
        Ok(())
    }
}

#[async_trait::async_trait]
impl RecordStream for Rows {
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
