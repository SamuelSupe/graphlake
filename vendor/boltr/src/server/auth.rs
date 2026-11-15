//! Authentication validator trait for Bolt servers.

use crate::error::BoltError;
use crate::server::AuthCredentials;

/// Authenticated identity returned by the validator.
#[derive(Debug, Clone, Default)]
pub struct AuthInfo {
    /// Authenticated principal (user/token identifier).
    pub principal: String,
    /// If true, the client should change credentials before proceeding.
    pub credentials_expired: bool,
    /// Close the connection when the validator drops the credential grant.
    pub credential: Option<tokio::sync::watch::Receiver<()>>,
}

/// Validates authentication credentials during the LOGON phase.
#[async_trait::async_trait]
pub trait AuthValidator: Send + Sync + 'static {
    /// Refresh externally managed grants before dispatching an authenticated message.
    fn refresh_credentials(&self) {}

    /// Check connection-wide authorization before dispatching an authenticated message.
    async fn session_allowed(&self) -> bool {
        true
    }

    /// Validate the given credentials.
    /// Return `Ok(AuthInfo)` with the authenticated identity, or `Err(BoltError)` to reject.
    async fn validate(&self, credentials: &AuthCredentials) -> Result<AuthInfo, BoltError>;
}
