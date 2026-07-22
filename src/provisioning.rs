//! Server-neutral fixture provisioning boundary.

use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FixtureSpec {
    pub name: String,
    pub participants: Vec<String>,
    pub groups: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FixtureHandle {
    pub id: String,
}

#[derive(Debug, Error)]
pub enum ProvisioningError {
    #[error("provisioning is unavailable: {0}")]
    Unavailable(String),
}

/// An optional boundary for server-specific tenant, identity, and group setup.
///
/// The benchmark core never assumes a particular TAK Server administration API.
pub trait Provisioner: Send + Sync {
    /// # Errors
    ///
    /// Returns an error when the server cannot prepare the requested isolated fixture.
    fn prepare(&self, spec: &FixtureSpec) -> Result<FixtureHandle, ProvisioningError>;

    /// # Errors
    ///
    /// Returns an error when the fixture cannot be cleaned up.
    fn cleanup(&self, fixture: &FixtureHandle) -> Result<(), ProvisioningError>;
}
