//! Helpers reserved for TCP/TLS fixture servers shared by integration tests.

use crate::provisioning::{FixtureHandle, FixtureSpec, Provisioner, ProvisioningError};
use std::net::SocketAddr;
use tokio::net::TcpListener;

/// # Errors
///
/// Returns an I/O error when the loopback listener cannot be bound.
pub async fn bind_loopback() -> std::io::Result<(TcpListener, SocketAddr)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    Ok((listener, address))
}

/// Deterministic provider for tests that must exercise fixture lifecycle logic.
#[derive(Debug, Default)]
pub struct FakeProvisioner;

impl Provisioner for FakeProvisioner {
    fn prepare(&self, spec: &FixtureSpec) -> Result<FixtureHandle, ProvisioningError> {
        if spec.participants.is_empty() {
            return Err(ProvisioningError::Unavailable(
                "a fixture needs at least one participant".into(),
            ));
        }
        Ok(FixtureHandle {
            id: format!("fake-{}", spec.name),
        })
    }
    fn cleanup(&self, _fixture: &FixtureHandle) -> Result<(), ProvisioningError> {
        Ok(())
    }
}
