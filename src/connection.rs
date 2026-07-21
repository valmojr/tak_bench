use std::{
    fs,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use rustls::{
    ClientConfig, RootCertStore,
    pki_types::{CertificateDer, PrivateKeyDer, ServerName, pem::PemObject},
};
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf},
    net::TcpStream,
};
use tokio_rustls::TlsConnector;

use crate::config::{TargetConfig, TimeoutConfig, TlsConfig};

#[derive(Debug, Error)]
pub enum ConnectError {
    #[error("TCP connection failed")]
    Tcp(#[source] anyhow::Error),
    #[error("TLS handshake failed")]
    TlsHandshake(#[source] anyhow::Error),
    #[error("CA certificate could not be loaded")]
    CaLoad(#[source] anyhow::Error),
    #[error("client certificate could not be loaded")]
    ClientCertificateLoad(#[source] anyhow::Error),
    #[error("client key could not be loaded")]
    ClientKeyLoad(#[source] anyhow::Error),
    #[error("TLS configuration failed")]
    TlsConfiguration(#[source] anyhow::Error),
}

impl ConnectError {
    #[must_use]
    pub fn is_tls(&self) -> bool {
        !matches!(self, Self::Tcp(_))
    }

    #[must_use]
    pub fn category(&self) -> crate::report::ErrorCategory {
        use crate::report::ErrorCategory;
        match self {
            Self::Tcp(_) => ErrorCategory::TcpConnectFailed,
            Self::TlsHandshake(_) => ErrorCategory::TlsHandshakeFailed,
            Self::CaLoad(_) => ErrorCategory::CaLoadFailed,
            Self::ClientCertificateLoad(_) => ErrorCategory::ClientCertificateLoadFailed,
            Self::ClientKeyLoad(_) => ErrorCategory::ClientKeyLoadFailed,
            Self::TlsConfiguration(_) => ErrorCategory::TlsConfigurationFailed,
        }
    }

    #[must_use]
    pub fn phase(&self) -> crate::report::ErrorPhase {
        if self.is_tls() {
            crate::report::ErrorPhase::TlsHandshake
        } else {
            crate::report::ErrorPhase::Connect
        }
    }
}

pub trait AsyncStream: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T> AsyncStream for T where T: AsyncRead + AsyncWrite + Send + Unpin {}

pub struct ClientConnection {
    stream: Box<dyn AsyncStream>,
    pub handshake_time: Duration,
}

pub type ConnectionReader = ReadHalf<Box<dyn AsyncStream>>;
pub type ConnectionWriter = WriteHalf<Box<dyn AsyncStream>>;

impl ClientConnection {
    /// # Errors
    ///
    /// Returns an error when the socket cannot write or flush the event.
    pub async fn send(&mut self, bytes: &[u8]) -> Result<()> {
        self.stream
            .write_all(bytes)
            .await
            .context("writing CoT event")?;
        self.stream.flush().await.context("flushing CoT event")
    }

    #[must_use]
    pub fn into_split(self) -> (ConnectionReader, ConnectionWriter) {
        tokio::io::split(self.stream)
    }
}

/// # Errors
///
/// Returns an error when the stream cannot write or flush the payload.
pub async fn write_all(
    writer: &mut ConnectionWriter,
    bytes: &[u8],
    timeout: Duration,
) -> Result<()> {
    tokio::time::timeout(timeout, async {
        writer.write_all(bytes).await.context("writing CoT event")?;
        writer.flush().await.context("flushing CoT event")
    })
    .await
    .context("write timed out")?
}

/// # Errors
///
/// Returns an error for failed TCP/TLS connection, invalid certificate material,
/// or TLS hostname verification failure.
pub async fn connect(
    target: &TargetConfig,
    tls: &TlsConfig,
    timeouts: &TimeoutConfig,
) -> std::result::Result<ClientConnection, ConnectError> {
    let started = Instant::now();
    let tcp = tokio::time::timeout(timeouts.connect, TcpStream::connect(&target.server))
        .await
        .context("connect timed out")
        .and_then(|result| result.with_context(|| format!("connecting to {}", target.server)))
        .map_err(ConnectError::Tcp)?;
    tcp.set_nodelay(true)
        .context("enabling TCP_NODELAY")
        .map_err(ConnectError::Tcp)?;
    if !tls.enabled {
        return Ok(ClientConnection {
            stream: Box::new(tcp),
            handshake_time: started.elapsed(),
        });
    }
    let sni = target
        .sni
        .as_deref()
        .unwrap_or_else(|| crate::config::host_from_server(&target.server));
    let server_name = ServerName::try_from(sni.to_owned())
        .context("invalid TLS SNI hostname")
        .map_err(ConnectError::TlsConfiguration)?;
    let connector = TlsConnector::from(Arc::new(build_tls_config(tls)?));
    let stream = tokio::time::timeout(timeouts.tls_handshake, connector.connect(server_name, tcp))
        .await
        .context("TLS handshake timed out")
        .and_then(|result| result.context("TLS handshake failed"))
        .map_err(ConnectError::TlsHandshake)?;
    Ok(ClientConnection {
        stream: Box::new(stream),
        handshake_time: started.elapsed(),
    })
}

fn build_tls_config(tls: &TlsConfig) -> std::result::Result<ClientConfig, ConnectError> {
    let ca_path = tls
        .ca
        .as_ref()
        .context("TLS requires a CA; hostname verification cannot be disabled")
        .map_err(ConnectError::TlsConfiguration)?;
    let mut roots = RootCertStore::empty();
    let ca_pem = fs::read(ca_path)
        .context("opening CA certificate")
        .map_err(ConnectError::CaLoad)?;
    for cert in CertificateDer::pem_slice_iter(&ca_pem) {
        roots
            .add(
                cert.context("reading CA certificate")
                    .map_err(ConnectError::CaLoad)?,
            )
            .context("adding CA certificate")
            .map_err(ConnectError::CaLoad)?;
    }
    let builder = ClientConfig::builder().with_root_certificates(roots);
    match (&tls.client_cert, &tls.client_key) {
        (None, None) => Ok(builder.with_no_client_auth()),
        (Some(cert_path), Some(key_path)) => {
            let cert_pem = fs::read(cert_path)
                .context("opening client certificate")
                .map_err(ConnectError::ClientCertificateLoad)?;
            let certs = CertificateDer::pem_slice_iter(&cert_pem)
                .collect::<Result<Vec<_>, _>>()
                .context("reading client certificate")
                .map_err(ConnectError::ClientCertificateLoad)?;
            let key_pem = fs::read(key_path)
                .context("opening client key")
                .map_err(ConnectError::ClientKeyLoad)?;
            let key = PrivateKeyDer::from_pem_slice(&key_pem)
                .context("reading client private key")
                .map_err(ConnectError::ClientKeyLoad)?;
            Ok(builder
                .with_client_auth_cert(certs, key)
                .context("configuring client certificate")
                .map_err(ConnectError::TlsConfiguration)?)
        }
        _ => Err(ConnectError::TlsConfiguration(anyhow::anyhow!(
            "both client certificate and client key are required for mTLS"
        ))),
    }
}
