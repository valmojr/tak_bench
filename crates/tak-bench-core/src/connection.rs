use std::{
    fs::File,
    io::BufReader,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use rustls::{ClientConfig, RootCertStore, pki_types::ServerName};
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf},
    net::TcpStream,
};
use tokio_rustls::TlsConnector;

use crate::config::{TargetConfig, TlsConfig};

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
pub async fn write_all(writer: &mut ConnectionWriter, bytes: &[u8]) -> Result<()> {
    writer.write_all(bytes).await.context("writing CoT event")?;
    writer.flush().await.context("flushing CoT event")
}

/// # Errors
///
/// Returns an error for failed TCP/TLS connection, invalid certificate material,
/// or TLS hostname verification failure.
pub async fn connect(target: &TargetConfig, tls: &TlsConfig) -> Result<ClientConnection> {
    let started = Instant::now();
    let tcp = TcpStream::connect(&target.server)
        .await
        .with_context(|| format!("connecting to {}", target.server))?;
    tcp.set_nodelay(true).context("enabling TCP_NODELAY")?;
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
    let server_name = ServerName::try_from(sni.to_owned()).context("invalid TLS SNI hostname")?;
    let connector = TlsConnector::from(Arc::new(build_tls_config(tls)?));
    let stream = connector
        .connect(server_name, tcp)
        .await
        .context("TLS handshake failed")?;
    Ok(ClientConnection {
        stream: Box::new(stream),
        handshake_time: started.elapsed(),
    })
}

fn build_tls_config(tls: &TlsConfig) -> Result<ClientConfig> {
    let ca_path = tls
        .ca
        .as_ref()
        .context("TLS requires tls.ca; hostname verification cannot be disabled")?;
    let mut roots = RootCertStore::empty();
    let mut reader = BufReader::new(
        File::open(ca_path).with_context(|| format!("opening CA {}", ca_path.display()))?,
    );
    for cert in rustls_pemfile::certs(&mut reader) {
        roots
            .add(cert.context("reading CA certificate")?)
            .context("adding CA certificate")?;
    }
    let builder = ClientConfig::builder().with_root_certificates(roots);
    match (&tls.client_cert, &tls.client_key) {
        (None, None) => Ok(builder.with_no_client_auth()),
        (Some(cert_path), Some(key_path)) => {
            let mut cert_reader =
                BufReader::new(File::open(cert_path).with_context(|| {
                    format!("opening client certificate {}", cert_path.display())
                })?);
            let certs = rustls_pemfile::certs(&mut cert_reader)
                .collect::<Result<Vec<_>, _>>()
                .context("reading client certificate")?;
            let mut key_reader = BufReader::new(
                File::open(key_path)
                    .with_context(|| format!("opening client key {}", key_path.display()))?,
            );
            let key = rustls_pemfile::private_key(&mut key_reader)
                .context("reading client private key")?
                .context("client key file contains no private key")?;
            Ok(builder
                .with_client_auth_cert(certs, key)
                .context("configuring client certificate")?)
        }
        _ => bail!("both tls.client_cert and tls.client_key are required for mTLS"),
    }
}
