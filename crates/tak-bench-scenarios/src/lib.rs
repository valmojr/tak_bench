//! Built-in, server-neutral workload scenarios.

use std::{
    collections::HashMap,
    sync::{Arc, atomic::Ordering},
    time::Instant,
};

use anyhow::{Result, bail};
use rand::Rng;
use tak_bench_core::{
    config::{AppConfig, InvalidEventKind, ParticipantConfig, ParticipantRole, RoutingAssertion},
    connection::{self, ConnectionReader, ConnectionWriter},
    metrics::Metrics,
    scheduler::start_delays,
};
use tak_bench_protocol::{CotStreamDecoder, PositionEvent, inspect_event};
use time::OffsetDateTime;
use tokio::{
    io::AsyncReadExt,
    sync::{Mutex, watch},
    time::{MissedTickBehavior, interval},
};
use uuid::Uuid;

#[derive(Debug, Clone, serde::Serialize)]
pub struct AssertionResult {
    pub sender: String,
    pub passed: bool,
    pub expected_receivers: u64,
    pub forbidden_receivers: u64,
}
#[derive(Debug, Default, serde::Serialize)]
pub struct ScenarioOutcome {
    pub assertions: Vec<AssertionResult>,
}

#[derive(Default)]
struct CorrelationLedger {
    sent: HashMap<Uuid, (String, Instant)>,
    seen: HashMap<(String, Uuid), Instant>,
}

/// Runs fixed `CoT` positions. Routing checks only observe the stream; they never configure a server.
///
/// # Errors
///
/// Returns an error for a failed connection, timeout, invalid schedule, or exhausted reconnect budget.
pub async fn run_fixed_positions(
    config: AppConfig,
    metrics: Arc<Metrics>,
    stop: watch::Receiver<bool>,
) -> Result<ScenarioOutcome> {
    run_fixed_positions_with_options(
        config,
        metrics,
        stop,
        tak_bench_core::safety::SafetyOptions::default(),
    )
    .await
}

/// Runs fixed positions after applying the full authorization and environment safety policy.
///
/// # Errors
///
/// Returns an error for unsafe configuration, failed connection, timeout, invalid schedule, or
/// exhausted reconnect budget.
pub async fn run_fixed_positions_with_options(
    config: AppConfig,
    metrics: Arc<Metrics>,
    mut stop: watch::Receiver<bool>,
    safety_options: tak_bench_core::safety::SafetyOptions,
) -> Result<ScenarioOutcome> {
    tak_bench_core::safety::validate_with_options(&config, safety_options)?;
    let deadline = tokio::time::Instant::now() + config.run.duration;
    let participants = participants(&config);
    let delays = start_delays(
        u32::try_from(participants.len()).map_err(|_| anyhow::anyhow!("too many participants"))?,
        &config.scheduler,
    )?;
    let ledger = Arc::new(Mutex::new(CorrelationLedger::default()));
    let (cancel_tx, cancel) = watch::channel(*stop.borrow());
    let forward_tx = cancel_tx.clone();
    let forward_stop = tokio::spawn(async move {
        wait_for_stop(&mut stop).await;
        let _ = forward_tx.send(true);
    });
    let mut tasks = tokio::task::JoinSet::new();
    for (participant, delay) in participants.into_iter().zip(delays) {
        let config = config.clone();
        let metrics = Arc::clone(&metrics);
        let ledger = Arc::clone(&ledger);
        let mut stop = cancel.clone();
        tasks.spawn(async move {
            if !wait_for_delay(delay, &mut stop, deadline).await {
                return Ok(());
            }
            run_participant(participant, config, metrics, ledger, stop, deadline).await
        });
    }
    let mut first_error = None;
    while let Some(task) = tasks.join_next().await {
        let result = task
            .map_err(|error| anyhow::anyhow!("participant task failed: {error}"))
            .and_then(std::convert::identity);
        if first_error.is_none()
            && let Err(error) = result
        {
            first_error = Some(error);
            let _ = cancel_tx.send(true);
        }
    }
    forward_stop.abort();
    let _ = forward_stop.await;
    if let Some(error) = first_error {
        return Err(error);
    }
    Ok(ScenarioOutcome {
        assertions: evaluate_routing(&config.scenario.routing, &*ledger.lock().await),
    })
}

async fn wait_for_stop(stop: &mut watch::Receiver<bool>) {
    loop {
        if *stop.borrow() || stop.changed().await.is_err() {
            return;
        }
    }
}

async fn wait_for_delay(
    delay: std::time::Duration,
    stop: &mut watch::Receiver<bool>,
    deadline: tokio::time::Instant,
) -> bool {
    if *stop.borrow() || tokio::time::Instant::now() >= deadline {
        return false;
    }
    tokio::select! {
        () = tokio::time::sleep(delay) => true,
        () = wait_for_stop(stop) => false,
        () = tokio::time::sleep_until(deadline) => false,
    }
}

fn participants(config: &AppConfig) -> Vec<ParticipantConfig> {
    if !config.participants.is_empty() {
        return config.participants.clone();
    }
    (0..config.run.clients)
        .map(|index| ParticipantConfig {
            id: format!("client-{index}"),
            ..ParticipantConfig::default()
        })
        .collect()
}

async fn run_participant(
    participant: ParticipantConfig,
    config: AppConfig,
    metrics: Arc<Metrics>,
    ledger: Arc<Mutex<CorrelationLedger>>,
    mut stop: watch::Receiver<bool>,
    deadline: tokio::time::Instant,
) -> Result<()> {
    let mut attempt = 0_u32;
    let recovery_started = Instant::now();
    loop {
        if *stop.borrow() || tokio::time::Instant::now() >= deadline {
            return Ok(());
        }
        metrics.connection_attempts.fetch_add(1, Ordering::Relaxed);
        let connect = connection::connect(&config.target, &config.tls, &config.timeouts);
        let connection_result = tokio::select! {
            result = connect => result,
            () = wait_for_stop(&mut stop) => return Ok(()),
            () = tokio::time::sleep_until(deadline) => return Ok(()),
        };
        let failure = match connection_result {
            Ok(connection) => {
                metrics.connection_successes.fetch_add(1, Ordering::Relaxed);
                metrics.active_connections.fetch_add(1, Ordering::Relaxed);
                metrics.record_handshake(connection.handshake_time).await;
                if attempt > 0 {
                    metrics.reconnects.fetch_add(1, Ordering::Relaxed);
                    metrics.record_recovery(recovery_started.elapsed()).await;
                }
                let result = run_connected(
                    &participant,
                    &config,
                    Arc::clone(&metrics),
                    Arc::clone(&ledger),
                    stop.clone(),
                    deadline,
                    connection,
                )
                .await;
                metrics.active_connections.fetch_sub(1, Ordering::Relaxed);
                if let Err(error) = &result {
                    metrics.connection_failures.fetch_add(1, Ordering::Relaxed);
                    if config.tls.enabled && is_tls_failure(error) {
                        metrics.tls_failures.fetch_add(1, Ordering::Relaxed);
                    }
                    if attempt > 0 {
                        metrics.reconnect_failures.fetch_add(1, Ordering::Relaxed);
                    }
                }
                let error = match result {
                    Ok(()) => return Ok(()),
                    Err(error) => error,
                };
                if !config.reconnect.enabled || tokio::time::Instant::now() >= deadline {
                    return Err(error);
                }
                error
            }
            Err(error) => {
                metrics.connection_failures.fetch_add(1, Ordering::Relaxed);
                if is_tls_failure(&error) {
                    metrics.tls_failures.fetch_add(1, Ordering::Relaxed);
                }
                if !config.reconnect.enabled {
                    return Err(error);
                }
                metrics.reconnect_failures.fetch_add(1, Ordering::Relaxed);
                error
            }
        };
        if attempt >= config.reconnect.max_attempts {
            bail!(
                "participant {} exhausted reconnect attempts",
                participant.id
            );
        }
        attempt += 1;
        if !wait_for_delay(reconnect_delay(&config, attempt), &mut stop, deadline).await {
            return if *stop.borrow() { Ok(()) } else { Err(failure) };
        }
    }
}

fn is_tls_failure(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<tak_bench_core::connection::ConnectError>()
            .is_some_and(tak_bench_core::connection::ConnectError::is_tls)
            || cause
                .downcast_ref::<std::io::Error>()
                .is_some_and(|error| error.kind() == std::io::ErrorKind::InvalidData)
    })
}

fn reconnect_delay(config: &AppConfig, attempt: u32) -> std::time::Duration {
    let multiplier = 2_u32.saturating_pow(attempt.saturating_sub(1));
    let base = config
        .reconnect
        .min_backoff
        .saturating_mul(multiplier)
        .min(config.reconnect.max_backoff);
    let jitter = f64::from(config.reconnect.jitter_percent.min(100)) / 100.0;
    if jitter == 0.0 {
        return base;
    }
    let factor = rand::rng().random_range((1.0 - jitter)..=(1.0 + jitter));
    base.mul_f64(factor)
}

async fn run_connected(
    participant: &ParticipantConfig,
    config: &AppConfig,
    metrics: Arc<Metrics>,
    ledger: Arc<Mutex<CorrelationLedger>>,
    mut stop: watch::Receiver<bool>,
    deadline: tokio::time::Instant,
    connection: connection::ClientConnection,
) -> Result<()> {
    let (reader, mut writer) = connection.into_split();
    if config.scenario.slow_connect.enabled
        && participant.role != ParticipantRole::ReceiveOnly
        && !wait_for_delay(
            config.scenario.slow_connect.initial_write_delay,
            &mut stop,
            deadline,
        )
        .await
    {
        return Ok(());
    }
    if participant.role == ParticipantRole::ReceiveOnly {
        return read_events(
            participant,
            reader,
            &metrics,
            &ledger,
            &mut stop,
            deadline,
            config.timeouts.read,
        )
        .await;
    }
    if participant.role == ParticipantRole::SendOnly {
        return send_positions(
            participant,
            config,
            &metrics,
            &ledger,
            &mut writer,
            &mut stop,
            deadline,
        )
        .await;
    }
    let reader_participant = participant.clone();
    let mut reader_stop = stop.clone();
    let read_timeout = config.timeouts.read;
    let reader_metrics = Arc::clone(&metrics);
    let reader_ledger = Arc::clone(&ledger);
    let mut reader_task = tokio::spawn(async move {
        read_events(
            &reader_participant,
            reader,
            &reader_metrics,
            &reader_ledger,
            &mut reader_stop,
            deadline,
            read_timeout,
        )
        .await
    });
    let mut sender = std::pin::pin!(send_positions(
        participant,
        config,
        &metrics,
        &ledger,
        &mut writer,
        &mut stop,
        deadline,
    ));
    tokio::select! {
        sent = &mut sender => { reader_task.abort(); let _ = reader_task.await; sent }
        read = &mut reader_task => {
            read.map_err(|error| anyhow::anyhow!("reader task failed: {error}"))?
        }
    }
}

async fn read_events(
    participant: &ParticipantConfig,
    mut reader: ConnectionReader,
    metrics: &Metrics,
    ledger: &Mutex<CorrelationLedger>,
    stop: &mut watch::Receiver<bool>,
    deadline: tokio::time::Instant,
    read_timeout: std::time::Duration,
) -> Result<()> {
    let mut decoder = CotStreamDecoder::new(8 * 1024 * 1024);
    let mut buffer = [0_u8; 8192];
    if let Some(pause) = participant.pause_read_for
        && !wait_for_delay(pause, stop, deadline).await
    {
        return Ok(());
    }
    loop {
        tokio::select! {
            changed = stop.changed() => if changed.is_err() || *stop.borrow() { return Ok(()); },
            () = tokio::time::sleep_until(deadline) => return Ok(()),
            result = tokio::time::timeout(read_timeout, reader.read(&mut buffer)) => {
                let count = if let Ok(result) = result {
                    result?
                } else {
                    metrics.message_timeouts.fetch_add(1, Ordering::Relaxed);
                    bail!("read timed out");
                };
                if count == 0 { bail!("peer closed the connection before the run deadline"); }
                if let Some(delay) = participant.read_delay
                    && !wait_for_delay(delay, stop, deadline).await
                {
                    return Ok(());
                }
                for raw in decoder.push(&buffer[..count])? {
                    let event = inspect_event(raw)?; metrics.received_messages.fetch_add(1, Ordering::Relaxed);
                    if let Some(correlation) = event.correlation_id { let mut state = ledger.lock().await;
                        if state.seen.insert((participant.id.clone(), correlation), Instant::now()).is_some() { metrics.duplicate_messages.fetch_add(1, Ordering::Relaxed); }
                        else if let Some((_, sent)) = state.sent.get(&correlation) { metrics.record_delivery(sent.elapsed()).await; }
                    }
                }
            }
        }
    }
}

async fn send_positions(
    participant: &ParticipantConfig,
    config: &AppConfig,
    metrics: &Metrics,
    ledger: &Mutex<CorrelationLedger>,
    writer: &mut ConnectionWriter,
    stop: &mut watch::Receiver<bool>,
    deadline: tokio::time::Instant,
) -> Result<()> {
    let mut ticker = interval(emission_interval(config));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    ticker.tick().await;
    let mut batch = OutgoingBatch::default();
    let batch_size = config.scenario.fragmentation.events_per_write.max(1);
    let mut event_count = 0;
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let invalid_kind = invalid_kind(config, event_count);
                let invalid_xml = invalid_or_position(participant, config, event_count);
                let correlation = Uuid::new_v4();
                let xml = invalid_xml.unwrap_or_else(|| position_xml(participant, config, correlation));
                event_count += 1;
                if invalid_kind == Some(InvalidEventKind::OversizedFrame) {
                    flush_batch(writer, participant, config, metrics, ledger, &mut batch).await?;
                    if let Err(error) = write_fragmented(writer, xml.as_bytes(), &config.scenario.fragmentation.chunk_sizes, config.timeouts.write).await {
                        record_write_failure(metrics, 1, &error);
                        return Err(error);
                    }
                    metrics.sent_messages.fetch_add(1, Ordering::Relaxed);
                } else {
                    batch.bytes.extend_from_slice(xml.as_bytes());
                    batch.events += 1;
                    if invalid_kind.is_none() {
                        batch.correlations.push((correlation, Instant::now()));
                    }
                    if batch.bytes.len() > 8 * 1024 * 1024 {
                        record_local_drop(metrics, batch.events);
                        bail!("outgoing batch exceeds frame safety limit");
                    }
                    if batch.events >= u64::try_from(batch_size).unwrap_or(u64::MAX) {
                        flush_batch(writer, participant, config, metrics, ledger, &mut batch).await?;
                    }
                }
                if config.scenario.abrupt_disconnect.enabled
                    && event_count
                        >= config.scenario.abrupt_disconnect.after_events.max(1)
                {
                    if batch.events > 0 {
                        record_local_drop(metrics, batch.events);
                    }
                    return Ok(());
                }
            }
            changed = stop.changed() => if changed.is_err() || *stop.borrow() { break; },
            () = tokio::time::sleep_until(deadline) => break,
        }
    }
    if batch.events > 0 {
        record_local_drop(metrics, batch.events);
    }
    Ok(())
}

#[derive(Default)]
struct OutgoingBatch {
    bytes: Vec<u8>,
    correlations: Vec<(Uuid, Instant)>,
    events: u64,
}

fn emission_interval(config: &AppConfig) -> std::time::Duration {
    config.run.max_rate.map_or(config.run.gps_interval, |rate| {
        config
            .run
            .gps_interval
            .max(std::time::Duration::from_secs_f64(1.0 / rate))
    })
}

fn record_local_drop(metrics: &Metrics, events: u64) {
    metrics
        .local_dropped_messages
        .fetch_add(events, Ordering::Relaxed);
    metrics
        .dropped_messages
        .fetch_add(events, Ordering::Relaxed);
}

fn record_write_failure(metrics: &Metrics, events: u64, error: &anyhow::Error) {
    metrics
        .dropped_messages
        .fetch_add(events, Ordering::Relaxed);
    if error.chain().any(|cause| {
        cause
            .downcast_ref::<tokio::time::error::Elapsed>()
            .is_some()
    }) {
        metrics
            .message_timeouts
            .fetch_add(events, Ordering::Relaxed);
    }
}

async fn flush_batch(
    writer: &mut ConnectionWriter,
    participant: &ParticipantConfig,
    config: &AppConfig,
    metrics: &Metrics,
    ledger: &Mutex<CorrelationLedger>,
    batch: &mut OutgoingBatch,
) -> Result<()> {
    if batch.bytes.is_empty() {
        return Ok(());
    }
    if let Err(error) = write_fragmented(
        writer,
        &batch.bytes,
        &config.scenario.fragmentation.chunk_sizes,
        config.timeouts.write,
    )
    .await
    {
        record_write_failure(metrics, batch.events, &error);
        return Err(error);
    }
    metrics
        .sent_messages
        .fetch_add(batch.events, Ordering::Relaxed);
    let mut state = ledger.lock().await;
    for (correlation, sent_at) in batch.correlations.drain(..) {
        state
            .sent
            .insert(correlation, (participant.id.clone(), sent_at));
    }
    batch.bytes.clear();
    batch.events = 0;
    Ok(())
}

fn position_xml(
    participant: &ParticipantConfig,
    config: &AppConfig,
    correlation_id: Uuid,
) -> String {
    let now = OffsetDateTime::now_utc();
    PositionEvent {
        uid: format!("tak-bench-{}", participant.id),
        callsign: participant.id.clone(),
        latitude: -23.5505,
        longitude: -46.6333,
        altitude_m: 760.0,
        course_deg: 0.0,
        speed_mps: 0.0,
        time: now,
        stale: now
            + time::Duration::seconds(
                i64::try_from(config.run.gps_interval.as_secs()).unwrap_or(30) * 2,
            ),
        correlation_id,
    }
    .to_xml()
}
fn invalid_or_position(
    _participant: &ParticipantConfig,
    config: &AppConfig,
    count: u32,
) -> Option<String> {
    Some(match invalid_kind(config, count)? {
        InvalidEventKind::MalformedXml => "<event".into(),
        InvalidEventKind::UnterminatedXml => "<event uid=\"invalid\">".into(),
        InvalidEventKind::OversizedFrame => {
            format!("<event>{}</event>", "x".repeat(8 * 1024 * 1024 + 1))
        }
        InvalidEventKind::InvalidCoordinates => {
            "<event><point lat=\"nan\" lon=\"999\"/></event>".into()
        }
        InvalidEventKind::InvalidTime => "<event time=\"not-a-time\"></event>".into(),
    })
}

fn invalid_kind(config: &AppConfig, count: u32) -> Option<InvalidEventKind> {
    if !config.scenario.invalid.enabled || count >= config.scenario.invalid.max_events.unwrap_or(0)
    {
        return None;
    }
    Some(
        config
            .scenario
            .invalid
            .kind
            .unwrap_or(InvalidEventKind::MalformedXml),
    )
}

async fn write_fragmented(
    writer: &mut ConnectionWriter,
    bytes: &[u8],
    chunks: &[usize],
    timeout: std::time::Duration,
) -> Result<()> {
    if chunks.is_empty() {
        return connection::write_all(writer, bytes, timeout).await;
    }
    let mut offset = 0;
    for chunk in chunks.iter().copied().cycle() {
        if offset == bytes.len() {
            return Ok(());
        }
        let end = offset.saturating_add(chunk.max(1)).min(bytes.len());
        connection::write_all(writer, &bytes[offset..end], timeout).await?;
        offset = end;
    }
    Ok(())
}

fn evaluate_routing(
    assertions: &[RoutingAssertion],
    state: &CorrelationLedger,
) -> Vec<AssertionResult> {
    assertions
        .iter()
        .map(|assertion| {
            let correlations: Vec<_> = state
                .sent
                .iter()
                .filter(|(_, (sender, _))| sender == &assertion.sender)
                .map(|(id, _)| *id)
                .collect();
            let timeout = assertion
                .timeout
                .unwrap_or(std::time::Duration::from_secs(30));
            let expected = correlations
                .iter()
                .flat_map(|id| {
                    assertion
                        .receivers
                        .iter()
                        .map(move |receiver| (receiver, id))
                })
                .filter(|(receiver, id)| {
                    state
                        .seen
                        .get(&((*receiver).clone(), **id))
                        .is_some_and(|seen| {
                            state
                                .sent
                                .get(*id)
                                .is_some_and(|(_, sent)| seen.duration_since(*sent) <= timeout)
                        })
                })
                .count() as u64;
            let forbidden = correlations
                .iter()
                .flat_map(|id| {
                    assertion
                        .forbidden_receivers
                        .iter()
                        .map(move |receiver| (receiver, id))
                })
                .filter(|(receiver, id)| state.seen.contains_key(&((*receiver).clone(), **id)))
                .count() as u64;
            AssertionResult {
                sender: assertion.sender.clone(),
                passed: !correlations.is_empty()
                    && expected == (correlations.len() * assertion.receivers.len()) as u64
                    && forbidden == 0,
                expected_receivers: expected,
                forbidden_receivers: forbidden,
            }
        })
        .collect()
}

#[must_use]
pub fn minimum_stale_interval(interval: std::time::Duration) -> std::time::Duration {
    interval.saturating_mul(2)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rcgen::{BasicConstraints, Certificate, CertificateParams, IsCa, KeyPair};
    use rustls::{
        RootCertStore, ServerConfig,
        pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer},
        server::WebPkiClientVerifier,
    };
    use std::{path::PathBuf, sync::atomic::AtomicUsize};
    use tak_bench_core::{
        config::{
            AbruptDisconnectConfig, Environment, InvalidScenarioConfig, RampStep, RampStrategy,
            ReconnectConfig, RoutingAssertion, SchedulerConfig, SlowConnectConfig,
        },
        safety,
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{
            TcpListener,
            tcp::{OwnedReadHalf, OwnedWriteHalf},
        },
        sync::{mpsc, oneshot},
    };

    struct TestAuthority {
        certificate: Certificate,
        key: KeyPair,
    }

    impl TestAuthority {
        fn new(name: &str) -> Self {
            let mut params = CertificateParams::new(vec![name.into()]).unwrap();
            params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
            let key = KeyPair::generate().unwrap();
            let certificate = params.self_signed(&key).unwrap();
            Self { certificate, key }
        }

        fn issue(&self, name: &str) -> (Certificate, KeyPair) {
            let key = KeyPair::generate().unwrap();
            let certificate = CertificateParams::new(vec![name.into()])
                .unwrap()
                .signed_by(&key, &self.certificate, &self.key)
                .unwrap();
            (certificate, key)
        }
    }

    #[derive(Default)]
    struct TempPemFiles {
        paths: Vec<PathBuf>,
    }

    impl TempPemFiles {
        fn write(&mut self, label: &str, contents: impl AsRef<[u8]>) -> PathBuf {
            let path = PathBuf::from("/tmp")
                .join(format!("tak-bench-test-{}-{label}.pem", Uuid::new_v4()));
            std::fs::write(&path, contents).unwrap();
            self.paths.push(path.clone());
            path
        }
    }

    impl Drop for TempPemFiles {
        fn drop(&mut self) {
            for path in &self.paths {
                let _ = std::fs::remove_file(path);
            }
        }
    }

    fn client_tls(
        files: &mut TempPemFiles,
        server_ca: &Certificate,
        identity: Option<(&Certificate, &KeyPair)>,
    ) -> tak_bench_core::config::TlsConfig {
        let ca = files.write("ca", server_ca.pem());
        let (client_cert, client_key) = identity.map_or((None, None), |(certificate, key)| {
            (
                Some(files.write("client-cert", certificate.pem())),
                Some(files.write("client-key", key.serialize_pem())),
            )
        });
        tak_bench_core::config::TlsConfig {
            enabled: true,
            ca: Some(ca),
            client_cert,
            client_key,
            ..tak_bench_core::config::TlsConfig::default()
        }
    }

    fn server_config(
        server: &Certificate,
        server_key: &KeyPair,
        client_ca: Option<&Certificate>,
    ) -> ServerConfig {
        let builder = ServerConfig::builder();
        let builder = if let Some(client_ca) = client_ca {
            let mut roots = RootCertStore::empty();
            roots.add(client_ca.der().clone()).unwrap();
            let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
                .build()
                .unwrap();
            builder.with_client_cert_verifier(verifier)
        } else {
            builder.with_no_client_auth()
        };
        builder
            .with_single_cert(
                vec![server.der().clone()],
                PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(server_key.serialize_der())),
            )
            .unwrap()
    }

    async fn tls_server(
        config: ServerConfig,
    ) -> (std::net::SocketAddr, tokio::task::JoinHandle<bool>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
        let task = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            acceptor.accept(tcp).await.is_ok()
        });
        (address, task)
    }

    fn tls_target(
        address: std::net::SocketAddr,
        sni: &str,
    ) -> tak_bench_core::config::TargetConfig {
        tak_bench_core::config::TargetConfig {
            server: address.to_string(),
            sni: Some(sni.into()),
        }
    }

    fn loopback_config(address: std::net::SocketAddr) -> AppConfig {
        AppConfig {
            authorization: tak_bench_core::config::AuthorizationConfig { acknowledged: true },
            target: tak_bench_core::config::TargetConfig {
                server: address.to_string(),
                sni: None,
            },
            run: tak_bench_core::config::RunConfig {
                clients: 1,
                max_clients: 3,
                duration: std::time::Duration::from_millis(500),
                gps_interval: std::time::Duration::from_millis(30),
                ..tak_bench_core::config::RunConfig::default()
            },
            timeouts: tak_bench_core::config::TimeoutConfig {
                connect: std::time::Duration::from_millis(50),
                tls_handshake: std::time::Duration::from_millis(50),
                read: std::time::Duration::from_millis(300),
                write: std::time::Duration::from_millis(50),
            },
            ..AppConfig::default()
        }
    }

    async fn relay(listener: TcpListener, accepted: Arc<AtomicUsize>) {
        let (tx, _) = tokio::sync::broadcast::channel::<(usize, Vec<u8>)>(32);
        let mut next = 0;
        while let Ok((stream, _)) = listener.accept().await {
            let id = next;
            next += 1;
            accepted.fetch_add(1, Ordering::Relaxed);
            let (mut read, mut write) = tokio::io::split(stream);
            let sender = tx.clone();
            let mut receiver = tx.subscribe();
            tokio::spawn(async move {
                loop {
                    match receiver.recv().await {
                        Ok((source, bytes)) if source != id => {
                            if write.write_all(&bytes).await.is_err() {
                                break;
                            }
                        }
                        Ok(_) => {}
                        Err(_) => break,
                    }
                }
            });
            tokio::spawn(async move {
                let mut buffer = [0_u8; 4096];
                loop {
                    match read.read(&mut buffer).await {
                        Ok(0) | Err(_) => break,
                        Ok(count) => {
                            let _ = sender.send((id, buffer[..count].to_vec()));
                        }
                    }
                }
            });
        }
    }

    async fn relay_direction(
        mut reader: OwnedReadHalf,
        mut writer: OwnedWriteHalf,
        direction: usize,
        observed: mpsc::Sender<usize>,
    ) {
        let mut buffer = [0_u8; 4096];
        let mut reported = false;
        loop {
            match reader.read(&mut buffer).await {
                Ok(0) | Err(_) => break,
                Ok(count) => {
                    if !reported {
                        reported = true;
                        let _ = observed.send(direction).await;
                    }
                    if writer.write_all(&buffer[..count]).await.is_err() {
                        break;
                    }
                }
            }
        }
    }

    async fn abrupt_disconnect_server(
        listener: TcpListener,
        accepted: Arc<AtomicUsize>,
        first_closed: oneshot::Sender<()>,
        stable_ready: oneshot::Sender<()>,
        directions: mpsc::Sender<usize>,
        reconnects: usize,
    ) {
        let (first, _) = listener.accept().await.unwrap();
        accepted.fetch_add(1, Ordering::Relaxed);
        drop(first);
        let _ = first_closed.send(());

        let (stable_a, _) = listener.accept().await.unwrap();
        accepted.fetch_add(1, Ordering::Relaxed);
        let (stable_b, _) = listener.accept().await.unwrap();
        accepted.fetch_add(1, Ordering::Relaxed);
        let (a_read, a_write) = stable_a.into_split();
        let (b_read, b_write) = stable_b.into_split();
        tokio::spawn(relay_direction(a_read, b_write, 0, directions.clone()));
        tokio::spawn(relay_direction(b_read, a_write, 1, directions));
        let _ = stable_ready.send(());

        for _ in 0..reconnects {
            let (reconnect, _) = listener.accept().await.unwrap();
            accepted.fetch_add(1, Ordering::Relaxed);
            drop(reconnect);
        }
    }

    async fn wait_for_sent(metrics: &Metrics, expected: u64) {
        for _ in 0..10_000 {
            if metrics.sent_messages.load(Ordering::Relaxed) == expected {
                return;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(metrics.sent_messages.load(Ordering::Relaxed), expected);
    }

    async fn wait_for_connections(metrics: &Metrics, expected: u64) {
        for _ in 0..10_000 {
            if metrics.connection_successes.load(Ordering::Relaxed) == expected {
                return;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(
            metrics.connection_successes.load(Ordering::Relaxed),
            expected
        );
    }

    #[tokio::test]
    async fn tls_hostname_validation_uses_an_ephemeral_local_ca() {
        let authority = TestAuthority::new("test-ca");
        let (server, server_key) = authority.issue("example.test");
        let (address, task) = tls_server(server_config(&server, &server_key, None)).await;
        let mut files = TempPemFiles::default();
        let tls = client_tls(&mut files, &authority.certificate, None);
        let connected = connection::connect(
            &tls_target(address, "example.test"),
            &tls,
            &tak_bench_core::config::TimeoutConfig::default(),
        )
        .await
        .is_ok();
        assert!(connected);
        assert!(task.await.unwrap());
    }

    #[tokio::test]
    async fn tls_rejects_an_incorrect_sni() {
        let authority = TestAuthority::new("test-ca");
        let (server, server_key) = authority.issue("example.test");
        let (address, task) = tls_server(server_config(&server, &server_key, None)).await;
        let mut files = TempPemFiles::default();
        let tls = client_tls(&mut files, &authority.certificate, None);
        let rejected = connection::connect(
            &tls_target(address, "wrong.test"),
            &tls,
            &tak_bench_core::config::TimeoutConfig::default(),
        )
        .await
        .is_err();
        assert!(rejected);
        assert!(!task.await.unwrap());
    }

    #[tokio::test]
    async fn tls_handshake_timeout_is_bounded() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let (_tcp, _) = listener.accept().await.unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        });
        let authority = TestAuthority::new("test-ca");
        let mut files = TempPemFiles::default();
        let tls = client_tls(&mut files, &authority.certificate, None);
        let timeouts = tak_bench_core::config::TimeoutConfig {
            tls_handshake: std::time::Duration::from_millis(10),
            ..tak_bench_core::config::TimeoutConfig::default()
        };
        assert!(
            connection::connect(&tls_target(address, "example.test"), &tls, &timeouts)
                .await
                .is_err()
        );
        task.await.unwrap();
    }

    #[tokio::test]
    async fn mtls_requires_and_accepts_an_ephemeral_client_certificate() {
        let authority = TestAuthority::new("test-ca");
        let (server, server_key) = authority.issue("example.test");
        let (client, client_key) = authority.issue("client.test");
        let (address, task) = tls_server(server_config(
            &server,
            &server_key,
            Some(&authority.certificate),
        ))
        .await;
        let mut files = TempPemFiles::default();
        let tls = client_tls(
            &mut files,
            &authority.certificate,
            Some((&client, &client_key)),
        );
        let connected = connection::connect(
            &tls_target(address, "example.test"),
            &tls,
            &tak_bench_core::config::TimeoutConfig::default(),
        )
        .await
        .is_ok();
        assert!(connected);
        assert!(task.await.unwrap());
    }

    #[tokio::test]
    async fn mtls_rejects_a_client_without_a_certificate() {
        let authority = TestAuthority::new("trusted-ca");
        let (server, server_key) = authority.issue("example.test");
        let (address, task) = tls_server(server_config(
            &server,
            &server_key,
            Some(&authority.certificate),
        ))
        .await;
        let mut files = TempPemFiles::default();
        let tls = client_tls(&mut files, &authority.certificate, None);
        let _client_result = connection::connect(
            &tls_target(address, "example.test"),
            &tls,
            &tak_bench_core::config::TimeoutConfig::default(),
        )
        .await;
        assert!(!task.await.unwrap());
    }

    #[tokio::test]
    async fn mtls_rejects_a_client_signed_by_an_untrusted_ca() {
        let trusted = TestAuthority::new("trusted-ca");
        let untrusted = TestAuthority::new("untrusted-ca");
        let (server, server_key) = trusted.issue("example.test");
        let (client, client_key) = untrusted.issue("client.test");
        let (address, task) = tls_server(server_config(
            &server,
            &server_key,
            Some(&trusted.certificate),
        ))
        .await;
        let mut files = TempPemFiles::default();
        let tls = client_tls(
            &mut files,
            &trusted.certificate,
            Some((&client, &client_key)),
        );
        let _client_result = connection::connect(
            &tls_target(address, "example.test"),
            &tls,
            &tak_bench_core::config::TimeoutConfig::default(),
        )
        .await;
        assert!(!task.await.unwrap());
    }

    #[tokio::test]
    async fn tcp_roles_routing_and_fragmented_batches_work_end_to_end() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let accepted = Arc::new(AtomicUsize::new(0));
        let server = tokio::spawn(relay(listener, Arc::clone(&accepted)));
        let mut config = loopback_config(address);
        config.run.clients = 2;
        config.participants = vec![
            ParticipantConfig {
                id: "sender".into(),
                role: ParticipantRole::SendOnly,
                ..ParticipantConfig::default()
            },
            ParticipantConfig {
                id: "receiver".into(),
                role: ParticipantRole::ReceiveOnly,
                ..ParticipantConfig::default()
            },
        ];
        config.scenario.fragmentation.chunk_sizes = vec![3, 7, 11];
        config.scenario.fragmentation.events_per_write = 2;
        config.scenario.routing = vec![RoutingAssertion {
            sender: "sender".into(),
            receivers: vec!["receiver".into()],
            forbidden_receivers: vec![],
            timeout: Some(std::time::Duration::from_secs(1)),
        }];
        let metrics = Arc::new(Metrics::new());
        let (_stop_tx, stop) = watch::channel(false);
        let outcome = run_fixed_positions(config, Arc::clone(&metrics), stop)
            .await
            .unwrap();
        assert!(outcome.assertions[0].passed);
        assert!(metrics.received_messages.load(Ordering::Relaxed) > 0);
        assert_eq!(accepted.load(Ordering::Relaxed), 2);
        server.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn slow_reader_does_not_block_other_participants() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let accepted = Arc::new(AtomicUsize::new(0));
        let server = tokio::spawn(relay(listener, Arc::clone(&accepted)));
        let mut config = loopback_config(address);
        config.run.clients = 3;
        config.run.duration = std::time::Duration::from_secs(3);
        config.run.gps_interval = std::time::Duration::from_secs(1);
        config.timeouts.read = std::time::Duration::from_secs(5);
        config.timeouts.write = std::time::Duration::from_secs(1);
        config.scheduler = SchedulerConfig {
            strategy: RampStrategy::Step,
            steps: vec![
                RampStep {
                    at: std::time::Duration::ZERO,
                    clients: 2,
                },
                RampStep {
                    at: std::time::Duration::from_millis(100),
                    clients: 3,
                },
            ],
            ..SchedulerConfig::default()
        };
        config.participants = vec![
            ParticipantConfig {
                id: "slow-reader".into(),
                role: ParticipantRole::ReceiveOnly,
                pause_read_for: Some(std::time::Duration::from_secs(4)),
                ..ParticipantConfig::default()
            },
            ParticipantConfig {
                id: "fast-reader".into(),
                role: ParticipantRole::ReceiveOnly,
                ..ParticipantConfig::default()
            },
            ParticipantConfig {
                id: "sender".into(),
                role: ParticipantRole::SendOnly,
                ..ParticipantConfig::default()
            },
        ];
        let metrics = Arc::new(Metrics::new());
        let (_stop_tx, stop) = watch::channel(false);
        run_fixed_positions(config, Arc::clone(&metrics), stop)
            .await
            .unwrap();
        assert!(metrics.sent_messages.load(Ordering::Relaxed) > 0);
        assert!(metrics.received_messages.load(Ordering::Relaxed) > 0);
        assert_eq!(accepted.load(Ordering::Relaxed), 3);
        server.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn slow_first_write_is_bounded_and_cancelled_by_watch() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (accepted_tx, accepted_rx) = oneshot::channel();
        let (payload_tx, mut payload_rx) = mpsc::channel(1);
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let _ = accepted_tx.send(());
            let mut buffer = [0_u8; 1024];
            let count = stream.read(&mut buffer).await.unwrap();
            let _ = payload_tx.send(count).await;
        });
        let mut config = loopback_config(address);
        config.run.duration = std::time::Duration::from_secs(5);
        config.run.gps_interval = std::time::Duration::from_secs(1);
        config.timeouts.write = std::time::Duration::from_secs(2);
        config.scenario.slow_connect = SlowConnectConfig {
            enabled: true,
            initial_write_delay: std::time::Duration::from_secs(2),
        };
        config.participants = vec![ParticipantConfig {
            id: "delayed".into(),
            role: ParticipantRole::SendOnly,
            ..ParticipantConfig::default()
        }];
        let metrics = Arc::new(Metrics::new());
        let (stop_tx, stop) = watch::channel(false);
        let runner = tokio::spawn(run_fixed_positions(config, Arc::clone(&metrics), stop));
        accepted_rx.await.unwrap();
        wait_for_connections(&metrics, 1).await;
        tokio::task::yield_now().await;
        tokio::time::advance(std::time::Duration::from_millis(1_999)).await;
        tokio::task::yield_now().await;
        assert!(matches!(
            payload_rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        tokio::time::advance(std::time::Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        tokio::time::advance(std::time::Duration::from_millis(999)).await;
        tokio::task::yield_now().await;
        assert!(matches!(
            payload_rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        tokio::time::advance(std::time::Duration::from_millis(1)).await;
        tokio::select! {
            count = payload_rx.recv() => assert!(count.is_some_and(|count| count > 0)),
            () = tokio::time::sleep(std::time::Duration::from_millis(500)) => panic!("first write exceeded its configured bound"),
        }
        stop_tx.send(true).unwrap();
        assert!(runner.await.unwrap().is_ok());
        server.await.unwrap();
        assert_eq!(metrics.sent_messages.load(Ordering::Relaxed), 1);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (accepted_tx, accepted_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let _ = accepted_tx.send(());
            let mut received = Vec::new();
            stream.read_to_end(&mut received).await.unwrap();
            received
        });
        let mut config = loopback_config(address);
        config.run.duration = std::time::Duration::from_secs(60);
        config.scenario.slow_connect = SlowConnectConfig {
            enabled: true,
            initial_write_delay: std::time::Duration::from_secs(30),
        };
        config.participants = vec![ParticipantConfig {
            id: "cancelled".into(),
            role: ParticipantRole::SendOnly,
            ..ParticipantConfig::default()
        }];
        let (stop_tx, stop) = watch::channel(false);
        let runner = tokio::spawn(run_fixed_positions(config, Arc::new(Metrics::new()), stop));
        accepted_rx.await.unwrap();
        tokio::task::yield_now().await;
        stop_tx.send(true).unwrap();
        let result = tokio::time::timeout(std::time::Duration::from_secs(1), runner)
            .await
            .unwrap()
            .unwrap();
        assert!(result.is_ok());
        assert!(server.await.unwrap().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn slow_first_write_stops_at_the_run_deadline() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut received = Vec::new();
            stream.read_to_end(&mut received).await.unwrap();
            received
        });
        let mut config = loopback_config(address);
        config.run.duration = std::time::Duration::from_secs(1);
        config.scenario.slow_connect = SlowConnectConfig {
            enabled: true,
            initial_write_delay: std::time::Duration::from_secs(30),
        };
        config.participants = vec![ParticipantConfig {
            id: "deadline".into(),
            role: ParticipantRole::SendOnly,
            ..ParticipantConfig::default()
        }];
        let (_stop_tx, stop) = watch::channel(false);
        assert!(
            run_fixed_positions(config, Arc::new(Metrics::new()), stop)
                .await
                .is_ok()
        );
        assert!(server.await.unwrap().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn abrupt_disconnect_isolated_and_reconnects_stop_at_max_attempts() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let accepted = Arc::new(AtomicUsize::new(0));
        let (first_closed_tx, first_closed_rx) = oneshot::channel();
        let (stable_ready_tx, stable_ready_rx) = oneshot::channel();
        let (direction_tx, mut direction_rx) = mpsc::channel(2);
        let server = tokio::spawn(abrupt_disconnect_server(
            listener,
            Arc::clone(&accepted),
            first_closed_tx,
            stable_ready_tx,
            direction_tx,
            2,
        ));

        let mut config = loopback_config(address);
        config.run.duration = std::time::Duration::from_secs(4);
        config.run.gps_interval = std::time::Duration::from_secs(1);
        config.timeouts.read = std::time::Duration::from_secs(5);
        config.reconnect = ReconnectConfig {
            enabled: true,
            min_backoff: std::time::Duration::from_secs(1),
            max_backoff: std::time::Duration::from_secs(1),
            max_attempts: 2,
            jitter_percent: 0,
        };
        let metrics = Arc::new(Metrics::new());
        let ledger = Arc::new(Mutex::new(CorrelationLedger::default()));
        let (_stop_tx, stop) = watch::channel(false);
        let deadline = tokio::time::Instant::now() + config.run.duration;
        let flaky_config = config.clone();
        let flaky_metrics = Arc::clone(&metrics);
        let flaky_ledger = Arc::clone(&ledger);
        let flaky_stop = stop.clone();
        let flaky = tokio::spawn(async move {
            run_participant(
                ParticipantConfig {
                    id: "flaky".into(),
                    ..ParticipantConfig::default()
                },
                flaky_config,
                flaky_metrics,
                flaky_ledger,
                flaky_stop,
                deadline,
            )
            .await
        });
        first_closed_rx.await.unwrap();

        let stable_a_config = config.clone();
        let stable_a_metrics = Arc::clone(&metrics);
        let stable_a_ledger = Arc::clone(&ledger);
        let stable_a_stop = stop.clone();
        let stable_a = tokio::spawn(async move {
            run_participant(
                ParticipantConfig {
                    id: "stable-a".into(),
                    ..ParticipantConfig::default()
                },
                stable_a_config,
                stable_a_metrics,
                stable_a_ledger,
                stable_a_stop,
                deadline,
            )
            .await
        });
        let stable_b = tokio::spawn(run_participant(
            ParticipantConfig {
                id: "stable-b".into(),
                ..ParticipantConfig::default()
            },
            config,
            Arc::clone(&metrics),
            Arc::clone(&ledger),
            stop,
            deadline,
        ));
        stable_ready_rx.await.unwrap();

        let mut directions = [false; 2];
        while !directions.iter().all(|observed| *observed) {
            let direction = direction_rx.recv().await.unwrap();
            directions[direction] = true;
        }
        let (flaky, stable_a, stable_b) = tokio::join!(flaky, stable_a, stable_b);
        assert!(flaky.unwrap().is_err());
        assert!(stable_a.unwrap().is_ok());
        assert!(stable_b.unwrap().is_ok());
        server.await.unwrap();
        assert_eq!(accepted.load(Ordering::Relaxed), 5);
        assert_eq!(metrics.connection_attempts.load(Ordering::Relaxed), 5);
        assert_eq!(metrics.reconnects.load(Ordering::Relaxed), 2);
        assert!(metrics.received_messages.load(Ordering::Relaxed) >= 2);
    }

    #[tokio::test(start_paused = true)]
    async fn invalid_payload_kinds_are_bounded_against_a_loopback_fixture() {
        let kinds = [
            InvalidEventKind::MalformedXml,
            InvalidEventKind::UnterminatedXml,
            InvalidEventKind::OversizedFrame,
            InvalidEventKind::InvalidCoordinates,
            InvalidEventKind::InvalidTime,
        ];
        for kind in kinds {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (accepted_tx, accepted_rx) = oneshot::channel();
            let server = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                let _ = accepted_tx.send(());
                let mut received = Vec::new();
                stream.read_to_end(&mut received).await.unwrap();
                received
            });
            let mut config = loopback_config(address);
            config.environment = Environment::Local;
            config.run.duration = std::time::Duration::from_millis(3_100);
            config.run.gps_interval = std::time::Duration::from_secs(1);
            config.run.max_rate = Some(1.0);
            config.timeouts.write = std::time::Duration::from_secs(60);
            config.participants = vec![ParticipantConfig {
                id: "invalid-sender".into(),
                role: ParticipantRole::SendOnly,
                ..ParticipantConfig::default()
            }];
            config.scenario.invalid = InvalidScenarioConfig {
                enabled: true,
                kind: Some(kind),
                max_events: Some(2),
            };
            assert!(safety::validate(&config, false).is_ok());
            let expected_invalid = invalid_or_position(&config.participants[0], &config, 0)
                .unwrap()
                .into_bytes();
            assert!(invalid_or_position(&config.participants[0], &config, 1).is_some());
            assert!(invalid_or_position(&config.participants[0], &config, 2).is_none());

            let metrics = Arc::new(Metrics::new());
            let (_stop_tx, stop) = watch::channel(false);
            let runner = tokio::spawn(run_fixed_positions(config, Arc::clone(&metrics), stop));
            accepted_rx.await.unwrap();
            wait_for_connections(&metrics, 1).await;
            tokio::task::yield_now().await;
            tokio::time::advance(std::time::Duration::from_millis(999)).await;
            tokio::task::yield_now().await;
            assert_eq!(metrics.sent_messages.load(Ordering::Relaxed), 0);
            tokio::time::advance(std::time::Duration::from_millis(1)).await;
            wait_for_sent(&metrics, 1).await;
            tokio::time::advance(std::time::Duration::from_millis(999)).await;
            tokio::task::yield_now().await;
            assert_eq!(metrics.sent_messages.load(Ordering::Relaxed), 1);
            tokio::time::advance(std::time::Duration::from_millis(1)).await;
            wait_for_sent(&metrics, 2).await;
            tokio::time::advance(std::time::Duration::from_secs(1)).await;
            wait_for_sent(&metrics, 3).await;
            tokio::time::advance(std::time::Duration::from_millis(100)).await;
            assert!(runner.await.unwrap().is_ok());
            let received = server.await.unwrap();
            let invalid_bytes = expected_invalid.len();
            assert!(received.len() > invalid_bytes * 2);
            assert_eq!(&received[..invalid_bytes], expected_invalid.as_slice());
            assert_eq!(
                &received[invalid_bytes..invalid_bytes * 2],
                expected_invalid.as_slice()
            );
            let valid = String::from_utf8(received[invalid_bytes * 2..].to_vec()).unwrap();
            assert!(inspect_event(valid).is_ok());
            assert_eq!(metrics.sent_messages.load(Ordering::Relaxed), 3);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn participant_failure_cancels_and_joins_remaining_work() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (first, _) = listener.accept().await.unwrap();
            let (mut second, _) = listener.accept().await.unwrap();
            drop(first);
            let mut received = Vec::new();
            second.read_to_end(&mut received).await.unwrap();
            received
        });
        let mut config = loopback_config(address);
        config.run.clients = 2;
        config.run.duration = std::time::Duration::from_secs(60);
        config.timeouts.read = std::time::Duration::from_secs(60);
        config.participants = vec![
            ParticipantConfig {
                id: "failing".into(),
                role: ParticipantRole::ReceiveOnly,
                ..ParticipantConfig::default()
            },
            ParticipantConfig {
                id: "cancelled".into(),
                role: ParticipantRole::ReceiveOnly,
                ..ParticipantConfig::default()
            },
        ];
        let metrics = Arc::new(Metrics::new());
        let (_stop_tx, stop) = watch::channel(false);
        assert!(
            run_fixed_positions(config, Arc::clone(&metrics), stop)
                .await
                .is_err()
        );
        assert!(server.await.unwrap().is_empty());
        assert_eq!(metrics.active_connections.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.connection_attempts.load(Ordering::Relaxed), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_discards_pending_batches_without_writing() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (accepted_tx, accepted_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let _ = accepted_tx.send(());
            let mut received = Vec::new();
            stream.read_to_end(&mut received).await.unwrap();
            received
        });
        let mut config = loopback_config(address);
        config.run.duration = std::time::Duration::from_secs(60);
        config.run.gps_interval = std::time::Duration::from_secs(1);
        config.scenario.fragmentation.events_per_write = 2;
        config.participants = vec![ParticipantConfig {
            id: "batched".into(),
            role: ParticipantRole::SendOnly,
            ..ParticipantConfig::default()
        }];
        let metrics = Arc::new(Metrics::new());
        let (stop_tx, stop) = watch::channel(false);
        let runner = tokio::spawn(run_fixed_positions(config, Arc::clone(&metrics), stop));
        accepted_rx.await.unwrap();
        wait_for_connections(&metrics, 1).await;
        tokio::time::advance(std::time::Duration::from_millis(1_500)).await;
        tokio::task::yield_now().await;
        stop_tx.send(true).unwrap();
        assert!(runner.await.unwrap().is_ok());
        assert!(server.await.unwrap().is_empty());
        assert_eq!(metrics.sent_messages.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.local_dropped_messages.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.dropped_messages.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn abrupt_disconnect_accounts_for_an_unflushed_batch() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut received = Vec::new();
            stream.read_to_end(&mut received).await.unwrap();
            received
        });
        let mut config = loopback_config(address);
        config.scenario.fragmentation.events_per_write = 2;
        config.scenario.abrupt_disconnect = AbruptDisconnectConfig {
            enabled: true,
            after_events: 1,
        };
        config.participants = vec![ParticipantConfig {
            id: "abrupt-batch".into(),
            role: ParticipantRole::SendOnly,
            ..ParticipantConfig::default()
        }];
        let metrics = Arc::new(Metrics::new());
        let (_stop_tx, stop) = watch::channel(false);
        assert!(
            run_fixed_positions(config, Arc::clone(&metrics), stop)
                .await
                .is_ok()
        );
        assert!(server.await.unwrap().is_empty());
        assert_eq!(metrics.sent_messages.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.local_dropped_messages.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.dropped_messages.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn max_rate_controls_the_runtime_emission_interval() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (accepted_tx, accepted_rx) = oneshot::channel();
        let (payload_tx, mut payload_rx) = mpsc::channel(1);
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let _ = accepted_tx.send(());
            let mut buffer = [0_u8; 1024];
            let count = stream.read(&mut buffer).await.unwrap();
            let _ = payload_tx.send(count).await;
        });
        let mut config = loopback_config(address);
        config.run.duration = std::time::Duration::from_secs(5);
        config.run.gps_interval = std::time::Duration::from_secs(1);
        config.run.max_rate = Some(0.5);
        config.participants = vec![ParticipantConfig {
            id: "rate-limited".into(),
            role: ParticipantRole::SendOnly,
            ..ParticipantConfig::default()
        }];
        let metrics = Arc::new(Metrics::new());
        let (stop_tx, stop) = watch::channel(false);
        let runner = tokio::spawn(run_fixed_positions(config, Arc::clone(&metrics), stop));
        accepted_rx.await.unwrap();
        wait_for_connections(&metrics, 1).await;
        tokio::time::advance(std::time::Duration::from_millis(1_999)).await;
        tokio::task::yield_now().await;
        assert!(matches!(
            payload_rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        tokio::time::advance(std::time::Duration::from_millis(1)).await;
        assert!(payload_rx.recv().await.is_some_and(|count| count > 0));
        stop_tx.send(true).unwrap();
        assert!(runner.await.unwrap().is_ok());
        server.await.unwrap();
        assert_eq!(metrics.sent_messages.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn reconnect_deadline_preserves_the_last_connection_failure() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let mut config = loopback_config(address);
        config.run.duration = std::time::Duration::from_secs(1);
        config.reconnect = ReconnectConfig {
            enabled: true,
            min_backoff: std::time::Duration::from_secs(2),
            max_backoff: std::time::Duration::from_secs(2),
            max_attempts: 5,
            jitter_percent: 0,
        };
        config.participants = vec![ParticipantConfig {
            id: "unreachable".into(),
            role: ParticipantRole::ReceiveOnly,
            ..ParticipantConfig::default()
        }];
        let metrics = Arc::new(Metrics::new());
        let (_stop_tx, stop) = watch::channel(false);
        assert!(
            run_fixed_positions(config, Arc::clone(&metrics), stop)
                .await
                .is_err()
        );
        assert_eq!(metrics.connection_attempts.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.connection_failures.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn eof_triggers_bounded_reconnect() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (first, _) = listener.accept().await.unwrap();
            drop(first);
            let (_second, _) = listener.accept().await.unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        });
        let mut config = loopback_config(address);
        config.participants = vec![ParticipantConfig {
            id: "receiver".into(),
            role: ParticipantRole::ReceiveOnly,
            ..ParticipantConfig::default()
        }];
        config.reconnect = ReconnectConfig {
            enabled: true,
            min_backoff: std::time::Duration::from_millis(1),
            max_backoff: std::time::Duration::from_millis(2),
            max_attempts: 2,
            jitter_percent: 0,
        };
        let metrics = Arc::new(Metrics::new());
        let (_stop_tx, stop) = watch::channel(false);
        let _ = run_fixed_positions(config, Arc::clone(&metrics), stop).await;
        assert!(metrics.reconnects.load(Ordering::Relaxed) >= 1);
        server.abort();
    }

    #[tokio::test]
    async fn read_timeout_is_reported_against_a_real_socket() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        });
        let mut config = loopback_config(address);
        config.timeouts.read = std::time::Duration::from_millis(40);
        config.participants = vec![ParticipantConfig {
            id: "receiver".into(),
            role: ParticipantRole::ReceiveOnly,
            ..ParticipantConfig::default()
        }];
        let (_stop_tx, stop) = watch::channel(false);
        let metrics = Arc::new(Metrics::new());
        assert!(
            run_fixed_positions(config, Arc::clone(&metrics), stop)
                .await
                .is_err()
        );
        assert_eq!(metrics.message_timeouts.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.connection_failures.load(Ordering::Relaxed), 1);
        server.abort();
    }

    #[test]
    fn reconnect_backoff_is_bounded() {
        let config = AppConfig {
            reconnect: ReconnectConfig {
                min_backoff: std::time::Duration::from_secs(1),
                max_backoff: std::time::Duration::from_secs(3),
                jitter_percent: 0,
                ..ReconnectConfig::default()
            },
            ..AppConfig::default()
        };
        assert_eq!(
            reconnect_delay(&config, 1),
            std::time::Duration::from_secs(1)
        );
        assert_eq!(
            reconnect_delay(&config, 3),
            std::time::Duration::from_secs(3)
        );
    }

    #[test]
    fn routing_reports_expected_and_forbidden_receivers_without_payloads() {
        let id = Uuid::nil();
        let mut state = CorrelationLedger::default();
        state.sent.insert(id, ("sender".into(), Instant::now()));
        state.seen.insert(("receiver".into(), id), Instant::now());
        let result = evaluate_routing(
            &[RoutingAssertion {
                sender: "sender".into(),
                receivers: vec!["receiver".into()],
                forbidden_receivers: vec!["forbidden".into()],
                timeout: None,
            }],
            &state,
        );
        assert!(result[0].passed);
        state.seen.insert(("forbidden".into(), id), Instant::now());
        assert!(
            !evaluate_routing(
                &[RoutingAssertion {
                    sender: "sender".into(),
                    receivers: vec!["receiver".into()],
                    forbidden_receivers: vec!["forbidden".into()],
                    timeout: None
                }],
                &state
            )[0]
            .passed
        );
        assert!(
            !evaluate_routing(
                &[RoutingAssertion {
                    sender: "missing".into(),
                    receivers: vec!["receiver".into()],
                    forbidden_receivers: vec![],
                    timeout: None,
                }],
                &CorrelationLedger::default(),
            )[0]
            .passed
        );
    }

    #[test]
    fn default_participants_follow_client_count() {
        let config = AppConfig {
            run: tak_bench_core::config::RunConfig {
                clients: 2,
                ..tak_bench_core::config::RunConfig::default()
            },
            ..AppConfig::default()
        };
        assert_eq!(participants(&config).len(), 2);
    }
}
