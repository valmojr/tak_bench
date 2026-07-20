use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use hdrhistogram::Histogram;
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct Metrics {
    pub connection_attempts: AtomicU64,
    pub connection_successes: AtomicU64,
    pub connection_failures: AtomicU64,
    pub tls_failures: AtomicU64,
    pub sent_messages: AtomicU64,
    pub received_messages: AtomicU64,
    pub duplicate_messages: AtomicU64,
    pub dropped_messages: AtomicU64,
    pub message_timeouts: AtomicU64,
    pub reconnects: AtomicU64,
    pub reconnect_failures: AtomicU64,
    pub local_dropped_messages: AtomicU64,
    pub active_connections: AtomicU64,
    handshake_us: Mutex<Histogram<u64>>,
    delivery_us: Mutex<Histogram<u64>>,
    recovery_us: Mutex<Histogram<u64>>,
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            connection_attempts: AtomicU64::new(0),
            connection_successes: AtomicU64::new(0),
            connection_failures: AtomicU64::new(0),
            tls_failures: AtomicU64::new(0),
            sent_messages: AtomicU64::new(0),
            received_messages: AtomicU64::new(0),
            duplicate_messages: AtomicU64::new(0),
            dropped_messages: AtomicU64::new(0),
            message_timeouts: AtomicU64::new(0),
            reconnects: AtomicU64::new(0),
            reconnect_failures: AtomicU64::new(0),
            local_dropped_messages: AtomicU64::new(0),
            active_connections: AtomicU64::new(0),
            handshake_us: Mutex::new(
                Histogram::new_with_bounds(1, 600_000_000, 3).expect("valid histogram bounds"),
            ),
            delivery_us: Mutex::new(
                Histogram::new_with_bounds(1, 600_000_000, 3).expect("valid histogram bounds"),
            ),
            recovery_us: Mutex::new(
                Histogram::new_with_bounds(1, 600_000_000, 3).expect("valid histogram bounds"),
            ),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct MetricsSnapshot {
    pub connection_attempts: u64,
    pub connection_successes: u64,
    pub connection_failures: u64,
    pub tls_failures: u64,
    pub sent_messages: u64,
    pub received_messages: u64,
    pub duplicate_messages: u64,
    pub dropped_messages: u64,
    pub message_timeouts: u64,
    pub reconnects: u64,
    pub reconnect_failures: u64,
    pub local_dropped_messages: u64,
    pub active_connections: u64,
    pub handshake_p50_ms: Option<f64>,
    pub handshake_p95_ms: Option<f64>,
    pub delivery_p50_ms: Option<f64>,
    pub delivery_p95_ms: Option<f64>,
    pub delivery_p99_ms: Option<f64>,
    pub recovery_p95_ms: Option<f64>,
}

impl Metrics {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
    pub async fn record_handshake(&self, elapsed: Duration) {
        let micros = u64::try_from(elapsed.as_micros())
            .unwrap_or(u64::MAX)
            .max(1);
        let _ = self.handshake_us.lock().await.record(micros);
    }
    pub async fn record_delivery(&self, elapsed: Duration) {
        let micros = u64::try_from(elapsed.as_micros())
            .unwrap_or(u64::MAX)
            .max(1);
        let _ = self.delivery_us.lock().await.record(micros);
    }
    pub async fn record_recovery(&self, elapsed: Duration) {
        let micros = u64::try_from(elapsed.as_micros())
            .unwrap_or(u64::MAX)
            .max(1);
        let _ = self.recovery_us.lock().await.record(micros);
    }
    pub async fn snapshot(&self) -> MetricsSnapshot {
        let histogram = self.handshake_us.lock().await;
        let delivery = self.delivery_us.lock().await;
        let recovery = self.recovery_us.lock().await;
        let percentile = |histogram: &Histogram<u64>, q| {
            if histogram.is_empty() {
                None
            } else {
                Some(
                    f64::from(u32::try_from(histogram.value_at_quantile(q)).unwrap_or(u32::MAX))
                        / 1_000.0,
                )
            }
        };
        MetricsSnapshot {
            connection_attempts: self.connection_attempts.load(Ordering::Relaxed),
            connection_successes: self.connection_successes.load(Ordering::Relaxed),
            connection_failures: self.connection_failures.load(Ordering::Relaxed),
            tls_failures: self.tls_failures.load(Ordering::Relaxed),
            sent_messages: self.sent_messages.load(Ordering::Relaxed),
            received_messages: self.received_messages.load(Ordering::Relaxed),
            duplicate_messages: self.duplicate_messages.load(Ordering::Relaxed),
            dropped_messages: self.dropped_messages.load(Ordering::Relaxed),
            message_timeouts: self.message_timeouts.load(Ordering::Relaxed),
            reconnects: self.reconnects.load(Ordering::Relaxed),
            reconnect_failures: self.reconnect_failures.load(Ordering::Relaxed),
            local_dropped_messages: self.local_dropped_messages.load(Ordering::Relaxed),
            active_connections: self.active_connections.load(Ordering::Relaxed),
            handshake_p50_ms: percentile(&histogram, 0.5),
            handshake_p95_ms: percentile(&histogram, 0.95),
            delivery_p50_ms: percentile(&delivery, 0.5),
            delivery_p95_ms: percentile(&delivery, 0.95),
            delivery_p99_ms: percentile(&delivery, 0.99),
            recovery_p95_ms: percentile(&recovery, 0.95),
        }
    }
}
