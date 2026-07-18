use crate::{config::AbortConfig, metrics::MetricsSnapshot};

#[derive(Debug, Clone, PartialEq)]
pub enum ThresholdViolation {
    ConnectionErrorRate(f64),
    MessageErrorRate(f64),
    P95Latency(f64),
    P99Latency(f64),
    DroppedMessages(u64),
}

#[must_use]
pub fn evaluate(config: &AbortConfig, snapshot: &MetricsSnapshot) -> Option<ThresholdViolation> {
    let samples = snapshot.connection_attempts.max(snapshot.sent_messages);
    if samples < config.min_samples.unwrap_or(1) {
        return None;
    }
    let connection_error_rate = ratio(snapshot.connection_failures, snapshot.connection_attempts);
    if config
        .connection_error_rate
        .is_some_and(|limit| connection_error_rate > limit)
    {
        return Some(ThresholdViolation::ConnectionErrorRate(
            connection_error_rate,
        ));
    }
    let message_errors = snapshot
        .message_timeouts
        .saturating_add(snapshot.dropped_messages);
    let message_error_rate = ratio(message_errors, snapshot.sent_messages);
    if config
        .message_error_rate
        .is_some_and(|limit| message_error_rate > limit)
    {
        return Some(ThresholdViolation::MessageErrorRate(message_error_rate));
    }
    if config.p95_latency.is_some_and(|limit| {
        snapshot
            .delivery_p95_ms
            .is_some_and(|value| value > limit.as_secs_f64() * 1_000.0)
    }) {
        return Some(ThresholdViolation::P95Latency(
            snapshot.delivery_p95_ms.unwrap_or_default(),
        ));
    }
    if config.p99_latency.is_some_and(|limit| {
        snapshot
            .delivery_p99_ms
            .is_some_and(|value| value > limit.as_secs_f64() * 1_000.0)
    }) {
        return Some(ThresholdViolation::P99Latency(
            snapshot.delivery_p99_ms.unwrap_or_default(),
        ));
    }
    if config
        .max_dropped_messages
        .is_some_and(|limit| snapshot.dropped_messages > limit)
    {
        return Some(ThresholdViolation::DroppedMessages(
            snapshot.dropped_messages,
        ));
    }
    None
}

fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        f64::from(u32::try_from(numerator).unwrap_or(u32::MAX))
            / f64::from(u32::try_from(denominator).unwrap_or(u32::MAX))
    }
}
