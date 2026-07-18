# Metrics and latency

Handshake latency is local monotonic elapsed time from dial start to an established TCP or TLS session. Delivery latency is also local monotonic time: the sender records immediately before writing a CoT event and an observer records after decoding the same correlation ID. It is available only after a preflight proves the server preserves the generated CoT detail extension; otherwise it is reported as unavailable.

The report includes connection attempts, successes, failures, TLS failures, active connections, disconnects, sent and received messages, drops, duplicates, timeouts, throughput and HDR histogram percentiles. Server CPU and memory are never inferred; external Prometheus/API/file import is a later feature.
