# Metrics and latency

Handshake latency is local monotonic elapsed time from dial start to an established TCP or TLS session. Delivery latency is also local monotonic time: the sender records immediately before writing a CoT event and an observer records after decoding the same correlation ID. It is available only after a preflight proves the server preserves the generated CoT detail extension; otherwise it is reported as unavailable.

The report includes connection attempts, established sessions, session failures, TLS failures, active connections, sent and received messages, local and transport drops, duplicates, message timeouts, and HDR histogram percentiles. Write failures count attempted events as dropped; timeout categories are tracked without including payload or certificate contents. Server CPU, memory and throughput are not inferred; external Prometheus/API/file import is a later feature.
