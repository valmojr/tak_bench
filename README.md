# tak_bench

`tak_bench` is a Rust tool for explicitly authorized testing of TAK/CoT servers that accept CoT XML over raw TCP, TLS, or mTLS. Its stream framing follows the official TAK Server `StreamingCotProtocol`: fragmented and concatenated CoT events, as well as authentication preambles, are supported.

> Use this tool only against servers you administer or are authorized to test.

## Safety

Commands that open connections require `--acknowledge-authorization` or `authorization.acknowledged: true`. The destination must be listed in `allow_hosts`, except for loopback in `local`. Production additionally requires `--environment production --allow-production`, accepts only `smoke`, and limits runs to three clients, 15 minutes, and one position every 30 seconds or less frequently.

Invalid events are blocked by default. In staging they require `--allow-invalid-events`; in local and temporary environments they still require `max_events` and a maximum rate of one event per second.

## Usage

Validate a configuration without opening a connection:

```bash
cargo run -p tak-bench-cli -- validate --config examples/functional.yaml
```

Run an explicitly authorized local TCP smoke test:

```bash
cargo run -p tak-bench-cli -- smoke \
  --server 127.0.0.1:8089 \
  --acknowledge-authorization \
  --duration 2m
```

A YAML configuration can define TLS/mTLS, ramp-up, thresholds, fragmentation, and JSON output. CLI flags override equivalent fields. Start with [functional.yaml](examples/functional.yaml).

## Current capabilities

- Fixed-position CoT events with a UID and per-event correlation ID.
- TCP, TLS, and mTLS with hostname verification always enabled.
- Concurrent reading and writing, received/duplicate message counts, and local delivery latency when the correlation extension is preserved.
- `immediate`, `linear`, `step`, and `randomized` ramps; connection, message, latency, and drop thresholds that cooperatively stop a run.
- Terminal and JSON reports, including handshake and delivery metrics plus the stop reason.
- A server-neutral `Provisioner` interface and `FakeProvisioner` for tests; no Vanguarda-specific or other server-specific API is embedded.

## Development

```bash
cargo fmt --check
cargo clippy --workspace -- -D warnings
cargo test --workspace
```

Chat and marker scenarios, route/GeoJSON movement, reconnect execution, group routing assertions, test PKI, and the opt-in compatibility suite against an official TAK Server remain on the [roadmap](docs/roadmap.md).
