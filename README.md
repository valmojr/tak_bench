# TAK Bench

`TAK Bench` is a Rust tool for explicitly authorized testing of TAK/CoT servers that accept CoT XML over raw TCP, TLS, or mTLS. Its stream framing follows the official TAK Server `StreamingCotProtocol`: fragmented and concatenated CoT events, as well as authentication preambles, are supported.

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

A YAML configuration can define TLS/mTLS, participant roles, ramp-up, timeouts, reconnect, routing observations, fragmentation, thresholds, and stable JSON output. CLI flags override equivalent fields. Unsupported scenario and scheduling options are rejected before dialing. Start with [functional-routing.yaml](examples/functional-routing.yaml).

## Current capabilities

- Fixed-position CoT events with a UID and per-event correlation ID.
- TCP, TLS, and mTLS with hostname verification always enabled.
- Concurrent reading and writing, received/duplicate message counts, and local delivery latency when the correlation extension is preserved.
- `immediate`, `linear`, `step`, and `randomized` ramps; connection, message, latency, and drop thresholds that cooperatively stop a run.
- Participant roles (`send_only`, `receive_only`, and `send_receive`), bounded reconnect with jitter, per-operation timeouts, CoT batching and fragmentation.
- Observational routing assertions: sender correlations must arrive at named receivers and not at forbidden receivers; the harness does not configure server routing.
- Terminal and JSON reports with final status, abort reason, sanitized configuration, metrics, and assertion results.
- A server-neutral `Provisioner` interface and `FakeProvisioner` for tests; no Vanguarda-specific or other server-specific API is embedded.

## Development

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo build --workspace --release --locked
cargo package --workspace --locked
```

Slow local readers, bounded abrupt disconnects, bounded slow first writes, and carefully rate-limited invalid inputs are opt-in scenario controls. They are never production-safe. See [scenario guidance](docs/scenarios.md).

Before a release tag, run an authorized mTLS preflight and smoke workload against the intended TAK Server version using `examples/smoke-mtls.yaml`. Loopback fixtures validate transport behavior but do not claim compatibility with every server deployment. See the [GitHub Actions and release guide](docs/github-actions.md) for the tag and artifact process.

## License

Licensed under either the [Apache License 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT), at your option.
