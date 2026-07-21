# TAK Bench

`TAK Bench` is a Rust tool for explicitly authorized testing of TAK/CoT servers that accept CoT XML over raw TCP, TLS, or mTLS. Its stream framing follows the official TAK Server `StreamingCotProtocol`: fragmented and concatenated CoT events, as well as authentication preambles, are supported.

> Use this tool only against servers you administer or are authorized to test.

## Safety

Commands that open connections require `--acknowledge-authorization` or `authorization.acknowledged: true`. The destination must be listed in `allow_hosts`, except for loopback in `local`. Production additionally requires `--environment production --allow-production`, accepts only `smoke`, and limits runs to three clients, 15 minutes, and one position every 30 seconds or less frequently.

Invalid events are blocked by default. In staging they require `--allow-invalid-events`; in local and temporary environments they still require `max_events` and a maximum rate of one event per second.

## Usage

Validate a configuration without opening a connection:

```bash
cargo run -- validate --config examples/functional.yaml
```

Run an explicitly authorized local TCP smoke test:

```bash
cargo run -- smoke \
  --server 127.0.0.1:8089 \
  --acknowledge-authorization \
  --duration 2m
```

A YAML configuration can define TLS/mTLS, participant roles, ramp-up, timeouts, reconnect, readiness synchronization, routing observations, fragmentation, thresholds, and stable JSON output. CLI flags override equivalent fields. `--lifecycle-jsonl` (or `output.lifecycle_jsonl: true`) reserves stdout for sanitized orchestration events; the JSON report remains the primary artifact. Unsupported scenario and scheduling options are rejected before dialing. Start with [functional-routing.yaml](examples/functional-routing.yaml).

## Current capabilities

- Fixed-position CoT events with a UID and per-event correlation ID.
- TCP, TLS, and mTLS with hostname verification always enabled, including optional
  per-participant certificate/key path templates using `{participant_id}`.
- Concurrent reading and writing, received/duplicate message counts, and local delivery latency when the correlation extension is preserved.
- `immediate`, `linear`, `step`, and `randomized` ramps; connection, message, latency, and drop thresholds that cooperatively stop a run.
- Participant roles (`send_only`, `receive_only`, and `send_receive`), bounded reconnect with jitter, per-operation timeouts, CoT batching and fragmentation.
- Observational routing assertions: each expected or forbidden sender/receiver pair has its own sanitized result and observed count; the harness does not configure server routing.
- An optional readiness barrier gates sender workloads on named participants. "Ready" means that the participant completed TCP and, when configured, TLS/mTLS setup and can execute its local role. It does not claim server-side authorization, registration, presence, or policy acceptance.
- Optional sanitized JSON Lines lifecycle events (`participant_connected`, `participant_ready`, `participant_disconnected`, and `run_completed`) containing aliases and classified reasons only.
- Terminal and JSON reports with final status, abort reason, sanitized configuration, metrics, per-participant classified failures, and assertion results.
- A server-neutral `Provisioner` interface and `FakeProvisioner` for tests; no Vanguarda-specific or other server-specific API is embedded.
- A reusable `tak_bench::runner` module so external integrations can provision their own
  fixtures and execute the same guarded workload lifecycle as the CLI from one public crate.

## Compatibility contract

The harness only observes whether a server accepts TCP/TLS/mTLS connections, delivers events to clients, preserves the correlation identifier required by configured assertions, and closes or rejects sockets according to its own policy. It has no administrative view into a TAK Server.

Preservation of the correlation extension and acceptance of a receive-only client that sends no initial announcement are integration properties of the consumer's chosen server. `tak_bench` does not assume either behavior. Provisioning identities, certificates, groups, policy, revocation, and cleanup remains the external orchestrator's responsibility.

## Development

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
cargo build --release --locked
cargo package --locked
```

Slow local readers, bounded abrupt disconnects, bounded slow first writes, and carefully rate-limited invalid inputs are opt-in scenario controls. They are never production-safe. See [scenario guidance](docs/scenarios.md).

Before a release tag, run an authorized mTLS preflight and smoke workload against the intended TAK Server version using `examples/smoke-mtls.yaml`. Loopback fixtures validate transport behavior but do not claim compatibility with every server deployment. See the [GitHub Actions and release guide](docs/github-actions.md) for the tag and artifact process.

## License

Licensed under either the [Apache License 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT), at your option.
