# Architecture

`tak_bench` is a single Rust package with a library and the `tak-bench` binary. Its modules retain explicit boundaries: `protocol` owns CoT XML encoding and incremental stream framing; the configuration, connection, metrics, safety, and scheduling modules own guarded transport primitives; `scenarios` translates profiles into workloads; `report` serializes outcomes; `runner` owns the reusable execution lifecycle; and `main.rs` is the command-line boundary.

The protocol is raw TCP carrying one or more CoT `<event>` XML documents. The incremental decoder accepts split writes and concatenated events. TCP, TLS and mTLS are transport concerns, so routing policy does not enter the core.

An external project may implement `tak_bench::provisioning::Provisioner` to prepare and clean up its own fixture. It can supply temporary certificate paths in YAML, invoke the CLI, consume the JSON report, and run its cleanup independently. The library never assumes an administration API, identity model, routing model, or PKI mechanism.

When a server requires one identity per socket, `tls.client_cert_template` and
`tls.client_key_template` resolve `{participant_id}` for each configured participant. The
participant ID is restricted to path-safe ASCII characters, the templates must be supplied as a
pair, and they cannot be mixed with the global client certificate fields.

Readiness is entirely client-side: a participant becomes ready after its TCP connection and any configured TLS/mTLS handshake complete, immediately before it begins its local send/receive role. A configured barrier uses participant aliases and notifications to hold senders; it never polls or infers server sessions, authorization, registration, or routing policy.

Lifecycle JSON Lines use a closed schema of participant aliases, event names, completion states, and classified disconnect reasons. Runtime failures in the primary report likewise contain only alias, phase, and category. Credential paths, local temporary paths, raw error strings, certificates, keys, and CoT payloads are excluded.
