# Architecture

`tak_bench` is a Rust workspace. `tak-bench-protocol` owns CoT XML encoding and incremental stream framing; `tak-bench-core` owns transport, TLS, lifecycle, metrics, safety and scheduling; `tak-bench-scenarios` translates profiles into workloads; `tak-bench-report` serializes outcomes; and `tak-bench-cli` is the only command-line boundary.

The protocol is raw TCP carrying one or more CoT `<event>` XML documents. The incremental decoder accepts split writes and concatenated events. TCP, TLS and mTLS are transport concerns, so routing policy does not enter the core.

An external project may implement `Provisioner` to prepare and clean up its own fixture. It can supply temporary certificate paths in YAML, invoke the CLI, consume the JSON report, and run its cleanup independently. The core never assumes an administration API, identity model, routing model, or PKI mechanism.
