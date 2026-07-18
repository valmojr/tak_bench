# Architecture

`tak_bench` is a Rust workspace. `tak-bench-protocol` owns CoT XML encoding and incremental stream framing; `tak-bench-core` owns transport, TLS, lifecycle, metrics, safety and scheduling; `tak-bench-scenarios` translates profiles into workloads; `tak-bench-report` serializes outcomes; and `tak-bench-cli` is the only command-line boundary.

The MVP protocol is raw TCP carrying one or more CoT `<event>` XML documents. The incremental decoder accepts split writes and concatenated events. TCP, TLS and mTLS are transport concerns, so routing and Vanguarda-specific policy do not enter the core.

A future server adapter may declare routing, self-echo and correlation capabilities. `VanguardaProvisioner` will be an optional integration crate and is never activated for production automatically.
