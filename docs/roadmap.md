# Roadmap

1. **MVP:** CLI, guardrails, TCP/TLS/mTLS, 1–10 fixed-position clients, terminal/JSON reporting.
2. **Load:** ramp-up, 100+ clients, thresholds, reconnect and latency histograms.
3. **Scenarios:** markers, chat, routes, groups, slow clients and tightly rate-limited invalid inputs.
4. **Vanguarda integration:** synthetic tenant provisioning, cleanup, roles and revocation checks.
5. **Advanced:** spike/soak, distributed workers, Prometheus, JUnit and CI integration.

Each phase requires tests, formatting, Clippy, documentation, graceful shutdown and secret-free reports.
