# Roadmap

1. **MVP:** CLI, guardrails, TCP/TLS/mTLS, 1–10 fixed-position clients, terminal/JSON reporting.
2. **Load:** ramp-up, thresholds, reconnect and latency histograms.
3. **Scenarios:** markers, chat, routes, slow clients and tightly rate-limited invalid inputs.
4. **Extensions:** external fixture provisioning, cleanup and identity material through the neutral `Provisioner` boundary.
5. **Advanced:** spike/soak, distributed workers, Prometheus, JUnit and CI integration.

Each phase requires tests, formatting, Clippy, documentation, graceful shutdown and secret-free reports.
