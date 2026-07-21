# Scenarios and profiles

The built-in fixed CoT position scenario supports explicit participant roles, local routing observations, batching, fragmentation, bounded reconnect and timeouts. Routing assertions observe correlations only; they do not configure groups, permissions, or any server policy.

Each routing expectation is reported independently with `sender`, `receiver`, `expectation`, `passed`, and `received_count`; no CoT XML is retained in the report. During the functional profile, a read timeout on a receive-capable participant means that no message was observed during that interval and reading continues to the global deadline. EOF, TLS, connection, write, and parse failures remain failures. All observable assertions are evaluated after participant tasks finish, even when another participant failed.

`synchronization.wait_for_ready` is an optional sender barrier. Every listed alias must complete TCP plus any configured TLS/mTLS handshake before sending begins, or `synchronization.timeout` produces a deterministic `readiness_timeout` failure. This readiness state is local transport readiness only and is not server authorization or presence confirmation.

Profiles classify reports and select environment guardrails; participant counts, cadence, thresholds and reconnect behavior remain explicit configuration. Production only accepts `smoke`. `stress`, `spike`, and `soak` are limited to local or temporary environments.

`smoke` is suitable for an authorized production check without aggressive load. `functional` is for local or staging. `load` (10–250 clients) needs an authorized environment. `stress`, `spike`, `soak`, slow readers, slow first writes, abrupt disconnects, and invalid payloads are local/temporary only, except where staging guardrails explicitly permit invalid events. Production is not a stress-test environment.

Invalid payloads are opt-in, bounded by `max_events`, rate-limited to one attempt per second, and blocked in production. Supported cases are malformed XML, unterminated XML, over-sized frame, invalid coordinates, and invalid time.

The current runner implements only fixed-position workloads. Marker, chat, non-fixed movement and ramp-down are rejected during validation rather than silently falling back to another behavior. Certificate-path templates are supported for distinct participant mTLS identities. `max_rate`, when present, lengthens the emission interval and never increases the configured GPS cadence.
