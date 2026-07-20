# Scenarios and profiles

The built-in fixed CoT position scenario supports explicit participant roles, local routing observations, batching, fragmentation, bounded reconnect and timeouts. Routing assertions observe correlations only; they do not configure groups, permissions, or any server policy.

Profiles are safe templates: `smoke` is one client at 30 seconds; `functional` is 1–10 low-rate clients; `load` targets 10–250 staged clients; `stress` is local or temporary only and threshold-bound; `spike` drives controlled simultaneous actions; `soak` is moderate long-running load; `reconnect` uses configured backoff. Production only accepts `smoke`.

`smoke` is suitable for an authorized production check without aggressive load. `functional` is for local or staging. `load` (10–250 clients) needs an authorized environment. `stress`, `spike`, `soak`, slow readers, slow first writes, abrupt disconnects, and invalid payloads are local/temporary only, except where staging guardrails explicitly permit invalid events. Production is not a stress-test environment.

Invalid payloads are opt-in, bounded by `max_events`, rate-limited to one attempt per second, and blocked in production. Supported cases are malformed XML, unterminated XML, over-sized frame, invalid coordinates, and invalid time.
