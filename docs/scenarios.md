# Scenarios and profiles

The initial scenario is a fixed CoT position. Position, marker, chat, routes, groups, invalid payloads and slow clients are introduced progressively.

Profiles are safe templates: `smoke` is one client at 30 seconds; `functional` is 1–10 low-rate clients; `load` targets 10–250 staged clients; `stress` is local or temporary only and threshold-bound; `spike` drives controlled simultaneous actions; `soak` is moderate long-running load; `reconnect` uses configured backoff. Production only accepts `smoke`.

Ramp strategies are immediate, linear, step and seeded randomized. A step ramp is a monotonic list of `{ at, clients }`; ramp-down closes clients gradually.
