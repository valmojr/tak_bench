# Safety model

Every connection-opening command displays the mandated authorization warning and requires either `--acknowledge-authorization` or `authorization.acknowledged: true` in its YAML configuration.

Targets must match an exact `allow_hosts` entry. Loopback is implicitly allowed only in the `local` environment. TLS hostname verification remains enabled; no switch disables it.

Production requires `--environment production`, `--allow-production`, authorization acknowledgement and an explicit allowlist. Only `smoke` is accepted, with at most three actual participants, 15 minutes duration and 30-second or slower position cadence. Explicit participant counts must match `run.clients`, cannot exceed `max_clients`, and participant IDs and routing roles are validated before dialing. Stress, spike, soak, malformed events and disruptive client controls are rejected outside local or temporary environments.

Cancellation and deadlines cover ramp delays, connection attempts, reconnect backoff, slow first writes and slow reads. A participant failure cancels and joins the remaining workload before reporting; pending local batches are discarded and counted instead of being written after cancellation.
