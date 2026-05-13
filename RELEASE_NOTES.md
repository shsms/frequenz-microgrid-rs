# Frequenz Microgrid Release Notes

## New Features

- `Bounds<Q>` now implements `Display`, rendering as `[lower, upper]` and showing `None` for either side when unset (e.g. `[-1 kW, 2 kW]`, `[None, 5]`).

- `Backoff` is a new re-export from the crate root, exposing the bounded-exponential-with-jitter backoff scheduler used internally by the client and logical meter. `Backoff::next_retry_time` records a failure and returns the `tokio::time::Instant` at which the next attempt is due; sleep-and-retry callers `sleep_until` on it, and `select!`-driven callers poll for due retries via `Backoff::take_due(now)`. Schedules are configured via `BackoffConfig::try_new(initial, max, multiplier, jitter)`, which validates that the values produce a sensible schedule.

- The per-component telemetry-stream reconnect schedule now uses bounded exponential backoff with jitter (1s → 30s, ±25% by default) instead of a 3s linear schedule. Use the new `MicrogridClientHandle::new_from_client_with_backoff_config` to override the schedule.

- `LogicalMeterHandle::try_new` now retries failed component-graph builds with the same bounded-exponential-with-jitter schedule, instead of sleeping a flat 3s between attempts. A fleet of instances restarting at the same time against a struggling backend will spread their retries instead of lock-stepping.
