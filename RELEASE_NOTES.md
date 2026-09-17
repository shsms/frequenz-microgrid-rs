# Frequenz Microgrid Release Notes

## Summary

This release introduces the SteamBoilerPool, with support for streaming telemetry, bounds and health status.

## New Features

- `LogicalMeterHandle::steam_boiler::<M>()` streams a metric for a set of steam boilers.
- `Microgrid::steam_boiler_pool()` returns a `SteamBoilerPool` with the pool's active power, aggregated active-power bounds and health-partitioned telemetry snapshots.
- `test-utils`: `MockComponent::steam_boiler()` builds a steam boiler, and `MockComponent::add_sample_power_bounds()` attaches bounds to a component's streamed active-power samples.
