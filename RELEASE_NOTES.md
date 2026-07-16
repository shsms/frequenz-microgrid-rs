# Frequenz Microgrid Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

- The generated API types now come from the `frequenz-api-microgrid` crate. The re-export paths under `client::proto` stay the same. The newer protos add a `SteamBoiler` component category, so an exhaustive `match` on `ElectricalComponentCategory` needs a new arm.

- `LogicalMeterHandle::graph()` now returns a `ComponentGraph<GraphComponent, GraphConnection>`. The wrapper types deref to `ElectricalComponent` / `ElectricalComponentConnection`, so field access keeps working. Use `.0` to get the inner value.

- The `is_inverter`, `is_pv_inverter`, `is_battery_inverter` and `is_hybrid_inverter` helpers moved from `ElectricalComponent` to `GraphComponent`. Call them on components returned by the graph, or wrap a bare component first: `GraphComponent(component).is_pv_inverter()`. They are also stricter now: a component only counts as a PV, battery, or hybrid inverter when its category is `Inverter` too. Before, only the inverter type info was checked. The helpers now agree with how the component graph classifies a component.

- `BatteryPoolTelemetryTracker` and `PvPoolTelemetryTracker` are no longer public; they were an implementation detail. Use `BatteryPool::telemetry_snapshots()` / `PvPool::telemetry_snapshots()` to consume their snapshots.

- `PvPoolSnapshot` now exposes a single `inverters: ComponentHealthPartition` instead of the separate `healthy_inverters` / `unhealthy_inverters` maps:

  - `snapshot.healthy_inverters` → `snapshot.inverters.healthy`
  - `snapshot.unhealthy_inverters` → `snapshot.inverters.unhealthy`

- `InverterBatteryGroupStatus` (reached via `BatteryPoolSnapshot::groups()`) now groups its telemetry into `inverters: ComponentHealthPartition` and `batteries: ComponentHealthPartition`:

  - `status.healthy_inverters` → `status.inverters.healthy`
  - `status.unhealthy_inverters` → `status.inverters.unhealthy`
  - `status.healthy_batteries` → `status.batteries.healthy`
  - `status.unhealthy_batteries` → `status.batteries.unhealthy`

## New Features

- `PvPool` and `BatteryPool` can now be constructed empty, yielding a valid pool (zero power, empty bounds, empty snapshots) instead of an error, from either:

  - an explicit empty component set, or
  - `None` on a microgrid with no components of that kind.

- A new subscriber to a pool's `telemetry_snapshots()` or `power_bounds()` is now sent the pool's current snapshot / bounds immediately, instead of blocking until the next update.

- `ComponentGraphConfig` is now re-exported, so `LogicalMeterConfig::with_component_graph_config` can be called without depending on the component-graph crate directly.

- Logical-meter formulas can now use meter subtraction and summation from the component graph. When a component has no reading of its own, its metric can be computed from the meters around it. For example, a battery's AC active power becomes `COALESCE(#8, #5 - #6, 0.0)`: its own reading, else the parent meter minus its sibling, else zero. The subtraction needs readings from all the meters involved; if one of them is also missing, the formula still falls back to zero. Before, such metrics fell back to zero in more topologies.

- Category formulas (grid, battery, PV, ...) now prefer the sum of the components' own readings over a shared meter. For example, a PV pool's power was `COALESCE(#3, ...)` (meter first) and is now `COALESCE(#5 + #4, #3, ...)` (inverter sum first). The meter is still used as a fallback when a component reading is missing.

## Bug Fixes

- The pool, group, and component telemetry trackers no longer leak their tasks (while logging at error level every tick) once their consumers are gone; normal shutdown is now logged at debug.

- The client now evicts ended per-component telemetry streams from its cache, so a pool recreated on the same client receives telemetry again instead of silently getting none.

- Constructing a `BatteryPool` from a partial inverter-battery group (e.g. only one battery of a group that shares an inverter) is now rejected at construction with an error, instead of being accepted and later surfacing as an empty snapshot indistinguishable from a valid empty pool.
