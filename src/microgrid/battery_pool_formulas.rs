// License: MIT
// Copyright © 2026 Frequenz Energy-as-a-Service GmbH

//! The expressions behind a battery pool's SoC and capacity formulas.
//!
//! Every expression is built per battery from the pool's inverter-battery
//! groups: a sum for the usable capacity, a weighted mean for the SoC.
//! Each battery is gated on the health of the inverters in its group. Only
//! the first bounds entry of a SoC sample is read, as the Python SDK does;
//! a battery without SoC bounds contributes nothing.

use crate::client::proto::common::metrics::Metric as MetricPb;
use crate::microgrid::telemetry_tracker::battery_pool_telemetry_tracker::InverterBatteryGroup;
use crate::{Expr, FormulaExpr, Key, Source};

/// The pool's usable capacity in watt-hours: the sum over its batteries
/// of `capacity * max(upper - lower, 0) / 100`, where `lower` and `upper`
/// are the battery's SoC bounds in percent.
///
/// `None` when no battery contributes.
#[allow(dead_code)]
pub(crate) fn usable_capacity(groups: &[InverterBatteryGroup]) -> FormulaExpr {
    let (totals, flags): (Vec<_>, Vec<_>) = per_battery(groups, |battery, gate| {
        let usable = battery_usable(battery, gate);
        (
            usable.clone().coalesce(constant(0.0)),
            present(usable).coalesce(constant(0.0)),
        )
    })
    .unzip();
    let contributing = sum(flags);
    // `contributing / contributing` is 1 while at least one battery
    // contributes and `None` when none does.
    sum(totals) * (contributing.clone() / contributing)
}

fn leaf(component_id: u64, source: Source) -> FormulaExpr {
    Expr::Component(Key {
        component_id,
        source,
    })
}

fn constant(value: f32) -> FormulaExpr {
    Expr::Constant(Some(value))
}

/// 1 while `expr` has a reading, `None` while it has none.
fn present(expr: FormulaExpr) -> FormulaExpr {
    expr * constant(0.0) + constant(1.0)
}

/// `f` over every battery of every group, given the group's health gate.
fn per_battery<'a, T>(
    groups: &'a [InverterBatteryGroup],
    f: impl Fn(u64, FormulaExpr) -> T + Copy + 'a,
) -> impl Iterator<Item = T> + 'a {
    groups.iter().flat_map(move |group| {
        let gate = gate(group);
        group
            .battery_ids
            .iter()
            .map(move |&battery| f(battery, gate.clone()))
    })
}

/// The product of the health of every inverter in `group`: 1 while all
/// are healthy, `None` otherwise.
fn gate(group: &InverterBatteryGroup) -> FormulaExpr {
    group
        .inverter_ids
        .iter()
        .map(|&inverter| leaf(inverter, Source::Health))
        .reduce(|product, health| product * health)
        .unwrap_or_else(|| constant(1.0))
}

/// `capacity * max(upper - lower, 0) / 100 * gate` for one battery, in
/// watt-hours.
fn battery_usable(battery: u64, gate: FormulaExpr) -> FormulaExpr {
    let capacity = leaf(battery, Source::Value(MetricPb::BatteryCapacity));
    capacity * soc_span(battery) / constant(100.0) * gate
}

/// `max(upper - lower, 0)` for one battery's SoC bounds, in percent.
fn soc_span(battery: u64) -> FormulaExpr {
    let lower = leaf(battery, Source::LowerBound(MetricPb::BatterySocPct));
    let upper = leaf(battery, Source::UpperBound(MetricPb::BatterySocPct));
    (upper - lower).max(constant(0.0))
}

/// The sum of `terms`, or the known-missing constant when there are none.
fn sum(terms: impl IntoIterator<Item = FormulaExpr>) -> FormulaExpr {
    terms
        .into_iter()
        .reduce(|sum, term| sum + term)
        .unwrap_or(Expr::Constant(None))
}

#[cfg(test)]
mod tests {
    use super::*;
    use frequenz_microgrid_formula_engine::Reading;
    use std::collections::{BTreeSet, HashMap};

    fn group(inverters: &[u64], batteries: &[u64]) -> InverterBatteryGroup {
        InverterBatteryGroup::new(
            inverters.iter().copied().collect::<BTreeSet<_>>(),
            batteries.iter().copied().collect::<BTreeSet<_>>(),
        )
    }

    fn key(component_id: u64, source: Source) -> Key {
        Key {
            component_id,
            source,
        }
    }

    /// Readings for one battery: capacity in Wh, SoC bounds and SoC in
    /// percent. `None` is a known-missing reading.
    fn battery(
        id: u64,
        capacity: Option<f32>,
        lower: Option<f32>,
        upper: Option<f32>,
        soc: Option<f32>,
    ) -> [(Key, Option<f32>); 4] {
        [
            (key(id, Source::Value(MetricPb::BatteryCapacity)), capacity),
            (key(id, Source::LowerBound(MetricPb::BatterySocPct)), lower),
            (key(id, Source::UpperBound(MetricPb::BatterySocPct)), upper),
            (key(id, Source::Value(MetricPb::BatterySocPct)), soc),
        ]
    }

    fn healthy(inverter_id: u64) -> (Key, Option<f32>) {
        (key(inverter_id, Source::Health), Some(1.0))
    }

    fn unhealthy(inverter_id: u64) -> (Key, Option<f32>) {
        (key(inverter_id, Source::Health), None)
    }

    fn evaluate(
        expr: &FormulaExpr,
        readings: impl IntoIterator<Item = (Key, Option<f32>)>,
    ) -> Option<f32> {
        let mut values: HashMap<Key, Option<f32>> = readings.into_iter().collect();
        match expr.evaluate(&mut values).unwrap() {
            Reading::Known(value) => value,
            Reading::Unknown => panic!("every leaf must have a reading"),
        }
    }

    #[test]
    fn usable_capacity_sums_the_bounded_share_of_each_battery() {
        let expr = usable_capacity(&[group(&[3], &[4, 5])]);
        let value = evaluate(
            &expr,
            battery(4, Some(1000.0), Some(10.0), Some(90.0), Some(50.0))
                .into_iter()
                .chain(battery(
                    5,
                    Some(2000.0),
                    Some(20.0),
                    Some(100.0),
                    Some(50.0),
                ))
                .chain([healthy(3)]),
        );
        assert_eq!(value, Some(800.0 + 1600.0));
    }

    #[test]
    fn usable_capacity_skips_a_battery_with_a_missing_input() {
        let expr = usable_capacity(&[group(&[3], &[4, 5])]);
        let value = evaluate(
            &expr,
            battery(4, Some(1000.0), Some(10.0), None, Some(50.0))
                .into_iter()
                .chain(battery(
                    5,
                    Some(2000.0),
                    Some(20.0),
                    Some(100.0),
                    Some(50.0),
                ))
                .chain([healthy(3)]),
        );
        assert_eq!(value, Some(1600.0));
    }

    #[test]
    fn usable_capacity_skips_a_group_with_an_unhealthy_inverter() {
        let expr = usable_capacity(&[group(&[3], &[4]), group(&[6, 7], &[8])]);
        let value = evaluate(
            &expr,
            battery(4, Some(1000.0), Some(10.0), Some(90.0), Some(50.0))
                .into_iter()
                .chain(battery(
                    8,
                    Some(2000.0),
                    Some(20.0),
                    Some(100.0),
                    Some(50.0),
                ))
                .chain([healthy(3), healthy(6), unhealthy(7)]),
        );
        assert_eq!(value, Some(800.0));
    }

    #[test]
    fn usable_capacity_of_inverted_bounds_is_zero() {
        let expr = usable_capacity(&[group(&[3], &[4])]);
        let value = evaluate(
            &expr,
            battery(4, Some(1000.0), Some(90.0), Some(10.0), Some(50.0))
                .into_iter()
                .chain([healthy(3)]),
        );
        assert_eq!(value, Some(0.0));
    }

    #[test]
    fn usable_capacity_is_none_without_a_contributing_battery() {
        let expr = usable_capacity(&[group(&[3], &[4])]);
        let value = evaluate(
            &expr,
            battery(4, None, None, None, None)
                .into_iter()
                .chain([healthy(3)]),
        );
        assert_eq!(value, None);
        assert_eq!(evaluate(&usable_capacity(&[]), std::iter::empty()), None);

        let partial = usable_capacity(&[group(&[3], &[4, 5])]);
        let value = evaluate(
            &partial,
            battery(4, None, None, None, None)
                .into_iter()
                .chain(battery(
                    5,
                    Some(2000.0),
                    Some(20.0),
                    Some(100.0),
                    Some(50.0),
                ))
                .chain([healthy(3)]),
        );
        assert_eq!(
            value,
            Some(1600.0),
            "a non-contributing battery must not blank a contributing one"
        );
    }
}
