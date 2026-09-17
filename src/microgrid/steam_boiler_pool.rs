// License: MIT
// Copyright © 2026 Frequenz Energy-as-a-Service GmbH

//! Representation of a pool of steam boilers in the microgrid.
//!
//! A [`SteamBoilerPool`] aggregates a set of steam boilers — either an explicit
//! subset or every steam boiler in the microgrid — and exposes their combined
//! active power.
//!
//! Obtain one from [`Microgrid::steam_boiler_pool`].
//!
//! [`Microgrid::steam_boiler_pool`]: crate::Microgrid::steam_boiler_pool

use std::collections::BTreeSet;

use crate::{
    Error, Formula, LogicalMeterHandle, MicrogridClientHandle,
    client::proto::common::microgrid::electrical_components::ElectricalComponentCategory, metric,
    microgrid::pool_validation::validate_pool_ids, quantity::Power,
};

/// A pool of steam boilers in the microgrid.
///
/// Created with [`Microgrid::steam_boiler_pool`][mg], passing either an
/// explicit set of steam boiler component IDs or `None` to cover every steam
/// boiler in the microgrid. It exposes:
///
/// - [`power`](Self::power) — a [`Formula`] for the pool's aggregate active
///   power.
///
/// [mg]: crate::Microgrid::steam_boiler_pool
pub struct SteamBoilerPool {
    component_ids: Option<BTreeSet<u64>>,
    client: MicrogridClientHandle,
    logical_meter: LogicalMeterHandle,
}

impl SteamBoilerPool {
    /// Creates a new `SteamBoilerPool` instance with the given component IDs,
    /// client and logical meter handles.
    ///
    /// When `component_ids` is `Some`, every ID must refer to a steam boiler in
    /// the component graph; otherwise an error is returned. When it is `None`,
    /// the pool covers all steam boilers in the microgrid.
    pub(crate) fn try_new(
        component_ids: Option<BTreeSet<u64>>,
        client: MicrogridClientHandle,
        logical_meter: LogicalMeterHandle,
    ) -> Result<Self, Error> {
        let this = Self {
            component_ids,
            client,
            logical_meter,
        };
        validate_pool_ids(
            &this.component_ids,
            &this.get_all_steam_boiler_ids(),
            "steam boilers",
        )
        .inspect_err(|e| tracing::error!("{e}"))?;
        Ok(this)
    }

    fn get_all_steam_boiler_ids(&self) -> BTreeSet<u64> {
        self.logical_meter
            .graph()
            .components()
            .filter(|c| c.category() == ElectricalComponentCategory::SteamBoiler)
            .map(|c| c.id)
            .collect()
    }

    /// Returns a formula for the active power of the steam boiler pool.
    pub fn power(&mut self) -> Result<Formula<Power>, Error> {
        self.logical_meter
            .steam_boiler::<metric::AcPowerActive>(self.component_ids.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::SteamBoilerPool;
    use crate::client::test_utils::MockComponent;
    use crate::microgrid::test_utils::handles;

    /// grid → meter → [boiler meter → steam_boiler(4), steam_boiler(5)],
    ///                 [chp meter → chp(7)]
    fn graph() -> MockComponent {
        MockComponent::grid(1).with_children(vec![MockComponent::meter(2).with_children(vec![
            MockComponent::meter(3).with_children(vec![
                MockComponent::steam_boiler(4),
                MockComponent::steam_boiler(5),
            ]),
            MockComponent::meter(6).with_children(vec![MockComponent::chp(7)]),
        ])])
    }

    #[tokio::test]
    async fn try_new_accepts_empty_component_ids() {
        let (client, lm) = handles(graph()).await;
        let mut pool = SteamBoilerPool::try_new(Some(BTreeSet::new()), client, lm)
            .expect("an empty component_ids set should yield an empty pool");
        pool.power().expect("empty pool power formula");
    }

    #[tokio::test]
    async fn try_new_rejects_non_steam_boiler_component_ids() {
        let (client, lm) = handles(graph()).await;
        // 7 is a CHP: a valid component, but not a steam boiler.
        let err = SteamBoilerPool::try_new(Some([7].into()), client, lm)
            .err()
            .expect("a CHP ID should be rejected");
        assert!(
            err.to_string().contains("must be steam boilers"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn try_new_rejects_unknown_component_ids() {
        let (client, lm) = handles(graph()).await;
        // 99 is not in the graph at all.
        let err = SteamBoilerPool::try_new(Some([4, 99].into()), client, lm)
            .err()
            .expect("an unknown ID should be rejected");
        assert!(
            err.to_string().contains("must be steam boilers"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn power_formula_for_all_steam_boilers() {
        let (client, lm) = handles(graph()).await;
        let mut pool = SteamBoilerPool::try_new(None, client, lm).unwrap();
        assert_eq!(
            pool.power().unwrap().to_string(),
            "METRIC_AC_POWER_ACTIVE::(COALESCE(#5 + #4, #3, COALESCE(#5, 0.0) + COALESCE(#4, 0.0)))"
        );
    }

    #[tokio::test]
    async fn power_formula_for_explicit_steam_boiler() {
        let (client, lm) = handles(graph()).await;
        let mut pool = SteamBoilerPool::try_new(Some([4].into()), client, lm).unwrap();
        assert_eq!(
            pool.power().unwrap().to_string(),
            "METRIC_AC_POWER_ACTIVE::(COALESCE(#4, #3 - #5, 0.0))"
        );
    }
}
