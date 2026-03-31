use std::collections::HashMap;

use crate::proto::common::{
    metrics::{Metric, metric_value_variant},
    microgrid::electrical_components::ElectricalComponentTelemetry,
};

trait Distributor {
    fn new(electrical_components_telemetry: HashMap<u64, ElectricalComponentTelemetry>) -> Self;
}

struct BatteryDistributor {
    electrical_components_telemetry: HashMap<u64, ElectricalComponentTelemetry>,
    total_soc: f32,
}

impl Distributor for BatteryDistributor {
    fn new(electrical_components_telemetry: HashMap<u64, ElectricalComponentTelemetry>) -> Self {
        let total_soc = electrical_components_telemetry
            .values()
            .filter_map(|telemetry| {
                telemetry
                    .metric_samples
                    .iter()
                    .find(|x| x.metric == Metric::BatterySocPct as i32)
                    .and_then(|sample| sample.value.as_ref())
                    .and_then(|value| value.metric_value_variant.as_ref())
                    .map(|variant| match variant {
                        metric_value_variant::MetricValueVariant::SimpleMetric(
                            simple_metric_value,
                        ) => simple_metric_value.value,
                        metric_value_variant::MetricValueVariant::AggregatedMetric(
                            aggregated_metric_value,
                        ) => aggregated_metric_value.avg_value,
                    })
            })
            .sum();
        Self {
            electrical_components_telemetry,
            total_soc,
        }
    }
}

fn alloc<D: Distributor>(
    power: f64,
    bounds: Option<(f64, f64)>,
    distributor: &D,
    previous_allocations: HashMap<u64, f64>,
) -> HashMap<u64, f64> {
    previous_allocations
}
