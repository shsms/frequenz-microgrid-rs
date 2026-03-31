// License: MIT
// Copyright © 2026 Frequenz Energy-as-a-Service GmbH

//! A component health tracker that monitors the health of an electrical
//! component based on its telemetry data.

use std::{collections::HashSet, time::Duration};

use tokio::{
    select,
    sync::{broadcast, mpsc},
};

use crate::proto::common::microgrid::electrical_components::{
    ElectricalComponentStateCode, ElectricalComponentTelemetry,
};

pub(crate) struct ComponentHealthTracker {
    component_id: u64,
    missing_data_tolerance: Duration,
    component_data_rx: broadcast::Receiver<ElectricalComponentTelemetry>,
    component_status_tx: mpsc::Sender<ComponentHealthStatus>,
    healthy_state_codes: HashSet<ElectricalComponentStateCode>,
}

#[derive(PartialEq, Clone, Debug)]
pub(crate) enum ComponentHealthStatus {
    Healthy(u64),
    Unhealthy(u64),
}

impl ComponentHealthTracker {
    pub(super) fn new(
        component_id: u64,
        missing_data_tolerance: Duration,
        healthy_state_codes: HashSet<ElectricalComponentStateCode>,
        component_data_rx: broadcast::Receiver<ElectricalComponentTelemetry>,
        component_status_tx: mpsc::Sender<ComponentHealthStatus>,
    ) -> Self {
        Self {
            component_id,
            missing_data_tolerance,
            component_data_rx,
            component_status_tx,
            healthy_state_codes,
        }
    }

    pub(super) fn component_id(&self) -> u64 {
        self.component_id
    }

    fn state_from_data(&self, data: &ElectricalComponentTelemetry) -> ComponentHealthStatus {
        for state in data.state_snapshots.iter() {
            if !state.errors.is_empty() {
                return ComponentHealthStatus::Unhealthy(self.component_id);
            }
            for state in state.states.iter() {
                let Ok(state) = ElectricalComponentStateCode::try_from(*state) else {
                    tracing::warn!(
                        "Component {} has an invalid state code: {}",
                        self.component_id,
                        state
                    );
                    return ComponentHealthStatus::Unhealthy(self.component_id);
                };
                if !self.healthy_state_codes.contains(&state) {
                    return ComponentHealthStatus::Unhealthy(self.component_id);
                }
            }
        }
        ComponentHealthStatus::Healthy(data.electrical_component_id)
    }

    pub async fn run(mut self) {
        let mut interval = tokio::time::interval(self.missing_data_tolerance);
        let mut previous_status = ComponentHealthStatus::Unhealthy(self.component_id);
        loop {
            select! {
                component_data = self.component_data_rx.recv() => {
                    match component_data {
                        Ok(data) => {
                            // Reset the interval timer on receiving valid data
                            interval.reset();
                            let current_status = self.state_from_data(&data);
                            if current_status == previous_status {
                                continue;
                            }
                            previous_status = current_status.clone();
                            if let Err(e) = self.component_status_tx.send(current_status).await {
                                tracing::error!("Failed to send component status: {}", e);
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            continue;
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            drop(self.component_status_tx);
                            break;
                        }
                    }
                }
                _ = interval.tick() => {
                    // If we reach here, it means no data was received within the tolerance period
                    let current_status = ComponentHealthStatus::Unhealthy(self.component_id);
                    if previous_status == current_status {
                        continue;
                    }
                    previous_status = current_status.clone();
                    if let Err(e) = self.component_status_tx.send(current_status).await {
                        tracing::error!("Failed to send component status: {}", e);
                    }
                }
            }
        }
    }
}
