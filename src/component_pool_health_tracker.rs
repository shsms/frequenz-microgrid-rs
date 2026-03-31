// License: MIT
// Copyright © 2026 Frequenz Energy-as-a-Service GmbH

//! A component pool health tracker that monitors the health of a pool of
//! electrical components based on their telemetry data.

mod component_health_tracker;

use std::{collections::HashSet, time::Duration};

use crate::{
    Error, MicrogridClientHandle,
    proto::common::microgrid::electrical_components::ElectricalComponentStateCode,
};

use component_health_tracker::{ComponentHealthStatus, ComponentHealthTracker};

pub struct ComponentPoolHealthTracker {
    component_ids: Vec<u64>,
    component_health_trackers: Vec<ComponentHealthTracker>,
    component_status_rx: tokio::sync::mpsc::Receiver<ComponentHealthStatus>,
    component_pool_status_tx: tokio::sync::mpsc::Sender<ComponentPoolStatus>,
}

pub struct ComponentPoolStatus {
    pub healthy_components: HashSet<u64>,
    pub unhealthy_components: HashSet<u64>,
}

impl ComponentPoolHealthTracker {
    pub async fn try_new(
        component_ids: Vec<u64>,
        missing_data_tolerance: Duration,
        healthy_state_codes: HashSet<ElectricalComponentStateCode>,
        client: MicrogridClientHandle,
        component_pool_status_tx: tokio::sync::mpsc::Sender<ComponentPoolStatus>,
    ) -> Result<Self, Error> {
        let mut component_health_trackers = Vec::with_capacity(component_ids.len());
        let (component_status_tx, component_status_rx) = tokio::sync::mpsc::channel(100); // Example channel
        for component_id in &component_ids {
            let component_data_stream = client
                .receive_electrical_component_telemetry_stream(*component_id)
                .await?;
            let tracker = ComponentHealthTracker::new(
                *component_id,
                missing_data_tolerance,
                healthy_state_codes.clone(),
                component_data_stream,
                component_status_tx.clone(),
            );
            component_health_trackers.push(tracker);
        }

        Ok(Self {
            component_ids,
            component_health_trackers,
            component_status_rx,
            component_pool_status_tx,
        })
    }

    pub async fn run(mut self) {
        let mut healthy_components = HashSet::new();
        let mut unhealthy_components = HashSet::new();

        for tracker in self.component_health_trackers {
            let component_id = tracker.component_id();
            // Spawn a task for each component health tracker
            tokio::spawn(async move {
                tracker.run().await;
            });
            // Initialize the health status
            unhealthy_components.insert(component_id);
        }

        while let Some(status) = self.component_status_rx.recv().await {
            println!("Received component status: {:?}", status);
            match status {
                ComponentHealthStatus::Healthy(component_id) => {
                    healthy_components.insert(component_id);
                    unhealthy_components.remove(&component_id);
                }
                ComponentHealthStatus::Unhealthy(component_id) => {
                    unhealthy_components.insert(component_id);
                    healthy_components.remove(&component_id);
                }
            }

            // Send the current pool status
            let pool_status = ComponentPoolStatus {
                healthy_components: healthy_components.clone(),
                unhealthy_components: unhealthy_components.clone(),
            };
            if let Err(e) = self.component_pool_status_tx.send(pool_status).await {
                tracing::error!("Failed to send component pool status: {}", e);
            }
        }
        tracing::error!(
            "Component pool health tracker(component IDs {:?}) stopped receiving component status updates.",
            self.component_ids
        );
    }
}
