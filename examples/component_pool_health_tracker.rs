// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

use std::time::Duration;

use frequenz_microgrid::{
    ComponentPoolHealthTracker, Error, MicrogridClientHandle,
    proto::common::microgrid::components::ComponentStateCode,
};

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt::fmt()
        .with_file(true)
        .with_line_number(true)
        .init();

    let client = MicrogridClientHandle::new("http://[::1]:8800");
    // let mut logical_meter = LogicalMeterHandle::try_new(
    //     client,
    //     LogicalMeterConfig {
    //         resampling_interval: TimeDelta::try_seconds(1).unwrap(),
    //     },
    // )
    // .await?;

    let (tx, mut rx) = tokio::sync::mpsc::channel(100);

    let tracker = ComponentPoolHealthTracker::try_new(
        vec![381, 382, 383, 384, 385, 386],
        Duration::from_secs(10), // Missing data tolerance
        vec![
            ComponentStateCode::Standby,
            ComponentStateCode::Ready,
            ComponentStateCode::Charging,
            ComponentStateCode::Discharging,
        ]
        .into_iter()
        .collect(),
        client.clone(),
        tx,
    )
    .await?;

    tokio::spawn(tracker.run());

    while let Some(status) = rx.recv().await {
        match status {
            component_pool_health_tracker::ComponentPoolStatus {
                healthy_components,
                unhealthy_components,
            } => {
                println!(
                    "Healthy components: {:?}, Unhealthy components: {:?}",
                    healthy_components, unhealthy_components
                );
            }
        }
    }

    Ok(())
}
