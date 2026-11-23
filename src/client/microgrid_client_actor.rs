// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! The microgrid client actor that handles communication with the microgrid API.

use crate::client::{
    MicrogridApiClient,
    instruction::Instruction,
    proto::common::microgrid::electrical_components::ElectricalComponentTelemetry,
    proto::microgrid::{
        ListElectricalComponentConnectionsRequest, ListElectricalComponentsRequest,
        ReceiveElectricalComponentTelemetryStreamRequest,
        ReceiveElectricalComponentTelemetryStreamResponse, SetElectricalComponentPowerRequest,
    },
    proto::time,
    set_power,
};
use futures::{Stream, StreamExt};
use std::collections::HashMap;
use tokio::{
    select,
    sync::{broadcast, mpsc},
};
use tracing::Instrument as _;

use crate::Error;

/// How long to wait after a stream fails before retrying it.
const RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(3);

/// How long to wait for the service to start a telemetry stream. A start that
/// takes longer counts as failed, and is retried.
const STREAM_START_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// How long a retry round may keep starting due streams, one after another. The
/// round checks it after each start, so a slow start can take the round past
/// it.
const RETRY_ROUND_BUDGET: std::time::Duration = std::time::Duration::from_secs(1);

enum StreamStatus {
    Failed(u64),
    Ended(u64),
}

/// This actor owns the connection to the microgrid API and processes instructions
/// received from any connected `MicrogridClientHandle` instance.
///
/// It allows there to be multiple `MicrogridClientHandle` instances, all
/// sharing the same connection to the microgrid API.
pub(super) struct MicrogridClientActor<T> {
    client: T,
    instructions_rx: mpsc::Receiver<Instruction>,
    /// How long to wait for the answer to a list or bounds request.
    request_timeout: std::time::Duration,
}

impl<T: MicrogridApiClient> MicrogridClientActor<T> {
    pub(super) fn new_from_client(
        client: T,
        instructions_rx: mpsc::Receiver<Instruction>,
        request_timeout: std::time::Duration,
    ) -> Self {
        Self {
            client,
            instructions_rx,
            request_timeout,
        }
    }

    pub(super) async fn run(mut self) {
        let mut component_streams: HashMap<u64, broadcast::Sender<ElectricalComponentTelemetry>> =
            HashMap::new();

        let (stream_status_tx, mut stream_status_rx) = mpsc::channel(50);
        let mut retry_timer = tokio::time::interval(std::time::Duration::from_secs(1));
        retry_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut components_to_retry = HashMap::new();
        // Set once every handle is gone, so no instruction can come anymore.
        let mut all_handles_dropped = false;

        loop {
            // Without handles, the actor keeps running only the streams that
            // still have receivers, and stops when none is left.
            if all_handles_dropped && !any_stream_has_receivers(&component_streams) {
                tracing::debug!(
                    "MicrogridClientActor: All handles and receivers dropped, exiting."
                );
                return;
            }
            select! {
                instruction = self.instructions_rx.recv(), if !all_handles_dropped => {
                    let Some(instruction) = instruction else {
                        all_handles_dropped = true;
                        continue;
                    };
                    if let Err(e) = handle_instruction(
                        &mut self.client,
                        &mut component_streams,
                        &mut components_to_retry,
                        instruction,
                        stream_status_tx.clone(),
                        self.request_timeout,
                    ).await {
                        tracing::error!("MicrogridClientActor: Error handling instruction: {e}");
                    }
                }
                stream_status = stream_status_rx.recv() => {
                    match stream_status {
                        Some(StreamStatus::Failed(component_id)) => {
                            mark_stream_failed(&mut components_to_retry, component_id);
                        }
                        Some(StreamStatus::Ended(component_id)) => {
                            components_to_retry.remove(&component_id);
                            // A subscriber may have arrived between the stream
                            // task's no-receivers check and this message.
                            restart_or_forget(
                                &mut self.client,
                                &mut component_streams,
                                &mut components_to_retry,
                                component_id,
                                stream_status_tx.clone(),
                            )
                            .await;
                        }
                        None => {
                            tracing::error!("MicrogridClientActor: Stream status channel closed, exiting.");
                            return;
                        }
                    }
                }
                now = retry_timer.tick() => {
                    handle_retry_timer(
                        &mut self.client,
                        &mut component_streams,
                        &mut components_to_retry,
                        stream_status_tx.clone(),
                        now,
                    ).await;
                    // A slow round could leave the next tick due already. Wait
                    // a full period, so a waiting instruction gets a turn
                    // first.
                    retry_timer.reset();
                }
            }
        }
    }
}

/// Returns whether any component stream still has a receiver.
fn any_stream_has_receivers(
    component_streams: &HashMap<u64, broadcast::Sender<ElectricalComponentTelemetry>>,
) -> bool {
    component_streams.values().any(|tx| tx.receiver_count() > 0)
}

/// Handles the instructions received from the `MicrogridClientHandle` instances.
async fn handle_instruction<T: MicrogridApiClient>(
    client: &mut T,
    component_streams: &mut HashMap<u64, broadcast::Sender<ElectricalComponentTelemetry>>,
    components_to_retry: &mut HashMap<u64, tokio::time::Instant>,
    instruction: Instruction,
    stream_status_tx: mpsc::Sender<StreamStatus>,
    request_timeout: std::time::Duration,
) -> Result<(), Error> {
    match instruction {
        Instruction::ReceiveElectricalComponentTelemetryStream {
            electrical_component_id,
            response_tx,
        } => {
            // If a stream for the given component already exists, subscribe to
            // it and return.
            if let Some(stream) = component_streams.get(&electrical_component_id) {
                response_tx
                    .send(stream.subscribe())
                    .map_err(|_| Error::internal("failed to send response"))?;
                return Ok(());
            }

            // If a stream for the given electrical component does not exist,
            // create a new channel and start a task for streaming telemetry
            // from the API service into the channel.
            let (tx, rx) = broadcast::channel::<ElectricalComponentTelemetry>(100);
            component_streams.insert(electrical_component_id, tx.clone());
            start_electrical_component_telemetry_stream(
                client,
                components_to_retry,
                electrical_component_id,
                tx,
                stream_status_tx,
            )
            .await;

            response_tx.send(rx).map_err(|_| {
                tracing::error!("failed to send response");
                Error::internal("failed to send response")
            })?;
        }
        Instruction::ListElectricalComponents {
            response_tx,
            electrical_component_ids,
            electrical_component_categories,
        } => {
            let components = call_with_timeout(
                "list_components",
                request_timeout,
                client.list_electrical_components(ListElectricalComponentsRequest {
                    electrical_component_ids,
                    electrical_component_categories: electrical_component_categories
                        .into_iter()
                        .map(|c| c as i32)
                        .collect(),
                }),
            )
            .await
            .map(|r| r.electrical_components);

            response_tx
                .send(components)
                .map_err(|_| Error::internal("failed to send response"))?;
        }
        Instruction::ListElectricalComponentConnections {
            response_tx,
            source_electrical_component_ids,
            destination_electrical_component_ids,
        } => {
            let connections = call_with_timeout(
                "list_connections",
                request_timeout,
                client.list_electrical_component_connections(
                    ListElectricalComponentConnectionsRequest {
                        source_electrical_component_ids,
                        destination_electrical_component_ids,
                    },
                ),
            )
            .await
            .map(|r| r.electrical_component_connections);

            response_tx
                .send(connections)
                .map_err(|_| Error::internal("failed to send response"))?;
        }
        Instruction::AugmentElectricalComponentBounds {
            electrical_component_id,
            target_metric,
            bounds,
            request_lifetime,
            response_tx,
        } => {
            let response = call_with_timeout(
                "augment_electrical_component_bounds",
                request_timeout,
                client.augment_electrical_component_bounds(
                    crate::client::proto::microgrid::AugmentElectricalComponentBoundsRequest {
                        electrical_component_id,
                        target_metric: target_metric as i32,
                        bounds,
                        request_lifetime: time::request_lifetime_secs(request_lifetime),
                    },
                ),
            )
            .await
            .map(|r| {
                r.valid_until_time
                    .and_then(|t| match time::timestamp_to_datetime(t) {
                        dt @ Some(_) => dt,
                        None => {
                            tracing::error!(
                                concat!(
                                    "Received invalid valid_until_time in ",
                                    "AugmentElectricalComponentBoundsResponse: {:?}"
                                ),
                                t
                            );
                            None
                        }
                    })
            });

            response_tx
                .send(response)
                .map_err(|_| Error::internal("failed to send response"))?;
        }
        Instruction::SetElectricalComponentPower {
            electrical_component_id,
            power_type,
            power,
            request_lifetime,
            response_tx,
        } => {
            // The caller has given up, so don't set the power.
            if response_tx.is_closed() {
                return Ok(());
            }
            tokio::spawn(
                set_power::send_and_relay(
                    client.clone(),
                    SetElectricalComponentPowerRequest {
                        electrical_component_id,
                        power_type: power_type as i32,
                        power,
                        request_lifetime: time::request_lifetime_secs(request_lifetime),
                    },
                    response_tx,
                )
                .in_current_span(),
            );
        }
    }

    Ok(())
}

/// Waits at most `timeout` for `request` to complete, connecting included.
/// `name` identifies the request in errors and logs.
///
/// Returns an `ErrorKind::APIServerError` error if the request fails, or if it
/// gets no answer in time.
async fn call_with_timeout<R>(
    name: &str,
    timeout: std::time::Duration,
    request: impl Future<Output = Result<tonic::Response<R>, tonic::Status>>,
) -> Result<R, Error> {
    match tokio::time::timeout(timeout, request).await {
        Ok(Ok(response)) => Ok(response.into_inner()),
        Ok(Err(e)) => Err(Error::api_server_error(format!("{name} failed: {e}"))),
        Err(_) => {
            tracing::debug!(request = name, timeout = ?timeout, "Request got no answer in time");
            Err(Error::api_server_error(format!(
                "{name} got no answer within {timeout:?}"
            )))
        }
    }
}

/// Runs a retry round on a tick of the retry timer: restarts or forgets the
/// components whose stream retry is due, the one due longest first, see
/// [`restart_or_forget`].
///
/// Once the round has run for [`RETRY_ROUND_BUDGET`], it starts no more
/// streams, and the others wait for the next round.
async fn handle_retry_timer<T: MicrogridApiClient>(
    client: &mut T,
    component_streams: &mut HashMap<u64, broadcast::Sender<ElectricalComponentTelemetry>>,
    components_to_retry: &mut HashMap<u64, tokio::time::Instant>,
    stream_status_tx: mpsc::Sender<StreamStatus>,
    now: tokio::time::Instant,
) {
    // Due longest first: a stream is due a fixed delay after its last failure,
    // so streams that keep failing can't take every round from the others.
    let mut due: Vec<(tokio::time::Instant, u64)> = components_to_retry
        .iter()
        .filter(|(_, retry_at)| **retry_at <= now)
        .map(|(component_id, retry_at)| (*retry_at, *component_id))
        .collect();
    due.sort_unstable();
    let round_start = tokio::time::Instant::now();
    for (_, component_id) in due {
        restart_or_forget(
            client,
            component_streams,
            components_to_retry,
            component_id,
            stream_status_tx.clone(),
        )
        .await;
        // Each start can take up to `STREAM_START_TIMEOUT`, for example while
        // the service can't be reached. So once the round has run for
        // `RETRY_ROUND_BUDGET`, leave the other due streams for the next tick,
        // and let the actor handle instructions in between.
        if round_start.elapsed() >= RETRY_ROUND_BUDGET {
            break;
        }
    }
}

/// Restarts a component's stream if its sender still has receivers.
///
/// Otherwise forgets the component: nothing would write to the cached sender
/// anymore, so the next subscription starts a new stream instead.
async fn restart_or_forget<T: MicrogridApiClient>(
    client: &mut T,
    component_streams: &mut HashMap<u64, broadcast::Sender<ElectricalComponentTelemetry>>,
    components_to_retry: &mut HashMap<u64, tokio::time::Instant>,
    component_id: u64,
    stream_status_tx: mpsc::Sender<StreamStatus>,
) {
    match component_streams.get(&component_id) {
        Some(tx) if tx.receiver_count() > 0 => {
            let tx = tx.clone();
            start_electrical_component_telemetry_stream(
                client,
                components_to_retry,
                component_id,
                tx,
                stream_status_tx,
            )
            .await;
        }
        Some(_) => {
            tracing::debug!("Forgetting telemetry stream for {component_id}: no receivers");
            component_streams.remove(&component_id);
            components_to_retry.remove(&component_id);
        }
        None => {
            components_to_retry.remove(&component_id);
            tracing::error!("Component stream not found for restart: {component_id}");
        }
    }
}

/// Marks a new failure for a component's stream, so that the retry timer
/// restarts the stream [`RETRY_DELAY`] from now, replacing any earlier due
/// time.
fn mark_stream_failed(
    components_to_retry: &mut HashMap<u64, tokio::time::Instant>,
    component_id: u64,
) {
    components_to_retry.insert(component_id, tokio::time::Instant::now() + RETRY_DELAY);
}

/// Creates a new data stream for the given component ID and starts a task to
/// fetch data from it in a loop.
///
/// Marks a failure in `components_to_retry` if the stream does not start, and
/// removes the component from it if it does. The task reports to
/// `stream_status_tx` when the stream fails or ends later.
async fn start_electrical_component_telemetry_stream<T: MicrogridApiClient>(
    client: &mut T,
    components_to_retry: &mut HashMap<u64, tokio::time::Instant>,
    electrical_component_id: u64,
    tx: broadcast::Sender<ElectricalComponentTelemetry>,
    stream_status_tx: mpsc::Sender<StreamStatus>,
) {
    // A service can answer keepalive pings without answering the request, so
    // this timeout makes sure the wait ends.
    let started = tokio::time::timeout(
        STREAM_START_TIMEOUT,
        client.receive_electrical_component_telemetry_stream(
            ReceiveElectricalComponentTelemetryStreamRequest {
                electrical_component_id,
                filter: None,
            },
        ),
    )
    .await;
    let stream = match started {
        Ok(Ok(s)) => s.into_inner(),
        Ok(Err(e)) => {
            tracing::debug!("Failed to start telemetry stream for {electrical_component_id}: {e}",);
            mark_stream_failed(components_to_retry, electrical_component_id);
            return;
        }
        Err(_) => {
            tracing::warn!(
                component_id = electrical_component_id,
                timeout = ?STREAM_START_TIMEOUT,
                "Telemetry stream didn't start in time"
            );
            mark_stream_failed(components_to_retry, electrical_component_id);
            return;
        }
    };
    components_to_retry.remove(&electrical_component_id);

    // create a task to fetch data from the stream in a loop and put into a channel.
    tokio::spawn(
        run_electrical_component_telemetry_stream(
            stream,
            electrical_component_id,
            tx,
            stream_status_tx,
        )
        .in_current_span(),
    );
}

/// Runs a component's telemetry stream, then reports to the actor why it
/// stopped, unless the actor has stopped too.
async fn run_electrical_component_telemetry_stream(
    stream: impl Stream<Item = Result<ReceiveElectricalComponentTelemetryStreamResponse, tonic::Status>>
    + Unpin,
    electrical_component_id: u64,
    tx: broadcast::Sender<ElectricalComponentTelemetry>,
    stream_status_tx: mpsc::Sender<StreamStatus>,
) {
    // `forward_telemetry` takes the stream, so the stream is closed before this
    // task waits on the status channel. The server may limit open streams, and
    // the actor must not wait for a slot that this task holds.
    let status = forward_telemetry(stream, electrical_component_id, &tx).await;
    if stream_status_tx.send(status).await.is_err() {
        // The actor has stopped, normally because every client handle and
        // receiver is gone.
        tracing::debug!(
            "Not reporting stream status for {electrical_component_id}: client stopped"
        );
    }
}

/// Forwards telemetry from `stream` to `tx`. Returns [`StreamStatus::Ended`] as
/// soon as `tx` has no receivers, also while waiting for a message, and
/// [`StreamStatus::Failed`] if the stream stops first.
async fn forward_telemetry(
    mut stream: impl Stream<
        Item = Result<ReceiveElectricalComponentTelemetryStreamResponse, tonic::Status>,
    > + Unpin,
    electrical_component_id: u64,
    tx: &broadcast::Sender<ElectricalComponentTelemetry>,
) -> StreamStatus {
    let closed = tx.closed();
    tokio::pin!(closed);
    loop {
        let next_message = select! {
            biased;
            () = &mut closed => {
                tracing::debug!(
                    "Dropping ComponentData stream for component_id:{:?}",
                    electrical_component_id
                );
                return StreamStatus::Ended(electrical_component_id);
            }
            next = stream.next() => next,
        };
        let Some(message) = next_message else {
            tracing::error!(
                "get_component_data stream failed for {}",
                electrical_component_id,
            );
            break;
        };
        let data = match message {
            Ok(ReceiveElectricalComponentTelemetryStreamResponse { telemetry: Some(d) }) => d,
            Ok(ReceiveElectricalComponentTelemetryStreamResponse { telemetry: None }) => {
                tracing::warn!(
                    "get_component_data stream returned empty data for {}",
                    electrical_component_id
                );
                continue;
            }
            Err(e) => {
                tracing::warn!(
                    "get_component_data stream ended for {}: {:?}",
                    electrical_component_id,
                    e
                );
                break;
            }
        };

        // A send fails only when no receivers are left, and `closed` handles
        // that case.
        let _ = tx.send(data);
    }

    StreamStatus::Failed(electrical_component_id)
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, sync::Arc};

    use tokio::sync::broadcast;

    use super::{
        ElectricalComponentTelemetry, StreamStatus, run_electrical_component_telemetry_stream,
    };

    use crate::{
        MicrogridClientHandle,
        client::test_utils::{MockComponent, MockMicrogridApiClient, wait_for_open_streams},
    };

    /// Subscribes at the same time to every meter of a site with `count`
    /// meters, ids 100 and up, each sending `samples` samples before its stream
    /// ends. Returns the client too: the test must keep it alive.
    async fn subscribe_to_meters(
        count: u64,
        samples: usize,
    ) -> (
        MicrogridClientHandle,
        Vec<broadcast::Receiver<ElectricalComponentTelemetry>>,
    ) {
        let meters = (0..count)
            .map(|i| MockComponent::meter(100 + i).with_power(vec![1.0; samples]))
            .collect();
        let client = MicrogridClientHandle::new_from_client(MockMicrogridApiClient::new(
            MockComponent::grid(1)
                .with_children(vec![MockComponent::meter(2).with_children(meters)]),
        ));
        let receivers = futures::future::join_all((100..100 + count).map(|id| {
            let client = client.clone();
            async move {
                client
                    .receive_electrical_component_telemetry_stream(id)
                    .await
                    .unwrap()
            }
        }))
        .await;
        (client, receivers)
    }

    #[tokio::test(start_paused = true)]
    async fn test_many_subscribes_at_once_all_deliver() {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            let (_client, receivers) = subscribe_to_meters(200, 400).await;
            for mut rx in receivers {
                rx.recv().await.unwrap();
            }
        })
        .await
        .expect("the client actor stopped handling streams");
    }

    #[tokio::test(start_paused = true)]
    async fn test_many_streams_failing_at_once_all_reconnect() {
        // Every stream ends after its samples, all at the same time, and is
        // retried: one more message per stream means it reconnected.
        let samples = 10;
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let (_client, receivers) = subscribe_to_meters(100, samples).await;
            for mut rx in receivers {
                for _ in 0..=samples {
                    rx.recv().await.unwrap();
                }
            }
        })
        .await
        .expect("the client actor stopped handling streams");
    }

    #[tokio::test(start_paused = true)]
    async fn test_failed_stream_start_is_retried() {
        let client = MicrogridClientHandle::new_from_client(MockMicrogridApiClient::new(
            MockComponent::grid(1).with_children(vec![
                MockComponent::meter(2)
                    .with_power(vec![1.0; 10])
                    .with_failed_starts(2),
            ]),
        ));
        let start = tokio::time::Instant::now();
        let mut rx = client
            .receive_electrical_component_telemetry_stream(2)
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(30), rx.recv())
            .await
            .expect("the stream was not retried")
            .unwrap();
        // The retries after the two failed starts are due at 3 s and 6 s.
        assert!(start.elapsed() >= std::time::Duration::from_secs(6));
    }

    #[tokio::test(start_paused = true)]
    async fn test_stalled_stream_start_times_out_and_is_retried() {
        // The first start gets no answer for an hour.
        let client = MicrogridClientHandle::new_from_client(MockMicrogridApiClient::new(
            MockComponent::grid(1).with_children(vec![
                MockComponent::meter(2)
                    .with_power(vec![1.0; 10])
                    .with_failed_starts(1)
                    .with_failed_start_delay(std::time::Duration::from_secs(3600)),
            ]),
        ));
        let start = tokio::time::Instant::now();
        let mut rx = client
            .receive_electrical_component_telemetry_stream(2)
            .await
            .unwrap();
        assert_eq!(start.elapsed(), std::time::Duration::from_secs(10));
        tokio::time::timeout(std::time::Duration::from_secs(10), rx.recv())
            .await
            .expect("the stream was not retried after its start timed out")
            .unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn test_retry_round_tries_every_due_stream_that_fails_fast() {
        let api = MockMicrogridApiClient::new(MockComponent::grid(1).with_children(vec![
            MockComponent::meter(2)
                .with_power(vec![1.0; 10])
                .with_failed_starts(10),
            MockComponent::meter(3)
                .with_power(vec![1.0; 10])
                .with_failed_starts(10),
        ]));
        let attempts = api.telemetry_start_attempts();
        let client = MicrogridClientHandle::new_from_client(api);
        let _rx2 = client
            .receive_electrical_component_telemetry_stream(2)
            .await
            .unwrap();
        let _rx3 = client
            .receive_electrical_component_telemetry_stream(3)
            .await
            .unwrap();
        let total = || attempts.lock().unwrap().values().sum::<usize>();
        assert_eq!(total(), 2);

        // Both retries are due at 3 s, and both fail at once, so one round
        // tries both.
        tokio::time::sleep(std::time::Duration::from_millis(3500)).await;
        assert_eq!(total(), 4);
    }

    /// Returns a client whose service always rejects the streams of the meters
    /// in `ids`, each after `delay`, with a receiver for each of them, and
    /// rejects the stream of meter 2 once, without a delay.
    async fn rejected_streams_client(
        ids: std::ops::Range<u64>,
        delay: std::time::Duration,
    ) -> (
        MicrogridClientHandle,
        Vec<broadcast::Receiver<ElectricalComponentTelemetry>>,
    ) {
        let rejected = ids.clone().map(|id| {
            MockComponent::meter(id)
                .with_power(vec![1.0; 10])
                .with_failed_starts(usize::MAX)
                .with_failed_start_delay(delay)
        });
        let api = MockMicrogridApiClient::new(
            MockComponent::grid(1).with_children(
                rejected
                    .chain([MockComponent::meter(2)
                        .with_power(vec![1.0; 10])
                        .with_failed_starts(1)])
                    .collect(),
            ),
        );
        let client = MicrogridClientHandle::new_from_client(api);
        let mut receivers = Vec::new();
        for id in ids {
            receivers.push(
                client
                    .receive_electrical_component_telemetry_stream(id)
                    .await
                    .unwrap(),
            );
        }
        (client, receivers)
    }

    #[tokio::test(start_paused = true)]
    async fn test_fast_rejections_do_not_slow_other_telemetry_recovery() {
        let (client, _receivers) = rejected_streams_client(10..22, std::time::Duration::ZERO).await;
        // Later than the twelve, so meter 2 is due after them.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let mut working = client
            .receive_electrical_component_telemetry_stream(2)
            .await
            .unwrap();

        // Twelve older rejections must not take a tick each before meter 2
        // starts.
        tokio::time::timeout(std::time::Duration::from_secs(10), working.recv())
            .await
            .expect("fast rejections delayed a working stream")
            .unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn test_retry_round_tries_the_longest_due_stream_first() {
        // Twelve streams the service always rejects after 2 s, more than the
        // one slow failed start per round can keep on schedule, and one that
        // fails only once. A round stops after a slow failed start, but the
        // stream that waited longest must still get its turn. Each client has
        // its own retry map, so each run sees a new map order.
        for _ in 0..10 {
            let (client, _receivers) =
                rejected_streams_client(10..22, std::time::Duration::from_secs(2)).await;
            let mut rx = client
                .receive_electrical_component_telemetry_stream(2)
                .await
                .unwrap();
            tokio::time::timeout(std::time::Duration::from_secs(60), rx.recv())
                .await
                .expect("the retry of meter 2 was starved")
                .unwrap();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_a_backlog_of_failing_streams_doesnt_starve_a_new_retry() {
        // Twelve streams the service always rejects after 2 s fall behind their
        // retry schedule, because a round stops after one slow failed start. A
        // stream that fails much later must still be retried within one turn of
        // the twelve.
        let (client, _receivers) =
            rejected_streams_client(10..22, std::time::Duration::from_secs(2)).await;
        tokio::time::sleep(std::time::Duration::from_secs(600)).await;

        let mut rx = client
            .receive_electrical_component_telemetry_stream(2)
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(60), rx.recv())
            .await
            .expect("the retry of meter 2 waited behind the backlog")
            .unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn test_instruction_is_handled_between_slow_failed_retries() {
        // Three streams whose failed starts each take 10 s, like a connect
        // timeout. After each round, a waiting subscription must go before the
        // next round. The actor picks its next event at random, so 20 clients
        // make a false pass unlikely.
        for _ in 0..20 {
            let (client, _receivers) =
                rejected_streams_client(10..13, std::time::Duration::from_secs(10)).await;
            tokio::time::sleep(std::time::Duration::from_millis(60_500)).await;

            let start = tokio::time::Instant::now();
            let _rx = client
                .receive_electrical_component_telemetry_stream(2)
                .await
                .unwrap();
            let waited = start.elapsed();
            assert!(
                waited <= std::time::Duration::from_secs(10),
                "the subscription waited {waited:?}, behind more than one round"
            );
        }
    }

    /// Waits up to `tries` × 10 ms for the actor that owns the mock behind
    /// `shared` to stop, and returns whether it did.
    ///
    /// `shared` is part of the mock's shared state, such as
    /// `set_power_calls_handle()`. The actor owns the mock, so that state loses
    /// a holder when the actor stops.
    async fn actor_stopped<T>(shared: &Arc<T>, tries: u32) -> bool {
        for _ in 0..tries {
            if Arc::strong_count(shared) == 1 {
                return true;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        false
    }

    #[test]
    fn test_actor_retries_streams_with_receivers_after_every_handle_is_dropped() {
        // An actor that spins stalls paused time, so the test runs on its own
        // thread, and this thread fails it after a real-time limit instead of
        // hanging.
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let test = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .start_paused(true)
                .build()
                .unwrap();
            runtime.block_on(async {
                let api = MockMicrogridApiClient::new(MockComponent::grid(1).with_children(vec![
                    MockComponent::meter(2)
                        .with_power(vec![1.0; 400])
                        .with_failed_starts(1),
                ]));
                let calls = api.set_power_calls_handle();
                let client = MicrogridClientHandle::new_from_client(api);
                let mut rx = client
                    .receive_electrical_component_telemetry_stream(2)
                    .await
                    .unwrap();

                // The first start failed. The retry must still happen without a
                // handle.
                drop(client);
                tokio::time::timeout(std::time::Duration::from_secs(10), rx.recv())
                    .await
                    .expect("the stream was not retried after the handle was dropped")
                    .unwrap();

                // Without the receiver, nothing is left to keep the actor
                // running.
                drop(rx);
                assert!(
                    actor_stopped(&calls, 200).await,
                    "the actor kept running after every handle and receiver was dropped"
                );
            });
            let _ = done_tx.send(());
        });
        match done_rx.recv_timeout(std::time::Duration::from_secs(10)) {
            Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                if let Err(panic) = test.join() {
                    std::panic::resume_unwind(panic);
                }
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                panic!(
                    "the test didn't finish within 10 s of real time: something, \
                     such as a spinning actor, kept paused time from advancing"
                );
            }
        }
    }

    #[tokio::test]
    async fn test_actor_stops_when_every_handle_is_dropped() {
        let api = MockMicrogridApiClient::new(MockComponent::grid(1));
        let calls = api.set_power_calls_handle();
        let client = MicrogridClientHandle::new_from_client(api);
        let other_client = client.clone();

        drop(client);
        assert!(
            !actor_stopped(&calls, 5).await,
            "the actor stopped while a handle was left"
        );

        drop(other_client);
        assert!(
            actor_stopped(&calls, 200).await,
            "the actor kept running after every handle was dropped"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_failed_stream_is_not_retried_without_receivers() {
        let api = MockMicrogridApiClient::new(MockComponent::grid(1).with_children(vec![
            MockComponent::meter(2)
                .with_power(vec![1.0; 10])
                .with_failed_starts(1),
        ]));
        let attempts = api.telemetry_start_attempts();
        let client = MicrogridClientHandle::new_from_client(api);

        let rx = client
            .receive_electrical_component_telemetry_stream(2)
            .await
            .unwrap();
        drop(rx);
        tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        assert_eq!(
            attempts.lock().unwrap().get(&2),
            Some(&1),
            "a stream without receivers must not be retried",
        );

        // The component was forgotten, so a new subscription starts a new
        // stream.
        let mut rx = client
            .receive_electrical_component_telemetry_stream(2)
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv())
            .await
            .expect("the new subscription did not start a stream")
            .unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn test_stream_task_closes_its_stream_before_it_waits_to_report() {
        // A full status channel, so the task has to wait to report.
        let (status_tx, _status_rx) = tokio::sync::mpsc::channel(1);
        status_tx.try_send(StreamStatus::Failed(0)).unwrap();
        let (data_tx, data_rx) = tokio::sync::mpsc::channel(1);
        // No receivers, so the task stops at once.
        let (tx, _) = broadcast::channel(1);

        let task = tokio::spawn(run_electrical_component_telemetry_stream(
            tokio_stream::wrappers::ReceiverStream::new(data_rx),
            2,
            tx,
            status_tx,
        ));
        tokio::time::timeout(std::time::Duration::from_secs(1), data_tx.closed())
            .await
            .expect("the stream task kept its stream open");
        assert!(!task.is_finished());
    }

    /// Opens the stream of `meter`, reads one message, drops the receiver, and
    /// waits until the mock reports no open stream.
    async fn assert_stream_closes_when_receiver_drops(meter: MockComponent) {
        let id = meter.component.id;
        let api = MockMicrogridApiClient::new(MockComponent::grid(1).with_children(vec![meter]));
        let open = api.open_telemetry_streams();
        let client = MicrogridClientHandle::new_from_client(api);

        let mut rx = client
            .receive_electrical_component_telemetry_stream(id)
            .await
            .unwrap();
        let _ = rx.recv().await.unwrap();
        assert_eq!(open.lock().unwrap().ids(), BTreeSet::from([id]));

        drop(rx);
        wait_for_open_streams(
            &open,
            BTreeSet::new(),
            std::time::Duration::from_millis(200),
            10,
        )
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn test_mock_tracks_open_telemetry_streams() {
        // Enough samples to outlast the wait, so the stream can only close
        // because the receiver went away.
        assert_stream_closes_when_receiver_drops(
            MockComponent::meter(2).with_power(vec![1.0; 400]),
        )
        .await;
    }

    /// Starts an HTTP/2 server that answers pings and
    /// `ListElectricalComponents` calls, but never answers any other request,
    /// such as a telemetry stream request. Returns its address, and a notify
    /// that fires when such a request arrives.
    async fn server_that_never_starts_streams()
    -> (std::net::SocketAddr, std::sync::Arc<tokio::sync::Notify>) {
        use tonic::codegen::{Bytes, http};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let stream_requested = std::sync::Arc::new(tokio::sync::Notify::new());
        let notify = stream_requested.clone();
        tokio::spawn(async move {
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                let notify = notify.clone();
                tokio::spawn(async move {
                    let mut connection = h2::server::handshake(socket).await.unwrap();
                    // Unanswered requests stay open while the connection runs.
                    let mut unanswered = Vec::new();
                    while let Some(Ok((request, mut respond))) = connection.accept().await {
                        if !request.uri().path().ends_with("/ListElectricalComponents") {
                            unanswered.push((request, respond));
                            notify.notify_one();
                            continue;
                        }
                        let response = http::Response::builder()
                            .header("content-type", "application/grpc")
                            .body(())
                            .unwrap();
                        let mut body = respond.send_response(response, false).unwrap();
                        // An empty message: not compressed, zero bytes long.
                        body.send_data(Bytes::from_static(&[0, 0, 0, 0, 0]), false)
                            .unwrap();
                        let mut trailers = http::HeaderMap::new();
                        trailers.insert("grpc-status", http::HeaderValue::from_static("0"));
                        body.send_trailers(trailers).unwrap();
                    }
                });
            }
        });
        (address, stream_requested)
    }

    #[tokio::test]
    async fn test_stream_start_times_out_on_a_connection_that_answers_pings() {
        // The server answers the pings, so keepalive doesn't close the
        // connection, and only the start timeout ends the wait for the
        // telemetry stream.
        let (address, stream_requested) = server_that_never_starts_streams().await;
        let config = crate::MicrogridClientConfig::default().with_keep_alive(
            std::time::Duration::from_secs(1),
            std::time::Duration::from_secs(1),
        );
        let client =
            MicrogridClientHandle::try_new_with_config(format!("http://{address}"), config)
                .await
                .unwrap();

        let subscription = {
            let client = client.clone();
            tokio::spawn(async move {
                client
                    .receive_electrical_component_telemetry_stream(2)
                    .await
            })
        };
        // The actor is now waiting for the stream to start, so the list call
        // waits behind it.
        stream_requested.notified().await;
        let list_start = std::time::Instant::now();
        let components = tokio::time::timeout(
            std::time::Duration::from_secs(15),
            client.list_electrical_components(vec![], vec![]),
        )
        .await
        .expect("the stalled stream start held up the list call for good")
        .unwrap();
        assert!(components.is_empty());
        assert!(list_start.elapsed() >= std::time::Duration::from_secs(9));
        tokio::time::timeout(std::time::Duration::from_secs(15), subscription)
            .await
            .expect("the stalled stream start didn't time out")
            .unwrap()
            .unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn test_silent_component_stream_closes_when_its_receiver_drops() {
        // One sample, then silence: no message arrives after the receiver is
        // dropped, so only the dropped receiver can close the stream.
        assert_stream_closes_when_receiver_drops(
            MockComponent::meter(2)
                .with_power(vec![1.0])
                .with_silence_after_metrics(),
        )
        .await;
    }
}
