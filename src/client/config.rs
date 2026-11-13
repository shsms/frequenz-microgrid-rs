// License: MIT
// Copyright © 2026 Frequenz Energy-as-a-Service GmbH

//! Connection settings for the microgrid API client.

use std::time::Duration;

use tonic::transport::Endpoint;

use crate::Error;

/// The longest keepalive ping interval and ping timeout.
const MAX_KEEP_ALIVE_DURATION: Duration = Duration::from_secs(3600);

/// Settings for the connection to the microgrid API.
///
/// The defaults are a connect timeout of 5 seconds, and an HTTP/2 keepalive
/// ping after 60 seconds without data from the service. The connection closes
/// if a ping gets no answer within 20 seconds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MicrogridClientConfig {
    /// How long to wait for the TCP connection to the service.
    pub(crate) connect_timeout: Duration,
    /// When to ping the service, and how long to wait for its answer, or `None`
    /// for no pings.
    pub(crate) keep_alive: Option<KeepAlive>,
}

/// HTTP/2 keepalive settings.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct KeepAlive {
    /// How long the service may send nothing before the client pings it.
    pub(crate) interval: Duration,
    /// How long to wait for the answer to a ping before closing the connection.
    pub(crate) timeout: Duration,
}

impl Default for MicrogridClientConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            keep_alive: Some(KeepAlive {
                interval: Duration::from_secs(60),
                timeout: Duration::from_secs(20),
            }),
        }
    }
}

impl MicrogridClientConfig {
    /// Sets how long to wait for the TCP connection to the service.
    ///
    /// The time doesn't include the DNS lookup. Several addresses of one IP
    /// version each get an equal share of the time, one after another. The
    /// other IP version starts 300 ms later, or when all addresses of the first
    /// version fail, with its own full time. A call that can't connect in time
    /// fails, and a telemetry stream retries later. It must be more than zero.
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Pings the service when it has sent nothing for `ping_interval`, also
    /// while no call is running, and closes the connection if a ping gets no
    /// answer within `ping_timeout`.
    ///
    /// This finds a service that went away without closing the connection, for
    /// example after a network change. Calls and streams on a closed connection
    /// fail, and telemetry streams reconnect. Both durations must be more than
    /// zero and at most an hour.
    pub fn with_keep_alive(mut self, ping_interval: Duration, ping_timeout: Duration) -> Self {
        self.keep_alive = Some(KeepAlive {
            interval: ping_interval,
            timeout: ping_timeout,
        });
        self
    }

    /// Turns keepalive pings off.
    ///
    /// Use this for a service that rejects keepalive pings. For a service that
    /// only rejects frequent pings, use [`Self::with_keep_alive`] with a longer
    /// interval instead.
    ///
    /// Without pings, a service that went away without closing the connection
    /// is not noticed while the client only waits for data, as a telemetry
    /// stream does. A new call on that connection fails only when the operating
    /// system gives up resending it, which can take many minutes.
    pub fn without_keep_alive(mut self) -> Self {
        self.keep_alive = None;
        self
    }

    /// Returns an `ErrorKind::InvalidConfig` error if a duration is zero, or a
    /// keepalive duration is more than an hour.
    pub(super) fn validate(&self) -> Result<(), Error> {
        if self.connect_timeout.is_zero() {
            return Err(Error::invalid_config(format!(
                "connect_timeout must be positive, got {:?}",
                self.connect_timeout
            )));
        }
        if let Some(KeepAlive { interval, timeout }) = self.keep_alive
            && [interval, timeout]
                .iter()
                .any(|d| d.is_zero() || *d > MAX_KEEP_ALIVE_DURATION)
        {
            return Err(Error::invalid_config(format!(
                "keepalive ping interval and timeout must be positive and at most \
                 {MAX_KEEP_ALIVE_DURATION:?}, got {interval:?} and {timeout:?}"
            )));
        }
        Ok(())
    }

    /// Returns `endpoint` with these settings.
    pub(super) fn apply_to(&self, endpoint: Endpoint) -> Endpoint {
        let endpoint = endpoint.connect_timeout(self.connect_timeout);
        match self.keep_alive {
            Some(KeepAlive { interval, timeout }) => endpoint
                .http2_keep_alive_interval(interval)
                .keep_alive_timeout(timeout)
                .keep_alive_while_idle(true),
            None => endpoint,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, time::Duration};

    use tokio::io::AsyncReadExt as _;
    use tonic::transport::Endpoint;

    use super::{KeepAlive, MicrogridClientConfig};
    use crate::{ErrorKind, MicrogridClientHandle};

    /// Starts a server that accepts connections and never answers, and returns
    /// its address.
    async fn silent_server() -> SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let mut connections = Vec::new();
            loop {
                let (connection, _) = listener.accept().await.unwrap();
                connections.push(connection);
            }
        });
        address
    }

    #[test]
    fn test_defaults() {
        let config = MicrogridClientConfig::default();

        assert_eq!(config.connect_timeout, Duration::from_secs(5));
        assert_eq!(
            config.keep_alive,
            Some(KeepAlive {
                interval: Duration::from_secs(60),
                timeout: Duration::from_secs(20),
            })
        );
    }

    #[test]
    fn test_builders() {
        let config = MicrogridClientConfig::default()
            .with_connect_timeout(Duration::from_secs(3))
            .with_keep_alive(Duration::from_secs(5), Duration::from_secs(2));
        assert_eq!(config.connect_timeout, Duration::from_secs(3));
        assert_eq!(
            config.keep_alive,
            Some(KeepAlive {
                interval: Duration::from_secs(5),
                timeout: Duration::from_secs(2),
            })
        );

        assert_eq!(config.without_keep_alive().keep_alive, None);
    }

    #[test]
    fn test_connect_timeout_is_set_on_the_endpoint() {
        let endpoint = Endpoint::from_static("http://localhost");
        let config = MicrogridClientConfig::default();
        assert_eq!(
            config.apply_to(endpoint.clone()).get_connect_timeout(),
            Some(Duration::from_secs(5))
        );

        let config = config.with_connect_timeout(Duration::from_secs(3));
        assert_eq!(
            config.apply_to(endpoint).get_connect_timeout(),
            Some(Duration::from_secs(3))
        );
    }

    /// Returns the error kind with which `try_new_with_config` rejects
    /// `config`, or `None` if it accepts it.
    async fn rejection(config: MicrogridClientConfig) -> Option<ErrorKind> {
        MicrogridClientHandle::try_new_with_config("http://localhost", config)
            .await
            .err()
            .map(|e| e.kind())
    }

    #[tokio::test]
    async fn test_zero_durations_are_rejected() {
        let zero = Duration::ZERO;
        let one = Duration::from_secs(1);
        for config in [
            MicrogridClientConfig::default().with_connect_timeout(zero),
            MicrogridClientConfig::default().with_keep_alive(zero, one),
            MicrogridClientConfig::default().with_keep_alive(one, zero),
        ] {
            assert_eq!(rejection(config).await, Some(ErrorKind::InvalidConfig));
        }
    }

    #[tokio::test]
    async fn test_keep_alive_durations_over_an_hour_are_rejected() {
        let hour = Duration::from_secs(3600);
        let over = hour + Duration::from_nanos(1);
        for (interval, timeout) in [(over, hour), (hour, over), (Duration::MAX, hour)] {
            let config = MicrogridClientConfig::default().with_keep_alive(interval, timeout);
            assert_eq!(rejection(config).await, Some(ErrorKind::InvalidConfig));
        }

        let config = MicrogridClientConfig::default().with_keep_alive(hour, hour);
        assert_eq!(rejection(config).await, None);
    }

    #[tokio::test]
    async fn test_keep_alive_closes_a_silent_connection() {
        let address = silent_server().await;
        let config = MicrogridClientConfig::default()
            .with_keep_alive(Duration::from_millis(100), Duration::from_millis(100));
        let handle =
            MicrogridClientHandle::try_new_with_config(format!("http://{address}"), config)
                .await
                .unwrap();

        let result = tokio::time::timeout(
            Duration::from_secs(5),
            handle.list_electrical_components(vec![], vec![]),
        )
        .await
        .expect("the keepalive pings should close the silent connection");

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_keep_alive_closes_a_silent_idle_connection() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let config = MicrogridClientConfig::default()
            .with_keep_alive(Duration::from_millis(100), Duration::from_millis(100));
        let endpoint = Endpoint::from_shared(format!("http://{address}")).unwrap();
        // Connect without a call, so the connection has nothing running.
        let _channel = config.apply_to(endpoint).connect().await.unwrap();
        let (mut connection, _) = listener.accept().await.unwrap();

        let closed = async {
            let mut buffer = [0; 1024];
            while matches!(connection.read(&mut buffer).await, Ok(n) if n > 0) {}
        };
        tokio::time::timeout(Duration::from_secs(5), closed)
            .await
            .expect("the keepalive pings should close the idle connection");
    }

    #[tokio::test(start_paused = true)]
    async fn test_try_new_uses_the_default_keep_alive() {
        let address = silent_server().await;
        let handle = MicrogridClientHandle::try_new(format!("http://{address}"))
            .await
            .unwrap();

        // With paused time, the clock jumps ahead while the call waits, so the
        // default ping (after 60 s) and its timeout (20 s) pass at once.
        let start = tokio::time::Instant::now();
        let result = tokio::time::timeout(
            Duration::from_secs(200),
            handle.list_electrical_components(vec![], vec![]),
        )
        .await
        .expect("the default keepalive pings should close the silent connection");

        assert!(result.is_err());
        assert!(
            start.elapsed() >= Duration::from_secs(80),
            "{:?}",
            start.elapsed()
        );
    }
}
