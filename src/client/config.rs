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
/// The defaults are a connect timeout of 5 seconds, a timeout of 10 seconds for
/// list and bounds requests, and an HTTP/2 keepalive ping after 60 seconds
/// without data from the service. The connection closes if a ping gets no
/// answer within 20 seconds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MicrogridClientConfig {
    /// How long to wait for the TCP connection to the service.
    pub(crate) connect_timeout: Duration,
    /// How long to wait for the answer to a list or bounds request.
    pub(crate) request_timeout: Duration,
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
            request_timeout: Duration::from_secs(10),
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
    ///
    /// A telemetry stream start gives up after 10 seconds, and a list or bounds
    /// request after the request timeout, also when this timeout is longer. A
    /// start or request can then give up before its connect attempt does, and
    /// the next call can fail at once with that attempt's error. So keep this
    /// timeout well under both. A connect attempt can also take the DNS lookup,
    /// and up to 300 ms more when the other IP version takes over.
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Sets how long to wait for the service to answer a request that lists
    /// components or connections, or that augments bounds.
    ///
    /// The time includes connecting, if the client isn't connected yet. A
    /// request that gets no answer in time fails with an
    /// `ErrorKind::APIServerError`. The client handles these requests one at a
    /// time, so a request that gets no answer holds up other requests for at
    /// most this long. It must be more than zero.
    ///
    /// The time starts when the client starts the request, not when you call
    /// the method. A request that waits behind other requests or telemetry
    /// stream starts can take longer in total.
    ///
    /// Keep it well longer than the connect timeout. Otherwise a request can
    /// give up before its connect attempt does, and the next call can fail at
    /// once with that attempt's error. See [`Self::with_connect_timeout`] for
    /// how long a connect attempt can take.
    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
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
    /// stream does. A new set-power call on that connection fails only when the
    /// operating system gives up resending it, which can take many minutes. A
    /// list or bounds request fails after the request timeout, and so does
    /// every later one, until the operating system gives up.
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
        if self.request_timeout.is_zero() {
            return Err(Error::invalid_config(format!(
                "request_timeout must be positive, got {:?}",
                self.request_timeout
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
        assert_eq!(config.request_timeout, Duration::from_secs(10));
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
            .with_request_timeout(Duration::from_secs(7))
            .with_keep_alive(Duration::from_secs(5), Duration::from_secs(2));
        assert_eq!(config.connect_timeout, Duration::from_secs(3));
        assert_eq!(config.request_timeout, Duration::from_secs(7));
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
            MicrogridClientConfig::default().with_request_timeout(zero),
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
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let handle = MicrogridClientHandle::try_new(format!("http://{address}"))
            .await
            .unwrap();

        // A call makes the client connect. The call times out on its own, and
        // the connection stays open until the default keepalive closes it.
        // With paused time, the clock jumps ahead while the test waits, so the
        // default ping (after 60 s) and its timeout (20 s) pass at once.
        let closed = async {
            let (mut connection, _) = listener.accept().await.unwrap();
            let mut buffer = [0; 1024];
            while matches!(connection.read(&mut buffer).await, Ok(n) if n > 0) {}
        };
        let start = tokio::time::Instant::now();
        let (result, ()) = tokio::time::timeout(Duration::from_secs(90), async {
            tokio::join!(handle.list_electrical_components(vec![], vec![]), closed)
        })
        .await
        .expect("the default keepalive pings should close the silent connection");

        assert!(
            start.elapsed() >= Duration::from_secs(80),
            "{:?}",
            start.elapsed()
        );
        assert!(result.is_err());
    }
}
