// License: MIT
// Copyright © 2026 Frequenz Energy-as-a-Service GmbH

//! Bounded exponential backoff for retry loops.
//!
//! - [`BackoffConfig`] is the validated configuration.
//! - [`Backoff::next_retry_time`] records a failure and returns the
//!   [`tokio::time::Instant`] at which the next attempt is due. The
//!   deadline is also stored on the [`Backoff`], so `select!`-driven
//!   callers can poll it via [`Backoff::take_due`] instead of tracking
//!   it themselves.

use std::time::Duration;

/// Why a [`BackoffConfig`] is invalid.
#[derive(Debug, Clone, PartialEq)]
pub enum BackoffConfigError {
    /// `multiplier` was less than `1.0` or not finite — would shrink delays.
    InvalidMultiplier(f32),
    /// `jitter` was outside `[0.0, 1.0]` or not finite.
    InvalidJitter(f32),
    /// `initial` was greater than `max`.
    InitialExceedsMax { initial: Duration, max: Duration },
    /// `initial` or `max` was zero.
    ZeroDelay,
}

impl std::fmt::Display for BackoffConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::InvalidMultiplier(m) => {
                write!(f, "multiplier must be >= 1.0 and finite, got {m}")
            }
            Self::InvalidJitter(j) => {
                write!(f, "jitter must be in [0.0, 1.0] and finite, got {j}")
            }
            Self::InitialExceedsMax { initial, max } => {
                write!(f, "initial ({initial:?}) exceeds max ({max:?})")
            }
            Self::ZeroDelay => write!(f, "initial and max must be > 0"),
        }
    }
}

impl std::error::Error for BackoffConfigError {}

/// Configuration for [`Backoff`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BackoffConfig {
    initial: Duration,
    max: Duration,
    multiplier: f32,
    jitter: f32,
}

impl BackoffConfig {
    /// Returns a validated `BackoffConfig`.
    ///
    /// - `initial` and `max` must be `> 0` and `initial <= max`.
    /// - `multiplier` must be finite and `>= 1.0`.
    /// - `jitter` must be finite and in `[0.0, 1.0]`. Jitter is not applied
    ///   to the schedule yet; the value is stored for future use.
    pub fn try_new(
        initial: Duration,
        max: Duration,
        multiplier: f32,
        jitter: f32,
    ) -> Result<Self, BackoffConfigError> {
        if initial.is_zero() || max.is_zero() {
            return Err(BackoffConfigError::ZeroDelay);
        }
        if initial > max {
            return Err(BackoffConfigError::InitialExceedsMax { initial, max });
        }
        if !multiplier.is_finite() || multiplier < 1.0 {
            return Err(BackoffConfigError::InvalidMultiplier(multiplier));
        }
        if !jitter.is_finite() || !(0.0..=1.0).contains(&jitter) {
            return Err(BackoffConfigError::InvalidJitter(jitter));
        }
        Ok(Self {
            initial,
            max,
            multiplier,
            jitter,
        })
    }

    pub fn initial(&self) -> Duration {
        self.initial
    }
    pub fn max(&self) -> Duration {
        self.max
    }
    pub fn multiplier(&self) -> f32 {
        self.multiplier
    }
    pub fn jitter(&self) -> f32 {
        self.jitter
    }
}

impl Default for BackoffConfig {
    fn default() -> Self {
        // Hard-coded known-good values; bypasses `try_new` validation but we
        // own the literals so they can't drift out of range.
        Self {
            initial: Duration::from_secs(1),
            max: Duration::from_secs(30),
            multiplier: 2.0,
            jitter: 0.25,
        }
    }
}

/// A bounded-exponential backoff.
///
/// Pattern:
/// - On failure, call [`Self::next_retry_time`] to record the failure and
///   get the [`tokio::time::Instant`] at which the next attempt is due.
///   Sleep-and-retry callers can `tokio::time::sleep_until` on it; the
///   deadline is also stored on the [`Backoff`] for `select!` loops.
/// - On each timer tick, `select!` callers call [`Self::take_due`] to
///   check whether the pending retry is due and consume it.
/// - On success, drop the [`Backoff`] (or call [`Self::reset`] to reuse).
pub struct Backoff {
    config: BackoffConfig,
    current_delay: Option<Duration>,
    deadline: Option<tokio::time::Instant>,
}

impl Backoff {
    pub fn new(config: BackoffConfig) -> Self {
        Self {
            config,
            current_delay: None,
            deadline: None,
        }
    }

    /// Records a failure and returns the time at which the next attempt is
    /// due.
    pub fn next_retry_time(&mut self) -> tokio::time::Instant {
        let nominal = match self.current_delay {
            None => self.config.initial,
            Some(d) => d.mul_f32(self.config.multiplier).min(self.config.max),
        };
        self.current_delay = Some(nominal);
        let when = tokio::time::Instant::now() + nominal;
        self.deadline = Some(when);
        when
    }

    /// Returns the time at which the pending retry is due, or `None` if
    /// no retry is pending (no failure recorded, or the pending retry was
    /// already consumed by [`Self::take_due`]).
    pub fn deadline(&self) -> Option<tokio::time::Instant> {
        self.deadline
    }

    /// If the pending retry is due at or before `now`, consumes it and
    /// returns `true`.
    pub fn take_due(&mut self, now: tokio::time::Instant) -> bool {
        match self.deadline {
            Some(d) if d <= now => {
                self.deadline = None;
                true
            }
            _ => false,
        }
    }

    /// Clears any pending retry and resets the schedule. The next
    /// [`Self::next_retry_time`] returns `Instant::now() + config.initial`.
    pub fn reset(&mut self) {
        self.current_delay = None;
        self.deadline = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn no_jitter_config() -> BackoffConfig {
        BackoffConfig::try_new(
            Duration::from_secs(1),
            Duration::from_secs(30),
            2.0,
            0.0,
        )
        .unwrap()
    }

    #[test]
    fn config_rejects_invalid_values() {
        let one = Duration::from_secs(1);
        let two = Duration::from_secs(2);
        assert!(matches!(
            BackoffConfig::try_new(Duration::ZERO, one, 2.0, 0.0),
            Err(BackoffConfigError::ZeroDelay),
        ));
        assert!(matches!(
            BackoffConfig::try_new(two, one, 2.0, 0.0),
            Err(BackoffConfigError::InitialExceedsMax { .. }),
        ));
        assert!(matches!(
            BackoffConfig::try_new(one, two, 0.5, 0.0),
            Err(BackoffConfigError::InvalidMultiplier(_)),
        ));
        assert!(matches!(
            BackoffConfig::try_new(one, two, f32::NAN, 0.0),
            Err(BackoffConfigError::InvalidMultiplier(_)),
        ));
        assert!(matches!(
            BackoffConfig::try_new(one, two, 2.0, 1.5),
            Err(BackoffConfigError::InvalidJitter(_)),
        ));
        assert!(matches!(
            BackoffConfig::try_new(one, two, 2.0, f32::NAN),
            Err(BackoffConfigError::InvalidJitter(_)),
        ));
    }

    /// `Default::default` must produce a config that also passes `try_new`'s
    /// validation, so the two paths can't drift apart.
    #[test]
    fn config_default_validates() {
        let c = BackoffConfig::default();
        BackoffConfig::try_new(c.initial(), c.max(), c.multiplier(), c.jitter())
            .expect("default config must validate");
    }

    #[tokio::test(start_paused = true)]
    async fn backoff_is_bounded_exponential() {
        let mut backoff = Backoff::new(no_jitter_config());
        let start = tokio::time::Instant::now();
        for secs in [1u64, 2, 4, 8, 16, 30, 30, 30] {
            assert_eq!(backoff.next_retry_time(), start + Duration::from_secs(secs));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn backoff_reset_restarts_at_initial() {
        let mut backoff = Backoff::new(no_jitter_config());
        let start = tokio::time::Instant::now();
        assert_eq!(backoff.next_retry_time(), start + Duration::from_secs(1));
        assert_eq!(backoff.next_retry_time(), start + Duration::from_secs(2));
        backoff.reset();
        assert_eq!(backoff.deadline(), None);
        assert_eq!(backoff.next_retry_time(), start + Duration::from_secs(1));
    }

    #[tokio::test(start_paused = true)]
    async fn take_due_consumes_pending_retry() {
        let config =
            BackoffConfig::try_new(Duration::from_secs(3), Duration::from_secs(3), 1.0, 0.0)
                .unwrap();
        let mut backoff = Backoff::new(config);
        let start = tokio::time::Instant::now();

        assert_eq!(backoff.deadline(), None);
        assert!(!backoff.take_due(start));

        let when = backoff.next_retry_time();
        assert_eq!(when, start + Duration::from_secs(3));
        assert_eq!(backoff.deadline(), Some(when));

        tokio::time::advance(Duration::from_secs(2)).await;
        assert!(!backoff.take_due(tokio::time::Instant::now()));
        assert!(backoff.deadline().is_some());

        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(backoff.take_due(tokio::time::Instant::now()));
        assert_eq!(backoff.deadline(), None);
    }
}
