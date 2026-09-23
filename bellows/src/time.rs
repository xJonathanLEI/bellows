//! Portable time for task availability and lease deadlines.
//!
//! [`Instant`] is [`std::time::Instant`] natively and `web_time::Instant` on wasm.
//! Use it in portable backends and workers; durations remain [`std::time::Duration`].
//!
//! Instants are local deadlines, not serializable timestamps.

// Keep SystemTime and UNIX_EPOCH available through the same internal clock boundary for backend
// timestamp conversions. This also avoids mixing the standard and host clocks on wasm.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use std::time as clock;
#[cfg(target_arch = "wasm32")]
pub(crate) use web_time as clock;

pub use clock::Instant;

#[cfg(any(
    all(
        not(target_arch = "wasm32"),
        any(feature = "postgres", feature = "sqlite")
    ),
    all(feature = "cloudflare", any(target_arch = "wasm32", test))
))]
pub(crate) mod deadlines {
    use super::clock::{Instant, SystemTime, UNIX_EPOCH};
    use std::time::Duration;

    /// Capture adjacent clock samples; never pair wall time from before I/O with an instant after it.
    pub(crate) struct ClockSnapshot {
        instant: Instant,
        system: SystemTime,
    }

    impl ClockSnapshot {
        pub(crate) fn now() -> Self {
            #[cfg(target_arch = "wasm32")]
            {
                Self::sample_millisecond_clocks(Instant::now, SystemTime::now)
            }
            #[cfg(not(target_arch = "wasm32"))]
            {
                Self {
                    instant: Instant::now(),
                    system: SystemTime::now(),
                }
            }
        }

        // Workers expose millisecond clocks. Bracket the monotonic sample so a tick between
        // calls cannot introduce a one-millisecond offset that reverses on wire conversion.
        #[cfg(any(target_arch = "wasm32", test))]
        fn sample_millisecond_clocks(
            mut instant: impl FnMut() -> Instant,
            mut system: impl FnMut() -> SystemTime,
        ) -> Self {
            loop {
                let before = system();
                let instant = instant();
                if before == system() {
                    return Self {
                        instant,
                        system: before,
                    };
                }
            }
        }

        /// Absolute scheduling hints round up to milliseconds, never earlier than the deadline.
        /// Unlike SQL conversion, reject dates outside the shared JavaScript Date/safe-integer range.
        #[cfg(all(feature = "cloudflare", any(target_arch = "wasm32", test)))]
        pub(crate) fn to_wire_ms(&self, deadline: Instant) -> Option<u64> {
            let duration = self
                .to_system_time(deadline)?
                .duration_since(UNIX_EPOCH)
                .ok()?;
            let millis = duration.as_nanos().div_ceil(1_000_000);
            (millis <= 8_640_000_000_000_000).then_some(millis as u64)
        }

        pub(crate) fn to_system_time(&self, deadline: Instant) -> Option<SystemTime> {
            if deadline >= self.instant {
                self.system
                    .checked_add(deadline.duration_since(self.instant))
            } else {
                self.system
                    .checked_sub(self.instant.duration_since(deadline))
            }
        }

        pub(crate) fn to_instant(&self, deadline: SystemTime) -> Option<Instant> {
            match deadline.duration_since(self.system) {
                Ok(delta) => self.instant.checked_add(delta),
                Err(delta) => self.instant.checked_sub(delta.duration()),
            }
        }
    }

    pub(crate) fn unix_timestamp_ms(time: SystemTime) -> i64 {
        let duration = time.duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO);
        i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
    }

    /// SQL timestamps round down to milliseconds, preserving the existing backend convention.
    /// A wire scheduling hint must round up instead, so it never instructs an early invocation.
    pub(crate) fn instant_to_unix_ms(instant: Instant) -> i64 {
        let clocks = ClockSnapshot::now();
        clocks
            .to_system_time(instant)
            .map(unix_timestamp_ms)
            .unwrap_or_else(|| {
                if instant < clocks.instant {
                    0
                } else {
                    i64::MAX
                }
            })
    }

    /// Preserve past deadlines too. An unrepresentable deadline has no trustworthy hint.
    pub(crate) fn unix_ms_to_instant(unix_ms: i64) -> Option<Instant> {
        let deadline = if unix_ms >= 0 {
            UNIX_EPOCH.checked_add(Duration::from_millis(unix_ms.unsigned_abs()))
        } else {
            UNIX_EPOCH.checked_sub(Duration::from_millis(unix_ms.unsigned_abs()))
        }?;
        ClockSnapshot::now().to_instant(deadline)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn millisecond_clock_rollover_does_not_shift_round_trip_deadlines() {
            let instant = Instant::now();
            let system = UNIX_EPOCH + Duration::from_millis(1_700_000_000_123);
            let tick = Duration::from_millis(1);
            // The first sample straddles a wall-clock tick; the second is consistent.
            let mut instants = [instant, instant + tick].into_iter();
            let mut systems = [system, system + tick, system + tick, system + tick].into_iter();
            let before = ClockSnapshot::sample_millisecond_clocks(
                || instants.next().unwrap(),
                || systems.next().unwrap(),
            );
            let delay = Duration::from_millis(321);
            let after = ClockSnapshot::sample_millisecond_clocks(
                || instant + tick + delay,
                || system + tick + delay,
            );
            for deadline in [
                system - Duration::from_secs(60),
                system + Duration::from_secs(60),
            ] {
                let deadline_instant = before.to_instant(deadline).unwrap();
                assert_eq!(after.to_system_time(deadline_instant), Some(deadline));
                #[cfg(feature = "cloudflare")]
                assert_eq!(
                    after.to_wire_ms(deadline_instant),
                    Some(unix_timestamp_ms(deadline) as u64)
                );
            }
        }

        #[test]
        fn sql_and_wire_round_trip_past_and_future_deadlines() {
            let now = unix_timestamp_ms(SystemTime::now());
            for timestamp in [now - 60_000, now + 60_000] {
                let deadline = unix_ms_to_instant(timestamp).unwrap();
                let sql = instant_to_unix_ms(deadline);
                assert!((timestamp - 1..=timestamp + 1).contains(&sql));
                #[cfg(feature = "cloudflare")]
                {
                    let wire = ClockSnapshot::now().to_wire_ms(deadline).unwrap();
                    assert!((timestamp as u64..=timestamp as u64 + 1).contains(&wire));
                }
            }
        }

        #[cfg(feature = "cloudflare")]
        #[test]
        fn wire_rounding_and_checked_date_range() {
            let instant = Instant::now();
            let clocks = ClockSnapshot {
                instant,
                system: UNIX_EPOCH,
            };
            assert_eq!(clocks.to_wire_ms(instant), Some(0));
            assert_eq!(
                clocks.to_wire_ms(instant + Duration::from_nanos(1)),
                Some(1)
            );
            assert_eq!(
                clocks.to_wire_ms(instant + Duration::from_millis(17)),
                Some(17)
            );
            assert_eq!(clocks.to_wire_ms(instant - Duration::from_nanos(1)), None);
            let max = Duration::from_millis(8_640_000_000_000_000);
            assert_eq!(
                clocks.to_wire_ms(instant + max),
                Some(8_640_000_000_000_000)
            );
            assert_eq!(
                clocks.to_wire_ms(instant + max + Duration::from_nanos(1)),
                None
            );
        }

        #[test]
        fn past_and_future_deadlines_survive_delayed_io() {
            let instant = Instant::now();
            let system = UNIX_EPOCH + Duration::from_millis(1_700_000_000_123);
            let before = ClockSnapshot { instant, system };
            let delay = Duration::from_millis(321);
            let after = ClockSnapshot {
                instant: instant + delay,
                system: system + delay,
            };
            for delta in [Duration::from_millis(17), Duration::from_secs(60)] {
                for deadline in [system - delta, system + delta] {
                    let expected = before.to_instant(deadline).unwrap();
                    assert_eq!(after.to_instant(deadline), Some(expected));
                    assert_eq!(after.to_system_time(expected), Some(deadline));
                }
            }
        }

        #[test]
        fn sql_rounding_is_down_to_the_millisecond() {
            assert_eq!(
                unix_timestamp_ms(UNIX_EPOCH + Duration::from_nanos(1_999_999)),
                1
            );
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::{Instant, clock};

    #[test]
    fn native_clock_types_are_the_standard_types() {
        let instant: std::time::Instant = Instant::now();
        let _: Instant = instant;
        let wall_clock: std::time::SystemTime = clock::SystemTime::now();
        assert!(wall_clock.duration_since(clock::UNIX_EPOCH).is_ok());
    }
}
