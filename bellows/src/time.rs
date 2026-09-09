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
