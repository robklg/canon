//! The one owner of "now" as seconds since the Unix epoch — the single sanctioned
//! read of `SystemTime::now()` behind every `created_at`/`scanned_at`/`observed_at`
//! default in the tree.
//!
//! Every other spelling of `SystemTime::now().duration_since(UNIX_EPOCH)` is this
//! function re-derived. A second spelling drifts silently from this one's
//! `expect` reasoning, which is the failure this module exists to close by
//! having exactly one place to read.

use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(test)]
use std::cell::Cell;

#[cfg(test)]
thread_local! {
    // A test-only pinned value for `now_epoch_secs`. Read-layer fixtures need a
    // controlled `created_at`/`scanned_at` to assert ordering against, and the
    // clock itself is unownable — this is the owner's answer to that pressure,
    // in place of a fixture hand-rolling its own insert to bypass an
    // unownable clock (the fixture pressure `insert_decision_at` names).
    static TEST_NOW: Cell<Option<i64>> = const { Cell::new(None) };
}

/// Seconds since the Unix epoch, as the system clock reports it now.
///
/// Panics if the OS clock reads before 1970 — a decision cannot be dated by a
/// clock that cannot order its own past, and every caller already treats
/// "now" as infallible.
///
/// Under `cfg(test)`, [`with_test_now`] can pin the value this returns for the
/// duration of a closure, so a fixture that needs a specific timestamp calls
/// the real recording path instead of hand-writing its own insert.
pub fn now_epoch_secs() -> i64 {
    #[cfg(test)]
    if let Some(fixed) = TEST_NOW.with(Cell::get) {
        return fixed;
    }

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock reads before the Unix epoch")
        .as_secs() as i64
}

/// Pin [`now_epoch_secs`] to `value` for the duration of `f`, restoring
/// whatever was pinned before (or nothing) once `f` returns — including on
/// unwind, so a panicking fixture never leaves the pin standing for the next
/// test on the same thread.
#[cfg(test)]
pub fn with_test_now<R>(value: i64, f: impl FnOnce() -> R) -> R {
    let previous = TEST_NOW.with(|cell| cell.replace(Some(value)));
    struct Restore(Option<i64>);
    impl Drop for Restore {
        fn drop(&mut self) {
            TEST_NOW.with(|cell| cell.set(self.0));
        }
    }
    let _restore = Restore(previous);
    f()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn now_epoch_secs_reads_a_plausible_real_clock() {
        // Loose bounds: after this module's own authoring date, before an
        // absurd future — catches a unit mistake (millis vs. secs) without
        // pinning an exact value that ages.
        let now = now_epoch_secs();
        assert!(now > 1_700_000_000, "reads before 2023: {now}");
        assert!(now < 4_000_000_000, "reads after 2096: {now}");
    }

    #[test]
    fn with_test_now_pins_and_restores() {
        assert_ne!(now_epoch_secs(), 42);
        with_test_now(42, || {
            assert_eq!(now_epoch_secs(), 42);
        });
        assert_ne!(now_epoch_secs(), 42);
    }

    #[test]
    fn with_test_now_restores_on_panic() {
        let outer = now_epoch_secs();
        let result = std::panic::catch_unwind(|| {
            with_test_now(7, || {
                panic!("boom");
            });
        });
        assert!(result.is_err());
        assert_eq!(now_epoch_secs(), outer);
    }

    #[test]
    fn with_test_now_nests() {
        with_test_now(1, || {
            assert_eq!(now_epoch_secs(), 1);
            with_test_now(2, || {
                assert_eq!(now_epoch_secs(), 2);
            });
            assert_eq!(now_epoch_secs(), 1);
        });
    }
}
