//! **The user's-zone owner**: the one read of the machine's time zone — the `TZ` variable and
//! the system zone — behind every time Canon shows a person.
//!
//! [`UserZone::machine`] reads the environment once per process and classifies what it found
//! through a pure function ([`zone_from`]), so every `TZ` case is testable without touching the
//! process-wide variable. Every other read of the machine's zone is this owner re-derived, and
//! the architecture test refuses it by name (chrono's local clock, `TimeZone::system`/
//! `try_system`, `Zoned::now`, reads of `TZ`). The zone is handed to the frame rule as a
//! parameter; the jiff
//! zone inside never leaves this type, so nothing outside `core/domain/time` can read a time a
//! second way.
//!
//! Where it does not apply: machine fields (receipts, the book's inventory, `generated_at`)
//! are UTC with `Z`; a manifest's run reads the zone the manifest records, never this one.

use jiff::civil::{Date, DateTime};
use jiff::tz::{Offset, TimeZone};
use jiff::Timestamp;

use super::now_epoch_secs;

/// The user's clock: a zone's rules and what Canon can say about where they came from.
#[derive(Debug, Clone)]
pub struct UserZone {
    tz: TimeZone,
    label: ZoneLabel,
}

/// What the zone is called, as far as Canon can tell.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ZoneLabel {
    /// An IANA zone name (`Europe/Amsterdam`), from `TZ` or the system.
    Named(String),
    /// A fixed offset with no name.
    FixedOffset(Offset),
    /// A POSIX rule string from `TZ`, as written.
    RuleString(String),
    /// Zone rules with no name (an unnamed zone file).
    UnnamedRules,
    /// Nothing readable: times are read in UTC. Carries the `TZ` value Canon could not read,
    /// if one was set.
    Unknown { unreadable_tz: Option<String> },
}

#[cfg(test)]
thread_local! {
    // The test pin for `UserZone::machine`, the `with_test_now` shape.
    static TEST_ZONE: std::cell::RefCell<Option<UserZone>> = const { std::cell::RefCell::new(None) };
}

impl UserZone {
    /// **The user's-zone owner.** The machine's zone, read once per process.
    ///
    /// Under `cfg(test)` it answers only inside [`with_test_zone`] and panics otherwise: no
    /// test reads the machine's zone, because a test that passes on one machine's clock and
    /// fails on another's asserts nothing.
    pub fn machine() -> UserZone {
        #[cfg(test)]
        {
            TEST_ZONE.with(|cell| {
                cell.borrow().clone().expect(
                    "UserZone::machine read in a test without with_test_zone: \
                     no test reads the machine's zone",
                )
            })
        }
        #[cfg(not(test))]
        {
            static MACHINE: std::sync::OnceLock<UserZone> = std::sync::OnceLock::new();
            MACHINE
                .get_or_init(|| {
                    let tz_env = std::env::var_os("TZ").map(|v| v.to_string_lossy().into_owned());
                    zone_from(tz_env.as_deref(), TimeZone::try_system())
                })
                .clone()
        }
    }

    /// UTC. The clock an unknown zone falls back to, and the clock a manifest's run reads until
    /// the manifest records its own zone.
    pub fn utc() -> UserZone {
        UserZone {
            tz: TimeZone::UTC,
            label: ZoneLabel::Named("UTC".to_string()),
        }
    }

    pub fn label(&self) -> &ZoneLabel {
        &self.label
    }

    /// The first instant of `date` on this clock: the earlier instant where midnight repeats,
    /// and the first instant after the gap where midnight is skipped. The one sanctioned
    /// computation from a clock reading to an instant, for the trail's fetch bounds.
    ///
    /// `None` where that midnight lies outside the instants jiff holds — 9999-12-31 on a clock
    /// less than two hours ahead of UTC (the last instant, `Timestamp::MAX`, is
    /// 9999-12-30T22:00Z), or a day at jiff's first, near −9999-01-01: no instant is that day's
    /// start, and the caller draws its bound beyond every time Canon reads, not at one of them.
    pub fn day_start(&self, date: Date) -> Option<Timestamp> {
        self.tz
            .to_ambiguous_timestamp(date.at(0, 0, 0, 0))
            .compatible()
            .ok()
    }

    /// Today's date on this clock, read through the one owner of "now".
    pub fn today(&self) -> Date {
        let now = Timestamp::from_second(now_epoch_secs()).unwrap_or(Timestamp::UNIX_EPOCH);
        self.to_clock(now).date()
    }

    /// An instant read on this clock. Only the frame rule (and the owner's own `today`) calls
    /// it.
    pub(super) fn to_clock(&self, at: Timestamp) -> DateTime {
        self.tz.to_datetime(at)
    }

    /// A named zone for a test.
    #[cfg(test)]
    pub fn for_test(name: &str) -> UserZone {
        UserZone {
            tz: TimeZone::get(name).unwrap_or_else(|e| panic!("unknown test zone {name}: {e}")),
            label: ZoneLabel::Named(name.to_string()),
        }
    }
}

/// The pure classifier behind [`UserZone::machine`]: what `TZ` held (if it was set) and what
/// jiff made of the machine's zone.
///
/// `TZ` set but unreadable (`TZ=Europe/Amsterdm`) never falls through to the system zone —
/// jiff returns an error whenever `TZ` is set and no zone can be extracted from it — so it is
/// the unknown case, read in UTC, carrying the value; `date` reads an unknown `TZ` as UTC too.
pub(super) fn zone_from(tz_env: Option<&str>, system: Result<TimeZone, jiff::Error>) -> UserZone {
    let tz = match system {
        Ok(tz) => tz,
        Err(_) => {
            return UserZone {
                tz: TimeZone::UTC,
                label: ZoneLabel::Unknown {
                    unreadable_tz: tz_env.map(str::to_owned),
                },
            }
        }
    };
    let label = if let Some(name) = tz.iana_name().filter(|n| !n.is_empty()) {
        ZoneLabel::Named(name.to_string())
    } else if let Some(rule) = tz_env.filter(|s| TimeZone::posix(s).is_ok()) {
        ZoneLabel::RuleString(rule.to_string())
    } else if let Ok(offset) = tz.to_fixed_offset() {
        ZoneLabel::FixedOffset(offset)
    } else {
        ZoneLabel::UnnamedRules
    };
    UserZone { tz, label }
}

/// Pin [`UserZone::machine`] to `zone` for the duration of `f`, restoring whatever was pinned
/// before (or nothing) once `f` returns — including on unwind.
#[cfg(test)]
pub fn with_test_zone<R>(zone: UserZone, f: impl FnOnce() -> R) -> R {
    let previous = TEST_ZONE.with(|cell| cell.replace(Some(zone)));
    struct Restore(Option<UserZone>);
    impl Drop for Restore {
        fn drop(&mut self) {
            let previous = self.0.take();
            TEST_ZONE.with(|cell| *cell.borrow_mut() = previous);
        }
    }
    let _restore = Restore(previous);
    f()
}

#[cfg(test)]
impl UserZone {
    /// A fixed-offset zone for a test.
    pub fn for_test_offset(offset: Offset) -> UserZone {
        UserZone {
            tz: TimeZone::fixed(offset),
            label: ZoneLabel::FixedOffset(offset),
        }
    }

    /// The zone a machine with no readable zone gets: read in UTC, carrying the `TZ` value
    /// that could not be read, if one was set.
    pub fn for_test_unknown(unreadable_tz: Option<&str>) -> UserZone {
        UserZone {
            tz: TimeZone::UTC,
            label: ZoneLabel::Unknown {
                unreadable_tz: unreadable_tz.map(str::to_owned),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::domain::time::with_test_now;

    fn unreadable() -> jiff::Error {
        TimeZone::get("Europe/Amsterdm").unwrap_err()
    }

    #[test]
    fn no_tz_and_a_named_system_zone_is_named() {
        let zone = zone_from(None, TimeZone::get("Europe/Amsterdam"));
        assert_eq!(zone.label(), &ZoneLabel::Named("Europe/Amsterdam".into()));
    }

    #[test]
    fn no_tz_and_system_rules_with_no_name_are_unnamed_rules() {
        let zone = zone_from(None, TimeZone::posix("CET-1CEST,M3.5.0,M10.5.0/3"));
        assert_eq!(zone.label(), &ZoneLabel::UnnamedRules);
    }

    #[test]
    fn a_tz_iana_name_is_named() {
        let zone = zone_from(Some("America/New_York"), TimeZone::get("America/New_York"));
        assert_eq!(zone.label(), &ZoneLabel::Named("America/New_York".into()));
    }

    #[test]
    fn a_tz_zone_file_path_is_named_by_the_zone_it_names() {
        // jiff takes the name after `zoneinfo/` and looks it up in the zone database.
        let zone = zone_from(
            Some("/usr/share/zoneinfo/Europe/Amsterdam"),
            TimeZone::get("Europe/Amsterdam"),
        );
        assert_eq!(zone.label(), &ZoneLabel::Named("Europe/Amsterdam".into()));
    }

    #[test]
    fn a_tz_rule_string_is_kept_as_written() {
        let rule = "EST5EDT,M3.2.0,M11.1.0";
        let zone = zone_from(Some(rule), TimeZone::posix(rule));
        assert_eq!(zone.label(), &ZoneLabel::RuleString(rule.into()));
    }

    #[test]
    fn a_fixed_offset_with_no_name_is_a_fixed_offset() {
        let offset = Offset::from_hours(3).unwrap();
        let zone = zone_from(None, Ok(TimeZone::fixed(offset)));
        assert_eq!(zone.label(), &ZoneLabel::FixedOffset(offset));
    }

    #[test]
    fn nothing_readable_is_unknown_read_in_utc() {
        let zone = zone_from(None, Err(unreadable()));
        assert_eq!(
            zone.label(),
            &ZoneLabel::Unknown {
                unreadable_tz: None
            }
        );
        let noon = Timestamp::from_second(1_718_452_800).unwrap(); // 2024-06-15T12:00:00Z
        assert_eq!(zone.to_clock(noon).hour(), 12);
    }

    #[test]
    fn a_tz_set_but_unreadable_is_unknown_and_carries_the_value() {
        let zone = zone_from(Some("Europe/Amsterdm"), Err(unreadable()));
        assert_eq!(
            zone.label(),
            &ZoneLabel::Unknown {
                unreadable_tz: Some("Europe/Amsterdm".into())
            }
        );
    }

    #[test]
    fn with_test_zone_pins_and_restores() {
        with_test_zone(UserZone::for_test("Europe/Amsterdam"), || {
            assert_eq!(
                UserZone::machine().label(),
                &ZoneLabel::Named("Europe/Amsterdam".into())
            );
            with_test_zone(UserZone::for_test("America/New_York"), || {
                assert_eq!(
                    UserZone::machine().label(),
                    &ZoneLabel::Named("America/New_York".into())
                );
            });
            assert_eq!(
                UserZone::machine().label(),
                &ZoneLabel::Named("Europe/Amsterdam".into())
            );
        });
        let unpinned = std::panic::catch_unwind(UserZone::machine);
        assert!(unpinned.is_err(), "no test reads the machine's zone");
    }

    #[test]
    fn with_test_zone_restores_on_unwind() {
        let result = std::panic::catch_unwind(|| {
            with_test_zone(UserZone::for_test("Europe/Amsterdam"), || panic!("boom"));
        });
        assert!(result.is_err());
        assert!(std::panic::catch_unwind(UserZone::machine).is_err());
    }

    #[test]
    fn today_reads_now_through_its_owner_on_this_clock() {
        // 2023-12-31T23:30:00Z: still New Year's Eve in New York, already 1 January in
        // Amsterdam.
        with_test_now(1_704_065_400, || {
            assert_eq!(
                UserZone::for_test("Europe/Amsterdam").today(),
                jiff::civil::date(2024, 1, 1)
            );
            assert_eq!(
                UserZone::for_test("America/New_York").today(),
                jiff::civil::date(2023, 12, 31)
            );
        });
    }
}
