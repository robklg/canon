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
//!
//! **The zone-name parse** sits beside it: [`parse_zone_name`] is the one reading of a zone
//! name a person wrote (a manifest's `zone` line), and [`UserZone::recordable`] the one
//! answer to "can this machine's zone be written down". Both build a [`RecordedZone`], and
//! zone rules are looked up by name (`TimeZone::get`, `TimeZone::posix`) nowhere else — the
//! architecture test refuses it.

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
    /// A POSIX rule string from `TZ` whose offset changes (summer time), as written. One that
    /// never changes is a [`FixedOffset`](Self::FixedOffset).
    RuleString(String),
    /// Zone rules with no name whose offset changes (an unnamed zone file).
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

    /// UTC, for a test that needs a clock fixed by construction. No production read takes it:
    /// an unknown zone reads UTC through `zone_from`, and a manifest's run reads its recorded
    /// zone.
    #[cfg(test)]
    pub fn utc() -> UserZone {
        UserZone {
            tz: TimeZone::UTC,
            label: ZoneLabel::Named("UTC".to_string()),
        }
    }

    pub fn label(&self) -> &ZoneLabel {
        &self.label
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

    /// This zone as a manifest records it, where it can be written down: a name the zone
    /// data knows, `UTC`, or a fixed offset. Built from these rules themselves — no second
    /// lookup — so the recorded clock and this one are the same rules on this machine.
    ///
    /// Rules with no name that change offset (a `TZ` rule with summer time, an unnamed zone
    /// file) and a zone Canon cannot read have nothing to write that another run could read
    /// back, so they are refused.
    pub fn recordable(&self) -> Result<RecordedZone, Unrecordable> {
        let name = match &self.label {
            ZoneLabel::Named(name) => name.clone(),
            ZoneLabel::FixedOffset(offset) => offset_name(*offset),
            ZoneLabel::RuleString(rule) => return Err(Unrecordable::RuleString(rule.clone())),
            ZoneLabel::UnnamedRules => return Err(Unrecordable::UnnamedRules),
            ZoneLabel::Unknown { unreadable_tz } => {
                return Err(Unrecordable::Unknown {
                    unreadable_tz: unreadable_tz.clone(),
                })
            }
        };
        Ok(RecordedZone {
            name,
            clock: self.clone(),
        })
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

/// The plain name of a zone, as Canon's sentences say it: `Europe/Amsterdam`, `UTC`,
/// `+03:00`, `rules with no name`, `unknown`.
impl std::fmt::Display for ZoneLabel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ZoneLabel::Named(name) => f.write_str(name),
            ZoneLabel::FixedOffset(offset) => f.write_str(&offset_name(*offset)),
            ZoneLabel::RuleString(_) | ZoneLabel::UnnamedRules => f.write_str("rules with no name"),
            ZoneLabel::Unknown { .. } => f.write_str("unknown"),
        }
    }
}

/// A zone a manifest can record: a name this machine's zone data knows, `UTC`, or a fixed
/// offset — with the rules it names. Built only by [`parse_zone_name`] and
/// [`UserZone::recordable`], so whoever holds one holds a zone that has a name to write.
#[derive(Debug, Clone)]
pub struct RecordedZone {
    name: String,
    clock: UserZone,
}

impl RecordedZone {
    /// The name written into a manifest and a lock, and the name two zones are compared by.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The clock this zone reads times on.
    pub fn clock(&self) -> &UserZone {
        &self.clock
    }

    /// Whether two recorded zones are the same zone: the same parsed name — never the text a
    /// person wrote, so `europe/amsterdam` and `Europe/Amsterdam` are one zone — or the same
    /// rules through their whole history ([`UserZone::reads_like`]), so a link and the zone
    /// it points at (`US/Eastern`, `America/New_York`) are one zone too. The zone data gives
    /// a link its own name, so the name alone would call them two.
    pub fn same_zone(&self, other: &RecordedZone) -> bool {
        self.name == other.name || self.clock.reads_like(&other.clock)
    }
}

/// The clock a record states it read times on — the decision row's `zone`, the receipt's
/// `[meta].zone`, the book's sentences and its `meta.toml` all copy this one value, built
/// once per decision, so the records of one act cannot name two clocks. A record must be
/// readable without Canon, so the value is the text a stranger reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadTimesOn(String);

impl ReadTimesOn {
    /// For a command that reads the user's clock: the zone's name where it has one, a fixed
    /// offset's name, a `TZ` rule as written; rules with no name and no rule text are named
    /// by their offset now, said to be the offset at run time; a zone Canon cannot read is
    /// UTC, said to be unknown.
    pub fn of_user(zone: &UserZone) -> Self {
        match zone.recordable() {
            Ok(recorded) => ReadTimesOn::of_manifest(&recorded),
            Err(Unrecordable::RuleString(rule)) => ReadTimesOn(rule),
            Err(Unrecordable::UnnamedRules) => {
                let now = Timestamp::from_second(now_epoch_secs()).unwrap_or(Timestamp::UNIX_EPOCH);
                ReadTimesOn(format!(
                    "local rules with no name (UTC{} at run time)",
                    offset_name(zone.tz.to_offset(now))
                ))
            }
            Err(Unrecordable::Unknown { .. }) => {
                ReadTimesOn("UTC (this machine's time zone is unknown)".to_string())
            }
        }
    }

    /// For a manifest's run: the zone the manifest records, by its recorded name.
    pub fn of_manifest(zone: &RecordedZone) -> Self {
        ReadTimesOn(zone.name.clone())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl UserZone {
    /// Whether this clock reads every instant as `other` does: the same offset at the first
    /// instant, then the same changes of offset at the same instants, through the year
    /// 2200 — by which every zone has long been following its standing rule. Two zones that
    /// agree today but differ in their past (two neighbouring countries before they
    /// aligned) read a past instant differently, so today's offset is not enough.
    pub fn reads_like(&self, other: &UserZone) -> bool {
        self.tz.to_offset(Timestamp::MIN) == other.tz.to_offset(Timestamp::MIN)
            && offset_changes(&self.tz) == offset_changes(&other.tz)
    }
}

/// **The one answer to "where do these rules change offset?"**: each instant the offset
/// changes, from the first instant through the year 2200, with the offset it changes to.
/// Only a change of offset moves a reading; a transition that renames the zone's
/// abbreviation and keeps its offset is skipped, since zone data records those unevenly.
/// Rules with none are a fixed offset ([`fixed_offset_of`]); two clocks with the same are
/// one clock ([`UserZone::reads_like`]).
///
/// The walk ends where a transition does not move forward: for a zone file with no rule
/// after its last transition, jiff repeats that transition without end, and the zone keeps
/// its last offset from there on.
fn offset_changes(tz: &TimeZone) -> Vec<(Timestamp, Offset)> {
    let until = jiff::civil::date(2200, 1, 1)
        .at(0, 0, 0, 0)
        .to_zoned(TimeZone::UTC)
        .map(|z| z.timestamp())
        .unwrap_or(Timestamp::MAX);
    let mut last = tz.to_offset(Timestamp::MIN);
    let mut previous: Option<Timestamp> = None;
    let mut changes = Vec::new();
    for t in tz.following(Timestamp::MIN) {
        if t.timestamp() >= until || previous.is_some_and(|p| t.timestamp() <= p) {
            break;
        }
        previous = Some(t.timestamp());
        if t.offset() != last {
            last = t.offset();
            changes.push((t.timestamp(), last));
        }
    }
    changes
}

/// Why a zone line names no zone Canon can read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ZoneNameError {
    /// The line is empty.
    Empty,
    /// A time zone rule (`EST5EDT,M3.2.0,M11.1.0`), which has no name to record.
    RuleString(String),
    /// A name this machine's zone data does not know — misspelt, or newer than the data.
    NotKnownHere(String),
}

/// Why this machine's zone cannot be written into a manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Unrecordable {
    /// A `TZ` rule whose offset changes: rules with no name, carrying the rule as `TZ` held it.
    RuleString(String),
    /// A zone file with rules and no name.
    UnnamedRules,
    /// Canon cannot read the zone at all; carries the `TZ` value it could not read, if set.
    Unknown { unreadable_tz: Option<String> },
}

/// **The zone-name parse.** A zone name as a person wrote it, read as the zone it names.
///
/// In order: surrounding space trimmed; empty is no zone; a fixed offset (`+03:00`,
/// `-05:30`, `+03`) is that offset, named `±HH:MM`; a name the zone database knows is that
/// zone, named as the database spells it (`europe/amsterdam` → `Europe/Amsterdam`, `utc` →
/// `UTC`) — the person's own spelling is kept where the two differ by more than case, so a
/// name is never rewritten into one an older machine's data might not know; a time zone
/// rule is refused, never flattened to the offset it has today; anything else is a name
/// this machine does not know.
pub fn parse_zone_name(text: &str) -> Result<RecordedZone, ZoneNameError> {
    let text = text.trim();
    if text.is_empty() {
        return Err(ZoneNameError::Empty);
    }
    if let Some(offset) = parse_offset(text) {
        return Ok(RecordedZone {
            name: offset_name(offset),
            clock: UserZone {
                tz: TimeZone::fixed(offset),
                label: ZoneLabel::FixedOffset(offset),
            },
        });
    }
    if let Ok(tz) = TimeZone::get(text) {
        let name = match tz.iana_name() {
            Some(known) if known.eq_ignore_ascii_case(text) => known.to_string(),
            _ => text.to_string(),
        };
        return Ok(RecordedZone {
            name: name.clone(),
            clock: UserZone {
                tz,
                label: ZoneLabel::Named(name),
            },
        });
    }
    if TimeZone::posix(text).is_ok() {
        return Err(ZoneNameError::RuleString(text.to_string()));
    }
    Err(ZoneNameError::NotKnownHere(text.to_string()))
}

/// The one spelling of a fixed offset's name: `+03:00`, `-05:30`, `+05:45:30` where the
/// offset has seconds.
fn offset_name(offset: Offset) -> String {
    let total = offset.seconds();
    let sign = if total < 0 { '-' } else { '+' };
    let abs = total.unsigned_abs();
    let (h, m, s) = (abs / 3600, (abs % 3600) / 60, abs % 60);
    if s == 0 {
        format!("{sign}{h:02}:{m:02}")
    } else {
        format!("{sign}{h:02}:{m:02}:{s:02}")
    }
}

/// The offset of rules that never change it, where they never do: a fixed zone, or a `TZ`
/// rule or zone file with no change of offset ([`offset_changes`]) — jiff calls only the
/// first fixed.
fn fixed_offset_of(tz: &TimeZone) -> Option<Offset> {
    tz.to_fixed_offset().ok().or_else(|| {
        offset_changes(tz)
            .is_empty()
            .then(|| tz.to_offset(Timestamp::MIN))
    })
}

/// A fixed offset as a person writes one: a sign, two-digit hours, then optionally `:MM`
/// and `:SS`.
fn parse_offset(text: &str) -> Option<Offset> {
    let (sign, rest) = match text.as_bytes().first()? {
        b'+' => (1, text.get(1..)?),
        b'-' => (-1, text.get(1..)?),
        _ => return None,
    };
    let mut parts = rest.split(':');
    let mut field = |max: i32| -> Option<Option<i32>> {
        match parts.next() {
            None => Some(None),
            Some(p) if p.len() == 2 && p.bytes().all(|b| b.is_ascii_digit()) => {
                let n: i32 = p.parse().ok()?;
                (n <= max).then_some(Some(n))
            }
            Some(_) => None,
        }
    };
    let hours = field(25)??;
    let minutes = field(59)?.unwrap_or(0);
    let seconds = field(59)?.unwrap_or(0);
    if parts.next().is_some() {
        return None;
    }
    Offset::from_seconds(sign * (hours * 3600 + minutes * 60 + seconds)).ok()
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
    // Fixed rules before a rule's text: rules that never change offset — a `TZ` rule with no
    // summer time (`EST5`, `UTC-3`), or a zone file with one offset — are that offset, which
    // has a name to write; only rules that change it stay rules with no name.
    let label = if let Some(name) = tz.iana_name().filter(|n| !n.is_empty()) {
        ZoneLabel::Named(name.to_string())
    } else if let Some(offset) = fixed_offset_of(&tz) {
        ZoneLabel::FixedOffset(offset)
    } else if let Some(rule) = tz_env.filter(|s| TimeZone::posix(s).is_ok()) {
        ZoneLabel::RuleString(rule.to_string())
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

    /// **A `TZ` rule with no summer time is the fixed offset it names**, recorded and shown as
    /// that offset — POSIX signs are inverted, so `UTC-3` is three hours east, as `date` reads
    /// it. Only a rule whose offset changes stays a rule with no name.
    #[test]
    fn a_tz_rule_with_no_summer_time_is_its_fixed_offset() {
        for (rule, name) in [
            ("EST5", "-05:00"),
            ("UTC-3", "+03:00"),
            ("<+03>-3", "+03:00"),
        ] {
            let zone = zone_from(Some(rule), TimeZone::posix(rule));
            assert!(
                matches!(zone.label(), ZoneLabel::FixedOffset(_)),
                "{rule}: {:?}",
                zone.label()
            );
            assert_eq!(zone.label().to_string(), name, "{rule}");
            assert_eq!(zone.recordable().unwrap().name(), name, "{rule}");
        }
    }

    /// A zone file in its legacy form (TZif version 1, no rule after its last transition —
    /// the form jiff walks without end): one type per offset, and transitions as
    /// `(seconds since 1970, type index)`.
    fn tzif(offsets: &[i32], transitions: &[(i32, u8)]) -> Vec<u8> {
        let types: Vec<(i32, bool)> = offsets.iter().map(|o| (*o, false)).collect();
        let mut out = Vec::new();
        tzif_block(&mut out, b'\0', false, &types, transitions, b"ZZZ\0");
        out
    }

    /// A zone file in the form a modern zone database holds (TZif version 2): the legacy block,
    /// the same block with 64-bit times, then the rule that follows the last transition.
    fn tzif2(
        offsets: &[i32],
        transitions: &[(i32, u8)],
        abbreviation: &str,
        rule: &str,
    ) -> Vec<u8> {
        let types: Vec<(i32, bool)> = offsets.iter().map(|o| (*o, false)).collect();
        let chars = format!("{abbreviation}\0").into_bytes();
        let mut out = Vec::new();
        tzif_block(&mut out, b'2', false, &types, transitions, &chars);
        tzif_block(&mut out, b'2', true, &types, transitions, &chars);
        out.extend(format!("\n{rule}\n").into_bytes());
        out
    }

    /// One TZif header and data block; `types` are `(offset, is summer time)`, every one named
    /// by the one abbreviation in `chars`.
    fn tzif_block(
        out: &mut Vec<u8>,
        version: u8,
        wide: bool,
        types: &[(i32, bool)],
        transitions: &[(i32, u8)],
        chars: &[u8],
    ) {
        out.extend(b"TZif");
        out.push(version);
        out.extend([0u8; 15]);
        for count in [0, 0, 0, transitions.len(), types.len(), chars.len()] {
            out.extend((count as u32).to_be_bytes());
        }
        for (at, _) in transitions {
            if wide {
                out.extend(i64::from(*at).to_be_bytes());
            } else {
                out.extend(at.to_be_bytes());
            }
        }
        out.extend(transitions.iter().map(|(_, index)| *index));
        for (offset, summer) in types {
            out.extend(offset.to_be_bytes());
            out.extend([u8::from(*summer), 0u8]);
        }
        out.extend(chars);
    }

    /// **The walk over a zone's transitions ends**, and fails here rather than hanging where it
    /// would not: a legacy zone file repeats its last transition without end in jiff.
    #[test]
    fn the_transition_walk_ends_for_a_zone_file_with_no_rule_after_it() {
        let tz = TimeZone::tzif("", &tzif(&[21200, 19800], &[(-764_142_200, 1)])).unwrap();
        let (sent, received) = std::sync::mpsc::channel();
        std::thread::spawn(move || sent.send(offset_changes(&tz)));
        let changes = received
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("the walk over the transitions ended");
        let at = Timestamp::from_second(-764_142_200).unwrap();
        assert_eq!(changes, [(at, Offset::from_seconds(19800).unwrap())]);
    }

    /// **The walk stops only where transitions stop**: a first transition that keeps the offset
    /// (a summer-time flag alone) is passed over, and a later change still counts — so such a
    /// zone is not fixed, and two zones differing only after a shared first change are two.
    #[test]
    fn the_transition_walk_reads_past_a_transition_that_keeps_the_offset() {
        let keeps_then_changes = [(10800, false), (10800, true), (7200, false)];
        let mut bytes = Vec::new();
        tzif_block(
            &mut bytes,
            b'\0',
            false,
            &keeps_then_changes,
            &[(1_000_000, 1), (2_000_000, 2)],
            b"ZZZ\0",
        );
        let tz = TimeZone::tzif("", &bytes).unwrap();
        let at = Timestamp::from_second(2_000_000).unwrap();
        assert_eq!(
            offset_changes(&tz),
            [(at, Offset::from_seconds(7200).unwrap())]
        );
        assert_eq!(zone_from(None, Ok(tz)).label(), &ZoneLabel::UnnamedRules);

        let later = |last: i32| {
            zone_from(
                None,
                TimeZone::tzif(
                    "",
                    &tzif(&[10800, 7200, last], &[(1_000_000, 1), (2_000_000, 2)]),
                ),
            )
        };
        assert!(!later(3600).reads_like(&later(0)));
    }

    /// **Unnamed system rules that never change offset are that offset too** — the zone-file
    /// door, with no `TZ` text: a copied `Etc/GMT-3` reads as `TZ=UTC-3` does, and a copied
    /// zone whose offset once changed stays rules with no name. Rules whose offset never
    /// changes are fixed even where they name a summer time.
    #[test]
    fn unnamed_rules_that_never_change_offset_are_a_fixed_offset() {
        let east = Offset::from_hours(3).unwrap();
        let copied = zone_from(None, TimeZone::tzif("", &tzif(&[10800], &[])));
        assert_eq!(copied.label(), &ZoneLabel::FixedOffset(east));
        assert_eq!(copied.recordable().unwrap().name(), "+03:00");
        let copied_modern = zone_from(
            None,
            TimeZone::tzif("", &tzif2(&[10800], &[], "+03", "<+03>-3")),
        );
        assert_eq!(copied_modern.label(), &ZoneLabel::FixedOffset(east));
        let changed_once = TimeZone::tzif("", &tzif(&[21200, 19800], &[(-764_142_200, 1)]));
        assert_eq!(
            zone_from(None, changed_once).label(),
            &ZoneLabel::UnnamedRules
        );
        let zone = zone_from(None, TimeZone::posix("<+03>-3"));
        assert_eq!(zone.label(), &ZoneLabel::FixedOffset(east));
        assert_eq!(zone.recordable().unwrap().name(), "+03:00");
        let rule = "XXX3YYY3,M3.2.0,M11.1.0";
        let zone = zone_from(Some(rule), TimeZone::posix(rule));
        assert_eq!(zone.label(), &ZoneLabel::FixedOffset(-east));
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

    // ---- The zone-name parse ----

    #[test]
    fn a_zone_name_parses_to_its_canonical_name() {
        for (written, name) in [
            ("Europe/Amsterdam", "Europe/Amsterdam"),
            ("europe/amsterdam", "Europe/Amsterdam"),
            ("  America/New_York ", "America/New_York"),
            ("UTC", "UTC"),
            ("utc", "UTC"),
            ("+03:00", "+03:00"),
            ("+03", "+03:00"),
            ("-05:30", "-05:30"),
        ] {
            let zone = parse_zone_name(written).unwrap_or_else(|e| panic!("{written}: {e:?}"));
            assert_eq!(zone.name(), name, "{written}");
        }
    }

    #[test]
    fn a_rule_string_is_refused_never_flattened() {
        let rule = "EST5EDT4,M3.2.0,M11.1.0";
        assert_eq!(
            parse_zone_name(rule).unwrap_err(),
            ZoneNameError::RuleString(rule.into())
        );
    }

    #[test]
    fn an_unknown_name_is_not_known_here() {
        assert_eq!(
            parse_zone_name("Europe/Amsterdm").unwrap_err(),
            ZoneNameError::NotKnownHere("Europe/Amsterdm".into())
        );
        assert_eq!(
            parse_zone_name("+3:00").unwrap_err(),
            ZoneNameError::NotKnownHere("+3:00".into()),
            "an offset is written with two-digit hours"
        );
    }

    #[test]
    fn an_empty_zone_line_names_no_zone() {
        assert_eq!(parse_zone_name("").unwrap_err(), ZoneNameError::Empty);
        assert_eq!(parse_zone_name("   ").unwrap_err(), ZoneNameError::Empty);
    }

    /// Whatever a machine's zone writes into a manifest, the next run reads back as the same
    /// zone. Red-smoked by `recordable` writing a named zone's current offset instead of its
    /// name: the name comes back as `+02:00`, not the zone.
    #[test]
    fn every_recordable_machine_zone_parses_back() {
        let machines = [
            zone_from(None, TimeZone::get("Europe/Amsterdam")),
            zone_from(Some("America/New_York"), TimeZone::get("America/New_York")),
            zone_from(None, Ok(TimeZone::UTC)),
            UserZone::utc(),
            UserZone::for_test_offset(Offset::from_hours(3).unwrap()),
            UserZone::for_test_offset(Offset::from_seconds(-(5 * 3600 + 30 * 60)).unwrap()),
            zone_from(Some("UTC-3"), TimeZone::posix("UTC-3")),
            zone_from(None, TimeZone::posix("<+03>-3")),
            zone_from(
                Some("XXX3YYY3,M3.2.0,M11.1.0"),
                TimeZone::posix("XXX3YYY3,M3.2.0,M11.1.0"),
            ),
            zone_from(None, TimeZone::tzif("", &tzif(&[10800], &[]))),
            zone_from(
                None,
                TimeZone::tzif("", &tzif2(&[10800], &[], "+03", "<+03>-3")),
            ),
        ];
        for machine in machines {
            let recorded = machine.recordable().expect("recordable");
            let back = parse_zone_name(recorded.name()).expect("parses back");
            assert_eq!(back.name(), recorded.name());
            assert!(back.same_zone(&recorded), "{}", recorded.name());
            assert!(back.clock().reads_like(&machine), "{}", recorded.name());
        }
    }

    #[test]
    fn a_zone_with_no_name_cannot_be_recorded() {
        let rule = "EST5EDT4,M3.2.0,M11.1.0";
        let from_tz = zone_from(Some(rule), TimeZone::posix(rule));
        assert_eq!(
            from_tz.recordable().unwrap_err(),
            Unrecordable::RuleString(rule.into())
        );
        let unnamed = zone_from(None, TimeZone::posix("CET-1CEST,M3.5.0,M10.5.0/3"));
        assert_eq!(
            unnamed.recordable().unwrap_err(),
            Unrecordable::UnnamedRules
        );
        assert_eq!(
            UserZone::for_test_unknown(Some("Europe/Amsterdm"))
                .recordable()
                .unwrap_err(),
            Unrecordable::Unknown {
                unreadable_tz: Some("Europe/Amsterdm".into())
            }
        );
    }

    /// A recorded name carries the zone's rules, not today's offset: summer and winter read
    /// differently.
    #[test]
    fn a_recorded_zone_reads_summer_and_winter() {
        let zone = parse_zone_name("Europe/Amsterdam").unwrap();
        let summer = Timestamp::from_second(1_718_452_800).unwrap(); // 2024-06-15T12:00:00Z
        let winter = Timestamp::from_second(1_705_320_000).unwrap(); // 2024-01-15T12:00:00Z
        assert_eq!(zone.clock().to_clock(summer).hour(), 14);
        assert_eq!(zone.clock().to_clock(winter).hour(), 13);
    }

    /// Compared through the parse, never the text: a case variant is one zone.
    #[test]
    fn a_case_variant_names_the_same_zone() {
        let a = parse_zone_name("europe/amsterdam").unwrap();
        let b = parse_zone_name("Europe/Amsterdam").unwrap();
        assert_eq!(a.name(), b.name());
        assert!(a.same_zone(&b));
    }

    /// The zone data gives a link its own name, so the rules decide: a link and the zone it
    /// points at are one zone, and two zones that agree today but not in their past are two.
    #[test]
    fn a_link_names_the_same_zone_and_a_neighbour_does_not() {
        let link = parse_zone_name("US/Eastern").unwrap();
        let target = parse_zone_name("America/New_York").unwrap();
        assert_ne!(link.name(), target.name());
        assert!(link.same_zone(&target));

        let amsterdam = parse_zone_name("Europe/Amsterdam").unwrap();
        let paris = parse_zone_name("Europe/Paris").unwrap();
        assert!(!amsterdam.same_zone(&paris), "they differ before 1940");
        assert!(!amsterdam.same_zone(&target));
        assert!(!parse_zone_name("+01:00").unwrap().same_zone(&amsterdam));
    }

    #[test]
    fn a_zone_label_is_said_plainly() {
        assert_eq!(ZoneLabel::Named("UTC".into()).to_string(), "UTC");
        assert_eq!(
            ZoneLabel::FixedOffset(Offset::from_hours(3).unwrap()).to_string(),
            "+03:00"
        );
        assert_eq!(
            ZoneLabel::RuleString("EST5EDT4,M3.2.0,M11.1.0".into()).to_string(),
            "rules with no name"
        );
        assert_eq!(ZoneLabel::UnnamedRules.to_string(), "rules with no name");
        assert_eq!(
            ZoneLabel::Unknown {
                unreadable_tz: None
            }
            .to_string(),
            "unknown"
        );
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

    /// **A record names the clock it read times on in text a stranger can read**: a zone by
    /// its name, a fixed offset by its name, a `TZ` rule as written, unnamed rules by their
    /// offset at run time, and a zone Canon cannot read as UTC, said to be unknown — the same
    /// text whether or not `TZ` held something unreadable. A manifest's run names the zone the
    /// manifest records.
    #[test]
    fn read_times_on_states_each_kind_of_zone() {
        let of = |zone: &UserZone| ReadTimesOn::of_user(zone).as_str().to_string();
        assert_eq!(
            of(&UserZone::for_test("Europe/Amsterdam")),
            "Europe/Amsterdam"
        );
        assert_eq!(
            of(&UserZone::for_test_offset(Offset::from_hours(3).unwrap())),
            "+03:00"
        );
        let rule = "EST5EDT,M3.2.0,M11.1.0";
        assert_eq!(of(&zone_from(Some(rule), TimeZone::posix(rule))), rule);

        let unnamed = zone_from(None, TimeZone::posix("CET-1CEST,M3.5.0,M10.5.0/3"));
        assert_eq!(unnamed.label(), &ZoneLabel::UnnamedRules);
        // 2024-07-01T00:00:00Z, summer time; 2024-01-15T00:00:00Z, winter.
        with_test_now(1_719_792_000, || {
            assert_eq!(
                of(&unnamed),
                "local rules with no name (UTC+02:00 at run time)"
            );
        });
        with_test_now(1_705_276_800, || {
            assert_eq!(
                of(&unnamed),
                "local rules with no name (UTC+01:00 at run time)"
            );
        });

        for unknown in [
            UserZone::for_test_unknown(None),
            UserZone::for_test_unknown(Some("Europe/Amsterdm")),
        ] {
            assert_eq!(of(&unknown), "UTC (this machine's time zone is unknown)");
        }

        let recorded = parse_zone_name("europe/amsterdam").unwrap();
        assert_eq!(
            ReadTimesOn::of_manifest(&recorded).as_str(),
            "Europe/Amsterdam"
        );
    }
}
