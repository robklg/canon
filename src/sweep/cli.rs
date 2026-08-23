//! sweep command — the reduction-opportunities leaderboard.
//!
//! The finder seat: the one command whose output is places. Interface layer
//! only: parse args, call ops::sweep, format output. Every ranking and
//! grouping decision arrives in the typed result — the interface classifies
//! nothing. Finding paths render full, never capped: location is the
//! reader's primary context on this surface.

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;

use crate::core::domain::format::{format_count, format_size, shell_quote};
use crate::core::repo::Db;
use crate::notes::format_note_date;
use crate::sweep::domain::{
    HubEntry, LeaderboardEntry, Location, RelationClass, RelationShape, StructuralFinding,
    SuspendedRootTally, SweepParams,
};
use crate::sweep::ops::{compute_sweep, SweepOptions, SweepOutcome, SweepReport};

/// Hub members shown before the "… N more" footer; `--all` shows all.
const HUB_MEMBER_CAP: usize = 5;

/// Above this many suspended roots the footer counts roots rather than
/// naming them, and the way back becomes the suspended-root listing.
const SUSPENDED_ROOTS_NAMED_CAP: usize = 3;

/// Degenerate-universe messages — distinct by design: an empty archive is
/// an achievement, an unscanned one is a starting point.
const NO_ROOTS_MSG: &str = "No roots yet — nothing to sweep.";
const NO_HASHED_MSG: &str = "No hashed content yet. The sweep compares by content hash.";

pub fn run(db: &mut Db, limit: Option<usize>, all: bool) -> Result<()> {
    let outcome = compute_sweep(db.conn(), &SweepOptions { limit, all })?;
    println!("Sweep: all roots");
    match outcome {
        SweepOutcome::NoRoots => {
            println!();
            println!("{NO_ROOTS_MSG}");
            println!("Add one with 'canon scan <path> --add --role source'.");
        }
        SweepOutcome::NoHashedContent => {
            println!();
            println!("{NO_HASHED_MSG}");
            println!("Run 'canon scan' to hash sources.");
        }
        SweepOutcome::Report(report) => {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);
            print_report(&report, now, all);
        }
    }
    Ok(())
}

fn print_report(report: &SweepReport, now: i64, all: bool) {
    let mut header = String::from("Reduction opportunities — fresh, over hashed content");
    if report.stats.ubiquitous_objects_dropped > 0 {
        header.push_str(&format!(
            " · {} ignored",
            counted_phrase(report.stats.ubiquitous_objects_dropped, "ubiquitous object")
        ));
    }
    if report.empty_files_ignored > 0 {
        header.push_str(&format!(
            " · {} ignored",
            counted_phrase(report.empty_files_ignored, "empty file")
        ));
    }
    println!("{header}");

    if report.entries.is_empty() {
        println!();
        println!("{}", empty_board_line(report));
        print_footers(report, all);
        return;
    }

    for (i, entry) in report.entries.iter().enumerate() {
        println!();
        let (handoff_line, _) = handoff(entry);
        match entry {
            LeaderboardEntry::Single(finding) => {
                print_finding(i + 1, finding, report, now, &handoff_line)
            }
            LeaderboardEntry::Hub(hub) => print_hub(i + 1, hub, report, now, all, &handoff_line),
        }
    }
    print_footers(report, all);
}

/// The empty-board line. An empty leaderboard is an answer — but it must
/// never be a **false** one: the headline may deny that folder-level
/// redundancy exists only when nothing was withheld from the board. Where
/// something was, the redundancy is real and simply not shown, and a footer
/// contradicting the headline one line later is not a repair.
///
/// The floors are deliberately not in that list: "no redundancy worth
/// attention" is scoped to *above the floors* in the sentence itself, and
/// below-floor findings are by definition below that bar. (Under `--all` the
/// floors are lifted and this function cannot see it, so the bar it names is
/// then weaker than warranted — a claim short of the truth, never false.)
fn empty_board_line(report: &SweepReport) -> String {
    let floors = SweepParams::default();
    let bar = format!(
        "above {} / {} files",
        format_size(floors.emit_floor_bytes as i64),
        format_count(floors.emit_floor_files),
    );
    if report.beyond_cap > 0 {
        // The cap emptied the board, not the universe; the cap line below
        // says how many are waiting.
        "No reduction opportunities shown at this limit.".to_string()
    } else if !report.suspended.is_empty() {
        format!("No reduction opportunities {bar} outside the suspended roots.")
    } else {
        format!("No reduction opportunities {bar} — no folder-level redundancy worth attention.")
    }
}

fn print_footers(report: &SweepReport, all: bool) {
    // The parked lines print first: they explain a board that changed without
    // the user acting on the board, which the below-floor and cap lines never
    // do. They print under `--all` too — `--all` reveals what the floors and
    // the cap hid, and suspension is a door the user closed; the way back is
    // `canon roots unsuspend`, and only that.
    let mut lines: Vec<String> = suspended_lines(&report.suspended)
        .into_iter()
        .map(|(display, _)| display)
        .collect();
    if !all && report.stats.below_floor_subjects > 0 {
        lines.push(format!(
            "{} more below the emit floors (--all)",
            format_count(report.stats.below_floor_subjects)
        ));
    }
    if report.beyond_cap > 0 {
        let noun = if report.beyond_cap == 1 {
            "entry"
        } else {
            "entries"
        };
        lines.push(format!(
            "{} more {noun} (--limit/--all)",
            format_count(report.beyond_cap)
        ));
    }
    if !lines.is_empty() {
        println!();
        for line in &lines {
            println!("{line}");
        }
    }
}

/// The suspended-root footer lines: one per root, or — above three roots —
/// one line counting them. Each carries its own way back, built the way every
/// emitted command is: one builder returning `(display, argv)`, so the
/// round-trip test parses exactly what the user sees.
///
/// "Not ranked", deliberately — not "hidden", not "removed": the places still
/// exist, still ground claims about live places, and return whole on
/// unsuspend. Suspension changes position, never existence.
fn suspended_lines(tallies: &[SuspendedRootTally]) -> Vec<(String, Vec<String>)> {
    if tallies.is_empty() {
        return Vec::new();
    }
    if tallies.len() > SUSPENDED_ROOTS_NAMED_CAP {
        let argv = vec![
            "canon".to_string(),
            "roots".to_string(),
            "list".to_string(),
            "--suspended".to_string(),
        ];
        return vec![(
            format!(
                "{} — not ranked: {} · {}",
                counted_phrase(tallies.len(), "suspended root"),
                causes_phrase(
                    Cause {
                        places: tallies.iter().map(|t| t.places_on_it).sum(),
                        gain_bytes: tallies.iter().map(|t| t.gain_bytes_on_it).sum(),
                    },
                    Cause {
                        places: tallies.iter().map(|t| t.places_with_copies_on_it).sum(),
                        gain_bytes: tallies.iter().map(|t| t.gain_bytes_with_copies_on_it).sum(),
                    },
                    "them",
                ),
                display_argv(&argv),
            ),
            argv,
        )];
    }
    tallies
        .iter()
        .map(|t| {
            let argv = vec![
                "canon".to_string(),
                "roots".to_string(),
                "unsuspend".to_string(),
                format!("path:{}", t.root_path),
            ];
            (
                format!(
                    "{} suspended — not ranked: {} · {}",
                    t.root_path,
                    causes_phrase(
                        Cause {
                            places: t.places_on_it,
                            gain_bytes: t.gain_bytes_on_it,
                        },
                        Cause {
                            places: t.places_with_copies_on_it,
                            gain_bytes: t.gain_bytes_with_copies_on_it,
                        },
                        "it",
                    ),
                    display_argv(&argv),
                ),
                argv,
            )
        })
        .collect()
}

/// What one cause kept off the board: how many places, and what they were
/// worth. A named pair rather than loose numbers, so a caller cannot hand the
/// two causes' figures over crossed — the collapsed line builds one of these
/// from cross-root totals, which is why `causes_phrase` does not take a
/// `SuspendedRootTally` (its `root_path` would name no root).
struct Cause {
    places: usize,
    gain_bytes: u64,
}

/// One consequence stated once, two causes in parallel differing by one word:
/// places **on** the root, places **with copies on** it.
///
/// Each cause carries its own figure and the two are never added — the axes
/// are content-correlated, so one merged figure states the same bytes twice.
///
/// **The figure is rendered as an upper bound (`up to N`), and that is the
/// whole claim.** A sum of per-place gain is not what dismissing those places
/// would recover: they can be each other's evidence — three identical folders
/// on one parked root each report their own gain, and at most two of them can
/// ever be let go — so the sum overstates, without a bound. What is exactly
/// true is the inequality: every place resolves at most its own matched
/// bytes, so what is recoverable is never more than the sum.
///
/// Do **not** name this `gain`. The board defines that word as *what acting
/// on the finding resolves*, and `total gain:` earns it — a hub's members all
/// point into a counterpart that is never a member, so they are
/// co-dismissable and their sum is achievable within the concentration slack.
/// These summands have no such role separation. Borrowing the word would
/// assert the achievability the figure lacks, which is a stronger claim than
/// the bare number it was meant to hedge.
fn causes_phrase(on: Cause, with_copies: Cause, pronoun: &str) -> String {
    let mut causes: Vec<String> = Vec::new();
    if on.places > 0 {
        causes.push(format!(
            "{} on {pronoun} (up to {})",
            counted_phrase(on.places, "place"),
            format_size(on.gain_bytes as i64),
        ));
    }
    if with_copies.places > 0 {
        // The noun is elided when this is the second cause: "8 places on it,
        // 4 with copies on it".
        let count = if causes.is_empty() {
            counted_phrase(with_copies.places, "place")
        } else {
            format_count(with_copies.places)
        };
        causes.push(format!(
            "{count} with copies on {pronoun} (up to {})",
            format_size(with_copies.gain_bytes as i64),
        ));
    }
    causes.join(", ")
}

fn print_finding(
    rank: usize,
    finding: &StructuralFinding,
    report: &SweepReport,
    now: i64,
    handoff_line: &str,
) {
    println!(
        "#{rank}  {}{}",
        abs_path(&finding.subject),
        archive_mark(finding)
    );
    match &finding.shape {
        RelationShape::Pair {
            counterpart,
            class,
            pair_size_pct,
            pair_count_pct,
            counterpart_is_archive,
            counterpart_last_scanned_at,
            ..
        } => {
            match class {
                RelationClass::Mirror => println!(
                    "    mirrors {}  ({} by size · {} by count)",
                    abs_path(counterpart),
                    pct(*pair_size_pct),
                    pct(*pair_count_pct)
                ),
                RelationClass::Subset => println!(
                    "    {} inside {}  (by size · {} by count)",
                    pct(*pair_size_pct),
                    abs_path(counterpart),
                    pct(*pair_count_pct)
                ),
            }
            println!(
                "    {}",
                counterpart_line(
                    *counterpart_is_archive,
                    *counterpart_last_scanned_at,
                    finding.subject_last_scanned_at,
                    now
                )
            );
        }
        RelationShape::Coverage {
            locations,
            archived_locations,
            suspended_locations,
        } => {
            // Scattered evidence says how much of it stands behind a closed
            // door, beside the archived count it already carries. Suppressed
            // at zero: "0 suspended" is noise, and the entry is neither sunk
            // nor demoted for it — no count and no percentage moves.
            let mut qualifier = format!("{} archived", format_count(*archived_locations));
            if *suspended_locations > 0 {
                qualifier.push_str(&format!(
                    " · {} suspended",
                    format_count(*suspended_locations)
                ));
            }
            println!(
                "    {} exists elsewhere, across {} locations ({qualifier})",
                pct(finding.containment_size_pct),
                format_count(*locations),
            );
            println!(
                "    scattered; consolidation candidate · subject {}",
                age(finding.subject_last_scanned_at, now)
            );
        }
    }
    // Only when it would read below 100%: the threshold is where the
    // percentage below rounds up, and "compared on 100%" is a line that
    // says nothing.
    if finding.hash_coverage_pct < 0.9995 {
        println!("    compared on {} by size", pct(finding.hash_coverage_pct));
    }
    println!(
        "    gain: {} · {}     residual: {}",
        files_phrase(finding.gain_files),
        format_size(finding.gain_bytes as i64),
        residual_phrase(finding)
    );
    for ctx in &finding.context {
        println!(
            "    also {} inside {}",
            pct(ctx.size_pct),
            abs_path(&ctx.location)
        );
    }
    print_excluded_context(&finding.subject, report, "    ");
    print_notes(&finding.subject, report, "    ", None);
    if let RelationShape::Pair { counterpart, .. } = &finding.shape {
        print_notes(counterpart, report, "    ", Some("at counterpart"));
    }
    println!("    {handoff_line}");
}

fn print_hub(
    rank: usize,
    hub: &HubEntry,
    report: &SweepReport,
    now: i64,
    all: bool,
    handoff_line: &str,
) {
    // A hub's headline is the shared counterpart, not any one subject — the
    // reverse of a single finding, and the line below states that out loud
    // because readers assume the subject otherwise.
    println!("#{rank}  {}", abs_path(&hub.counterpart));
    let status = if hub.counterpart_is_archive {
        format!("archived, {}", age(hub.counterpart_last_scanned_at, now))
    } else {
        age(hub.counterpart_last_scanned_at, now)
    };
    println!(
        "    shared counterpart — {} hold copies inside it · {status}",
        counted_phrase(hub.members.len(), "place")
    );
    println!(
        "    total gain: {} · {}",
        files_phrase(hub.total_gain_files),
        format_size(hub.total_gain_bytes as i64)
    );
    print_notes(&hub.counterpart, report, "    ", None);
    let shown = if all {
        hub.members.len()
    } else {
        hub.members.len().min(HUB_MEMBER_CAP)
    };
    for member in &hub.members[..shown] {
        // Hub members are pair-shaped by construction; the coverage arm is
        // the same defensive fold the trail uses — render, never panic on a
        // line the interface cannot repair.
        let relation = match &member.shape {
            RelationShape::Pair {
                class: RelationClass::Mirror,
                pair_size_pct,
                ..
            } => format!("mirrors · {}", pct(*pair_size_pct)),
            RelationShape::Pair { pair_size_pct, .. } => {
                format!("{} inside", pct(*pair_size_pct))
            }
            RelationShape::Coverage { .. } => {
                format!("{} elsewhere", pct(member.containment_size_pct))
            }
        };
        println!(
            "      {}{}  {} · {} · {}",
            abs_path(&member.subject),
            archive_mark(member),
            relation,
            files_phrase(member.gain_files),
            format_size(member.gain_bytes as i64)
        );
        print_excluded_context(&member.subject, report, "        ");
        print_notes(&member.subject, report, "        ", None);
    }
    if hub.members.len() > shown {
        println!(
            "      … {} more (--all)",
            format_count(hub.members.len() - shown)
        );
    }
    println!("    {handoff_line}");
}

/// A subject standing on an archive root is already resolved under the
/// triage lens — stated on the finding, never silently.
fn archive_mark(finding: &StructuralFinding) -> &'static str {
    if finding.subject_is_archive {
        "  (in the archive)"
    } else {
        ""
    }
}

fn print_excluded_context(subject: &Location, report: &SweepReport, indent: &str) {
    if let Some(count) = report.excluded_context.get(subject) {
        println!(
            "{indent}{} here already excluded",
            counted_phrase(*count, "source")
        );
    }
}

fn print_notes(loc: &Location, report: &SweepReport, indent: &str, label: Option<&str>) {
    if let Some(notes) = report.notes.get(loc) {
        for note in notes {
            let suffix = label.map(|l| format!(", {l}")).unwrap_or_default();
            println!(
                "{indent}note ({}{suffix}): \"{}\"",
                format_note_date(note.created_at),
                note.text
            );
        }
    }
}

/// The counterpart-standing line of a pair finding, uniform across subset
/// and mirror (the relation line above it already carries the class).
/// Wording discipline: the line states the counterpart's standing and must
/// never imply a preferred side — a containment fact is not a worth verdict.
/// Declarative throughout: "inside X", never "keep X".
///
/// No suspension branch: a shown entry cannot have a parked root on either
/// side, so suspension is stated on the footer's own lines and — for
/// scattered evidence — as a location count, and nowhere else on the board.
fn counterpart_line(
    is_archive: bool,
    counterpart_scanned: Option<i64>,
    subject_scanned: Option<i64>,
    now: i64,
) -> String {
    let subject_part = format!("subject {}", age(subject_scanned, now));
    let standing = if is_archive { "archived" } else { "present" };
    format!(
        "counterpart: {standing}, {} · {subject_part}",
        age(counterpart_scanned, now)
    )
}

/// The judging handoff for an entry: the display line and the argv the
/// round-trip test parses. One builder for both, so the test parses exactly
/// what the user sees. Judging handoffs only — never an exclusion
/// invocation: the journey is find → judge → decide → record.
fn handoff(entry: &LeaderboardEntry) -> (String, Vec<String>) {
    let argv: Vec<String> = match entry {
        LeaderboardEntry::Single(finding) => match &finding.shape {
            RelationShape::Pair { counterpart, .. } => vec![
                "canon".into(),
                "survey".into(),
                ".".into(),
                "--other".into(),
                abs_path(counterpart),
            ],
            RelationShape::Coverage { .. } => vec!["canon".into(), "survey".into(), ".".into()],
        },
        LeaderboardEntry::Hub(hub) => {
            vec!["canon".into(), "survey".into(), abs_path(&hub.counterpart)]
        }
    };
    let display = display_argv(&argv);
    (format!("→ {display}"), argv)
}

/// An emitted command as the user sees it: shell-quoted, space-joined. The
/// argv beside it stays raw, which is what the round-trip test parses.
fn display_argv(argv: &[String]) -> String {
    argv.iter()
        .map(|a| shell_quote(a))
        .collect::<Vec<_>>()
        .join(" ")
}

fn abs_path(loc: &Location) -> String {
    if loc.rel_prefix.is_empty() {
        loc.root_path.clone()
    } else {
        format!("{}/{}", loc.root_path.trim_end_matches('/'), loc.rel_prefix)
    }
}

fn pct(fraction: f64) -> String {
    format!("{:.0}%", fraction * 100.0)
}

fn counted_phrase(count: usize, noun: &str) -> String {
    let plural = if count == 1 { "" } else { "s" };
    format!("{} {noun}{plural}", format_count(count))
}

fn files_phrase(files: u32) -> String {
    counted_phrase(files as usize, "file")
}

fn residual_phrase(finding: &StructuralFinding) -> String {
    if finding.residual_bytes == 0 && finding.residual_files == 0 {
        "none".to_string()
    } else {
        format!(
            "{} · {} nowhere else",
            files_phrase(finding.residual_files),
            format_size(finding.residual_bytes as i64)
        )
    }
}

fn age(scanned: Option<i64>, now: i64) -> String {
    match scanned {
        Some(ts) => {
            let days = (now - ts).max(0) / 86_400;
            if days == 0 {
                "scanned today".to_string()
            } else {
                format!("scanned {days}d ago")
            }
        }
        None => "scan age unknown".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sweep::domain::{FindingNature, FindingTier};
    use clap::Parser;

    fn loc(root_path: &str, rel: &str) -> Location {
        Location {
            root_id: 1,
            root_path: root_path.to_string(),
            rel_prefix: rel.to_string(),
        }
    }

    fn finding(shape: RelationShape) -> StructuralFinding {
        StructuralFinding {
            subject: loc("/r1", "subject"),
            subject_suspended: false,
            subject_is_archive: false,
            subject_last_scanned_at: None,
            tier: FindingTier::Clean,
            below_floors: false,
            shape,
            context: Vec::new(),
            containment_size_pct: 0.96,
            containment_count_pct: 0.9,
            gain_bytes: 1_000,
            gain_files: 10,
            residual_bytes: 0,
            residual_files: 0,
            archive_cover_pct: 0.0,
            hash_coverage_pct: 1.0,
            nature: FindingNature::Consolidate,
        }
    }

    fn pair(counterpart: Location) -> RelationShape {
        RelationShape::Pair {
            counterpart,
            class: RelationClass::Subset,
            pair_size_pct: 0.96,
            pair_count_pct: 0.9,
            counterpart_share_pct: 0.5,
            counterpart_suspended: false,
            counterpart_is_archive: false,
            counterpart_last_scanned_at: None,
        }
    }

    /// Every handoff shape the sweep can emit must parse through the real
    /// argument definitions — CLI drift becomes a test failure, not
    /// user-facing rot.
    #[test]
    fn every_handoff_shape_parses() {
        let entries = vec![
            LeaderboardEntry::Single(finding(pair(loc("/r2", "q")))),
            LeaderboardEntry::Single(finding(RelationShape::Coverage {
                locations: 5,
                archived_locations: 1,
                suspended_locations: 0,
            })),
            LeaderboardEntry::Hub(HubEntry {
                counterpart: loc("/r2", "hub"),
                counterpart_is_archive: false,
                counterpart_last_scanned_at: None,
                members: vec![finding(pair(loc("/r2", "hub")))],
                total_gain_bytes: 1_000,
                total_gain_files: 10,
            }),
            LeaderboardEntry::Single(finding(pair(loc("/Volumes/My Drive", "old backup/photos")))),
        ];
        for entry in &entries {
            let (display, argv) = handoff(entry);
            assert!(display.starts_with("→ canon survey"));
            crate::Cli::try_parse_from(&argv)
                .unwrap_or_else(|e| panic!("handoff must parse: {display}\n{e}"));
        }
    }

    #[test]
    fn spaced_paths_are_quoted_for_display_but_raw_in_argv() {
        let entry =
            LeaderboardEntry::Single(finding(pair(loc("/Volumes/My Drive", "old backup/photos"))));
        let (display, argv) = handoff(&entry);
        assert!(display.contains("'/Volumes/My Drive/old backup/photos'"));
        assert_eq!(argv.last().unwrap(), "/Volumes/My Drive/old backup/photos");
    }

    fn report_with(suspended: Vec<SuspendedRootTally>, beyond_cap: usize) -> SweepReport {
        SweepReport {
            entries: Vec::new(),
            beyond_cap,
            suspended,
            stats: crate::sweep::domain::SweepStats {
                ubiquitous_objects_dropped: 0,
                ubiquitous_bytes_dropped: 0,
                below_floor_subjects: 0,
            },
            empty_files_ignored: 0,
            excluded_context: std::collections::HashMap::new(),
            notes: std::collections::HashMap::new(),
        }
    }

    fn parked(
        root_path: &str,
        on_it: usize,
        copies: usize,
        gain_on_it: u64,
        gain_copies: u64,
    ) -> SuspendedRootTally {
        SuspendedRootTally {
            root_path: root_path.to_string(),
            places_on_it: on_it,
            places_with_copies_on_it: copies,
            gain_bytes_on_it: gain_on_it,
            gain_bytes_with_copies_on_it: gain_copies,
        }
    }

    #[test]
    fn the_footer_states_one_line_per_suspended_root_with_both_causes() {
        let lines = suspended_lines(&[
            parked(
                "/Volumes/fixture-both",
                8,
                4,
                185_300_000_000,
                12_100_000_000,
            ),
            parked("/Volumes/fixture-on-it", 8, 0, 185_300_000_000, 0),
            parked("/Volumes/fixture-copies", 0, 4, 0, 12_100_000_000),
        ]);
        let displays: Vec<&str> = lines.iter().map(|(d, _)| d.as_str()).collect();
        assert_eq!(displays.len(), 3, "one line per root, never one per reason");
        // One consequence stated once; two causes in parallel, differing by
        // one word — and each line names its own way back.
        assert!(displays[0].starts_with("/Volumes/fixture-both suspended — not ranked: "));
        // Each cause carries its own mass, and the two are never added: the
        // axes are content-correlated, so one merged figure would state the
        // same bytes twice on a board whose currency is gain.
        assert!(displays[0]
            .contains("8 places on it (up to 185.3 GB), 4 with copies on it (up to 12.1 GB)"));
        assert!(
            !displays[0].contains("197.4 GB"),
            "the masses are never summed"
        );
        assert!(displays[0].contains("canon roots unsuspend path:/Volumes/fixture-both"));
        assert!(displays[1].contains("8 places on it (up to 185.3 GB)"));
        assert!(!displays[1].contains("copies"));
        assert!(displays[2].contains("4 places with copies on it (up to 12.1 GB)"));
        assert!(!displays[2].contains(" on it, "));
    }

    #[test]
    fn above_three_roots_the_footer_counts_rather_than_names() {
        let tallies: Vec<SuspendedRootTally> = (0..5)
            .map(|i| parked(&format!("/Volumes/r{i}"), 5, 1, 1_000_000_000, 20_000_000))
            .collect();
        let lines = suspended_lines(&tallies);
        assert_eq!(lines.len(), 1);
        let (display, argv) = &lines[0];
        // The collapse sums each cause across roots — the same kind of sum,
        // never a merge of the two causes, and stated as the upper bound it
        // is rather than borrowing the board's `gain`.
        assert!(display.starts_with(
            "5 suspended roots — not ranked: 25 places on them (up to 5.0 GB), 5 with copies on them (up to 100.0 MB)"
        ));
        assert!(
            !display.contains("gain"),
            "the figure never claims achievability"
        );
        assert!(
            !display.contains("/Volumes/r0"),
            "roots are counted, not named"
        );
        assert_eq!(argv[1..], ["roots", "list", "--suspended"]);
        // Three roots is still named — the collapse is above three.
        assert_eq!(suspended_lines(&tallies[..3]).len(), 3);
    }

    #[test]
    fn no_suspended_roots_emits_no_footer_lines() {
        assert!(suspended_lines(&[]).is_empty());
    }

    /// The footer's ways back, under the same round-trip law the handoff lines
    /// obey (`every_handoff_shape_parses` covers those). A way back that does
    /// not run is worse than none: `roots unsuspend` takes a root specifier,
    /// never a bare path.
    #[test]
    fn every_emitted_argv_parses() {
        let mut emitted: Vec<(String, Vec<String>)> = suspended_lines(&[
            parked("/Volumes/fixture-both", 8, 4, 1_000, 500),
            parked("/Volumes/My Drive", 0, 2, 0, 1_000),
        ]);
        emitted.extend(suspended_lines(&[
            parked("/a", 1, 0, 1, 0),
            parked("/b", 1, 0, 1, 0),
            parked("/c", 1, 0, 1, 0),
            parked("/d", 1, 0, 1, 0),
        ]));
        for (display, argv) in &emitted {
            assert!(display.contains("canon roots "));
            crate::Cli::try_parse_from(argv)
                .unwrap_or_else(|e| panic!("emitted command must parse: {display}\n{e}"));
        }
        // The spaced path is quoted for display and raw in the argv, the same
        // discipline the handoff lines follow.
        assert!(emitted[1].0.contains("'path:/Volumes/My Drive'"));
        assert_eq!(emitted[1].1.last().unwrap(), "path:/Volumes/My Drive");
    }

    #[test]
    fn an_emptied_board_never_claims_there_is_no_redundancy() {
        // An empty leaderboard is an answer, but never a false one. With
        // places set aside behind a closed door the redundancy exists and is
        // simply not on this board — claiming otherwise is the false "empty"
        // the registry forbids, and a footer contradicting the headline one
        // line later is not a repair.
        // Nothing withheld: the board may say there is nothing there.
        let bare = report_with(Vec::new(), 0);
        assert!(empty_board_line(&bare).contains("no folder-level redundancy worth attention"));

        // Emptied by a closed door.
        let emptied = report_with(vec![parked("/Volumes/fixture-both", 8, 0, 1_000, 0)], 0);
        let line = empty_board_line(&emptied);
        assert!(!line.contains("no folder-level redundancy"));
        assert!(line.contains("outside the suspended roots"));

        // Emptied by the cap — `--limit 0` is accepted, and the cap line one
        // row below counts what is waiting. The denial is the same falsehood
        // whatever withheld the entries, so the guard tracks "was anything
        // withheld", not "was anything suspended".
        let capped = report_with(Vec::new(), 12);
        let line = empty_board_line(&capped);
        assert!(!line.contains("no folder-level redundancy"));
        assert!(line.contains("at this limit"));

        // Both: the cap is what emptied it, and says so.
        let both = report_with(vec![parked("/Volumes/fixture-both", 8, 0, 1_000, 0)], 12);
        assert!(!empty_board_line(&both).contains("no folder-level redundancy"));
        assert!(empty_board_line(&both).contains("at this limit"));
    }

    #[test]
    fn degenerate_messages_are_distinct() {
        assert_ne!(NO_ROOTS_MSG, NO_HASHED_MSG);
    }
}
