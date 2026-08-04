//! sweep command — the reduction-opportunities leaderboard.
//!
//! The finder seat: the one command whose output is places. Interface layer
//! only: parse args, call ops::sweep, format output. Every ranking and
//! grouping decision arrives in the typed result — the interface classifies
//! nothing. Finding paths render full, never capped: location is the
//! reader's primary context on this surface.

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;

use crate::domain::format::{format_count, format_size};
use crate::domain::sweep::{
    HubEntry, LeaderboardEntry, Location, RelationClass, RelationShape, StructuralFinding,
    SweepParams,
};
use crate::note::format_note_date;
use crate::ops::sweep::{compute_sweep, SweepOptions, SweepOutcome, SweepReport};
use crate::repo::Db;

/// Hub members shown before the "… N more" footer; `--all` shows all.
const HUB_MEMBER_CAP: usize = 5;

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
        let floors = SweepParams::default();
        println!();
        println!(
            "No reduction opportunities above {} / {} files — no folder-level redundancy worth attention.",
            format_size(floors.emit_floor_bytes as i64),
            format_count(floors.emit_floor_files),
        );
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

fn print_footers(report: &SweepReport, all: bool) {
    let mut lines: Vec<String> = Vec::new();
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
            counterpart_suspended,
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
                    *counterpart_suspended,
                    counterpart,
                    *counterpart_last_scanned_at,
                    finding.subject_last_scanned_at,
                    now
                )
            );
        }
        RelationShape::Coverage {
            locations,
            archived_locations,
        } => {
            println!(
                "    {} exists elsewhere, across {} locations ({} archived)",
                pct(finding.containment_size_pct),
                format_count(*locations),
                format_count(*archived_locations)
            );
            println!(
                "    scattered; consolidation candidate · subject {}",
                age(finding.subject_last_scanned_at, now)
            );
        }
    }
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
    println!("#{rank}  {}", abs_path(&hub.counterpart));
    let status = if hub.counterpart_suspended {
        format!(
            "on suspended root {} — reconnect to verify",
            hub.counterpart.root_path
        )
    } else if hub.counterpart_is_archive {
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
fn counterpart_line(
    is_archive: bool,
    suspended: bool,
    counterpart: &Location,
    counterpart_scanned: Option<i64>,
    subject_scanned: Option<i64>,
    now: i64,
) -> String {
    let subject_part = format!("subject {}", age(subject_scanned, now));
    if suspended {
        return format!(
            "counterpart on suspended root {} — reconnect to verify · {subject_part}",
            counterpart.root_path
        );
    }
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
pub(crate) fn handoff(entry: &LeaderboardEntry) -> (String, Vec<String>) {
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
    let display = argv
        .iter()
        .map(|a| shell_quote(a))
        .collect::<Vec<_>>()
        .join(" ");
    (format!("→ {display}"), argv)
}

use crate::domain::format::shell_quote;

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
    use crate::domain::sweep::{FindingNature, FindingTier};
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
            })),
            LeaderboardEntry::Hub(HubEntry {
                counterpart: loc("/r2", "hub"),
                counterpart_is_archive: false,
                counterpart_suspended: false,
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

    #[test]
    fn degenerate_messages_are_distinct() {
        assert_ne!(NO_ROOTS_MSG, NO_HASHED_MSG);
    }
}
