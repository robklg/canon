//! trail command — read the decision trail.
//!
//! Two lenses over one timeline: the scope lens ("what did I do here?",
//! newest first) and the time lens ("what did I do today?", day-grouped
//! story). Notes interleave as the thinking between the actions, visually
//! distinct — a thought must never read as an act.
//!
//! Interface layer only: parse args, call ops::trail, format output.

use std::collections::HashMap;
use std::io::{self, Write};
use std::path::PathBuf;

use anyhow::{anyhow, Result};
use chrono::{Local, TimeZone};
use serde::Serialize;

use crate::domain::decision::Decision;
use crate::domain::extraction::{DecisionExtraction, OriginDisposition};
use crate::domain::format::{cap_path, format_count, format_size};
use crate::domain::root::Root;
use crate::domain::trail::{
    fate_transition, parse_when, DayRollup, DecisionFamily, FateAspect, FateLine, TimelineEvent,
    WhenValue,
};
use crate::ops;
use crate::ops::scope::ResolvedScope;
use crate::ops::trail::{ExtractionRollup, TrailParams, TrailResult, TrailView, DEFAULT_LIMIT};
use crate::repo::{self, Db};

pub struct TrailArgs {
    pub paths: Vec<PathBuf>,
    pub global: bool,
    pub today: bool,
    pub since: Option<String>,
    pub on: Option<String>,
    pub limit: Option<usize>,
    pub all: bool,
    pub no_notes: bool,
    pub jsonl: bool,
}

pub fn run(db: &mut Db, args: TrailArgs) -> Result<()> {
    let today = Local::now().date_naive();
    let (timeframe, time_label) = if args.today {
        (Some(WhenValue::Since(today)), Some("today".to_string()))
    } else if let Some(input) = &args.since {
        let date = parse_when(input, today).map_err(|e| anyhow!(e))?;
        (Some(WhenValue::Since(date)), Some(format!("since {input}")))
    } else if let Some(input) = &args.on {
        let date = parse_when(input, today).map_err(|e| anyhow!(e))?;
        (Some(WhenValue::On(date)), Some(format!("on {input}")))
    } else {
        (None, None)
    };

    let all_roots = repo::root::fetch_all(db.conn())?;
    let resolved = ops::scope::resolve_scope(db.conn(), &args.paths, args.global, &all_roots)?;

    let limit = if args.all {
        None
    } else {
        Some(args.limit.unwrap_or(DEFAULT_LIMIT))
    };
    let params = TrailParams {
        prefixes: resolved.prefixes.clone(),
        timeframe,
        include_notes: !args.no_notes,
        limit,
    };
    let result = ops::trail::compute_trail(db.conn(), &params)?;

    let roots_map: HashMap<i64, Root> = all_roots.into_iter().map(|r| (r.id, r)).collect();
    if args.jsonl {
        crate::scope::print_list_scope(&resolved);
        print_jsonl(&result)?;
    } else {
        print_human(&result, &resolved, time_label.as_deref(), &roots_map, limit);
    }
    Ok(())
}

pub fn run_show(db: &mut Db, id: i64) -> Result<()> {
    let Some(show) = ops::trail::compute_show(db.conn(), id)? else {
        return Err(anyhow!("no decision #{id}"));
    };
    let d = &show.decision;
    println!("Decision #{} — {}", d.id, d.command);
    println!("  when:     {}", format_datetime(d.created_at));
    println!("  status:   {}", d.status);
    println!("  counts:   {}", format_counts(d));
    if let Some(reason) = &d.reason {
        println!("  reason:   \"{reason}\"");
    }
    println!("  command:  {}", d.command_line);
    match &d.scope {
        Some(scope) if !scope.is_empty() => println!("  scope:    {}", scope.join(", ")),
        _ => println!("  scope:    global"),
    }
    if !show.extractions.is_empty() {
        println!("  drew from:");
        for extraction in &show.extractions {
            let row = &extraction.row;
            let location = row.drawn_from();
            let files = format_count(row.files);
            let unit = if row.files == 1 { "file" } else { "files" };
            // The snapshot path stays primary; a root the index no longer
            // knows must not read as a live, visitable location.
            let marker = if extraction.root_removed {
                " (root removed)"
            } else {
                ""
            };
            match row.bytes {
                Some(bytes) => println!(
                    "    {location} — {files} {unit} ({}){marker}",
                    format_size(bytes)
                ),
                None => println!("    {location} — {files} {unit}{marker}"),
            }
        }
    }
    println!("  version:  {}", d.canon_version);
    if let Some(summary) = &d.summary {
        println!("  summary:  {summary}");
    }
    if show.receipts.is_empty() {
        if let Some(absence) = &show.receipt_absence {
            println!("  receipt:  {absence}");
        }
    } else {
        println!("  receipts:");
        for receipt in &show.receipts {
            println!("    {}/{}", receipt.root_display, receipt.rel_path);
        }
    }
    Ok(())
}

// ============================================================================
// Human rendering
// ============================================================================

fn print_human(
    result: &TrailResult,
    resolved: &ResolvedScope,
    time_label: Option<&str>,
    roots: &HashMap<i64, Root>,
    limit: Option<usize>,
) {
    let scope_part = if resolved.is_global() {
        "all roots".to_string()
    } else {
        resolved.prefixes.join(", ")
    };
    match time_label {
        Some(label) => println!("Decision trail: {scope_part} — {label}"),
        None => println!("Decision trail: {scope_part}"),
    }

    // Scope column: one width across the whole listing, capped so long
    // paths can't push the narration off-screen. Measured over the cells
    // that will actually be printed — extraction lines render the drawn-from
    // location, not the selection scope.
    let width = |events: &[&TimelineEvent]| -> usize {
        events
            .iter()
            .flat_map(|e| event_cells(e, resolved, roots, &result.extractions))
            .map(|cell| cell.chars().count())
            .max()
            .unwrap_or(0)
            .min(SCOPE_CELL_MAX)
    };
    match &result.view {
        TrailView::Recent(events) => {
            if result.total_decisions == 0 {
                println!();
                if resolved.is_global() {
                    println!("No decisions recorded.");
                } else {
                    println!("No decisions recorded here.");
                }
            }
            if !events.is_empty() {
                println!();
                let refs: Vec<&TimelineEvent> = events.iter().collect();
                let w = width(&refs);
                for event in events {
                    print_event(event, true, resolved, roots, w, &result.extractions);
                }
            }
        }
        TrailView::Days(days) => {
            if days.is_empty() {
                println!();
                println!("No decisions {}.", time_label.unwrap_or("in range"));
            }
            let refs: Vec<&TimelineEvent> = days.iter().flat_map(|d| &d.events).collect();
            let w = width(&refs);
            for day in days {
                println!();
                let weekday = day.date.format("%A %Y-%m-%d");
                if day.rollup.is_empty() {
                    println!("{weekday}");
                } else {
                    println!("{weekday} — {}", format_rollup(&day.rollup));
                }
                println!();
                for event in &day.events {
                    print_event(event, false, resolved, roots, w, &result.extractions);
                }
            }
        }
    }

    // The extraction rollup is a scope-lens-only footer ("Archived from
    // here") — it never appears alongside the day-grouped time lens.
    let rollup_line = match (&result.view, &result.extraction_rollup) {
        (TrailView::Recent(_), Some(rollup)) => Some(format_extraction_rollup(rollup)),
        _ => None,
    };

    if rollup_line.is_some() || result.earlier_decisions > 0 || result.unscoped_decisions > 0 {
        println!();
    }
    if let Some(line) = rollup_line {
        println!("{line}");
    }
    if result.earlier_decisions > 0 {
        let cap = limit.unwrap_or(DEFAULT_LIMIT);
        println!(
            "{} earlier decisions not shown (--limit N or --all; showing {cap}).",
            format_count(result.earlier_decisions)
        );
    }
    if result.unscoped_decisions > 0 {
        println!(
            "{} global decisions not shown (--global).",
            format_count(result.unscoped_decisions)
        );
    }
}

/// Maximum width of the scope column (the coverage compact-label precedent).
const SCOPE_CELL_MAX: usize = 35;

/// One timeline line: id, time, scope column, narration. Decisions carry
/// counts and reason; notes carry the `~` voice marker and never an id,
/// counts, or status — a thought must not be mistakable for an action.
///
/// Dedup rule: a decision with touching extraction rows renders the
/// *extraction aspect* instead of its summary line — one line per touching
/// row (the common case is one; a multi-root draw into a multi-prefix view
/// repeats the id) — never both a selection line and an extraction line.
fn print_event(
    event: &TimelineEvent,
    with_date: bool,
    resolved: &ResolvedScope,
    roots: &HashMap<i64, Root>,
    width: usize,
    extractions: &HashMap<i64, Vec<DecisionExtraction>>,
) {
    match event {
        TimelineEvent::Decision(d) => {
            let time = if with_date {
                format_datetime(d.created_at)
            } else {
                format_time(d.created_at)
            };
            let suffix = |line: &mut String| {
                if let Some(reason) = &d.reason {
                    line.push_str(&format!(" \u{00b7} \"{reason}\""));
                }
                if d.status != "completed" {
                    line.push_str(&format!("  [{}]", d.status));
                }
            };
            let cells = event_cells(event, resolved, roots, extractions);
            if let Some(rows) = extractions.get(&d.id) {
                for (row, cell) in rows.iter().zip(&cells) {
                    let mut line = format!(
                        "#{:<4} {time}  {cell:<width$}  {}",
                        d.id,
                        extraction_narration(row)
                    );
                    suffix(&mut line);
                    println!("{line}");
                }
                return;
            }
            let cell = &cells[0];
            let mut line = format!("#{:<4} {time}  {cell:<width$}  {}", d.id, headline(d));
            suffix(&mut line);
            println!("{line}");
        }
        TimelineEvent::Note(n) => {
            let cell = cap_path(&scope_cell(event, resolved, roots), SCOPE_CELL_MAX);
            let time = if with_date {
                format_datetime(n.created_at)
            } else {
                format_time(n.created_at)
            };
            println!("      {time}  {cell:<width$}  ~ {}", n.text);
        }
    }
}

/// Every scope cell an event will actually render — one per extraction line
/// for a decision shown in the extraction aspect, otherwise exactly one.
///
/// The column width and the printed lines both derive from this, because an
/// extraction cell (the drawn-from location) is a different string from the
/// selection-scope cell: width computed from one and lines printed with the
/// other pushes the wider narration out of alignment.
fn event_cells(
    event: &TimelineEvent,
    resolved: &ResolvedScope,
    roots: &HashMap<i64, Root>,
    extractions: &HashMap<i64, Vec<DecisionExtraction>>,
) -> Vec<String> {
    if let TimelineEvent::Decision(d) = event {
        if let Some(rows) = extractions.get(&d.id) {
            return rows
                .iter()
                .map(|row| cap_path(&relativize(&row.drawn_from(), resolved), SCOPE_CELL_MAX))
                .collect();
        }
    }
    vec![cap_path(
        &scope_cell(event, resolved, roots),
        SCOPE_CELL_MAX,
    )]
}

/// The extraction aspect's narration: `→ N files (size) to DEST (wording)`.
/// Disposition wording goes through `OriginDisposition`, never a free
/// literal; `None` (pre-vocabulary backfilled rows) omits the parenthetical.
fn extraction_narration(row: &DecisionExtraction) -> String {
    let files = format_count(row.files);
    let unit = if row.files == 1 { "file" } else { "files" };
    let mut line = match row.bytes {
        Some(bytes) => format!(
            "\u{2192} {files} {unit} ({}) to {}",
            format_size(bytes),
            row.destination_path
        ),
        None => format!("\u{2192} {files} {unit} to {}", row.destination_path),
    };
    if let Some(disposition) = row.disposition {
        let wording = match disposition {
            OriginDisposition::Retained => "copied; originals remain",
            OriginDisposition::Relocated => "moved",
        };
        line.push_str(&format!(" ({wording})"));
    }
    line
}

/// The scope-lens-only "Archived from here" footer: whole-history rollup of
/// this view's extraction-touching rows.
fn format_extraction_rollup(rollup: &ExtractionRollup) -> String {
    let files = format_count(rollup.files);
    let unit = if rollup.files == 1 { "file" } else { "files" };
    let dest_unit = if rollup.destinations == 1 {
        "destination"
    } else {
        "destinations"
    };
    match rollup.bytes {
        Some(bytes) => format!(
            "Archived from here: {files} {unit} ({}) \u{2192} {} {dest_unit}.",
            format_size(bytes),
            rollup.destinations
        ),
        None => format!(
            "Archived from here: {files} {unit} \u{2192} {} {dest_unit}.",
            rollup.destinations
        ),
    }
}

/// The location an event happened at, rendered for the scope column: relative
/// to the viewed prefix when the view has one (the CWD case), otherwise the
/// absolute path. Decisions with no recorded scope render as "global";
/// multi-path scopes show the first plus a count.
fn scope_cell(
    event: &TimelineEvent,
    resolved: &ResolvedScope,
    roots: &HashMap<i64, Root>,
) -> String {
    let location = match event {
        TimelineEvent::Decision(d) => match &d.scope {
            Some(paths) if !paths.is_empty() => {
                let first = relativize(&paths[0], resolved);
                if paths.len() > 1 {
                    format!("{first} +{}", paths.len() - 1)
                } else {
                    first
                }
            }
            _ => "global".to_string(),
        },
        TimelineEvent::Note(n) => {
            let absolute = match roots.get(&n.root_id) {
                Some(root) if n.rel_path.is_empty() => root.path.clone(),
                Some(root) => format!("{}/{}", root.path, n.rel_path),
                None => n.rel_path.clone(),
            };
            relativize(&absolute, resolved)
        }
    };
    location
}

/// Render a location relative to a single-prefix view ("." for the prefix
/// itself); fall back to the path as recorded (global views, multi-prefix
/// views, ancestor scopes, historical relative records).
fn relativize(path: &str, resolved: &ResolvedScope) -> String {
    if resolved.prefixes.len() == 1 {
        let prefix = &resolved.prefixes[0];
        if path == prefix {
            return ".".to_string();
        }
        if let Some(rel) = crate::domain::path_strip_prefix(path, prefix) {
            return rel.to_string();
        }
    }
    path.to_string()
}

/// The per-line narration is the stored summary — composed once at execution
/// time from the same numbers as the count columns (one composition, two
/// uses). Started rows have no summary yet; fall back to the command name.
fn headline(d: &Decision) -> String {
    match &d.summary {
        Some(summary) => summary.lines().next().unwrap_or(summary).to_string(),
        None => d.command.clone(),
    }
}

fn format_rollup(rollup: &DayRollup) -> String {
    let mut parts = Vec::new();
    if rollup.deleted.files > 0 {
        parts.push(fate_part(
            fate_word(DecisionFamily::Observe, FateAspect::Absent),
            &rollup.deleted,
        ));
    }
    if rollup.archived.files > 0 {
        parts.push(fate_part(
            fate_word(DecisionFamily::Archive, FateAspect::Present),
            &rollup.archived,
        ));
    }
    if rollup.excluded.files > 0 {
        parts.push(fate_part(
            fate_word(DecisionFamily::Exclude, FateAspect::Present),
            &rollup.excluded,
        ));
    }
    let mut line = parts.join(", ");
    if rollup.other_actions > 0 {
        let n = rollup.other_actions;
        let actions = if n == 1 { "action" } else { "actions" };
        if line.is_empty() {
            line = format!("{n} other {actions}");
        } else {
            line.push_str(&format!(" — and {n} other {actions}"));
        }
    }
    line
}

/// The registered word for a rollup fate line. Each of the three lines maps to
/// a fixed (family, aspect) that `fate_transition` is proven to resolve (its
/// totality test); `expect` documents that invariant rather than guessing.
fn fate_word(family: DecisionFamily, aspect: FateAspect) -> &'static str {
    fate_transition(family, aspect)
        .expect("rollup fate line must map to a transition")
        .as_str()
}

fn fate_part(verb: &str, fate: &FateLine) -> String {
    let files = format_count(fate.files);
    let unit = if fate.files == 1 { "file" } else { "files" };
    match fate.bytes {
        Some(bytes) => format!("{verb} {files} {unit} ({})", format_size(bytes)),
        None => format!("{verb} {files} {unit}"),
    }
}

fn format_counts(d: &Decision) -> String {
    let fmt = |c: Option<i64>| c.map_or("-".to_string(), |n| format_count(n));
    format!(
        "attempted {}, completed {}, failed {}, skipped {}",
        fmt(d.count_attempted),
        fmt(d.count_completed),
        fmt(d.count_failed),
        fmt(d.count_skipped)
    )
}

fn format_datetime(ts: i64) -> String {
    match Local.timestamp_opt(ts, 0) {
        chrono::LocalResult::Single(dt) => dt.format("%Y-%m-%d %H:%M").to_string(),
        _ => "????-??-?? ??:??".to_string(),
    }
}

fn format_time(ts: i64) -> String {
    match Local.timestamp_opt(ts, 0) {
        chrono::LocalResult::Single(dt) => dt.format("%H:%M").to_string(),
        _ => "??:??".to_string(),
    }
}

// ============================================================================
// JSONL rendering
// ============================================================================

#[derive(Serialize)]
struct JsonDecisionEvent<'a> {
    r#type: &'static str,
    id: i64,
    command: &'a str,
    created_at: i64,
    status: &'a str,
    count_attempted: Option<i64>,
    count_completed: Option<i64>,
    count_failed: Option<i64>,
    count_skipped: Option<i64>,
    reason: Option<&'a str>,
    scope: Option<&'a [String]>,
    summary: Option<&'a str>,
    receipt_root_id: Option<i64>,
    receipt_rel_path: Option<&'a str>,
    /// Additive; absent (not `[]`) for a decision with no extraction rows.
    /// Always the *full* row set for the decision, independent of which
    /// view/lens surfaced it (JSONL is a machine-output completeness
    /// contract, not a scoped-touching one).
    #[serde(skip_serializing_if = "Option::is_none")]
    extractions: Option<Vec<JsonExtraction<'a>>>,
}

#[derive(Serialize)]
struct JsonExtraction<'a> {
    root: &'a str,
    rel_prefix: &'a str,
    files: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    bytes: Option<i64>,
    destination: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    disposition: Option<&'static str>,
}

#[derive(Serialize)]
struct JsonNoteEvent<'a> {
    r#type: &'static str,
    created_at: i64,
    root_id: i64,
    rel_path: &'a str,
    text: &'a str,
}

fn json_extractions(rows: Option<&[DecisionExtraction]>) -> Option<Vec<JsonExtraction<'_>>> {
    rows.map(|rows| {
        rows.iter()
            .map(|r| JsonExtraction {
                root: &r.root_path,
                rel_prefix: &r.rel_prefix,
                files: r.files,
                bytes: r.bytes,
                destination: &r.destination_path,
                disposition: r.disposition.map(|d| d.as_str()),
            })
            .collect()
    })
}

fn print_jsonl(result: &TrailResult) -> Result<()> {
    let stdout = io::stdout();
    let mut handle = stdout.lock();
    let events: Box<dyn Iterator<Item = &TimelineEvent>> = match &result.view {
        TrailView::Recent(events) => Box::new(events.iter()),
        TrailView::Days(days) => Box::new(days.iter().flat_map(|d| d.events.iter())),
    };
    for event in events {
        let json = match event {
            TimelineEvent::Decision(d) => serde_json::to_string(&JsonDecisionEvent {
                r#type: "decision",
                id: d.id,
                command: &d.command,
                created_at: d.created_at,
                status: &d.status,
                count_attempted: d.count_attempted,
                count_completed: d.count_completed,
                count_failed: d.count_failed,
                count_skipped: d.count_skipped,
                reason: d.reason.as_deref(),
                scope: d.scope.as_deref(),
                summary: d.summary.as_deref(),
                receipt_root_id: d.receipt_root_id,
                receipt_rel_path: d.receipt_rel_path.as_deref(),
                extractions: json_extractions(result.extractions_all.get(&d.id).map(Vec::as_slice)),
            })?,
            TimelineEvent::Note(n) => serde_json::to_string(&JsonNoteEvent {
                r#type: "note",
                created_at: n.created_at,
                root_id: n.root_id,
                rel_path: &n.rel_path,
                text: &n.text,
            })?,
        };
        writeln!(handle, "{json}")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::note::Note;

    fn mk_note(text: &str) -> Note {
        Note {
            id: 1,
            root_id: 2,
            rel_path: "a/b".to_string(),
            text: text.to_string(),
            created_at: 1000,
        }
    }

    #[test]
    fn jsonl_note_event_shape() {
        let note = mk_note("check the RAW files");
        let json = serde_json::to_string(&JsonNoteEvent {
            r#type: "note",
            created_at: note.created_at,
            root_id: note.root_id,
            rel_path: &note.rel_path,
            text: &note.text,
        })
        .unwrap();
        assert_eq!(
            json,
            r#"{"type":"note","created_at":1000,"root_id":2,"rel_path":"a/b","text":"check the RAW files"}"#
        );
    }

    #[test]
    fn jsonl_decision_event_shape() {
        let json = serde_json::to_string(&JsonDecisionEvent {
            r#type: "decision",
            id: 61,
            command: "exclude_duplicates",
            created_at: 1000,
            status: "completed",
            count_attempted: Some(315),
            count_completed: Some(210),
            count_failed: None,
            count_skipped: Some(105),
            reason: Some("redundant backup"),
            scope: None,
            summary: Some("Excluded 210 duplicates"),
            receipt_root_id: Some(3),
            receipt_rel_path: Some(".canon-ledger/000061-exclude_duplicates.toml"),
            extractions: None,
        })
        .unwrap();
        assert!(json.starts_with(r#"{"type":"decision","id":61,"command":"exclude_duplicates""#));
        assert!(json.contains(r#""reason":"redundant backup""#));
        assert!(!json.contains("extractions"));
    }

    #[test]
    fn jsonl_decision_event_extractions_present_when_some() {
        let row = crate::domain::extraction::DecisionExtraction {
            decision_id: 61,
            root_id: 1,
            root_path: "/vol/photos".to_string(),
            rel_prefix: "2016/italy".to_string(),
            files: 47,
            bytes: Some(3_900_000),
            destination_root_id: Some(9),
            destination_path: "/archive/2016/Italy".to_string(),
            disposition: Some(OriginDisposition::Retained),
        };
        let json = serde_json::to_string(&JsonDecisionEvent {
            r#type: "decision",
            id: 61,
            command: "apply",
            created_at: 1000,
            status: "completed",
            count_attempted: None,
            count_completed: None,
            count_failed: None,
            count_skipped: None,
            reason: None,
            scope: None,
            summary: None,
            receipt_root_id: None,
            receipt_rel_path: None,
            extractions: json_extractions(Some(&[row])),
        })
        .unwrap();
        assert!(json.contains(r#""extractions":[{"root":"/vol/photos""#));
        assert!(json.contains(r#""rel_prefix":"2016/italy""#));
        assert!(json.contains(r#""files":47"#));
        assert!(json.contains(r#""bytes":3900000"#));
        assert!(json.contains(r#""destination":"/archive/2016/Italy""#));
        assert!(json.contains(r#""disposition":"retained""#));
    }

    #[test]
    fn rollup_line_composition() {
        let rollup = DayRollup {
            deleted: FateLine {
                files: 1350,
                bytes: Some(35_000_000_000),
            },
            archived: FateLine {
                files: 47,
                bytes: Some(3_900_000_000),
            },
            excluded: FateLine {
                files: 210,
                bytes: None,
            },
            other_actions: 2,
        };
        assert_eq!(
            format_rollup(&rollup),
            "deleted 1,350 files (35.0 GB), archived 47 files (3.9 GB), excluded 210 files — and 2 other actions"
        );
    }

    #[test]
    fn relativize_against_single_prefix() {
        let scoped = ResolvedScope {
            prefixes: vec!["/photos".to_string()],
            from_cwd: true,
            auto_include_archived: false,
        };
        assert_eq!(relativize("/photos", &scoped), ".");
        assert_eq!(relativize("/photos/italy", &scoped), "italy");
        // Ancestor of the view and unrelated paths stay absolute.
        assert_eq!(relativize("/", &scoped), "/");
        assert_eq!(relativize("/other", &scoped), "/other");

        let global = ResolvedScope {
            prefixes: Vec::new(),
            from_cwd: false,
            auto_include_archived: false,
        };
        assert_eq!(relativize("/photos/italy", &global), "/photos/italy");
    }

    #[test]
    fn rollup_line_only_other_actions() {
        let rollup = DayRollup {
            deleted: FateLine {
                files: 0,
                bytes: None,
            },
            archived: FateLine {
                files: 0,
                bytes: None,
            },
            excluded: FateLine {
                files: 0,
                bytes: None,
            },
            other_actions: 1,
        };
        assert_eq!(format_rollup(&rollup), "1 other action");
    }

    // ------------------------------------------------------------------
    // Extraction aspect line + rollup footer composition
    // ------------------------------------------------------------------

    fn mk_extraction_row(
        bytes: Option<i64>,
        disposition: Option<OriginDisposition>,
    ) -> DecisionExtraction {
        DecisionExtraction {
            decision_id: 42,
            root_id: 1,
            root_path: "/Volumes/old-laptop".to_string(),
            rel_prefix: "photos/2016/italy".to_string(),
            files: 47,
            bytes,
            destination_root_id: Some(9),
            destination_path: "/Archive/Media/2016/Italy".to_string(),
            disposition,
        }
    }

    fn mk_decision(id: i64, scope: Option<Vec<String>>) -> Decision {
        Decision {
            id,
            command: "apply".to_string(),
            scope,
            command_line: "canon apply m.lock".to_string(),
            reason: None,
            status: "completed".to_string(),
            count_attempted: None,
            count_completed: None,
            count_failed: None,
            count_skipped: None,
            summary: Some("Archived 47 files".to_string()),
            canon_version: "0.1.0".to_string(),
            created_at: 0,
            receipt_root_id: None,
            receipt_rel_path: None,
        }
    }

    #[test]
    fn extraction_narration_retained_wording() {
        let row = mk_extraction_row(Some(3_900_000_000), Some(OriginDisposition::Retained));
        assert_eq!(
            extraction_narration(&row),
            "\u{2192} 47 files (3.9 GB) to /Archive/Media/2016/Italy (copied; originals remain)"
        );
    }

    #[test]
    fn extraction_narration_relocated_wording() {
        let row = mk_extraction_row(Some(1_000), Some(OriginDisposition::Relocated));
        assert_eq!(
            extraction_narration(&row),
            "\u{2192} 47 files (1.0 KB) to /Archive/Media/2016/Italy (moved)"
        );
    }

    #[test]
    fn extraction_narration_bytes_none_omits_size() {
        let row = mk_extraction_row(None, Some(OriginDisposition::Retained));
        assert_eq!(
            extraction_narration(&row),
            "\u{2192} 47 files to /Archive/Media/2016/Italy (copied; originals remain)"
        );
    }

    #[test]
    fn extraction_narration_disposition_none_omits_parenthetical() {
        // Pre-vocabulary backfilled rows: rendered neutrally, never guessed.
        let row = mk_extraction_row(Some(100), None);
        assert_eq!(
            extraction_narration(&row),
            "\u{2192} 47 files (100 B) to /Archive/Media/2016/Italy"
        );
    }

    #[test]
    fn extraction_narration_singular_file() {
        let mut row = mk_extraction_row(Some(10), Some(OriginDisposition::Retained));
        row.files = 1;
        assert_eq!(
            extraction_narration(&row),
            "\u{2192} 1 file (10 B) to /Archive/Media/2016/Italy (copied; originals remain)"
        );
    }

    #[test]
    fn event_cells_measures_the_drawn_from_location_for_extraction_lines() {
        // The column width is measured over these cells, so an extraction
        // line's drawn-from location — not the decision's selection scope —
        // must be what comes back, or the narration falls out of alignment.
        let global = ResolvedScope {
            prefixes: Vec::new(),
            from_cwd: false,
            auto_include_archived: false,
        };
        let event = TimelineEvent::Decision(mk_decision(1, Some(vec!["/short".to_string()])));

        let mut extractions = HashMap::new();
        extractions.insert(1, vec![mk_extraction_row(None, None)]);
        // Capped at SCOPE_CELL_MAX like every other cell.
        let cells = event_cells(&event, &global, &HashMap::new(), &extractions);
        assert_eq!(cells, vec!["...mes/old-laptop/photos/2016/italy"]);
        assert!(cells[0].chars().count() <= SCOPE_CELL_MAX);

        // With no extraction rows the selection scope is the cell, as before.
        let cells = event_cells(&event, &global, &HashMap::new(), &HashMap::new());
        assert_eq!(cells, vec!["/short"]);
    }

    #[test]
    fn event_cells_returns_one_cell_per_extraction_row() {
        let global = ResolvedScope {
            prefixes: Vec::new(),
            from_cwd: false,
            auto_include_archived: false,
        };
        let event = TimelineEvent::Decision(mk_decision(1, None));
        let mut second = mk_extraction_row(None, None);
        second.root_path = "/Volumes/nikon-sd".to_string();
        second.rel_prefix = "dcim".to_string();
        let mut extractions = HashMap::new();
        extractions.insert(1, vec![mk_extraction_row(None, None), second]);

        let cells = event_cells(&event, &global, &HashMap::new(), &extractions);
        assert_eq!(
            cells,
            vec![
                "...mes/old-laptop/photos/2016/italy",
                "/Volumes/nikon-sd/dcim"
            ]
        );
    }

    #[test]
    fn extraction_rollup_footer_composition() {
        let rollup = ExtractionRollup {
            files: 1_251,
            bytes: Some(22_100_000_000),
            destinations: 2,
        };
        assert_eq!(
            format_extraction_rollup(&rollup),
            "Archived from here: 1,251 files (22.1 GB) \u{2192} 2 destinations."
        );
    }

    #[test]
    fn extraction_rollup_footer_singular_destination_and_omitted_bytes() {
        let rollup = ExtractionRollup {
            files: 1,
            bytes: None,
            destinations: 1,
        };
        assert_eq!(
            format_extraction_rollup(&rollup),
            "Archived from here: 1 file \u{2192} 1 destination."
        );
    }
}
