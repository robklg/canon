//! Trail's human-rendering stratum: composes the scope lens, the time lens,
//! and the composition card into the text `canon trail`/`trail show` print.

use std::collections::HashMap;

use chrono::{Local, TimeZone};

use crate::core::domain::extraction::{DecisionExtraction, OriginDisposition};
use crate::core::domain::fate::{fate_transition, DecisionFamily, FateAspect};
use crate::domain::decision::Decision;
use crate::domain::format::{cap_path, format_count, format_size};
use crate::domain::root::Root;
use crate::ops::scope::ResolvedScope;
use crate::trail::domain::composition::{CompositionCard, OriginLine, TransitionedLine};
use crate::trail::domain::placement::{aggregate_placement_lines, RowAspect};
use crate::trail::domain::timeline::{DayRollup, FateLine, TimelineEvent};
use crate::trail::ops::compute::{
    ArrivalRollup, ExtractionRollup, RearrangementRollup, DEFAULT_LIMIT,
};
use crate::trail::ops::show::ShowExtraction;
use crate::trail::{TrailResult, TrailView};

pub(super) fn print_human(
    result: &TrailResult,
    resolved: &ResolvedScope,
    time_label: Option<&str>,
    roots: &HashMap<i64, Root>,
    limit: Option<usize>,
    card: Option<&CompositionCard>,
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
    // location, arrival lines the destination, neither the selection scope.
    let width = |events: &[&TimelineEvent]| -> usize {
        events
            .iter()
            .flat_map(|e| event_cells(e, resolved, roots, &result.placements))
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
                    print_event(event, true, resolved, roots, w, &result.placements);
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
                    print_event(event, false, resolved, roots, w, &result.placements);
                }
            }
        }
    }

    // All three rollups are scope-lens-only footers — none appears alongside
    // the day-grouped time lens. Independent of each other: a view can draw
    // content out, receive content in, rearrange content within itself, any
    // combination, or none. Crossings are stated first (what this place traded
    // with the rest of the universe), then what merely moved inside it.
    let rollup_lines: Vec<String> = match &result.view {
        TrailView::Recent(_) => [
            result
                .extraction_rollup
                .as_ref()
                .map(format_extraction_rollup),
            result.arrival_rollup.as_ref().map(format_arrival_rollup),
            result
                .rearrangement_rollup
                .as_ref()
                .map(format_rearrangement_rollup),
        ]
        .into_iter()
        .flatten()
        .collect(),
        TrailView::Days(_) => Vec::new(),
    };

    if !rollup_lines.is_empty() || result.earlier_decisions > 0 || result.unscoped_decisions > 0 {
        println!();
    }
    for line in rollup_lines {
        println!("{line}");
    }
    if result.earlier_decisions > 0 {
        let cap = limit.unwrap_or(DEFAULT_LIMIT);
        println!(
            "{} earlier {} not shown (--limit N or --all; showing {cap}).",
            format_count(result.earlier_decisions),
            plural(result.earlier_decisions as i64, "decision")
        );
    }
    if result.unscoped_decisions > 0 {
        println!(
            "{} global {} not shown (--global).",
            format_count(result.unscoped_decisions),
            plural(result.unscoped_decisions, "decision")
        );
    }

    // The composition card: state, not events — "Arrived here" and "Standing
    // here" can honestly diverge (content arrived, then some of it was later
    // deleted or moved on). Scope-lens only, same as both rollups above.
    if let (TrailView::Recent(_), Some(card)) = (&result.view, card) {
        print_composition_card(card);
    }
}

/// Maximum number of origin lines the composition card shows before an
/// explicit remainder line — the origins section is the one that scales with
/// a location's history (many source drives feeding one archive folder over
/// years); transitioned/indexed-here/untracked stay small in practice.
const CARD_ORIGIN_CAP: usize = 10;

/// Maximum origin directories a `drew from:` group lists before an explicit
/// remainder line — a manifest pattern can fan one apply across many
/// folders, and the per-root summary line already carries the totals.
const DREW_FROM_DIR_CAP: usize = 5;

/// The `drew from:` block's lines, indentation included: one summary line
/// per origin root, then that root's origin directories when the draw
/// fanned out — capped with an explicit remainder, never a silent
/// truncation. Pure data — testable without capturing stdout.
pub(super) fn drew_from_lines(extractions: &[ShowExtraction]) -> Vec<String> {
    let mut out = Vec::new();
    for group in extractions {
        // The snapshot path stays primary; a root the index no longer knows
        // must not read as a live, visitable location. A retired origin
        // points at its book — bound history, not a dead end.
        let marker = match (&group.retired_book, group.root_removed) {
            (Some(book), _) => format!(" (root retired — the book: {book})"),
            (None, true) => ROOT_REMOVED_MARKER.to_string(),
            (None, false) => String::new(),
        };
        out.push(format!(
            "    {} — {}{marker}",
            group.location,
            files_with_size(group.files, group.bytes)
        ));
        for dir in group.directories.iter().take(DREW_FROM_DIR_CAP) {
            let shown = if dir.dir.is_empty() { "." } else { &dir.dir };
            out.push(format!(
                "      {shown} — {}",
                files_with_size(dir.files, dir.bytes)
            ));
        }
        let more = group.directories.len().saturating_sub(DREW_FROM_DIR_CAP);
        if more > 0 {
            let noun = if more == 1 {
                "directory"
            } else {
                "directories"
            };
            out.push(format!(
                "      \u{2026} and {} more {noun}",
                format_count(more)
            ));
        }
    }
    out
}

fn print_composition_card(card: &CompositionCard) {
    println!();
    println!("Standing here: {}", format_bucket(card.files, card.bytes));
    for line in composition_card_lines(card) {
        println!("  {line}");
    }
}

/// The card's body lines below the "Standing here" header: origins
/// (files desc, capped with an explicit remainder), transitioned,
/// indexed-here, untracked. Pure data — kept separate from
/// `print_composition_card` so the cap and ordering are testable without
/// capturing stdout.
fn composition_card_lines(card: &CompositionCard) -> Vec<String> {
    let mut lines = Vec::new();
    for line in card.origins.iter().take(CARD_ORIGIN_CAP) {
        lines.push(format_origin_line(line));
    }
    if card.origins.len() > CARD_ORIGIN_CAP {
        let more = card.origins.len() - CARD_ORIGIN_CAP;
        lines.push(format!(
            "\u{2026} and {} more {}.",
            format_count(more as i64),
            plural(more as i64, "origin")
        ));
    }
    for line in &card.transitioned {
        lines.push(format_transitioned_line(line));
    }
    if let Some(bucket) = &card.indexed_here {
        lines.push(format!(
            "first indexed here: {}",
            format_bucket(bucket.files, bucket.bytes)
        ));
    }
    if let Some(bucket) = &card.untracked {
        lines.push(format!(
            "untracked (predates recording): {}",
            format_bucket(bucket.files, bucket.bytes)
        ));
    }
    lines
}

/// `thing` / `things` — the one place the trail pluralizes a noun. Separate
/// from [`count_of`] because not every sentence puts the two adjacent (the
/// card's remainder line reads "and 2 more origins").
fn plural(n: i64, singular: &str) -> String {
    if n == 1 {
        singular.to_string()
    } else {
        format!("{singular}s")
    }
}

/// `N thing` / `N things`, thousands-separated.
fn count_of(n: i64, singular: &str) -> String {
    format!("{} {}", format_count(n), plural(n, singular))
}

/// `N files (size)`, or `N files` when the size isn't known.
///
/// The trail's recurring shape: every count that can carry a size — timeline
/// narrations, all three rollups, day-rollup fate parts, `trail show`'s
/// `drew from:` lines — renders through this, so "never guess a size" is
/// enforced by one signature rather than by repeated `match row.bytes`.
fn files_with_size(files: i64, bytes: Option<i64>) -> String {
    match bytes {
        Some(bytes) => format!("{} ({})", count_of(files, "file"), format_size(bytes)),
        None => count_of(files, "file"),
    }
}

/// [`files_with_size`] where the size is always known (the card's buckets).
fn format_bucket(files: i64, bytes: i64) -> String {
    files_with_size(files, Some(bytes))
}

/// The disposition parenthetical, or nothing when the row can't say.
///
/// The wording differs by direction (outbound `moved` vs inbound `moved in`)
/// and is registered vocabulary, so each caller passes its own pair rather
/// than the words being derived here — but the "append in parens, or omit
/// entirely" mechanics are shared.
fn disposition_suffix(
    disposition: Option<OriginDisposition>,
    retained: &str,
    relocated: &str,
) -> String {
    match disposition {
        Some(OriginDisposition::Retained) => format!(" ({retained})"),
        Some(OriginDisposition::Relocated) => format!(" ({relocated})"),
        // Pre-vocabulary backfilled rows: omit rather than guess.
        None => String::new(),
    }
}

/// One origin line: `from <root>` (single-origin, possibly several merged
/// decisions) or `via apply #N from M origins` (one multi-origin decision) —
/// the registered wording, never a free literal.
///
/// An origin root that *contains* the view reads `from elsewhere in <root>`:
/// the content genuinely arrived (its origin sits outside the viewed scope),
/// but a bare `from /archive` while standing in `/archive/2020` would read as
/// naming the place you're already in. The root is still named rather than
/// left implicit ("in this root"), because a view can span several roots.
fn format_origin_line(line: &OriginLine) -> String {
    match line {
        OriginLine::FromRoot {
            root_path,
            root_removed,
            from_within,
            files,
            bytes,
            decision_ids,
            first_at,
            last_at,
        } => {
            let marker = if *root_removed {
                ROOT_REMOVED_MARKER
            } else {
                ""
            };
            let source = if *from_within {
                format!("from elsewhere in {root_path}{marker}")
            } else {
                format!("from {root_path}{marker}")
            };
            let ids: Vec<String> = decision_ids.iter().map(|id| format!("#{id}")).collect();
            format!(
                "{source}: {} \u{00b7} {} \u{00b7} {}",
                format_bucket(*files, *bytes),
                ids.join(", "),
                format_date_range(*first_at, *last_at)
            )
        }
        OriginLine::MultiOrigin {
            decision_id,
            origin_count,
            files,
            bytes,
            at,
        } => {
            format!(
                "via apply #{decision_id} from {} origins: {} \u{00b7} {}",
                format_count(*origin_count),
                format_bucket(*files, *bytes),
                format_date_only(*at)
            )
        }
    }
}

/// A transitioned line: `<label> here (#N)` — `label` is the registered
/// transition word when one applies, else the raw command (self-explaining,
/// never guessed).
fn format_transitioned_line(line: &TransitionedLine) -> String {
    format!(
        "{} here (#{}): {}",
        line.label,
        line.decision_id,
        format_bucket(line.files, line.bytes)
    )
}

/// A single date, or a `first – last` range when the two differ (several
/// contributing decisions).
fn format_date_range(first_at: i64, last_at: i64) -> String {
    if first_at == last_at {
        format_date_only(first_at)
    } else {
        format!(
            "{} \u{2013} {}",
            format_date_only(first_at),
            format_date_only(last_at)
        )
    }
}

pub(super) fn format_date_only(ts: i64) -> String {
    match Local.timestamp_opt(ts, 0) {
        chrono::LocalResult::Single(dt) => dt.format("%Y-%m-%d").to_string(),
        _ => "????-??-??".to_string(),
    }
}

/// Maximum width of the scope column (the coverage compact-label precedent).
const SCOPE_CELL_MAX: usize = 35;

/// One timeline line: id, time, scope column, narration. Decisions carry
/// counts and reason; notes carry the `~` voice marker and never an id,
/// counts, or status — a thought must not be mistakable for an action.
///
/// Aspect rendering, applied per row from the ops-classified tagged map:
/// extraction-aspect rows render the outbound line, arrival-aspect rows the
/// inbound line (`←`), and a rearrangement row (both endpoints inside the
/// view) renders once as the extraction-aspect line with its destination
/// shown view-relative rather than absolute. Never both a selection line and
/// an extraction/arrival line for the same decision.
fn print_event(
    event: &TimelineEvent,
    with_date: bool,
    resolved: &ResolvedScope,
    roots: &HashMap<i64, Root>,
    width: usize,
    placements: &HashMap<i64, Vec<(DecisionExtraction, RowAspect)>>,
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
            let cells = event_cells(event, resolved, roots, placements);
            let lines = placements
                .get(&d.id)
                .map(|rows| aggregate_placement_lines(rows))
                .unwrap_or_default();
            if !lines.is_empty() {
                for (placement, cell) in lines.iter().zip(&cells) {
                    let row = &placement.row;
                    let narration = match placement.aspect {
                        RowAspect::Rearrangement => extraction_narration_with_destination(
                            row,
                            &relativize(&row.destination_path, resolved),
                        ),
                        RowAspect::Arrival => arrival_narration(row, roots),
                        // `Outside` cannot reach here — such rows were dropped
                        // at classification. Rendering it as a plain extraction
                        // with its absolute destination degrades gracefully
                        // rather than panicking on a line the interface has no
                        // way to repair.
                        RowAspect::Extraction | RowAspect::Outside => extraction_narration(row),
                    };
                    let mut line = format!("#{:<4} {time}  {cell:<width$}  {}", d.id, narration);
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

/// Every scope cell an event will actually render — one per touching
/// extraction or arrival row, otherwise exactly one.
///
/// The column width and the printed lines both derive from this, because an
/// extraction cell (the drawn-from location) or an arrival cell (the
/// destination) is a different string from the selection-scope cell: width
/// computed from one and lines printed with the other pushes the wider
/// narration out of alignment. Both passes read the same ops-classified
/// tagged rows, so the measured cells and the printed lines cannot diverge
/// in count or order.
fn event_cells(
    event: &TimelineEvent,
    resolved: &ResolvedScope,
    roots: &HashMap<i64, Root>,
    placements: &HashMap<i64, Vec<(DecisionExtraction, RowAspect)>>,
) -> Vec<String> {
    if let TimelineEvent::Decision(d) = event {
        if let Some(rows) = placements.get(&d.id) {
            let lines = aggregate_placement_lines(rows);
            if !lines.is_empty() {
                return lines
                    .iter()
                    .map(|placement| {
                        let location = match placement.aspect {
                            RowAspect::Arrival => placement.row.destination_path.clone(),
                            RowAspect::Extraction
                            | RowAspect::Rearrangement
                            | RowAspect::Outside => placement.row.drawn_from(),
                        };
                        cap_path(&relativize(&location, resolved), SCOPE_CELL_MAX)
                    })
                    .collect();
            }
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
    extraction_narration_with_destination(row, &row.destination_path)
}

/// `extraction_narration` with an overridden destination display — the
/// intra-view relocation case renders the same shape with a view-relative
/// destination instead of the row's absolute snapshot path.
fn extraction_narration_with_destination(row: &DecisionExtraction, destination: &str) -> String {
    format!(
        "\u{2192} {} to {destination}{}",
        files_with_size(row.files, row.bytes),
        disposition_suffix(row.disposition, "copied; originals remain", "moved")
    )
}

/// The arrival aspect's narration: `← N files (size) from ORIGIN (wording)`.
/// The mirror of `extraction_narration` for the inbound direction — "in"
/// wording (`moved in` / `copied in; originals remain`) rather than the
/// outbound's, and the origin carries the removed-root marker when its
/// source root is no longer known to the live index.
fn arrival_narration(row: &DecisionExtraction, roots: &HashMap<i64, Root>) -> String {
    format!(
        "\u{2190} {} from {}{}",
        files_with_size(row.files, row.bytes),
        origin_location(row, roots),
        disposition_suffix(row.disposition, "copied in; originals remain", "moved in")
    )
}

/// The established removed-root marker (`trail show`'s `drew from:` lines
/// use the same text) — a root the live index no longer knows must not read
/// as a visitable location.
const ROOT_REMOVED_MARKER: &str = " (root removed)";

/// An arrival row's drawn-from location, with the removed-root marker
/// appended when the origin's source root is gone from the live index.
///
/// Live-ness is decided by `DecisionExtraction::origin_root_removed` — the
/// same rule `trail show` and the composition card use, matched on the
/// snapshot path rather than `root_id` so a removed-and-re-added root doesn't
/// draw a spurious marker.
fn origin_location(row: &DecisionExtraction, roots: &HashMap<i64, Root>) -> String {
    let mut location = row.drawn_from();
    if row.origin_root_removed(roots.values().map(|r| r.path.as_str())) {
        location.push_str(ROOT_REMOVED_MARKER);
    }
    location
}

/// The scope-lens-only "Archived from here" footer: whole-history rollup of
/// this view's extraction-touching rows.
fn format_extraction_rollup(rollup: &ExtractionRollup) -> String {
    format!(
        "Archived from here: {} \u{2192} {}.",
        files_with_size(rollup.files, rollup.bytes),
        count_of(rollup.destinations as i64, "destination")
    )
}

/// The scope-lens-only "Arrived here" footer: whole-history rollup of this
/// view's arrival-touching rows — the mirror of `format_extraction_rollup`
/// for the inbound direction.
fn format_arrival_rollup(rollup: &ArrivalRollup) -> String {
    format!(
        "Arrived here: {} from {}.",
        files_with_size(rollup.files, rollup.bytes),
        count_of(rollup.origins as i64, "origin")
    )
}

/// The scope-lens-only "Rearranged here" footer: whole-history rollup of the
/// rows that crossed no boundary — both endpoints inside this view.
///
/// No counterparty clause, unlike its two siblings: content that left went
/// *somewhere* and content that arrived came *from* somewhere, but content
/// that was rearranged stayed here, and naming this place as its own
/// counterparty would say nothing.
fn format_rearrangement_rollup(rollup: &RearrangementRollup) -> String {
    format!(
        "Rearranged here: {}.",
        files_with_size(rollup.files, rollup.bytes)
    )
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
    format!("{verb} {}", files_with_size(fate.files, fate.bytes))
}

pub(super) fn format_counts(d: &Decision) -> String {
    let fmt = |c: Option<i64>| c.map_or("-".to_string(), format_count);
    format!(
        "attempted {}, completed {}, failed {}, skipped {}",
        fmt(d.count_attempted),
        fmt(d.count_completed),
        fmt(d.count_failed),
        fmt(d.count_skipped)
    )
}

pub(super) fn format_datetime(ts: i64) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::decision::Decision;
    use crate::trail::domain::timeline::TimelineEvent;

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

    fn mk_root(id: i64, path: &str) -> Root {
        Root {
            id,
            path: path.to_string(),
            role: "archive".to_string(),
            comment: None,
            last_scanned_at: None,
            suspended: false,
        }
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
    fn extraction_narration_with_destination_overrides_display_only() {
        // Intra-view relocation: same shape as extraction_narration, but the
        // destination text is whatever the caller passes (a view-relative
        // path), not the row's absolute snapshot.
        let row = mk_extraction_row(Some(1_000), Some(OriginDisposition::Retained));
        assert_eq!(
            extraction_narration_with_destination(&row, "."),
            "\u{2192} 47 files (1.0 KB) to . (copied; originals remain)"
        );
    }

    // ------------------------------------------------------------------
    // Arrival aspect narration + removed-root marker
    // ------------------------------------------------------------------

    #[test]
    fn arrival_narration_copied_in_wording() {
        let row = mk_extraction_row(Some(3_900_000_000), Some(OriginDisposition::Retained));
        let roots = HashMap::from([(1, mk_root(1, "/Volumes/old-laptop"))]);
        assert_eq!(
            arrival_narration(&row, &roots),
            "\u{2190} 47 files (3.9 GB) from /Volumes/old-laptop/photos/2016/italy (copied in; originals remain)"
        );
    }

    #[test]
    fn arrival_narration_moved_in_wording() {
        let row = mk_extraction_row(Some(1_000), Some(OriginDisposition::Relocated));
        let roots = HashMap::from([(1, mk_root(1, "/Volumes/old-laptop"))]);
        assert_eq!(
            arrival_narration(&row, &roots),
            "\u{2190} 47 files (1.0 KB) from /Volumes/old-laptop/photos/2016/italy (moved in)"
        );
    }

    #[test]
    fn arrival_narration_bytes_none_omits_size() {
        let row = mk_extraction_row(None, Some(OriginDisposition::Retained));
        let roots = HashMap::from([(1, mk_root(1, "/Volumes/old-laptop"))]);
        assert_eq!(
            arrival_narration(&row, &roots),
            "\u{2190} 47 files from /Volumes/old-laptop/photos/2016/italy (copied in; originals remain)"
        );
    }

    #[test]
    fn arrival_narration_disposition_none_omits_parenthetical() {
        // Pre-vocabulary backfilled rows: rendered neutrally, never guessed.
        let row = mk_extraction_row(Some(100), None);
        let roots = HashMap::from([(1, mk_root(1, "/Volumes/old-laptop"))]);
        assert_eq!(
            arrival_narration(&row, &roots),
            "\u{2190} 47 files (100 B) from /Volumes/old-laptop/photos/2016/italy"
        );
    }

    #[test]
    fn arrival_narration_singular_file() {
        let mut row = mk_extraction_row(Some(10), Some(OriginDisposition::Retained));
        row.files = 1;
        let roots = HashMap::from([(1, mk_root(1, "/Volumes/old-laptop"))]);
        assert_eq!(
            arrival_narration(&row, &roots),
            "\u{2190} 1 file (10 B) from /Volumes/old-laptop/photos/2016/italy (copied in; originals remain)"
        );
    }

    #[test]
    fn arrival_narration_marks_removed_origin_root() {
        let row = mk_extraction_row(Some(10), Some(OriginDisposition::Retained));
        // The origin's source root is absent from the live roots map.
        let roots: HashMap<i64, Root> = HashMap::new();
        assert_eq!(
            arrival_narration(&row, &roots),
            "\u{2190} 47 files (10 B) from /Volumes/old-laptop/photos/2016/italy (root removed) (copied in; originals remain)"
        );
    }

    #[test]
    fn arrival_narration_no_marker_when_origin_root_present() {
        let row = mk_extraction_row(Some(10), Some(OriginDisposition::Retained));
        let roots = HashMap::from([(1, mk_root(1, "/Volumes/old-laptop"))]);
        assert!(!arrival_narration(&row, &roots).contains(ROOT_REMOVED_MARKER));
    }

    #[test]
    fn arrival_narration_no_marker_when_origin_root_was_re_added() {
        // The row's snapshot id predates a remove-and-re-add, so the live
        // map holds the same location under a different id. Matching on ids
        // would call a drive that's plugged in "removed".
        let row = mk_extraction_row(Some(10), Some(OriginDisposition::Retained));
        assert_eq!(row.root_id, 1);
        let roots = HashMap::from([(77, mk_root(77, "/Volumes/old-laptop"))]);
        assert!(!arrival_narration(&row, &roots).contains(ROOT_REMOVED_MARKER));
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
        let event =
            TimelineEvent::Decision(Box::new(mk_decision(1, Some(vec!["/short".to_string()]))));

        let mut placements = HashMap::new();
        placements.insert(
            1,
            vec![(mk_extraction_row(None, None), RowAspect::Extraction)],
        );
        // Capped at SCOPE_CELL_MAX like every other cell.
        let cells = event_cells(&event, &global, &HashMap::new(), &placements);
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
        let event = TimelineEvent::Decision(Box::new(mk_decision(1, None)));
        let mut second = mk_extraction_row(None, None);
        second.root_path = "/Volumes/nikon-sd".to_string();
        second.rel_prefix = "dcim".to_string();
        let mut placements = HashMap::new();
        placements.insert(
            1,
            vec![
                (mk_extraction_row(None, None), RowAspect::Extraction),
                (second, RowAspect::Extraction),
            ],
        );

        let cells = event_cells(&event, &global, &HashMap::new(), &placements);
        assert_eq!(
            cells,
            vec![
                "...mes/old-laptop/photos/2016/italy",
                "/Volumes/nikon-sd/dcim"
            ]
        );
    }

    #[test]
    fn event_cells_returns_view_relative_destination_for_arrival_only_row() {
        // Arrival-only: the cell is the destination, not the drawn-from
        // location — the mirror of the extraction-aspect cell.
        let scoped = ResolvedScope {
            prefixes: vec!["/Archive/Media".to_string()],
            from_cwd: true,
            auto_include_archived: false,
        };
        let event = TimelineEvent::Decision(Box::new(mk_decision(1, None)));
        let mut placements = HashMap::new();
        placements.insert(1, vec![(mk_extraction_row(None, None), RowAspect::Arrival)]);

        let cells = event_cells(&event, &scoped, &HashMap::new(), &placements);
        assert_eq!(cells, vec!["2016/Italy"]);
    }

    #[test]
    fn event_cells_arrival_at_the_viewed_prefix_itself_renders_dot() {
        let scoped = ResolvedScope {
            prefixes: vec!["/Archive/Media/2016/Italy".to_string()],
            from_cwd: true,
            auto_include_archived: false,
        };
        let event = TimelineEvent::Decision(Box::new(mk_decision(1, None)));
        let mut placements = HashMap::new();
        placements.insert(1, vec![(mk_extraction_row(None, None), RowAspect::Arrival)]);

        let cells = event_cells(&event, &scoped, &HashMap::new(), &placements);
        assert_eq!(cells, vec!["."]);
    }

    #[test]
    fn event_cells_intra_view_relocation_uses_drawn_from_cell_not_destination() {
        // A rearrangement row (both endpoints inside the view) still renders
        // the extraction-aspect cell — the drawn-from location — never the
        // arrival cell.
        let scoped = ResolvedScope {
            prefixes: vec!["/Volumes/old-laptop".to_string()],
            from_cwd: true,
            auto_include_archived: false,
        };
        let event = TimelineEvent::Decision(Box::new(mk_decision(1, None)));
        let mut placements = HashMap::new();
        placements.insert(
            1,
            vec![(mk_extraction_row(None, None), RowAspect::Rearrangement)],
        );

        let cells = event_cells(&event, &scoped, &HashMap::new(), &placements);
        assert_eq!(cells, vec!["photos/2016/italy"]);
    }

    #[test]
    fn event_cells_renders_one_cell_for_same_root_same_aspect_rows() {
        // Two matched rows from one root with one aspect are one rendered
        // line — the cell shows their common prefix, not two entries. (On
        // today's one-row-per-root data this is invisible; it is the
        // precision-readiness step for finer rows.)
        let global = ResolvedScope {
            prefixes: Vec::new(),
            from_cwd: false,
            auto_include_archived: false,
        };
        let event = TimelineEvent::Decision(Box::new(mk_decision(1, None)));
        let mut first = mk_extraction_row(None, None);
        first.rel_prefix = "photos/2016/01".to_string();
        let mut second = mk_extraction_row(None, None);
        second.rel_prefix = "photos/2016/02".to_string();
        let placements = HashMap::from([(
            1,
            vec![
                (first, RowAspect::Extraction),
                (second, RowAspect::Extraction),
            ],
        )]);

        let cells = event_cells(&event, &global, &HashMap::new(), &placements);
        assert_eq!(cells, vec!["/Volumes/old-laptop/photos/2016"]);
    }

    #[test]
    fn event_cells_measures_every_row_of_a_mixed_origin_decision() {
        // The width pass and the print pass read the same tagged rows, so a
        // mixed-origin decision measures both cells: the rearrangement's
        // drawn-from location and the arrival's destination.
        let scoped = ResolvedScope {
            prefixes: vec!["/Volumes/old-laptop".to_string()],
            from_cwd: true,
            auto_include_archived: false,
        };
        let event = TimelineEvent::Decision(Box::new(mk_decision(42, None)));
        let inside = mk_extraction_row(None, None);
        let mut outside = mk_extraction_row(None, None);
        outside.root_id = 2;
        outside.root_path = "/Volumes/nikon-sd".to_string();
        outside.rel_prefix = "dcim".to_string();

        let placements = HashMap::from([(
            42,
            vec![
                (inside, RowAspect::Rearrangement),
                (outside, RowAspect::Arrival),
            ],
        )]);

        let cells = event_cells(&event, &scoped, &HashMap::new(), &placements);
        assert_eq!(
            cells,
            vec!["photos/2016/italy", "/Archive/Media/2016/Italy"]
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
    fn rollup_counterparty_counts_carry_thousands_separators() {
        // Every other count in the trail goes through format_count; a fleet
        // of drives shouldn't be the one place that prints "1251".
        assert!(format_extraction_rollup(&ExtractionRollup {
            files: 5,
            bytes: Some(10),
            destinations: 1_251,
        })
        .contains("1,251 destinations"));
        assert!(format_arrival_rollup(&ArrivalRollup {
            files: 5,
            bytes: Some(10),
            origins: 2_400,
        })
        .contains("2,400 origins"));
        assert!(format_origin_line(&OriginLine::MultiOrigin {
            decision_id: 1,
            origin_count: 1_050,
            files: 5,
            bytes: 10,
            at: local_ts_on("2026-05-12"),
        })
        .contains("from 1,050 origins"));
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

    #[test]
    fn arrival_rollup_footer_composition() {
        let rollup = ArrivalRollup {
            files: 1_251,
            bytes: Some(22_100_000_000),
            origins: 2,
        };
        assert_eq!(
            format_arrival_rollup(&rollup),
            "Arrived here: 1,251 files (22.1 GB) from 2 origins."
        );
    }

    #[test]
    fn arrival_rollup_footer_singular_origin_and_omitted_bytes() {
        let rollup = ArrivalRollup {
            files: 1,
            bytes: None,
            origins: 1,
        };
        assert_eq!(
            format_arrival_rollup(&rollup),
            "Arrived here: 1 file from 1 origin."
        );
    }

    #[test]
    fn rearrangement_rollup_footer_composition() {
        let rollup = RearrangementRollup {
            files: 47,
            bytes: Some(3_900_000_000),
        };
        assert_eq!(
            format_rearrangement_rollup(&rollup),
            "Rearranged here: 47 files (3.9 GB)."
        );
    }

    #[test]
    fn rearrangement_rollup_footer_singular_and_omitted_bytes() {
        let rollup = RearrangementRollup {
            files: 1,
            bytes: None,
        };
        assert_eq!(
            format_rearrangement_rollup(&rollup),
            "Rearranged here: 1 file."
        );
    }

    #[test]
    fn rearrangement_rollup_footer_never_names_a_counterparty() {
        // Its two siblings end in "→ N destinations." / "from N origins."
        // This one must not, whatever the numbers: the counterparty is here.
        let line = format_rearrangement_rollup(&RearrangementRollup {
            files: 47,
            bytes: Some(3_900),
        });
        assert!(!line.contains("destination"), "{line}");
        assert!(!line.contains("origin"), "{line}");
        assert!(line.ends_with('.'), "{line}");
    }

    #[test]
    fn all_three_rollup_footers_coexist() {
        // A view can trade in both directions and rearrange internally at
        // once; the footer states crossings first, then what stayed.
        let lines = [
            format_extraction_rollup(&ExtractionRollup {
                files: 1_251,
                bytes: Some(22_100_000_000),
                destinations: 2,
            }),
            format_arrival_rollup(&ArrivalRollup {
                files: 340,
                bytes: Some(8_200_000_000),
                origins: 3,
            }),
            format_rearrangement_rollup(&RearrangementRollup {
                files: 47,
                bytes: Some(3_900_000_000),
            }),
        ];
        assert_eq!(
            lines.join("\n"),
            "Archived from here: 1,251 files (22.1 GB) \u{2192} 2 destinations.\n\
             Arrived here: 340 files (8.2 GB) from 3 origins.\n\
             Rearranged here: 47 files (3.9 GB)."
        );
    }

    // ------------------------------------------------------------------
    // Composition card rendering
    // ------------------------------------------------------------------

    fn mk_from_root(
        root_path: &str,
        root_removed: bool,
        files: i64,
        decision_ids: Vec<i64>,
        first_at: i64,
        last_at: i64,
    ) -> OriginLine {
        OriginLine::FromRoot {
            root_path: root_path.to_string(),
            root_removed,
            from_within: false,
            files,
            bytes: files * 100,
            decision_ids,
            first_at,
            last_at,
        }
    }

    /// A timestamp that maps back to `date_str` under the local timezone
    /// used by `format_date_only` — noon avoids DST-transition edge cases.
    /// Timezone-independent by construction: the date is chosen first, the
    /// timestamp derived from it, never the reverse.
    fn local_ts_on(date_str: &str) -> i64 {
        let date = chrono::NaiveDate::parse_from_str(date_str, "%Y-%m-%d").unwrap();
        let naive = date.and_hms_opt(12, 0, 0).unwrap();
        Local
            .from_local_datetime(&naive)
            .single()
            .unwrap()
            .timestamp()
    }

    #[test]
    fn plural_and_count_of_agree() {
        assert_eq!(plural(1, "origin"), "origin");
        assert_eq!(plural(0, "origin"), "origins");
        assert_eq!(plural(2, "origin"), "origins");
        assert_eq!(count_of(1, "destination"), "1 destination");
        assert_eq!(count_of(1_251, "destination"), "1,251 destinations");
    }

    // drew_from_lines

    fn show_group(
        location: &str,
        files: i64,
        bytes: Option<i64>,
        root_removed: bool,
        dirs: &[(&str, i64)],
    ) -> ShowExtraction {
        ShowExtraction {
            location: location.to_string(),
            root_removed,
            retired_book: None,
            files,
            bytes,
            directories: dirs
                .iter()
                .map(|(dir, files)| crate::trail::ops::show::ShowDrewDir {
                    dir: dir.to_string(),
                    files: *files,
                    bytes: None,
                })
                .collect(),
        }
    }

    #[test]
    fn drew_from_single_directory_group_is_one_line() {
        let lines = drew_from_lines(&[show_group(
            "/a/photos/2016/italy",
            47,
            Some(3_900_000),
            false,
            &[],
        )]);
        assert_eq!(lines, vec!["    /a/photos/2016/italy — 47 files (3.9 MB)"]);
    }

    #[test]
    fn drew_from_marks_removed_roots_on_the_summary_line() {
        let mut retired = show_group("/Volumes/gone/dcim", 12, None, true, &[]);
        retired.retired_book = Some("/archive/retired/gone".to_string());
        let retired_lines = drew_from_lines(&[retired]);
        assert!(
            retired_lines[0].contains("(root retired — the book: /archive/retired/gone)"),
            "{retired_lines:?}"
        );

        let lines = drew_from_lines(&[show_group("/Volumes/gone/dcim", 12, None, true, &[])]);
        assert_eq!(
            lines,
            vec![format!(
                "    /Volumes/gone/dcim — 12 files{ROOT_REMOVED_MARKER}"
            )]
        );
    }

    #[test]
    fn drew_from_lists_directories_under_the_summary() {
        let lines = drew_from_lines(&[show_group(
            "/a/m",
            245,
            None,
            false,
            &[("m/01", 105), ("m/02", 140)],
        )]);
        assert_eq!(
            lines,
            vec![
                "    /a/m — 245 files",
                "      m/01 — 105 files",
                "      m/02 — 140 files",
            ]
        );
    }

    #[test]
    fn drew_from_caps_directories_with_an_explicit_remainder() {
        let dirs: Vec<(String, i64)> = (1..=7).map(|i| (format!("m/{i:02}"), i)).collect();
        let dir_refs: Vec<(&str, i64)> = dirs.iter().map(|(d, f)| (d.as_str(), *f)).collect();
        let lines = drew_from_lines(&[show_group("/a/m", 28, None, false, &dir_refs)]);
        // Summary + 5 listed + remainder — never a silent truncation.
        assert_eq!(lines.len(), 7);
        assert_eq!(lines[6], "      \u{2026} and 2 more directories");
        // A root-level directory renders as ".", not an empty cell.
        let dot = drew_from_lines(&[show_group("/a", 3, None, false, &[("", 1), ("x", 2)])]);
        assert_eq!(dot[1], "      . — 1 file");
    }

    #[test]
    fn files_with_size_omits_an_unknown_size() {
        // The "never guess a size" rule, now enforced in one place for every
        // count the trail renders.
        assert_eq!(files_with_size(1, Some(10)), "1 file (10 B)");
        assert_eq!(
            files_with_size(1_251, Some(22_100_000_000)),
            "1,251 files (22.1 GB)"
        );
        assert_eq!(files_with_size(1, None), "1 file");
        assert_eq!(files_with_size(47, None), "47 files");
    }

    #[test]
    fn disposition_suffix_omits_when_the_row_cannot_say() {
        assert_eq!(
            disposition_suffix(Some(OriginDisposition::Retained), "kept", "gone"),
            " (kept)"
        );
        assert_eq!(
            disposition_suffix(Some(OriginDisposition::Relocated), "kept", "gone"),
            " (gone)"
        );
        // Pre-vocabulary backfilled rows add nothing at all — not "()".
        assert_eq!(disposition_suffix(None, "kept", "gone"), "");
    }

    #[test]
    fn format_bucket_singular_and_plural() {
        assert_eq!(format_bucket(1, 10), "1 file (10 B)");
        assert_eq!(format_bucket(2, 2_000), "2 files (2.0 KB)");
    }

    #[test]
    fn format_origin_line_from_root_single_decision_single_date() {
        let ts = local_ts_on("2024-01-05");
        let line = mk_from_root("/Volumes/old-laptop", false, 47, vec![12], ts, ts);
        assert_eq!(
            format_origin_line(&line),
            "from /Volumes/old-laptop: 47 files (4.7 KB) \u{b7} #12 \u{b7} 2024-01-05"
        );
    }

    #[test]
    fn format_origin_line_from_within_says_elsewhere_in_the_root() {
        let ts = local_ts_on("2026-05-12");
        let line = OriginLine::FromRoot {
            root_path: "/archive".to_string(),
            root_removed: false,
            from_within: true,
            files: 47,
            bytes: 3_900_000_000,
            decision_ids: vec![42],
            first_at: ts,
            last_at: ts,
        };
        assert_eq!(
            format_origin_line(&line),
            "from elsewhere in /archive: 47 files (3.9 GB) \u{b7} #42 \u{b7} 2026-05-12"
        );
    }

    #[test]
    fn format_origin_line_from_within_keeps_the_removed_root_marker() {
        // Both annotations are about the same root and must compose, not
        // displace each other.
        let ts = local_ts_on("2026-05-12");
        let line = OriginLine::FromRoot {
            root_path: "/archive".to_string(),
            root_removed: true,
            from_within: true,
            files: 1,
            bytes: 100,
            decision_ids: vec![42],
            first_at: ts,
            last_at: ts,
        };
        let text = format_origin_line(&line);
        assert!(text.starts_with("from elsewhere in /archive"), "{text}");
        assert!(text.contains(ROOT_REMOVED_MARKER), "{text}");
    }

    #[test]
    fn format_origin_line_from_root_merged_decisions_show_date_range_and_all_ids() {
        let first = local_ts_on("2024-01-05");
        let last = local_ts_on("2024-02-04");
        let line = mk_from_root("/Volumes/old-laptop", false, 15, vec![1, 2], first, last);
        let text = format_origin_line(&line);
        assert!(text.contains("#1, #2"));
        assert!(text.contains("2024-01-05 \u{2013} 2024-02-04"));
    }

    #[test]
    fn format_origin_line_from_root_marks_removed_root() {
        let ts = local_ts_on("2024-01-05");
        let line = mk_from_root("/Volumes/gone", true, 1, vec![1], ts, ts);
        assert!(format_origin_line(&line).starts_with("from /Volumes/gone (root removed):"));
    }

    #[test]
    fn format_origin_line_multi_origin_wording() {
        let line = OriginLine::MultiOrigin {
            decision_id: 42,
            origin_count: 3,
            files: 12,
            bytes: 1_200,
            at: local_ts_on("2024-01-05"),
        };
        assert_eq!(
            format_origin_line(&line),
            "via apply #42 from 3 origins: 12 files (1.2 KB) \u{b7} 2024-01-05"
        );
    }

    #[test]
    fn format_transitioned_line_composition() {
        let line = TransitionedLine {
            decision_id: 30,
            label: "excluded".to_string(),
            files: 4,
            bytes: 400,
        };
        assert_eq!(
            format_transitioned_line(&line),
            "excluded here (#30): 4 files (400 B)"
        );
    }

    fn mk_card(
        origins: Vec<OriginLine>,
        transitioned: Vec<TransitionedLine>,
        indexed_here: Option<crate::trail::domain::composition::BucketCount>,
        untracked: Option<crate::trail::domain::composition::BucketCount>,
    ) -> CompositionCard {
        let files = origins.iter().map(|o| o.files()).sum::<i64>()
            + transitioned.iter().map(|t| t.files).sum::<i64>()
            + indexed_here.map(|b| b.files).unwrap_or(0)
            + untracked.map(|b| b.files).unwrap_or(0);
        CompositionCard {
            files,
            bytes: files * 100,
            origins,
            transitioned,
            indexed_here,
            untracked,
        }
    }

    #[test]
    fn composition_card_lines_order_origins_transitioned_indexed_untracked() {
        let card = mk_card(
            vec![mk_from_root("/a", false, 5, vec![1], 0, 0)],
            vec![TransitionedLine {
                decision_id: 2,
                label: "excluded".to_string(),
                files: 1,
                bytes: 100,
            }],
            Some(crate::trail::domain::composition::BucketCount {
                files: 1,
                bytes: 100,
            }),
            Some(crate::trail::domain::composition::BucketCount {
                files: 1,
                bytes: 100,
            }),
        );
        let lines = composition_card_lines(&card);
        assert_eq!(lines.len(), 4);
        assert!(lines[0].starts_with("from /a"));
        assert!(lines[1].starts_with("excluded here"));
        assert!(lines[2].starts_with("first indexed here"));
        assert!(lines[3].starts_with("untracked (predates recording)"));
    }

    #[test]
    fn composition_card_lines_caps_origins_with_explicit_remainder() {
        let origins: Vec<OriginLine> = (0..12)
            .map(|i| mk_from_root(&format!("/vol{i}"), false, 1, vec![i], 0, 0))
            .collect();
        let card = mk_card(origins, Vec::new(), None, None);
        let lines = composition_card_lines(&card);
        // 10 origin lines + one remainder line.
        assert_eq!(lines.len(), 11);
        assert_eq!(lines[10], "\u{2026} and 2 more origins.");
    }

    #[test]
    fn composition_card_lines_no_remainder_when_under_cap() {
        let origins: Vec<OriginLine> = (0..3)
            .map(|i| mk_from_root(&format!("/vol{i}"), false, 1, vec![i], 0, 0))
            .collect();
        let card = mk_card(origins, Vec::new(), None, None);
        let lines = composition_card_lines(&card);
        assert_eq!(lines.len(), 3);
        assert!(!lines.iter().any(|l| l.contains("more")));
    }

    #[test]
    fn arrived_and_standing_rollups_render_independently_when_they_diverge() {
        // Event-vs-state divergence: content arrived (5) but some was later
        // deleted, leaving fewer standing (3) — both numbers must render
        // distinctly, never collapse into one.
        let arrival_rollup = ArrivalRollup {
            files: 5,
            bytes: Some(500),
            origins: 1,
        };
        let card = mk_card(
            vec![mk_from_root("/vol/a", false, 3, vec![7], 0, 0)],
            Vec::new(),
            None,
            None,
        );

        let arrived_line = format_arrival_rollup(&arrival_rollup);
        let standing_line = format!("Standing here: {}", format_bucket(card.files, card.bytes));
        assert!(arrived_line.contains("5 files"));
        assert!(standing_line.contains("3 files"));
        assert_ne!(arrived_line, standing_line);
    }
}
