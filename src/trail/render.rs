//! Trail's human-rendering stratum: composes the scope lens, the time lens,
//! and the composition card into the text `canon trail`/`trail show` print.

use std::collections::HashMap;

use chrono::{Local, TimeZone};

use crate::core::domain::decision::Decision;
use crate::core::domain::extraction::{DecisionExtraction, OriginDisposition};
use crate::core::domain::fate::{fate_transition, DecisionFamily, FateAspect};
use crate::core::domain::format::{cap_path, format_count, format_size};
use crate::core::domain::root::Root;
use crate::core::ops::scope::ResolvedScope;
use crate::trail::domain::composition::{CompositionCard, OriginLine, TransitionedLine};
use crate::trail::domain::placement::{aggregate_placement_lines, RowAspect, ScopeMatch};
use crate::trail::domain::timeline::{decision_act, DayRollup, FateLine, TimelineEvent};
use crate::trail::ops::compute::{
    ArrivalRollup, ExtractionRollup, RearrangementRollup, DEFAULT_LIMIT,
};
use crate::trail::ops::crossings::{
    CounterpartLine, CrossingBody, CrossingDelivery, CrossingPlace, CrossingSection,
    CrossingsParams, CrossingsResult, NothingCrossed, Reconciliation,
};
use crate::trail::ops::show::ShowExtraction;
use crate::trail::{TrailResult, TrailView};

/// The trail's header line: where it looked, when, and — behind a closed
/// door — the pause.
///
/// **The pause is read from the scope, never from the rows.** It is a
/// property of the place being read at, so it is stated whether or not the
/// `roots_suspend` decision happens to fall inside the listing's cap, and
/// whether the place was named or stood in. A reader deducing the door from
/// a row that might not be there is exactly the silence the closed door is
/// supposed to end.
fn trail_header(scope_part: &str, time_label: Option<&str>, resolved: &ResolvedScope) -> String {
    let pause = crate::scope::parked_pause(&resolved.pause)
        .map(|line| format!(" — {line}"))
        .unwrap_or_default();
    match time_label {
        Some(label) => format!("Decision trail: {scope_part} — {label}{pause}"),
        None => format!("Decision trail: {scope_part}{pause}"),
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn print_human(
    result: &TrailResult,
    resolved: &ResolvedScope,
    time_label: Option<&str>,
    roots: &HashMap<i64, Root>,
    limit: Option<usize>,
    card: Option<&CompositionCard>,
    long: bool,
) {
    let scope_part = if resolved.is_global() {
        "all roots".to_string()
    } else {
        resolved.prefixes.join(", ")
    };
    println!("{}", trail_header(&scope_part, time_label, resolved));

    let view_root = result.view_root.as_deref();

    // Scope column: one width across the whole listing, capped so long
    // paths can't push the narration off-screen. Measured over the cells
    // that will actually be printed — extraction lines render the drawn-from
    // location, arrival lines the destination, neither the selection scope.
    //
    // The act column is measured in the same pass and over the same events,
    // for the same reason: two passes disagreeing about what a row will
    // print is what `event_cells` exists to prevent, and a second column
    // sized independently would reintroduce it one column over.
    let cells_of = |events: &[&TimelineEvent], root: Option<&str>| -> Vec<String> {
        events
            .iter()
            .flat_map(|e| {
                event_cells(
                    e,
                    resolved,
                    roots,
                    root,
                    &result.placements,
                    &result.scope_matches,
                )
            })
            .collect()
    };
    let scope_width = |cells: &[String]| -> usize {
        cells
            .iter()
            .map(|cell| cell.chars().count())
            .max()
            .unwrap_or(0)
            .min(SCOPE_CELL_MAX)
    };
    let act_width = |events: &[&TimelineEvent]| -> usize {
        events
            .iter()
            .filter_map(|e| event_act(e))
            .map(|act| act.chars().count())
            .max()
            .unwrap_or(0)
    };

    // The legend is printed exactly when the listing uses the shape it
    // explains: a leading `/` means "measured from the root", which is
    // indistinguishable from an absolute path without being told once. On a
    // run where every place sits at or below the viewed folder, nothing here
    // needs explaining and nothing is said.
    //
    // Whether the shape appears is decided by rendering the same cells with
    // no root to measure from and comparing — exact, and asked of the one
    // function that makes the choice, rather than sniffed back out of a
    // string that no longer records which arm produced it.
    let announce_root = |events: &[&TimelineEvent], cells: &[String]| {
        if let Some(root) = view_root {
            let absolute = cells_of(events, None);
            if cells.iter().zip(&absolute).any(|(a, b)| a != b) {
                println!("Places are relative to this folder; a leading / is relative to {root}.");
            }
        }
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
                if long {
                    for event in events {
                        println!();
                        print_long_event(
                            event,
                            true,
                            roots,
                            &result.placements,
                            &result.scope_matches,
                        );
                    }
                } else {
                    let refs: Vec<&TimelineEvent> = events.iter().collect();
                    let cells = cells_of(&refs, view_root);
                    announce_root(&refs, &cells);
                    let w = scope_width(&cells);
                    let aw = act_width(&refs);
                    println!();
                    for event in events {
                        print_event(
                            event,
                            true,
                            resolved,
                            roots,
                            view_root,
                            w,
                            aw,
                            &result.placements,
                            &result.scope_matches,
                        );
                    }
                }
            }
        }
        TrailView::Days(days) => {
            if days.is_empty() {
                println!();
                println!("No decisions {}.", time_label.unwrap_or("in range"));
            }
            // Long mode sizes no columns: an entry is as tall as it needs
            // to be, so there is nothing to measure and nothing to explain.
            let (w, aw) = if long {
                (0, 0)
            } else {
                let refs: Vec<&TimelineEvent> = days.iter().flat_map(|d| &d.events).collect();
                let cells = cells_of(&refs, view_root);
                announce_root(&refs, &cells);
                (scope_width(&cells), act_width(&refs))
            };
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
                    if long {
                        print_long_event(
                            event,
                            false,
                            roots,
                            &result.placements,
                            &result.scope_matches,
                        );
                        println!();
                    } else {
                        print_event(
                            event,
                            false,
                            resolved,
                            roots,
                            view_root,
                            w,
                            aw,
                            &result.placements,
                            &result.scope_matches,
                        );
                    }
                }
            }
        }
    }

    // All three rollups are scope-lens-only footers — none appears alongside
    // the day-grouped time lens. Independent of each other: a view can draw
    // content out, receive content in, rearrange content within itself, any
    // combination, or none. Crossings are stated first (what this place traded
    // with the rest of the universe), then what merely moved inside it.
    let rollup_lines = match &result.view {
        TrailView::Recent(_) => rollup_block_lines(
            result.extraction_rollup.as_ref(),
            result.arrival_rollup.as_ref(),
            result.rearrangement_rollup.as_ref(),
        ),
        TrailView::Days(_) => Vec::new(),
    };

    if !rollup_lines.is_empty() || result.earlier_decisions > 0 || result.unscoped_decisions > 0 {
        println!();
    }
    for line in &rollup_lines {
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

/// The scope-lens rollup block, teaching line included. Pure data — the same
/// separation `composition_card_lines` keeps, so the ordering is testable
/// without capturing stdout, which is what the hint's placement needs.
///
/// The hint sits **between** the two crossing rollups and the rearrangement
/// one. It teaches the door, the door itemizes crossings, and a rearrangement
/// has no counterpart to itemize — so beneath the whole block, "these totals"
/// would point last at the single total the door has nothing to say about.
///
/// A view whose only rollup is the rearrangement goes **untaught**: the door
/// answers "nothing crossed" there, so the invitation would promise places it
/// cannot list. That joins the global and time-lens views the door already
/// leaves untaught — a recorded category, not a new one.
fn rollup_block_lines(
    extraction: Option<&ExtractionRollup>,
    arrival: Option<&ArrivalRollup>,
    rearrangement: Option<&RearrangementRollup>,
) -> Vec<String> {
    let mut lines: Vec<String> = [
        extraction.map(format_extraction_rollup),
        arrival.map(format_arrival_rollup),
    ]
    .into_iter()
    .flatten()
    .collect();
    if !lines.is_empty() {
        lines.push(CROSSINGS_HINT.to_string());
    }
    lines.extend(rearrangement.map(format_rearrangement_rollup));
    lines
}

/// The crossings door's human output: the rollup sentences, expanded.
///
/// Section headers are the rollup lines verbatim in form — the door *is*
/// those lines made expandable, so no new noun is coined for it. An unnamed
/// section keeps the counterparty clause and itemizes the counterparties; a
/// named one replaces that clause with the counterpart itself and drops to
/// the deliveries beneath it.
///
/// Counterpart paths render **full and unelided**, on their own line. The
/// path is this door's key — it is what the reader copies from one invocation
/// into the next — so a column-aligned form that would elide or wrap a long
/// path breaks the reach chain at its first hop.
pub(super) fn print_crossings(
    result: &CrossingsResult,
    resolved: &ResolvedScope,
    params: &CrossingsParams,
) {
    let scope_part = if resolved.is_global() {
        "all roots".to_string()
    } else {
        resolved.prefixes.join(", ")
    };
    let pause = crate::scope::parked_pause(&resolved.pause)
        .map(|line| format!(" — {line}"))
        .unwrap_or_default();
    println!("Crossings: {scope_part}{pause}");

    for section in &result.sections {
        println!();
        for line in crossing_section_lines(section, resolved, params) {
            println!("{line}");
        }
    }

    if let Some(hint) = drill_down_hint(result, params) {
        println!();
        println!("{hint}");
    }

    if let Some(nothing) = &result.nothing_crossed {
        println!();
        println!("{}", nothing_crossed_line(nothing, resolved, params));
    }

    if let Some(reconciliation) = &result.reconciliation {
        println!();
        println!("{}", reconciliation_line(reconciliation));
    }
}

/// The one deliberate register crossing, composed: two observations on each
/// axis, and no cause on either.
///
/// The clauses are composed **independently**, and each states an "of the"
/// construction only where its own two numbers arithmetically permit one.
/// Containment fails in both directions — a decision stamp survives a
/// scan-observed move into the view, so what stands here can exceed what this
/// door records as delivered — which is why the standing-beyond-delivered
/// branch drops the construction entirely rather than stating an arithmetic
/// impossibility from the one line designed never to guess. The decisions
/// clause never had one to drop: the trace failed for it first.
///
/// **Equal counts do not license a claim about the same files.** The identity
/// reading — "all of what was delivered still stands" — is the strongest
/// statement anything on this surface makes, and the containment trace forbids
/// it in exactly the way it forbids "of the": deliver 229 here and one
/// elsewhere, lose one of the 229, and let a scan observe the stray moving in
/// with its stamp intact, and the counts match while the sets do not. So
/// equality states the two counts plainly, in the same shape the
/// standing-beyond-delivered case uses — which still spares the reader the
/// subtraction that made `229 of the 229` read badly, without claiming what
/// these rows cannot show. Suppressing the line stays rejected: an absent line
/// cannot be told from "nothing was lost", nor from "there is no card at all".
///
/// The decisions clause carries **no pronoun**. Two of the three files-clause
/// forms now end on the delivered count, so a "them" would sit next to the
/// wrong number and say that N decisions stand behind the *delivered* files
/// while announcing a different delivered figure in the same breath. Naming
/// nothing and mirroring the files clause's own shape says it exactly once.
fn reconciliation_line(r: &Reconciliation) -> String {
    let files = match r.standing.cmp(&r.delivered) {
        // The one form that *is* a proportion, and the only one whose two
        // numbers license saying so. Unreachable at `delivered == 1`, since
        // a card line carries at least one standing file.
        std::cmp::Ordering::Less => format!(
            "{} of the {} files delivered",
            format_count(r.standing),
            format_count(r.delivered)
        ),
        // Equal and greater share one spelling: two counts, side by side,
        // claiming nothing about which files they are. Spelled out rather
        // than left to a `_` arm — this is the one claim-bearing line on the
        // surface, and a law match here states its cases.
        std::cmp::Ordering::Equal | std::cmp::Ordering::Greater => format!(
            "{} {} {}; {} {} delivered",
            format_count(r.standing),
            plural(r.standing, "file"),
            stand_agrees(r.standing),
            format_count(r.delivered),
            was_agrees(r.delivered)
        ),
    };
    let decisions = if r.standing_decisions == r.delivered_decisions {
        String::new()
    } else {
        format!(
            " \u{2014} {} {}; {} delivered",
            count_of(r.standing_decisions as i64, "decision"),
            stand_agrees(r.standing_decisions as i64),
            format_count(r.delivered_decisions as i64)
        )
    };
    format!("Standing here: {files}{decisions}.")
}

/// `stand` / `stands`, agreeing with its subject's count.
fn stand_agrees(n: i64) -> &'static str {
    if n == 1 {
        "stands"
    } else {
        "stand"
    }
}

/// `was` / `were`, agreeing with its subject's count.
fn was_agrees(n: i64) -> &'static str {
    if n == 1 {
        "was"
    } else {
        "were"
    }
}

/// Whether this view teaches its own drill-down, and with what.
///
/// **Once per view, never per entry** — an `Option` rather than a per-line
/// decision, so a second invitation is unrepresentable rather than merely
/// avoided.
///
/// The test is **per section, not per view**: a bare section's entries are
/// handles whether or not a sibling section was named. Naming a counterpart
/// opens one direction and leaves the other listing counterparts as usual, so
/// a view can hold a named section and a bare one at once — standing in
/// `/archive/2016` with `--destination /archive` is the shape — and the bare
/// half still has entries nothing else announces. A view whose every section
/// is named teaches nothing, which is the case the rule was written for: the
/// reader has already stepped through the door. Machine output carries no
/// hints at all — belt and braces, since `--jsonl` returns before this
/// renderer is reached.
///
/// A view with no bare entries has no handle to teach: "nothing crossed"
/// carries nothing to drill into.
///
/// The flag is **derived from the sections actually rendered**, so the hint
/// can only ever name a flag the entries on screen feed. A one-directional
/// view — the common case — names one flag; a view rendering both sections
/// names both, which is the case the two-flag spelling was written for.
fn drill_down_hint(result: &CrossingsResult, params: &CrossingsParams) -> Option<String> {
    if params.machine_output {
        return None;
    }
    let mut flags: Vec<&str> = result
        .sections
        .iter()
        .filter(|section| {
            section.named.is_none() && matches!(&section.body, CrossingBody::Counterparts { .. })
        })
        .map(|section| entry_flag(section.aspect))
        .collect();
    flags.dedup();
    // Origin before destination where both render, the settled spelling.
    flags.sort_by_key(|flag| *flag != "--origin");

    let (first, rest) = flags.split_first()?;
    let mut invocation = format!("`canon trail crossings {first} <path>`");
    for flag in rest {
        invocation.push_str(&format!(" or `{flag} <path>`"));
    }
    Some(format!("{invocation} {CROSSINGS_DRILL_DOWN_PAYOFF}"))
}

/// One section's lines, indentation included. Pure data — the same separation
/// `drew_from_lines` and `composition_card_lines` keep, so the caps, the
/// ordering and the wording are testable without capturing stdout.
fn crossing_section_lines(
    section: &CrossingSection,
    resolved: &ResolvedScope,
    params: &CrossingsParams,
) -> Vec<String> {
    let mut out = Vec::new();
    let counts = files_with_size(section.files, section.bytes);
    // Where this section stands. "Here" is the view — but the flag naming the
    // section's *inside* end narrows the counts without narrowing the word,
    // so wherever that flag is present the place is named instead. This holds
    // whether or not the outside end was also named: with both flags the
    // counts are narrowed at both ends, and saying "here" is exactly as
    // unaccountable as it is with one.
    let inside = inside_end(section.aspect, params);
    match &section.named {
        Some(counterpart) => {
            let head = match section.aspect {
                RowAspect::Arrival => format!(
                    "Arrived {}: {counts} from {}",
                    inside.map_or("here".to_string(), |path| format!("at {path}")),
                    counterpart.path
                ),
                RowAspect::Extraction | RowAspect::Rearrangement | RowAspect::Outside => {
                    format!(
                        "Archived from {}: {counts} \u{2192} {}",
                        inside.unwrap_or("here"),
                        counterpart.path
                    )
                }
            };
            // The marker takes its own line rather than trailing the path:
            // both are long, and the path must stay copy-pasteable to the end
            // of the line it is on.
            let marker = origin_marker(
                counterpart.retired_book.as_deref(),
                counterpart.root_removed,
            );
            if marker.is_empty() {
                out.push(format!("{head}."));
            } else {
                out.push(head);
                out.push(format!(
                    "  ({}).",
                    marker.trim().trim_start_matches('(').trim_end_matches(')')
                ));
            }
        }
        None => out.push(match section.aspect {
            RowAspect::Arrival => format!(
                "Arrived {}: {counts} from {}.",
                inside.map_or("here".to_string(), |path| format!("at {path}")),
                count_of(section.counterparty_count as i64, "origin")
            ),
            RowAspect::Extraction | RowAspect::Rearrangement | RowAspect::Outside => format!(
                "Archived from {}: {counts} \u{2192} {}.",
                inside.unwrap_or("here"),
                count_of(section.counterparty_count as i64, "destination")
            ),
        }),
    }

    match &section.body {
        CrossingBody::Counterparts { lines, more } => {
            for line in lines {
                out.extend(counterpart_lines(line));
            }
            if *more > 0 {
                let noun = match section.aspect {
                    RowAspect::Arrival => "origin",
                    RowAspect::Extraction | RowAspect::Rearrangement | RowAspect::Outside => {
                        "destination"
                    }
                };
                out.push(format!(
                    "  \u{2026} and {} more {}.",
                    format_count(*more as i64),
                    plural(*more as i64, noun)
                ));
            }
        }
        CrossingBody::Deliveries { lines, more } => {
            for line in lines {
                out.extend(delivery_lines(line, section, resolved, params));
            }
            if *more > 0 {
                out.push(format!(
                    "  \u{2026} and {} more {}.",
                    format_count(*more as i64),
                    plural(*more as i64, "decision")
                ));
            }
        }
    }
    out
}

/// The place a section's **inside** end was narrowed to, if a flag named it.
///
/// The exact complement of `ops::crossings`' outside-end rule: a section's
/// counterpart is its outside end (origin for an arrival, destination for an
/// extraction), so the *other* flag is the one narrowing the side the reader
/// is standing on. That flag shrinks the counts without shrinking the word
/// "here", which is what leaves the same sentence at the same place carrying
/// smaller numbers with nothing to account for them.
fn inside_end(aspect: RowAspect, params: &CrossingsParams) -> Option<&str> {
    match aspect {
        RowAspect::Arrival => params.destination.as_deref(),
        RowAspect::Extraction | RowAspect::Rearrangement | RowAspect::Outside => {
            params.origin.as_deref()
        }
    }
}

/// The head and continuation indents of a keyed entry, per surface.
///
/// The card's lines are printed under [`CARD_LINE_PREFIX`], so its
/// continuations land in the same column as the door's — the two surfaces
/// carry identical facts and must carry them identically.
const DOOR_INDENTS: (&str, &str) = ("  ", "      ");
/// The card's half of [`DOOR_INDENTS`] — see there.
const CARD_INDENTS: (&str, &str) = ("", "    ");

/// One keyed entry: a path on its own line, its marker beneath it when it has
/// one, then its counts.
///
/// The shape both surfaces that name a place compose through — the door's
/// counterpart entries and the card's origin lines. It exists because the
/// same three facts were being spelled two ways: the door over three lines,
/// the card crammed onto one that ran past two hundred characters on a
/// retired root. Three lines survive any path length; one line does not, so
/// that is the shape both take.
///
/// The path is **whole and unelided** and ends its own line: it is the key
/// the reader copies from one invocation into the next, so a column-aligned
/// form that could elide it breaks the reach chain at its first hop.
///
/// The marker and the counts are both continuations of the head above them,
/// so they **indent alike** — one column of difference between two lines
/// hanging off the same path reads as structure that is not there. That is
/// one parameter here rather than a rule kept twice.
fn keyed_entry_lines(head: &str, marker: &str, counts: &str, indents: (&str, &str)) -> Vec<String> {
    let (head_indent, continuation) = indents;
    let mut out = vec![format!("{head_indent}{head}")];
    if !marker.is_empty() {
        out.push(format!("{continuation}{}", marker.trim()));
    }
    out.push(format!("{continuation}{counts}"));
    out
}

/// One counterpart entry, in the shared keyed-entry shape.
fn counterpart_lines(line: &CounterpartLine) -> Vec<String> {
    let mut counts = format!(
        "{} \u{00b7} {} \u{00b7} {}",
        files_with_size(line.files, line.bytes),
        decision_chip(&line.decision_ids),
        format_date_range(line.first_at, line.last_at)
    );
    // A coarsened key must say how much it stands for, or it reads as the
    // whole truth. One folder needs no saying — the path above *is* it.
    if line.folders > 1 {
        counts.push_str(&format!(
            " \u{00b7} {}",
            count_of(line.folders as i64, "folder")
        ));
    }
    keyed_entry_lines(
        &line.counterpart.path,
        &origin_marker(
            line.counterpart.retired_book.as_deref(),
            line.counterpart.root_removed,
        ),
        &counts,
        DOOR_INDENTS,
    )
}

/// A count of decisions, or — where there is exactly one — the decision.
///
/// A list of one is not a list, so the count-over-list rule that banished id
/// *lists* from these surfaces has nothing to say here: hiding the single id
/// makes the reader open a second view to learn a number the line already
/// holds, when a bare id is already a taught handle in this tool (`canon
/// trail 57` hints at `trail show 57`). Three surfaces render it — the two
/// bare-view directions and the card's origin lines — through this one
/// spelling.
///
/// The ids come off the aggregation the count is computed from; nothing here
/// looks a decision up.
fn decision_chip(ids: &[i64]) -> String {
    match ids {
        [id] => format!("decision #{id}"),
        _ => count_of(ids.len() as i64, "decision"),
    }
}

/// One delivery: the decision's header line, its places, then its reason.
///
/// Each end renders against **its own anchor** — the named counterpart for
/// the named side, the view for the other — falling back to absolute where
/// there is none. The counterpart path itself stays whole in the header; the
/// per-row directories beneath it are interior structure, not keys.
fn delivery_lines(
    line: &CrossingDelivery,
    section: &CrossingSection,
    resolved: &ResolvedScope,
    params: &CrossingsParams,
) -> Vec<String> {
    let disposition = match section.aspect {
        RowAspect::Arrival => disposition_suffix(
            line.disposition,
            INBOUND_DISPOSITION.0,
            INBOUND_DISPOSITION.1,
        ),
        RowAspect::Extraction | RowAspect::Rearrangement | RowAspect::Outside => {
            disposition_suffix(
                line.disposition,
                OUTBOUND_DISPOSITION.0,
                OUTBOUND_DISPOSITION.1,
            )
        }
    };
    // The parenthetical is the timeline's shape; here the word stands on its
    // own at the end of a column line.
    let disposition = disposition
        .trim()
        .trim_start_matches('(')
        .trim_end_matches(')')
        .to_string();

    let mut out = vec![format!(
        "  #{:<4} {}   {}{}",
        line.decision_id,
        format_date_only(line.at),
        files_with_size(line.files, line.bytes),
        if disposition.is_empty() {
            String::new()
        } else {
            format!("   {disposition}")
        }
    )];

    let anchor_for = |side_is_origin: bool| -> Option<&str> {
        let named = if side_is_origin {
            params.origin.as_deref()
        } else {
            params.destination.as_deref()
        };
        named.or_else(|| {
            if resolved.prefixes.len() == 1 {
                Some(resolved.prefixes[0].as_str())
            } else {
                None
            }
        })
    };
    let origin_anchor = anchor_for(true);
    let destination_anchor = anchor_for(false);

    let cap = place_cap(params);
    let shown: Vec<&CrossingPlace> = line.places.iter().take(cap).collect();
    let width = shown
        .iter()
        .map(|p| under(&p.origin, origin_anchor).chars().count())
        .max()
        .unwrap_or(0);
    for place in &shown {
        out.push(format!(
            "        {:<width$}  \u{2192} {}",
            under(&place.origin, origin_anchor),
            under(&place.destination, destination_anchor)
        ));
    }
    let more = line.places.len().saturating_sub(cap);
    if more > 0 {
        let noun = if more == 1 { "place" } else { "places" };
        out.push(format!(
            "        \u{2026} and {} more {noun}",
            format_count(more)
        ));
    }
    if let Some(reason) = &line.reason {
        out.push(format!("        \"{reason}\""));
    }
    out
}

/// How many places one delivery lists before its remainder line.
///
/// A remainder has to have a door, and `--all` is the invocation that opens
/// it: the flag uncaps this listing exactly as it uncaps the listing above,
/// or the reader is told what is missing and given no way to reach it — the
/// very shape this whole surface exists to answer, one level down.
/// `--limit N` sizes the listing of entries and not this one; the places
/// beneath a single delivery are that delivery's interior, so they keep
/// their own constant.
fn place_cap(params: &CrossingsParams) -> usize {
    match params.limit {
        Some(_) => DREW_FROM_DIR_CAP,
        None => usize::MAX,
    }
}

/// A location measured from its own anchor, or whole where there is none.
/// The anchor's own path renders as `.`, matching the timeline's view-relative
/// cell.
fn under(path: &str, anchor: Option<&str>) -> String {
    match anchor {
        Some(anchor) if path == anchor => ".".to_string(),
        Some(anchor) => crate::core::domain::path_strip_prefix(path, anchor)
            .map(str::to_string)
            .unwrap_or_else(|| path.to_string()),
        None => path.to_string(),
    }
}

/// What to say when nothing crossed.
///
/// A view whose every row stayed inside it has to name the rearrangement, or
/// the silence reads as "nothing ever happened here" — the opposite of the
/// truth about a heavily curated archive.
///
/// And where a counterpart was **named**, this line names it back. Otherwise
/// the answer never mentions what was asked about: a named counterpart lives
/// on a `CrossingSection`, empty sections are omitted rather than printed
/// empty, and the two together make the ask unreachable exactly when nothing
/// matched it. The refusal path already names the paths it refuses — a
/// self-explaining answer names what was asked — and this is the same
/// principle at the site that fell between those two rules.
///
/// The observable shape: pointing `--origin` at a place Canon knows but that
/// is no delivery's endpoint — a retired book's own directory sitting inside
/// the archive, an indexed source folder never applied from — answered with a
/// sentence naming nothing, while a path Canon has *never* heard of got a
/// statement naming it. The near miss was less informative than the total one.
fn nothing_crossed_line(
    nothing: &NothingCrossed,
    resolved: &ResolvedScope,
    params: &CrossingsParams,
) -> String {
    // A **global** view has no "here" to measure between: it borrowed the
    // named counterpart as its boundary, so that place *is* the subject
    // rather than a second end. Both variants take that carve-out in every
    // reachable case, or whichever misses it says "here" where there is none.
    // The unnamed global cells are unreachable — the interface refuses a
    // boundless ask — and are spelled anyway rather than folded away, so the
    // two variants stay readable side by side.
    let named = asked_about(params);
    let global = resolved.is_global();
    match nothing {
        NothingCrossed::Rearranged { files, bytes } => {
            let counts = files_with_size(*files, *bytes);
            match (&named, global) {
                // Globally the boundary is borrowed from what was named, so
                // "within it" reaches for the right place. Where *both*
                // counterparts are named only one of them is promoted, so the
                // pronoun has two antecedents — but a rearrangement requires
                // both endpoints inside the boundary *and* past both filters,
                // which puts the rows under both named paths, so the sentence
                // is true whichever one the reader binds it to.
                (Some(place), true) => {
                    format!("No recorded crossing at {place}. {counts} were rearranged within it.")
                }
                // Scoped, the rearrangement happened within the **view**, not
                // within the named counterpart — so the boundary is named
                // rather than left to a pronoun that would now reach for the
                // place mentioned just before it.
                (Some(place), false) => format!(
                    "Nothing crossed between here and {place}. \
                     {counts} were rearranged within this boundary."
                ),
                (None, _) => {
                    format!("Nothing crossed this boundary. {counts} were rearranged within it.")
                }
            }
        }
        NothingCrossed::Nothing => match (&named, global) {
            (Some(place), true) => format!("No recorded crossing at {place}."),
            (Some(place), false) => format!("Nothing has crossed between here and {place}."),
            (None, true) => "No recorded crossing.".to_string(),
            (None, false) => "Nothing has crossed this boundary.".to_string(),
        },
    }
}

/// The place (or places) a flag named, for a line that has no section to
/// carry them. `None` when the ask named none, which is the bare view.
fn asked_about(params: &CrossingsParams) -> Option<String> {
    match (params.origin.as_deref(), params.destination.as_deref()) {
        // Both flags on one place is one place, not the same path twice.
        (Some(origin), Some(destination)) if origin == destination => Some(origin.to_string()),
        (Some(origin), Some(destination)) => Some(format!("{origin} or {destination}")),
        (Some(place), None) | (None, Some(place)) => Some(place.to_string()),
        (None, None) => None,
    }
}

/// The line that teaches the door, printed once beneath the **two crossing
/// rollups** — above `Rearranged here`, which the door does not itemize.
///
/// `crossings` appears on no output line of its own, so the surfaces that
/// invite it name it at the moment of need — the same move the all-digits
/// `trail show` hint makes. Once, not once per rollup.
///
/// It states **what running it yields**, not the gesture. "Expand these by
/// place" named a motion with no visible referent — "these" pointed at a
/// single rollup line, and a reader had to run the command to decode its own
/// invitation. The rollup totals itemized per counterpart place is what comes
/// back, so that is what the line says.
const CROSSINGS_HINT: &str = "  `canon trail crossings` to list the places behind these totals";

/// The card's own teacher, printed beneath its origins block.
///
/// The card's origin lines **are** door handles — the rendered path is
/// exactly the argument `--origin` takes — and until now the only thing that
/// said so was the origins *remainder*, which fires only on a capped card. A
/// card showing all its origins taught nothing, precisely where the handles
/// are. The payoff is named, not the flag: nothing in a flag's name says what
/// is behind it, so the line says it.
/// Its payoff carries the same referent the drill-down's does, for the same
/// reason: the card's origin lines already name their decision where there is
/// one, so promising "the decisions" would offer what is on screen. What
/// naming an origin adds is the folders.
const CARD_ORIGIN_HINT: &str =
    "`canon trail crossings --origin <path>` to list the folders behind an origin";

/// What naming an entry adds that the bare listing does not already show —
/// the drill-down hint's promise, held as **one word with a checked
/// referent** rather than as free prose.
///
/// A hint is a claim about a *different* rendering, and a claim held only as
/// prose goes stale silently. Two ways it can: the promised thing moves onto
/// the listing, so the invitation offers what is already on screen; or the
/// promise is reworded past what naming an entry actually delivers. Neither
/// breaks anything a reader of this file would notice.
///
/// So the promise names a rendering feature, and
/// `the_drill_down_hint_promises_only_what_naming_an_entry_adds` pins the
/// feature from both sides — present where an entry is named, absent from the
/// bare entry, and named in this const. Move the folder pairs onto the bare
/// listing and the pin fails; reword the promise away from what is delivered
/// and it fails too. That does not make English self-verifying, and it is not
/// meant to: it gives the sentence a referent a test can hold.
const CROSSINGS_DRILL_DOWN_PAYOFF: &str = "to list the folders behind an entry";

/// Which flag opens a section's entries.
///
/// The exact complement of the operations layer's outside-end rule: a
/// section's counterpart is its outside end, so an **extraction** section
/// lists destinations and `--destination` opens them, while an **arrival**
/// section lists origins and `--origin` does. Derived from the rendered
/// sections rather than written into the hint, because a hint naming the flag
/// that does *not* match what is on screen invites the reader to substitute a
/// listed path into a dead end — and since a counterpart that matches nothing
/// now answers "nothing has crossed between here and <path>", it is a
/// *silent* dead end. Exhaustive, so a new aspect is a compile error rather
/// than a hint that quietly points the wrong way.
fn entry_flag(aspect: RowAspect) -> &'static str {
    match aspect {
        RowAspect::Extraction => "--destination",
        RowAspect::Arrival => "--origin",
        // Unreachable: a section is only ever one of the two crossings.
        RowAspect::Rearrangement | RowAspect::Outside => "--destination",
    }
}

/// Maximum number of origin lines the composition card shows before an
/// explicit remainder line — the origins section scales with a location's
/// history (many source drives feeding one archive folder over years).
/// Standings, indexed-here and untracked need no cap: each is merged, so its
/// line count is bounded by the vocabulary. Gaps have their own cap below.
const CARD_ORIGIN_CAP: usize = 10;

/// Maximum number of gap lines the composition card shows before an explicit
/// remainder line. Gaps are the one part of the transitioned section that
/// stays per-decision, so they are the one part whose length tracks the
/// history rather than the vocabulary.
const CARD_GAP_CAP: usize = 5;

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
        let marker = origin_marker(group.retired_book.as_deref(), group.root_removed);
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

/// The indent every card body line is printed under.
///
/// Named rather than spelled inline because it is half of an alignment claim:
/// the card's continuations land in the door's column only once this prefix is
/// applied, so the test that pins that claim has to measure through the same
/// value the printer uses. Spelled twice, the two could part with nothing
/// failing.
const CARD_LINE_PREFIX: &str = "  ";

fn print_composition_card(card: &CompositionCard) {
    println!();
    println!("Standing here: {}", format_bucket(card.files, card.bytes));
    for line in composition_card_lines(card) {
        println!("{CARD_LINE_PREFIX}{line}");
    }
}

/// The card's body lines below the "Standing here" header: origins
/// (files desc, capped with an explicit remainder), transitioned standings,
/// transitioned gaps (capped likewise), indexed-here, untracked. Pure data —
/// kept separate from `print_composition_card` so the caps and ordering are
/// testable without capturing stdout.
fn composition_card_lines(card: &CompositionCard) -> Vec<String> {
    let mut lines = Vec::new();
    // The cap counts **origins, not physical lines**: an origin now emits
    // several, and capping the emitted lines would truncate one entry
    // mid-shape.
    for line in card.origins.iter().take(CARD_ORIGIN_CAP) {
        lines.extend(format_origin_line(line));
    }
    // Whether an origin line puts a path on screen that `--origin` can take.
    //
    // `MultiOrigin` names no root — it reads `via apply #N from M origins` —
    // so a card whose origins are all multi-origin has no handle to teach,
    // and the invitation would name a `<path>` the reader cannot find
    // anywhere above it. Same rule the rollup hint's placement rests on: a
    // hint must not attach to a line it cannot deliver on.
    fn names_a_path(line: &OriginLine) -> bool {
        matches!(line, OriginLine::FromRoot { .. })
    }

    // Decided once, for both emission sites: the invitation names a `<path>`,
    // so it prints only where the card put one on screen. Evaluated over the
    // **shown** origins rather than all of them — the reader can only
    // substitute what they can see, and the remainder line is precisely the
    // one whose own origins were declined.
    let teach_the_door = card.origins.iter().take(CARD_ORIGIN_CAP).any(names_a_path);
    let hint = if teach_the_door {
        format!(" \u{2014} {CARD_ORIGIN_HINT}")
    } else {
        String::new()
    };

    // The origins block teaches its own door — **one invitation, never two**.
    // A capped card's remainder absorbs the teacher rather than stacking a
    // second line beneath it; one branch decides, so stacking is
    // unrepresentable rather than merely avoided.
    if card.origins.len() > CARD_ORIGIN_CAP {
        let more = card.origins.len() - CARD_ORIGIN_CAP;
        // The remainder is where the reader most needs the door — the origins
        // it names are the ones this line just declined to print.
        lines.push(format!(
            "\u{2026} and {} more {}{hint}",
            format_count(more as i64),
            plural(more as i64, "origin")
        ));
    } else if teach_the_door {
        lines.push(CARD_ORIGIN_HINT.to_string());
    }
    // Standings are unbounded — merging keys them on the label, so their
    // count is bounded by the transition vocabulary, not by the history.
    // Gaps stay per-decision and so *can* grow without limit; they take the
    // same explicit-remainder cap the origins section uses, because
    // per-decision and uncapped together is exactly the class this section
    // was rewritten to remove.
    let (standings, gaps): (Vec<&TransitionedLine>, Vec<&TransitionedLine>) = card
        .transitioned
        .iter()
        .partition(|line| matches!(line, TransitionedLine::Standing { .. }));
    for line in standings {
        lines.push(format_transitioned_line(line));
    }
    for line in gaps.iter().take(CARD_GAP_CAP) {
        lines.push(format_transitioned_line(line));
    }
    if gaps.len() > CARD_GAP_CAP {
        let more = gaps.len() - CARD_GAP_CAP;
        lines.push(format!(
            "\u{2026} and {} more {}.",
            format_count(more as i64),
            plural(more as i64, "gap")
        ));
    }
    if let Some(bucket) = &card.indexed_here {
        lines.push(format!(
            "first indexed here: {}",
            format_bucket(bucket.files, bucket.bytes)
        ));
    }
    if let Some(bucket) = &card.untracked {
        // These sources *are* tracked — indexed, present, counted in the sum
        // directly above. What is missing is the record of how they arrived,
        // and the row cannot say why: predating recording is one cause among
        // several indistinguishable ones, so the line names the absence and
        // stops there. Self-explaining gaps, never a guessed cause.
        lines.push(format!(
            "arrival unrecorded: {}",
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

/// The disposition words, one spelling each, per direction: `(retained,
/// relocated)`.
///
/// Registered vocabulary with a single carrier rather than four literals at
/// four call sites. The trail states the **recorded act** and nothing about
/// the origin's present state: an origin's files may be long gone, or sit on
/// a drive Canon can no longer observe, and a line claiming otherwise is
/// unverifiable exactly where it is read most (a retired root). An extraction
/// is read from the origin, an arrival from the destination, and the *same*
/// delivery must not acquire a third wording when a second surface renders
/// it.
const OUTBOUND_DISPOSITION: (&str, &str) = ("copied", "moved");
/// The inbound half of [`OUTBOUND_DISPOSITION`] — see there.
const INBOUND_DISPOSITION: (&str, &str) = ("copied in", "moved in");

/// The disposition parenthetical, or nothing when the row can't say.
///
/// The words differ by direction and come from [`OUTBOUND_DISPOSITION`] /
/// [`INBOUND_DISPOSITION`], never a literal at the call site — but the
/// "append in parens, or omit entirely" mechanics are shared, which is what
/// this function owns.
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

/// One origin line: `arrived from <root>` (single-origin, possibly several
/// merged decisions) or `via apply #N from M origins` (one multi-origin
/// decision) — the registered wording, never a free literal.
///
/// The card's lines all answer one question — *how did what stands here come
/// to stand here* — so an origin reads as an action in the same grammar as
/// its sibling labels (`excluded`, `rearranged`, `first indexed here`), not
/// as a bare prepositional phrase. `MultiOrigin` is deliberately left alone:
/// it names no root, and making it parallel would mean coining a wording for
/// a line nothing asked about.
///
/// An origin root that *contains* the view reads `arrived from elsewhere in
/// <root>`: the content genuinely arrived (its origin sits outside the viewed
/// scope), but a bare `/archive` while standing in `/archive/2020` would read
/// as naming the place you're already in. The root is still named rather than
/// left implicit ("in this root"), because a view can span several roots.
///
/// The line carries a **count** of the decisions behind it, not their ids.
/// The count is the density signal — how much of a relationship this place had
/// with that one — while the ids were doorknobs: fifteen of them, and until
/// `trail crossings` existed, the acts behind an origin line were reachable
/// from nowhere the reader was standing. The transitioned section could always
/// drop its ids on the argument that the acts behind it are the timeline's to
/// hold, directly above; the origins section could not make that claim until
/// there was a door. Now it can. A count of **one** names its decision
/// ([`decision_chip`]) — a list of one is not a list, and hiding the single id
/// sends the reader through a door to learn a number the line already holds.
///
/// The line takes the **door's shape** ([`keyed_entry_lines`]) and returns
/// several lines: path, then the marker where there is one, then the counts.
/// It carried identical facts to a counterpart entry while spelling them
/// differently, and on a retired root the one-line form ran past two hundred
/// characters — ten times over on a real card. **Uniform**: a short line takes
/// the shape too, because a
/// card whose line shape varies with content length scans worse than one that
/// does not.
fn format_origin_line(line: &OriginLine) -> Vec<String> {
    match line {
        OriginLine::FromRoot {
            root_path,
            root_removed,
            retired_book,
            from_within,
            files,
            bytes,
            decision_ids,
            first_at,
            last_at,
        } => {
            let head = if *from_within {
                format!("arrived from elsewhere in {root_path}")
            } else {
                format!("arrived from {root_path}")
            };
            keyed_entry_lines(
                &head,
                &origin_marker(retired_book.as_deref(), *root_removed),
                &format!(
                    "{} \u{00b7} {} \u{00b7} {}",
                    format_bucket(*files, *bytes),
                    decision_chip(decision_ids),
                    format_date_range(*first_at, *last_at)
                ),
                CARD_INDENTS,
            )
        }
        // One line: it names no root, so there is no path to protect from
        // wrapping and no marker to hang beneath one. It already leads with
        // its decision id.
        OriginLine::MultiOrigin {
            decision_id,
            origin_count,
            files,
            bytes,
            at,
        } => {
            vec![format!(
                "via apply #{decision_id} from {} origins: {} \u{00b7} {}",
                format_count(*origin_count),
                format_bucket(*files, *bytes),
                format_date_only(*at)
            )]
        }
    }
}

/// A transitioned line. A **standing** reads `<label>: N files (size)` — a
/// present-tense fact about this place, merged across every decision that
/// produced it, so it carries no id (the acts are the timeline's, directly
/// above). A **gap** keeps the older `<label> here (#N)` shape: it is about
/// one specific decision, and naming that decision is the whole point.
///
/// `label` is the registered transition word when one applies, else the raw
/// command (self-explaining, never guessed).
fn format_transitioned_line(line: &TransitionedLine) -> String {
    match line {
        TransitionedLine::Standing {
            label,
            files,
            bytes,
        } => format!("{label}: {}", format_bucket(*files, *bytes)),
        TransitionedLine::Gap {
            decision_id,
            label,
            files,
            bytes,
        } => format!(
            "{label} here (#{decision_id}): {}",
            format_bucket(*files, *bytes)
        ),
    }
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
#[allow(clippy::too_many_arguments)]
fn print_event(
    event: &TimelineEvent,
    with_date: bool,
    resolved: &ResolvedScope,
    roots: &HashMap<i64, Root>,
    view_root: Option<&str>,
    width: usize,
    act_width: usize,
    placements: &HashMap<i64, Vec<(DecisionExtraction, RowAspect)>>,
    scope_matches: &HashMap<i64, ScopeMatch>,
) {
    match event {
        TimelineEvent::Decision(d) => {
            let time = if with_date {
                format_datetime(d.created_at)
            } else {
                format_time(d.created_at)
            };
            let suffix = |line: &mut String| line.push_str(&decision_suffix(d));
            let cells = event_cells(event, resolved, roots, view_root, placements, scope_matches);
            let act = decision_act(&d.command);
            let lines = placements
                .get(&d.id)
                .map(|rows| aggregate_placement_lines(rows))
                .unwrap_or_default();
            if !lines.is_empty() {
                // Both sides aggregate the same rows the same way, so the
                // counts match; if that ever stops being true, zip drops
                // lines instead of complaining.
                for (placement, cell) in lines.iter().zip(&cells) {
                    let row = &placement.row;
                    let narration = match placement.aspect {
                        RowAspect::Rearrangement => extraction_narration_with_destination(
                            row,
                            &relativize(&row.destination_path, resolved, view_root),
                        ),
                        RowAspect::Arrival => arrival_narration(row, roots),
                        // `Outside` cannot reach here — such rows were dropped
                        // at classification. Rendering it as a plain extraction
                        // with its absolute destination degrades gracefully
                        // rather than panicking on a line the interface has no
                        // way to repair.
                        RowAspect::Extraction | RowAspect::Outside => extraction_narration(row),
                    };
                    let mut line = format!(
                        "#{:<4} {time}  {act:<act_width$}  {cell:<width$}  {}",
                        d.id, narration
                    );
                    suffix(&mut line);
                    println!("{line}");
                }
                return;
            }
            let cell = &cells[0];
            let mut line = format!(
                "#{:<4} {time}  {act:<act_width$}  {cell:<width$}  {}",
                d.id,
                headline(d)
            );
            suffix(&mut line);
            println!("{line}");
        }
        TimelineEvent::Note(n) => {
            let cell = cap_path(
                &scope_cell(event, resolved, roots, view_root, scope_matches),
                SCOPE_CELL_MAX,
            );
            let time = if with_date {
                format_datetime(n.created_at)
            } else {
                format_time(n.created_at)
            };
            // A note carries no act: it is a thought, not a decision. The
            // column is held open rather than closed up, so the scope cells
            // stay aligned down the listing; the `~` is what marks the voice.
            let blank = "";
            println!(
                "      {time}  {blank:<act_width$}  {cell:<width$}  ~ {}",
                n.text
            );
        }
    }
}

/// Long mode's one left rail, matching the width of the `#{id:<4} ` field
/// the column mode opens a decision line with. Every line below a header
/// starts here, and a note's timestamp does too — three different indents on
/// one listing is the ragged edge this mode exists to remove.
const LONG_INDENT: &str = "      ";

/// Print one timeline entry in long mode: several lines instead of several
/// columns.
///
/// **Multi-line, not a wider column.** An uncapped scope column pushes the
/// narration off the right of the screen; a taller entry survives any path
/// length. That is also why the width pass does not run here — there is no
/// column to size, so nothing measured and nothing printed can drift apart.
///
/// Places are **absolute, uncapped and unelided**: root-relative rendering
/// is a scoped-view convenience, and this mode exists to be copied out of.
/// The line shape is `<place>`, then `<narration>`, under a header line
/// carrying the id, the time and the act.
///
/// Long mode changes only how an event renders — never which events are
/// shown, never their order, never the rollups or the card below them.
fn print_long_event(
    event: &TimelineEvent,
    with_date: bool,
    roots: &HashMap<i64, Root>,
    placements: &HashMap<i64, Vec<(DecisionExtraction, RowAspect)>>,
    scope_matches: &HashMap<i64, ScopeMatch>,
) {
    for line in long_event_lines(event, with_date, roots, placements, scope_matches) {
        println!("{line}");
    }
}

/// The lines of one long-mode entry, indentation included. Pure data — the
/// same separation `drew_from_lines` and `composition_card_lines` keep, so
/// the shape is testable without capturing stdout.
fn long_event_lines(
    event: &TimelineEvent,
    with_date: bool,
    roots: &HashMap<i64, Root>,
    placements: &HashMap<i64, Vec<(DecisionExtraction, RowAspect)>>,
    scope_matches: &HashMap<i64, ScopeMatch>,
) -> Vec<String> {
    let mut out = Vec::new();
    match event {
        TimelineEvent::Decision(d) => {
            let time = if with_date {
                format_datetime(d.created_at)
            } else {
                format_time(d.created_at)
            };
            out.push(format!("#{:<4} {time}  {}", d.id, decision_act(&d.command)));

            let lines = placements
                .get(&d.id)
                .map(|rows| aggregate_placement_lines(rows))
                .unwrap_or_default();
            if lines.is_empty() {
                out.push(format!(
                    "{LONG_INDENT}{}",
                    long_decision_place(d, scope_matches)
                ));
                out.push(format!(
                    "{LONG_INDENT}{}{}",
                    headline(d),
                    decision_suffix(d)
                ));
                return out;
            }
            // One place/narration pair per placement line, the same rows the
            // column mode renders — absolute on both sides here, since a
            // relative destination is not something you can paste.
            for placement in &lines {
                let row = &placement.row;
                let (place, narration) = match placement.aspect {
                    RowAspect::Arrival => {
                        (row.destination_path.clone(), arrival_narration(row, roots))
                    }
                    RowAspect::Extraction | RowAspect::Rearrangement | RowAspect::Outside => {
                        (row.drawn_from(), extraction_narration(row))
                    }
                };
                out.push(format!("{LONG_INDENT}{place}"));
                out.push(format!("{LONG_INDENT}{narration}{}", decision_suffix(d)));
            }
        }
        TimelineEvent::Note(n) => {
            let time = if with_date {
                format_datetime(n.created_at)
            } else {
                format_time(n.created_at)
            };
            // No act word: a thought is not an act, and the `~` below is what
            // marks the voice — the same rule the column mode's blank act
            // cell keeps.
            out.push(format!("{LONG_INDENT}{time}"));
            out.push(format!("{LONG_INDENT}{}", note_absolute_path(n, roots)));
            out.push(format!("{LONG_INDENT}~ {}", n.text));
        }
    }
    out
}

/// A decision's place in long mode: the matched scope where one was carried,
/// the display column's first entry otherwise — the same precedence the
/// column mode uses, spelled out rather than abbreviated.
fn long_decision_place(d: &Decision, scope_matches: &HashMap<i64, ScopeMatch>) -> String {
    let (place, others) = match scope_matches.get(&d.id) {
        Some(m) => (m.matched.clone(), m.other_count),
        None => match &d.scope {
            Some(paths) if !paths.is_empty() => (paths[0].clone(), paths.len() - 1),
            _ => return "global".to_string(),
        },
    };
    if others == 0 {
        place
    } else {
        format!(
            "{place}   (+{} other {})",
            others,
            plural(others as i64, "place")
        )
    }
}

/// A note's full location, absolute — long mode's whole point for notes,
/// which the column mode can only show relativized and capped.
fn note_absolute_path(n: &crate::notes::Note, roots: &HashMap<i64, Root>) -> String {
    match roots.get(&n.root_id) {
        Some(root) if n.rel_path.is_empty() => root.path.clone(),
        Some(root) => format!("{}/{}", root.path, n.rel_path),
        None => n.rel_path.clone(),
    }
}

/// The reason-and-status tail a decision's narration carries. One spelling,
/// both render modes — the two shapes differ in layout, never in what a row
/// is allowed to say about itself.
fn decision_suffix(d: &Decision) -> String {
    let mut out = String::new();
    if let Some(reason) = &d.reason {
        out.push_str(&format!(" \u{00b7} \"{reason}\""));
    }
    if d.status != "completed" {
        out.push_str(&format!("  [{}]", d.status));
    }
    out
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
    view_root: Option<&str>,
    placements: &HashMap<i64, Vec<(DecisionExtraction, RowAspect)>>,
    scope_matches: &HashMap<i64, ScopeMatch>,
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
                        cap_path(&relativize(&location, resolved, view_root), SCOPE_CELL_MAX)
                    })
                    .collect();
            }
        }
    }
    vec![cap_path(
        &scope_cell(event, resolved, roots, view_root, scope_matches),
        SCOPE_CELL_MAX,
    )]
}

/// The act a timeline row states, or `None` for a note — a thought is not an
/// act, and the column stays blank rather than borrowing a word.
fn event_act(event: &TimelineEvent) -> Option<&str> {
    match event {
        TimelineEvent::Decision(d) => Some(decision_act(&d.command)),
        TimelineEvent::Note(_) => None,
    }
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
        disposition_suffix(
            row.disposition,
            OUTBOUND_DISPOSITION.0,
            OUTBOUND_DISPOSITION.1
        )
    )
}

/// The arrival aspect's narration: `← N files (size) from ORIGIN (wording)`.
/// The mirror of `extraction_narration` for the inbound direction —
/// [`INBOUND_DISPOSITION`] rather than the outbound's, and the origin carries
/// the removed-root marker when its source root is no longer known to the
/// live index.
fn arrival_narration(row: &DecisionExtraction, roots: &HashMap<i64, Root>) -> String {
    format!(
        "\u{2190} {} from {}{}",
        files_with_size(row.files, row.bytes),
        origin_location(row, roots),
        disposition_suffix(
            row.disposition,
            INBOUND_DISPOSITION.0,
            INBOUND_DISPOSITION.1
        )
    )
}

/// The established removed-root marker (`trail show`'s `drew from:` lines
/// use the same text) — a root the live index no longer knows must not read
/// as a visitable location.
const ROOT_REMOVED_MARKER: &str = " (root removed)";

/// What an origin location's own line says about the root behind it, one
/// spelling for every surface that names an origin.
///
/// The snapshot path stays primary — a root the index no longer knows must
/// not read as a live, visitable location — and a **retired** origin points
/// at its book instead: bound history, not a dead end. Precedence is the
/// book's: a retired root is also a removed one, and `(root removed)` there
/// would answer a question the book already answers better. A live root
/// carries neither.
///
/// Three surfaces render this and must render it identically: `trail show`'s
/// `drew from:` groups, the composition card's origin lines, and
/// `trail crossings`' counterparts. Before this had one carrier they had
/// drifted — one door pointed at the book while its neighbour, naming the
/// same root, said only `(root removed)`.
fn origin_marker(retired_book: Option<&str>, root_removed: bool) -> String {
    match (retired_book, root_removed) {
        (Some(book), _) => format!(" (root retired \u{2014} the book: {book})"),
        (None, true) => ROOT_REMOVED_MARKER.to_string(),
        (None, false) => String::new(),
    }
}

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

/// The location an event happened at, rendered for the scope column.
///
/// For a decision, the place named is the one the operations layer says
/// **matched** this view (`TrailResult.scope_matches`) — the join the query
/// already computed to decide the decision surfaces at all. Nothing is
/// re-derived here: a 31-prefix scan used to be labelled by its *first*
/// recorded prefix, which had nothing to do with the view the reader was
/// standing in. `+N` counts the decision's other recorded places.
///
/// Falls back to the display column's first entry when no match was carried
/// — a global view (which matched nothing by construction) and a decision
/// with no `decision_scopes` rows of its own, surfaced by an extraction row
/// instead. A decision with no recorded scope at all renders "global".
fn scope_cell(
    event: &TimelineEvent,
    resolved: &ResolvedScope,
    roots: &HashMap<i64, Root>,
    view_root: Option<&str>,
    scope_matches: &HashMap<i64, ScopeMatch>,
) -> String {
    match event {
        TimelineEvent::Decision(d) => {
            if let Some(m) = scope_matches.get(&d.id) {
                let matched = relativize(&m.matched, resolved, view_root);
                return with_remainder(matched, m.other_count);
            }
            match &d.scope {
                Some(paths) if !paths.is_empty() => {
                    with_remainder(relativize(&paths[0], resolved, view_root), paths.len() - 1)
                }
                _ => "global".to_string(),
            }
        }
        TimelineEvent::Note(n) => relativize(&note_absolute_path(n, roots), resolved, view_root),
    }
}

/// `place` alone, or `place +N` when the decision names other places too.
fn with_remainder(place: String, other_count: usize) -> String {
    if other_count == 0 {
        place
    } else {
        format!("{place} +{other_count}")
    }
}

/// Render a location for the scope column, in three descending degrees of
/// relativity.
///
/// **View-relative** first, when the view has one prefix ("." for the prefix
/// itself) — the CWD case, unchanged. **Root-relative** next, measured from
/// the single root that contains the whole view (`TrailResult.view_root`),
/// which is what lets a scope *above* or *beside* the viewed folder render as
/// something a reader can hold in one line; the header states that root once,
/// so the leading `/` marks a path as root-relative rather than absolute.
/// **Absolute** last — global views, multi-root views, and any path outside
/// the view's root, where there is no shared frame to measure from and a
/// shortened path would be a lie.
fn relativize(path: &str, resolved: &ResolvedScope, view_root: Option<&str>) -> String {
    if resolved.prefixes.len() == 1 {
        let prefix = &resolved.prefixes[0];
        if path == prefix {
            return ".".to_string();
        }
        if let Some(rel) = crate::core::domain::path_strip_prefix(path, prefix) {
            return rel.to_string();
        }
    }
    if let Some(root) = view_root {
        if path == root {
            return "/".to_string();
        }
        if let Some(rel) = crate::core::domain::path_strip_prefix(path, root) {
            return format!("/{rel}");
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
    use crate::core::domain::decision::Decision;
    use crate::trail::domain::composition::BucketCount;
    use crate::trail::domain::timeline::TimelineEvent;
    use crate::trail::ops::crossings::Counterpart;

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

    // ------------------------------------------------------------------
    // Day rollup (time lens)
    // ------------------------------------------------------------------

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
    // Scope cells + event cells
    // ------------------------------------------------------------------

    #[test]
    fn relativize_against_single_prefix() {
        let scoped = ResolvedScope {
            prefixes: vec!["/photos".to_string()],
            set_aside: Vec::new(),
            parked: Vec::new(),
            pause: Vec::new(),

            from_cwd: true,
            auto_include_archived: false,
        };
        assert_eq!(relativize("/photos", &scoped, None), ".");
        assert_eq!(relativize("/photos/italy", &scoped, None), "italy");
        // Ancestor of the view and unrelated paths stay absolute.
        assert_eq!(relativize("/", &scoped, None), "/");
        assert_eq!(relativize("/other", &scoped, None), "/other");

        let global = ResolvedScope {
            prefixes: Vec::new(),
            set_aside: Vec::new(),
            parked: Vec::new(),
            pause: Vec::new(),

            from_cwd: false,
            auto_include_archived: false,
        };
        assert_eq!(relativize("/photos/italy", &global, None), "/photos/italy");
    }

    /// The scope cell names the place the operations layer says matched —
    /// never element zero of the display column, which is what labelled a
    /// 31-prefix scan by a folder unrelated to the view.
    #[test]
    fn the_scope_cell_names_the_carried_match_not_the_first_recorded_place() {
        let scoped = ResolvedScope {
            prefixes: vec!["/a/foto".to_string()],
            set_aside: Vec::new(),
            parked: Vec::new(),
            pause: Vec::new(),

            from_cwd: true,
            auto_include_archived: false,
        };
        let event = TimelineEvent::Decision(Box::new(mk_decision(
            1,
            Some(vec![
                "/a/admin".to_string(),
                "/a/foto".to_string(),
                "/a/misc".to_string(),
            ]),
        )));
        let matches = HashMap::from([(
            1,
            ScopeMatch {
                matched: "/a/foto".to_string(),
                other_count: 2,
            },
        )]);

        assert_eq!(
            scope_cell(&event, &scoped, &HashMap::new(), Some("/a"), &matches),
            ". +2"
        );
    }

    /// No match carried (a global view, or a decision surfaced by an
    /// extraction row alone): today's behaviour, unchanged.
    #[test]
    fn the_scope_cell_falls_back_to_the_display_column_with_no_match() {
        let global = ResolvedScope {
            prefixes: Vec::new(),
            set_aside: Vec::new(),
            parked: Vec::new(),
            pause: Vec::new(),

            from_cwd: false,
            auto_include_archived: false,
        };
        let event = TimelineEvent::Decision(Box::new(mk_decision(
            1,
            Some(vec!["/a/admin".to_string(), "/a/foto".to_string()]),
        )));
        assert_eq!(
            scope_cell(&event, &global, &HashMap::new(), None, &HashMap::new()),
            "/a/admin +1"
        );
    }

    #[test]
    fn a_decision_with_no_recorded_scope_still_renders_global() {
        let global = ResolvedScope {
            prefixes: Vec::new(),
            set_aside: Vec::new(),
            parked: Vec::new(),
            pause: Vec::new(),

            from_cwd: false,
            auto_include_archived: false,
        };
        let event = TimelineEvent::Decision(Box::new(mk_decision(1, None)));
        assert_eq!(
            scope_cell(&event, &global, &HashMap::new(), None, &HashMap::new()),
            "global"
        );
    }

    /// Three descending frames. View-relative first (unchanged), then
    /// root-relative for a place elsewhere in the view's root, then absolute
    /// where there is no shared frame to measure from.
    #[test]
    fn places_render_root_relative_in_a_single_root_view() {
        let scoped = ResolvedScope {
            prefixes: vec!["/archive/2016".to_string()],
            set_aside: Vec::new(),
            parked: Vec::new(),
            pause: Vec::new(),

            from_cwd: true,
            auto_include_archived: false,
        };
        let root = Some("/archive");
        // View-relative wins: no regression on the `.`-and-below behaviour.
        assert_eq!(relativize("/archive/2016", &scoped, root), ".");
        assert_eq!(relativize("/archive/2016/03", &scoped, root), "03");
        // Elsewhere in the root: measured from the root.
        assert_eq!(relativize("/archive/2020", &scoped, root), "/2020");
        assert_eq!(relativize("/archive", &scoped, root), "/");
        // Outside the root: absolute, because nothing shared measures it.
        assert_eq!(
            relativize("/Volumes/sd/dcim", &scoped, root),
            "/Volumes/sd/dcim"
        );
    }

    #[test]
    fn a_multi_root_view_renders_absolute() {
        let scoped = ResolvedScope {
            prefixes: vec!["/a/x".to_string(), "/b/y".to_string()],
            set_aside: Vec::new(),
            parked: Vec::new(),
            pause: Vec::new(),

            from_cwd: false,
            auto_include_archived: false,
        };
        // `view_root` is None for a multi-root view; every place stays whole.
        assert_eq!(relativize("/a/x", &scoped, None), "/a/x");
        assert_eq!(relativize("/b/y/z", &scoped, None), "/b/y/z");
    }

    #[test]
    fn a_global_view_renders_absolute() {
        let global = ResolvedScope {
            prefixes: Vec::new(),
            set_aside: Vec::new(),
            parked: Vec::new(),
            pause: Vec::new(),

            from_cwd: false,
            auto_include_archived: false,
        };
        assert_eq!(relativize("/a/x", &global, None), "/a/x");
    }

    /// A place under the viewed prefix keeps its view-relative form even
    /// though a root frame is available — the precedence is load-bearing.
    #[test]
    fn a_scope_under_the_viewed_prefix_still_renders_view_relative() {
        let scoped = ResolvedScope {
            prefixes: vec!["/archive/2016".to_string()],
            set_aside: Vec::new(),
            parked: Vec::new(),
            pause: Vec::new(),

            from_cwd: true,
            auto_include_archived: false,
        };
        assert_eq!(
            relativize("/archive/2016/03/raw", &scoped, Some("/archive")),
            "03/raw"
        );
    }

    // The act column — derived, never a coined literal.

    #[test]
    fn the_act_is_the_registered_transition_word_where_one_exists() {
        assert_eq!(decision_act("apply"), "archived");
        assert_eq!(decision_act("exclude_set"), "excluded");
        assert_eq!(decision_act("exclude_duplicates"), "excluded");
        assert_eq!(decision_act("exclude_clear"), "restored");
    }

    /// The fallback arm: the stored identifier, exactly as recorded. The
    /// underscore is accepted residue — coining a word here is `/vision`'s.
    #[test]
    fn an_unregistered_command_renders_its_identifier() {
        assert_eq!(decision_act("scan"), "scan");
        assert_eq!(decision_act("cluster_generate"), "cluster_generate");
        assert_eq!(decision_act("roots_rm"), "roots_rm");
        // A command identifier from a newer Canon: rendered raw, never dropped.
        assert_eq!(decision_act("some_future_command"), "some_future_command");
    }

    /// A note is a thought, not an act — the column stays blank and the `~`
    /// keeps marking the voice.
    #[test]
    fn a_note_line_carries_no_act() {
        let note = TimelineEvent::Note(crate::notes::Note {
            id: 1,
            root_id: 1,
            rel_path: "x".to_string(),
            text: "thought".to_string(),
            created_at: 0,
        });
        assert_eq!(event_act(&note), None);
        let decision = TimelineEvent::Decision(Box::new(mk_decision(1, None)));
        assert_eq!(event_act(&decision), Some("archived"));
    }

    #[test]
    fn event_cells_measures_the_drawn_from_location_for_extraction_lines() {
        // The column width is measured over these cells, so an extraction
        // line's drawn-from location — not the decision's selection scope —
        // must be what comes back, or the narration falls out of alignment.
        let global = ResolvedScope {
            prefixes: Vec::new(),
            set_aside: Vec::new(),
            parked: Vec::new(),
            pause: Vec::new(),

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
        let cells = event_cells(
            &event,
            &global,
            &HashMap::new(),
            None,
            &placements,
            &HashMap::new(),
        );
        assert_eq!(cells, vec!["...mes/old-laptop/photos/2016/italy"]);
        assert!(cells[0].chars().count() <= SCOPE_CELL_MAX);

        // With no extraction rows the selection scope is the cell, as before.
        let cells = event_cells(
            &event,
            &global,
            &HashMap::new(),
            None,
            &HashMap::new(),
            &HashMap::new(),
        );
        assert_eq!(cells, vec!["/short"]);
    }

    #[test]
    fn event_cells_returns_one_cell_per_extraction_row() {
        let global = ResolvedScope {
            prefixes: Vec::new(),
            set_aside: Vec::new(),
            parked: Vec::new(),
            pause: Vec::new(),

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

        let cells = event_cells(
            &event,
            &global,
            &HashMap::new(),
            None,
            &placements,
            &HashMap::new(),
        );
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
            set_aside: Vec::new(),
            parked: Vec::new(),
            pause: Vec::new(),

            from_cwd: true,
            auto_include_archived: false,
        };
        let event = TimelineEvent::Decision(Box::new(mk_decision(1, None)));
        let mut placements = HashMap::new();
        placements.insert(1, vec![(mk_extraction_row(None, None), RowAspect::Arrival)]);

        let cells = event_cells(
            &event,
            &scoped,
            &HashMap::new(),
            None,
            &placements,
            &HashMap::new(),
        );
        assert_eq!(cells, vec!["2016/Italy"]);
    }

    #[test]
    fn event_cells_arrival_at_the_viewed_prefix_itself_renders_dot() {
        let scoped = ResolvedScope {
            prefixes: vec!["/Archive/Media/2016/Italy".to_string()],
            set_aside: Vec::new(),
            parked: Vec::new(),
            pause: Vec::new(),

            from_cwd: true,
            auto_include_archived: false,
        };
        let event = TimelineEvent::Decision(Box::new(mk_decision(1, None)));
        let mut placements = HashMap::new();
        placements.insert(1, vec![(mk_extraction_row(None, None), RowAspect::Arrival)]);

        let cells = event_cells(
            &event,
            &scoped,
            &HashMap::new(),
            None,
            &placements,
            &HashMap::new(),
        );
        assert_eq!(cells, vec!["."]);
    }

    #[test]
    fn event_cells_intra_view_relocation_uses_drawn_from_cell_not_destination() {
        // A rearrangement row (both endpoints inside the view) still renders
        // the extraction-aspect cell — the drawn-from location — never the
        // arrival cell.
        let scoped = ResolvedScope {
            prefixes: vec!["/Volumes/old-laptop".to_string()],
            set_aside: Vec::new(),
            parked: Vec::new(),
            pause: Vec::new(),

            from_cwd: true,
            auto_include_archived: false,
        };
        let event = TimelineEvent::Decision(Box::new(mk_decision(1, None)));
        let mut placements = HashMap::new();
        placements.insert(
            1,
            vec![(mk_extraction_row(None, None), RowAspect::Rearrangement)],
        );

        let cells = event_cells(
            &event,
            &scoped,
            &HashMap::new(),
            None,
            &placements,
            &HashMap::new(),
        );
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
            set_aside: Vec::new(),
            parked: Vec::new(),
            pause: Vec::new(),

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

        let cells = event_cells(
            &event,
            &global,
            &HashMap::new(),
            None,
            &placements,
            &HashMap::new(),
        );
        assert_eq!(cells, vec!["/Volumes/old-laptop/photos/2016"]);
    }

    #[test]
    fn event_cells_measures_every_row_of_a_mixed_origin_decision() {
        // The width pass and the print pass read the same tagged rows, so a
        // mixed-origin decision measures both cells: the rearrangement's
        // drawn-from location and the arrival's destination.
        let scoped = ResolvedScope {
            prefixes: vec!["/Volumes/old-laptop".to_string()],
            set_aside: Vec::new(),
            parked: Vec::new(),
            pause: Vec::new(),

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

        let cells = event_cells(
            &event,
            &scoped,
            &HashMap::new(),
            None,
            &placements,
            &HashMap::new(),
        );
        assert_eq!(
            cells,
            vec!["photos/2016/italy", "/Archive/Media/2016/Italy"]
        );

        // The act column is measured in the same pass, over the same events.
        // A decision contributes one act however many rows it renders — the
        // two columns are sized together so neither can drift from the other.
        assert_eq!(event_act(&event), Some("archived"));
    }

    // ------------------------------------------------------------------
    // Long mode — the pasteable shape
    // ------------------------------------------------------------------

    fn mk_note(rel_path: &str, text: &str) -> crate::notes::Note {
        crate::notes::Note {
            id: 1,
            root_id: 1,
            rel_path: rel_path.to_string(),
            text: text.to_string(),
            created_at: 0,
        }
    }

    /// The mode's whole point: the location arrives whole, no cap, no
    /// ellipsis — something you can paste.
    #[test]
    fn long_mode_renders_the_full_path_unelided() {
        let event = TimelineEvent::Decision(Box::new(mk_decision(
            71,
            Some(vec![
                "/Volumes/backup-drive/Media Archive/old laptop/albums/2007".to_string(),
            ]),
        )));
        let lines = long_event_lines(
            &event,
            true,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
        );
        assert_eq!(
            lines[1],
            "      /Volumes/backup-drive/Media Archive/old laptop/albums/2007"
        );
        assert!(lines[1].chars().count() > SCOPE_CELL_MAX);
        assert!(!lines.iter().any(|l| l.contains('\u{2026}')));
    }

    /// Root-relative rendering is a scoped-view convenience; this mode exists
    /// to be copied out of, so the path stays absolute wherever it is run.
    #[test]
    fn long_mode_renders_absolute_paths_even_in_a_scoped_view() {
        // `long_event_lines` takes no `ResolvedScope` and no `view_root` at
        // all — the absoluteness is structural, not a branch that could be
        // taken the other way by a caller.
        let event = TimelineEvent::Decision(Box::new(mk_decision(
            1,
            Some(vec!["/archive/2016".to_string()]),
        )));
        let matches = HashMap::from([(
            1,
            ScopeMatch {
                matched: "/archive/2016".to_string(),
                other_count: 0,
            },
        )]);
        let lines = long_event_lines(&event, true, &HashMap::new(), &HashMap::new(), &matches);
        assert_eq!(lines[1], "      /archive/2016");
    }

    #[test]
    fn long_mode_works_in_a_global_view() {
        // A decision with no recorded scope at all still names its place.
        let event = TimelineEvent::Decision(Box::new(mk_decision(1, None)));
        let lines = long_event_lines(
            &event,
            true,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
        );
        assert_eq!(lines[1], "      global");
    }

    #[test]
    fn long_mode_gives_a_note_its_full_location() {
        let roots = HashMap::from([(1, mk_root(1, "/Volumes/backup-drive/Media Archive"))]);
        let event = TimelineEvent::Note(mk_note(
            "old laptop/albums/trip_2007_2010",
            "bulk-transfer this",
        ));
        let lines = long_event_lines(&event, true, &roots, &HashMap::new(), &HashMap::new());
        assert_eq!(
            lines[1],
            "      /Volumes/backup-drive/Media Archive/old laptop/albums/trip_2007_2010"
        );
        // A thought, not an act: the `~` marks it and the act line stays bare.
        assert_eq!(lines[2], "      ~ bulk-transfer this");
        assert!(!lines[0].contains("note"));
    }

    /// `+3` in a column becomes words when there is room for words.
    #[test]
    fn long_mode_spells_out_the_remainder() {
        let event = TimelineEvent::Decision(Box::new(mk_decision(1, None)));
        let matches = HashMap::from([(
            1,
            ScopeMatch {
                matched: "/a/foto".to_string(),
                other_count: 30,
            },
        )]);
        let lines = long_event_lines(&event, true, &HashMap::new(), &HashMap::new(), &matches);
        assert_eq!(lines[1], "      /a/foto   (+30 other places)");

        let one = HashMap::from([(
            1,
            ScopeMatch {
                matched: "/a/foto".to_string(),
                other_count: 1,
            },
        )]);
        let lines = long_event_lines(&event, true, &HashMap::new(), &HashMap::new(), &one);
        assert_eq!(lines[1], "      /a/foto   (+1 other place)");
    }

    /// Long mode changes how an event renders, never which events render —
    /// it sees the same placement rows and produces one pair per line.
    #[test]
    fn long_mode_does_not_change_which_events_are_shown() {
        let event = TimelineEvent::Decision(Box::new(mk_decision(42, None)));
        let mut second = mk_extraction_row(None, None);
        second.root_path = "/Volumes/nikon-sd".to_string();
        second.rel_prefix = "dcim".to_string();
        let placements = HashMap::from([(
            42,
            vec![
                (mk_extraction_row(None, None), RowAspect::Extraction),
                (second, RowAspect::Extraction),
            ],
        )]);

        let long = long_event_lines(&event, true, &HashMap::new(), &placements, &HashMap::new());
        let global = ResolvedScope {
            prefixes: Vec::new(),
            set_aside: Vec::new(),
            parked: Vec::new(),
            pause: Vec::new(),

            from_cwd: false,
            auto_include_archived: false,
        };
        let cells = event_cells(
            &event,
            &global,
            &HashMap::new(),
            None,
            &placements,
            &HashMap::new(),
        );
        // Header + one place/narration pair per row the column mode measures.
        assert_eq!(long.len(), 1 + 2 * cells.len());
        assert_eq!(long[1], "      /Volumes/old-laptop/photos/2016/italy");
        assert_eq!(long[3], "      /Volumes/nikon-sd/dcim");
    }

    /// One left rail: every line below a header starts at the same column,
    /// and a note's timestamp starts there too — the ragged edge is exactly
    /// what this mode exists to remove.
    #[test]
    fn long_mode_lines_share_one_left_rail() {
        let roots = HashMap::from([(1, mk_root(1, "/a"))]);
        let decision = TimelineEvent::Decision(Box::new(mk_decision(1, None)));
        let note = TimelineEvent::Note(mk_note("x", "thought"));

        let d = long_event_lines(&decision, true, &roots, &HashMap::new(), &HashMap::new());
        let n = long_event_lines(&note, true, &roots, &HashMap::new(), &HashMap::new());

        // The header's `#{id:<4} ` field is the rail's width: the timestamp
        // starts exactly where every line below it does.
        let head: Vec<char> = d[0].chars().collect();
        assert_eq!(head[LONG_INDENT.len() - 1], ' ');
        assert!(head[LONG_INDENT.len()].is_ascii_digit(), "{:?}", d[0]);
        for line in d[1..].iter().chain(n.iter()) {
            assert!(line.starts_with(LONG_INDENT), "{line:?}");
            assert!(!line.starts_with(&format!("{LONG_INDENT} ")), "{line:?}");
        }
    }

    /// Inside a day group the date is already stated by the group header, so
    /// the entry carries the time alone — the same rule the column mode has.
    #[test]
    fn long_mode_applies_inside_day_groups() {
        let event = TimelineEvent::Decision(Box::new(mk_decision(7, None)));
        let dated = long_event_lines(
            &event,
            true,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
        );
        let grouped = long_event_lines(
            &event,
            false,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
        );
        assert!(dated[0].starts_with("#7"));
        assert!(grouped[0].starts_with("#7"));
        assert!(dated[0].len() > grouped[0].len());
        // Everything below the header is identical in both.
        assert_eq!(dated[1..], grouped[1..]);
    }

    // ------------------------------------------------------------------
    // Extraction aspect narration
    // ------------------------------------------------------------------

    #[test]
    fn extraction_narration_retained_wording() {
        let row = mk_extraction_row(Some(3_900_000_000), Some(OriginDisposition::Retained));
        assert_eq!(
            extraction_narration(&row),
            "\u{2192} 47 files (3.9 GB) to /Archive/Media/2016/Italy (copied)"
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
            "\u{2192} 47 files to /Archive/Media/2016/Italy (copied)"
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
            "\u{2192} 1 file (10 B) to /Archive/Media/2016/Italy (copied)"
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
            "\u{2192} 47 files (1.0 KB) to . (copied)"
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
            "\u{2190} 47 files (3.9 GB) from /Volumes/old-laptop/photos/2016/italy (copied in)"
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
            "\u{2190} 47 files from /Volumes/old-laptop/photos/2016/italy (copied in)"
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
            "\u{2190} 1 file (10 B) from /Volumes/old-laptop/photos/2016/italy (copied in)"
        );
    }

    #[test]
    fn arrival_narration_marks_removed_origin_root() {
        let row = mk_extraction_row(Some(10), Some(OriginDisposition::Retained));
        // The origin's source root is absent from the live roots map.
        let roots: HashMap<i64, Root> = HashMap::new();
        assert_eq!(
            arrival_narration(&row, &roots),
            "\u{2190} 47 files (10 B) from /Volumes/old-laptop/photos/2016/italy (root removed) (copied in)"
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

    // ------------------------------------------------------------------
    // The disposition vocabulary
    // ------------------------------------------------------------------

    /// One spelling per disposition word, per direction — and neither
    /// direction's pair leaks into the other. The consts are the only
    /// source; a fifth literal at a call site would show up here.
    #[test]
    fn the_disposition_words_have_one_spelling_per_direction() {
        let retained = mk_extraction_row(Some(10), Some(OriginDisposition::Retained));
        let relocated = mk_extraction_row(Some(10), Some(OriginDisposition::Relocated));
        let roots = HashMap::from([(1, mk_root(1, "/Volumes/old-laptop"))]);

        assert!(extraction_narration(&retained).ends_with("(copied)"));
        assert!(extraction_narration(&relocated).ends_with("(moved)"));
        assert!(arrival_narration(&retained, &roots).ends_with("(copied in)"));
        assert!(arrival_narration(&relocated, &roots).ends_with("(moved in)"));

        // The inbound pair never reaches an outbound line, nor the reverse:
        // "moved" is a prefix of "moved in", so the check is on the whole
        // parenthetical, not on a substring of the word.
        for line in [
            extraction_narration(&retained),
            extraction_narration(&relocated),
        ] {
            assert!(!line.contains(INBOUND_DISPOSITION.0));
            assert!(!line.contains(INBOUND_DISPOSITION.1));
        }
        for line in [
            arrival_narration(&retained, &roots),
            arrival_narration(&relocated, &roots),
        ] {
            assert!(!line.ends_with(&format!("({})", OUTBOUND_DISPOSITION.0)));
            assert!(!line.ends_with(&format!("({})", OUTBOUND_DISPOSITION.1)));
        }
    }

    /// A row that cannot say how it was delivered says nothing — never a
    /// guess, and distinguishable from every word the vocabulary holds.
    #[test]
    fn a_none_disposition_still_renders_nothing() {
        let unknown = mk_extraction_row(Some(10), None);
        let roots = HashMap::from([(1, mk_root(1, "/Volumes/old-laptop"))]);

        assert_eq!(disposition_suffix(None, "copied", "moved"), "");
        for line in [
            extraction_narration(&unknown),
            arrival_narration(&unknown, &roots),
        ] {
            // The line ends at the location; the only parenthetical left is
            // the size, which `files_with_size` owns.
            assert!(!line.ends_with(')'), "{line}");
            for word in [
                OUTBOUND_DISPOSITION.0,
                OUTBOUND_DISPOSITION.1,
                INBOUND_DISPOSITION.0,
                INBOUND_DISPOSITION.1,
            ] {
                assert!(!line.contains(word), "{line} names {word}");
            }
        }
    }

    // ------------------------------------------------------------------
    // Crossings sections
    // ------------------------------------------------------------------

    fn scope_of(prefixes: &[&str]) -> ResolvedScope {
        ResolvedScope {
            prefixes: prefixes.iter().map(|p| p.to_string()).collect(),
            set_aside: Vec::new(),
            parked: Vec::new(),
            pause: Vec::new(),

            from_cwd: false,
            auto_include_archived: false,
        }
    }

    fn crossings_params_of(origin: Option<&str>, destination: Option<&str>) -> CrossingsParams {
        CrossingsParams {
            prefixes: vec!["/archive".to_string()],
            origin: origin.map(String::from),
            destination: destination.map(String::from),
            // An ordinary invocation, not `--all` — `None` is what the flag
            // means, and a helper that spelled it by default would hand every
            // test the uncapped view nobody asked for.
            limit: Some(DEFAULT_LIMIT),
            machine_output: false,
        }
    }

    fn plain_counterpart(path: &str) -> Counterpart {
        Counterpart {
            path: path.to_string(),
            root_removed: false,
            retired_book: None,
        }
    }

    /// `decisions` is a count, spelled here as consecutive ids so a fixture
    /// asking for one decision gets a nameable one — the singleton chip
    /// renders the id, so a builder handing out none could not exercise it.
    fn counterpart_line(path: &str, files: i64, decisions: usize, at: i64) -> CounterpartLine {
        CounterpartLine {
            counterpart: plain_counterpart(path),
            files,
            bytes: Some(files * 100),
            decision_ids: (1..=decisions as i64).collect(),
            folders: 1,
            first_at: at,
            last_at: at,
        }
    }

    fn covering(mut line: CounterpartLine, folders: usize) -> CounterpartLine {
        line.folders = folders;
        line
    }

    fn bare_section(
        aspect: RowAspect,
        lines: Vec<CounterpartLine>,
        more: usize,
    ) -> CrossingSection {
        CrossingSection {
            aspect,
            files: lines.iter().map(|l| l.files).sum(),
            bytes: Some(lines.iter().map(|l| l.files * 100).sum()),
            counterparty_count: lines.len() + more,
            decision_count: {
                let mut ids: Vec<i64> = lines.iter().flat_map(|l| l.decision_ids.clone()).collect();
                ids.sort_unstable();
                ids.dedup();
                ids.len()
            },
            named: None,
            body: CrossingBody::Counterparts { lines, more },
        }
    }

    /// The door is those rollup lines made expandable, so its headers are
    /// those sentences in form — no new noun is coined for the same fact.
    #[test]
    fn section_headers_are_the_rollup_sentences() {
        let ts = local_ts_on("2026-07-11");
        let arrivals = bare_section(
            RowAspect::Arrival,
            vec![counterpart_line("/vol/sd", 5, 1, ts)],
            9,
        );
        let lines = crossing_section_lines(
            &arrivals,
            &scope_of(&["/archive"]),
            &crossings_params_of(None, None),
        );
        assert_eq!(
            lines[0],
            format_arrival_rollup(&ArrivalRollup {
                files: 5,
                bytes: Some(500),
                origins: 10,
            })
        );

        let extractions = bare_section(
            RowAspect::Extraction,
            vec![counterpart_line("/archive/Media", 5, 1, ts)],
            1,
        );
        let lines = crossing_section_lines(
            &extractions,
            &scope_of(&["/vol/sd"]),
            &crossings_params_of(None, None),
        );
        assert_eq!(
            lines[0],
            format_extraction_rollup(&ExtractionRollup {
                files: 5,
                bytes: Some(500),
                destinations: 2,
            })
        );
    }

    /// A section narrowed by the flag naming its *inside* end still prints an
    /// unnamed header, and must not print the unnarrowed sentence with it:
    /// the reader would meet the same words at the same place carrying
    /// smaller numbers, with nothing to account for them.
    #[test]
    fn a_narrowed_section_header_names_what_narrowed_it() {
        let ts = local_ts_on("2026-07-11");

        // Outbound, narrowed by --origin: its outside end (the destination)
        // is unnamed, so the header is the unnamed shape — but it says where.
        let out = bare_section(
            RowAspect::Extraction,
            vec![counterpart_line("/archive/Media", 2, 1, ts)],
            0,
        );
        let lines = crossing_section_lines(
            &out,
            &scope_of(&["/vol/sd"]),
            &crossings_params_of(Some("/vol/sd/photos"), None),
        );
        assert_eq!(
            lines[0],
            "Archived from /vol/sd/photos: 2 files (200 B) \u{2192} 1 destination."
        );

        // Inbound, narrowed by --destination.
        let inbound = bare_section(
            RowAspect::Arrival,
            vec![counterpart_line("/vol/sd", 2, 1, ts)],
            0,
        );
        let lines = crossing_section_lines(
            &inbound,
            &scope_of(&["/archive"]),
            &crossings_params_of(None, Some("/archive/Media")),
        );
        assert_eq!(
            lines[0],
            "Arrived at /archive/Media: 2 files (200 B) from 1 origin."
        );

        // Unnarrowed, the sentence is the rollup's own, unchanged.
        let lines = crossing_section_lines(
            &inbound,
            &scope_of(&["/archive"]),
            &crossings_params_of(None, None),
        );
        assert_eq!(lines[0], "Arrived here: 2 files (200 B) from 1 origin.");
    }

    /// **Both flags named.** The section's outside end is named, so it takes
    /// the named header — and its inside end is narrowed too, so that header
    /// must not say "here" either. Naming only the outside end would leave
    /// "here" printed beside counts narrowed at both ends, which is the
    /// unaccountable reading in its worst form: two narrowings, neither
    /// visible.
    #[test]
    fn a_section_narrowed_at_both_ends_names_both_of_them() {
        let named = |aspect, path: &str| CrossingSection {
            aspect,
            files: 2,
            bytes: Some(200),
            counterparty_count: 1,
            decision_count: 1,
            named: Some(plain_counterpart(path)),
            body: CrossingBody::Deliveries {
                lines: Vec::new(),
                more: 0,
            },
        };

        let lines = crossing_section_lines(
            &named(RowAspect::Arrival, "/vol/sd"),
            &scope_of(&["/archive"]),
            &crossings_params_of(Some("/vol/sd"), Some("/archive/Raw")),
        );
        assert_eq!(
            lines[0],
            "Arrived at /archive/Raw: 2 files (200 B) from /vol/sd."
        );

        let lines = crossing_section_lines(
            &named(RowAspect::Extraction, "/archive/Raw"),
            &scope_of(&["/vol/sd"]),
            &crossings_params_of(Some("/vol/sd/2019"), Some("/archive/Raw")),
        );
        assert_eq!(
            lines[0],
            "Archived from /vol/sd/2019: 2 files (200 B) \u{2192} /archive/Raw."
        );

        // One flag only: the named header keeps "here", because nothing
        // narrowed the side the reader is standing on.
        let lines = crossing_section_lines(
            &named(RowAspect::Arrival, "/vol/sd"),
            &scope_of(&["/archive"]),
            &crossings_params_of(Some("/vol/sd"), None),
        );
        assert_eq!(lines[0], "Arrived here: 2 files (200 B) from /vol/sd.");
    }

    /// The path is this door's key — it is what the reader copies from one
    /// invocation into the next — so it renders whole. `cap_path` would break
    /// the reach chain at its first hop.
    #[test]
    fn a_counterpart_path_renders_full_and_unelided() {
        let long = "/Volumes/old-backup/archived/2016/photos/italy/second-week/raw-files";
        assert!(long.chars().count() > SCOPE_CELL_MAX);
        let ts = local_ts_on("2026-07-11");
        let section = bare_section(
            RowAspect::Arrival,
            vec![counterpart_line(long, 5, 1, ts)],
            0,
        );
        let lines = crossing_section_lines(
            &section,
            &scope_of(&["/archive"]),
            &crossings_params_of(None, None),
        );
        assert!(lines.iter().any(|l| l.trim() == long), "{lines:?}");
        assert!(!lines
            .iter()
            .any(|l| l.contains('\u{2026}') && l.contains("Volumes")));

        // And in a named section's header.
        let named = CrossingSection {
            aspect: RowAspect::Arrival,
            files: 5,
            bytes: Some(500),
            counterparty_count: 1,
            decision_count: 1,
            named: Some(plain_counterpart(long)),
            body: CrossingBody::Deliveries {
                lines: Vec::new(),
                more: 0,
            },
        };
        let lines = crossing_section_lines(
            &named,
            &scope_of(&["/archive"]),
            &crossings_params_of(Some(long), None),
        );
        assert!(lines[0].ends_with(&format!("from {long}.")), "{}", lines[0]);
    }

    #[test]
    fn the_listing_caps_with_an_explicit_remainder() {
        let ts = local_ts_on("2026-07-11");
        let section = bare_section(
            RowAspect::Arrival,
            vec![counterpart_line("/vol/sd", 5, 1, ts)],
            8,
        );
        let lines = crossing_section_lines(
            &section,
            &scope_of(&["/archive"]),
            &crossings_params_of(None, None),
        );
        assert_eq!(lines.last().unwrap().trim(), "\u{2026} and 8 more origins.");

        // Per delivery's places, too — the same cap and discipline the
        // `drew from:` block applies to the identical shape.
        let places: Vec<CrossingPlace> = (0..8)
            .map(|i| CrossingPlace {
                origin: format!("/vol/sd/dir{i}"),
                destination: "/archive/Media".to_string(),
            })
            .collect();
        let named = CrossingSection {
            aspect: RowAspect::Arrival,
            files: 8,
            bytes: Some(80),
            counterparty_count: 1,
            decision_count: 1,
            named: Some(plain_counterpart("/vol/sd")),
            body: CrossingBody::Deliveries {
                lines: vec![CrossingDelivery {
                    decision_id: 48,
                    at: ts,
                    files: 8,
                    bytes: Some(80),
                    disposition: Some(OriginDisposition::Relocated),
                    reason: None,
                    places,
                }],
                more: 0,
            },
        };
        let lines = crossing_section_lines(
            &named,
            &scope_of(&["/archive"]),
            &crossings_params_of(Some("/vol/sd"), None),
        );
        assert!(
            lines
                .iter()
                .any(|l| l.trim() == "\u{2026} and 3 more places"),
            "{lines:?}"
        );
    }

    /// `--all` uncaps the places beneath a delivery, exactly as it uncaps the
    /// listing of entries above them.
    ///
    /// The remainder line names places the reader cannot otherwise reach, and
    /// an unreachable remainder is the defect this whole surface exists to
    /// answer one level up — so the cap and the flag that opens it are pinned
    /// together, in one test, against the same data.
    #[test]
    fn all_uncaps_the_places_beneath_a_delivery() {
        let ts = local_ts_on("2026-07-11");
        let places: Vec<CrossingPlace> = (0..8)
            .map(|i| CrossingPlace {
                origin: format!("/vol/sd/dir{i}"),
                destination: "/archive/Media".to_string(),
            })
            .collect();
        let section = CrossingSection {
            aspect: RowAspect::Arrival,
            files: 8,
            bytes: Some(80),
            counterparty_count: 1,
            decision_count: 1,
            named: Some(plain_counterpart("/vol/sd")),
            body: CrossingBody::Deliveries {
                lines: vec![CrossingDelivery {
                    decision_id: 48,
                    at: ts,
                    files: 8,
                    bytes: Some(80),
                    disposition: Some(OriginDisposition::Relocated),
                    reason: None,
                    places,
                }],
                more: 0,
            },
        };
        let scope = scope_of(&["/archive"]);

        // The default invocation caps, and says so.
        let capped = crossing_section_lines(
            &section,
            &scope,
            &crossings_params_of(Some("/vol/sd"), None),
        );
        assert!(
            capped.iter().any(|l| l.contains("more places")),
            "{capped:?}"
        );
        assert_eq!(
            capped.iter().filter(|l| l.contains(" \u{2192} ")).count(),
            DREW_FROM_DIR_CAP,
            "{capped:?}"
        );

        // `--all` is `limit: None`, and it must leave no remainder anywhere.
        let uncapped = crossing_section_lines(
            &section,
            &scope,
            &CrossingsParams {
                limit: None,
                ..crossings_params_of(Some("/vol/sd"), None)
            },
        );
        assert!(
            !uncapped.iter().any(|l| l.contains("more places")),
            "{uncapped:?}"
        );
        assert_eq!(
            uncapped.iter().filter(|l| l.contains(" \u{2192} ")).count(),
            8,
            "{uncapped:?}"
        );
    }

    /// A counterpart entry's marker and its counts both hang off the path
    /// above them, so they indent alike — a one-column difference reads as
    /// structure that is not there.
    #[test]
    fn a_counterparts_continuation_lines_align_under_its_path() {
        let ts = local_ts_on("2026-07-11");
        let book = "/archive/books/2026-08-11-backup-archived";
        let mut line = counterpart_line("/Volumes/gone", 5, 1, ts);
        line.counterpart.root_removed = true;
        line.counterpart.retired_book = Some(book.to_string());

        let lines = counterpart_lines(&line);
        assert_eq!(lines[0], "  /Volumes/gone");
        assert!(lines[1].contains(book), "{lines:?}");

        let indent = |l: &String| l.len() - l.trim_start().len();
        assert_eq!(indent(&lines[1]), indent(&lines[2]), "{lines:?}");
    }

    /// The coarsening must be visible where it happened and silent where it
    /// did not: a grouped entry says how many finer places it stands for, and
    /// a leaf entry says nothing, because the path above it *is* the place.
    #[test]
    fn a_coverage_count_renders_only_past_one_folder() {
        let ts = local_ts_on("2026-07-11");

        let grouped = counterpart_lines(&covering(
            counterpart_line("/archive/Media/2016/03", 262, 2, ts),
            44,
        ));
        assert!(
            grouped.last().unwrap().contains("44 folders"),
            "{grouped:?}"
        );

        let leaf = counterpart_lines(&counterpart_line("/archive/Media/2016/03/08", 262, 2, ts));
        assert!(
            !leaf.last().unwrap().contains("folder"),
            "a leaf entry states no coverage: {leaf:?}"
        );

        // And the singular is never reached by the count itself, since one
        // folder is exactly the case that renders nothing.
        let one = counterpart_lines(&covering(counterpart_line("/archive/Docs", 3, 1, ts), 1));
        assert!(!one.last().unwrap().contains("folder"), "{one:?}");
    }

    /// A count of exactly one names the decision; two or more stay a count.
    /// One spelling, and this is the door's site of it.
    #[test]
    fn a_singleton_decision_count_names_its_id_at_an_entry() {
        let ts = local_ts_on("2026-07-11");

        assert_eq!(decision_chip(&[57]), "decision #57");
        assert_eq!(decision_chip(&[57, 58]), "2 decisions");

        let mut line = counterpart_line("/archive/Docs", 3, 1, ts);
        line.decision_ids = vec![57];
        let rendered = counterpart_lines(&line);
        assert!(
            rendered.last().unwrap().contains("decision #57"),
            "{rendered:?}"
        );
        // The singleton's date is one date, never a degenerate range.
        assert!(
            !rendered.last().unwrap().contains('\u{2013}'),
            "{rendered:?}"
        );

        let plural = counterpart_lines(&counterpart_line("/archive/Media", 9, 3, ts));
        assert!(plural.last().unwrap().contains("3 decisions"), "{plural:?}");
        assert!(!plural.last().unwrap().contains('#'), "{plural:?}");
    }

    // ------------------------------------------------------------------
    // The reconciliation line
    // ------------------------------------------------------------------

    fn reconciliation(
        standing: i64,
        delivered: i64,
        standing_decisions: usize,
        delivered_decisions: usize,
    ) -> Reconciliation {
        Reconciliation {
            standing,
            delivered,
            standing_decisions,
            delivered_decisions,
        }
    }

    /// The five shapes the line takes, composed from two independent clauses.
    #[test]
    fn the_reconciliation_line_composes_five_shapes() {
        assert_eq!(
            reconciliation_line(&reconciliation(229, 229, 3, 3)),
            "Standing here: 229 files stand; 229 were delivered."
        );
        assert_eq!(
            reconciliation_line(&reconciliation(8_151, 8_199, 15, 15)),
            "Standing here: 8,151 of the 8,199 files delivered."
        );
        assert_eq!(
            reconciliation_line(&reconciliation(8_151, 8_199, 15, 17)),
            "Standing here: 8,151 of the 8,199 files delivered \u{2014} 15 decisions stand; \
             17 delivered."
        );
        assert_eq!(
            reconciliation_line(&reconciliation(229, 229, 3, 4)),
            "Standing here: 229 files stand; 229 were delivered \u{2014} 3 decisions stand; \
             4 delivered."
        );
        assert_eq!(
            reconciliation_line(&reconciliation(8_300, 8_199, 15, 15)),
            "Standing here: 8,300 files stand; 8,199 were delivered."
        );
    }

    /// **Equal counts are not a claim about the same files.** Deliver 229
    /// here and one elsewhere, lose one of the 229, and let a scan observe
    /// the stray moving in with its stamp intact: the counts match and the
    /// sets do not. So the equality branch states two counts and never
    /// asserts identity between them — the same doctrine that keeps "of the"
    /// off the standing-beyond-delivered branch, at the branch it had not
    /// reached.
    #[test]
    fn equal_counts_never_claim_the_same_files() {
        let line = reconciliation_line(&reconciliation(229, 229, 3, 3));
        assert!(!line.contains("all "), "{line}");
        assert!(!line.contains("still stand"), "{line}");
        assert!(!line.contains("of the"), "{line}");
        assert_eq!(line, "Standing here: 229 files stand; 229 were delivered.");
    }

    /// **Red-smoke**: containment fails in both directions — a decision stamp
    /// survives a scan-observed move into the view — so where standing
    /// exceeds delivered the line must drop the "of the" construction rather
    /// than state an arithmetic impossibility from the one line designed
    /// never to guess.
    #[test]
    fn standing_beyond_delivered_drops_the_of_the_construction() {
        let line = reconciliation_line(&reconciliation(8_300, 8_199, 17, 15));
        assert!(!line.contains("of the"), "{line}");
        assert!(line.contains("8,300 files"), "{line}");
        assert!(line.contains("8,199 were delivered"), "{line}");
        // The decisions clause never had one to drop, so it reads the same
        // whichever way that axis runs.
        assert!(line.contains("17 decisions stand; 15 delivered"), "{line}");
    }

    /// Equality still spares the reader the subtraction that made
    /// `229 of the 229` read badly — the two counts sit side by side and
    /// match on sight.
    #[test]
    fn equality_reads_without_arithmetic() {
        let line = reconciliation_line(&reconciliation(229, 229, 3, 3));
        assert!(
            line.contains("229 files stand; 229 were delivered"),
            "{line}"
        );
        assert!(!line.contains("229 of the 229"), "{line}");
    }

    /// Suppressing on equality stays rejected: an absent line cannot be told
    /// from "nothing was lost", nor from "there is no card at all".
    #[test]
    fn the_line_is_present_on_equality() {
        assert!(!reconciliation_line(&reconciliation(229, 229, 3, 3)).is_empty());
        // Down to the degenerate single file, which still reads as a sentence.
        assert_eq!(
            reconciliation_line(&reconciliation(1, 1, 1, 1)),
            "Standing here: 1 file stands; 1 was delivered."
        );
    }

    /// Number agreement at every boundary the line has — noun **and** verb.
    /// A count and a verb that disagree beside it ("1 were delivered") undo
    /// the care the rest of the sentence takes, and the singular is reachable
    /// on both clauses.
    #[test]
    fn the_reconciliation_clauses_agree_in_number() {
        let decisions = reconciliation_line(&reconciliation(1, 1, 1, 2));
        assert!(
            decisions.contains("1 decision stands; 2 delivered"),
            "{decisions}"
        );

        // The case that was wrong: a single delivered file beside a plural
        // standing count.
        let plain = reconciliation_line(&reconciliation(2, 1, 1, 1));
        assert_eq!(plain, "Standing here: 2 files stand; 1 was delivered.");
        assert!(!plain.contains("1 were"), "{plain}");

        // The singular on the standing side, which the shared arm reaches
        // only at 1-and-1: standing >= delivered is what enters this arm, so
        // a singular standing count forces a singular delivered one and the
        // cross-verb case above is the only place the two can differ.
        assert_eq!(
            reconciliation_line(&reconciliation(1, 1, 1, 1)),
            "Standing here: 1 file stands; 1 was delivered."
        );
    }

    /// The decisions clause carries no pronoun, because two of the three
    /// files-clause forms end on the **delivered** count — a "them" there
    /// would say these decisions stand behind the delivered files while
    /// naming a different delivered figure in the same breath.
    #[test]
    fn the_decisions_clause_names_no_pronoun() {
        for line in [
            reconciliation_line(&reconciliation(229, 229, 3, 4)),
            reconciliation_line(&reconciliation(8_300, 8_199, 17, 15)),
            reconciliation_line(&reconciliation(8_151, 8_199, 15, 17)),
        ] {
            assert!(!line.contains("them"), "{line}");
            assert!(line.contains("decisions stand;"), "{line}");
        }
    }

    /// A named section reads in the inbound voice; the counterpart's marker
    /// is `drew_from_lines`' own, and the path stays whole on its own line.
    #[test]
    fn a_retired_counterpart_renders_the_book_pointer_in_drew_froms_wording() {
        let book = "/archive/books/2026-08-11-backup-archived";
        let section = CrossingSection {
            aspect: RowAspect::Arrival,
            files: 5,
            bytes: Some(500),
            counterparty_count: 1,
            decision_count: 1,
            named: Some(Counterpart {
                path: "/Volumes/gone".to_string(),
                root_removed: true,
                retired_book: Some(book.to_string()),
            }),
            body: CrossingBody::Deliveries {
                lines: Vec::new(),
                more: 0,
            },
        };
        let lines = crossing_section_lines(
            &section,
            &scope_of(&["/archive"]),
            &crossings_params_of(Some("/Volumes/gone"), None),
        );
        assert_eq!(lines[0], "Arrived here: 5 files (500 B) from /Volumes/gone");
        assert_eq!(
            lines[1].trim(),
            format!("(root retired \u{2014} the book: {book}).")
        );
        assert!(!lines.iter().any(|l| l.contains("root removed")));
    }

    /// Each end renders against its own anchor — the named counterpart for
    /// the named side, the view for the other.
    #[test]
    fn a_delivery_measures_each_end_from_its_own_anchor() {
        let ts = local_ts_on("2026-08-02");
        let section = CrossingSection {
            aspect: RowAspect::Arrival,
            files: 6,
            bytes: Some(60),
            counterparty_count: 1,
            decision_count: 1,
            named: Some(plain_counterpart("/Volumes/gone")),
            body: CrossingBody::Deliveries {
                lines: vec![CrossingDelivery {
                    decision_id: 48,
                    at: ts,
                    files: 6,
                    bytes: Some(60),
                    disposition: Some(OriginDisposition::Relocated),
                    reason: Some("italy trip".to_string()),
                    places: vec![CrossingPlace {
                        origin: "/Volumes/gone/Photos/2016".to_string(),
                        destination: "/archive/Media/2016".to_string(),
                    }],
                }],
                more: 0,
            },
        };
        let lines = crossing_section_lines(
            &section,
            &scope_of(&["/archive"]),
            &crossings_params_of(Some("/Volumes/gone"), None),
        );
        assert!(lines[1].starts_with("  #48 "), "{}", lines[1]);
        // Inbound voice, and the disposition word is the registered one.
        assert!(lines[1].ends_with("moved in"), "{}", lines[1]);
        assert_eq!(lines[2].trim(), "Photos/2016  \u{2192} Media/2016");
        assert_eq!(lines[3].trim(), "\"italy trip\"");
    }

    /// A view that only rearranged must say so. "Nothing crossed" alone reads
    /// as "nothing ever happened here", which is the opposite of the truth
    /// about a heavily curated archive.
    #[test]
    fn nothing_crossed_names_the_rearrangement() {
        let bare = crossings_params_of(None, None);
        let line = nothing_crossed_line(
            &NothingCrossed::Rearranged {
                files: 47,
                bytes: Some(3_900_000_000),
            },
            &scope_of(&["/archive"]),
            &bare,
        );
        assert_eq!(
            line,
            "Nothing crossed this boundary. 47 files (3.9 GB) were rearranged within it."
        );

        let empty = nothing_crossed_line(&NothingCrossed::Nothing, &scope_of(&["/archive"]), &bare);
        assert_eq!(empty, "Nothing has crossed this boundary.");
    }

    /// A named counterpart lives on a `CrossingSection`, and empty sections
    /// are omitted — so with nothing matched, the place the reader asked
    /// about had no way to reach the output at all. The answer named nothing,
    /// while a path Canon has *never* heard of gets a statement naming it: the
    /// near miss was less informative than the total one.
    ///
    /// Live shape: `--origin` pointed at a retired book's own directory, which
    /// sits inside the archive root and is known (its files are indexed) but
    /// is no delivery's endpoint.
    #[test]
    fn a_no_crossing_answer_names_the_counterpart_it_was_asked_about() {
        let book = "/archive/retired/archived-2026-08-08";
        let scoped = scope_of(&["/archive"]);

        let line = nothing_crossed_line(
            &NothingCrossed::Nothing,
            &scoped,
            &crossings_params_of(Some(book), None),
        );
        assert_eq!(
            line,
            format!("Nothing has crossed between here and {book}.")
        );

        // A named destination reads the same way — the ask is what is named,
        // not which flag named it.
        let destination = nothing_crossed_line(
            &NothingCrossed::Nothing,
            &scoped,
            &crossings_params_of(None, Some(book)),
        );
        assert_eq!(destination, line);

        // The rearrangement voice names it too, or the fix would recreate the
        // same silence one variant over.
        let rearranged = nothing_crossed_line(
            &NothingCrossed::Rearranged {
                files: 9,
                bytes: Some(900),
            },
            &scoped,
            &crossings_params_of(Some(book), None),
        );
        assert_eq!(
            rearranged,
            format!(
                "Nothing crossed between here and {book}. \
                 9 files (900 B) were rearranged within this boundary."
            )
        );

        // Two different counterparts are both named; the same path twice is
        // one place, not a repetition.
        let both = nothing_crossed_line(
            &NothingCrossed::Nothing,
            &scoped,
            &crossings_params_of(Some("/vol/sd"), Some(book)),
        );
        assert_eq!(
            both,
            format!("Nothing has crossed between here and /vol/sd or {book}.")
        );
        assert_eq!(
            asked_about(&crossings_params_of(Some(book), Some(book))),
            Some(book.to_string())
        );

        // A global view has no "here" — it borrowed the named counterpart as
        // its boundary, so that place is the subject rather than a second end.
        let mut global_params = crossings_params_of(Some(book), None);
        global_params.prefixes = Vec::new();
        let global = nothing_crossed_line(&NothingCrossed::Nothing, &scope_of(&[]), &global_params);
        assert_eq!(global, format!("No recorded crossing at {book}."));

        // The fourth cell of the 2x2: global *and* rearranged. The carve-out
        // belongs to both variants, or whichever misses it says "here" where
        // a borrowed boundary has none.
        let global_rearranged = nothing_crossed_line(
            &NothingCrossed::Rearranged {
                files: 47,
                bytes: Some(4_700),
            },
            &scope_of(&[]),
            &global_params,
        );
        assert!(!global_rearranged.contains("here"), "{global_rearranged}");
        assert_eq!(
            global_rearranged,
            format!("No recorded crossing at {book}. 47 files (4.7 KB) were rearranged within it.")
        );
    }

    /// "within it" must not reach for the place named just before it. The
    /// rearranged set is measured against the **view's** boundary, not the
    /// named counterpart's, so a scoped view names the boundary rather than
    /// leaving a pronoun to bind to the nearer noun.
    #[test]
    fn a_named_rearrangement_says_which_boundary_it_was_within() {
        let line = nothing_crossed_line(
            &NothingCrossed::Rearranged {
                files: 9,
                bytes: Some(900),
            },
            &scope_of(&["/archive"]),
            &crossings_params_of(Some("/archive/A"), None),
        );
        assert_eq!(
            line,
            "Nothing crossed between here and /archive/A. \
             9 files (900 B) were rearranged within this boundary."
        );
        assert!(!line.contains("within it"), "{line}");
    }

    // ------------------------------------------------------------------
    // Rollup footer composition
    // ------------------------------------------------------------------

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
        assert!(origin_text(&OriginLine::MultiOrigin {
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
    // Shared wording substrate
    // ------------------------------------------------------------------

    #[test]
    fn plural_and_count_of_agree() {
        assert_eq!(plural(1, "origin"), "origin");
        assert_eq!(plural(0, "origin"), "origins");
        assert_eq!(plural(2, "origin"), "origins");
        assert_eq!(count_of(1, "destination"), "1 destination");
        assert_eq!(count_of(1_251, "destination"), "1,251 destinations");
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
            retired_book: None,
            from_within: false,
            files,
            bytes: files * 100,
            decision_ids,
            first_at,
            last_at,
        }
    }

    /// [`mk_from_root`] for a removed origin root whose story was bound into
    /// a book.
    fn mk_retired_from_root(root_path: &str, book: &str, ts: i64) -> OriginLine {
        match mk_from_root(root_path, true, 47, vec![12], ts, ts) {
            OriginLine::FromRoot {
                root_path,
                root_removed,
                from_within,
                files,
                bytes,
                decision_ids,
                first_at,
                last_at,
                ..
            } => OriginLine::FromRoot {
                root_path,
                root_removed,
                retired_book: Some(book.to_string()),
                from_within,
                files,
                bytes,
                decision_ids,
                first_at,
                last_at,
            },
            other => other,
        }
    }

    /// An origin entry's lines joined, for the assertions that are about its
    /// *content* rather than its shape. The shape has its own pins below.
    fn origin_text(line: &OriginLine) -> String {
        format_origin_line(line).join("\n")
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

    /// The card's origin entry in the door's shape: the path ends its own
    /// line, the counts hang beneath it. Uniform — this is a short live-root
    /// line and it takes the shape too, because a card whose lines change
    /// shape with their length scans worse than one that does not.
    #[test]
    fn the_cards_origin_lines_take_the_doors_shape() {
        let ts = local_ts_on("2024-01-05");
        let line = mk_from_root("/Volumes/old-laptop", false, 47, vec![12], ts, ts);
        assert_eq!(
            format_origin_line(&line),
            vec![
                "arrived from /Volumes/old-laptop".to_string(),
                "    47 files (4.7 KB) \u{b7} decision #12 \u{b7} 2024-01-05".to_string(),
            ]
        );
    }

    #[test]
    fn format_origin_line_from_within_says_elsewhere_in_the_root() {
        let ts = local_ts_on("2026-05-12");
        let line = OriginLine::FromRoot {
            root_path: "/archive".to_string(),
            root_removed: false,
            retired_book: None,
            from_within: true,
            files: 47,
            bytes: 3_900_000_000,
            decision_ids: vec![42],
            first_at: ts,
            last_at: ts,
        };
        assert_eq!(
            format_origin_line(&line),
            vec![
                "arrived from elsewhere in /archive".to_string(),
                "    47 files (3.9 GB) \u{b7} decision #42 \u{b7} 2026-05-12".to_string(),
            ]
        );
    }

    /// The alignment rule at its second site: the marker and the counts both
    /// hang off the path above them, so they indent alike — on the card
    /// exactly as on the door, since the card's lines are printed under
    /// `CARD_LINE_PREFIX` and land in the same column.
    #[test]
    fn the_card_and_door_continuations_align_alike() {
        let ts = local_ts_on("2026-07-11");
        let book = "/archive/books/2026-08-11-backup-archived";

        let card = format_origin_line(&mk_retired_from_root("/Volumes/gone", book, ts));
        assert_eq!(card.len(), 3, "path, marker, counts: {card:?}");
        let indent = |l: &String| l.len() - l.trim_start().len();
        assert_eq!(indent(&card[1]), indent(&card[2]), "{card:?}");

        let mut entry = counterpart_line("/Volumes/gone", 5, 1, ts);
        entry.counterpart.root_removed = true;
        entry.counterpart.retired_book = Some(book.to_string());
        let door = counterpart_lines(&entry);

        // Measured **through the prefix the printer actually applies**, not a
        // hand-written stand-in for it: the claim is about the column on
        // screen, so a change to that prefix has to break this.
        let printed = |line: &String| format!("{CARD_LINE_PREFIX}{line}");
        assert_eq!(
            indent(&printed(&card[1])),
            indent(&door[1]),
            "{card:?} {door:?}"
        );
        assert_eq!(indent(&printed(&card[0])), indent(&door[0]));
    }

    /// The cap counts origins, not the lines they emit: an entry is never
    /// truncated mid-shape.
    #[test]
    fn card_origin_cap_counts_origins_not_lines() {
        let ts = local_ts_on("2026-07-11");
        let origins: Vec<OriginLine> = (0..12)
            .map(|i| mk_from_root(&format!("/vol{i}"), false, 1, vec![i], ts, ts))
            .collect();
        let lines = composition_card_lines(&mk_card(origins, Vec::new(), None, None));
        // Ten origins at two lines each, then one remainder line.
        assert_eq!(lines.len(), 21, "{lines:?}");
        assert_eq!(
            lines
                .iter()
                .filter(|l| l.starts_with("arrived from"))
                .count(),
            10
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
            retired_book: None,
            from_within: true,
            files: 1,
            bytes: 100,
            decision_ids: vec![42],
            first_at: ts,
            last_at: ts,
        };
        let lines = format_origin_line(&line);
        assert_eq!(lines[0], "arrived from elsewhere in /archive");
        assert_eq!(lines[1], format!("    {}", ROOT_REMOVED_MARKER.trim()));
    }

    #[test]
    fn format_origin_line_from_root_merged_decisions_show_the_count_and_date_range() {
        let first = local_ts_on("2024-01-05");
        let last = local_ts_on("2024-02-04");
        let line = mk_from_root("/Volumes/old-laptop", false, 15, vec![1, 2], first, last);
        let text = origin_text(&line);
        assert!(text.contains("2 decisions"), "{text}");
        assert!(text.contains("2024-01-05 \u{2013} 2024-02-04"), "{text}");
    }

    /// The doorknobs become a handle. The count stays — it is the density
    /// signal, how much of a relationship this place had with that one — but
    /// no id appears, because the acts behind the line now have a door.
    #[test]
    fn an_origin_line_carries_a_decision_count_not_a_list() {
        let ts = local_ts_on("2024-01-05");
        let many: Vec<i64> = (1..=15).collect();
        let text = origin_text(&mk_from_root("/vol/sd", false, 8_151, many, ts, ts));
        assert!(text.contains("15 decisions"), "{text}");
        assert!(!text.contains('#'), "{text}");
    }

    /// A **list of one is not a list**, so the count-over-list rule above has
    /// nothing to say here: the card's third site of the singleton chip names
    /// the decision, which is the id `trail show` takes.
    #[test]
    fn one_decision_names_its_id_on_the_card() {
        let ts = local_ts_on("2024-01-05");
        let text = origin_text(&mk_from_root("/vol/sd", false, 1, vec![7], ts, ts));
        assert!(text.contains("decision #7"), "{text}");
        assert!(!text.contains("1 decision "), "{text}");
    }

    #[test]
    fn the_date_range_is_retained() {
        let first = local_ts_on("2026-08-02");
        let last = local_ts_on("2026-08-09");
        let text = origin_text(&mk_from_root("/vol/sd", false, 5, vec![1, 2], first, last));
        assert!(text.ends_with("2026-08-02 \u{2013} 2026-08-09"), "{text}");
    }

    /// `MultiOrigin` names no root and already carries a single id; making it
    /// parallel would mean coining a wording for a line nothing asked about.
    #[test]
    fn a_multi_origin_line_is_unchanged() {
        let line = OriginLine::MultiOrigin {
            decision_id: 42,
            origin_count: 3,
            files: 5,
            bytes: 500,
            at: local_ts_on("2026-05-12"),
        };
        assert_eq!(
            format_origin_line(&line),
            vec!["via apply #42 from 3 origins: 5 files (500 B) \u{b7} 2026-05-12".to_string()]
        );
    }

    /// `MultiOrigin` stays **one line** through the shape change: it names no
    /// root, so there is no path to protect and no marker to hang beneath one.
    #[test]
    fn multi_origin_stays_one_line() {
        let line = OriginLine::MultiOrigin {
            decision_id: 42,
            origin_count: 3,
            files: 5,
            bytes: 500,
            at: local_ts_on("2026-05-12"),
        };
        assert_eq!(format_origin_line(&line).len(), 1);
    }

    #[test]
    fn format_origin_line_from_root_marks_removed_root() {
        let ts = local_ts_on("2024-01-05");
        let line = mk_from_root("/Volumes/gone", true, 1, vec![1], ts, ts);
        let lines = format_origin_line(&line);
        assert_eq!(lines[0], "arrived from /Volumes/gone");
        assert_eq!(lines[1], format!("    {}", ROOT_REMOVED_MARKER.trim()));
    }

    /// The card's grammar, over both variants: an origin line answers *how
    /// did this come to stand here* in the same voice as its sibling action
    /// labels, rather than as a bare prepositional phrase.
    #[test]
    fn an_origin_line_reads_arrived_from() {
        let ts = local_ts_on("2024-01-05");
        assert_eq!(
            format_origin_line(&mk_from_root("/vol/sd", false, 1, vec![1], ts, ts))[0],
            "arrived from /vol/sd"
        );

        let within = match mk_from_root("/archive", false, 1, vec![1], ts, ts) {
            OriginLine::FromRoot {
                root_path,
                root_removed,
                retired_book,
                files,
                bytes,
                decision_ids,
                first_at,
                last_at,
                ..
            } => OriginLine::FromRoot {
                root_path,
                root_removed,
                retired_book,
                from_within: true,
                files,
                bytes,
                decision_ids,
                first_at,
                last_at,
            },
            other => other,
        };
        assert_eq!(
            format_origin_line(&within)[0],
            "arrived from elsewhere in /archive"
        );
    }

    /// The marker is byte-identical to the one `drew_from_lines` renders for
    /// the same book. One door pointing at the book while its neighbour,
    /// naming the same root, says only `(root removed)` is the drift these
    /// two surfaces are here to stay out of; a second spelling reintroduces
    /// it silently.
    #[test]
    fn a_retired_origin_root_carries_the_book_pointer_in_drew_froms_wording() {
        let ts = local_ts_on("2024-01-05");
        let book = "/archive/books/2026-08-11-backup-archived";
        let card_line = format_origin_line(&mk_retired_from_root("/Volumes/gone", book, ts));

        let drew = drew_from_lines(&[ShowExtraction {
            location: "/Volumes/gone".to_string(),
            root_removed: true,
            retired_book: Some(book.to_string()),
            files: 1,
            bytes: None,
            directories: Vec::new(),
        }]);

        let marker = format!(" (root retired \u{2014} the book: {book})");
        // The card hangs the marker on its own line, so the leading space is
        // layout rather than text; the marker itself is the same string,
        // because both surfaces reach it through `origin_marker`.
        assert_eq!(
            card_line[1],
            format!("    {}", marker.trim()),
            "{card_line:?}"
        );
        assert!(drew[0].contains(&marker), "{}", drew[0]);
        // The book wins outright: a retired root is also a removed one, and
        // saying both would answer with the worse of the two.
        assert!(
            !origin_text(&mk_retired_from_root("/Volumes/gone", book, ts))
                .contains(ROOT_REMOVED_MARKER.trim()),
            "{card_line:?}"
        );
    }

    #[test]
    fn a_plainly_removed_origin_root_keeps_the_root_removed_marker() {
        let ts = local_ts_on("2024-01-05");
        let line = mk_from_root("/Volumes/gone", true, 1, vec![1], ts, ts);
        let text = origin_text(&line);
        assert!(text.contains(ROOT_REMOVED_MARKER.trim()), "{text}");
        assert!(!text.contains("the book:"), "{text}");
    }

    #[test]
    fn a_live_origin_root_carries_no_marker() {
        let ts = local_ts_on("2024-01-05");
        let line = mk_from_root("/vol/sd", false, 1, vec![1], ts, ts);
        let text = origin_text(&line);
        assert!(!text.contains(ROOT_REMOVED_MARKER.trim()), "{text}");
        assert!(!text.contains("root retired"), "{text}");
        // A live root has no marker line at all: path, then counts.
        assert_eq!(format_origin_line(&line).len(), 2);
    }

    /// The label names the absence and stops. The cause is unknowable from
    /// the row — predating recording is one of several indistinguishable
    /// reasons — and these sources are not untracked at all: they are
    /// indexed, present, and counted in the header directly above.
    #[test]
    fn an_unrecorded_arrival_states_no_cause() {
        let card = CompositionCard {
            files: 3,
            bytes: 300,
            origins: Vec::new(),
            transitioned: vec![TransitionedLine::Standing {
                label: "excluded".to_string(),
                files: 1,
                bytes: 100,
            }],
            indexed_here: None,
            untracked: Some(BucketCount {
                files: 2,
                bytes: 200,
            }),
        };
        let lines = composition_card_lines(&card);
        let last = lines.last().unwrap();
        assert_eq!(last, "arrival unrecorded: 2 files (200 B)");
        for cause in ["predates", "recording", "untracked"] {
            assert!(!last.contains(cause), "{last} names {cause}");
        }
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
            origin_text(&line),
            "via apply #42 from 3 origins: 12 files (1.2 KB) \u{b7} 2024-01-05"
        );
    }

    /// A standing is a present-tense fact: the word, the counts, no id.
    #[test]
    fn format_transitioned_line_standing_carries_no_id() {
        let line = TransitionedLine::Standing {
            label: "excluded".to_string(),
            files: 4,
            bytes: 400,
        };
        assert_eq!(format_transitioned_line(&line), "excluded: 4 files (400 B)");
    }

    /// A gap is about one decision, and says which.
    #[test]
    fn format_transitioned_line_gap_keeps_its_id() {
        let line = TransitionedLine::Gap {
            decision_id: 30,
            label: "transition unrecorded".to_string(),
            files: 4,
            bytes: 400,
        };
        assert_eq!(
            format_transitioned_line(&line),
            "transition unrecorded here (#30): 4 files (400 B)"
        );
    }

    fn mk_card(
        origins: Vec<OriginLine>,
        transitioned: Vec<TransitionedLine>,
        indexed_here: Option<crate::trail::domain::composition::BucketCount>,
        untracked: Option<crate::trail::domain::composition::BucketCount>,
    ) -> CompositionCard {
        let files = origins.iter().map(|o| o.files()).sum::<i64>()
            + transitioned.iter().map(|t| t.bucket().files).sum::<i64>()
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
            vec![TransitionedLine::Standing {
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
        // The one origin takes two lines (path, counts) and the origins block
        // teaches its door; the siblings are one line each and unchanged.
        assert_eq!(lines.len(), 6);
        assert!(lines[0].starts_with("arrived from /a"));
        assert!(lines[1].trim_start().starts_with("5 files"));
        assert_eq!(lines[2], CARD_ORIGIN_HINT);
        assert!(lines[3].starts_with("excluded:"));
        assert!(lines[4].starts_with("first indexed here"));
        assert!(lines[5].starts_with("arrival unrecorded"));
    }

    #[test]
    fn composition_card_lines_caps_origins_with_explicit_remainder() {
        let origins: Vec<OriginLine> = (0..12)
            .map(|i| mk_from_root(&format!("/vol{i}"), false, 1, vec![i], 0, 0))
            .collect();
        let card = mk_card(origins, Vec::new(), None, None);
        let lines = composition_card_lines(&card);
        // 10 origins at two lines each + one remainder line.
        assert_eq!(lines.len(), 21);
        assert!(
            lines[20].starts_with("\u{2026} and 2 more origins"),
            "{}",
            lines[20]
        );
    }

    /// **One invitation, never two.** A capped card's remainder absorbs the
    /// teacher; an uncapped card still teaches, because that is exactly where
    /// the handles are and where nothing used to say so.
    #[test]
    fn a_capped_card_renders_one_invitation_not_two() {
        let origins: Vec<OriginLine> = (0..12)
            .map(|i| mk_from_root(&format!("/vol{i}"), false, 1, vec![i], 0, 0))
            .collect();
        let capped = composition_card_lines(&mk_card(origins, Vec::new(), None, None));
        assert_eq!(
            capped
                .iter()
                .filter(|l| l.contains("canon trail crossings"))
                .count(),
            1,
            "{capped:?}"
        );
        let remainder = capped.last().unwrap();
        assert!(
            remainder.starts_with("\u{2026} and 2 more origins"),
            "{remainder}"
        );
        assert!(remainder.ends_with(CARD_ORIGIN_HINT), "{remainder}");
    }

    /// The card's origin lines are door handles, and the remainder line fires
    /// only on a capped card — so without this a card showing all its origins
    /// teaches nothing, exactly where the handles are.
    #[test]
    fn an_uncapped_card_still_teaches_its_origin_door() {
        let few: Vec<OriginLine> = (0..2)
            .map(|i| mk_from_root(&format!("/vol{i}"), false, 1, vec![i], 0, 0))
            .collect();
        let lines = composition_card_lines(&mk_card(few, Vec::new(), None, None));
        assert_eq!(
            lines
                .iter()
                .filter(|l| l.contains("canon trail crossings"))
                .count(),
            1,
            "{lines:?}"
        );
        // It names the payoff, not the gesture: nothing in a flag's name says what
        // is behind it, so the line says it.
        assert!(CARD_ORIGIN_HINT.contains("to list the folders behind an origin"));

        // A card with no origins has no handles, so it teaches nothing.
        let originless = composition_card_lines(&mk_card(
            Vec::new(),
            vec![TransitionedLine::Standing {
                label: "excluded".to_string(),
                files: 1,
                bytes: 100,
            }],
            None,
            None,
        ));
        for line in &originless {
            assert!(!line.contains("canon trail crossings"), "{line}");
        }
    }

    /// `MultiOrigin` names no root, so a card carrying only multi-origin
    /// lines puts no path on screen for `--origin` to take. The invitation
    /// must not print there — the rule the rollup hint's placement rests on,
    /// at the site that would otherwise break it.
    #[test]
    fn a_card_whose_origins_name_no_path_does_not_teach_the_origin_door() {
        let card = mk_card(
            vec![OriginLine::MultiOrigin {
                decision_id: 42,
                origin_count: 3,
                files: 5,
                bytes: 500,
                at: local_ts_on("2026-05-12"),
            }],
            Vec::new(),
            None,
            None,
        );
        let lines = composition_card_lines(&card);
        assert!(!lines.iter().any(|l| l.contains("<path>")), "{lines:?}");
        for line in &lines {
            assert!(!line.contains("canon trail crossings"), "{line}");
        }

        // **The capped card**, which is the branch the rule condemns most
        // directly: the only origins the remainder line is about are the ones
        // it just declined to print, so a `<path>` there is the least
        // substitutable of all. Eleven multi-origin lines fill the window and
        // the remainder must stay bare.
        let capped = mk_card(
            (0..(CARD_ORIGIN_CAP + 1) as i64)
                .map(|i| OriginLine::MultiOrigin {
                    decision_id: i,
                    origin_count: 3,
                    files: 5,
                    bytes: 500,
                    at: local_ts_on("2026-05-12"),
                })
                .collect(),
            Vec::new(),
            None,
            None,
        );
        let capped = composition_card_lines(&capped);
        assert!(
            capped
                .last()
                .unwrap()
                .starts_with("\u{2026} and 1 more origin"),
            "{:?}",
            capped.last()
        );
        for line in &capped {
            assert!(!line.contains("<path>"), "{line}");
            assert!(!line.contains("canon trail crossings"), "{line}");
        }

        // And the gate reads the **shown** origins, not all of them: a
        // `FromRoot` beyond the cap puts no path on screen, so it cannot make
        // the invitation substitutable. Only a window-blind gate teaches here.
        let mut beyond: Vec<OriginLine> = (0..CARD_ORIGIN_CAP as i64)
            .map(|i| OriginLine::MultiOrigin {
                decision_id: i,
                origin_count: 3,
                files: 5,
                bytes: 500,
                at: local_ts_on("2026-05-12"),
            })
            .collect();
        beyond.push(mk_from_root("/vol/sd", false, 1, vec![99], 0, 0));
        for line in composition_card_lines(&mk_card(beyond, Vec::new(), None, None)) {
            assert!(
                !line.contains("canon trail crossings"),
                "the only path is beyond the cap: {line}"
            );
        }

        // One `FromRoot` beside it is enough — there is now a path to take.
        let mixed = mk_card(
            vec![
                OriginLine::MultiOrigin {
                    decision_id: 42,
                    origin_count: 3,
                    files: 5,
                    bytes: 500,
                    at: local_ts_on("2026-05-12"),
                },
                mk_from_root("/vol/sd", false, 1, vec![7], 0, 0),
            ],
            Vec::new(),
            None,
            None,
        );
        assert_eq!(
            composition_card_lines(&mixed)
                .iter()
                .filter(|l| l.contains("canon trail crossings"))
                .count(),
            1
        );
    }

    /// The rollup block with any combination of its three lines present.
    fn rollup_block(extraction: bool, arrival: bool, rearrangement: bool) -> Vec<String> {
        rollup_block_lines(
            extraction.then_some(&ExtractionRollup {
                files: 5,
                bytes: Some(500),
                destinations: 2,
            }),
            arrival.then_some(&ArrivalRollup {
                files: 3,
                bytes: Some(300),
                origins: 1,
            }),
            rearrangement.then_some(&RearrangementRollup {
                files: 2,
                bytes: Some(200),
            }),
        )
    }

    /// **Red-smoke against today's ordering.** The hint sits beneath the two
    /// *crossing* rollups and above `Rearranged here`: it teaches the door,
    /// the door itemizes crossings, and the rearrangement rollup is the one
    /// line it never expands. Printed beneath the whole block — where it was
    /// — "these totals" points last at the total the door has nothing to say
    /// about.
    ///
    /// Asserted on the printed order, since the defect is positional and a
    /// const alone cannot show it.
    #[test]
    fn the_hint_sits_above_the_rearrangement_rollup() {
        let block = rollup_block(true, true, true);
        let hint = block.iter().position(|l| l == CROSSINGS_HINT).unwrap();
        let rearranged = block
            .iter()
            .position(|l| l.starts_with("Rearranged here"))
            .unwrap();
        assert!(hint < rearranged, "{block:?}");
        assert!(
            block[hint - 1].starts_with("Arrived here"),
            "the hint hangs off the crossing rollups: {block:?}"
        );
    }

    /// A view whose only rollup is the rearrangement goes untaught: the door
    /// answers "nothing crossed" there and itemizes nothing, so the
    /// invitation would promise places it cannot list. That joins the global
    /// and time-lens views the door already leaves untaught.
    #[test]
    fn a_rearrangement_only_view_is_not_taught_the_door() {
        let block = rollup_block(false, false, true);
        assert_eq!(block.len(), 1, "{block:?}");
        assert!(!block[0].contains("canon trail crossings"), "{block:?}");
    }

    fn crossings_result(sections: Vec<CrossingSection>) -> CrossingsResult {
        CrossingsResult {
            sections,
            nothing_crossed: None,
            reconciliation: None,
            decisions: Vec::new(),
            extractions_all: HashMap::new(),
        }
    }

    /// The bare view's entries are handles — the rendered path is exactly the
    /// argument the flags take — and nothing announced it. Taught once for
    /// the whole view, never once per entry.
    #[test]
    fn the_bare_view_teaches_its_drill_down_once() {
        let ts = local_ts_on("2026-07-11");
        let result = crossings_result(vec![
            bare_section(
                RowAspect::Extraction,
                vec![
                    counterpart_line("/archive/Media/2016/03", 262, 2, ts),
                    counterpart_line("/archive/Docs", 3, 1, ts),
                ],
                0,
            ),
            bare_section(
                RowAspect::Arrival,
                vec![counterpart_line("/Volumes/sd", 5, 1, ts)],
                0,
            ),
        ]);
        // Two sections, three entries, one invitation — and because both
        // directions rendered, it names both flags.
        assert_eq!(
            drill_down_hint(&result, &crossings_params_of(None, None)),
            Some(format!(
                "`canon trail crossings --origin <path>` or `--destination <path>` \
                 {CROSSINGS_DRILL_DOWN_PAYOFF}"
            ))
        );
    }

    /// **The hint can only name a flag the entries on screen feed.** It is
    /// derived from the rendered sections, not written down: an extraction
    /// section lists destinations, so `--destination` opens them; an arrival
    /// section lists origins, so `--origin` does.
    ///
    /// The defect this removes: a source-side view lists destinations while
    /// the hint led with `--origin`, so substituting a listed path into it
    /// narrowed the *other* end, matched nothing, and answered "nothing has
    /// crossed between here and <that path>" — a dead end that reads as a
    /// finding.
    #[test]
    fn the_drill_down_hint_names_the_flag_its_entries_feed() {
        let ts = local_ts_on("2026-07-11");
        let bare = crossings_params_of(None, None);
        let entry = |path: &str| counterpart_line(path, 5, 1, ts);

        let outbound = crossings_result(vec![bare_section(
            RowAspect::Extraction,
            vec![entry("/archive/Media/2016/03")],
            0,
        )]);
        let hint = drill_down_hint(&outbound, &bare).unwrap();
        assert!(hint.contains("--destination <path>"), "{hint}");
        assert!(!hint.contains("--origin"), "{hint}");

        let inbound = crossings_result(vec![bare_section(
            RowAspect::Arrival,
            vec![entry("/Volumes/sd")],
            0,
        )]);
        let hint = drill_down_hint(&inbound, &bare).unwrap();
        assert!(hint.contains("--origin <path>"), "{hint}");
        assert!(!hint.contains("--destination"), "{hint}");
    }

    /// **The hint's promise is pinned from both sides**, because a claim
    /// about a different rendering, held only as prose, goes stale silently.
    ///
    /// Two assertions, each catching a different drift:
    /// * the promised feature is **absent** from a bare entry — move it onto
    ///   the listing and this fails, rather than leaving the hint offering
    ///   what is already on screen;
    /// * it is **present** where an entry is named, and the promise names it
    ///   — reword away from what is delivered and this fails.
    #[test]
    fn the_drill_down_hint_promises_only_what_naming_an_entry_adds() {
        let ts = local_ts_on("2026-07-11");
        // Deliberately **not** keyed on an indent: the drift this half exists
        // to catch would most naturally arrive at the entry's *own*
        // continuation indent, which is what `keyed_entry_lines` already
        // hands out. Keying on eight spaces would make the assertion true by
        // the indent width alone and blind to the shape it names.
        let pairs = |lines: &[String]| lines.iter().filter(|l| l.contains('\u{2192}')).count();

        // A bare entry: a path, its counts, and a folder *count* — never the
        // folders themselves. The whole shape is a head, an optional marker and a
        // counts line, so a listing of any kind, at any indent, shows up as
        // one line more than that.
        let bare_entry = counterpart_lines(&covering(
            counterpart_line("/archive/Media/2016/03", 262, 2, ts),
            9,
        ));
        assert_eq!(
            bare_entry.len(),
            2,
            "a bare entry is exactly a head and a counts line; anything \
             enumerating places adds one, at whatever indent: {bare_entry:?}"
        );
        assert_eq!(
            pairs(&bare_entry),
            0,
            "the bare listing already shows what the hint promises: {bare_entry:?}"
        );

        // The marked shape too, so the exact count above cannot be satisfied
        // by a drift that happens to replace the marker line.
        let mut marked = covering(counterpart_line("/Volumes/gone", 5, 1, ts), 4);
        marked.counterpart.root_removed = true;
        let marked = counterpart_lines(&marked);
        assert_eq!(marked.len(), 3, "head, marker, counts: {marked:?}");
        assert_eq!(pairs(&marked), 0, "{marked:?}");

        // The same content with the entry named: the folders, listed.
        let named = CrossingSection {
            aspect: RowAspect::Extraction,
            files: 262,
            bytes: Some(26_200),
            counterparty_count: 1,
            decision_count: 1,
            named: Some(plain_counterpart("/archive/Media/2016/03")),
            body: CrossingBody::Deliveries {
                lines: Vec::new(),
                more: 0,
            },
        };
        let delivery = CrossingDelivery {
            decision_id: 23,
            at: ts,
            files: 262,
            bytes: Some(26_200),
            disposition: None,
            reason: None,
            places: vec![
                CrossingPlace {
                    origin: "/vol/sd/03/01".to_string(),
                    destination: "/archive/Media/2016/03/01".to_string(),
                },
                CrossingPlace {
                    origin: "/vol/sd/03/02".to_string(),
                    destination: "/archive/Media/2016/03/02".to_string(),
                },
            ],
        };
        let named_entry = delivery_lines(
            &delivery,
            &named,
            &scope_of(&["/vol/sd"]),
            &crossings_params_of(None, Some("/archive/Media/2016/03")),
        );
        assert_eq!(pairs(&named_entry), 2, "{named_entry:?}");

        // And both promises name that feature, so neither can drift apart
        // from it silently. The card's hint is the drill-down's twin — its
        // origin lines name their decision too, so it makes the same promise
        // about the same thing and takes the same pin.
        for promise in [CROSSINGS_DRILL_DOWN_PAYOFF, CARD_ORIGIN_HINT] {
            assert!(promise.contains("folders"), "{promise}");
            assert!(
                !promise.contains("decisions"),
                "the entries already name their decision: {promise}"
            );
        }

        // The card's own rendering, held to the same claim its hint makes —
        // the string check above is only half a pin, and covering one site's
        // structure while asserting about two is the shape this whole test
        // exists to refuse.
        let card_origin =
            format_origin_line(&mk_from_root("/vol/sd", false, 262, vec![12], ts, ts));
        assert_eq!(
            card_origin.len(),
            2,
            "a card origin is a head and a counts line; a folder listing would \
             add one: {card_origin:?}"
        );
        assert_eq!(pairs(&card_origin), 0, "{card_origin:?}");
    }

    /// A named view has already stepped through the door this line describes.
    #[test]
    fn a_named_view_does_not_teach_the_drill_down() {
        let ts = local_ts_on("2026-07-11");
        let named = CrossingSection {
            aspect: RowAspect::Arrival,
            files: 5,
            bytes: Some(500),
            counterparty_count: 1,
            decision_count: 1,
            named: Some(plain_counterpart("/Volumes/sd")),
            body: CrossingBody::Deliveries {
                lines: vec![CrossingDelivery {
                    decision_id: 7,
                    at: ts,
                    files: 5,
                    bytes: Some(500),
                    disposition: None,
                    reason: None,
                    places: Vec::new(),
                }],
                more: 0,
            },
        };
        assert_eq!(
            drill_down_hint(
                &crossings_result(vec![named]),
                &crossings_params_of(Some("/Volumes/sd"), None)
            ),
            None
        );

        // And a view with nothing to drill into teaches nothing either.
        assert_eq!(
            drill_down_hint(
                &crossings_result(Vec::new()),
                &crossings_params_of(None, None)
            ),
            None
        );
    }

    /// A named counterpart opens one direction and leaves the other listing
    /// counterparts, so a view can hold a named section and a bare one at
    /// once. The bare half's entries are still handles nothing else
    /// announces, so it is still taught — the test is per section, and only a
    /// view whose every section is named goes untaught.
    #[test]
    fn a_view_with_one_named_and_one_bare_section_still_teaches_the_bare_one() {
        let ts = local_ts_on("2026-07-11");
        let named = CrossingSection {
            aspect: RowAspect::Extraction,
            files: 5,
            bytes: Some(500),
            counterparty_count: 1,
            decision_count: 1,
            named: Some(plain_counterpart("/archive")),
            body: CrossingBody::Deliveries {
                lines: Vec::new(),
                more: 0,
            },
        };
        let bare = bare_section(
            RowAspect::Arrival,
            vec![counterpart_line("/Volumes/sd", 5, 1, ts)],
            0,
        );
        let hint = drill_down_hint(
            &crossings_result(vec![named, bare]),
            &crossings_params_of(None, Some("/archive")),
        )
        .expect("the bare arrival section still has entries to open");
        // The bare section is the arrival one, so it teaches `--origin` and
        // never the flag that already named its sibling.
        assert!(hint.contains("--origin <path>"), "{hint}");
        assert!(!hint.contains("--destination"), "{hint}");
    }

    #[test]
    fn jsonl_carries_no_hints() {
        let ts = local_ts_on("2026-07-11");
        let result = crossings_result(vec![bare_section(
            RowAspect::Extraction,
            vec![counterpart_line("/archive/Docs", 3, 1, ts)],
            0,
        )]);
        let mut machine = crossings_params_of(None, None);
        machine.machine_output = true;
        assert_eq!(drill_down_hint(&result, &machine), None);
    }

    /// Every invitation this surface prints says **what running it yields**,
    /// never only the gesture — a backquoted literal command, then `to <plain
    /// verb> <payoff>`.
    #[test]
    fn every_invitation_names_its_payoff() {
        let ts = local_ts_on("2026-07-11");
        let drill_down = drill_down_hint(
            &crossings_result(vec![bare_section(
                RowAspect::Extraction,
                vec![counterpart_line("/archive/Docs", 3, 1, ts)],
                0,
            )]),
            &crossings_params_of(None, None),
        )
        .unwrap();
        for hint in [CROSSINGS_HINT, CARD_ORIGIN_HINT, &drill_down] {
            assert!(hint.contains("`canon trail crossings"), "{hint}");
            assert!(hint.contains(" to list "), "{hint}");
        }
    }

    /// One teaching line beneath the rollup block, not one per rollup — and
    /// the hint names the command the block's own lines invite.
    #[test]
    fn the_rollup_block_teaches_the_door_once() {
        assert!(CROSSINGS_HINT.contains("canon trail crossings"));
        assert_eq!(CROSSINGS_HINT.matches("canon trail crossings").count(), 1);
        assert_eq!(
            rollup_block(true, true, true)
                .iter()
                .filter(|l| *l == CROSSINGS_HINT)
                .count(),
            1
        );

        // It is not part of any rollup sentence, so three rollups cannot
        // print it three times.
        for line in [
            format_extraction_rollup(&ExtractionRollup {
                files: 1,
                bytes: None,
                destinations: 1,
            }),
            format_arrival_rollup(&ArrivalRollup {
                files: 1,
                bytes: None,
                origins: 1,
            }),
            format_rearrangement_rollup(&RearrangementRollup {
                files: 1,
                bytes: None,
            }),
        ] {
            assert!(!line.contains("crossings"), "{line}");
        }
    }

    /// The door does not teach its own name inside its own output.
    #[test]
    fn a_crossings_section_never_teaches_the_door() {
        let ts = local_ts_on("2026-07-11");
        let section = bare_section(
            RowAspect::Arrival,
            vec![counterpart_line("/vol/sd", 5, 1, ts)],
            3,
        );
        for line in crossing_section_lines(
            &section,
            &scope_of(&["/archive"]),
            &crossings_params_of(None, None),
        ) {
            assert!(!line.contains("canon trail crossings"), "{line}");
        }
    }

    /// An empty section is omitted rather than printed empty — a section
    /// exists only when `rollup_parts` found rows for it, so this is a
    /// property of the computation, asserted at the surface that would show
    /// the defect.
    #[test]
    fn a_section_with_nothing_in_it_does_not_print() {
        let result = CrossingsResult {
            sections: Vec::new(),
            nothing_crossed: Some(NothingCrossed::Nothing),
            reconciliation: None,
            decisions: Vec::new(),
            extractions_all: HashMap::new(),
        };
        assert!(result.sections.is_empty());
        assert!(nothing_crossed_line(
            result.nothing_crossed.as_ref().unwrap(),
            &scope_of(&["/archive"]),
            &crossings_params_of(None, None)
        )
        .contains("Nothing"));
    }

    /// Gaps stay per-decision, so they are the one part of the transitioned
    /// section whose length tracks the history — they take the same
    /// explicit-remainder cap the origins section uses, never a silent
    /// truncation.
    #[test]
    fn gap_lines_cap_with_an_explicit_remainder() {
        let gaps: Vec<TransitionedLine> = (0..8)
            .map(|i| TransitionedLine::Gap {
                decision_id: i,
                label: "transition unrecorded".to_string(),
                files: 1,
                bytes: 100,
            })
            .collect();
        let card = mk_card(Vec::new(), gaps, None, None);
        let lines = composition_card_lines(&card);
        // 5 gap lines + one remainder line.
        assert_eq!(lines.len(), 6);
        assert_eq!(lines[5], "\u{2026} and 3 more gaps.");
    }

    /// Standings are merged, so their count is bounded by the vocabulary,
    /// not by the history — the gap cap must not reach them.
    #[test]
    fn standings_are_not_capped_by_the_gap_cap() {
        let standings: Vec<TransitionedLine> = (0..8)
            .map(|i| TransitionedLine::Standing {
                label: format!("label{i}"),
                files: 1,
                bytes: 100,
            })
            .collect();
        let card = mk_card(Vec::new(), standings, None, None);
        let lines = composition_card_lines(&card);
        assert_eq!(lines.len(), 8);
        assert!(!lines.iter().any(|l| l.contains("more")));
    }

    #[test]
    fn composition_card_lines_no_remainder_when_under_cap() {
        let origins: Vec<OriginLine> = (0..3)
            .map(|i| mk_from_root(&format!("/vol{i}"), false, 1, vec![i], 0, 0))
            .collect();
        let card = mk_card(origins, Vec::new(), None, None);
        let lines = composition_card_lines(&card);
        // Three origins at two lines each, then the hint — and nothing left
        // over to name.
        assert_eq!(lines.len(), 7);
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

    // ------------------------------------------------------------------
    // drew_from_lines — trail show detail
    // ------------------------------------------------------------------

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

    /// **The pause is stated independent of the cap.** It is a property of
    /// the scope being read at, so a `--limit 1` view of a closed root still
    /// says the door is closed — the reader never has to spot the
    /// `roots_suspend` row among the listing, and there may not be one in
    /// view to spot.
    #[test]
    fn the_pause_is_stated_independent_of_the_cap() {
        use crate::core::domain::root::ParkedRoot;

        let mut parked = ResolvedScope {
            prefixes: vec!["/mnt/d1/photos".to_string()],
            set_aside: Vec::new(),
            parked: Vec::new(),
            pause: vec![ParkedRoot {
                root_id: 7,
                root_path: "/mnt/d1".to_string(),
            }],
            from_cwd: true,
            auto_include_archived: false,
        };

        assert_eq!(
            trail_header("/mnt/d1/photos", None, &parked),
            "Decision trail: /mnt/d1/photos — /mnt/d1 suspended · canon roots unsuspend path:/mnt/d1"
        );
        // The time lens carries it too — both labels, one line.
        assert!(trail_header("/mnt/d1/photos", Some("today"), &parked)
            .contains("— today — /mnt/d1 suspended · "));

        parked.pause = Vec::new();
        assert_eq!(
            trail_header("/mnt/d1/photos", None, &parked),
            "Decision trail: /mnt/d1/photos"
        );
    }
}
