//! Trail's CLI stratum: parse args, call the ops layer, dispatch to the
//! render/jsonl strata.
//!
//! Two lenses over one timeline: the scope lens ("what did I do here?",
//! newest first) and the time lens ("what did I do today?", day-grouped
//! story). Notes interleave as the thinking between the actions, visually
//! distinct — a thought must never read as an act.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Result};
use chrono::Local;

use crate::core::domain::root::Root;
use crate::core::repo::{self, Db};
use crate::trail::domain::composition::{card_applies, ViewShape};
use crate::trail::domain::timeline::{parse_when, WhenValue};
use crate::trail::jsonl::{print_crossings_jsonl, print_jsonl, JsonRetiredScopeEvent};
use crate::trail::ops;
use crate::trail::ops::compute::{TrailParams, DEFAULT_LIMIT};
use crate::trail::ops::show::{PointerRelocation, ScopeRelation, ShowScope};
use crate::trail::render::{
    drew_from_lines, format_counts, format_date_only, format_datetime, print_crossings, print_human,
};

pub struct TrailArgs {
    pub paths: Vec<PathBuf>,
    pub global: bool,
    pub today: bool,
    pub since: Option<String>,
    pub on: Option<String>,
    pub limit: Option<usize>,
    pub all: bool,
    pub no_notes: bool,
    /// `-l`/`--long`: multi-line entries carrying each place's full absolute
    /// path, uncapped and unelided — the pasteable mode.
    pub long: bool,
    pub jsonl: bool,
}

/// How a trail invocation ended, for the interface to turn into an exit
/// code. `PlaceUnknown` is not an error: the question was well-formed and
/// answered — the answer is that Canon has never known this place — so it
/// carries a non-zero exit without an `Error:` prefix.
#[must_use]
#[derive(Debug, PartialEq, Eq)]
pub enum TrailExit {
    Reported,
    PlaceUnknown,
}

pub fn run(db: &mut Db, args: TrailArgs) -> Result<TrailExit> {
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
    let resolved = match open_scope(db, &args.paths, args.global, args.jsonl, &all_roots)? {
        ScopeDoor::Resolved(resolved) => resolved,
        ScopeDoor::Answered(exit) => return Ok(exit),
    };

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
    let result = crate::trail::ops::compute::compute_trail(db.conn(), &params)?;

    // Gated before computing, not after: a global/time-lens/--jsonl run then
    // does the DB work of the trail query only, not the card's as well. The
    // rule itself is `trail::domain::composition::card_applies`.
    let card = if card_applies(ViewShape {
        machine_output: args.jsonl,
        global: resolved.is_global(),
        time_lens: params.timeframe.is_some(),
    }) {
        ops::composition::compute_composition(db.conn(), &params.prefixes)?
    } else {
        None
    };

    let roots_map: HashMap<i64, Root> = all_roots.into_iter().map(|r| (r.id, r)).collect();
    if args.jsonl {
        // Scope goes to stderr on this branch only: stdout under --jsonl
        // carries nothing but the event stream.
        crate::scope::print_list_scope(&mut std::io::stderr(), &resolved);
        print_jsonl(&result)?;
    } else {
        print_human(
            &result,
            &resolved,
            time_label.as_deref(),
            &roots_map,
            limit,
            card.as_ref(),
            args.long,
        );
    }
    Ok(TrailExit::Reported)
}

pub struct CrossingsArgs {
    pub paths: Vec<PathBuf>,
    /// Narrow to what was drawn from at-or-under this location.
    pub origin: Option<String>,
    /// Narrow to what was placed at-or-under this location.
    pub destination: Option<String>,
    pub global: bool,
    pub limit: Option<usize>,
    pub all: bool,
    pub jsonl: bool,
}

/// `canon trail crossings` — the counterpart door.
///
/// The bare view expands the rollups `canon trail` already prints; naming a
/// counterpart narrows to one relation and drops to row grain. The way in is
/// from the output: every path this takes is one the trail just printed.
pub fn run_crossings(db: &mut Db, args: CrossingsArgs) -> Result<TrailExit> {
    let all_roots = repo::root::fetch_all(db.conn())?;
    let origin = resolve_counterpart(args.origin.as_deref(), &all_roots)?;
    let destination = resolve_counterpart(args.destination.as_deref(), &all_roots)?;

    let resolved = match open_scope(db, &args.paths, args.global, args.jsonl, &all_roots)? {
        ScopeDoor::Resolved(resolved) => resolved,
        ScopeDoor::Answered(exit) => return Ok(exit),
    };

    if is_boundless(&resolved, origin.as_ref(), destination.as_ref()) {
        return Err(anyhow!(boundless_refusal(args.global, &all_roots)));
    }

    let params = ops::crossings::CrossingsParams {
        prefixes: resolved.prefixes.clone(),
        origin,
        destination,
        limit: if args.all {
            None
        } else {
            Some(args.limit.unwrap_or(DEFAULT_LIMIT))
        },
        machine_output: args.jsonl,
    };
    let result = match ops::crossings::compute_crossings(db.conn(), &params)? {
        ops::crossings::Crossings::UnknownCounterpart(paths) => {
            for path in &paths {
                eprintln!(
                    "No history known at {path} — no sources, notes, or decisions record this place."
                );
            }
            return Ok(TrailExit::PlaceUnknown);
        }
        ops::crossings::Crossings::Reported(result) => result,
    };

    if args.jsonl {
        // Scope to stderr on this branch only: stdout under --jsonl carries
        // nothing but the event stream. The events themselves are the
        // trail's, unmodified — a filtered decision set, each carrying its
        // full row data, so a decision serialises identically wherever it
        // was surfaced from.
        crate::scope::print_list_scope(&mut std::io::stderr(), &resolved);
        print_crossings_jsonl(&result.decisions, &result.extractions_all)?;
    } else {
        print_crossings(&result, &resolved, &params);
    }
    Ok(TrailExit::Reported)
}

/// A counterpart path, in the form the index stores.
///
/// **The same three arms the scope pipeline runs, plus the trail's own
/// leniency instead of its failure.** A counterpart is an argument of exactly
/// the kind a positional scope path is, so it must not resolve by a second
/// rule: soft-match against known roots first (offline, and the form-tolerant
/// one), then the filesystem, and only then — where both are silent — the
/// lexical clean.
///
/// That last arm is why this cannot simply be the scope pipeline: it bails
/// where nothing resolves, and the removed and retired mount paths this door
/// exists for are precisely paths that no longer exist. Trail conjugates that
/// boundary rather than consuming it, here as everywhere else — a path Canon
/// cannot resolve is carried forward and answered by the evidence gate, not
/// refused at the parser.
///
/// A root spec is refused by name. `id:N` cannot name a sub-root counterpart
/// at all, and it is silently useless for a removed root, whose id went with
/// it — while the paths this door takes are the ones the trail's own output
/// just printed.
fn resolve_counterpart(value: Option<&str>, roots: &[Root]) -> Result<Option<String>> {
    let Some(value) = value else {
        return Ok(None);
    };
    if let Some(spec) = value.strip_prefix("id:").or(value.strip_prefix("path:")) {
        return Err(anyhow!(
            "counterparts are named by path, not by root spec — use the path itself \
             (e.g. '{spec}'), as printed in the trail's own output"
        ));
    }
    let path = Path::new(value);
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("/"));
    if let Some(matched) = crate::core::domain::path::resolve_path(path, roots, &cwd) {
        return Ok(Some(matched));
    }
    if let Ok(canonical) = std::fs::canonicalize(path) {
        return Ok(Some(canonical.to_string_lossy().into_owned()));
    }
    Ok(Some(
        crate::core::domain::path::clean_path(path, &cwd)
            .to_string_lossy()
            .into_owned(),
    ))
}

/// Why this ask has no boundary, and the way out of it — one sentence per
/// cause, each carrying its own remedy rather than a shared tail that fits
/// none of them well.
///
/// **Two causes today, not three.** Standing inside a suspended root used to
/// arrive here, because the CWD arm skipped suspended roots and "nowhere"
/// fell through to global; this door re-detected the suspension itself and
/// refused. It no longer arrives: the boundary hands back the parked root's
/// own scope, so crossings measures the boundary that is actually there and
/// the pause is stated in the header like every other remembering view. The
/// retired sentence's reading — the door is named, never re-described as
/// absence, and unsuspending is what is offered — survives as the spine's.
fn boundless_refusal(global: bool, _roots: &[Root]) -> String {
    if global {
        return "a global crossings view needs a counterpart to measure against: \
                add --origin <path> or --destination <path>"
            .to_string();
    }
    "no root contains this directory, so there is no boundary to measure \
     against — cd into a root, or name a counterpart with --origin <path> \
     or --destination <path>"
        .to_string()
}

/// Whether this ask has no boundary to measure against at all.
///
/// A global view has none of its own and borrows the named counterpart. With
/// neither named there is nothing to borrow, and every endpoint in the
/// universe reads as inside it — so every row classifies as a rearrangement
/// and the door reports "nothing crossed" about content that demonstrably
/// crossed. That is a **false historical claim**, not an empty view, which is
/// why the ask is refused rather than answered.
///
/// **Keyed on the resolved scope, never on the `--global` flag.** The flag is
/// only one of the two ways to arrive here: standing outside every known root
/// resolves to global *silently*, by the project's CWD fallback, and reaches
/// the identical unbounded state without `--global` ever being typed. Reading
/// the flag leaves that second door wide open, and the view it opens onto is
/// the whole universe misreported as one place.
fn is_boundless(
    resolved: &crate::core::ops::scope::ResolvedScope,
    origin: Option<&String>,
    destination: Option<&String>,
) -> bool {
    resolved.is_global() && origin.is_none() && destination.is_none()
}

/// What opening the trail's scope door produced.
enum ScopeDoor {
    Resolved(crate::core::ops::scope::ResolvedScope),
    /// The door already spoke — a retired statement was printed, or every
    /// asked-about place is unknown — and the caller returns the carried exit
    /// without computing anything.
    Answered(TrailExit),
}

/// The trail's scope door: resolve, answer a retired place with its book, and
/// conjugate the boundary's set-asides through the evidence gate.
///
/// **The gate runs at every door alike, or a `cd` defeats it** — and so does
/// a second subcommand. A place reached by standing in it is the same place
/// as one named on the command line, and every trail surface that takes a
/// scope reaches it through here rather than through a second spelling of the
/// same seventy lines.
///
/// It prints, so it stays in the interface layer; it decides nothing the
/// operations layer did not already decide.
fn open_scope(
    db: &mut Db,
    paths: &[PathBuf],
    global: bool,
    jsonl: bool,
    all_roots: &[Root],
) -> Result<ScopeDoor> {
    let mut resolved =
        match crate::core::ops::scope::resolve_scope(db.conn(), paths, global, all_roots) {
            Ok(crate::core::ops::scope::Door::Open(resolved)) => resolved,
            // Remembering: a pause of attention does not make Canon forget,
            // so the trail reads at the parked place — the same place,
            // answering the same way whether it was named or is where the
            // user is standing. What it does *not* do is exempt the place
            // from the evidence gate below: the door is a fact to state
            // beside the answer, never a reason to stop asking the question.
            Ok(crate::core::ops::scope::Door::Closed(closed)) => closed.read_here(),
            // An explicit path that misses every live root may be a retired
            // root's old mount path — then the retirement is the answer, not
            // the error. On a live root, a miss means the place has no
            // sources standing in it; whether it has a *story* is the next
            // question, asked below. Anything else propagates the original
            // error untouched.
            Err(err) => {
                if let Some(statement) = retired_scope_statement(db.conn(), paths)? {
                    emit_retired_statement(&statement, jsonl)?;
                    return Ok(ScopeDoor::Answered(TrailExit::Reported));
                }
                match crate::core::ops::scope::resolve_history_scope(paths, all_roots) {
                    // Every path here is sourceless — each must earn its
                    // place in the view the same way a set-aside does.
                    Some(mut resolved) => {
                        resolved.set_aside = std::mem::take(&mut resolved.prefixes);
                        resolved
                    }
                    None => return Err(err),
                }
            }
        };

    // Trail conjugates the boundary's set-asides rather than consuming them.
    // A present-tense command is right to skip a place with no sources; the
    // trail's subject is what *happened*, so a place a move-mode apply
    // emptied still answers — and a place nothing records at all is said to
    // be unknown rather than rendered as a plausible, empty view of
    // somewhere it never was.
    //
    // A root's own top is exempt at both doors alike — the boundary policy
    // always keeps a root-level path, and a root that has been added is a
    // place Canon knows by definition.
    // Two kinds of prefix arrive here ungated, and both must earn their place
    // in the view the way a set-aside does: one reached by standing in it,
    // because CWD defaulting is a context switch rather than a claim about
    // content, and one behind a **closed door**, because the door answered
    // before the source gate could ask.
    //
    // **The gate is conjugated behind a door, never skipped.** It reads
    // sources, notes, extractions and decisions, none of which a closed door
    // hides, so it answers correctly there — and skipping it would render a
    // plausible, empty view of a place Canon has never heard of, with a
    // header claiming paths are relative to it. What the door changes is the
    // *statement*: an unknown place behind one names the door too, so the
    // reader is never left wondering whether the pause is what hid it.
    //
    // A root's own top is exempt at every door alike: a root that has been
    // added is a place Canon knows by definition.
    let mut to_check = std::mem::take(&mut resolved.set_aside);

    // **Remembering has no set-aside register.** A door the ask named is a
    // place to *read*, not a place to skip — the permit says knowledge Canon
    // already holds still reads, and a live keeper standing beside it changes
    // nothing about that. So the boundary's `parked` partition moves onto the
    // reading side here: its paths into the gate, its doors onto the pause the
    // header states. Present-tense views keep the two registers apart, because
    // for them setting aside is the whole answer; for this one there is no
    // such answer to give, and leaving them apart made the same place answer
    // differently depending on what was named beside it.
    let reading_behind_a_door = !resolved.pause.is_empty();
    let named_parked = std::mem::take(&mut resolved.parked);
    // Every door this ask touches, from both arms — the wholly-closed one,
    // whose places `read_here` already put on the pause, and the mixed one,
    // whose places arrive here. Held apart from `pause` until the gate has
    // run, because a door is only *stood behind* if something of it survived:
    // see the derivation below.
    let mut doors = std::mem::take(&mut resolved.pause);
    for place in &named_parked {
        if !doors.iter().any(|root| root.root_id == place.root.root_id) {
            doors.push(place.root.clone());
        }
    }
    // The order to restore below. **Kept-then-parked, not the literal ask**:
    // the boundary partitioned these into two lists and the interleaving is
    // not recoverable from here, so a scope naming a parked path before a live
    // one renders the live one first. Ordering is preserved *within* what the
    // boundary handed over, which is as far as this can honestly reach.
    let asked: Vec<String> = resolved
        .prefixes
        .iter()
        .cloned()
        .chain(named_parked.iter().map(|place| place.path.clone()))
        .collect();
    // A parked place takes **the same root-top exemption every other prefix
    // takes**: a root that has been added is a place Canon knows by
    // definition, and exempting it only when it arrives through the other arm
    // would make the same root top answer differently beside a sibling — the
    // very shape this whole conjugation exists to remove, one door narrower.
    for place in named_parked {
        if scope_is_a_root_top(&place.path, all_roots) {
            resolved.prefixes.push(place.path);
        } else {
            to_check.push(place.path);
        }
    }

    // Asked before the doors were gathered: the prefixes that arrive *ungated*
    // are the CWD arm's and a wholly-closed door's. A live keeper came through
    // the source gate already and is not re-asked.
    if resolved.from_cwd || reading_behind_a_door {
        let (exempt, gated): (Vec<String>, Vec<String>) = std::mem::take(&mut resolved.prefixes)
            .into_iter()
            .partition(|prefix| scope_is_a_root_top(prefix, all_roots));
        resolved.prefixes = exempt;
        to_check.extend(gated);
    }

    let mut unknown: Vec<String> = Vec::new();
    for prefix in to_check {
        match ops::place::place_knowledge(db.conn(), &prefix, all_roots)? {
            ops::place::PlaceKnowledge::Retired(statement) => {
                emit_retired_statement(&statement, jsonl)?;
                return Ok(ScopeDoor::Answered(TrailExit::Reported));
            }
            ops::place::PlaceKnowledge::Evidence => resolved.prefixes.push(prefix),
            ops::place::PlaceKnowledge::Unknown => unknown.push(prefix),
        }
    }
    restore_asked_order(&mut resolved.prefixes, &asked);
    restore_asked_order(&mut unknown, &asked);

    // **The pause names the doors the surviving view actually stands behind.**
    // A door is gathered from the ask, but a place the gate dropped is not in
    // the view, and a header offering the way back through a door none of the
    // rendered rows lie behind points at nothing. The absence lines above still
    // name their own door, from `doors`, because that statement *is* about the
    // dropped place.
    resolved.pause = doors
        .iter()
        .filter(|root| {
            resolved
                .prefixes
                .iter()
                .any(|prefix| crate::core::domain::path::path_is_under(prefix, &root.root_path))
        })
        .cloned()
        .collect();
    for prefix in &unknown {
        eprintln!("{}", no_history_line(prefix, &doors));
    }
    if !unknown.is_empty() && paths.len() == 1 {
        if let Some(arg) = paths[0].to_str() {
            if crate::trail::domain::place::looks_like_decision_id(arg) {
                eprintln!("(Did you mean 'canon trail show {arg}'?)");
            }
        }
    }
    if (!paths.is_empty() || resolved.from_cwd) && resolved.prefixes.is_empty() {
        return Ok(ScopeDoor::Answered(TrailExit::PlaceUnknown));
    }

    // The silent CWD-global fallback: standing inside a retired root's old
    // mount path, the trail must state the retirement rather than quietly
    // showing the whole universe.
    if !global && paths.is_empty() && resolved.is_global() {
        if let Ok(cwd) = std::env::current_dir() {
            let cleaned = crate::core::domain::path::clean_path(&cwd, &cwd);
            if let Some(statement) =
                crate::retire::find_retirement_covering_path(db.conn(), &cleaned.to_string_lossy())?
            {
                emit_retired_statement(&statement, jsonl)?;
                return Ok(ScopeDoor::Answered(TrailExit::Reported));
            }
        }
    }

    Ok(ScopeDoor::Resolved(resolved))
}

/// Whether a scope path is a root's own top rather than a place inside one.
/// Pure given its inputs; the exemption it carries is the boundary policy's
/// own ("root-level paths are always kept").
fn scope_is_a_root_top(prefix: &str, roots: &[Root]) -> bool {
    crate::core::domain::root::find_containing_root(prefix, roots)
        .is_none_or(|(_, _, _, rel)| rel.is_empty())
}

/// Put a list of prefixes back into the order they were asked in.
///
/// The evidence gate splits a scope and pushes the survivors back, so what
/// comes out of it is ordered by disposition rather than by the ask. That is
/// invisible to every consumer but the reader, who sees the header and the
/// unknown-place lines — and who has no way to account for an order they did
/// not type. Anything not in the ask (a set-aside the boundary produced
/// separately) keeps its place at the end.
fn restore_asked_order(prefixes: &mut [String], asked: &[String]) {
    prefixes.sort_by_key(|prefix| asked.iter().position(|a| a == prefix).unwrap_or(usize::MAX));
}

/// What a place with no history is told about itself.
///
/// **An absence states what it observes, and the door is stated beside it —
/// never in its place.** The absence is the same absence either way, because
/// the gate reads sources, notes, extractions and decisions and a closed door
/// hides none of them. So the observation comes first and unchanged; the door
/// follows as a second fact, because a reader standing behind one would
/// otherwise wonder whether the pause is what hid the place. Substituting the
/// door for the cause would be worse than silence: it would offer a way back
/// that does not lead anywhere, since unsuspending reveals nothing about a
/// path Canon has never heard of.
///
/// Pure: composed here so the claim can be pinned without a process.
fn no_history_line(prefix: &str, pause: &[crate::core::domain::root::ParkedRoot]) -> String {
    let absence = format!(
        "No history known at {prefix} — no sources, notes, or decisions record this place."
    );
    match pause
        .iter()
        .find(|root| crate::core::domain::path::path_is_under(prefix, &root.root_path))
    {
        Some(root) => format!("{absence} {}", root.pause_line()),
        None => absence,
    }
}

/// The newest bound retirement covering any of the requested paths, cleaned
/// lexically (the old mount path may no longer exist on disk, so resolution
/// must not require it to).
fn retired_scope_statement(
    conn: &rusqlite::Connection,
    paths: &[PathBuf],
) -> Result<Option<crate::retire::RetiredScope>> {
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("/"));
    for path in paths {
        let cleaned = crate::core::domain::path::clean_path(path, &cwd);
        if let Some(statement) =
            crate::retire::find_retirement_covering_path(conn, &cleaned.to_string_lossy())?
        {
            return Ok(Some(statement));
        }
    }
    Ok(None)
}

/// The retired-scope statement: this place's story is closed and bound —
/// stated as fact, pointing at the book (exit 0: the command answered the
/// question asked). Under `--jsonl` the statement is one typed JSON object —
/// the documented clean-stdout contract holds on this path too.
fn emit_retired_statement(s: &crate::retire::RetiredScope, jsonl: bool) -> Result<()> {
    if jsonl {
        let json = serde_json::to_string(&JsonRetiredScopeEvent {
            r#type: "retired_scope",
            root_path: &s.root_path,
            retired_at: s.retired_at,
            reason: s.reason.as_deref(),
            book: &s.book_display,
            decision_id: s.decision_id,
        })?;
        println!("{json}");
        return Ok(());
    }
    print_retired_statement(s);
    Ok(())
}

fn print_retired_statement(s: &crate::retire::RetiredScope) {
    match &s.reason {
        Some(reason) => println!(
            "This place is retired: {} — retired {}, \"{}\".",
            s.root_path,
            format_date_only(s.retired_at),
            reason
        ),
        None => println!(
            "This place is retired: {} — retired {}.",
            s.root_path,
            format_date_only(s.retired_at)
        ),
    }
    println!(
        "The story is bound at {} (decision #{}).",
        s.book_display, s.decision_id
    );
}

pub fn run_show(db: &mut Db, id: i64) -> Result<()> {
    // Environment access is the interface's. Cleaned lexically and never
    // canonicalized — the trail's standing treatment of paths, because an old
    // mount path need not exist to be asked about. A failure yields `None`,
    // which produces no markers and recorded order.
    let cwd = std::env::current_dir().ok().map(|cwd| {
        crate::core::domain::path::clean_path(&cwd, &cwd)
            .to_string_lossy()
            .into_owned()
    });
    let Some(show) = crate::trail::ops::show::compute_show(db.conn(), id, cwd.as_deref())? else {
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
    if show.scopes.is_empty() {
        println!("  scope:    global");
    } else {
        for line in show_scope_lines(&show.scopes) {
            println!("{line}");
        }
    }
    if !show.extractions.is_empty() {
        println!("  drew from:");
        for line in drew_from_lines(&show.extractions) {
            println!("{line}");
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
            match &receipt.relocation {
                Some(PointerRelocation::Gathered { book_ledger_path }) => {
                    println!("      (root retired — gathered into the book at {book_ledger_path})");
                }
                Some(PointerRelocation::NotGathered { book_path }) => {
                    println!(
                        "      (root retired — not gathered into the book; the book at {book_path} records why)"
                    );
                }
                Some(PointerRelocation::Unreachable { book_path }) => {
                    println!(
                        "      (root retired — the story is bound at {book_path}, not reachable now)"
                    );
                }
                None => {}
            }
        }
    }
    Ok(())
}

/// Maximum scopes `trail show` lists before an explicit remainder line —
/// the value `drew_from_lines` uses in this same file, for the same reason:
/// one decision can name dozens of places, and a wall of them is unreadable.
const SHOW_SCOPE_CAP: usize = 5;

/// The `scope:` block's lines, indentation included: one place per line,
/// the ones bearing on where the reader stands first and marked, capped with
/// an explicit remainder. Pure data — testable without capturing stdout, the
/// same separation `drew_from_lines` keeps eight lines below it.
///
/// The markers name the relation ops classified; this function renders and
/// never classifies. Ordering is ops' too — the hoist is what keeps the
/// relevant place out of the truncated remainder, so it must not be
/// re-decided here.
fn show_scope_lines(scopes: &[ShowScope]) -> Vec<String> {
    let mut out = Vec::new();
    for (i, scope) in scopes.iter().take(SHOW_SCOPE_CAP).enumerate() {
        let label = if i == 0 { "  scope:  " } else { "          " };
        let marker = match scope.relation {
            ScopeRelation::Here => "  (here)",
            ScopeRelation::WithinHere => "  (within here)",
            ScopeRelation::Unrelated => "",
        };
        out.push(format!("{label}  {}{marker}", scope.display_path));
    }
    let more = scopes.len().saturating_sub(SHOW_SCOPE_CAP);
    if more > 0 {
        out.push(format!(
            "            \u{2026} and {} more {}",
            crate::core::domain::format::format_count(more),
            if more == 1 { "place" } else { "places" }
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::testing::{insert_root, setup_test_db};

    fn roots_of(conn: &rusqlite::Connection) -> Vec<Root> {
        repo::root::fetch_all(conn).unwrap()
    }

    fn scope(prefixes: &[&str], from_cwd: bool) -> crate::core::ops::scope::ResolvedScope {
        crate::core::ops::scope::ResolvedScope {
            prefixes: prefixes.iter().map(|p| p.to_string()).collect(),
            set_aside: Vec::new(),
            parked: Vec::new(),
            pause: Vec::new(),
            from_cwd,
            auto_include_archived: false,
        }
    }

    /// One top-level function's source, for the structural guards below.
    ///
    /// The region ends at the **next top-level item**, not at the first
    /// column-1 `}`: rustfmt separates items with a blank line and starts
    /// each in column 1, while everything inside a body is indented. Slicing
    /// at the first `\n}\n` looked equivalent and is not — a column-1 `}`
    /// inside a raw string truncates the region early, and a guard that
    /// asserts the *absence* of something then passes by examining a
    /// fragment. An absence check is only as good as its region.
    fn fn_body(name: &str) -> &'static str {
        let source = include_str!("cli.rs");
        let start = source
            .find(&format!("\nfn {name}("))
            .or_else(|| source.find(&format!("\npub fn {name}(")))
            .unwrap_or_else(|| panic!("{name} not found"));
        let rest = &source[start + 1..];
        let end = rest
            .match_indices("\n\n")
            .find(|(i, _)| {
                rest[i + 2..]
                    .chars()
                    .next()
                    .is_some_and(|c| !c.is_whitespace())
            })
            .map(|(i, _)| i)
            .unwrap_or(rest.len());
        &rest[..end]
    }

    /// **The scope-door extraction's guard.** `open_scope` holds the retired
    /// statement, the evidence gate and the CWD-global fallback, and its
    /// whole point is that both entry points reach it rather than carrying
    /// two spellings of it. "Behaves identically" is then true by
    /// construction — *provided both actually call it*, which is the part a
    /// behavioural test cannot see and a refactor can quietly break.
    ///
    /// So the property pinned here is structural: neither entry point
    /// resolves scope by itself. A future edit that inlines resolution back
    /// into either one fails here rather than at a user's terminal, where the
    /// symptom would be a `cd` defeating the evidence gate at one door only.
    #[test]
    fn the_scope_door_behaves_identically_at_both_entry_points() {
        for entry in ["run", "run_crossings"] {
            let body = fn_body(entry);
            assert!(
                body.contains("open_scope("),
                "{entry} must reach the scope door"
            );
            assert!(
                !body.contains("resolve_scope("),
                "{entry} must not resolve scope itself — that is open_scope's"
            );
            assert!(
                !body.contains("place_knowledge("),
                "{entry} must not run the evidence gate itself — that is open_scope's"
            );
        }
    }

    /// The refusal keys on the **resolved scope**, not on `--global`. Both
    /// doors reach the unbounded state: the flag, and the silent CWD fallback
    /// when no known root contains the working directory. Gating on the flag
    /// left the second one open, and what came through it was every row in
    /// the universe reported as a rearrangement — "nothing crossed" stated
    /// about content that had crossed, at exit 0.
    #[test]
    fn a_boundless_ask_is_refused_however_it_became_global() {
        // The flag's door.
        assert!(is_boundless(&scope(&[], false), None, None));
        // The silent CWD door — no flag typed, and this is the case a
        // flag-keyed guard misses entirely.
        assert!(is_boundless(&scope(&[], true), None, None));

        // A counterpart supplies the boundary, so a global view is answerable.
        let counterpart = "/Volumes/sd".to_string();
        assert!(!is_boundless(&scope(&[], true), Some(&counterpart), None));
        assert!(!is_boundless(&scope(&[], false), None, Some(&counterpart)));

        // A scoped view is its own boundary and needs no counterpart.
        assert!(!is_boundless(&scope(&["/archive"], true), None, None));

        // **And the door actually consults it.** The assertions above pin the
        // predicate; nothing in this crate invokes `run_crossings`, so
        // deleting the refusal that calls it would leave the whole suite
        // green while the defect returned — which is precisely the shape that
        // left the symlink arm unguarded. Structural, for the same reason the
        // scope-door guard is: a call site's existence is what a behavioural
        // test here cannot see.
        let body = fn_body("run_crossings");
        assert!(
            body.contains("is_boundless("),
            "run_crossings must refuse a boundless ask, not merely be able to detect one"
        );
        assert!(
            body.contains("boundless_refusal("),
            "the refusal must state its cause"
        );
    }

    /// `id:N` cannot name a sub-root counterpart at all, and it is silently
    /// useless for the removed roots this door exists for — the id went with
    /// the root. Refused by name rather than misread as a relative path.
    #[test]
    fn a_root_spec_argument_is_refused_by_name() {
        let conn = setup_test_db();
        insert_root(&conn, "/photos", "source", false);
        let roots = roots_of(&conn);

        for spec in ["id:3", "path:/photos"] {
            let err = resolve_counterpart(Some(spec), &roots)
                .expect_err("a root spec must be refused")
                .to_string();
            assert!(err.contains("named by path"), "{err}");
        }
    }

    /// A counterpart is an argument of exactly the kind a positional scope
    /// path is, so it resolves by the same rule: soft-match against known
    /// roots first, so a path already in the index comes back in the byte
    /// form the index stores.
    #[test]
    fn a_counterpart_under_a_known_root_resolves_to_the_stored_form() {
        let conn = setup_test_db();
        insert_root(&conn, "/photos", "source", false);
        let roots = roots_of(&conn);

        assert_eq!(
            resolve_counterpart(Some("/photos/2016"), &roots).unwrap(),
            Some("/photos/2016".to_string())
        );
    }

    /// The arm the scope pipeline does not have: where neither the index nor
    /// the disk can resolve a path, it is carried forward lexically rather
    /// than refused at the parser. The removed and retired mount paths this
    /// door exists for are precisely paths that no longer exist, and whether
    /// Canon knows them is the evidence gate's question, not the parser's.
    #[test]
    fn a_counterpart_that_no_longer_exists_is_still_askable() {
        let conn = setup_test_db();
        insert_root(&conn, "/photos", "source", false);
        let roots = roots_of(&conn);

        assert_eq!(
            resolve_counterpart(Some("/Volumes/long-gone/photos"), &roots).unwrap(),
            Some("/Volumes/long-gone/photos".to_string())
        );
    }

    /// **The arm the whole deviation exists for.** A path that resolves under
    /// no known root but *does* exist on disk is canonicalized, so a symlinked
    /// spelling reaches the location the index recorded. Without this arm the
    /// flag and the positional path beside it resolve by two different rules,
    /// and the door refuses to open on a place that is right there — which is
    /// how the defect was found, on a platform where `/tmp` is a symlink.
    ///
    /// Pinned because deleting the arm leaves every other test green.
    #[test]
    fn a_counterpart_reached_through_a_symlink_resolves_to_the_real_location() {
        let conn = setup_test_db();
        let roots = roots_of(&conn);

        let dir = tempfile::tempdir().unwrap();
        let real = dir.path().join("real");
        std::fs::create_dir(&real).unwrap();
        let link = dir.path().join("link");
        std::os::unix::fs::symlink(&real, &link).unwrap();

        let asked = link.join("2016");
        std::fs::create_dir(&asked).unwrap();

        let resolved = resolve_counterpart(Some(asked.to_str().unwrap()), &roots)
            .unwrap()
            .unwrap();
        let expected = std::fs::canonicalize(&asked).unwrap();
        assert_eq!(resolved, expected.to_string_lossy());
        // The lexical spelling is what a bare clean would have produced, and
        // it is not what the index stores.
        assert_ne!(resolved, asked.to_string_lossy());
    }

    /// Trailing separators and `.` segments are cleaned, so a path pasted
    /// with a trailing slash still matches the stored form.
    #[test]
    fn a_counterpart_is_cleaned_before_it_is_matched() {
        let conn = setup_test_db();
        let roots = roots_of(&conn);
        assert_eq!(
            resolve_counterpart(Some("/Volumes/gone/./photos/"), &roots).unwrap(),
            Some("/Volumes/gone/photos".to_string())
        );
    }

    /// The gate runs at both doors or a `cd` defeats it — but a root's own
    /// top is exempt at both alike, matching the boundary policy's
    /// "root-level paths are always kept".
    #[test]
    fn a_scope_at_a_root_top_is_exempt_from_the_evidence_gate() {
        let conn = setup_test_db();
        insert_root(&conn, "/photos", "source", false);
        let roots = roots_of(&conn);

        assert!(scope_is_a_root_top("/photos", &roots));
        assert!(!scope_is_a_root_top("/photos/2012", &roots));
    }

    fn mk_scope(path: &str, relation: ScopeRelation) -> ShowScope {
        ShowScope {
            display_path: path.to_string(),
            relation,
        }
    }

    /// The comma-wall becomes a list — the same idiom `drew_from_lines`
    /// applies eight lines below it, to the same shape of data.
    #[test]
    fn show_renders_one_scope_per_line() {
        let lines = show_scope_lines(&[
            mk_scope("/a/foto", ScopeRelation::Here),
            mk_scope("/a/admin", ScopeRelation::Unrelated),
        ]);
        assert_eq!(
            lines,
            vec![
                "  scope:    /a/foto  (here)".to_string(),
                "            /a/admin".to_string(),
            ]
        );
    }

    #[test]
    fn show_marks_a_scope_inside_the_cwd() {
        let lines = show_scope_lines(&[mk_scope("/a/foto/2016", ScopeRelation::WithinHere)]);
        assert_eq!(lines, vec!["  scope:    /a/foto/2016  (within here)"]);
    }

    /// Capped with an explicit remainder, never a silent truncation. Ops has
    /// already hoisted the marked scope, which is why the cap is safe here.
    #[test]
    fn show_caps_the_scope_list_with_an_explicit_remainder() {
        let mut scopes = vec![mk_scope("/a/foto", ScopeRelation::Here)];
        scopes
            .extend((0..30).map(|i| mk_scope(&format!("/a/dir{i:02}"), ScopeRelation::Unrelated)));

        let lines = show_scope_lines(&scopes);
        assert_eq!(lines.len(), 6);
        assert_eq!(lines[0], "  scope:    /a/foto  (here)");
        assert_eq!(lines[5], "            \u{2026} and 26 more places");
    }

    /// A path under no known root cannot be gated on evidence it could never
    /// have — the global fallback owns that case, so it reads as exempt here.
    #[test]
    fn a_scope_under_no_known_root_is_exempt() {
        let conn = setup_test_db();
        insert_root(&conn, "/photos", "source", false);
        assert!(scope_is_a_root_top("/elsewhere", &roots_of(&conn)));
    }

    // ========================================================================
    // The closed door — remembering
    // ========================================================================

    /// A pause of attention does not make Canon forget. Standing in a closed
    /// root used to show the whole universe's trail; it now shows this
    /// place's, with the door stated once in the header.
    #[test]
    fn a_parked_cwd_reads_its_own_trail_not_the_global_one() {
        use crate::core::ops::scope::{resolve_scope_at, Door};

        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", true);
        let roots = roots_of(&conn);

        let door = resolve_scope_at(
            &conn,
            &[],
            false,
            &roots,
            Some(std::path::Path::new("/photos/2011")),
        )
        .unwrap();
        let reading = match door {
            Door::Closed(closed) => closed.read_here(),
            Door::Open(open) => panic!("expected a closed door, got {open:?}"),
        };

        assert!(!reading.is_global(), "the parked place is the subject");
        assert_eq!(reading.prefixes, vec!["/photos/2011".to_string()]);
        assert_eq!(reading.pause[0].root_id, root_id);
    }

    /// **`crossings` at a closed door reads.** It used to re-detect the
    /// suspension here and refuse — the one surface that noticed, answering
    /// differently from the same place named on the command line. The
    /// boundary now hands back the parked root's own scope, so there *is* a
    /// boundary to measure against.
    #[test]
    fn crossings_at_a_parked_cwd_measures_the_parked_boundary() {
        let conn = setup_test_db();
        insert_root(&conn, "/photos", "source", true);
        let roots = roots_of(&conn);

        // The refusal's remaining causes are two, and neither is a door.
        assert!(!boundless_refusal(true, &roots).contains("suspended"));
        assert!(!boundless_refusal(false, &roots).contains("suspended"));
        assert!(
            !fn_body("boundless_refusal").contains("is_active"),
            "the door is derived at the boundary and never re-detected here"
        );

        // And a parked CWD is no longer boundless: it has its own scope.
        let scope = scope(&["/photos/2011"], true);
        assert!(!is_boundless(&scope, None, None));
    }

    /// **An unknown place behind a closed door names the door.** The gate
    /// runs there — it reads sources, notes, extractions and decisions, none
    /// of which a door hides — so a path Canon has never heard of is still
    /// said to be unknown rather than rendered as a plausible empty view. What
    /// the door adds is the cause, so the reader is not left wondering whether
    /// the pause is what hid the place.
    #[test]
    fn an_unknown_place_behind_a_door_names_the_door() {
        use crate::core::domain::root::ParkedRoot;

        let parked = ParkedRoot {
            root_id: 1,
            root_path: "/photos".to_string(),
        };

        let behind = no_history_line("/photos/nosuchdir", std::slice::from_ref(&parked));
        assert_eq!(
            behind,
            "No history known at /photos/nosuchdir — no sources, notes, or decisions record this place. /photos suspended · canon roots unsuspend path:/photos"
        );
        assert!(
            behind.starts_with(&no_history_line("/photos/nosuchdir", &[])),
            "the observation is unchanged; the door is a second fact beside it: {behind}"
        );

        // A place under no closed door keeps the plain sentence, and a place
        // under *another* root's door is not attributed to it.
        let plain =
            "No history known at /live/nosuchdir — no sources, notes, or decisions record this place.";
        assert_eq!(no_history_line("/live/nosuchdir", &[]), plain);
        assert_eq!(no_history_line("/live/nosuchdir", &[parked]), plain);
    }
}
