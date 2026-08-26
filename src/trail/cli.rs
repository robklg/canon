//! Trail's CLI stratum: parse args, call the ops layer, dispatch to the
//! render/jsonl strata.
//!
//! Two lenses over one timeline: the scope lens ("what did I do here?",
//! newest first) and the time lens ("what did I do today?", day-grouped
//! story). Notes interleave as the thinking between the actions, visually
//! distinct — a thought must never read as an act.

use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{anyhow, Result};
use chrono::Local;

use crate::core::domain::root::Root;
use crate::core::repo::{self, Db};
use crate::trail::domain::composition::{card_applies, ViewShape};
use crate::trail::domain::timeline::{parse_when, WhenValue};
use crate::trail::jsonl::{print_jsonl, JsonRetiredScopeEvent};
use crate::trail::ops;
use crate::trail::ops::compute::{TrailParams, DEFAULT_LIMIT};
use crate::trail::ops::show::{PointerRelocation, ScopeRelation, ShowScope};
use crate::trail::render::{
    drew_from_lines, format_counts, format_date_only, format_datetime, print_human,
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
    let mut resolved = match crate::core::ops::scope::resolve_scope(
        db.conn(),
        &args.paths,
        args.global,
        &all_roots,
    ) {
        Ok(resolved) => resolved,
        // An explicit path that misses every live root may be a retired
        // root's old mount path — then the retirement is the answer, not
        // the error. On a live root, a miss means the place has no sources
        // standing in it; whether it has a *story* is the next question,
        // asked below. Anything else propagates the original error
        // untouched.
        Err(err) => {
            if let Some(statement) = retired_scope_statement(db.conn(), &args.paths)? {
                emit_retired_statement(&statement, args.jsonl)?;
                return Ok(TrailExit::Reported);
            }
            match crate::core::ops::scope::resolve_history_scope(&args.paths, &all_roots) {
                // Every path here is sourceless — each must earn its place
                // in the view the same way a set-aside does.
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
    // A place reached by standing in it is the same place as one named on
    // the command line, and answers the same way: the gate runs at both
    // doors, or a `cd` would defeat it. A root's own top is exempt at both
    // doors alike — the boundary policy always keeps a root-level path, and
    // a root that has been added is a place Canon knows by definition.
    let mut to_check = std::mem::take(&mut resolved.set_aside);
    if resolved.from_cwd && !cwd_scope_is_a_root_top(&resolved.prefixes, &all_roots) {
        to_check.append(&mut resolved.prefixes);
    }

    let mut unknown: Vec<String> = Vec::new();
    for prefix in to_check {
        match ops::place::place_knowledge(db.conn(), &prefix, &all_roots)? {
            ops::place::PlaceKnowledge::Retired(statement) => {
                emit_retired_statement(&statement, args.jsonl)?;
                return Ok(TrailExit::Reported);
            }
            ops::place::PlaceKnowledge::Evidence => resolved.prefixes.push(prefix),
            ops::place::PlaceKnowledge::Unknown => unknown.push(prefix),
        }
    }
    for prefix in &unknown {
        eprintln!(
            "No history known at {prefix} — no sources, notes, or decisions record this place."
        );
    }
    if !unknown.is_empty() && args.paths.len() == 1 {
        if let Some(arg) = args.paths[0].to_str() {
            if crate::trail::domain::place::looks_like_decision_id(arg) {
                eprintln!("(Did you mean 'canon trail show {arg}'?)");
            }
        }
    }
    if (!args.paths.is_empty() || resolved.from_cwd) && resolved.prefixes.is_empty() {
        return Ok(TrailExit::PlaceUnknown);
    }

    // The silent CWD-global fallback: standing inside a retired root's old
    // mount path, `canon trail` must state the retirement rather than
    // quietly showing the whole universe.
    if !args.global && args.paths.is_empty() && resolved.is_global() {
        if let Ok(cwd) = std::env::current_dir() {
            let cleaned = crate::core::domain::path::clean_path(&cwd, &cwd);
            if let Some(statement) =
                crate::retire::find_retirement_covering_path(db.conn(), &cleaned.to_string_lossy())?
            {
                emit_retired_statement(&statement, args.jsonl)?;
                return Ok(TrailExit::Reported);
            }
        }
    }

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

/// Whether a CWD-defaulted scope is a root's own top rather than a place
/// inside one. Pure given its inputs; the exemption it carries is the
/// boundary policy's own ("root-level paths are always kept").
fn cwd_scope_is_a_root_top(prefixes: &[String], roots: &[Root]) -> bool {
    prefixes.iter().any(|p| {
        crate::core::domain::root::find_containing_root(p, roots)
            .is_none_or(|(_, _, _, rel)| rel.is_empty())
    })
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

    /// The gate runs at both doors or a `cd` defeats it — but a root's own
    /// top is exempt at both alike, matching the boundary policy's
    /// "root-level paths are always kept".
    #[test]
    fn a_cwd_scope_at_a_root_top_is_exempt_from_the_evidence_gate() {
        let conn = setup_test_db();
        insert_root(&conn, "/photos", "source", false);
        let roots = roots_of(&conn);

        assert!(cwd_scope_is_a_root_top(&["/photos".to_string()], &roots));
        assert!(!cwd_scope_is_a_root_top(
            &["/photos/2012".to_string()],
            &roots
        ));
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
    fn a_cwd_scope_under_no_known_root_is_exempt() {
        let conn = setup_test_db();
        insert_root(&conn, "/photos", "source", false);
        assert!(cwd_scope_is_a_root_top(
            &["/elsewhere".to_string()],
            &roots_of(&conn)
        ));
    }
}
