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
use crate::trail::ops::show::PointerRelocation;
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
    let resolved =
        match crate::ops::scope::resolve_scope(db.conn(), &args.paths, args.global, &all_roots) {
            Ok(resolved) => resolved,
            // An explicit path that misses every live root may be a retired
            // root's old mount path — then the retirement is the answer, not
            // the error. On a live root, a miss means an emptied place — no
            // sources stand there, but its history (extraction rows, notes)
            // still does, and refusing the question would 404 exactly the
            // best-resolved places. Anything else propagates the original
            // error untouched.
            Err(err) => {
                if let Some(statement) = retired_scope_statement(db.conn(), &args.paths)? {
                    emit_retired_statement(&statement, args.jsonl)?;
                    return Ok(());
                }
                match crate::ops::scope::resolve_history_scope(&args.paths, &all_roots) {
                    Some(resolved) => resolved,
                    None => return Err(err),
                }
            }
        };

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
                return Ok(());
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
        crate::scope::print_list_scope(&resolved);
        print_jsonl(&result)?;
    } else {
        print_human(
            &result,
            &resolved,
            time_label.as_deref(),
            &roots_map,
            limit,
            card.as_ref(),
        );
    }
    Ok(())
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
    let Some(show) = crate::trail::ops::show::compute_show(db.conn(), id)? else {
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
