use anyhow::{Context, Result};
use std::io::{self, BufRead};

use crate::domain::config::{LedgerConfig, RecordingMode};
use crate::domain::decision::{DecisionCommand, DecisionStatus};
use crate::domain::scope::DecisionScope;
use crate::facts::ops::import as facts_import;
use crate::facts::ops::import::ImportRecord;
use crate::ops::decision::{DecisionCounts, DecisionParams, DecisionRecorder};
use crate::repo::{self, Db};

pub fn import_run(
    db: &mut Db,
    allow_archived: bool,
    verbose: bool,
    command_line: &str,
    config: &LedgerConfig,
    no_receipt: bool,
) -> Result<()> {
    let conn = db.conn_mut();
    let mut state = facts_import::init_state(conn)?;

    let decision = DecisionParams {
        command: DecisionCommand::ImportFacts,
        scope: Vec::new(),
        command_line: command_line.to_string(),
        reason: None,
        record_enabled: config.recording != RecordingMode::Off,
        receipt_enabled: config.recording == RecordingMode::Full && !no_receipt,
        ledger_config: config.clone(),
    };
    let mut recorder = DecisionRecorder::start(conn, &decision, None);

    let stdin = io::stdin();
    for line in stdin.lock().lines() {
        let line = line.context("Failed to read line from stdin")?;
        if line.trim().is_empty() {
            continue;
        }

        state.stats.lines_processed += 1;

        let record: ImportRecord = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(e) => {
                eprintln!(
                    "Warning: Failed to parse line {}: {}",
                    state.stats.lines_processed, e
                );
                continue;
            }
        };

        match facts_import::process_record(conn, &record, &mut state, allow_archived) {
            Ok(outcome) => {
                for warning in &outcome.warnings {
                    eprintln!("Warning: {warning}");
                }
                if verbose {
                    for line in &outcome.verbose_lines {
                        eprintln!("{line}");
                    }
                }
            }
            Err(e) => {
                eprintln!(
                    "Warning: Failed to process source_id {}: {}",
                    record.source_id, e
                );
            }
        }
    }

    // Print type mismatch warnings with remediation hint
    if !state.type_mismatch_keys.is_empty() {
        eprintln!("\nType mismatch warnings:");
        let mut keys: Vec<_> = state.type_mismatch_keys.iter().collect();
        keys.sort_by_key(|(k, _)| *k);
        for (key, (existing, attempted)) in keys {
            eprintln!("  {key}: existing type is {existing}, attempted to import {attempted}");
        }
        eprintln!("\nTo change the type, first delete existing facts:");
        eprintln!("  canon facts delete --key <key>");
        eprintln!("Then re-import with the new type.");
    }

    let summary = state.stats.compose_summary();

    // The command reads stdin, so no scope exists at start(); record the
    // durable scope index from the roots actually acted on (whole-root
    // entries), so scoped trail views surface this import.
    let roots = repo::root::fetch_all(conn)?;
    let scope_pairs: Vec<DecisionScope> = state
        .touched_roots
        .iter()
        .filter_map(|root_id| {
            roots
                .iter()
                .find(|r| r.id == *root_id)
                .map(|r| DecisionScope::new(r.id, r.path.clone(), String::new()))
        })
        .collect();
    if !scope_pairs.is_empty() {
        recorder.record_scopes(conn, &scope_pairs);
    }

    recorder.complete(
        conn,
        DecisionStatus::Completed,
        DecisionCounts {
            attempted: Some(state.stats.lines_processed as i64),
            completed: Some(state.stats.facts_imported as i64),
            failed: None,
            skipped: Some(state.stats.skipped_stale as i64),
        },
        &summary,
    );
    for w in recorder.take_warnings() {
        eprintln!("{w}");
    }

    println!("{}", summary);

    // Update query planner statistics after bulk changes
    eprintln!("Updating query statistics...");
    db.run_analyze()?;

    Ok(())
}
