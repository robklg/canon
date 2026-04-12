use anyhow::{Context, Result};
use std::io::{self, BufRead};

use crate::domain::config::LedgerConfig;
use crate::domain::decision::{DecisionCommand, DecisionStatus};
use crate::ops;
use crate::ops::decision::{DecisionCounts, DecisionParams, DecisionRecorder};
use crate::ops::import_facts::ImportRecord;
use crate::repo::Db;

pub fn run(db: &mut Db, allow_archived: bool, verbose: bool, command_line: &str, no_record: bool) -> Result<()> {
    let conn = db.conn_mut();
    let mut state = ops::import_facts::init_state(conn)?;

    let decision = DecisionParams {
        command: DecisionCommand::ImportFacts,
        scope: None,
        command_line: command_line.to_string(),
        reason: None,
        record_enabled: !no_record,
        receipt_enabled: false,
        ledger_config: LedgerConfig::default(),
    };
    let mut recorder = DecisionRecorder::start(conn, &decision);

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

        match ops::import_facts::process_record(conn, &record, &mut state, allow_archived) {
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
