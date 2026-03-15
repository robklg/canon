use anyhow::{Context, Result};
use std::io::{self, BufRead};

use crate::ceremony;
use crate::ops;
use crate::ops::import_facts::ImportRecord;
use crate::repo::Db;

pub fn run(db: &mut Db, allow_archived: bool, verbose: bool) -> Result<()> {
    let conn = db.conn_mut();
    let mut state = ops::import_facts::init_state(conn)?;

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

    let stats = &state.stats;
    println!(
        "Processed {} lines: {} facts imported, {} skipped (stale), {} skipped (reserved), {} skipped (archived), {} skipped (type mismatch), {} objects created, {} facts promoted",
        ceremony::format_count(stats.lines_processed),
        ceremony::format_count(stats.facts_imported),
        ceremony::format_count(stats.skipped_stale),
        ceremony::format_count(stats.skipped_reserved),
        ceremony::format_count(stats.skipped_archived),
        ceremony::format_count(stats.skipped_type_mismatch),
        ceremony::format_count(stats.objects_created),
        ceremony::format_count(stats.facts_promoted)
    );

    // Update query planner statistics after bulk changes
    db.run_analyze()?;

    Ok(())
}
