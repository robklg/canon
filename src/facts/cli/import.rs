use anyhow::{Context, Result};
use std::io::BufRead;

use crate::core::domain::config::{LedgerConfig, RecordingMode};
use crate::core::domain::decision::{DecisionCommand, DecisionStatus};
use crate::core::domain::scope::DecisionScope;
use crate::core::ops::decision::{
    ArmedRecorder, DecisionCounts, DecisionParams, DecisionRecorder, UnwindTerminal,
};
use crate::core::repo::{self, Connection, Db};
use crate::facts::ops::import as facts_import;
use crate::facts::ops::import::ImportRecord;

/// The roots this run actually touched, as whole-root scopes.
///
/// The command reads a stream, so no scope exists at `start()`; these are what
/// make the row visible from the folder the user was enriching instead of
/// behind a scoped view's "global decisions not shown" footer. Reached from
/// both the completing path and the failure arm, which is the point — a run
/// that died mid-stream is the one a user is most likely to go looking for.
fn touched_scopes(
    conn: &Connection,
    state: &facts_import::ImportState,
) -> Result<Vec<DecisionScope>> {
    let roots = repo::root::fetch_all(conn)?;
    Ok(state
        .touched_roots
        .iter()
        .filter_map(|root_id| {
            roots
                .iter()
                .find(|r| r.id == *root_id)
                .map(|r| DecisionScope::new(r.id, r.path.clone(), String::new()))
        })
        .collect())
}

/// Settle the row from the failure arm, with everything the loop holds.
///
/// The guard below would settle this row anyway, so this is the floor being
/// raised rather than the only settlement: here the counts, the touched roots
/// and the error text are all in hand, and none of them reaches a cleanup that
/// cannot see the error. The word is `partial` because each record commits on
/// its own — the facts imported before the failure stand.
///
/// Best-effort throughout: a scope fetch that fails leaves the row scope-less
/// rather than replacing one true record with none.
fn settle_early(
    recorder: &mut ArmedRecorder<'_>,
    conn: &Connection,
    state: &facts_import::ImportState,
    error: &anyhow::Error,
) {
    if let Ok(scopes) = touched_scopes(conn, state) {
        if !scopes.is_empty() {
            recorder.record_scopes(conn, &scopes);
        }
    }
    let summary = format!(
        "Import ended early after {} lines ({} facts imported, which stand): {error:#}",
        state.stats.lines_processed, state.stats.facts_imported
    );
    recorder.complete(
        conn,
        DecisionStatus::Partial,
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
}

/// `input` rather than stdin directly: the caller passes `stdin.lock()`, and a
/// test passes a reader that fails partway, which is the only way to reach this
/// window's `?` from a test at all.
pub fn import_run(
    db: &mut Db,
    input: impl BufRead,
    allow_archived: bool,
    verbose: bool,
    command_line: &str,
    config: &LedgerConfig,
    no_receipt: bool,
) -> Result<()> {
    let conn = db.conn();
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
    // Armed as the backstop for anything the two explicit arms below do not
    // reach. Both of those settle first, which makes the guard a no-op there.
    let mut recorder = DecisionRecorder::start(conn, &decision, None).armed(
        conn,
        UnwindTerminal {
            status: DecisionStatus::Partial,
            summary: "Import ended early; facts imported before the failure stand".to_string(),
        },
    );

    for line in input.lines() {
        let line = match line.context("Failed to read line from stdin") {
            Ok(line) => line,
            Err(e) => {
                settle_early(&mut recorder, conn, &state, &e);
                return Err(e);
            }
        };
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

    let scope_pairs = match touched_scopes(conn, &state) {
        Ok(scopes) => scopes,
        Err(e) => {
            settle_early(&mut recorder, conn, &state, &e);
            return Err(e);
        }
    };
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::testing::{insert_object, insert_root, insert_source, setup_test_db};
    use std::io::BufReader;

    /// A stream that yields its bytes and then breaks, so the import's stdin
    /// `?` fires exactly where a truncated pipe would put it. This is what the
    /// `impl BufRead` parameter is for: the window is otherwise unreachable
    /// from a test.
    struct BreaksAfterItsBytes {
        data: Vec<u8>,
        pos: usize,
    }

    impl std::io::Read for BreaksAfterItsBytes {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            if self.pos >= self.data.len() {
                return Err(std::io::Error::other("the stream broke"));
            }
            let n = (self.data.len() - self.pos).min(buf.len());
            buf[..n].copy_from_slice(&self.data[self.pos..self.pos + n]);
            self.pos += n;
            Ok(n)
        }
    }

    /// The longest window in the story, and the one site that settles
    /// explicitly rather than leaving it to the guard — because here the
    /// counts, the touched roots and the error are all in hand, and none of
    /// them reaches a cleanup that cannot see the error.
    ///
    /// The scopes are the part worth holding: `record_scopes` normally runs
    /// after the loop, so a run that died mid-stream would leave a scope-less
    /// row, hidden from a scoped `trail` view behind its "global decisions not
    /// shown" footer — invisible from the very folder being enriched.
    ///
    /// *Smoke*: replace the failure arm with `?`. The guard then settles with
    /// empty counts, no scope and the backstop sentence, and every assertion
    /// below but the status fails.
    #[test]
    fn a_failed_import_settles_partial_with_its_counts_and_scopes() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "h1", false);
        let s1 = insert_source(&conn, root_id, "a.jpg", Some(obj));
        let obj2 = insert_object(&conn, "h2", false);
        let s2 = insert_source(&conn, root_id, "b.jpg", Some(obj2));
        let mut db = Db::from_connection(conn);

        let two_good_lines = format!(
            "{{\"source_id\":{s1},\"basis_rev\":0,\"facts\":{{\"content.Make\":\"Canon\"}}}}\n\
             {{\"source_id\":{s2},\"basis_rev\":0,\"facts\":{{\"content.Make\":\"Nikon\"}}}}\n"
        );
        let reader = BufReader::new(BreaksAfterItsBytes {
            data: two_good_lines.into_bytes(),
            pos: 0,
        });

        let err = import_run(
            &mut db,
            reader,
            false,
            false,
            "canon import-facts",
            &LedgerConfig::default(),
            true,
        )
        .unwrap_err();
        assert!(err.to_string().contains("Failed to read line from stdin"));

        // Counts read nullable: the guard's backstop writes `none()`, so the
        // smoke below fails on these assertions rather than on a NULL column.
        let (status, summary, attempted, completed): (String, String, Option<i64>, Option<i64>) =
            db.conn()
                .query_row(
                    "SELECT status, summary, count_attempted, count_completed FROM decisions",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
                )
                .unwrap();
        assert_eq!(status, "partial");
        assert_eq!(attempted, Some(2), "both lines were read and processed");
        assert_eq!(completed, Some(2), "and both facts stand");
        assert!(
            summary
                .starts_with("Import ended early after 2 lines (2 facts imported, which stand): "),
            "the counts come first and the error last: {summary}"
        );
        assert!(
            summary.contains("the stream broke"),
            "the one summary in this story that carries the error text: {summary}"
        );

        let scoped_root: i64 = db
            .conn()
            .query_row("SELECT root_id FROM decision_scopes", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            scoped_root, root_id,
            "the touched root must be on the row, or a scoped trail view hides it"
        );
    }
}
