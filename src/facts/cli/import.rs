use anyhow::Result;
use std::collections::{BTreeSet, HashMap};
use std::io::{self, BufRead, Write};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::time::{Duration, Instant};

use crate::core::domain::config::{LedgerConfig, RecordingMode};
use crate::core::domain::conversion::Loss;
use crate::core::domain::decision::{DecisionCommand, DecisionStatus};
use crate::core::domain::fact::{FactType, KeyTypes};
use crate::core::domain::format::first_chars;
use crate::core::domain::format_count;
use crate::core::domain::scope::DecisionScope;
use crate::core::domain::time::zone::ReadTimesOn;
use crate::core::ops::decision::{
    ArmedRecorder, DecisionCounts, DecisionParams, DecisionRecorder, UnwindTerminal,
};
use crate::core::repo::{self, Connection, Db};
use crate::expr::{convert_command, mixed_type_way_out, printed_key};
use crate::facts::domain::{KnownHint, Misfit, Reading, SkipCause};
use crate::facts::ops::import::{
    self as facts_import, ImportRecord, ImportStats, RecordSkip, Skipped, VerboseLine,
    BATCH_RECORDS, BATCH_WINDOW,
};

/// The roots this run actually touched, as whole-root scopes.
///
/// The command reads a stream, so no scope exists at `start()`; these are what
/// make the row visible from the folder the user was enriching instead of
/// behind a scoped view's "global decisions not shown" footer. Reached from
/// both the completing path and the failure arm, which is the point — a run
/// that died mid-stream is the one a user is most likely to go looking for.
fn touched_scopes(conn: &Connection, touched_roots: &BTreeSet<i64>) -> Result<Vec<DecisionScope>> {
    let roots = repo::root::fetch_all(conn)?;
    Ok(touched_roots
        .iter()
        .filter_map(|root_id| {
            roots
                .iter()
                .find(|r| r.id == *root_id)
                .map(|r| DecisionScope::new(r.id, r.path.clone(), String::new()))
        })
        .collect())
}

/// The decision row's counts: records read in committed batches, facts imported, and records
/// set aside whole (a value's skip carries another unit, which the summary states).
fn counts(stats: &ImportStats) -> DecisionCounts {
    DecisionCounts {
        attempted: Some(stats.records as i64),
        completed: Some(stats.facts_imported as i64),
        failed: None,
        skipped: Some(stats.records_skipped() as i64),
    }
}

/// Settle the row from the failure arm, with everything the loop holds.
///
/// The guard below would settle this row anyway, so this is the floor being
/// raised rather than the only settlement: here the counts, the touched roots
/// and the error text are all in hand, and none of them reaches a cleanup that
/// cannot see the error. The word is `partial` because each batch commits on
/// its own — the facts in committed batches stand.
///
/// Best-effort throughout: a scope fetch that fails leaves the row scope-less
/// rather than replacing one true record with none.
fn settle_early(
    recorder: &mut ArmedRecorder<'_>,
    conn: &Connection,
    stats: &ImportStats,
    touched_roots: &BTreeSet<i64>,
    error: &anyhow::Error,
    err: &mut impl Write,
) {
    if let Ok(scopes) = touched_scopes(conn, touched_roots) {
        if !scopes.is_empty() {
            recorder.record_scopes(conn, &scopes);
        }
    }
    let summary = stats.compose_early_stop(error);
    recorder.complete(conn, DecisionStatus::Partial, counts(stats), &summary);
    for w in recorder.take_warnings() {
        let _ = writeln!(err, "{w}");
    }
}

/// Read the input's lines on a thread of their own, outside any transaction, so a batch never
/// holds the write lock while waiting for a slow pipeline. The thread is detached: a run that
/// ends early does not wait on an input that may never close. It stops after a read error.
fn read_lines(input: impl BufRead + Send + 'static) -> Receiver<io::Result<String>> {
    let (send, lines) = mpsc::sync_channel(BATCH_RECORDS);
    std::thread::spawn(move || {
        for line in input.lines() {
            let broke = line.is_err();
            if send.send(line).is_err() || broke {
                break;
            }
        }
    });
    lines
}

/// Why a batch closed.
enum Closed {
    /// It holds `BATCH_RECORDS` records.
    Full,
    /// The window passed since its first record.
    Window,
    /// The input ended.
    End,
    /// The input broke.
    ReadError(io::Error),
}

/// Take the next batch: block for its first record, then take records until the batch is full
/// or its window has passed. A blank line is skipped; a line that is not a record is warned
/// about and not counted.
fn next_batch(
    lines: &Receiver<io::Result<String>>,
    window: Duration,
    line_number: &mut u64,
    err: &mut impl Write,
) -> (Vec<ImportRecord>, Closed) {
    let mut batch = Vec::new();
    let mut closes_at: Option<Instant> = None;
    loop {
        let next = match closes_at {
            None => lines.recv().map_err(|_| RecvTimeoutError::Disconnected),
            Some(at) => lines.recv_timeout(at.saturating_duration_since(Instant::now())),
        };
        let line = match next {
            Ok(Ok(line)) => line,
            Ok(Err(e)) => return (batch, Closed::ReadError(e)),
            Err(RecvTimeoutError::Disconnected) => return (batch, Closed::End),
            Err(RecvTimeoutError::Timeout) => return (batch, Closed::Window),
        };
        if line.trim().is_empty() {
            continue;
        }
        *line_number += 1;
        match serde_json::from_str::<ImportRecord>(&line) {
            Ok(record) => {
                closes_at.get_or_insert_with(|| Instant::now() + window);
                batch.push(record);
                if batch.len() >= BATCH_RECORDS {
                    return (batch, Closed::Full);
                }
            }
            Err(e) => {
                let _ = writeln!(err, "Warning: Failed to parse line {line_number}: {e}");
            }
        }
    }
}

/// `input` rather than stdin directly: the caller passes stdin, and a test passes a reader that
/// fails partway, which is the only way to reach this window's error arm from a test at all.
/// It is read on a thread of its own, so it must be `Send` and own what it reads. `err` takes
/// every line the run writes besides its summary; the caller passes stderr, and a test a buffer
/// it can read.
#[allow(clippy::too_many_arguments)]
pub fn import_run(
    db: &mut Db,
    input: impl BufRead + Send + 'static,
    allow_archived: bool,
    verbose: bool,
    command_line: &str,
    config: &LedgerConfig,
    no_receipt: bool,
    err: &mut impl Write,
) -> Result<()> {
    import_closing_batches(
        db,
        input,
        allow_archived,
        verbose,
        command_line,
        config,
        no_receipt,
        BATCH_WINDOW,
        err,
    )
}

/// The run, with a batch closing `window` after its first record. Apart from the window it is
/// `import_run`, which passes `BATCH_WINDOW`; a test that counts batches by their size passes
/// one it cannot reach.
#[allow(clippy::too_many_arguments)]
fn import_closing_batches(
    db: &mut Db,
    input: impl BufRead + Send + 'static,
    allow_archived: bool,
    verbose: bool,
    command_line: &str,
    config: &LedgerConfig,
    no_receipt: bool,
    window: Duration,
    err: &mut impl Write,
) -> Result<()> {
    let conn = db.conn();

    let decision = DecisionParams {
        command: DecisionCommand::ImportFacts,
        scope: Vec::new(),
        command_line: command_line.to_string(),
        reason: None,
        record_enabled: config.recording != RecordingMode::Off,
        receipt_enabled: config.recording == RecordingMode::Full && !no_receipt,
        ledger_config: config.clone(),
        zone: ReadTimesOn::of_user(crate::user_zone()),
    };
    // Armed as the backstop for anything the two explicit arms below do not
    // reach, and before any batch opens a transaction on this connection. Both
    // arms settle first, which makes the guard a no-op there.
    let mut recorder = DecisionRecorder::start(conn, &decision, None).armed(
        conn,
        UnwindTerminal {
            status: DecisionStatus::Partial,
            summary: "Import ended early; facts in committed batches stand".to_string(),
        },
    );

    let lines = read_lines(input);
    let mut stats = ImportStats::default();
    let mut touched_roots = BTreeSet::new();
    let mut line_number = 0u64;
    let ended_early = loop {
        let (batch, closed) = next_batch(&lines, window, &mut line_number, err);
        if !batch.is_empty() {
            match facts_import::import_batch(conn, &batch, allow_archived) {
                Ok(outcome) => {
                    for warning in &outcome.warnings {
                        let _ = writeln!(err, "Warning: {warning}");
                    }
                    if verbose {
                        for line in verbose_lines(&outcome.verbose) {
                            let _ = writeln!(err, "{line}");
                        }
                    }
                    stats.add(outcome.stats);
                    touched_roots.extend(outcome.touched_roots);
                }
                Err(e) => break Some(e),
            }
        }
        match closed {
            Closed::Full | Closed::Window => {}
            Closed::End => break None,
            Closed::ReadError(e) => {
                break Some(anyhow::Error::from(e).context("Failed to read line from stdin"))
            }
        }
    };

    let at_end = facts_import::types_at_end(conn, &keys_needing_their_end_type(&stats));
    for line in skip_lines(&stats, at_end.as_ref().ok(), verbose) {
        let _ = writeln!(err, "{line}");
    }

    if let Some(e) = ended_early {
        settle_early(&mut recorder, conn, &stats, &touched_roots, &e, err);
        return Err(e);
    }
    if let Err(e) = at_end {
        settle_early(&mut recorder, conn, &stats, &touched_roots, &e, err);
        return Err(e);
    }

    let summary = stats.compose_summary();

    let scope_pairs = match touched_scopes(conn, &touched_roots) {
        Ok(scopes) => scopes,
        Err(e) => {
            settle_early(&mut recorder, conn, &stats, &touched_roots, &e, err);
            return Err(e);
        }
    };
    if !scope_pairs.is_empty() {
        recorder.record_scopes(conn, &scope_pairs);
    }

    recorder.complete(conn, DecisionStatus::Completed, counts(&stats), &summary);
    for w in recorder.take_warnings() {
        let _ = writeln!(err, "{w}");
    }

    println!("{}", summary);

    // Update query planner statistics after bulk changes
    let _ = writeln!(err, "Updating query statistics...");
    db.run_analyze()?;

    Ok(())
}

/// The `--verbose` lines of a batch: each file's header, then a line per value, the header
/// printed only before its file's first line, so a file whose values print nothing (nulls,
/// source facts passed back) prints no header.
fn verbose_lines(lines: &[VerboseLine]) -> Vec<String> {
    let mut printed = Vec::new();
    let mut header = None;
    for line in lines {
        match line {
            VerboseLine::Source { .. } => header = verbose_line(line),
            VerboseLine::Stored { .. } | VerboseLine::Skipped { .. } => {
                if let Some(text) = verbose_line(line) {
                    printed.extend(header.take());
                    printed.push(text);
                }
            }
        }
    }
    printed
}

/// One `--verbose` line; none for a value that is nothing to fix (a null, a source fact passed
/// back).
fn verbose_line(line: &VerboseLine) -> Option<String> {
    match line {
        VerboseLine::Source {
            root_path,
            rel_path,
        } => Some(format!("[{root_path}] {rel_path}")),
        VerboseLine::Stored {
            key,
            written,
            on_content,
        } => Some(format!(
            "  {key}: {written} ({})",
            if *on_content {
                "on object"
            } else {
                "on source"
            }
        )),
        VerboseLine::Skipped {
            key,
            written,
            cause,
        } => {
            way_out(key, cause, None).map(|way| format!("  {key}: {written} skipped ({})", way.one))
        }
    }
}

/// The keys whose type at the end of the run decides their way out.
fn keys_needing_their_end_type(stats: &ImportStats) -> Vec<&str> {
    stats
        .skipped_values
        .keys()
        .filter(|(_, cause)| matches!(cause, SkipCause::NotExact | SkipCause::MixedKey))
        .map(|(key, _)| key.as_str())
        .collect()
}

/// A cause's line: how one value and several are said, and the way out that fits it.
struct WayOut {
    one: String,
    many: String,
    way_out: String,
    /// Lines printed below the line.
    below: Vec<String>,
}

/// The type a key holds when the lines print, for the one cause whose way out depends on it.
enum AtEnd<'a> {
    NoType,
    Holds(FactType),
    Mixed(&'a KeyTypes),
}

/// **The way-out table**: each skip cause's words and the way out that fits it, one arm per
/// cause. `None` for a null value and a source fact passed back, which print no line. The
/// way out of a number Canon cannot hold exactly follows the type its key ended the run in
/// (`at_end`, read once after the last committed batch); with none given it is not asked for.
fn way_out(
    key: &str,
    cause: &SkipCause,
    at_end: Option<&HashMap<String, KeyTypes>>,
) -> Option<WayOut> {
    let line = |one: &str, many: &str, way_out: String| WayOut {
        one: one.to_string(),
        many: many.to_string(),
        way_out,
        below: Vec::new(),
    };
    let convert = convert_command(key, None);
    let to_text = convert_command(key, Some(FactType::Text));
    let mixed_lines = |types: Option<&KeyTypes>| match types {
        Some(types) => mixed_type_way_out(&[(key, types)]),
        None => Vec::new(),
    };
    let mixed = "these values arrive when the pipeline runs again after the key is converted";
    Some(match cause {
        SkipCause::NoValue | SkipCause::SourceFact => return None,
        SkipCause::DoesNotRead(Reading::Number) => line(
            "does not read as a number",
            "do not read as a number",
            format!(
                "have the tool write numbers or omit values that are no number, or {convert} to \
                 see the other ways"
            ),
        ),
        SkipCause::DoesNotRead(Reading::Time) => line(
            "does not read as a time",
            "do not read as a time",
            format!(
                "have the tool write a time Canon reads (see \"How Canon interprets a time\" in \
                 the facts documentation) or omit values that are no time (an empty EXIF date \
                 such as 0000:00:00 00:00:00 is best omitted), or {convert} to see the other ways"
            ),
        ),
        SkipCause::WouldLose(Loss::LeadingZero | Loss::TrailingZero) => line(
            "would lose what the text carried",
            "would lose what the text carried",
            format!(
                "{to_text} to keep values like these as written, or have the tool write them as \
                 JSON numbers"
            ),
        ),
        SkipCause::WouldLose(Loss::NotExact) => line(
            "would lose what the text carried",
            "would lose what the text carried",
            format!("{to_text} to keep values like these as written"),
        ),
        SkipCause::NumberForTime => line(
            "is a number, and the key holds times",
            "are numbers, and the key holds times",
            "have the tool write the time as text (a year as \"2019\"), or, for a year, write \
             the number with the datetime hint"
                .to_string(),
        ),
        SkipCause::NotExact => {
            let end = match at_end.and_then(|types| types.get(key)) {
                None => AtEnd::NoType,
                Some(types) => match types.single() {
                    Some(t) => AtEnd::Holds(t),
                    None => AtEnd::Mixed(types),
                },
            };
            let (way_out, below) = match end {
                AtEnd::Holds(FactType::Num) => (
                    format!("{to_text} to keep values like these as written"),
                    Vec::new(),
                ),
                AtEnd::NoType => (
                    "have the tool write them as JSON strings to keep them as text".to_string(),
                    Vec::new(),
                ),
                AtEnd::Holds(FactType::Text) => (
                    "the key now holds text, so these arrive as written when the pipeline runs \
                     again"
                        .to_string(),
                    Vec::new(),
                ),
                // A path is never a stored key's type; it answers as a key that takes no number.
                AtEnd::Holds(FactType::Time | FactType::Path) => (
                    "a time key does not take numbers: import them under a new key".to_string(),
                    Vec::new(),
                ),
                AtEnd::Mixed(types) => (mixed.to_string(), mixed_lines(Some(types))),
            };
            WayOut {
                one: "is a number Canon cannot hold exactly".to_string(),
                many: "are numbers Canon cannot hold exactly".to_string(),
                way_out,
                below,
            }
        }
        SkipCause::BoolForTime => line(
            "is true or false, and the key holds times",
            "are true or false, and the key holds times",
            "a time key does not take true or false: import them under a new key, or have the \
             tool write times"
                .to_string(),
        ),
        SkipCause::Structure(Reading::Number) => line(
            "is a list or object, and the key holds numbers",
            "are lists or objects, and the key holds numbers",
            "a number key does not take lists or objects: import them under a new key, or have \
             the tool write numbers"
                .to_string(),
        ),
        SkipCause::Structure(Reading::Time) => line(
            "is a list or object, and the key holds times",
            "are lists or objects, and the key holds times",
            "a time key does not take lists or objects: import them under a new key, or have \
             the tool write times"
                .to_string(),
        ),
        SkipCause::HintMisfit(Misfit::DatetimeForText) => line(
            "is hinted datetime, and the key holds text",
            "are hinted datetime, and the key holds text",
            format!(
                "{} if this key should hold times; otherwise have the tool write text",
                convert_command(key, Some(FactType::Time))
            ),
        ),
        SkipCause::HintMisfit(Misfit::DurationForText) => line(
            "is hinted duration, and the key holds text",
            "are hinted duration, and the key holds text",
            format!(
                "import the durations under a new key, or {convert} to see the ways for \
                 durations; otherwise have the tool write text"
            ),
        ),
        SkipCause::HintMisfit(Misfit::DatetimeForNumber) => line(
            "is hinted datetime, and the key holds numbers",
            "are hinted datetime, and the key holds numbers",
            "a number key does not take times: import the times under a new key, or have the \
             tool write numbers"
                .to_string(),
        ),
        SkipCause::HintMisfit(Misfit::DurationForTime) => line(
            "is hinted duration, and the key holds times",
            "are hinted duration, and the key holds times",
            "a duration is not a time: import durations under a new key".to_string(),
        ),
        SkipCause::HintDoesNotRead(KnownHint::Datetime) => line(
            "does not read as its datetime hint",
            "do not read as their datetime hint",
            "have the tool correct these values, or omit them (an empty EXIF date such as \
             0000:00:00 00:00:00 is best omitted)"
                .to_string(),
        ),
        SkipCause::HintDoesNotRead(KnownHint::Duration) => line(
            "does not read as its duration hint",
            "do not read as their duration hint",
            "have the tool correct these values, or omit them".to_string(),
        ),
        SkipCause::HintDoesNotRead(KnownHint::Number) => line(
            "does not read as its number hint",
            "do not read as their number hint",
            "have the tool correct these values, or omit them".to_string(),
        ),
        SkipCause::UnknownHint(hint) => line(
            &format!("carries the hint `{hint}`, which Canon does not read"),
            &format!("carry the hint `{hint}`, which Canon does not read"),
            "Canon reads three type hints, datetime, duration and number; have the tool drop \
             this one, and the key decides"
                .to_string(),
        ),
        SkipCause::MixedKey => WayOut {
            one: "is for a key of mixed type".to_string(),
            many: "are for a key of mixed type".to_string(),
            way_out: mixed.to_string(),
            below: mixed_lines(at_end.and_then(|types| types.get(key))),
        },
        SkipCause::NamedTwice => line(
            "is for a key named twice in one record",
            "are for a key named twice in one record",
            "have the tool write each key under one name (a key passed back by `worklist --emit` \
             under the name it was emitted as)"
                .to_string(),
        ),
        SkipCause::SourceName => line(
            "is under a name starting with `source.`, which only Canon's own source facts have",
            "are under a name starting with `source.`, which only Canon's own source facts have",
            "have the tool name them another way (the key printed is the name as the tool wrote \
             it)"
            .to_string(),
        ),
    })
}

/// The longest example value a line shows, in characters.
const EXAMPLE_CHARS: usize = 60;

/// A skipped value as the line shows it: as the tool wrote it, cut at `EXAMPLE_CHARS`.
fn example(value: &str) -> String {
    if value.chars().count() > EXAMPLE_CHARS {
        format!("{}…", first_chars(value, EXAMPLE_CHARS))
    } else {
        value.to_string()
    }
}

/// One cause's line for one key, with the lines below it.
fn skip_line(
    key: &str,
    cause: &SkipCause,
    skipped: &Skipped,
    at_end: Option<&HashMap<String, KeyTypes>>,
    verbose: bool,
) -> Vec<String> {
    let Some(way) = way_out(key, cause, at_end) else {
        return Vec::new();
    };
    let value = example(skipped.most_frequent().unwrap_or_default());
    let first = if skipped.count == 1 {
        format!(
            "{}: 1 value, {value}, {}; {}",
            printed_key(key),
            way.one,
            way.way_out
        )
    } else {
        format!(
            "{}: {} values, such as {value}, {}{}; {}",
            printed_key(key),
            format_count(skipped.count),
            way.many,
            if verbose {
                ""
            } else {
                " (--verbose lists each)"
            },
            way.way_out
        )
    };
    std::iter::once(first).chain(way.below).collect()
}

/// The lines stating what a run skipped, before its summary: one per key and cause, in key
/// order then cause order, then the line for records in archive roots.
fn skip_lines(
    stats: &ImportStats,
    at_end: Option<&HashMap<String, KeyTypes>>,
    verbose: bool,
) -> Vec<String> {
    let mut lines: Vec<String> = stats
        .skipped_values
        .iter()
        .flat_map(|((key, cause), skipped)| skip_line(key, cause, skipped, at_end, verbose))
        .collect();
    let archived = stats
        .skipped_records
        .get(&RecordSkip::Archived)
        .copied()
        .unwrap_or_default();
    match archived {
        0 => {}
        1 => lines.push(
            "1 record names a source in an archive root; run the pipeline again with \
             `--allow archived` on `canon import-facts` to import facts for it"
                .to_string(),
        ),
        n => lines.push(format!(
            "{} records name sources in archive roots; run the pipeline again with \
             `--allow archived` on `canon import-facts` to import facts for them",
            format_count(n)
        )),
    }
    lines
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::testing::{insert_object, insert_root, insert_source, setup_test_db};
    use crate::facts::domain::Reading;
    use std::io::BufReader;

    fn skipped(values: &[(&str, u64)]) -> Skipped {
        Skipped {
            count: values.iter().map(|(_, n)| n).sum(),
            values: values.iter().map(|(v, n)| (v.to_string(), *n)).collect(),
        }
    }

    fn line(key: &str, cause: SkipCause, values: &[(&str, u64)]) -> Vec<String> {
        skip_line(key, &cause, &skipped(values), None, false)
    }

    /// Each cause's line against its exact text: the count, one value as the tool wrote it,
    /// the cause, and the way out that fits the cause — the "would lose" line fitted to what
    /// is lost.
    #[test]
    fn every_skip_cause_names_its_way_out() {
        use crate::facts::domain::{KnownHint as H, Misfit as M, SkipCause as C};
        let two = &[("\"x\"", 2)];
        let k = "content.K";
        let rows: Vec<(C, &str)> = vec![
            (
                C::DoesNotRead(Reading::Number),
                "content.K: 2 values, such as \"x\", do not read as a number (--verbose lists \
                 each); have the tool write numbers or omit values that are no number, or \
                 `canon facts convert content.K` to see the other ways",
            ),
            (
                C::DoesNotRead(Reading::Time),
                "content.K: 2 values, such as \"x\", do not read as a time (--verbose lists \
                 each); have the tool write a time Canon reads (see \"How Canon interprets a \
                 time\" in the facts documentation) or omit values that are no time (an empty \
                 EXIF date such as 0000:00:00 00:00:00 is best omitted), or `canon facts \
                 convert content.K` to see the other ways",
            ),
            (
                C::WouldLose(Loss::LeadingZero),
                "content.K: 2 values, such as \"x\", would lose what the text carried \
                 (--verbose lists each); `canon facts convert content.K --to text` to keep \
                 values like these as written, or have the tool write them as JSON numbers",
            ),
            (
                C::WouldLose(Loss::TrailingZero),
                "content.K: 2 values, such as \"x\", would lose what the text carried \
                 (--verbose lists each); `canon facts convert content.K --to text` to keep \
                 values like these as written, or have the tool write them as JSON numbers",
            ),
            (
                C::WouldLose(Loss::NotExact),
                "content.K: 2 values, such as \"x\", would lose what the text carried \
                 (--verbose lists each); `canon facts convert content.K --to text` to keep \
                 values like these as written",
            ),
            (
                C::NumberForTime,
                "content.K: 2 values, such as \"x\", are numbers, and the key holds times \
                 (--verbose lists each); have the tool write the time as text (a year as \
                 \"2019\"), or, for a year, write the number with the datetime hint",
            ),
            (
                C::BoolForTime,
                "content.K: 2 values, such as \"x\", are true or false, and the key holds times \
                 (--verbose lists each); a time key does not take true or false: import them \
                 under a new key, or have the tool write times",
            ),
            (
                C::Structure(Reading::Number),
                "content.K: 2 values, such as \"x\", are lists or objects, and the key holds \
                 numbers (--verbose lists each); a number key does not take lists or objects: \
                 import them under a new key, or have the tool write numbers",
            ),
            (
                C::Structure(Reading::Time),
                "content.K: 2 values, such as \"x\", are lists or objects, and the key holds \
                 times (--verbose lists each); a time key does not take lists or objects: \
                 import them under a new key, or have the tool write times",
            ),
            (
                C::HintMisfit(M::DatetimeForText),
                "content.K: 2 values, such as \"x\", are hinted datetime, and the key holds \
                 text (--verbose lists each); `canon facts convert content.K --to time` if this \
                 key should hold times; otherwise have the tool write text",
            ),
            (
                C::HintMisfit(M::DurationForText),
                "content.K: 2 values, such as \"x\", are hinted duration, and the key holds \
                 text (--verbose lists each); import the durations under a new key, or `canon \
                 facts convert content.K` to see the ways for durations; otherwise have the tool \
                 write text",
            ),
            (
                C::HintMisfit(M::DatetimeForNumber),
                "content.K: 2 values, such as \"x\", are hinted datetime, and the key holds \
                 numbers (--verbose lists each); a number key does not take times: import the \
                 times under a new key, or have the tool write numbers",
            ),
            (
                C::HintMisfit(M::DurationForTime),
                "content.K: 2 values, such as \"x\", are hinted duration, and the key holds \
                 times (--verbose lists each); a duration is not a time: import durations under \
                 a new key",
            ),
            (
                C::HintDoesNotRead(H::Datetime),
                "content.K: 2 values, such as \"x\", do not read as their datetime hint \
                 (--verbose lists each); have the tool correct these values, or omit them (an \
                 empty EXIF date such as 0000:00:00 00:00:00 is best omitted)",
            ),
            (
                C::HintDoesNotRead(H::Duration),
                "content.K: 2 values, such as \"x\", do not read as their duration hint \
                 (--verbose lists each); have the tool correct these values, or omit them",
            ),
            (
                C::HintDoesNotRead(H::Number),
                "content.K: 2 values, such as \"x\", do not read as their number hint \
                 (--verbose lists each); have the tool correct these values, or omit them",
            ),
            (
                C::UnknownHint("bogus".into()),
                "content.K: 2 values, such as \"x\", carry the hint `bogus`, which Canon does \
                 not read (--verbose lists each); Canon reads three type hints, datetime, \
                 duration and number; have the tool drop this one, and the key decides",
            ),
            (
                C::NamedTwice,
                "content.K: 2 values, such as \"x\", are for a key named twice in one record \
                 (--verbose lists each); have the tool write each key under one name (a key \
                 passed back by `worklist --emit` under the name it was emitted as)",
            ),
            (
                C::SourceName,
                "content.K: 2 values, such as \"x\", are under a name starting with `source.`, \
                 which only Canon's own source facts have (--verbose lists each); have the tool \
                 name them another way (the key printed is the name as the tool wrote it)",
            ),
        ];
        for (cause, expected) in rows {
            assert_eq!(line(k, cause.clone(), two), [expected], "{cause:?}");
        }

        // A key of mixed type: the line, then the way out the mixed type's owner prints.
        let mixed = KeyTypes::from_counts([(FactType::Text, 2), (FactType::Num, 1)]).unwrap();
        let at_end = HashMap::from([(k.to_string(), mixed)]);
        assert_eq!(
            skip_line(k, &C::MixedKey, &skipped(two), Some(&at_end), false),
            [
                "content.K: 2 values, such as \"x\", are for a key of mixed type (--verbose \
                 lists each); these values arrive when the pipeline runs again after the key is \
                 converted",
                "To see its values and convert it to one type:",
                "  `canon facts convert content.K`",
            ]
        );
    }

    /// A number Canon cannot hold exactly: one line per key, its way out read from the type
    /// the key ended the run in, so one key never prints two answers.
    #[test]
    fn a_not_exact_line_follows_the_type_its_key_ends_in() {
        let k = "content.Serial";
        let ends = |t: Option<KeyTypes>| -> String {
            let at_end: HashMap<String, KeyTypes> =
                t.into_iter().map(|t| (k.to_string(), t)).collect();
            skip_line(
                k,
                &SkipCause::NotExact,
                &skipped(&[("9007199254740993", 1)]),
                Some(&at_end),
                false,
            )
            .join("\n")
        };
        let holds = |t| KeyTypes::from_counts([(t, 1)]);
        assert_eq!(
            ends(holds(FactType::Num)),
            "content.Serial: 1 value, 9007199254740993, is a number Canon cannot hold exactly; \
             `canon facts convert content.Serial --to text` to keep values like these as written"
        );
        assert_eq!(
            ends(None),
            "content.Serial: 1 value, 9007199254740993, is a number Canon cannot hold exactly; \
             have the tool write them as JSON strings to keep them as text"
        );
        assert_eq!(
            ends(holds(FactType::Text)),
            "content.Serial: 1 value, 9007199254740993, is a number Canon cannot hold exactly; \
             the key now holds text, so these arrive as written when the pipeline runs again"
        );
        assert_eq!(
            ends(holds(FactType::Time)),
            "content.Serial: 1 value, 9007199254740993, is a number Canon cannot hold exactly; \
             a time key does not take numbers: import them under a new key"
        );
        assert_eq!(
            ends(KeyTypes::from_counts([
                (FactType::Text, 1),
                (FactType::Num, 1)
            ])),
            "content.Serial: 1 value, 9007199254740993, is a number Canon cannot hold exactly; \
             these values arrive when the pipeline runs again after the key is converted\n\
             To see its values and convert it to one type:\n  \
             `canon facts convert content.Serial`"
        );
    }

    /// Through a real import: a key new at the run's start whose first value Canon cannot
    /// hold exactly prints one line, its way out following what the rest of the run made of
    /// the key.
    #[test]
    fn a_not_exact_line_is_one_per_key_and_follows_the_key_at_the_end() {
        // After a number the key holds numbers, and the third record's too-large number is
        // skipped as well; after text the key holds text, and that number lands as written.
        for (later, expected_start, expected_end) in [
            (
                "12",
                "content.Serial: 2 values, such as 9007199254740993, ",
                "`canon facts convert content.Serial --to text` to keep values like these as written",
            ),
            (
                "\"SN12AB\"",
                "content.Serial: 1 value, 9007199254740993, ",
                "the key now holds text, so these arrive as written when the pipeline runs again",
            ),
        ] {
            let conn = setup_test_db();
            let root = insert_root(&conn, "/photos", "source", false);
            let a = insert_source(&conn, root, "a.jpg", None);
            let b = insert_source(&conn, root, "b.jpg", None);
            let c = insert_source(&conn, root, "c.jpg", None);
            let records: Vec<ImportRecord> = [
                format!(r#"{{"source_id":{a},"basis_rev":0,"facts":{{"Serial":9007199254740993}}}}"#),
                format!(r#"{{"source_id":{b},"basis_rev":0,"facts":{{"Serial":{later}}}}}"#),
                format!(r#"{{"source_id":{c},"basis_rev":0,"facts":{{"Serial":9007199254740995}}}}"#),
            ]
            .iter()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
            let outcome = facts_import::import_batch(&conn, &records, false).unwrap();
            let at_end = facts_import::types_at_end(&conn, &keys_needing_their_end_type(&outcome.stats)).unwrap();
            let lines = skip_lines(&outcome.stats, Some(&at_end), false);
            assert_eq!(lines.len(), 1, "{later}: {lines:?}");
            assert!(lines[0].ends_with(expected_end), "{later}: {}", lines[0]);
            assert!(lines[0].starts_with(expected_start), "{later}: {}", lines[0]);
        }
    }

    /// **A file's header prints only before a line of its own.** A file whose values print
    /// nothing gets no header; a file with a skipped value that prints, and one with a stored
    /// value, each get theirs.
    #[test]
    fn a_file_whose_values_print_nothing_prints_no_header() {
        let header = |rel: &str| VerboseLine::Source {
            root_path: "/photos".into(),
            rel_path: rel.into(),
        };
        let lines = [
            header("a.jpg"),
            VerboseLine::Skipped {
                key: "content.K".into(),
                written: "null".into(),
                cause: SkipCause::NoValue,
            },
            VerboseLine::Skipped {
                key: "source.mtime".into(),
                written: "5".into(),
                cause: SkipCause::SourceFact,
            },
            header("b.jpg"),
            VerboseLine::Stored {
                key: "content.K".into(),
                written: "\"x\"".into(),
                on_content: false,
            },
            header("c.jpg"),
            VerboseLine::Skipped {
                key: "source.foo".into(),
                written: "1".into(),
                cause: SkipCause::SourceName,
            },
        ];
        let printed = verbose_lines(&lines);
        assert_eq!(
            printed[..3],
            [
                "[/photos] b.jpg",
                "  content.K: \"x\" (on source)",
                "[/photos] c.jpg"
            ]
        );
        assert_eq!(printed.len(), 4, "{printed:?}");
        assert!(
            printed[3].starts_with("  source.foo: 1 skipped ("),
            "{printed:?}"
        );
    }

    /// A null value and a source fact passed back are nothing to fix: no line. A name
    /// starting with `source.` that no source fact has is a tool mistake: a line.
    #[test]
    fn null_and_source_facts_print_no_line() {
        let mut stats = ImportStats::default();
        for (key, cause) in [
            ("content.K", SkipCause::NoValue),
            ("source.mtime", SkipCause::SourceFact),
            ("source.foo", SkipCause::SourceName),
        ] {
            stats
                .skipped_values
                .insert((key.to_string(), cause), skipped(&[("1", 1)]));
        }
        let lines = skip_lines(&stats, None, false);
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert!(lines[0].starts_with("source.foo: 1 value, 1, is under a name starting with"));
        for (key, cause) in [
            ("content.K", SkipCause::NoValue),
            ("source.mtime", SkipCause::SourceFact),
        ] {
            assert_eq!(
                verbose_line(&VerboseLine::Skipped {
                    key: key.into(),
                    written: "null".into(),
                    cause,
                }),
                None,
                "no --verbose line, not even an empty one"
            );
        }
    }

    /// The line names the value written most often, right after the count, and points at
    /// `--verbose` unless the run was verbose; one value takes the singular; a long value is
    /// cut at 60 characters.
    #[test]
    fn a_line_names_its_most_frequent_value_and_points_at_verbose() {
        let cause = SkipCause::DoesNotRead(Reading::Time);
        let values = skipped(&[("\"0000:00:00 00:00:00\"", 3), ("\"misc\"", 1)]);
        let plain = skip_line("content.CreateDate", &cause, &values, None, false);
        assert!(plain[0].starts_with(
            "content.CreateDate: 4 values, such as \"0000:00:00 00:00:00\", do not read as a \
             time (--verbose lists each); "
        ));
        let verbose = skip_line("content.CreateDate", &cause, &values, None, true);
        assert!(verbose[0].starts_with(
            "content.CreateDate: 4 values, such as \"0000:00:00 00:00:00\", do not read as a \
             time; "
        ));

        let one = skip_line(
            "content.Taken",
            &SkipCause::NumberForTime,
            &skipped(&[("2019", 1)]),
            None,
            false,
        );
        assert_eq!(
            one[0],
            "content.Taken: 1 value, 2019, is a number, and the key holds times; have the \
             tool write the time as text (a year as \"2019\"), or, for a year, write the number \
             with the datetime hint"
        );
        let hinted = skip_line(
            "content.K",
            &SkipCause::HintDoesNotRead(KnownHint::Datetime),
            &skipped(&[("\"x\"", 1)]),
            None,
            false,
        );
        assert!(hinted[0]
            .starts_with("content.K: 1 value, \"x\", does not read as its datetime hint; "));

        let long = format!("\"{}\"", "a".repeat(80));
        let cut = skip_line("content.K", &cause, &skipped(&[(&long, 2)]), None, false);
        let shown = format!("such as {}…, do not", first_chars(&long, 60));
        assert!(cut[0].contains(&shown), "{}", cut[0]);

        assert_eq!(
            verbose_line(&VerboseLine::Skipped {
                key: "content.Taken".into(),
                written: "2019".into(),
                cause: SkipCause::NumberForTime,
            })
            .as_deref(),
            Some("  content.Taken: 2019 skipped (is a number, and the key holds times)")
        );
    }

    /// A key that needs quotes prints in the printing rule's form at the line's head, and in
    /// a command as the full stored key quoted for the shell only, so the command runs as
    /// pasted.
    #[test]
    fn a_key_is_printed_by_the_printing_rule_and_quoted_for_the_shell_in_a_command() {
        let lines = skip_line(
            "content.File Size",
            &SkipCause::WouldLose(Loss::NotExact),
            &skipped(&[("\"9007199254740993\"", 1)]),
            None,
            false,
        );
        assert_eq!(
            lines[0],
            "\"content.File Size\": 1 value, \"9007199254740993\", would lose what the text \
             carried; `canon facts convert 'content.File Size' --to text` to keep values like \
             these as written"
        );
    }

    #[test]
    fn the_archive_line_names_allow_archived() {
        let mut stats = ImportStats::default();
        stats.skipped_records.insert(RecordSkip::Archived, 3);
        assert_eq!(
            skip_lines(&stats, None, false),
            [
                "3 records name sources in archive roots; run the pipeline again with `--allow \
              archived` on `canon import-facts` to import facts for them"
            ]
        );
        stats.skipped_records.insert(RecordSkip::Archived, 1);
        assert_eq!(
            skip_lines(&stats, None, false),
            [
                "1 record names a source in an archive root; run the pipeline again with `--allow \
              archived` on `canon import-facts` to import facts for it"
            ]
        );
    }

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

        let err = crate::core::domain::time::zone::with_test_zone(
            crate::core::domain::time::zone::UserZone::utc(),
            || {
                import_run(
                    &mut db,
                    reader,
                    false,
                    false,
                    "canon import-facts",
                    &LedgerConfig::default(),
                    true,
                    &mut Vec::new(),
                )
            },
        )
        .unwrap_err();
        assert!(err.to_string().contains("Failed to read line from stdin"));

        // Counts read nullable: the guard's backstop writes `none()`, so a run
        // settled by the backstop fails on these assertions rather than on a NULL
        // column.
        let (status, summary, attempted, completed): (String, String, Option<i64>, Option<i64>) =
            db.conn()
                .query_row(
                    "SELECT status, summary, count_attempted, count_completed FROM decisions",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
                )
                .unwrap();
        assert_eq!(status, "partial");
        assert_eq!(
            attempted,
            Some(2),
            "both records were read, in a committed batch"
        );
        assert_eq!(completed, Some(2), "and both facts stand");
        assert!(
            summary.starts_with(
                "Import ended early after 2 records (2 facts imported in committed batches, \
                 which stand): "
            ),
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

    /// Input the test writes line by line and holds open until it drops the sender, as a
    /// slow pipeline does.
    struct HeldOpen {
        lines: Receiver<String>,
        pending: Vec<u8>,
        pos: usize,
    }

    impl std::io::Read for HeldOpen {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            if self.pos == self.pending.len() {
                match self.lines.recv() {
                    Ok(line) => {
                        self.pending = line.into_bytes();
                        self.pos = 0;
                    }
                    Err(_) => return Ok(0),
                }
            }
            let n = (self.pending.len() - self.pos).min(buf.len());
            buf[..n].copy_from_slice(&self.pending[self.pos..self.pos + n]);
            self.pos += n;
            Ok(n)
        }
    }

    fn held_open() -> (mpsc::Sender<String>, BufReader<HeldOpen>) {
        let (send, lines) = mpsc::channel();
        let input = HeldOpen {
            lines,
            pending: Vec::new(),
            pos: 0,
        };
        (send, BufReader::new(input))
    }

    fn record_line(source_id: i64, facts: &str) -> String {
        format!("{{\"source_id\":{source_id},\"basis_rev\":0,\"facts\":{facts}}}\n")
    }

    /// Run the import on a thread of its own, as the command runs beside its pipeline; the
    /// lines it writes come back with its result.
    fn run_beside(
        mut db: Db,
        input: impl BufRead + Send + 'static,
    ) -> std::thread::JoinHandle<(Result<()>, String)> {
        std::thread::spawn(move || {
            let mut err = Vec::new();
            let result = crate::core::domain::time::zone::with_test_zone(
                crate::core::domain::time::zone::UserZone::utc(),
                || {
                    import_run(
                        &mut db,
                        input,
                        false,
                        false,
                        "canon import-facts",
                        &LedgerConfig::default(),
                        true,
                        &mut err,
                    )
                },
            );
            (result, String::from_utf8(err).unwrap())
        })
    }

    fn open_file(path: &std::path::Path) -> Db {
        crate::core::repo::open_with_options(
            path,
            crate::core::repo::DbOptions {
                debug_sql: false,
                profile: false,
            },
        )
        .unwrap()
    }

    fn stored(conn: &Connection, source_id: i64, key: &str) -> bool {
        conn.query_row(
            "SELECT COUNT(*) FROM facts WHERE entity_id = ?1 AND key = ?2",
            rusqlite::params![source_id, key],
            |r| r.get::<_, i64>(0),
        )
        .unwrap()
            > 0
    }

    /// Wait until `until` holds, failing after `limit`.
    fn wait_for(limit: Duration, what: &str, mut until: impl FnMut() -> bool) -> Duration {
        let started = Instant::now();
        while !until() {
            assert!(started.elapsed() < limit, "{what} not within {limit:?}");
            std::thread::sleep(Duration::from_millis(5));
        }
        started.elapsed()
    }

    /// **A batch closes at 500 records**: 1,001 records commit in three transactions that
    /// write facts, counted by the connection's own hooks. One record fails mid-batch; it is
    /// undone and the others land.
    #[test]
    fn a_batch_closes_at_500_records() {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        use std::sync::Arc;

        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let sources: Vec<i64> = (0..1001)
            .map(|i| insert_source(&conn, root, &format!("{i}.jpg"), None))
            .collect();
        conn.execute_batch(
            "CREATE TEMP TRIGGER fail_boom BEFORE INSERT ON facts WHEN NEW.key = 'content.Boom' \
             BEGIN SELECT RAISE(ABORT, 'planted'); END",
        )
        .unwrap();
        let wrote_facts = Arc::new(AtomicBool::new(false));
        let commits = Arc::new(AtomicUsize::new(0));
        let flag = Arc::clone(&wrote_facts);
        conn.update_hook(Some(move |_, _: &str, table: &str, _| {
            if table == "facts" {
                flag.store(true, Ordering::SeqCst);
            }
        }));
        let (flag, counted) = (Arc::clone(&wrote_facts), Arc::clone(&commits));
        conn.commit_hook(Some(move || {
            if flag.swap(false, Ordering::SeqCst) {
                counted.fetch_add(1, Ordering::SeqCst);
            }
            false
        }));

        let input: String = sources
            .iter()
            .enumerate()
            .map(|(i, &s)| match i {
                700 => record_line(s, &format!("{{\"content.N\":{i},\"content.Boom\":\"x\"}}")),
                _ => record_line(s, &format!("{{\"content.N\":{i}}}")),
            })
            .collect();
        let mut db = Db::from_connection(conn);
        crate::core::domain::time::zone::with_test_zone(
            crate::core::domain::time::zone::UserZone::utc(),
            || {
                // A window no run reaches: every batch here closes on its size.
                import_closing_batches(
                    &mut db,
                    BufReader::new(std::io::Cursor::new(input.into_bytes())),
                    false,
                    false,
                    "canon import-facts",
                    &LedgerConfig::default(),
                    true,
                    Duration::from_secs(3600),
                    &mut Vec::new(),
                )
            },
        )
        .unwrap();

        assert_eq!(commits.load(Ordering::SeqCst), 3, "500, 500 and 1");
        let (stored, attempted, completed): (i64, i64, i64) = db
            .conn()
            .query_row(
                "SELECT (SELECT COUNT(*) FROM facts WHERE key = 'content.N'), \
                 count_attempted, count_completed FROM decisions",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(stored, 1000, "every record but the failing one");
        assert_eq!((attempted, completed), (1001, 1000));
    }

    /// **A batch closes about a second after its first record**, so a slow pipeline's facts
    /// do not wait on records that have not come: one record, the input held open, seen
    /// committed by a second connection no sooner than the window and within 3 s.
    #[test]
    fn a_batch_closes_about_a_second_after_its_first_record() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("canon.db");
        let db = open_file(&path);
        let root = insert_root(&db, "/photos", "source", false);
        let source = insert_source(&db, root, "a.jpg", None);
        let watcher = open_file(&path);

        let (send, input) = held_open();
        let run = run_beside(db, input);
        let sent = Instant::now();
        send.send(record_line(source, r#"{"content.Make":"Canon"}"#))
            .unwrap();
        wait_for(Duration::from_secs(3), "the record", || {
            stored(&watcher, source, "content.Make")
        });
        assert!(
            sent.elapsed() >= BATCH_WINDOW,
            "the batch waited for its window"
        );

        drop(send);
        let (result, _) = run.join().unwrap();
        result.unwrap();
    }

    /// **A locked database ends the run `partial`, and the committed batches stand.** The
    /// first batch commits a number and skips a text for the same key; then another writer
    /// holds the lock, and the second batch gives up waiting. The skipped value's line still
    /// prints, the row says how far the run got and why it stopped, and the second batch's
    /// fact is not there.
    ///
    /// The import's wait is the test's busy handler: it gives up once, which is the batch,
    /// and then waits until the lock is released, so the run's own settlement lands.
    #[test]
    fn a_locked_database_ends_the_run_partial_keeping_committed_batches() {
        use std::sync::atomic::{AtomicBool, Ordering};
        static GAVE_UP: AtomicBool = AtomicBool::new(false);
        fn give_up_once(_: i32) -> bool {
            if GAVE_UP.swap(true, Ordering::SeqCst) {
                std::thread::sleep(Duration::from_millis(5));
                true
            } else {
                false
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("canon.db");
        let db = open_file(&path);
        let root = insert_root(&db, "/photos", "source", false);
        let [first, second, third] =
            ["a.jpg", "b.jpg", "c.jpg"].map(|name| insert_source(&db, root, name, None));
        db.busy_handler(Some(give_up_once)).unwrap();
        let locker = open_file(&path);

        let (send, input) = held_open();
        let run = run_beside(db, input);
        send.send(record_line(first, r#"{"content.K":5}"#)).unwrap();
        send.send(record_line(second, r#"{"content.K":"abc"}"#))
            .unwrap();
        wait_for(Duration::from_secs(3), "the first batch", || {
            stored(&locker, first, "content.K")
        });

        locker.execute_batch("BEGIN IMMEDIATE").unwrap();
        send.send(record_line(third, r#"{"content.K":7}"#)).unwrap();
        wait_for(Duration::from_secs(5), "the second batch giving up", || {
            GAVE_UP.load(Ordering::SeqCst)
        });
        locker.execute_batch("ROLLBACK").unwrap();

        let (result, err) = run.join().unwrap();
        drop(send);
        assert!(result.is_err());
        assert!(
            err.contains(
                "content.K: 1 value, \"abc\", does not read as a number; have the tool write \
                 numbers or omit values that are no number, or `canon facts convert content.K` \
                 to see the other ways"
            ),
            "the committed batch's line prints: {err}"
        );
        let (status, summary, attempted, completed): (String, String, i64, i64) = locker
            .query_row(
                "SELECT status, summary, count_attempted, count_completed FROM decisions",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap();
        assert_eq!(status, "partial");
        assert!(
            summary.starts_with(
                "Import ended early after 2 records (1 fact imported in committed batches, \
                 which stand; 1 value skipped): "
            ),
            "{summary}"
        );
        assert!(summary.contains("database is locked"), "{summary}");
        assert_eq!((attempted, completed), (2, 1));
        assert!(stored(&locker, first, "content.K"));
        assert!(!stored(&locker, third, "content.K"));
    }
}
