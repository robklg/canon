use anyhow::{bail, Context, Result};
use clap::Parser;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::collections::HashMap;
use std::io::{self, BufRead, Write};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Parser)]
#[command(name = "canonargs")]
#[command(about = "Run a command for each worklist entry and emit facts for import")]
struct Cli {
    /// Output mode: single fact with this key (command outputs single value)
    #[arg(long, group = "mode")]
    fact: Option<String>,

    /// Type hint for --fact mode (datetime, duration, number)
    #[arg(long, requires = "fact")]
    r#type: Option<String>,

    /// Output mode: key=value pairs (one per line)
    /// Use key:type=value to specify types (e.g., DateTimeOriginal:datetime=2024:07:23)
    #[arg(long, group = "mode")]
    kv: bool,

    /// Output mode: JSON object
    #[arg(long, group = "mode")]
    json: bool,

    /// Command and arguments to run ({} is replaced with file path)
    #[arg(last = true, required = true)]
    command: Vec<String>,
}

/// Input format - accepts both worklist entries and enriched entries (for chaining)
#[derive(Deserialize)]
struct InputEntry {
    source_id: i64,
    path: String,
    basis_rev: i64,
    /// The size and modification time the index held for the file at its last scan, which the
    /// worklist copies. A later `canonargs` in a chain reads the earlier one's output, which
    /// carries neither.
    #[serde(default)]
    size: Option<i64>,
    #[serde(default)]
    mtime: Option<i64>,
    // Optional fields from enriched input (for chaining), passed on as written
    #[serde(default)]
    facts: HashMap<String, Box<RawValue>>,
    // observed_at is ignored on input - we always use current time
}

#[derive(Serialize)]
struct FactOutput {
    source_id: i64,
    path: String, // Include path to enable chaining
    basis_rev: i64,
    observed_at: i64,
    facts: HashMap<String, Box<RawValue>>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Validate that exactly one mode is specified
    let mode = if let Some(ref key) = cli.fact {
        OutputMode::SingleFact {
            key: key.clone(),
            type_hint: cli.r#type.clone(),
        }
    } else if cli.kv {
        OutputMode::KeyValue
    } else if cli.json {
        OutputMode::Json
    } else {
        bail!("Must specify one of --fact <key>, --kv, or --json");
    };

    let stdin = io::stdin();
    let stdout = io::stdout();
    run(
        stdin.lock(),
        &mut stdout.lock(),
        &mut io::stderr(),
        &cli.command,
        &mode,
    )
}

/// Process every entry of `input`: facts to `out`, one warning per skipped entry to `err`, and,
/// when anything was skipped, one closing line on `err` counting the skips by cause.
fn run(
    input: impl BufRead,
    out: &mut impl Write,
    err: &mut impl Write,
    command: &[String],
    mode: &OutputMode,
) -> Result<()> {
    let mut tally = Tally::default();

    for line in input.lines() {
        let line = line.context("Failed to read line from stdin")?;
        if line.trim().is_empty() {
            continue;
        }
        tally.entries += 1;

        let entry: InputEntry = match serde_json::from_str(&line) {
            Ok(e) => e,
            Err(e) => {
                writeln!(err, "Warning: Failed to parse input entry: {}", e)?;
                tally.other += 1;
                continue;
            }
        };

        match process_entry(&entry, command, mode) {
            Ok(output) => {
                let json = serde_json::to_string(&output)?;
                writeln!(out, "{}", json)?;
            }
            Err(skipped) => {
                writeln!(err, "Warning: {}: {}", entry.path, skipped)?;
                tally.count(&skipped);
            }
        }
    }

    if let Some(line) = tally.closing_line() {
        writeln!(err, "{line}")?;
    }
    Ok(())
}

/// Why an entry produced no facts.
#[derive(Debug)]
enum Skipped {
    /// Nothing stands at the entry's path: its storage is away, or the file moved or went.
    NoFile,
    /// The file's size or modification time differs from what the last scan recorded.
    Changed,
    /// Anything else: an unreadable file, a failed command, output that held no fact.
    Other(anyhow::Error),
}

impl std::fmt::Display for Skipped {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Skipped::NoFile => write!(f, "no file at this path"),
            Skipped::Changed => write!(
                f,
                "the file changed after the last scan (its size or modification time differs); \
                 scan it, then make the worklist again"
            ),
            Skipped::Other(e) => write!(f, "{e}"),
        }
    }
}

/// The entries read and the ones skipped, by cause.
#[derive(Default)]
struct Tally {
    entries: usize,
    no_file: usize,
    changed: usize,
    other: usize,
}

impl Tally {
    fn count(&mut self, skipped: &Skipped) {
        match skipped {
            Skipped::NoFile => self.no_file += 1,
            Skipped::Changed => self.changed += 1,
            Skipped::Other(_) => self.other += 1,
        }
    }

    /// `Skipped N of M entries: A with no file, B changed after the last scan, C other (see the
    /// warnings above)`, a zero part left out; none when nothing was skipped.
    fn closing_line(&self) -> Option<String> {
        let skipped = self.no_file + self.changed + self.other;
        if skipped == 0 {
            return None;
        }
        let parts: Vec<String> = [
            (self.no_file, "with no file"),
            (self.changed, "changed after the last scan"),
            (self.other, "other"),
        ]
        .into_iter()
        .filter(|&(n, _)| n > 0)
        .map(|(n, words)| format!("{n} {words}"))
        .collect();
        Some(format!(
            "Skipped {skipped} of {} entries: {} (see the warnings above)",
            self.entries,
            parts.join(", ")
        ))
    }
}

/// The file at the entry's path, checked before the tool runs, so no value is read from a file
/// that is not the one the index describes. The read does not follow a link: only regular files
/// become sources, so a path that is now a link is not the file the scan saw. Where the entry
/// carries the last scan's size and modification time (a worklist entry), both must match.
fn check_file(entry: &InputEntry) -> Result<(), Skipped> {
    let metadata = match std::fs::symlink_metadata(&entry.path) {
        Ok(m) => m,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Err(Skipped::NoFile),
        Err(e) => return Err(Skipped::Other(anyhow::anyhow!("cannot read the file: {e}"))),
    };
    #[cfg(unix)]
    if let (Some(size), Some(mtime)) = (entry.size, entry.mtime) {
        use std::os::unix::fs::MetadataExt;
        if metadata.size() as i64 != size || metadata.mtime() != mtime {
            return Err(Skipped::Changed);
        }
    }
    #[cfg(not(unix))]
    let _ = metadata;
    Ok(())
}

enum OutputMode {
    SingleFact {
        key: String,
        type_hint: Option<String>,
    },
    KeyValue,
    Json,
}

fn process_entry(
    entry: &InputEntry,
    command_template: &[String],
    mode: &OutputMode,
) -> Result<FactOutput, Skipped> {
    check_file(entry)?;
    run_command(entry, command_template, mode).map_err(Skipped::Other)
}

fn run_command(
    entry: &InputEntry,
    command_template: &[String],
    mode: &OutputMode,
) -> Result<FactOutput> {
    // Build command by replacing {} with path
    let command: Vec<String> = command_template
        .iter()
        .map(|arg| {
            if arg == "{}" {
                entry.path.clone()
            } else {
                arg.replace("{}", &entry.path)
            }
        })
        .collect();

    if command.is_empty() {
        bail!("Empty command");
    }

    // Execute command
    let output = Command::new(&command[0])
        .args(&command[1..])
        .output()
        .with_context(|| format!("Failed to execute: {}", command[0]))?;

    if !output.status.success() {
        bail!(
            "Command failed with status {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let stdout = String::from_utf8(output.stdout).context("Command output is not valid UTF-8")?;

    // Parse output based on mode
    let new_facts = parse_output(&stdout, mode)?;

    if new_facts.is_empty() {
        bail!("No facts produced");
    }

    // Merge with existing facts from input (new facts override existing)
    let mut facts = entry.facts.clone();
    facts.extend(new_facts);

    Ok(FactOutput {
        source_id: entry.source_id,
        path: entry.path.clone(),
        basis_rev: entry.basis_rev,
        observed_at: current_timestamp(),
        facts,
    })
}

/// The text as a JSON number, written as given, where JSON reads it as one: serde_json
/// decides, and a number starts with `-` or a digit. Never read through a float, so Canon
/// sees the number the tool wrote.
fn json_number(value: &str) -> Option<Box<RawValue>> {
    if !matches!(value.chars().next(), Some('-' | '0'..='9')) {
        return None;
    }
    serde_json::from_str::<&RawValue>(value)
        .ok()
        .map(|raw| raw.to_owned())
}

fn made(value: serde_json::Value) -> Box<RawValue> {
    serde_json::value::to_raw_value(&value).expect("a JSON value serializes")
}

/// Create a typed fact value with type hint
fn typed_value(value: &str, type_hint: &str) -> Box<RawValue> {
    // For number type, pass it on as a JSON number where it is one. Any other text (`inf`,
    // `NaN`, `+5`, a word) is forwarded with its hint, never replaced, so the import states it.
    if type_hint == "number" {
        if let Some(n) = json_number(value) {
            return n;
        }
    }
    made(serde_json::json!({
        "value": value,
        "type": type_hint
    }))
}

fn parse_output(stdout: &str, mode: &OutputMode) -> Result<HashMap<String, Box<RawValue>>> {
    let mut facts = HashMap::new();

    match mode {
        OutputMode::SingleFact { key, type_hint } => {
            let value = stdout.trim();
            if value.is_empty() {
                bail!("Empty output");
            }
            let fact_value = match type_hint {
                Some(t) => typed_value(value, t),
                None => made(serde_json::Value::String(value.to_string())),
            };
            facts.insert(key.clone(), fact_value);
        }
        OutputMode::KeyValue => {
            for line in stdout.lines() {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                if let Some((key_part, value)) = line.split_once('=') {
                    let key_part = key_part.trim();
                    let value = value.trim();
                    if key_part.is_empty() {
                        continue;
                    }
                    // Parse key:type or just key
                    let (key, type_hint) = if let Some((k, t)) = key_part.split_once(':') {
                        (k.trim(), Some(t.trim()))
                    } else {
                        (key_part, None)
                    };
                    let fact_value = match type_hint {
                        Some(t) => typed_value(value, t),
                        None => made(serde_json::Value::String(value.to_string())),
                    };
                    facts.insert(key.to_string(), fact_value);
                } else {
                    eprintln!("Warning: Skipping malformed key=value line: {}", line);
                }
            }
        }
        OutputMode::Json => {
            // Each value is passed on as the extractor wrote it.
            let parsed: &RawValue =
                serde_json::from_str(stdout.trim()).context("Failed to parse JSON output")?;
            if !parsed.get().starts_with('{') {
                bail!("JSON output must be an object");
            }
            facts = serde_json::from_str(parsed.get()).context("Failed to parse JSON output")?;
        }
    }

    Ok(facts)
}

fn current_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A number is passed on as the JSON number it is; any other text is forwarded with its
    /// hint, never replaced. The defects it names: a value with no JSON number written as `0`,
    /// a zero nobody measured; and a number respelled through a float, `12` as `12.0`.
    #[test]
    fn a_number_with_no_json_number_is_forwarded_never_replaced() {
        assert_eq!(typed_value("12", "number").get(), "12");
        assert_eq!(typed_value("1e400", "number").get(), "1e400");
        for written in ["+5", "inf", "-inf", "NaN", "twelve"] {
            let forwarded: serde_json::Value =
                serde_json::from_str(typed_value(written, "number").get()).unwrap();
            assert_eq!(
                forwarded,
                serde_json::json!({"value": written, "type": "number"}),
                "{written}"
            );
        }
    }

    #[test]
    fn a_json_number_is_passed_on_as_written() {
        for written in ["9007199254740993", "1920", "1.10", "-0"] {
            assert_eq!(typed_value(written, "number").get(), written);
        }
    }

    /// A fact from an earlier stage reaches the output byte for byte.
    #[test]
    fn a_number_passed_through_a_chain_is_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("a.jpg");
        std::fs::write(&file, b"x").unwrap();
        let entry: InputEntry = serde_json::from_str(&format!(
            r#"{{"source_id":1,"path":{},"basis_rev":0,"facts":{{"content.A":1.10,"content.B":1e5,"content.C":-0,"content.D":123456789012345678901}}}}"#,
            serde_json::to_string(file.to_str().unwrap()).unwrap()
        ))
        .unwrap();
        let output = process_entry(
            &entry,
            &["echo".to_string(), "x".to_string()],
            &OutputMode::SingleFact {
                key: "content.E".to_string(),
                type_hint: None,
            },
        )
        .unwrap();
        let line = serde_json::to_string(&output).unwrap();
        for written in [
            r#""content.A":1.10"#,
            r#""content.B":1e5"#,
            r#""content.C":-0"#,
            r#""content.D":123456789012345678901"#,
            r#""content.E":"x""#,
        ] {
            assert!(line.contains(written), "{written} in {line}");
        }
    }

    #[test]
    fn a_json_extractor_number_is_passed_on_as_written() {
        let facts = parse_output(
            r#"{"A": 1.10, "B": -0, "C": 123456789012345678901}"#,
            &OutputMode::Json,
        )
        .unwrap();
        assert_eq!(facts["A"].get(), "1.10");
        assert_eq!(facts["B"].get(), "-0");
        assert_eq!(facts["C"].get(), "123456789012345678901");
        assert!(parse_output("[1]", &OutputMode::Json).is_err());
    }

    fn single_fact() -> OutputMode {
        OutputMode::SingleFact {
            key: "content.E".to_string(),
            type_hint: None,
        }
    }

    /// A worklist entry for `path`, carrying the size and modification time given.
    fn entry_line(path: &std::path::Path, size: Option<i64>, mtime: Option<i64>) -> String {
        let mut line = serde_json::json!({
            "source_id": 1,
            "path": path.to_str().unwrap(),
            "root_id": 1,
            "basis_rev": 0,
        });
        if let (Some(size), Some(mtime)) = (size, mtime) {
            line["size"] = size.into();
            line["mtime"] = mtime.into();
        }
        line.to_string()
    }

    fn entry_at(path: &std::path::Path, size: Option<i64>, mtime: Option<i64>) -> InputEntry {
        serde_json::from_str(&entry_line(path, size, mtime)).unwrap()
    }

    /// The size and modification time a scan would record for the file.
    fn scanned(path: &std::path::Path) -> (i64, i64) {
        use std::os::unix::fs::MetadataExt;
        let m = std::fs::symlink_metadata(path).unwrap();
        (m.size() as i64, m.mtime())
    }

    /// The defect it names: a tool that exits 0 and prints text for a missing file, its text
    /// stored as a value. The command here would succeed, and must never run.
    #[test]
    fn an_absent_file_is_skipped_before_the_command_runs() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("ran");
        let command = vec![
            "sh".to_string(),
            "-c".to_string(),
            format!("touch {}; echo x", marker.display()),
        ];
        let absent = dir.path().join("gone.jpg");
        for entry in [
            entry_at(&absent, Some(1), Some(1)),
            entry_at(&absent, None, None),
        ] {
            let Err(skipped) = process_entry(&entry, &command, &single_fact()) else {
                panic!("the entry ran");
            };
            assert!(matches!(skipped, Skipped::NoFile), "{skipped}");
            assert_eq!(skipped.to_string(), "no file at this path");
        }
        assert!(!marker.exists(), "the command ran for an absent file");
    }

    /// The defect it names: a value read from a file other than the one the last scan saw.
    #[test]
    fn a_file_whose_size_or_time_differs_is_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("a.jpg");
        std::fs::write(&file, b"abc").unwrap();
        let (size, mtime) = scanned(&file);
        let command = vec!["echo".to_string(), "x".to_string()];
        for (size, mtime) in [(size + 1, mtime), (size, mtime + 1)] {
            let entry = entry_at(&file, Some(size), Some(mtime));
            let Err(skipped) = process_entry(&entry, &command, &single_fact()) else {
                panic!("the entry ran");
            };
            assert!(matches!(skipped, Skipped::Changed), "{skipped}");
        }
    }

    /// The allowed case, set up as a worklist entry is: size and time as a scan records them.
    #[test]
    fn a_file_matching_its_entry_runs() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("a.jpg");
        std::fs::write(&file, b"abc").unwrap();
        let (size, mtime) = scanned(&file);
        let entry = entry_at(&file, Some(size), Some(mtime));
        let output = process_entry(
            &entry,
            &["echo".to_string(), "x".to_string()],
            &single_fact(),
        )
        .unwrap();
        assert_eq!(output.facts["content.E"].get(), r#""x""#);
    }

    /// A later stage in a chain carries no size or time: an existing file runs, an absent one is
    /// skipped.
    #[test]
    fn a_chained_entry_checks_existence_only() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("a.jpg");
        std::fs::write(&file, b"abc").unwrap();
        let command = vec!["echo".to_string(), "x".to_string()];
        assert!(process_entry(&entry_at(&file, None, None), &command, &single_fact()).is_ok());
        let absent = dir.path().join("gone.jpg");
        assert!(matches!(
            process_entry(&entry_at(&absent, None, None), &command, &single_fact()),
            Err(Skipped::NoFile)
        ));
    }

    /// The closing line counts every skipped entry by cause, a zero part left out; a run that
    /// skipped nothing has none (the allowed case).
    #[test]
    fn the_closing_line_counts_every_skipped_entry() {
        let dir = tempfile::tempdir().unwrap();
        let good = dir.path().join("a.jpg");
        std::fs::write(&good, b"abc").unwrap();
        let (size, mtime) = scanned(&good);
        let command = vec!["echo".to_string(), "x".to_string()];
        let drive = |lines: Vec<String>| {
            let (mut out, mut err) = (Vec::new(), Vec::new());
            run(
                lines.join("\n").as_bytes(),
                &mut out,
                &mut err,
                &command,
                &single_fact(),
            )
            .unwrap();
            (
                String::from_utf8(out).unwrap(),
                String::from_utf8(err).unwrap(),
            )
        };

        let (out, err) = drive(vec![
            entry_line(&dir.path().join("gone.jpg"), Some(1), Some(1)),
            "not json".to_string(),
            entry_line(&good, Some(size), Some(mtime)),
            entry_line(&good, Some(size + 1), Some(mtime)),
        ]);
        assert_eq!(out.lines().count(), 1, "{out}");
        assert_eq!(
            err.lines().last().unwrap(),
            "Skipped 3 of 4 entries: 1 with no file, 1 changed after the last scan, 1 other \
             (see the warnings above)"
        );

        let (out, err) = drive(vec![entry_line(&good, Some(size), Some(mtime))]);
        assert_eq!(out.lines().count(), 1);
        assert_eq!(err, "", "nothing skipped, nothing said");
    }
}
