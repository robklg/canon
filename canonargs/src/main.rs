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
    let mut stdout_handle = stdout.lock();

    for line in stdin.lock().lines() {
        let line = line.context("Failed to read line from stdin")?;
        if line.trim().is_empty() {
            continue;
        }

        let entry: InputEntry = match serde_json::from_str(&line) {
            Ok(e) => e,
            Err(e) => {
                eprintln!("Warning: Failed to parse input entry: {}", e);
                continue;
            }
        };

        match process_entry(&entry, &cli.command, &mode) {
            Ok(output) => {
                let json = serde_json::to_string(&output)?;
                writeln!(stdout_handle, "{}", json)?;
            }
            Err(e) => {
                eprintln!("Warning: {}: {}", entry.path, e);
            }
        }
    }

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
        let entry: InputEntry = serde_json::from_str(
            r#"{"source_id":1,"path":"a.jpg","basis_rev":0,"facts":{"content.A":1.10,"content.B":1e5,"content.C":-0,"content.D":123456789012345678901}}"#,
        )
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
}
