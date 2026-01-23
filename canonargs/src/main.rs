use anyhow::{bail, Context, Result};
use clap::Parser;
use serde::{Deserialize, Serialize};
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

    /// Type hint for --fact mode (datetime or duration)
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
    // Optional fields from enriched input (for chaining)
    #[serde(default)]
    facts: HashMap<String, serde_json::Value>,
    // observed_at is ignored on input - we always use current time
}

#[derive(Serialize)]
struct FactOutput {
    source_id: i64,
    path: String,  // Include path to enable chaining
    basis_rev: i64,
    observed_at: i64,
    facts: HashMap<String, serde_json::Value>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Validate that exactly one mode is specified
    let mode = if let Some(ref key) = cli.fact {
        OutputMode::SingleFact { key: key.clone(), type_hint: cli.r#type.clone() }
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
    SingleFact { key: String, type_hint: Option<String> },
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

    let stdout = String::from_utf8(output.stdout)
        .context("Command output is not valid UTF-8")?;

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

/// Create a typed fact value with type hint
fn typed_value(value: &str, type_hint: &str) -> serde_json::Value {
    // For number type, parse and store as a JSON number
    if type_hint == "number" {
        if let Ok(n) = value.parse::<f64>() {
            return serde_json::Value::Number(
                serde_json::Number::from_f64(n).unwrap_or_else(|| serde_json::Number::from(0))
            );
        }
        // Fall through to typed hint if parsing fails
    }
    serde_json::json!({
        "value": value,
        "type": type_hint
    })
}

fn parse_output(stdout: &str, mode: &OutputMode) -> Result<HashMap<String, serde_json::Value>> {
    let mut facts = HashMap::new();

    match mode {
        OutputMode::SingleFact { key, type_hint } => {
            let value = stdout.trim();
            if value.is_empty() {
                bail!("Empty output");
            }
            let fact_value = match type_hint {
                Some(t) => typed_value(value, t),
                None => serde_json::Value::String(value.to_string()),
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
                        None => serde_json::Value::String(value.to_string()),
                    };
                    facts.insert(key.to_string(), fact_value);
                } else {
                    eprintln!("Warning: Skipping malformed key=value line: {}", line);
                }
            }
        }
        OutputMode::Json => {
            let parsed: serde_json::Value = serde_json::from_str(stdout.trim())
                .context("Failed to parse JSON output")?;

            match parsed {
                serde_json::Value::Object(map) => {
                    for (key, value) in map {
                        facts.insert(key, value);
                    }
                }
                _ => bail!("JSON output must be an object"),
            }
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
