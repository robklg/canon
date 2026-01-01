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

    /// Output mode: key=value pairs (one per line)
    #[arg(long, group = "mode")]
    kv: bool,

    /// Output mode: JSON object
    #[arg(long, group = "mode")]
    json: bool,

    /// Command and arguments to run ({} is replaced with file path)
    #[arg(last = true, required = true)]
    command: Vec<String>,
}

#[derive(Deserialize)]
struct WorklistEntry {
    source_id: i64,
    path: String,
    basis_rev: i64,
}

#[derive(Serialize)]
struct FactOutput {
    source_id: i64,
    basis_rev: i64,
    observed_at: i64,
    facts: HashMap<String, serde_json::Value>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Validate that exactly one mode is specified
    let mode = if let Some(ref key) = cli.fact {
        OutputMode::SingleFact(key.clone())
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

        let entry: WorklistEntry = match serde_json::from_str(&line) {
            Ok(e) => e,
            Err(e) => {
                eprintln!("Warning: Failed to parse worklist entry: {}", e);
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
    SingleFact(String),
    KeyValue,
    Json,
}

fn process_entry(
    entry: &WorklistEntry,
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
    let facts = parse_output(&stdout, mode)?;

    if facts.is_empty() {
        bail!("No facts produced");
    }

    Ok(FactOutput {
        source_id: entry.source_id,
        basis_rev: entry.basis_rev,
        observed_at: current_timestamp(),
        facts,
    })
}

fn parse_output(stdout: &str, mode: &OutputMode) -> Result<HashMap<String, serde_json::Value>> {
    let mut facts = HashMap::new();

    match mode {
        OutputMode::SingleFact(key) => {
            let value = stdout.trim();
            if value.is_empty() {
                bail!("Empty output");
            }
            facts.insert(key.clone(), serde_json::Value::String(value.to_string()));
        }
        OutputMode::KeyValue => {
            for line in stdout.lines() {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                if let Some((key, value)) = line.split_once('=') {
                    let key = key.trim();
                    let value = value.trim();
                    if !key.is_empty() {
                        facts.insert(
                            key.to_string(),
                            serde_json::Value::String(value.to_string()),
                        );
                    }
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
