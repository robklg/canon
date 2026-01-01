use anyhow::{bail, Context, Result};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use crate::cluster::{Manifest, ManifestSource};

#[derive(Default)]
struct ApplyStats {
    copied: u64,
    skipped_exists: u64,
    skipped_missing: u64,
    errors: u64,
}

pub fn run(manifest_path: &Path, dry_run: bool) -> Result<()> {
    let content = fs::read_to_string(manifest_path)
        .with_context(|| format!("Failed to read manifest: {}", manifest_path.display()))?;

    let manifest: Manifest = toml::from_str(&content)
        .with_context(|| format!("Failed to parse manifest: {}", manifest_path.display()))?;

    let base_dir = PathBuf::from(&manifest.output.base_dir);
    let mut stats = ApplyStats::default();

    for source in &manifest.sources {
        match process_source(source, &manifest.output.pattern, &base_dir, dry_run) {
            Ok(action) => match action {
                ApplyAction::Copied => stats.copied += 1,
                ApplyAction::SkippedExists => stats.skipped_exists += 1,
                ApplyAction::SkippedMissing => stats.skipped_missing += 1,
            },
            Err(e) => {
                eprintln!("Error processing {}: {}", source.path, e);
                stats.errors += 1;
            }
        }
    }

    let mode = if dry_run { " (dry-run)" } else { "" };
    println!(
        "Applied{}: {} copied, {} skipped (exists), {} skipped (missing), {} errors",
        mode, stats.copied, stats.skipped_exists, stats.skipped_missing, stats.errors
    );

    Ok(())
}

enum ApplyAction {
    Copied,
    SkippedExists,
    SkippedMissing,
}

fn process_source(
    source: &ManifestSource,
    pattern: &str,
    base_dir: &Path,
    dry_run: bool,
) -> Result<ApplyAction> {
    let src_path = Path::new(&source.path);

    // Check if source exists
    if !src_path.exists() {
        if dry_run {
            println!("SKIP (missing): {}", source.path);
        }
        return Ok(ApplyAction::SkippedMissing);
    }

    // Expand pattern to get destination path
    let dest_rel = expand_pattern(pattern, source, src_path)?;
    let dest_path = base_dir.join(&dest_rel);

    // Check if destination exists
    if dest_path.exists() {
        // TODO: Could check hash here to verify it's the same file
        if dry_run {
            println!("SKIP (exists): {} -> {}", source.path, dest_path.display());
        }
        return Ok(ApplyAction::SkippedExists);
    }

    if dry_run {
        println!("COPY: {} -> {}", source.path, dest_path.display());
    } else {
        // Create parent directories
        if let Some(parent) = dest_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create directory: {}", parent.display()))?;
        }

        // Copy the file
        fs::copy(src_path, &dest_path)
            .with_context(|| format!("Failed to copy {} to {}", source.path, dest_path.display()))?;

        println!("Copied: {} -> {}", source.path, dest_path.display());
    }

    Ok(ApplyAction::Copied)
}

fn expand_pattern(pattern: &str, source: &ManifestSource, src_path: &Path) -> Result<String> {
    let mut result = pattern.to_string();

    // Build substitution map
    let mut vars: HashMap<&str, String> = HashMap::new();

    // Built-in variables from source path
    if let Some(filename) = src_path.file_name().and_then(|s| s.to_str()) {
        vars.insert("filename", filename.to_string());
    }
    if let Some(stem) = src_path.file_stem().and_then(|s| s.to_str()) {
        vars.insert("stem", stem.to_string());
    }
    if let Some(ext) = src_path.extension().and_then(|s| s.to_str()) {
        vars.insert("ext", ext.to_string());
    }

    // Source ID and hash
    vars.insert("id", source.id.to_string());
    if let Some(ref hash) = source.hash_value {
        vars.insert("hash", hash.clone());
        vars.insert("hash_short", hash.chars().take(8).collect());
    }

    // Date/time from facts (if available)
    if let Some(dt) = source.facts.get("exif.datetime_original") {
        if let Some(ts) = dt.as_i64() {
            let dt = chrono::DateTime::from_timestamp(ts, 0);
            if let Some(dt) = dt {
                vars.insert("year", dt.format("%Y").to_string());
                vars.insert("month", dt.format("%m").to_string());
                vars.insert("day", dt.format("%d").to_string());
                vars.insert("date", dt.format("%Y-%m-%d").to_string());
            }
        }
    }

    // Add all facts as variables
    for (key, value) in &source.facts {
        let str_value = match value {
            serde_json::Value::String(s) => s.clone(),
            serde_json::Value::Number(n) => n.to_string(),
            serde_json::Value::Bool(b) => b.to_string(),
            _ => continue,
        };
        // Replace dots with underscores for fact keys to make them valid in patterns
        let safe_key = key.replace('.', "_");
        vars.insert(Box::leak(safe_key.into_boxed_str()), str_value);
    }

    // Perform substitutions
    for (key, value) in &vars {
        let placeholder = format!("{{{}}}", key);
        result = result.replace(&placeholder, value);
    }

    // Check for unresolved placeholders
    if result.contains('{') && result.contains('}') {
        // Extract unresolved placeholder for error message
        if let Some(start) = result.find('{') {
            if let Some(end) = result[start..].find('}') {
                let unresolved = &result[start..start + end + 1];
                bail!(
                    "Unresolved placeholder {} in pattern. Available: {:?}",
                    unresolved,
                    vars.keys().collect::<Vec<_>>()
                );
            }
        }
    }

    // Sanitize path (remove potentially dangerous characters)
    let result = result
        .replace("..", "_")
        .replace('\0', "_");

    Ok(result)
}
