//! Book verification — the hinge the release movement's safety hangs on.

use std::path::Path;

use anyhow::{bail, Context, Result};
use serde::Deserialize;

use crate::core::domain::fate::{fate_transition, DecisionFamily, FateAspect};
use crate::retire::domain::{
    STANDING_CONTENTLESS, STANDING_COVERED, STANDING_MISSING_UNEXPLAINED, STANDING_PRESENT,
};

use super::compile::{MetaCounts, MetaLedger};

/// Lenient read side of `meta.toml` for verification. Identity and account
/// are prose for the future reader; verification needs only the claims it
/// can check against the directory.
#[derive(Deserialize)]
struct MetaDoc {
    version: u32,
    #[serde(default)]
    gaps: Vec<String>,
    counts: MetaCounts,
    ledger: MetaLedger,
    /// Absent on books bound before the telling — they verify unchanged.
    #[serde(default)]
    story: Option<MetaStoryDoc>,
}

#[derive(Deserialize)]
struct MetaStoryDoc {
    file: String,
}

#[derive(Deserialize)]
struct InventoryLineDoc {
    fate: String,
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct BookVerification {
    pub entries: i64,
}

/// Structural verification of a compiled book: parse `meta.toml` back,
/// stream-count the inventory per fate, and require every artifact the meta
/// claims. Deliberately not an existence test — a book that fails here is
/// partial or tampered, and the removal movement must not proceed on it.
pub fn verify_book(dir: &Path) -> Result<BookVerification> {
    let meta_path = dir.join("meta.toml");
    let meta_raw = std::fs::read_to_string(&meta_path)
        .with_context(|| format!("Book meta missing or unreadable: {}", meta_path.display()))?;
    let meta: MetaDoc = toml::from_str(&meta_raw).context("Book meta failed to parse")?;
    if meta.version != 1 {
        bail!(
            "Book meta version {} is not supported by this canon (expected 1)",
            meta.version
        );
    }

    for file in ["README.md", "timeline.md", "notes.md"] {
        if !dir.join(file).is_file() {
            bail!("Book is incomplete: {file} is missing");
        }
    }

    let inventory = std::fs::File::open(dir.join("inventory.jsonl"))
        .context("Book is incomplete: inventory.jsonl is missing")?;
    let reader = std::io::BufReader::new(inventory);
    let mut counted = MetaCounts {
        entries: 0,
        archived_from_here: 0,
        covered: 0,
        excluded: 0,
        deleted: 0,
        present: 0,
        contentless: 0,
        missing_unexplained: 0,
    };
    // The same word derivations the writer used — the never-literal law
    // holds on the read side too.
    let archived = fate_transition(DecisionFamily::Archive, FateAspect::Present)
        .expect("Archive+Present is a registered transition")
        .as_str();
    let excluded = fate_transition(DecisionFamily::Exclude, FateAspect::Present)
        .expect("Exclude is a registered transition")
        .as_str();
    let deleted = fate_transition(DecisionFamily::Observe, FateAspect::Absent)
        .expect("Observe+Absent is a registered transition")
        .as_str();
    for (index, line) in std::io::BufRead::lines(reader).enumerate() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let doc: InventoryLineDoc = serde_json::from_str(&line)
            .with_context(|| format!("inventory.jsonl line {} failed to parse", index + 1))?;
        counted.entries += 1;
        match doc.fate.as_str() {
            w if w == archived => counted.archived_from_here += 1,
            w if w == excluded => counted.excluded += 1,
            w if w == deleted => counted.deleted += 1,
            w if w == STANDING_COVERED => counted.covered += 1,
            w if w == STANDING_PRESENT => counted.present += 1,
            w if w == STANDING_CONTENTLESS => counted.contentless += 1,
            w if w == STANDING_MISSING_UNEXPLAINED => counted.missing_unexplained += 1,
            other => bail!(
                "inventory.jsonl line {}: unknown fate word {other:?}",
                index + 1
            ),
        }
    }

    if counted != meta.counts {
        bail!(
            "Book counts disagree with the inventory — meta claims {:?}, the inventory holds {:?}",
            meta.counts,
            counted
        );
    }

    if meta.ledger.gathered {
        let found = count_files(&dir.join("ledger"))?;
        if found != meta.ledger.files {
            bail!(
                "Book ledger disagrees — meta claims {} gathered files, ledger/ holds {}",
                meta.ledger.files,
                found
            );
        }
    } else if meta.gaps.is_empty() {
        bail!("Book says the ledger was not gathered but records no gap explaining it");
    }

    // The telling is a claimed artifact: it must exist and hold text.
    // Verification anchors on the dossier — the counts recounted above are
    // the inventory's; the story's prose is the user's (it may be
    // hand-refined at the binding) and is never recounted.
    if let Some(story) = &meta.story {
        let len = std::fs::metadata(dir.join(&story.file))
            .map(|m| m.len())
            .unwrap_or(0);
        if len == 0 {
            bail!(
                "Book claims its story at {} but the file is missing or empty",
                story.file
            );
        }
    }

    Ok(BookVerification {
        entries: counted.entries,
    })
}

pub(super) fn count_files(dir: &Path) -> Result<usize> {
    if !dir.is_dir() {
        return Ok(0);
    }
    let mut count = 0;
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            count += count_files(&entry.path())?;
        } else if file_type.is_file() {
            count += 1;
        }
    }
    Ok(count)
}
