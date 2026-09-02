//! Turning a manifest's destination pattern into a concrete path for one source.
//!
//! Expansion needs the source's facts on hand, so the two halves stay together:
//! building the evaluation context for a lock entry, and evaluating a pattern
//! against it. Planning and the apply command both expand patterns, so the pair
//! is shared rather than owned by either.

use std::collections::HashMap;

use anyhow::Result;

use crate::archive::domain::LockEntry;
use crate::core::domain::fact::FactValue;
use crate::core::domain::path::path_strip_prefix;
use crate::expr::{
    evaluate, EvalContext, Pattern, PatternFacts, SourceAttributes, Unmeasured, OBJECT_HASH,
};

/// Build an EvalContext for a source using pre-fetched facts and cached root paths.
///
/// The scope-relative path comes off the lock entry, borrowed rather than
/// copied: it was settled when the selection was, and nothing here re-derives
/// it. That is what lets this run identically at plan time, at status time and
/// in the confirmation samples — three readers, one measurement, taken once.
fn build_eval_context<'a>(
    source: &'a LockEntry,
    unmeasured: Unmeasured,
    root_paths: &HashMap<i64, String>,
    facts: &PatternFacts,
) -> Result<EvalContext<'a>> {
    let mut ctx = EvalContext::new();

    let root_path = root_paths
        .get(&source.root_id)
        .ok_or_else(|| anyhow::anyhow!("Root {} not found in cache", source.root_id))?;

    let rel_path = if source.path == *root_path {
        String::new()
    } else if let Some(rel) = path_strip_prefix(&source.path, root_path) {
        rel.to_string()
    } else {
        source.path.clone()
    };

    // The attributes are the lock entry's, which is a snapshot taken at
    // generation time rather than a live row. That is the right reading and
    // not a compromise: apply refuses to transfer a source whose size, mtime
    // or partial hash has moved since the lock was written, so a pattern
    // naming `{source.mtime|year}` is naming the file the manifest was
    // written about.
    //
    // The gate covers exactly those three, which is worth saying because two
    // attributes here fall outside it: `device` and `inode` can change while
    // size and mtime hold (an in-place replacement preserving times), so
    // `{source.inode}` would place under the snapshot's value rather than the
    // current one. Nobody keys a destination on an inode, and the gate is the
    // right one for the attributes that decide real destinations — but the
    // claim above is about three fields, not eight.
    ctx.set_source(SourceAttributes {
        id: source.id,
        root_id: source.root_id,
        root_path: root_path.clone(),
        rel_path,
        size: source.size,
        mtime: source.mtime,
        device: source.device,
        inode: source.inode,
    });
    ctx.set_scope_rel(source.scope_rel_path.as_deref(), unmeasured);

    // No filter here, and none is owed: `PatternFacts` cannot hold a key the
    // context supplies, because the prefetch that built it never asked for
    // one. What stood here was a copy of that rule, kept in agreement with
    // three others by comment alone.
    for entry in facts.for_source(source.id) {
        ctx.set_fact(&entry.key, entry.value.clone());
    }

    if let Some(ref hash) = source.hash_value {
        ctx.set_fact(OBJECT_HASH, FactValue::Text(hash.clone()));
    }

    Ok(ctx)
}

/// Evaluate a pattern for a source, returning the destination relative path.
///
/// `unmeasured` is the lock's own answer to *why* an entry might carry no
/// scope-relative path (`LockFile::unmeasured_reason`) — a per-run value,
/// handed in the same way the root cache and the prefetched facts are, because
/// an entry cannot answer it and a refusal that guesses prescribes a remedy
/// that may not work.
pub fn evaluate_pattern(
    pattern: &Pattern,
    source: &LockEntry,
    unmeasured: Unmeasured,
    root_paths: &HashMap<i64, String>,
    facts: &PatternFacts,
) -> Result<String> {
    let ctx = build_eval_context(source, unmeasured, root_paths, facts)?;
    evaluate(pattern, &ctx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::parse_pattern;

    /// The fixture the documented examples are written against: one source at
    /// `/photos/IMG_001.jpg`, 100 bytes, modified 2024-06-15 12:00:00 UTC.
    ///
    /// The mtime is the value `expr::domain::value`'s own display pin already
    /// asserts renders as `2024-06-15`, so `|year` reading `2024` off it is
    /// derived from a pinned claim rather than captured from a run.
    fn entry() -> LockEntry {
        LockEntry {
            id: 1,
            root_id: 1,
            path: "/photos/IMG_001.jpg".to_string(),
            device: 16777220,
            inode: 12345678,
            size: 100,
            mtime: 1718452800,
            partial_hash: String::new(),
            object_id: Some(1),
            hash_type: Some("sha256".to_string()),
            hash_value: Some("abcdef1234567890".to_string()),
            scope_rel_path: None,
        }
    }

    fn roots() -> HashMap<i64, String> {
        HashMap::from([(1, "/photos".to_string())])
    }

    fn expand(pattern: &str) -> Result<String> {
        let parsed = parse_pattern(pattern).unwrap();
        evaluate_pattern(
            &parsed,
            &entry(),
            Unmeasured::NoScopeRecorded,
            &roots(),
            &PatternFacts::from_entries(HashMap::new()),
        )
    }

    #[test]
    fn a_pattern_may_name_the_source_extension() {
        assert_eq!(
            expand("{source.ext}/{filename}").unwrap(),
            "jpg/IMG_001.jpg"
        );
    }

    #[test]
    fn a_pattern_may_name_the_source_mtime() {
        assert_eq!(
            expand("{source.mtime|year}/{filename}").unwrap(),
            "2024/IMG_001.jpg"
        );
    }

    #[test]
    fn the_documented_extension_and_year_pattern_expands() {
        // `docs/src/reference/expr.md` promises this exact pattern.
        assert_eq!(
            expand("{source.ext}/{source.mtime|year}/{filename}").unwrap(),
            "jpg/2024/IMG_001.jpg"
        );
    }

    #[test]
    fn a_pattern_may_name_the_source_size() {
        assert_eq!(expand("{source.size}").unwrap(), "100");
    }

    #[test]
    fn a_pattern_may_name_the_source_id() {
        assert_eq!(expand("{source.id}").unwrap(), "1");
    }

    /// The consequence, end to end, rather than the mechanism. A row literally
    /// named `source.mtime` and holding a different year exists in the facts
    /// table; a manifest's pattern names that key; the file must still land
    /// under the year the source actually carries.
    ///
    /// This is what the four skiplist comments were protecting and what
    /// nothing exercised — *"a stored fact shadows a built-in and destinations
    /// move."* The expected value is the source's own mtime read through the
    /// same pinned instant the rest of this corpus uses, not the planted
    /// row's; a test asserting the planted year would pass against the defect.
    ///
    /// **What it was red-smoked against.** Two independent mechanisms now hold
    /// this: the prefetch never fetches the key, and evaluation resolves
    /// computed built-ins before it consults the facts map. Removing only the
    /// first leaves this green — the prefetch pins in `expr/ops/pattern.rs`
    /// are what catch that — so the defect this test names is the two of them
    /// gone together, and that is the smoke it was verified against.
    #[test]
    fn a_stored_fact_under_a_computed_name_does_not_move_the_destination() {
        use crate::core::testing::{insert_fact, insert_root, insert_source, setup_test_db};
        use crate::expr::{extract_fact_keys, prefetch_pattern_facts};

        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let source = insert_source(&conn, root, "IMG_001.jpg", None);
        // 2019-01-01 — a year the source does not have, so a shadowed
        // evaluation is visible in the destination rather than a coincidence.
        insert_fact(&conn, source, "source.mtime", "1546300800");

        let pattern = parse_pattern("{source.mtime|year}/{filename}").unwrap();
        let needed = extract_fact_keys(&pattern);
        let facts = prefetch_pattern_facts(&mut conn, &[source], &needed).unwrap();

        let mut entry = entry();
        entry.id = source;
        entry.root_id = root;
        let roots = HashMap::from([(root, "/photos".to_string())]);

        let dest = evaluate_pattern(
            &pattern,
            &entry,
            Unmeasured::NoScopeRecorded,
            &roots,
            &facts,
        )
        .unwrap();
        assert_eq!(dest, "2024/IMG_001.jpg");
        assert_ne!(dest, "2019/IMG_001.jpg", "the stored fact decided the year");
    }
}
