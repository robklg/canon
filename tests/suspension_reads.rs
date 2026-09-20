//! The suspension-read census.
//!
//! Deny-by-default, like the law roster and the citation guards: every raw
//! read of the closed-door vocabulary's four raw shapes — `.suspended`
//! (field access or assignment), `root_suspended` (the denormalized `Source`
//! field), `is_active()`, `is_suspended()` — **outside `core`** is
//! enumerated and matched exactly, both directions, against `LICENSED`. A
//! new raw read that is not in the table fails the build until it is either
//! routed through `core::domain::root`'s typed vocabulary
//! (`ParkedRoot`/`Door`/`open_roots`) or licensed here by name with a
//! reason; a licensed row whose site has moved or vanished fails until the
//! row is deleted. This is the instrument that stops the two lookups —
//! the raw field and the typed `ParkedRoot` it should have gone through —
//! drifting apart again at a fourth door.
//!
//! **What counts as a "read" here.** Comments are excluded — this census is
//! about code that consults the vocabulary, not prose that mentions it
//! (unlike the fate-word census, which cares about string literals hiding in
//! comments too; there is no literal-string drift risk here). Test code is
//! excluded the same way `architecture.rs`'s censuses exclude it, through the
//! shared `tests/common` helpers those censuses moved here from: a
//! `#[cfg(test)]` module's body is blanked, and a stratum's externalised
//! `tests/` directory is skipped by path — a fixture that builds a `Root` or
//! `Source` by hand has no other way to set the field, and an assertion that
//! reads an already-computed result is not a re-derivation.
//!
//! **The matcher is lexical, not type-aware** — like `FATE_WORD_EXEMPT`
//! before it, this cannot tell a raw `Root.suspended` read from an unrelated
//! type's own field that happens to share the word (`story::Location`'s and
//! `survey::LocationResult`'s own `suspended: bool`, already derived once via
//! `Root::parked()`/`is_active()` and carried as data from there on; the
//! sweep's `SuspendedRootTally` collections; `trail`'s `OriginLine::suspended()`
//! method, which reads `parked.is_some()`). The census does not need to tell
//! them apart mechanically — `LICENSED` is where a human already did, once,
//! and the exact-match requirement is what keeps that reading honest: a
//! stale row is as much a build failure as an unreviewed new one.

use std::fs;
use std::path::Path;

mod common;

use common::{collect_rs_files, is_externalised_test_file, production_text};

/// Every site outside `core` where the closed-door vocabulary's raw shape is
/// read in production code, `(file, token, count)` — exact-match both
/// directions, like `FATE_WORD_EXEMPT`/`PROBE_EXEMPT` in `architecture.rs`.
/// Grouped by the reason shared across its rows; add a new row under the
/// group it belongs to, or start a new group and argue it. Never merge two
/// counts across files into one row — the count is a claim about *that
/// file*.
const LICENSED: &[(&str, &str, usize)] = &[
    // --- The routed derivation sites: `Root::is_active()`/
    //     `Source::is_active()` called exactly once to populate a plain bool
    //     marker or filter a selection. A bare marker consumed for a filter,
    //     a count, or a per-row tag with no runnable command attached does
    //     not need the full `ParkedRoot` payload — only a *statement* (a
    //     sentence naming the way back) does, and none of these compose one.
    ("exclude/ops/types.rs", "is_active()", 1),
    ("facts/ops/import.rs", "is_active()", 1),
    ("scan/ops/pipeline.rs", "is_active()", 1),
    ("survey/domain/analysis.rs", "is_active()", 1),
    // The selection-side filter and its hidden-excluded count
    // (`survey/CLAUDE.md`'s own words: "`is_active()` survives on the
    // selection side alone... what you survey is what you can see"), plus
    // the `ArchiveCopy.root_suspended` construction one `only_on_suspended`
    // call below it feeds — the same pattern as `exclude/ops/types.rs`'s row
    // above, constructing core's own carried field from the sanctioned
    // accessor rather than reading a raw one.
    ("survey/ops/compute.rs", "is_active()", 3),
    ("survey/ops/compute.rs", "root_suspended", 1),
    // The shared selection facility every scope-taking command routes
    // through (`expr::select_sources`) — the identical bare-filter shape.
    ("expr/ops/selection.rs", "is_active()", 1),
    // --- The mutator's own precondition, on the state it is about to flip.
    //     `roots suspend`/`roots unsuspend` read the current bit to refuse a
    //     no-op with load-bearing text the caller pattern-matches on
    //     ("already suspended" / "not suspended") — this is not a second
    //     spelling of the closed-door *statement*, it is the one place in the
    //     tree whose entire job is that bit.
    ("roots/ops.rs", "is_suspended()", 2),
    // --- The complementary query: "give me the closed ones" (`--suspended`)
    //     is deliberately not routed through `open_roots`'s parked half,
    //     being the complementary question to it rather than a second
    //     spelling of the same one; `roots list`'s per-row `[suspended]` tag
    //     is a plain marker on a listing that is itself the way back, no
    //     `ParkedRoot` payload needed.
    ("roots/cli.rs", "is_suspended()", 2),
    // --- The book compile's own historical-state fields: `derive_posture`
    //     and `BookMeta.suspended` record whether the root *was* suspended at
    //     retirement, permanently — genuinely about the root's observed state
    //     at that moment, never a live door question. Both sites read
    //     `Root::is_suspended()` rather than the raw field, so what is
    //     licensed here is the closed-door vocabulary's sanctioned accessor,
    //     called for a historical-snapshot purpose rather than a live
    //     door statement.
    ("retire/ops/compile.rs", "is_suspended()", 2),
    // --- Core's own carried projection, read as data rather than
    //     re-detected: `ArchiveLocation.root: ArchiveCopy` and its
    //     `root_suspended` field are populated by core's own SQL projections
    //     (`core/CLAUDE.md`: "Carried as data rather than looked up per
    //     surface — a surface asking the roots table again is the
    //     re-detection the closed door's rule forbids"). Reading the field
    //     off an already-fetched `ArchiveCopy` is exactly what that sentence
    //     asks for, not a violation of it.
    ("story/domain/splitter.rs", "root_suspended", 1),
    // --- Downstream consumers of an already-derived marker field, never a
    //     second derivation: `story::Location.suspended` and
    //     `survey::LocationResult`-family `.suspended` fields are each
    //     populated exactly once, via `Root::parked()`/`is_active()`, and
    //     every site below reads that already-typed bool back — for
    //     `standing_first` ordering (`core::domain::resolution`'s
    //     stated-never-headlined law), for a rendered marker (`door_marker`,
    //     the survey CLAUDE.md's one spelling "for cited locations"), or
    //     copied forward into a sibling struct that carries the same
    //     already-derived fact onward.
    ("story/ops/render.rs", ".suspended", 3),
    ("survey/ops/compute.rs", ".suspended", 7),
    ("survey/render.rs", ".suspended", 10),
    // --- The sweep engine's lens-free plain data, `sweep/CLAUDE.md`'s own
    //     documented carve-out: "The engine's two suspension-aware lines are
    //     not an exception and must not become one" (the live-preferred
    //     counterpart choice, and `Coverage.suspended_locations`, at
    //     `localization.rs`) plus their two siblings on the same lens-free
    //     shape — `assembly.rs`'s `subject_suspended` and
    //     `localization.rs`'s `counterpart_suspended`/`location_suspended` —
    //     plain `bool` fields the *lens*, not the engine, later turns into
    //     ranked, `ParkedRoot`-typed board output. Retyping any of these to
    //     carry `ParkedRoot` would push lens vocabulary into the engine,
    //     which the lens-separation law names as the one thing not to do.
    ("sweep/domain/structural/assembly.rs", ".suspended", 1),
    ("sweep/domain/structural/localization.rs", ".suspended", 4),
    // --- Not the vocabulary at all: `SweepReport.suspended`/
    //     `RankedSweep.suspended` are `Vec<SuspendedRootTally>`, each entry
    //     already carrying a `ParkedRoot`. The field is spelled "suspended"
    //     for readability; it holds structured door data, not a bool, so
    //     reading it forwards an already-typed collection rather than
    //     deriving anything.
    ("sweep/cli.rs", ".suspended", 2),
    ("sweep/ops.rs", ".suspended", 1),
    // --- Trail's own typed accessor: `OriginLine::suspended()` (defined in
    //     `trail::domain::composition`) reads `parked.is_some()` off the
    //     `Option<ParkedRoot>` the composition card already carries — a
    //     well-typed downstream method, not a raw field read, that the
    //     lexical matcher can only see as `.suspended(` on its two call
    //     sites.
    ("trail/domain/composition.rs", ".suspended", 2),
];

/// Where a raw read is legitimately outside `core` — see `LICENSED`'s own
/// per-row reasons. Not a second table: this exists only to give the test
/// below one name to iterate, mirroring `FATE_WORD_EXEMPT`'s shape.
fn licensed() -> Vec<(String, String, usize)> {
    LICENSED
        .iter()
        .map(|(f, t, n)| ((*f).to_string(), (*t).to_string(), *n))
        .collect()
}

/// A raw read of the closed-door vocabulary outside `core`, red-smoked: every
/// row in `LICENSED` is real today, and a new raw read anywhere else fails
/// the build rather than merely being reported.
#[test]
fn every_raw_suspension_read_outside_core_is_licensed_by_name() {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
    let src_root = Path::new(&manifest_dir).join("src");

    let mut hits: Vec<(String, usize, &'static str, String)> = Vec::new();
    for path in collect_rs_files(&src_root) {
        let rel = path
            .strip_prefix(&src_root)
            .expect("file under src")
            .to_string_lossy()
            .replace('\\', "/");
        if rel.starts_with("core/") || is_externalised_test_file(&rel) {
            continue;
        }
        let text = fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("failed to read {}: {}", path.display(), e));
        let production = production_text(&rel, &text);
        for (line_no, token, line) in hits_in_text(&production) {
            hits.push((rel.clone(), line_no, token, line));
        }
    }

    let mut by_file_token: Vec<(String, &'static str)> =
        hits.iter().map(|(f, _, t, _)| (f.clone(), *t)).collect();
    by_file_token.sort();
    let mut found: Vec<(String, String, usize)> = Vec::new();
    for (f, t) in by_file_token {
        match found.last_mut() {
            Some(last) if last.0 == f && last.1 == t => last.2 += 1,
            _ => found.push((f, t.to_string(), 1)),
        }
    }

    // `LICENSED` is grouped by shared reason, not sorted by file — a more
    // useful layout for the human editing it than the alphabetical order
    // `found` naturally comes out in. Sorting both sides here keeps the
    // comparison exact-match without forcing that layout on the table.
    let mut expected = licensed();
    expected.sort();

    assert_eq!(
        found,
        expected,
        "\n  A raw read of the closed-door vocabulary was found outside `core`: {}\n\n  \
         Found: {found:?}\n  Licensed: {expected:?}\n\n  \
         If this is a genuine new re-derivation, route it through \
         `core::domain::root`'s typed vocabulary instead (`Root::parked()`, \
         `open_roots`, `ParkedRoot`, `Door`) rather than reading `.suspended`/\
         `root_suspended`/`is_active()`/`is_suspended()` directly. If it is a \
         bare bool consumed only for a filter, a count, or a marker with no \
         statement attached, add it to `LICENSED` with its exact count and \
         the reason, under the group it matches or a \
         new one. If a licensed row is genuinely gone (the site was fixed, \
         moved, or its count changed), delete or update its row in the same \
         commit.",
        hits.iter()
            .map(|(f, l, t, line)| format!("\n    {f}:{l}: [{t}] {}", line.trim()))
            .collect::<Vec<_>>()
            .join(""),
    );
}

/// Every hit in one file's already-production-filtered text: `(line_no,
/// token, the line itself)`. The one matching pass both the real census and
/// its own red smoke run, so a smoke test that passes against synthetic text
/// proves something about the real scan rather than about a second copy of
/// its logic.
fn hits_in_text(production: &str) -> Vec<(usize, &'static str, String)> {
    let mut found = Vec::new();
    for (line_no, line) in production.lines().enumerate() {
        if line.trim_start().starts_with("//") {
            continue;
        }
        for (token, needle) in [
            ("is_active()", "is_active()"),
            ("is_suspended()", "is_suspended()"),
            ("root_suspended", "root_suspended"),
        ] {
            if line.contains(needle) {
                found.push((line_no + 1, token, line.to_string()));
            }
        }
        for _ in 0..dot_suspended_starts(line).len() {
            found.push((line_no + 1, ".suspended", line.to_string()));
        }
    }
    found
}

/// Byte offsets in `line` where a whole-word `.suspended` starts — a leading
/// dot followed by exactly `suspended`, ended by a non-identifier character
/// (or end of line). Matches a field access, an assignment target, or a
/// method call's name alike (`.suspended` and `.suspended()` both end the
/// word the same way); never matches `.is_suspended(...)`, whose text right
/// after the dot is `is_suspended`, not `suspended`.
fn dot_suspended_starts(line: &str) -> Vec<usize> {
    let bytes = line.as_bytes();
    let mut starts = Vec::new();
    let mut i = 0;
    while let Some(pos) = line[i..].find(".suspended") {
        let start = i + pos;
        let after = start + ".suspended".len();
        let boundary_ok = bytes
            .get(after)
            .map(|c| !(c.is_ascii_alphanumeric() || *c == b'_'))
            .unwrap_or(true);
        if boundary_ok {
            starts.push(start);
        }
        i = after;
    }
    starts
}

#[cfg(test)]
mod self_tests {
    use super::*;

    #[test]
    fn dot_suspended_matches_field_access_and_method_calls() {
        assert_eq!(dot_suspended_starts("root.suspended,").len(), 1);
        assert_eq!(
            dot_suspended_starts("standing_first(a.suspended, b.suspended)").len(),
            2
        );
        assert_eq!(dot_suspended_starts("a.suspended()").len(), 1);
    }

    #[test]
    fn dot_suspended_does_not_match_is_suspended_calls() {
        assert!(dot_suspended_starts("root.is_suspended()").is_empty());
        assert!(dot_suspended_starts("!root.is_suspended()").is_empty());
    }

    #[test]
    fn dot_suspended_does_not_match_a_longer_field_name() {
        // A planted defect this matcher must not catch: `.suspended_at` is a
        // different field, sharing only a prefix.
        assert!(dot_suspended_starts("event.suspended_at").is_empty());
    }

    /// Red smoke over the real matching pipeline (`hits_in_text`, the exact
    /// function the census runs per file), not a second copy of it: every
    /// one of the four raw shapes, planted in synthetic text with no
    /// corresponding `LICENSED` row, is seen — proving the guard would
    /// refuse a genuinely new site the table does not carry.
    #[test]
    fn every_raw_shape_is_seen_by_the_real_matcher() {
        let text = "\
fn f(root: &Root, source: &Source) {
    let a = root.suspended;
    let b = source.root_suspended;
    let c = source.is_active();
    let d = root.is_suspended();
}
";
        let hits = hits_in_text(text);
        let tokens: Vec<&str> = hits.iter().map(|(_, t, _)| *t).collect();
        for expected in [
            ".suspended",
            "root_suspended",
            "is_active()",
            "is_suspended()",
        ] {
            assert!(
                tokens.contains(&expected),
                "the matcher missed a planted `{expected}` read — {tokens:?}"
            );
        }
        // None of these four lines is `roots/ops.rs`/`retire/ops/compile.rs`/
        // etc., so comparing this synthetic file's hits against `licensed()`
        // finds no matching row at any of the four tokens — exactly the
        // shape a genuinely new, unlicensed site takes in the real test.
        let synthetic_file = "synthetic/planted.rs".to_string();
        let synthetic_hits: Vec<(String, String, usize)> = hits
            .iter()
            .map(|(_, t, _)| (synthetic_file.clone(), t.to_string(), 1))
            .collect();
        for hit in &synthetic_hits {
            assert!(
                !licensed().contains(hit),
                "a planted site accidentally matched an existing licensed row: {hit:?}"
            );
        }
    }

    /// Red smoke, the comment carve-out: a raw read spelled only in a
    /// comment must not be seen — this census cares about code that reads
    /// the vocabulary, not prose that mentions it.
    #[test]
    fn a_comment_only_mention_is_not_a_hit() {
        let text = "// root.suspended is discussed here, not read\nfn f() {}\n";
        assert!(hits_in_text(text).is_empty());
    }

    /// Red smoke, the other direction: a count drifting on an already
    /// licensed row must also be caught — `found` would carry the real
    /// count, `licensed()` the stale one, and `assert_eq!` refuses a partial
    /// match rather than silently accepting a subset.
    #[test]
    fn a_stale_count_on_a_licensed_row_would_not_match() {
        let stale = licensed()
            .into_iter()
            .map(|(f, t, n)| (f, t, n + 1))
            .collect::<Vec<_>>();
        assert_ne!(stale, licensed());
    }
}
