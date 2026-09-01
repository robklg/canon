//! The law roster: Canon's named laws, and the check that a citation resolves.
//!
//! A law is carried by three things together — the owner that speaks it once,
//! the test that verifies it, and a row in this register. Prose is a citation,
//! never a carrier: a name spelled in a CLAUDE.md with nothing behind it is a
//! description, and the point of this file is that the two can be told apart.
//!
//! The check runs **both directions**:
//!
//! - **Direction A** — every law name the prose spells resolves to a row here,
//!   or to a named entry on the unqualified list with the reason it is not a
//!   law. The list is matched by set equality, so it can neither grow silently
//!   nor rot silently: a new unresolved citation fails, and an entry whose
//!   prose was fixed fails until the entry is deleted.
//! - **Direction B** — every row's carrier is alive: the owner resolves against
//!   the tree, the verifying test exists, a unit reach names a real subsystem.
//!
//! Deliberately not checked: that a row is cited in prose anywhere. A meaning
//! spoken once in code with a pinned battery and no prose sentence is still a
//! law, and refusing it would refuse the rows this register was seeded with.
//! The remaining gap — an owner that is load-bearing and has no row at all —
//! is not reachable from here and stays open by name.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

mod common;

use common::Universe;

// ---------------------------------------------------------------------------
// The matcher.
// ---------------------------------------------------------------------------

/// One law citation found in prose: the key, and where it was read.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Citation {
    key: String,
    file: String,
    line: usize,
}

/// Function words the name-walk stops at. Closed, and spelled once: the walk's
/// whole grain is "how far left of the word `law` is still the law's name", and
/// a word that can begin a sentence about a law is where the name ends.
const STOP_WORDS: &[&str] = &[
    // articles and determiners
    "a", "an", "the", "this", "that", "these", "those", "its", "their", "our", "his", "her", "my",
    "your", "no", "each", "every", "some", "any", "other", "same", "one",
    // copulas and auxiliaries
    "is", "are", "was", "were", "be", "been", "being", "has", "have", "had", "do", "does", "did",
    // prepositions and conjunctions
    "and", "or", "but", "with", "without", "before", "after", "because", "as", "at", "by", "for",
    "from", "in", "into", "of", "on", "to", "under", "over", "than", "then", "so", "if", "when",
    "where", "while", "via", "per", // pronouns
    "it", "they", "we", "you", "i", "he", "she",
];

/// Characters that end a clause. A name never spans one, which is what keeps
/// `(verb/noun/plane/law)` from reading as a law called "verb noun plane".
const SEGMENT_BREAKS: &[char] = &['.', ':', ';', ',', '!', '?', '(', ')', '/', '|', '—', '–'];

/// Law citations in one file's text, under the rule declared in this module's
/// own words:
///
/// > Read **paragraph by paragraph** — a blank line, never a line break, is
/// > where one statement ends. Blank out backticked code spans and `*`
/// > emphasis. Split the paragraph into clause segments. Within a segment,
/// > find each token equal to `law` or `law's` — never the plural, which is
/// > generic throughout this corpus — and **walk left, collecting tokens,
/// > stopping at the first stop word**. A possessive is a qualifier: if the
/// > collected run contains a token ending `'s` or `s'`, the name is what
/// > follows the last one. The key is the surviving run, lowercased and
/// > space-joined. **An empty run is anaphora, not a citation, and is silent.**
///
/// **Blanked, not deleted**, so every offset stays an offset into the real
/// text and a citation can say which line it was read on.
///
/// One lexical limit remains, stated because an undeclared one is the
/// secretly-dumb kind: **fence lines toggle the span like any backticks**, so
/// a fenced block contiguous with its fences is blanked wholesale, and only
/// fence content set apart by blank lines is read as prose. Nothing in this
/// corpus spells a law name in or around a fence today. Tolerable in both
/// directions: blanked example text asserts nothing, and set-apart prose that
/// names a law is a real citation wherever it happens to sit.
///
/// This is deliberately dumb, and the dumbness is stated so that it is not
/// secretly dumb. It over-produces on correct English — a sentence *about*
/// laws, a law *site*, a *kind* of law — and those go to a named list with
/// their reasons rather than being cleverness in here. The one failure the
/// register cannot tolerate is the invisible one, a name silently missed. That
/// is why the walk needs no determiner (`**Slice-sum law**` is a real citation
/// and a rule wanting "the" in front of it would have dropped it), why the
/// possessive of the word counts, and why the unit is the paragraph: each of
/// those three was a name this matcher lost in silence until it did not.
///
/// Pure: no I/O, no tree, no roster. This is the seam the red smoke feeds.
fn law_citations(display_path: &str, text: &str) -> Vec<Citation> {
    let mut out = Vec::new();
    for (base, paragraph) in paragraphs(text) {
        for (key, offset) in law_keys_in(paragraph) {
            out.push(Citation {
                key,
                file: display_path.to_string(),
                line: 1 + text[..base + offset].matches('\n').count(),
            });
        }
    }
    out
}

/// Maximal runs of consecutive non-blank lines, with each run's byte offset.
///
/// A paragraph, not a line, is the unit a name is read in. Prose wraps: the
/// corpus spells "the contentless / law's canary" and "the context-supplied /
/// law is applied" across a break, and a line-scoped matcher loses both in
/// silence — the failure this register least tolerates. A blank line is where
/// one statement ends and the next begins, so it is the only break that stops
/// a name.
fn paragraphs(text: &str) -> Vec<(usize, &str)> {
    let mut out = Vec::new();
    let mut start: Option<usize> = None;
    let mut offset = 0;
    for line in text.split_inclusive('\n') {
        if line.trim().is_empty() {
            if let Some(from) = start.take() {
                out.push((from, &text[from..offset]));
            }
        } else if start.is_none() {
            start = Some(offset);
        }
        offset += line.len();
    }
    if let Some(from) = start {
        out.push((from, &text[from..]));
    }
    out
}

/// What a character is to the walk.
enum Class {
    /// Part of a token.
    Token(char),
    /// Ends a token, but not the clause.
    Gap,
    /// Ends the clause: a name never spans one.
    Break,
}

/// Classify one paragraph's characters, with each one's byte offset. Backticked
/// spans and `*` emphasis are blanked rather than deleted, so every offset here
/// is still an offset into the real text and a citation can say which line it
/// was read on.
fn classify(paragraph: &str) -> Vec<(usize, Class)> {
    let mut out = Vec::with_capacity(paragraph.len());
    let mut in_code_span = false;
    for (offset, c) in paragraph.char_indices() {
        let class = if c == '`' {
            in_code_span = !in_code_span;
            Class::Gap
        } else if in_code_span {
            // A code span is not prose: it holds paths, identifiers and type
            // names, and `contentless_law_tests.rs` is a filename rather than
            // a law called "contentless".
            Class::Gap
        } else if SEGMENT_BREAKS.contains(&c) {
            Class::Break
        } else if c.is_ascii_alphanumeric() || c == '\'' || c == '-' {
            Class::Token(c)
        } else {
            Class::Gap
        };
        out.push((offset, class));
    }
    out
}

/// Every law name in one paragraph, each with the byte offset of the word it
/// was read from.
fn law_keys_in(paragraph: &str) -> Vec<(String, usize)> {
    let mut keys = Vec::new();
    let mut segment: Vec<(String, usize)> = Vec::new();
    let mut current: Option<(String, usize)> = None;

    let close = |current: &mut Option<(String, usize)>, segment: &mut Vec<(String, usize)>| {
        if let Some(token) = current.take() {
            segment.push(token);
        }
    };

    for (offset, class) in classify(paragraph) {
        match class {
            Class::Token(c) => match current.as_mut() {
                Some((text, _)) => text.push(c),
                // A token starts on a letter: `2026` is not a name, `v2` is.
                None if c.is_ascii_alphabetic() => current = Some((c.to_string(), offset)),
                None => {}
            },
            Class::Gap => close(&mut current, &mut segment),
            Class::Break => {
                close(&mut current, &mut segment);
                keys.extend(names_in_segment(&segment));
                segment.clear();
            }
        }
    }
    close(&mut current, &mut segment);
    keys.extend(names_in_segment(&segment));
    keys
}

/// The names read from one clause: one per occurrence of the word.
fn names_in_segment(segment: &[(String, usize)]) -> Vec<(String, usize)> {
    let mut out = Vec::new();
    for (i, (token, offset)) in segment.iter().enumerate() {
        if !is_law_token(token) {
            continue;
        }
        let before: Vec<String> = segment[..i].iter().map(|(t, _)| t.clone()).collect();
        if let Some(key) = name_left_of(&before) {
            out.push((key, *offset));
        }
    }
    out
}

/// The word a citation is built around: `law`, and its possessive, which
/// carries a name exactly as the bare form does ("the never-literal law's
/// correct scope"). The **plural** is not here: it is generic throughout this
/// corpus ("claims about laws resolve to their verifying tests"), and the
/// plural possessive with it.
fn is_law_token(token: &str) -> bool {
    token.eq_ignore_ascii_case("law") || token.eq_ignore_ascii_case("law's")
}

/// The name standing immediately left of `law` in one clause: tokens collected
/// leftward to the first stop word, then truncated after the last possessive.
/// `None` when nothing survives — anaphora ("the command because the law"),
/// which is silence by structure rather than by judgment.
fn name_left_of(before: &[String]) -> Option<String> {
    let mut run: Vec<&str> = Vec::new();
    for token in before.iter().rev() {
        if STOP_WORDS.contains(&token.to_ascii_lowercase().as_str()) {
            break;
        }
        run.push(token.as_str());
    }
    run.reverse();

    // A possessive qualifies the name; it is never part of it. `the sweep's
    // round-trip law` is the round-trip law, read in the sweep.
    if let Some(last) = run
        .iter()
        .rposition(|t| t.ends_with("'s") || t.ends_with("s'"))
    {
        run.drain(..=last);
    }

    if run.is_empty() {
        return None;
    }
    Some(run.join(" ").to_ascii_lowercase())
}

// ---------------------------------------------------------------------------
// Self-tests over synthetic corpora — the matcher's red smoke. Every expected
// value below is derived from the declared rule above, never captured from a
// run, and each test was written by planting the defect it names and watching
// it fail. Two of them name their defect exactly: the two matcher designs that
// were traced against the real corpus and broke.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod self_tests {
    use super::*;

    fn keys(text: &str) -> Vec<String> {
        law_citations("synthetic.md", text)
            .into_iter()
            .map(|c| c.key)
            .collect()
    }

    #[test]
    fn flags_a_planted_unregistered_name() {
        let found = law_citations("synthetic.md", "It answers to the widget-alignment law.\n");
        assert_eq!(
            found,
            vec![Citation {
                key: "widget-alignment".to_string(),
                file: "synthetic.md".to_string(),
                line: 1,
            }],
            "a planted name must be caught, with where it was read",
        );
    }

    #[test]
    fn flags_a_determinerless_bold_citation() {
        // Red smoke against the rejected "require a determiner" design, which
        // would have missed this real corpus shape in silence — the one
        // failure mode the register cannot tolerate, because it is invisible.
        assert_eq!(keys("**Slice-sum law** (test-enforced)\n"), ["slice-sum"]);
    }

    #[test]
    fn reads_a_multiword_name() {
        assert_eq!(
            keys("the sweep's lens separation law\n"),
            ["lens separation"]
        );
        assert_eq!(
            keys("the two-claims placement law places it\n"),
            ["two-claims placement"],
        );
    }

    #[test]
    fn truncates_at_a_possessive() {
        assert_eq!(keys("the sweep's round-trip law\n"), ["round-trip"]);
        assert_eq!(
            keys("the Retirement conventions' union-listing law\n"),
            ["union-listing"],
        );
    }

    #[test]
    fn allows_digits_in_a_name() {
        assert_eq!(keys("the v2 law holds\n"), ["v2"]);
    }

    #[test]
    fn silent_on_anaphoric_law() {
        // Red smoke against the rejected "grab N tokens left of `law`" design.
        // All four are verbatim corpus phrases where `law` refers back to a law
        // already named; a fixed-width grab reads the sentence as the name.
        for phrase in [
            "refused by the command because the law\n",
            "the check short-circuits before the law\n",
            "the once-sweep-local rule became the law\n",
            "it is the law\n",
        ] {
            assert!(
                keys(phrase).is_empty(),
                "anaphora is not a citation: {phrase:?} produced {:?}",
                keys(phrase),
            );
        }
        // The same sentence shape does still yield a name where one is spelled.
        assert_eq!(keys("a computation the separation law\n"), ["separation"]);
    }

    #[test]
    fn silent_inside_a_code_span() {
        assert!(keys("the canary lives in `src/contentless_law_tests.rs`\n").is_empty());
        assert!(keys("see `same_physical_file` and `physical_law` here\n").is_empty());
    }

    #[test]
    fn silent_across_a_slash_boundary() {
        assert!(keys("ownership follows what a thing is (verb/noun/plane/law)\n").is_empty());
    }

    #[test]
    fn reads_a_possessive_of_the_word_itself() {
        // `law's` carries a name exactly as the bare word does. Missing it is
        // a silent false negative, which is the one failure the register
        // cannot tolerate — and it hid a real named law until it was fixed.
        assert_eq!(
            keys("named constants, the never-literal law's correct scope\n"),
            ["never-literal"],
        );
        assert_eq!(
            keys("the sweep's lens separation law's edge\n"),
            ["lens separation"]
        );
    }

    #[test]
    fn reads_a_citation_wrapped_across_a_line_break() {
        // Prose wraps. Both of these are real corpus shapes that a line-scoped
        // matcher dropped in silence.
        assert_eq!(
            keys("which no production code calls — the contentless\nlaw's canary is its only consumer\n"),
            ["contentless"],
        );
        assert_eq!(
            keys("the place the context-supplied\nlaw is applied rather than merely stated\n"),
            ["context-supplied"],
        );
    }

    #[test]
    fn a_blank_line_stops_a_name() {
        // A break between statements is not a place a name may span.
        assert!(keys("a paragraph ending in separation\n\nlaw opens the next one\n").is_empty());
    }

    #[test]
    fn a_citation_reports_the_line_the_word_was_read_on() {
        // Paragraph-scoped matching must not cost line attribution.
        let found = law_citations("x.md", "intro\n\nthe contentless\nlaw's canary\n");
        assert_eq!(found.len(), 1);
        assert_eq!(
            found[0].line, 4,
            "the line the word sits on, not the paragraph's"
        );
    }

    #[test]
    fn a_table_row_does_not_read_across_its_cells() {
        // A markdown row is several clauses, not one. Without the separator as
        // a break, this header yields the key `warrant positive mechanic limit`
        // — a name no one wrote, pinned to three column titles, and changing
        // if any of them is reworded.
        assert_eq!(
            keys("| Warrant | Positive mechanic | Limit law, and where it is enforced |\n"),
            ["limit"],
        );
    }

    #[test]
    fn a_fence_blanks_its_block_and_a_blank_line_ends_that() {
        // A fence's backticks toggle the span like any others, so a block
        // contiguous with its fences is blanked wholesale and asserts nothing.
        assert!(keys("```\nthe separation law\n```\n").is_empty());
        // A blank line starts a new paragraph, where the span state resets and
        // the text is read as the prose it looks like. Both directions are
        // tolerable, which is why this is a declared limit and not a defect.
        assert_eq!(keys("```\n\nthe separation law\n\n```\n"), ["separation"]);
        // And a closed fence leaves the rest of its paragraph readable.
        assert_eq!(keys("```\nx\n```\nthe separation law\n"), ["separation"]);
    }

    #[test]
    fn silent_on_the_plural() {
        assert!(
            keys("claims about laws resolve to their verifying tests\n").is_empty(),
            "the plural is generic throughout this corpus",
        );
    }

    #[test]
    fn reads_every_citation_on_a_line_with_its_place() {
        let found = law_citations(
            "x.md",
            "prelude\nthe separation law, and the slice-sum law\n",
        );
        assert_eq!(
            found,
            vec![
                Citation {
                    key: "separation".to_string(),
                    file: "x.md".to_string(),
                    line: 2,
                },
                Citation {
                    key: "slice-sum".to_string(),
                    file: "x.md".to_string(),
                    line: 2,
                },
            ],
        );
    }
}

// ---------------------------------------------------------------------------
// The roster.
// ---------------------------------------------------------------------------

/// One named law: the meaning, and the three things that carry it.
struct Law {
    /// The citation key: exactly how prose spells the name, lowercased.
    name: &'static str,
    /// Where the meaning is spoken once — a module path or a repo-relative
    /// file path, resolved against the tree.
    owner: &'static str,
    /// The test that verifies it. `fn <verifier>` must exist under `src/` or
    /// `tests/`. Where a law is verified by a whole module, this names one
    /// representative test in it.
    verifier: &'static str,
    /// Where the rule binds. The default is the unit that recognised it;
    /// binding wider than that is a finding about the whole tree, recorded
    /// here rather than inferred from which file the name was typed into.
    reach: Reach,
    /// Who found that reach, and on what standing.
    authority: Authority,
    /// The date of the decision this row's authority comes from.
    record: &'static str,
}

/// How far a law binds.
enum Reach {
    /// Everywhere in the tree.
    Canon,
    /// One directory under `src/` — the unit that recognised it. Checked, so a
    /// subsystem rename that orphans a row fails the build.
    Unit(&'static str),
}

/// The standing a row's reach rests on. Named for the standing itself, not for
/// who holds it: paired with `record`, a date, this is what a reader follows
/// back to the decision.
enum Authority {
    /// Distilled from an accepted architecture decision, and canon by it.
    Distilled,
    /// A whole-tree search for spread decided this reach — sometimes widening
    /// it, sometimes finding none and confirming it stays where it was. The
    /// search is what this stands on, which is why it is not named for either
    /// answer.
    Searched,
    /// Recognised by the unit that owns the mechanism, at its own reach —
    /// the default, and the one a change may claim for itself.
    Recognised,
}

impl Authority {
    fn as_str(&self) -> &'static str {
        match self {
            Authority::Distilled => "distilled",
            Authority::Searched => "searched",
            Authority::Recognised => "recognised",
        }
    }
}

/// The register. A law's row is added by the change that mints it, in the same
/// commit: a name with no row here fails the build the moment prose spells it.
const LAWS: &[Law] = &[
    Law {
        name: "aggregate-only",
        owner: "core/domain/extraction.rs",
        verifier: "build_extraction_rows_one_row_per_directory_pair",
        reach: Reach::Unit("core"),
        authority: Authority::Recognised,
        record: "2026-07-19",
    },
    Law {
        name: "agreement",
        owner: "core/domain/resolution.rs",
        verifier: "agreement_law_place_sums_fold_to_the_account",
        reach: Reach::Unit("core"),
        authority: Authority::Recognised,
        record: "2026-08-03",
    },
    Law {
        name: "contentless",
        owner: "core/domain/source.rs",
        verifier: "the_classifier_reads_the_empty_source_as_contentless",
        reach: Reach::Canon,
        authority: Authority::Distilled,
        record: "2026-08-04",
    },
    Law {
        name: "context-supplied",
        owner: "expr/domain/pattern.rs",
        verifier: "the_context_supplied_set_is_spelled_only_inside_expr",
        reach: Reach::Unit("expr"),
        authority: Authority::Recognised,
        record: "2026-08-30",
    },
    Law {
        name: "extraction round-trip",
        owner: "core/domain/extraction.rs",
        verifier: "round_trip_law_backfill_matches_forward_recording",
        reach: Reach::Unit("core"),
        authority: Authority::Recognised,
        record: "2026-07-19",
    },
    Law {
        name: "fate-vocabulary",
        owner: "core/domain/fate.rs",
        verifier: "fate_transition_covers_family_aspect_matrix",
        reach: Reach::Unit("core"),
        authority: Authority::Recognised,
        record: "2026-07-04",
    },
    Law {
        name: "four-layer",
        owner: "tests/architecture.rs",
        verifier: "architecture_rules_hold",
        reach: Reach::Canon,
        authority: Authority::Distilled,
        record: "2026-08-07",
    },
    Law {
        name: "handoff round-trip",
        owner: "sweep/cli.rs",
        verifier: "every_emitted_argv_parses",
        reach: Reach::Unit("sweep"),
        authority: Authority::Recognised,
        record: "2026-08-15",
    },
    Law {
        name: "one-fetch",
        owner: "story/ops/report.rs",
        verifier: "report_over_is_compute_story_minus_the_fetch",
        reach: Reach::Unit("story"),
        authority: Authority::Recognised,
        record: "2026-08-05",
    },
    Law {
        name: "physical-identity",
        owner: "scan/domain.rs",
        verifier: "the_law_reads_content_evidence_never_device_or_inode",
        reach: Reach::Canon,
        authority: Authority::Distilled,
        record: "2026-08-23",
    },
    Law {
        name: "posture",
        owner: "core/domain/fate.rs",
        verifier: "fate_posture_observed_only_for_scan_deletion",
        reach: Reach::Unit("core"),
        authority: Authority::Recognised,
        record: "2026-07-04",
    },
    Law {
        name: "recorded-scope resolution",
        owner: "core/domain/scope.rs",
        verifier: "a_prefix_under_no_root_is_carried_never_dropped",
        reach: Reach::Unit("core"),
        authority: Authority::Recognised,
        record: "2026-08-30",
    },
    Law {
        name: "slice-sum",
        owner: "story/domain/splitter.rs",
        verifier: "slice_sum_law_reconciles_through_any_fold",
        reach: Reach::Unit("story"),
        authority: Authority::Recognised,
        record: "2026-08-03",
    },
    Law {
        name: "two-claims placement",
        owner: "trail/domain/placement.rs",
        verifier: "placement_in_view_is_descendant_or_equal_only",
        reach: Reach::Unit("trail"),
        authority: Authority::Recognised,
        record: "2026-08-12",
    },
    Law {
        name: "union-listing",
        owner: "retire/ops/shelf.rs",
        verifier: "shelf_listing_marks_a_recorded_retirement_without_a_standing_book",
        reach: Reach::Unit("retire"),
        authority: Authority::Recognised,
        record: "2026-08-06",
    },
    Law {
        name: "which-ledger",
        owner: "retire/ops/frame.rs",
        verifier: "the_trace_chain_names_the_archives_ledger",
        reach: Reach::Unit("retire"),
        authority: Authority::Recognised,
        record: "2026-08-05",
    },
    // These five carry no prose sentence anywhere yet. A meaning spoken once
    // in code with a pinned battery is a law whether or not anyone has written
    // about it, which is why nothing here checks that a row is cited.
    Law {
        name: "prospective-claim settlement",
        owner: "core/ops/decision.rs",
        verifier: "a_failed_receipt_write_clears_the_claim",
        reach: Reach::Unit("core"),
        authority: Authority::Searched,
        record: "2026-08-29",
    },
    Law {
        name: "suspension citation-evidence split",
        owner: "sweep/domain/structural/localization.rs",
        verifier: "a_live_scope_wins_the_counterpart_choice_over_a_suspended_one",
        reach: Reach::Canon,
        authority: Authority::Searched,
        record: "2026-08-29",
    },
    Law {
        name: "view-match",
        owner: "trail/ops/compute.rs",
        verifier: "show_and_the_timeline_agree_on_what_matched",
        reach: Reach::Unit("trail"),
        authority: Authority::Searched,
        record: "2026-08-29",
    },
    Law {
        name: "scope-vantage",
        owner: "expr/domain/vantage.rs",
        verifier: "the_vantage_never_rises_above_its_root",
        reach: Reach::Unit("expr"),
        authority: Authority::Searched,
        record: "2026-08-29",
    },
    Law {
        name: "boundary-borrowing",
        owner: "trail/domain/crossings.rs",
        verifier: "a_global_view_borrows_the_named_counterpart_as_its_boundary",
        reach: Reach::Unit("trail"),
        authority: Authority::Searched,
        record: "2026-08-29",
    },
];

/// Names the matcher reads out of the prose that resolve to no row, each with
/// the reason. This is a work queue, not a permission list: it shrinks. An
/// entry leaves when the prose it describes is fixed or the law it points at
/// acquires a carrier, and the check below fails until the entry goes with it.
///
/// Reasons come from a closed vocabulary so the queue sorts by what it would
/// take to burn an entry down. The first four say **this key is not a law's own
/// name**, and burn down by rewording the prose; the last two say **the name is
/// right and the carrier is short**, and those are the ones that become rows.
///
/// - `sentence about laws` — the prose is *about* laws; nothing is named.
/// - `law site` — names a place a law is enforced, not the law.
/// - `kind of law` — a category (a limit law, a cross-cutting law), not a name.
/// - `prose variant` — a law referred to by something other than its own name.
///   Burns down by rewording, never by an alias field: one verifying test
///   answering to two names is what this register exists to prevent.
/// - `owner not found` — named as a law, but nothing in code speaks it.
/// - `verifier not found` — an owner exists, and no test names the claim. A
///   two-thirds carrier: the most interesting thing this list holds.
///
/// **One reason per key, and a key can have more than one referent.** Three
/// keys here (`round-trip`, `limit`, `separation`) are read from sites that
/// mean different laws, and the reason names the dominant one. Splitting them
/// is rewording work on the prose, not something this list can express.
const UNQUALIFIED: &[(&str, &str)] = &[
    ("bidirectional", "owner not found"),
    ("cross-cutting", "kind of law"),
    ("index", "prose variant"),
    ("last interface-layer", "law site"),
    ("lens separation", "verifier not found"),
    ("limit", "kind of law"),
    ("named", "sentence about laws"),
    ("never-literal", "verifier not found"),
    ("own", "prose variant"),
    ("path", "verifier not found"),
    ("purity", "prose variant"),
    ("round-trip", "prose variant"),
    ("separation", "prose variant"),
    ("suite-wide", "prose variant"),
    ("test-enforced", "kind of law"),
    ("two-claims", "prose variant"),
    ("v2", "owner not found"),
];

// ---------------------------------------------------------------------------
// The two directions, as pure checks over supplied evidence.
// ---------------------------------------------------------------------------

/// Direction A: the citation keys that resolve to nothing — no row, read from
/// the given prose. Keyed by name so the failure can say where each was read.
fn unregistered_keys(citations: &[Citation], laws: &[Law]) -> BTreeMap<String, Vec<String>> {
    let registered: BTreeSet<&str> = laws.iter().map(|l| l.name).collect();
    let mut out: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for c in citations {
        if registered.contains(c.key.as_str()) {
            continue;
        }
        out.entry(c.key.clone())
            .or_default()
            .push(format!("{}:{}", c.file, c.line));
    }
    out
}

/// Direction B: every way a row can outlive its carrier.
fn carrier_violations(
    laws: &[Law],
    universe: &Universe,
    test_fns: &BTreeSet<String>,
    units: &BTreeSet<String>,
) -> Vec<String> {
    let mut violations = Vec::new();
    let mut seen: BTreeSet<&str> = BTreeSet::new();
    for law in laws {
        // Each line carries the row's provenance, because the reader of a
        // broken carrier needs to know which decision to go back to.
        let who = format!("`{}` ({} {})", law.name, law.authority.as_str(), law.record,);
        if !seen.insert(law.name) {
            violations.push(format!("{who} has more than one row — one key, one law"));
        }
        if !universe.resolves_file_citation(law.owner) {
            violations.push(format!(
                "{who} names owner `{}`, which does not resolve",
                law.owner,
            ));
        }
        if !test_fns.contains(law.verifier) {
            violations.push(format!(
                "{who} names verifier `{}`, and no such function exists",
                law.verifier,
            ));
        }
        match law.reach {
            Reach::Canon => {}
            Reach::Unit(unit) => {
                if !units.contains(unit) {
                    violations.push(format!(
                        "{who} reaches unit `{unit}`, which is not a directory under src/",
                    ));
                }
            }
        }
    }
    violations
}

/// Every `fn <name>` spelled anywhere under `src/` or `tests/`. A verifier is
/// checked by existence only: whether the test is a good one is the reviewer's
/// question, not the build's.
fn function_names(root: &Path) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    for dir in ["src", "tests"] {
        for file in common::collect_rs_files(&root.join(dir)) {
            let text = fs::read_to_string(&file)
                .unwrap_or_else(|e| panic!("failed to read {}: {}", file.display(), e));
            names.extend(declared_fn_names(&text));
        }
    }
    names
}

fn declared_fn_names(text: &str) -> Vec<String> {
    let mut names = Vec::new();
    let bytes = text.as_bytes();
    for (i, _) in text.match_indices("fn ") {
        // `fn` must be its own word: `xfn ` is not a declaration.
        if i > 0 && (bytes[i - 1].is_ascii_alphanumeric() || bytes[i - 1] == b'_') {
            continue;
        }
        let name: String = text[i + 3..]
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect();
        if !name.is_empty() {
            names.push(name);
        }
    }
    names
}

/// The directories under `src/` a `Reach::Unit` may name.
fn unit_names(root: &Path) -> BTreeSet<String> {
    let mut units = BTreeSet::new();
    if let Ok(entries) = fs::read_dir(root.join("src")) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    units.insert(name.to_string());
                }
            }
        }
    }
    units
}

/// The prose corpus this register reads, and how much of it was there.
///
/// CLAUDE.md is git-ignored at every depth, so a checkout of the tracked tree
/// alone has **none** of it. That is not a broken corpus, it is a corpus that
/// is not present — and the difference matters here in a way it does not for
/// the citation guard next door: that guard only ever *adds* violations, so
/// absence makes it silently pass, while this one asserts a set equality that
/// absence would turn into "every entry is stale". A checkout with no corpus
/// must not fail a check about prose it does not have.
struct Corpus {
    citations: Vec<Citation>,
    files_read: usize,
}

fn read_corpus(root: &Path) -> Corpus {
    let mut citations = Vec::new();
    let mut files_read = 0;
    for path in common::claude_md_paths(root) {
        let Ok(text) = fs::read_to_string(&path) else {
            continue; // git-ignored file, absent in this checkout.
        };
        files_read += 1;
        let display = path
            .strip_prefix(root)
            .unwrap_or(&path)
            .to_string_lossy()
            .to_string();
        citations.extend(law_citations(&display, &text));
    }
    Corpus {
        citations,
        files_read,
    }
}

// ---------------------------------------------------------------------------
// The checks over the real tree.
// ---------------------------------------------------------------------------

/// What direction A concluded. `NoCorpus` is a real answer, not a skip: the
/// prose this check reads is git-ignored at every depth, so a checkout of the
/// tracked tree alone has none of it, and a set equality against nothing would
/// report every entry as stale. A check with no input says so.
#[derive(Debug, PartialEq, Eq)]
enum DirectionA {
    NoCorpus,
    Resolved,
    Unresolved {
        unlisted: Vec<String>,
        stale: Vec<String>,
    },
}

fn direction_a(corpus: &Corpus, laws: &[Law], listed: &[(&str, &str)]) -> DirectionA {
    if corpus.files_read == 0 {
        return DirectionA::NoCorpus;
    }
    let unregistered = unregistered_keys(&corpus.citations, laws);
    let found: BTreeSet<&str> = unregistered.keys().map(String::as_str).collect();
    let listed: BTreeSet<&str> = listed.iter().map(|(k, _)| *k).collect();

    let unlisted: Vec<String> = found
        .difference(&listed)
        .map(|k| format!("  {}  — read at {}", k, unregistered[*k].join(", ")))
        .collect();
    let stale: Vec<String> = listed
        .difference(&found)
        .map(|k| format!("  {k}"))
        .collect();

    if unlisted.is_empty() && stale.is_empty() {
        DirectionA::Resolved
    } else {
        DirectionA::Unresolved { unlisted, stale }
    }
}

#[test]
fn the_unqualified_list_is_exactly_what_the_tree_produces() {
    let root = common::repo_root();
    let (new_names, stale) = match direction_a(&read_corpus(&root), LAWS, UNQUALIFIED) {
        // Not a silent pass: a checkout with no prose is a different state
        // from a checkout whose prose agrees, and the reader is told which.
        DirectionA::NoCorpus => {
            eprintln!("no CLAUDE.md present in this checkout — direction A has no corpus to check");
            return;
        }
        DirectionA::Resolved => return,
        DirectionA::Unresolved { unlisted, stale } => (unlisted, stale),
    };

    panic!(
        "\n  The prose names laws the register does not carry, or carries names \
         the prose no longer spells.\n\n  \
         Unresolved and unlisted — give each a row, or list it with its reason:\n{}\n  \
         Listed but no longer produced — delete these entries, the prose that \
         needed them is gone:\n{}\n  \
         The list shrinks. A name that turns out to be a law gets a row; a name \
         the prose stops spelling loses its entry in the same commit.",
        new_names.join("\n"),
        stale.join("\n"),
    );
}

/// The six reasons an entry may carry. Closed: a seventh is a decision about
/// what the queue can say, not a word someone reaches for while adding a row.
const REASONS: &[&str] = &[
    "sentence about laws",
    "law site",
    "kind of law",
    "prose variant",
    "owner not found",
    "verifier not found",
];

fn wellformed_problems(listed: &[(&str, &str)]) -> Vec<String> {
    let mut problems = Vec::new();
    let mut seen: BTreeSet<&str> = BTreeSet::new();
    for (key, reason) in listed {
        if !seen.insert(key) {
            // The direction-A check compares sets, so a duplicate key is
            // absorbed and a second, contradictory reason for it would never
            // surface. It has to be caught here or nowhere.
            problems.push(format!("`{key}` is listed more than once"));
        }
        if !REASONS.contains(reason) {
            problems.push(format!(
                "`{key}` gives the reason `{reason}`, which is not one of the six",
            ));
        }
    }
    problems
}

#[test]
fn every_unqualified_entry_is_well_formed() {
    let problems = wellformed_problems(UNQUALIFIED);
    assert!(
        problems.is_empty(),
        "\n  The unqualified list is a register too, and its entries make claims:\n{}\n",
        problems
            .iter()
            .map(|p| format!("  {p}"))
            .collect::<Vec<_>>()
            .join("\n"),
    );
}

#[test]
fn every_law_row_has_a_living_carrier() {
    let root = common::repo_root();
    let violations = carrier_violations(
        LAWS,
        &Universe::build(&root),
        &function_names(&root),
        &unit_names(&root),
    );
    assert!(
        violations.is_empty(),
        "\n  A law's carrier is its owner, its verifying test and this row \
         together. These rows have lost one:\n{}\n  \
         Repair the row in the commit that moved the code, or delete it — a law \
         dies with its reason.",
        violations
            .iter()
            .map(|v| format!("  {v}"))
            .collect::<Vec<_>>()
            .join("\n"),
    );
}

// ---------------------------------------------------------------------------
// Self-tests for the two directions, over synthetic rows — each planted defect
// is one way a register rots, and each assertion is derived from the rule it
// guards rather than from a run.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod register_self_tests {
    use super::*;

    fn universe() -> Universe {
        Universe::from_parts(
            &["trail", "trail::domain", "trail::domain::placement"],
            &["src/trail/domain/placement.rs"],
            &[],
        )
    }

    fn verifiers() -> BTreeSet<String> {
        ["placement_in_view_is_descendant_or_equal_only"]
            .iter()
            .map(|s| s.to_string())
            .collect()
    }

    fn units() -> BTreeSet<String> {
        ["trail"].iter().map(|s| s.to_string()).collect()
    }

    fn sound_row() -> Law {
        Law {
            name: "two-claims placement",
            owner: "trail/domain/placement.rs",
            verifier: "placement_in_view_is_descendant_or_equal_only",
            reach: Reach::Unit("trail"),
            authority: Authority::Recognised,
            record: "2026-08-12",
        }
    }

    fn check(laws: &[Law]) -> Vec<String> {
        carrier_violations(laws, &universe(), &verifiers(), &units())
    }

    #[test]
    fn a_whole_carrier_passes() {
        assert!(check(&[sound_row()]).is_empty());
    }

    #[test]
    fn flags_a_row_whose_owner_is_gone() {
        let row = Law {
            owner: "trail/domain/moved_away.rs",
            ..sound_row()
        };
        let v = check(&[row]);
        assert_eq!(v.len(), 1, "{v:?}");
        assert!(v[0].contains("moved_away.rs"), "{v:?}");
    }

    #[test]
    fn flags_a_row_whose_verifier_is_gone() {
        let row = Law {
            verifier: "a_test_that_was_deleted",
            ..sound_row()
        };
        let v = check(&[row]);
        assert_eq!(v.len(), 1, "{v:?}");
        assert!(v[0].contains("a_test_that_was_deleted"), "{v:?}");
    }

    #[test]
    fn flags_a_unit_reach_naming_no_subsystem() {
        // The subsystem-rename class: the code is fine, the row is stale.
        let row = Law {
            reach: Reach::Unit("trails"),
            ..sound_row()
        };
        let v = check(&[row]);
        assert_eq!(v.len(), 1, "{v:?}");
        assert!(v[0].contains("trails"), "{v:?}");
        // Canon reach names no unit, so it has nothing to go stale.
        let row = Law {
            reach: Reach::Canon,
            ..sound_row()
        };
        assert!(check(&[row]).is_empty());
    }

    #[test]
    fn flags_a_duplicate_law_name() {
        let v = check(&[sound_row(), sound_row()]);
        assert_eq!(v.len(), 1, "{v:?}");
        assert!(v[0].contains("one key, one law"), "{v:?}");
    }

    #[test]
    fn a_violation_names_the_decision_to_go_back_to() {
        let row = Law {
            verifier: "gone",
            authority: Authority::Searched,
            record: "2026-08-29",
            ..sound_row()
        };
        let v = check(&[row]);
        assert!(
            v[0].contains("searched") && v[0].contains("2026-08-29"),
            "a broken carrier must say whose decision it was: {v:?}",
        );
    }

    fn corpus(files_read: usize, keys: &[&str]) -> Corpus {
        Corpus {
            citations: keys
                .iter()
                .map(|k| Citation {
                    key: (*k).to_string(),
                    file: "CLAUDE.md".to_string(),
                    line: 1,
                })
                .collect(),
            files_read,
        }
    }

    #[test]
    fn an_absent_corpus_is_an_answer_not_a_stale_list() {
        // A checkout of the tracked tree has no CLAUDE.md at all. Reporting
        // every entry as stale would be a claim about prose that is not here.
        let verdict = direction_a(&corpus(0, &[]), &[sound_row()], &[("ghost", "law site")]);
        assert_eq!(verdict, DirectionA::NoCorpus);
        // The same empty citation set *with* prose present is a real staleness.
        let verdict = direction_a(&corpus(18, &[]), &[sound_row()], &[("ghost", "law site")]);
        assert!(matches!(verdict, DirectionA::Unresolved { .. }));
    }

    #[test]
    fn direction_a_names_both_ways_the_list_can_be_wrong() {
        let verdict = direction_a(
            &corpus(18, &["widget-alignment"]),
            &[sound_row()],
            &[("ghost", "law site")],
        );
        let DirectionA::Unresolved { unlisted, stale } = verdict else {
            panic!("both a new name and a dead entry must fail");
        };
        assert!(unlisted[0].contains("widget-alignment"), "{unlisted:?}");
        assert!(stale[0].contains("ghost"), "{stale:?}");
        // A registered name is silenced by its row, not by the list.
        assert_eq!(
            direction_a(&corpus(18, &["two-claims placement"]), &[sound_row()], &[]),
            DirectionA::Resolved,
        );
    }

    #[test]
    fn an_unqualified_entry_must_be_unique_and_use_the_closed_vocabulary() {
        assert!(wellformed_problems(&[("a", "law site"), ("b", "kind of law")]).is_empty());

        let dup = wellformed_problems(&[("a", "law site"), ("a", "kind of law")]);
        assert_eq!(dup.len(), 1, "a duplicate key is absorbed by the set comparison, so it has to be caught here: {dup:?}");
        assert!(dup[0].contains("more than once"), "{dup:?}");

        let bad = wellformed_problems(&[("a", "not really a law")]);
        assert_eq!(bad.len(), 1, "{bad:?}");
        assert!(bad[0].contains("not one of the six"), "{bad:?}");

        assert_eq!(
            wellformed_problems(&[("a", "")]).len(),
            1,
            "an empty reason claims nothing"
        );
    }

    #[test]
    fn a_registered_key_resolves_and_an_unregistered_one_does_not() {
        let cites = [
            Citation {
                key: "two-claims placement".to_string(),
                file: "CLAUDE.md".to_string(),
                line: 3,
            },
            Citation {
                key: "widget-alignment".to_string(),
                file: "src/trail/CLAUDE.md".to_string(),
                line: 9,
            },
        ];
        let out = unregistered_keys(&cites, &[sound_row()]);
        assert_eq!(out.len(), 1, "a row silences its own name: {out:?}");
        assert_eq!(out["widget-alignment"], ["src/trail/CLAUDE.md:9"]);
    }
}
