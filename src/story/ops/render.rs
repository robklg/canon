//! The place walk — the composed reading over a `StoryReport`.
//!
//! The walk is an artifact composition, not terminal display (the
//! receipt-body precedent): this module composes the lines, consumers print
//! or bind them. Today's one consumer is `canon roots story` (the live,
//! judgment reading — present tense, trail handoffs, the open questions of a
//! deciding reader). The bound telling — the reference reading the
//! retirement compile writes into the book — arrives as its own voicing
//! over this same composition; structure is computed once here so the two
//! can never drift.

use crate::domain::format::{format_count, format_date, format_size, format_time_ago, shell_quote};
use crate::story::domain::acts::ActGroup;
use crate::story::domain::locations::LocationAggregate;
use crate::story::domain::place::StoryPlace;

use super::report::StoryReport;

/// The two readers of the one composition. Structure — the place walk, the
/// slices, the once-rules, every aggregate — is computed once; only wordings
/// differ, and they differ *by voicing*, never by consumer accident.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Voicing {
    /// The live review (`canon roots story`): present tense, judgment
    /// furniture ("no decision here"), trail handoffs — the deciding reader.
    Judgment,
    /// The bound reading (the book's `story.md`): the ever-axis — what was
    /// ever here, told by where it went — in plain fate diction, no
    /// handoffs, no bind-time claims. The future reader, without Canon.
    Reference,
}

/// The whole report as lines — pure, so rendering is testable.
pub fn story_lines(report: &StoryReport, cap: usize, now: i64) -> Vec<String> {
    let mut lines = Vec::new();
    lines.push(format!("Story: {}", report.root.path));
    lines.push(String::new());
    lines.push(format!("  role           {}", report.root.role));
    if let Some(comment) = &report.root.comment {
        lines.push(format!("  comment        {comment}"));
    }
    if report.root.is_suspended() {
        lines.push("  suspended      yes".to_string());
    }
    lines.push(format!(
        "  first indexed  {}",
        match report.first_indexed {
            Some(ts) => format_date(ts),
            None => "unknown".to_string(),
        }
    ));
    lines.push(format!(
        "  last scan      {}",
        match report.root.last_scanned_at {
            Some(ts) => format!("{} ({})", format_date(ts), format_time_ago(Some(ts), now)),
            None => "never".to_string(),
        }
    ));
    if !report.reachable {
        lines.push("  unreachable    the story as last observed — reconnect to verify".to_string());
    }
    lines.push(String::new());
    lines.push("The places".to_string());

    let mut shown = 0usize;
    let mut omitted = 0usize;
    render_place(
        &report.places,
        0,
        &report.root.path,
        Voicing::Judgment,
        cap,
        &mut shown,
        &mut omitted,
        &mut lines,
    );
    if omitted > 0 {
        lines.push(String::new());
        lines.push(format!(
            "  … and {omitted} more places (--all shows everything)"
        ));
    }

    let account = &report.account;
    lines.push(String::new());
    let mut unresolved = format_count(account.unresolved);
    if account.unhashed_unresolved > 0 {
        unresolved.push_str(&format!(
            " ({} never hashed)",
            format_count(account.unhashed_unresolved)
        ));
    }
    let mut parts: Vec<String> = Vec::new();
    if account.archived_standing > 0 {
        parts.push(format!(
            "{} archived from here",
            format_count(account.archived_standing)
        ));
    }
    parts.push(format!("{} covered", format_count(account.covered)));
    parts.push(format!("{} excluded", format_count(account.excluded)));
    if account.contentless > 0 {
        parts.push(format!("{} empty files", format_count(account.contentless)));
    }
    parts.push(format!("{unresolved} unresolved"));
    lines.push(format!(
        "Standing: {} sources — {}",
        format_count(account.standing()),
        parts.join(" · "),
    ));
    lines.push("Whether this story is complete is yours to judge.".to_string());
    lines.push(format!(
        "For the readiness gate: canon roots retire path:{} --dry-run",
        report.root.path
    ));
    lines
}

/// The place map alone, reference-voiced — the retirement compile frames
/// it into the bound telling. Always full: the bound story admits no
/// display cap, so the place-omission line is structurally unreachable
/// here; only the location aggregates' own honesty remainders may count
/// omissions.
pub fn reference_place_lines(report: &StoryReport) -> Vec<String> {
    let mut lines = Vec::new();
    let mut shown = 0usize;
    let mut omitted = 0usize;
    render_place(
        &report.places,
        0,
        &report.root.path,
        Voicing::Reference,
        usize::MAX,
        &mut shown,
        &mut omitted,
        &mut lines,
    );
    lines
}

/// `1 file` / `N files` — the reference voicing pluralizes; the judgment
/// voicing keeps its historical fixed plural (byte-parity law).
pub fn file_noun(n: i64) -> String {
    if n == 1 {
        "1 file".to_string()
    } else {
        format!("{} files", format_count(n))
    }
}

/// Whether a place earns its own block (the bare root is forced anyway).
fn place_renderable(place: &StoryPlace) -> bool {
    !place.acts.is_empty()
        || !place.standing.is_empty()
        || !place.covered_where.is_empty()
        || !place.notes.is_empty()
}

fn count_renderable(place: &StoryPlace) -> usize {
    place_renderable(place) as usize + place.children.iter().map(count_renderable).sum::<usize>()
}

#[allow(clippy::too_many_arguments)]
fn render_place(
    place: &StoryPlace,
    depth: usize,
    root_path: &str,
    voicing: Voicing,
    cap: usize,
    shown: &mut usize,
    omitted: &mut usize,
    lines: &mut Vec<String>,
) {
    let forced_root = depth == 0 && place.children.is_empty();
    let renderable = place_renderable(place) || forced_root;
    if renderable {
        if *shown >= cap {
            // The whole subtree drops; the omission line carries the count.
            *omitted += count_renderable(place);
            return;
        }
        *shown += 1;
        let indent = "  ".repeat(depth + 1);
        lines.push(String::new());
        let name = if place.rel_path.is_empty() {
            "(root)".to_string()
        } else {
            place.rel_path.clone()
        };
        let breadth = if place.folder_breadth > 1 {
            format!(
                "   · across {} folders",
                format_count(place.folder_breadth as i64)
            )
        } else {
            String::new()
        };
        lines.push(format!("{indent}{name}{breadth}"));
        for group in &place.acts {
            act_lines(group, &indent, voicing, lines);
        }
        // "no decision here" speaks for the question content only — covered,
        // unresolved, missing: nothing evidences a decision either way.
        // Excluded standing is different: exclusion is always a deliberate
        // act, so at an undecided place it evidences an UNRECORDED decision
        // — its line says so instead (never "no decision here"). Archived
        // standing likewise evidences the apply; contentless has nothing to
        // decide (the contentless law) — neither joins the question here.
        // Judgment furniture: the reference reading states facts — its
        // "preserved by copies" definition (the entries guide) already says
        // nothing was chosen; an open question has no place in a book.
        let question =
            place.standing.covered + place.standing.unresolved + place.standing.missing_unexplained;
        if voicing == Voicing::Judgment && place.undecided() && question > 0 {
            lines.push(format!("{indent}  no decision here"));
        }
        if voicing == Voicing::Judgment
            && place.undecided()
            && place.standing.is_empty()
            && place.covered_where.is_empty()
            && !place.notes.is_empty()
            && place.children.is_empty()
        {
            // A note-forced leaf whose content is all gone: say so, rather
            // than leaving the testimony hanging beside nothing. What left
            // is narrated by the containing place's act slices. A noted
            // place WITH children stays bare — its content stands one line
            // down, claimed by the deeper places. Bind-time furniture: the
            // reference voicing drops it (the ever-axis makes no "now"
            // claims; the note stands where it was written).
            lines.push(format!("{indent}  nothing stands here now"));
        }
        standing_lines(place, &indent, voicing, lines);
        for note in &place.notes {
            lines.push(format!(
                "{indent}  note: {}",
                indent_multiline(&note.text, &format!("{indent}        "))
            ));
        }
        if !place_renderable(place) {
            lines.push(match voicing {
                Voicing::Judgment => format!("{indent}  nothing indexed here"),
                Voicing::Reference => format!("{indent}  nothing was ever indexed here"),
            });
        }
        if voicing == Voicing::Judgment {
            let abs = if place.rel_path.is_empty() {
                root_path.to_string()
            } else {
                format!("{root_path}/{}", place.rel_path)
            };
            let (display, _argv) = trail_handoff(&abs);
            lines.push(format!("{indent}  {display}"));
        }
    }
    let child_depth = if renderable { depth + 1 } else { depth };
    for child in &place.children {
        render_place(
            child,
            child_depth,
            root_path,
            voicing,
            cap,
            shown,
            omitted,
            lines,
        );
    }
}

/// One act group in the what/why register. The arrow means *sent there by
/// your act* — observed coverage renders with "copies stand in" instead,
/// never the arrow.
///
/// Reference voicing: content leads, fate follows (`N files · chosen for
/// the archive → dest`) — the ever-axis in the line shape itself. The
/// moved/copied split is omitted (a bind-time mechanical fact; the dossier
/// keeps it), and the scan-observed marker folds into the fate phrase.
fn act_lines(group: &ActGroup, indent: &str, voicing: Voicing, lines: &mut Vec<String>) {
    let mut line = match voicing {
        Voicing::Judgment => format!(
            "{indent}  {} {} files",
            group.transition,
            format_count(group.files)
        ),
        Voicing::Reference => format!("{indent}  {}", file_noun(group.files)),
    };
    if let Some(bytes) = group.bytes {
        if bytes > 0 {
            line.push_str(&format!(", {}", format_size(bytes)));
        }
    }
    match voicing {
        Voicing::Judgment => {
            if group.observed {
                line.push_str(" (scan-observed)");
            }
            if let (Some(moved), Some(copied)) = (group.moved, group.copied) {
                if moved > 0 && copied > 0 {
                    line.push_str(&format!(
                        " ({} moved, {} copied)",
                        format_count(moved),
                        format_count(copied)
                    ));
                }
            }
        }
        Voicing::Reference => {
            // The reference derivation over the registered transition words
            // (the never-literal law's voicing site: registered word in,
            // plain fate phrase out). An unrecognized word renders raw —
            // stated, never dropped.
            let phrase = match group.transition {
                "archived" => "chosen for the archive",
                "excluded" => "let go",
                "restored" => "returned to consideration",
                "deleted" if group.observed => "deleted — a scan observed the loss",
                other => other,
            };
            line.push_str(&format!(" · {phrase}"));
        }
    }
    if !group.destination.is_empty() {
        line.push_str(&format!(" → {}", fmt_locations(&group.destination)));
    }
    if group.decisions.len() == 1 {
        let decision = &group.decisions[0];
        line.push_str(&format!("   #{}", decision.id));
        // The once-rule: the full reason renders only at the decision's
        // first emitted slice in reading order; every other slice cites the
        // bare id.
        if let Some(reason) = &decision.reason {
            if decision.reason_here {
                line.push_str(&format!(
                    " · \"{}\"",
                    indent_multiline(reason, &format!("{indent}      "))
                ));
            }
        }
        lines.push(line);
    } else {
        line.push_str(&format!("   across {} decisions", group.decisions.len()));
        lines.push(line);
        let summary = group.reason_summary();
        for (reason, ids) in &summary.reasons {
            let ids = ids
                .iter()
                .map(|id| format!("#{id}"))
                .collect::<Vec<_>>()
                .join(", ");
            lines.push(format!(
                "{indent}    · \"{}\"   {ids}",
                indent_multiline(reason, &format!("{indent}       "))
            ));
        }
        if !summary.cited.is_empty() {
            let ids = summary
                .cited
                .iter()
                .map(|id| format!("#{id}"))
                .collect::<Vec<_>>()
                .join(", ");
            lines.push(format!("{indent}    · {ids}"));
        }
        if !summary.without_reason.is_empty() {
            // Ids, not a bare count: "without reason" must never read as
            // "without decision" — these are real recorded acts.
            let ids = summary
                .without_reason
                .iter()
                .map(|id| format!("#{id}"))
                .collect::<Vec<_>>()
                .join(", ");
            if summary.without_reason.len() == 1 {
                lines.push(format!("{indent}    · {ids} — no reason given"));
            } else {
                lines.push(format!("{indent}    · {ids} — without reason"));
            }
        }
    }
}

fn standing_lines(place: &StoryPlace, indent: &str, voicing: Voicing, lines: &mut Vec<String>) {
    let standing = &place.standing;
    if standing.archived > 0 && voicing == Voicing::Judgment {
        // Judgment-only: on the ever-axis the act register owns this fate —
        // "N archived from here (still standing)" is exactly the bind-time
        // claim the reference reading forswears. Its covered-where rider
        // drops with it: the act's arrow already answers where.
        let mut line = format!(
            "{indent}  {} archived from here",
            format_count(standing.archived)
        );
        // The covered-where answer serves both archive-standing buckets;
        // it rides the covered line when one renders, else lands here.
        if standing.covered == 0 && !place.covered_where.is_empty() {
            line.push_str(&format!(
                " — copies stand in {}",
                fmt_locations(&place.covered_where)
            ));
        }
        lines.push(line);
    }
    if standing.covered > 0 {
        let mut line = match voicing {
            Voicing::Judgment => {
                format!("{indent}  {} covered", format_count(standing.covered))
            }
            Voicing::Reference => format!(
                "{indent}  {} · preserved by copies in the archive",
                file_noun(standing.covered)
            ),
        };
        if !place.covered_where.is_empty() {
            let locations = fmt_locations(&place.covered_where);
            line.push_str(&match voicing {
                Voicing::Judgment => format!(" — copies stand in {locations}"),
                Voicing::Reference => format!(" — {locations}"),
            });
        }
        lines.push(line);
    }
    if standing.contentless > 0 {
        // The standing stated, and why it is outside the coverage question
        // — never what happened to these files: a pre-law apply may have
        // left them behind, and this line cannot see either way (the
        // carried-with-this-place wording claimed history it couldn't
        // verify; friction 2026-08-04). Same phrase coverage uses — one
        // vocabulary across surfaces. The reference reading keeps the bare
        // referent; the entries guide defines it once.
        let noun = if standing.contentless == 1 {
            "empty file"
        } else {
            "empty files"
        };
        lines.push(match voicing {
            Voicing::Judgment => format!(
                "{indent}  {} {noun} (no content to cover)",
                format_count(standing.contentless)
            ),
            Voicing::Reference => {
                format!("{indent}  {} {noun}", format_count(standing.contentless))
            }
        });
    }
    if standing.excluded > 0 && !place.standing_coincides() {
        let mut line = match voicing {
            Voicing::Judgment => {
                format!("{indent}  {} excluded", format_count(standing.excluded))
            }
            Voicing::Reference => {
                format!("{indent}  {} · let go", file_noun(standing.excluded))
            }
        };
        // Exclusion is always a deliberate act, so a stampless row
        // evidences a decision whose record is absent (pre-provenance, or
        // recording off) — state the gap for exactly those rows. The marker
        // is row-grain, never place-grain: a place-level gate was wrong in
        // both directions (a mixed place's stampless rows earned no marker;
        // an undecided place's stamped rows earned a false one).
        let stampless = standing.excluded_stampless;
        if stampless == standing.excluded {
            line.push_str(match voicing {
                Voicing::Judgment => " (no recorded decision)",
                Voicing::Reference => " — no record of the decision survives",
            });
        } else if stampless > 0 {
            line.push_str(&match voicing {
                Voicing::Judgment => {
                    format!(" (no recorded decision for {})", format_count(stampless))
                }
                Voicing::Reference => format!(
                    " — for {} of these, no record of the decision survives",
                    format_count(stampless)
                ),
            });
        }
        lines.push(line);
    }
    if standing.unresolved > 0 {
        let mut line = match voicing {
            Voicing::Judgment => {
                format!("{indent}  {} unresolved", format_count(standing.unresolved))
            }
            Voicing::Reference => format!(
                "{indent}  {} · no known copy in the archive",
                file_noun(standing.unresolved)
            ),
        };
        if standing.unhashed_unresolved > 0 {
            line.push_str(&match voicing {
                Voicing::Judgment => format!(
                    " ({} never hashed — cannot be content-verified)",
                    format_count(standing.unhashed_unresolved)
                ),
                Voicing::Reference => format!(
                    " ({} were never content-checked)",
                    format_count(standing.unhashed_unresolved)
                ),
            });
        }
        lines.push(line);
    }
    if standing.missing_unexplained > 0 {
        lines.push(match voicing {
            Voicing::Judgment => format!(
                "{indent}  {} missing, unexplained",
                format_count(standing.missing_unexplained)
            ),
            Voicing::Reference => format!(
                "{indent}  {} · went missing, without a recorded reason",
                file_noun(standing.missing_unexplained)
            ),
        });
    }
}

/// A location aggregate for one line: a single coherent answer renders as
/// the bare path; a genuine divergence lists prefixes with counts; the
/// remainder is counted, never silent.
pub fn fmt_locations(agg: &LocationAggregate) -> String {
    let mut out = if agg.locations.len() == 1 && agg.omitted_locations == 0 {
        agg.locations[0].path.clone()
    } else {
        agg.locations
            .iter()
            .map(|l| format!("{} ({})", l.path, format_count(l.files)))
            .collect::<Vec<_>>()
            .join(", ")
    };
    if agg.omitted_locations > 0 {
        out.push_str(&format!(" … and {} more locations", agg.omitted_locations));
    }
    out
}

fn indent_multiline(text: &str, indent: &str) -> String {
    text.replace('\n', &format!("\n{indent}"))
}

/// The drill-down handoff: display and argv from one builder, so the
/// round-trip test parses exactly what the user sees (the sweep's law).
/// The test lives with the clap definitions (interface), calling this.
pub fn trail_handoff(abs_path: &str) -> (String, Vec<String>) {
    let argv: Vec<String> = vec!["canon".into(), "trail".into(), abs_path.into()];
    let display = argv
        .iter()
        .map(|a| shell_quote(a))
        .collect::<Vec<_>>()
        .join(" ");
    (format!("→ {display}"), argv)
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;
    use crate::core::domain::resolution::ResolutionAccount;
    use crate::domain::note::Note;
    use crate::domain::root::Root;
    use crate::story::domain::acts::{ActDecision, ActGroup};
    use crate::story::domain::locations::{LocationAggregate, LocationCount};
    use crate::story::domain::place::PlaceStanding;

    // `place`/`locations`/`act`/`report`/`report_with`/`zero_account` below
    // are deliberately forked with `retire/ops/frame.rs`'s copies, not
    // shared — see that file's matching comment for why. Each builder
    // constructs its domain type via an exhaustive struct literal (no `..`
    // spread), so a field added to the type breaks both copies at compile
    // time rather than letting them silently drift apart in shape.
    fn place(rel: &str) -> StoryPlace {
        StoryPlace {
            rel_path: rel.to_string(),
            acts: vec![],
            standing: PlaceStanding::default(),
            covered_where: LocationAggregate::default(),
            notes: vec![],
            folder_breadth: 0,
            children: vec![],
        }
    }

    fn locations(entries: &[(&str, i64)]) -> LocationAggregate {
        LocationAggregate {
            locations: entries
                .iter()
                .map(|(path, files)| LocationCount {
                    path: path.to_string(),
                    files: *files,
                })
                .collect(),
            omitted_locations: 0,
        }
    }

    fn act(transition: &'static str, files: i64, decisions: Vec<(i64, Option<&str>)>) -> ActGroup {
        ActGroup {
            transition,
            observed: false,
            destination: LocationAggregate::default(),
            files,
            present_files: 0,
            bytes: None,
            moved: None,
            copied: None,
            decisions: decisions
                .into_iter()
                .enumerate()
                .map(|(i, (id, reason))| ActDecision {
                    id,
                    created_at: (i as i64 + 1) * 100,
                    reason: reason.map(str::to_string),
                    reason_here: true,
                })
                .collect(),
        }
    }

    fn zero_account() -> ResolutionAccount {
        ResolutionAccount {
            archived_files: 0,
            archived_bytes: None,
            archived_moved: 0,
            archived_copied: 0,
            archived_unrecorded: 0,
            deleted: 0,
            unexplained_missing: 0,
            archived_standing: 0,
            covered: 0,
            excluded: 0,
            contentless: 0,
            unresolved: 0,
            unhashed_unresolved: 0,
        }
    }

    fn report(places: StoryPlace) -> StoryReport {
        report_with(places, zero_account())
    }

    fn report_with(places: StoryPlace, account: ResolutionAccount) -> StoryReport {
        StoryReport {
            root: Root {
                id: 1,
                path: "/r".to_string(),
                role: "source".to_string(),
                comment: None,
                last_scanned_at: None,
                suspended: false,
            },
            first_indexed: None,
            reachable: true,
            places,
            account,
        }
    }

    fn assert_has_line(lines: &[String], needle: &str) {
        assert!(
            lines.iter().any(|l| l.contains(needle)),
            "missing {needle:?} in:\n{}",
            lines.join("\n")
        );
    }

    fn assert_no_line(lines: &[String], needle: &str) {
        assert!(
            !lines.iter().any(|l| l.contains(needle)),
            "unexpected {needle:?} in:\n{}",
            lines.join("\n")
        );
    }

    #[test]
    fn rendering_shows_containment_acts_and_the_undecided() {
        let mut italy = place("pictures/italy");
        italy.standing.covered = 2;
        italy.covered_where = locations(&[("/archive/a", 1), ("/archive/b", 1)]);

        let mut pictures = place("pictures");
        let mut archived = act("archived", 5, vec![(42, Some("the Italy trip"))]);
        archived.destination = locations(&[("/archive/media", 5)]);
        pictures.acts.push(archived);
        pictures.children.push(italy);

        let mut root = place("");
        root.children.push(pictures);

        let lines = story_lines(&report(root), usize::MAX, 0);
        assert_has_line(&lines, "  pictures");
        assert_has_line(
            &lines,
            "archived 5 files → /archive/media   #42 · \"the Italy trip\"",
        );
        assert_has_line(&lines, "    pictures/italy");
        assert_has_line(&lines, "      no decision here");
        assert_has_line(
            &lines,
            "      2 covered — copies stand in /archive/a (1), /archive/b (1)",
        );
        assert_has_line(&lines, "→ canon trail /r/pictures");
        assert_has_line(&lines, "→ canon trail /r/pictures/italy");
        assert_has_line(&lines, "Whether this story is complete is yours to judge.");
        assert_has_line(&lines, "canon roots retire path:/r --dry-run");
    }

    #[test]
    fn multi_decision_acts_enumerate_the_whys() {
        let mut old = place("old");
        old.acts.push(act(
            "excluded",
            4890,
            vec![
                (57, Some("installer junk")),
                (61, Some("installer junk")),
                (63, None),
            ],
        ));
        let mut root = place("");
        root.children.push(old);

        let lines = story_lines(&report(root), usize::MAX, 0);
        assert_has_line(&lines, "excluded 4,890 files   across 3 decisions");
        assert_has_line(&lines, "· \"installer junk\"   #57, #61");
        // The reasonless decision shows its id — "without reason" must
        // never read as "without decision".
        assert_has_line(&lines, "· #63 — no reason given");
    }

    #[test]
    fn coincidence_omits_the_excluded_line() {
        // The stutter resolved: the excluded standing is exactly what the
        // act narrates (same count, all standing), so the standing line
        // says nothing the act line hasn't.
        let mut old = place("old");
        let mut group = act("excluded", 176, vec![(57, Some("installer junk"))]);
        group.present_files = 176;
        old.acts.push(group);
        old.standing.excluded = 176;
        let mut root = place("");
        root.children.push(old);

        let lines = story_lines(&report(root), usize::MAX, 0);
        assert_has_line(&lines, "excluded 176 files   #57 · \"installer junk\"");
        assert_no_line(&lines, "176 excluded");
    }

    #[test]
    fn tombstone_mismatch_renders_both() {
        // The act's whole-history count exceeds what stands: omitting the
        // standing line would misread as all three still standing.
        let mut old = place("old");
        let mut group = act("excluded", 3, vec![(57, None)]);
        group.present_files = 2;
        old.acts.push(group);
        old.standing.excluded = 2;
        let mut root = place("");
        root.children.push(old);

        let lines = story_lines(&report(root), usize::MAX, 0);
        assert_has_line(&lines, "excluded 3 files   #57");
        assert_has_line(&lines, "2 excluded");
    }

    #[test]
    fn coincident_excluded_omits_even_beside_covered() {
        // Amended 2026-08-04 (the excluded-twice friction): covered/
        // unresolved/missing stay never-omittable and render their own
        // lines, but they no longer force a bare restatement of the act
        // register's excluded count beside them — exact coincidence omits
        // the excluded line regardless of the other buckets.
        let mut old = place("old");
        let mut group = act("excluded", 2, vec![(57, None)]);
        group.present_files = 2;
        old.acts.push(group);
        old.standing.excluded = 2;
        old.standing.covered = 1;
        let mut root = place("");
        root.children.push(old);

        let lines = story_lines(&report(root), usize::MAX, 0);
        assert_has_line(&lines, "1 covered");
        assert_has_line(&lines, "excluded 2 files"); // the act register narrates
        assert_no_line(&lines, "2 excluded"); // the standing restatement is gone
    }

    #[test]
    fn a_cited_slice_shows_the_bare_id() {
        // A slice whose reason renders elsewhere cites the bare id — no
        // quote, no repetition.
        let mut old = place("old");
        let mut group = act("excluded", 4, vec![(57, Some("installer junk"))]);
        group.decisions[0].reason_here = false;
        old.acts.push(group);
        old.standing.excluded = 4;
        let mut root = place("");
        root.children.push(old);

        let lines = story_lines(&report(root), usize::MAX, 0);
        assert_has_line(&lines, "excluded 4 files   #57");
        assert_no_line(&lines, "installer junk");
    }

    #[test]
    fn cited_ids_share_one_line_in_the_register() {
        // Multi-decision register: reasoned-here entries render in full,
        // cited ids collapse to one bare line, and "without reason" stays
        // an exact truth-claim about decisions with no reason anywhere.
        let mut old = place("old");
        let mut group = act(
            "excluded",
            500,
            vec![
                (131, Some("scattered sweep")),
                (155, Some("old exports")),
                (63, None),
            ],
        );
        group.decisions[0].reason_here = false; // #131 cited
        old.acts.push(group);
        let mut root = place("");
        root.children.push(old);

        let lines = story_lines(&report(root), usize::MAX, 0);
        assert_has_line(&lines, "· \"old exports\"   #155");
        assert_has_line(&lines, "· #131");
        assert_has_line(&lines, "· #63 — no reason given");
        assert_no_line(&lines, "scattered sweep");
    }

    #[test]
    fn observed_deletions_read_as_observations() {
        let mut gone = place("gone");
        let mut deleted = act("deleted", 1204, vec![(70, None)]);
        deleted.observed = true;
        gone.acts.push(deleted);
        let mut root = place("");
        root.children.push(gone);

        let lines = story_lines(&report(root), usize::MAX, 0);
        assert_has_line(&lines, "deleted 1,204 files (scan-observed)   #70");
    }

    #[test]
    fn notes_render_verbatim_at_their_place() {
        let mut keep = place("keep");
        keep.notes.push(Note {
            id: 7,
            root_id: 1,
            rel_path: "keep".to_string(),
            text: "beautiful pictures, still need a home".to_string(),
            created_at: 100,
        });
        let mut root = place("");
        root.children.push(keep);

        let lines = story_lines(&report(root), usize::MAX, 0);
        assert_has_line(&lines, "note: beautiful pictures, still need a home");
    }

    #[test]
    fn a_note_forced_empty_leaf_says_nothing_stands_here_now() {
        // The honest form for a noted file whose content moved away: the
        // testimony is not left hanging beside nothing. Judgment furniture —
        // the reference voicing drops it (the ever-axis makes no "now"
        // claims).
        let build = || {
            let mut noted = place("moved-away.jpg");
            noted.notes.push(Note {
                id: 7,
                root_id: 1,
                rel_path: "moved-away.jpg".to_string(),
                text: "the good one".to_string(),
                created_at: 100,
            });
            let mut root = place("");
            root.children.push(noted);
            root
        };

        let judgment = story_lines(&report(build()), usize::MAX, 0);
        assert_has_line(&judgment, "nothing stands here now");

        let reference = reference_place_lines(&report(build()));
        assert_no_line(&reference, "nothing stands here now");
    }

    #[test]
    fn the_cap_counts_omissions_and_keeps_the_close() {
        let mut root = place("");
        for name in ["a", "b", "c"] {
            let mut child = place(name);
            child.standing.unresolved = 1;
            root.children.push(child);
        }
        let lines = story_lines(&report(root), 1, 0);
        assert_has_line(&lines, "… and 2 more places (--all shows everything)");
        assert_has_line(&lines, "Standing: 0 sources");
    }

    #[test]
    fn the_cap_never_leaves_a_reason_citation_dangling() {
        // The once-rule renders a decision's full reason at its first slice
        // in render order and cites the bare id everywhere else; the cap
        // truncates in that same order, as a prefix cut. Together they
        // guarantee a rendered citation always has its reason rendered in
        // the same output — reorder the walk or change the cap's shape and
        // the site place can drop while a citing slice survives, leaving a
        // bare id that points at nothing.
        let reason = "duplicates of the album";
        let build = || {
            let mut early = place("a-early");
            early
                .acts
                .push(act("excluded", 2, vec![(31, Some(reason))]));
            let mut late = place("z-late");
            let mut cited = act("excluded", 3, vec![(31, Some(reason))]);
            cited.decisions[0].reason_here = false;
            late.acts.push(cited);
            let mut root = place("");
            root.children.push(early);
            root.children.push(late);
            root
        };

        for cap in [1, 2] {
            let lines = story_lines(&report(build()), cap, 0);
            let has_bare_cite = lines
                .iter()
                .any(|l| l.contains("#31") && !l.contains(reason));
            if has_bare_cite {
                assert!(
                    lines.iter().any(|l| l.contains(reason)),
                    "citation #31 dangles at cap {cap} in:\n{}",
                    lines.join("\n")
                );
            }
        }
    }

    #[test]
    fn judgment_keeps_the_historical_fixed_plural_on_one_file() {
        // The judgment act line is a fixed plural by the byte-parity pin —
        // "excluded 1 files" is the historical wording. Routing it through
        // the reference voicing's file_noun would silently change pinned
        // output; this pins the singular case the golden fixture misses.
        let mut lone = place("lone");
        lone.acts.push(act("excluded", 1, vec![(7, None)]));
        let mut root = place("");
        root.children.push(lone);

        let lines = story_lines(&report(root), usize::MAX, 0);
        assert_has_line(&lines, "excluded 1 files");
    }

    #[test]
    fn a_bare_root_still_tells_its_empty_story() {
        let lines = story_lines(&report(place("")), usize::MAX, 0);
        assert_has_line(&lines, "  (root)");
        assert_has_line(&lines, "nothing indexed here");
    }

    // -----------------------------------------------------------------
    // The reference voicing — the bound reading's wordings over the same
    // structure. Every dropped line is asserted absent: the ever-axis
    // makes no bind-time claims and carries no judgment furniture.
    // -----------------------------------------------------------------

    #[test]
    fn reference_leads_with_content_and_names_the_fate() {
        let mut pictures = place("pictures");
        let mut archived = act("archived", 5, vec![(42, Some("the Italy trip"))]);
        archived.destination = locations(&[("/archive/media", 5)]);
        pictures.acts.push(archived);
        let mut root = place("");
        root.children.push(pictures);

        let lines = reference_place_lines(&report(root));
        assert_has_line(
            &lines,
            "5 files · chosen for the archive → /archive/media   #42 · \"the Italy trip\"",
        );
        assert_no_line(&lines, "archived 5 files");
    }

    #[test]
    fn reference_drops_judgment_furniture_and_handoffs() {
        let mut italy = place("pictures/italy");
        italy.standing.covered = 2;
        italy.covered_where = locations(&[("/archive/a", 1), ("/archive/b", 1)]);
        let mut root = place("");
        root.children.push(italy);

        let lines = reference_place_lines(&report(root));
        assert_no_line(&lines, "no decision here");
        assert_no_line(&lines, "canon trail");
        assert_no_line(&lines, "copies stand in");
        assert_has_line(
            &lines,
            "2 files · preserved by copies in the archive — /archive/a (1), /archive/b (1)",
        );
    }

    #[test]
    fn reference_drops_the_archived_standing_line() {
        // The act register owns the fate on the ever-axis; "still standing
        // here" is exactly the bind-time claim the book forswears. The
        // covered-where rider drops with it — the act's arrow answers where.
        let build = || {
            let mut kept = place("kept");
            let mut archived = act("archived", 10, vec![(42, None)]);
            archived.destination = locations(&[("/archive/media", 10)]);
            kept.acts.push(archived);
            kept.standing.archived = 10;
            kept.covered_where = locations(&[("/archive/media", 10)]);
            let mut root = place("");
            root.children.push(kept);
            root
        };

        let judgment = story_lines(&report(build()), usize::MAX, 0);
        assert_has_line(
            &judgment,
            "10 archived from here — copies stand in /archive/media",
        );

        let lines = reference_place_lines(&report(build()));
        assert_no_line(&lines, "archived from here");
        assert_no_line(&lines, "copies stand in");
        assert_has_line(&lines, "10 files · chosen for the archive → /archive/media");
    }

    #[test]
    fn reference_words_exclusion_as_let_go() {
        let mut old = place("old");
        old.acts
            .push(act("excluded", 3, vec![(57, Some("installer junk"))]));
        old.standing.excluded = 2; // tombstone mismatch: both lines render
        let mut root = place("");
        root.children.push(old);

        let lines = reference_place_lines(&report(root));
        assert_has_line(&lines, "3 files · let go   #57 · \"installer junk\"");
        assert_has_line(&lines, "2 files · let go");
        assert_no_line(&lines, "excluded 3 files");
    }

    #[test]
    fn reference_states_the_unrecorded_decision_plainly() {
        let mut old = place("old");
        old.standing.excluded = 4;
        old.standing.excluded_stampless = 4;
        let mut root = place("");
        root.children.push(old);

        let lines = reference_place_lines(&report(root));
        assert_has_line(
            &lines,
            "4 files · let go — no record of the decision survives",
        );
        assert_no_line(&lines, "(no recorded decision)");
    }

    #[test]
    fn the_no_record_marker_is_row_grain_not_place_grain() {
        // The mixed-place shape: cited acts at the place AND a stampless
        // standing remainder — the marker names exactly the stampless rows.
        let build = || {
            let mut mixed = place("usr");
            mixed
                .acts
                .push(act("excluded", 3, vec![(153, Some("not important"))]));
            mixed.standing.excluded = 10;
            mixed.standing.excluded_stampless = 7;
            let mut root = place("");
            root.children.push(mixed);
            root
        };

        let lines = reference_place_lines(&report(build()));
        assert_has_line(
            &lines,
            "10 files · let go — for 7 of these, no record of the decision survives",
        );

        let judgment = story_lines(&report(build()), usize::MAX, 0);
        assert!(
            judgment
                .iter()
                .any(|l| l.contains("10 excluded (no recorded decision for 7)")),
            "missing judgment partial marker in:\n{}",
            judgment.join("\n")
        );
    }

    #[test]
    fn stamped_standing_at_an_undecided_place_earns_no_false_marker() {
        // Regression: the old place-level gate marked every excluded row at
        // an act-less place "no record survives" — false when the rows are
        // stamped by decisions whose slices render elsewhere.
        let mut old = place("old");
        old.standing.excluded = 4;
        old.standing.excluded_stampless = 0;
        let mut root = place("");
        root.children.push(old);

        let lines = reference_place_lines(&report(root));
        assert_has_line(&lines, "4 files · let go");
        assert_no_line(&lines, "no record of the decision survives");
    }

    #[test]
    fn reference_coincidence_still_omits_the_restatement() {
        let mut old = place("old");
        let mut group = act("excluded", 176, vec![(57, Some("installer junk"))]);
        group.present_files = 176;
        old.acts.push(group);
        old.standing.excluded = 176;
        let mut root = place("");
        root.children.push(old);

        let lines = reference_place_lines(&report(root));
        assert_has_line(&lines, "176 files · let go   #57 · \"installer junk\"");
        assert_eq!(
            lines.iter().filter(|l| l.contains("let go")).count(),
            1,
            "coincident standing must not restate the act: {lines:?}"
        );
    }

    #[test]
    fn reference_folds_observation_into_the_deleted_phrase() {
        let mut gone = place("gone");
        let mut deleted = act("deleted", 1204, vec![(70, None)]);
        deleted.observed = true;
        gone.acts.push(deleted);
        let mut root = place("");
        root.children.push(gone);

        let lines = reference_place_lines(&report(root));
        assert_has_line(
            &lines,
            "1,204 files · deleted — a scan observed the loss   #70",
        );
        assert_no_line(&lines, "(scan-observed)");
    }

    #[test]
    fn reference_omits_the_moved_copied_split() {
        let build = || {
            let mut kept = place("kept");
            let mut archived = act("archived", 5, vec![(42, None)]);
            archived.moved = Some(2);
            archived.copied = Some(3);
            kept.acts.push(archived);
            let mut root = place("");
            root.children.push(kept);
            root
        };

        let judgment = story_lines(&report(build()), usize::MAX, 0);
        assert_has_line(&judgment, "(2 moved, 3 copied)");

        let lines = reference_place_lines(&report(build()));
        assert_no_line(&lines, "moved");
        assert_no_line(&lines, "copied");
    }

    #[test]
    fn reference_pluralizes_the_file_noun() {
        let mut lone = place("lone");
        lone.standing.unresolved = 1;
        lone.standing.missing_unexplained = 1;
        lone.standing.contentless = 1;
        let mut root = place("");
        root.children.push(lone);

        let lines = reference_place_lines(&report(root));
        assert_has_line(&lines, "1 file · no known copy in the archive");
        assert_has_line(&lines, "1 file · went missing, without a recorded reason");
        assert_has_line(&lines, "1 empty file");
        assert_no_line(&lines, "1 files");
        assert_no_line(&lines, "unresolved");
        assert_no_line(&lines, "(no content to cover)");
    }

    #[test]
    fn reference_keeps_the_never_hashed_honesty_in_plain_words() {
        let mut lone = place("lone");
        lone.standing.unresolved = 3;
        lone.standing.unhashed_unresolved = 2;
        let mut root = place("");
        root.children.push(lone);

        let lines = reference_place_lines(&report(root));
        assert_has_line(
            &lines,
            "3 files · no known copy in the archive (2 were never content-checked)",
        );
        assert_no_line(&lines, "never hashed");
    }

    #[test]
    fn reference_is_always_full_and_notes_stay_verbatim() {
        let mut root = place("");
        for name in ["a", "b", "c"] {
            let mut child = place(name);
            child.standing.unresolved = 1;
            child.notes.push(Note {
                id: 1,
                root_id: 1,
                rel_path: name.to_string(),
                text: format!("note at {name}"),
                created_at: 100,
            });
            root.children.push(child);
        }
        let lines = reference_place_lines(&report(root));
        for name in ["a", "b", "c"] {
            assert_has_line(&lines, &format!("  {name}"));
            assert_has_line(&lines, &format!("note: note at {name}"));
        }
        assert_no_line(&lines, "more places");
    }

    #[test]
    fn reference_bare_root_says_ever() {
        let lines = reference_place_lines(&report(place("")));
        assert_has_line(&lines, "nothing was ever indexed here");
        // The judgment wording is not a substring of the reference one.
        assert_no_line(&lines, "  nothing indexed here");
    }

    #[test]
    fn judgment_golden_fixture_is_line_identical() {
        // The byte-parity pin: the judgment voicing through the voicing
        // refactor, exact lines. If a wording site forgets its match arm,
        // this fails before any real archive does.
        let mut italy = place("pictures/italy");
        italy.standing.covered = 2;
        italy.covered_where = locations(&[("/archive/a", 2)]);

        let mut pictures = place("pictures");
        let mut archived = act("archived", 5, vec![(42, Some("the Italy trip"))]);
        archived.destination = locations(&[("/archive/media", 5)]);
        pictures.acts.push(archived);
        pictures.standing.contentless = 1;
        pictures.children.push(italy);

        let mut root = place("");
        root.children.push(pictures);

        let lines = story_lines(&report(root), usize::MAX, 0);
        let expected = vec![
            "Story: /r",
            "",
            "  role           source",
            "  first indexed  unknown",
            "  last scan      never",
            "",
            "The places",
            "",
            "  pictures",
            "    archived 5 files → /archive/media   #42 · \"the Italy trip\"",
            "    1 empty file (no content to cover)",
            "    → canon trail /r/pictures",
            "",
            "    pictures/italy",
            "      no decision here",
            "      2 covered — copies stand in /archive/a",
            "      → canon trail /r/pictures/italy",
            "",
            "Standing: 0 sources — 0 covered · 0 excluded · 0 unresolved",
            "Whether this story is complete is yours to judge.",
            "For the readiness gate: canon roots retire path:/r --dry-run",
        ];
        assert_eq!(lines, expected);
    }

    #[test]
    fn trail_handoff_round_trips_through_the_real_cli() {
        // The handoff-law discipline: CLI drift is a test failure.
        for path in ["/r/photos", "/r/with space/x"] {
            let (display, argv) = trail_handoff(path);
            assert!(display.starts_with("→ canon trail "), "{display}");
            crate::Cli::try_parse_from(&argv)
                .unwrap_or_else(|e| panic!("handoff must parse: {display}\n{e}"));
        }
        let (display, _) = trail_handoff("/r/with space/x");
        assert!(display.contains("'/r/with space/x'"), "{display}");
    }
}
