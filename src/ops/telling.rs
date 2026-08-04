//! The story's telling — the composed reading over a `StoryReport`.
//!
//! The telling is an artifact composition, not terminal display (the
//! receipt-body precedent): ops composes the lines, consumers print or bind
//! them. Today's one consumer is `canon roots story` (the live, judgment
//! reading — present tense, trail handoffs, the open questions of a deciding
//! reader). The bound telling — the reference reading the retirement compile
//! writes into the book — arrives as its own voicing over this same
//! composition; structure is computed once here so the two can never drift.

use anyhow::{bail, Result};

use crate::domain::format::{format_count, format_date, format_size, format_time_ago, shell_quote};
use crate::domain::story::{ActGroup, LocationAggregate, StoryParams, StoryPlace};
use crate::ops::story::StoryReport;

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
fn file_noun(n: i64) -> String {
    if n == 1 {
        "1 file".to_string()
    } else {
        format!("{} files", format_count(n))
    }
}

// ---------------------------------------------------------------------------
// The bound telling's frame — the beginning, the entries guide, the tally,
// the gaps paragraph, the last page. Diction, not frame: the ceremony lives
// in the structure, the sentences go plain. Composed only from recorded
// facts; the sentences records cannot produce (the root's human shape, the
// foreword) arrive via the ceremony's edit pass or stay honestly absent.
// ---------------------------------------------------------------------------

/// What the frame needs beyond the report: the ceremony's own facts.
pub struct TellingFrame {
    /// Suggested title (see `suggested_title`); the edit pass may override.
    pub title: String,
    /// The user's `--reason` on the retirement, quoted in the beginning.
    pub retirement_reason: Option<String>,
    /// The day the book is written (unix seconds; rendered as a date).
    pub bound_on: i64,
    /// `CARGO_PKG_VERSION` — the closing stamp names its writer.
    pub canon_version: String,
    /// Where the chosen content lives now: the aggregate over the apply
    /// decisions' recorded destinations (empty → the clause is omitted,
    /// never guessed).
    pub archived_destinations: LocationAggregate,
}

/// The suggested book title: the place's own name, with its comment when
/// one was given (`mydisk — Old backup`). The edit pass owns any richer
/// prose.
pub fn suggested_title(root_path: &str, comment: Option<&str>) -> String {
    let name = root_path.rsplit('/').next().filter(|s| !s.is_empty());
    let name = name.unwrap_or(root_path);
    match comment {
        Some(c) if !c.trim().is_empty() => format!("{name} — {}", c.trim()),
        _ => name.to_string(),
    }
}

/// The placeholder the composed draft carries in its Foreword section. The
/// edit pass replaces it with the user's own words — or, left exactly as
/// it is, the whole section drops out of the bound page.
pub const FOREWORD_SENTINEL: &str = "[Write your foreword here — your own words about this \
place, signed as you wish. Leave this line as it is and the section will be left out.]";

/// The full placeholder section as composed — one derivation, shared by
/// the frame (which writes it) and the finalize step (which drops it
/// verbatim or keeps whatever the user made of it). Exact match only.
pub fn foreword_placeholder_section() -> String {
    format!(
        "## Foreword\n\n{FOREWORD_SENTINEL}\n\n\
         *Written at the retirement by the person who made the decisions in this\n\
         book. Their words, unedited.*\n"
    )
}

/// The telling as it goes to the compile: the finalized text and whether a
/// human hand refined it. The text binds verbatim — the dossier beside it
/// remains the machine truth (custody, not curation) — and the reading
/// settings travel with it, stamped into the book's meta.
pub struct TellingArtifact {
    pub text: String,
    /// True only when the finalized text differs from the finalized
    /// composed draft — an honesty claim about the words, not a keystroke.
    pub hand_edited: bool,
    /// The story params that shaped this reading.
    pub params: StoryParams,
}

/// Finalize a telling for binding: an untouched foreword placeholder
/// section drops out cleanly (exact match only — anything the user made of
/// it stays verbatim, never fuzzily repaired); an empty telling is refused
/// — absence of the artifact is not a story.
pub fn finalize_telling(text: &str) -> Result<String> {
    let section = foreword_placeholder_section();
    let with_break = format!("{section}\n");
    let finalized = if let Some(pos) = text.find(&with_break) {
        let mut out = String::with_capacity(text.len());
        out.push_str(&text[..pos]);
        out.push_str(&text[pos + with_break.len()..]);
        out
    } else if let Some(pos) = text.find(&section) {
        let mut out = String::with_capacity(text.len());
        out.push_str(&text[..pos]);
        out.push_str(&text[pos + section.len()..]);
        out
    } else {
        text.to_string()
    };
    if finalized.trim().is_empty() {
        bail!("The story is empty — a book must not bind an empty telling.");
    }
    Ok(finalized)
}

/// The complete reference-voiced telling — the text of the book's
/// `story.md` as composed. Every sentence derives from records; absence is
/// stated, never fictionalized.
pub fn compose_reference_telling(report: &StoryReport, frame: &TellingFrame) -> String {
    let mut lines: Vec<String> = Vec::new();
    let account = &report.account;

    // The beginning: what this is, dated.
    lines.push(format!("# {}", frame.title));
    lines.push(String::new());
    lines.push("This is the record of one place: what was on it, what was kept, what was".into());
    lines.push(format!(
        "let go, and why. It was written on {}, the day the place was retired.",
        format_date(frame.bound_on)
    ));
    lines.push("It is plain text and needs nothing but a reader.".into());
    lines.push(String::new());

    // What this place was — recorded identity only; the human shape (a
    // drive, a working copy, a server's disk) is the edit pass's to add.
    lines.push("## What this was".into());
    lines.push(String::new());
    let mut spans: Vec<String> = Vec::new();
    if let Some(ts) = report.first_indexed {
        spans.push(format!("first indexed {}", format_date(ts)));
    }
    if let Some(ts) = report.root.last_scanned_at {
        spans.push(format!("last scanned {}", format_date(ts)));
    }
    if spans.is_empty() {
        lines.push(format!("`{}`", report.root.path));
    } else {
        lines.push(format!("`{}` — {}.", report.root.path, spans.join(", ")));
    }
    match account.ever_indexed() {
        Some(0) => lines.push("It never held a file.".into()),
        Some(n) => lines.push(format!(
            "Over that time, {} files passed through it.",
            format_count(n)
        )),
        // The whole-life total is unsupported (records predate the
        // moved/copied vocabulary) — omitted, never guessed; the tally
        // below still states every line it can.
        None => {}
    }
    lines.push(String::new());
    match &frame.retirement_reason {
        Some(reason) => lines.push(format!("It was retired with these words: *\"{reason}\"*")),
        None => lines.push("No reason was recorded at the retirement.".into()),
    }
    lines.push(String::new());

    // The foreword placeholder — the one voice that isn't Canon's.
    lines.push(foreword_placeholder_section().trim_end().to_string());
    lines.push(String::new());

    // The key to the map, and the traceability claim — an honesty
    // statement, deliberately its own paragraph, not a formatting note.
    lines.push("## Reading the entries".into());
    lines.push(String::new());
    lines.push("Each entry below is a folder. Indentation means containment; \"across N".into());
    lines.push("folders\" means the entry speaks for that many folders beneath it. Within".into());
    lines.push("each one, files are grouped by what became of them:".into());
    lines.push(String::new());
    lines.push("- **chosen for the archive → path** — deliberately kept. The path is where".into());
    lines.push("  it lives now.".into());
    lines.push("- **let go** — dismissed after consideration. The quoted words are the".into());
    lines.push("  reason given at the moment of deciding.".into());
    lines.push("- **preserved by copies in the archive** — the same content, verified".into());
    lines.push("  byte-for-byte, was archived from somewhere else. Nothing was chosen".into());
    lines.push("  here; nothing was lost either.".into());
    lines.push("- **no known copy in the archive** — said plainly wherever it is true.".into());
    lines.push("- **empty file** — zero bytes: a name and a date, no content.".into());
    lines.push(String::new());
    lines.push("`#N` cites a decision. A reason is written out once, at its first".into());
    lines.push("appearance; later entries cite the bare number. `note:` lines were written".into());
    lines.push("at the folder, while the work was going on.".into());
    lines.push(String::new());
    lines.push("Nothing here is a summary you have to take on faith. `timeline.md` lists".into());
    lines.push("every decision with its date and words, `inventory.jsonl` names every".into());
    lines.push("single file and its fate, and `ledger/` holds the original receipts, filed".into());
    lines.push("by the same numbers.".into());
    lines.push(String::new());

    // The places — the map itself, always full.
    lines.push("## The places".into());
    lines.extend(reference_place_lines(report));
    lines.push(String::new());

    // The tally: every file ever held, told by where it went.
    lines.push("## Where everything went".into());
    lines.push(String::new());
    let tally = tally_lines(account, &frame.archived_destinations);
    if tally.is_empty() {
        lines.push("It never held a file — there is nothing to count.".into());
    } else {
        match account.ever_indexed() {
            Some(n) if n > 0 => lines.push(format!(
                "Over its time in the index, this place held {} files:",
                format_count(n)
            )),
            _ => lines.push("This is where its files went:".into()),
        }
        lines.push(String::new());
        lines.extend(tally);
    }
    let gaps = gaps_paragraph(account);
    if !gaps.is_empty() {
        lines.push(String::new());
        lines.extend(gaps);
    }
    lines.push(String::new());

    // The last page — a story has a last page, not a trailing map.
    lines.push("## The last page".into());
    lines.push(String::new());
    lines.push(format!(
        "On {} this place was retired. With this book written, the place itself",
        format_date(frame.bound_on)
    ));
    lines.push("was free to go. The archive carries what was chosen. This book carries".into());
    lines.push("the rest.".into());
    lines.push(String::new());
    lines.push("---".into());
    lines.push(String::new());
    lines.push(format!(
        "This is one telling of the record, written by Canon v{} at the",
        frame.canon_version
    ));
    lines.push("retirement, with the reading settings of its day. The facts beneath it".into());
    lines.push("live beside it — `inventory.jsonl`, `timeline.md`, `notes.md`, `ledger/` —".into());
    lines.push("and another telling could be drawn from them. This is the one written at".into());
    lines.push("the letting-go.".into());

    let mut text = lines.join("\n");
    text.push('\n');
    text
}

/// The tally's aligned lines: a left cell (count, bytes where supported)
/// and a plain-words fate, one line per non-zero bucket — zero buckets are
/// omitted, never ceremonially stated.
fn tally_lines(
    account: &crate::domain::retire::ResolutionAccount,
    archived_destinations: &LocationAggregate,
) -> Vec<String> {
    let mut cells: Vec<(String, Vec<String>)> = Vec::new();
    if account.archived_files > 0 {
        let mut cell = file_noun(account.archived_files);
        if let Some(bytes) = account.archived_bytes {
            if bytes > 0 {
                cell.push_str(&format!(", {}", format_size(bytes)));
            }
        }
        let mut desc = vec!["chosen for the archive".to_string()];
        if !archived_destinations.is_empty() {
            desc[0].push_str(" — they live on under");
            desc.push(fmt_locations(archived_destinations));
        }
        cells.push((cell, desc));
    }
    if account.covered > 0 {
        cells.push((
            file_noun(account.covered),
            vec!["preserved by copies in the archive, archived from other places".into()],
        ));
    }
    if account.excluded > 0 {
        cells.push((
            file_noun(account.excluded),
            vec!["let go, by the decisions told above".into()],
        ));
    }
    if account.contentless > 0 {
        let noun = if account.contentless == 1 {
            "1 empty file".to_string()
        } else {
            format!("{} empty files", format_count(account.contentless))
        };
        cells.push((noun, vec!["names and dates without content".into()]));
    }
    if account.deleted > 0 {
        cells.push((
            file_noun(account.deleted),
            vec!["deleted, as scans observed".into()],
        ));
    }
    if account.unexplained_missing > 0 {
        cells.push((
            file_noun(account.unexplained_missing),
            vec!["went missing along the way, without a recorded reason".into()],
        ));
    }
    if account.unresolved > 0 {
        cells.push((
            file_noun(account.unresolved),
            vec!["no known copy in the archive — released knowingly".into()],
        ));
    }

    let width = cells
        .iter()
        .map(|(c, _)| c.chars().count())
        .max()
        .unwrap_or(0);
    let mut lines = Vec::new();
    for (cell, desc) in &cells {
        let pad = " ".repeat(width - cell.chars().count());
        lines.push(format!("  {cell}{pad}    {}", desc[0]));
        for cont in &desc[1..] {
            lines.push(format!("  {}    {cont}", " ".repeat(width)));
        }
    }
    lines
}

/// The gaps stated as prose, immediately under the tally: left open on
/// purpose — seen, weighed, and accepted. Zero gaps → no paragraph.
fn gaps_paragraph(account: &crate::domain::retire::ResolutionAccount) -> Vec<String> {
    let missing = account.unexplained_missing;
    let unresolved = account.unresolved;
    if missing == 0 && unresolved == 0 {
        return Vec::new();
    }
    let mut lines = Vec::new();
    if missing > 0 && unresolved > 0 {
        lines.push("The last two lines are gaps. They are printed here, unrounded and".into());
        lines.push("folded into no larger number, because they were left open on purpose:".into());
        lines.push("seen, weighed, and accepted as the price of finishing.".into());
    } else {
        lines.push("The last line is a gap. It is printed here, unrounded and folded into".into());
        lines.push("no larger number, because it was left open on purpose: seen, weighed,".into());
        lines.push("and accepted as the price of finishing.".into());
    }
    if missing > 0 {
        lines.push(format!(
            "What became of the {} is not known.",
            format_count(missing)
        ));
    }
    if unresolved > 0 {
        let verb = if unresolved == 1 { "was" } else { "were" };
        lines.push(format!(
            "The {} {verb} let go in the knowledge that no verified copy exists",
            format_count(unresolved)
        ));
        lines.push("anywhere in the archive.".into());
    }
    lines
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
        if place.undecided() {
            // Exclusion is always a deliberate act; at a place with no act
            // slices, the excluded standing evidences a decision whose
            // record is absent (pre-provenance, or recording off) — state
            // the gap, never "no decision here".
            line.push_str(match voicing {
                Voicing::Judgment => " (no recorded decision)",
                Voicing::Reference => " — no record of the decision survives",
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
fn fmt_locations(agg: &LocationAggregate) -> String {
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
    use super::*;
    use crate::domain::note::Note;
    use crate::domain::retire::ResolutionAccount;
    use crate::domain::root::Root;
    use crate::domain::story::{
        ActDecision, ActGroup, LocationAggregate, LocationCount, PlaceStanding, StoryPlace,
    };
    use crate::ops::story::StoryReport;

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

    // -----------------------------------------------------------------
    // The frame — the bound telling's beginning, guide, tally, gaps, and
    // last page around the reference map.
    // -----------------------------------------------------------------

    fn full_account() -> ResolutionAccount {
        ResolutionAccount {
            archived_files: 5,
            archived_bytes: Some(500),
            archived_moved: 5,
            archived_copied: 0,
            archived_unrecorded: 0,
            deleted: 2,
            unexplained_missing: 4,
            archived_standing: 0,
            covered: 2,
            excluded: 3,
            contentless: 1,
            unresolved: 1,
            unhashed_unresolved: 0,
        }
    }

    fn frame() -> TellingFrame {
        TellingFrame {
            title: "r — Old backup".to_string(),
            retirement_reason: Some("the story is complete".to_string()),
            bound_on: 0,
            canon_version: "0.9.0".to_string(),
            archived_destinations: locations(&[("/archive/media", 5)]),
        }
    }

    fn section_positions(text: &str, sections: &[&str]) -> Vec<usize> {
        sections
            .iter()
            .map(|s| {
                text.find(s)
                    .unwrap_or_else(|| panic!("missing section {s:?} in:\n{text}"))
            })
            .collect()
    }

    #[test]
    fn the_frame_composes_every_section_in_order() {
        let mut old = place("old");
        old.acts
            .push(act("excluded", 3, vec![(57, Some("installer junk"))]));
        let mut root = place("");
        root.children.push(old);

        let text = compose_reference_telling(&report_with(root, full_account()), &frame());
        let positions = section_positions(
            &text,
            &[
                "# r — Old backup",
                "## What this was",
                "## Foreword",
                "## Reading the entries",
                "## The places",
                "## Where everything went",
                "## The last page",
                "\n---\n",
            ],
        );
        assert!(
            positions.windows(2).all(|w| w[0] < w[1]),
            "sections out of order: {positions:?}\n{text}"
        );
        assert!(text.contains("This is the record of one place"));
        assert!(text.contains("It was written on 1970-01-01, the day the place was retired."));
        assert!(text.contains("It was retired with these words: *\"the story is complete\"*"));
        assert!(text.contains(FOREWORD_SENTINEL));
        assert!(text.contains("Nothing here is a summary you have to take on faith."));
        assert!(text.contains("3 files · let go   #57 · \"installer junk\""));
        assert!(text.contains("written by Canon v0.9.0"));
        assert!(text.contains("This is the one written at\nthe letting-go."));
    }

    #[test]
    fn the_tally_reconciles_with_the_account() {
        let text = compose_reference_telling(&report_with(place(""), full_account()), &frame());
        // ever_indexed = standing 7 + deleted 2 + unexplained 4 + moved 5.
        assert!(text.contains("Over its time in the index, this place held 18 files:"));
        assert!(text.contains("5 files, 500 B    chosen for the archive — they live on under"));
        assert!(text.contains("/archive/media"));
        assert!(
            text.contains("2 files           preserved by copies in the archive, archived from")
        );
        assert!(text.contains("3 files           let go, by the decisions told above"));
        assert!(text.contains("1 empty file      names and dates without content"));
        assert!(text.contains("2 files           deleted, as scans observed"));
        assert!(text
            .contains("4 files           went missing along the way, without a recorded reason"));
        assert!(
            text.contains("1 file            no known copy in the archive — released knowingly")
        );
    }

    #[test]
    fn zero_buckets_are_omitted_from_the_tally() {
        let mut account = zero_account();
        account.covered = 2;
        let text = compose_reference_telling(&report_with(place(""), account), &frame());
        assert!(text.contains("preserved by copies"));
        assert!(!text.contains("chosen for the archive —"));
        assert!(!text.contains("deleted, as scans observed"));
        assert!(!text.contains("gap"), "no gaps paragraph without gaps");
    }

    #[test]
    fn gaps_get_their_own_paragraph_only_when_real() {
        let mut both = zero_account();
        both.unexplained_missing = 4;
        both.unresolved = 1;
        let text = compose_reference_telling(&report_with(place(""), both), &frame());
        assert!(text.contains("The last two lines are gaps."));
        assert!(text.contains("What became of the 4 is not known."));
        assert!(text.contains("The 1 was let go in the knowledge that no verified copy exists"));

        let mut one = zero_account();
        one.unexplained_missing = 4;
        let text = compose_reference_telling(&report_with(place(""), one), &frame());
        assert!(text.contains("The last line is a gap."));
        assert!(text.contains("What became of the 4 is not known."));
        assert!(!text.contains("no verified copy"));
    }

    #[test]
    fn no_reason_is_stated_plainly() {
        let mut f = frame();
        f.retirement_reason = None;
        let text = compose_reference_telling(&report(place("")), &f);
        assert!(text.contains("No reason was recorded at the retirement."));
        assert!(!text.contains("retired with these words"));
    }

    #[test]
    fn an_unsupported_whole_life_total_is_omitted_never_guessed() {
        let mut account = full_account();
        account.archived_unrecorded = 1;
        let text = compose_reference_telling(&report_with(place(""), account), &frame());
        assert!(text.contains("This is where its files went:"));
        assert!(!text.contains("passed through it"));
        assert!(!text.contains("this place held"));
    }

    #[test]
    fn an_empty_root_still_gets_the_full_frame() {
        let text = compose_reference_telling(&report(place("")), &frame());
        assert!(text.contains("It never held a file."));
        assert!(text.contains("nothing was ever indexed here"));
        assert!(text.contains("## Reading the entries"));
        assert!(text.contains("## The last page"));
        // No scan dates recorded: the identity line is the bare path.
        assert!(text.contains("`/r`\n"));
        assert!(!text.contains("first indexed"));
    }

    #[test]
    fn the_suggested_title_is_name_and_comment() {
        assert_eq!(
            suggested_title("/volumes/x/mydisk", Some("Old backup")),
            "mydisk — Old backup"
        );
        assert_eq!(suggested_title("/volumes/x/mydisk", None), "mydisk");
        assert_eq!(suggested_title("/volumes/x/mydisk", Some("  ")), "mydisk");
    }

    // -----------------------------------------------------------------
    // Finalize — the exact-match sentinel discipline.
    // -----------------------------------------------------------------

    #[test]
    fn finalize_drops_the_untouched_foreword_section() {
        let text = compose_reference_telling(&report(place("")), &frame());
        let finalized = finalize_telling(&text).unwrap();
        assert!(!finalized.contains("## Foreword"));
        assert!(!finalized.contains(FOREWORD_SENTINEL));
        assert!(
            !finalized.contains("\n\n\n"),
            "no blank-line run left behind:\n{finalized}"
        );
        assert!(finalized.contains("## Reading the entries"));
    }

    #[test]
    fn finalize_keeps_an_edited_foreword_verbatim() {
        let text = compose_reference_telling(&report(place("")), &frame())
            .replace(FOREWORD_SENTINEL, "> my own words about this place\n\n— me");
        let finalized = finalize_telling(&text).unwrap();
        assert!(finalized.contains("## Foreword"));
        assert!(finalized.contains("> my own words about this place"));
        assert!(finalized.contains("*Written at the retirement"));
    }

    #[test]
    fn finalize_never_fuzzily_repairs_a_touched_sentinel() {
        // One character changed inside the placeholder: exact match fails,
        // the section stays exactly as the user left it.
        let text = compose_reference_telling(&report(place("")), &frame())
            .replace("Write your foreword", "write your foreword");
        let finalized = finalize_telling(&text).unwrap();
        assert!(finalized.contains("## Foreword"));
        assert!(finalized.contains("write your foreword"));
    }

    #[test]
    fn finalize_refuses_an_empty_telling() {
        assert!(finalize_telling("").is_err());
        assert!(finalize_telling("  \n\n \n").is_err());
        // A telling that is nothing but the untouched placeholder is empty
        // once the section drops.
        assert!(finalize_telling(&foreword_placeholder_section()).is_err());
    }

    #[test]
    fn finalize_without_a_foreword_section_is_identity() {
        let text = "# a place\n\nits story\n";
        assert_eq!(finalize_telling(text).unwrap(), text);
    }

    #[test]
    fn multiple_destinations_render_with_counts() {
        let mut f = frame();
        f.archived_destinations = locations(&[("/archive/a", 3), ("/archive/b", 2)]);
        let text = compose_reference_telling(&report_with(place(""), full_account()), &f);
        assert!(text.contains("/archive/a (3), /archive/b (2)"));
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
}
