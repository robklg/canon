//! The bound telling's frame — the beginning, the entries guide, the tally,
//! the gaps paragraph, the last page around story's reference-voiced place
//! map. Diction, not frame: the ceremony lives in the structure, the
//! sentences go plain. Composed only from recorded facts; the sentences
//! records cannot produce (the root's human shape, the foreword) arrive via
//! the ceremony's edit pass or stay honestly absent.

use anyhow::{bail, Result};

use crate::domain::format::{format_count, format_date, format_size};
use crate::story::{
    fmt_locations, reference_place_lines, LocationAggregate, StoryParams, StoryReport,
};

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
    /// Receipts standing in the drive's own ledger (`.canon-ledger/`) when
    /// the telling composes: `Some(n)` counted from the same source the
    /// bind's gather reads; `None` when the root is unreachable (the
    /// compile records the gather gap). Drives the trace-chain paragraph's
    /// drive-local sentence — archive-side receipts are never in the book,
    /// and the paragraph says which ledger holds what.
    pub drive_ledger: Option<usize>,
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
    lines.push("- **returned to consideration** — a letting-go undone: the file was".into());
    lines.push("  brought back into consideration.".into());
    lines.push(String::new());
    lines.push("`#N` cites a decision. A reason is written out once, at its first".into());
    lines.push("appearance; later entries cite the bare number. `note:` lines were written".into());
    lines.push("at the folder, while the work was going on.".into());
    lines.push(String::new());
    // The which-ledger law: a source root's
    // ledger only ever holds deletion receipts — apply and exclusion
    // receipts live at the archive. The paragraph names where each kind
    // lives; it never claims the book's `ledger/` holds the receipts for
    // the decisions cited above.
    lines.push("Nothing here is a summary you have to take on faith. `timeline.md` lists".into());
    lines.push("every decision with its date and words, and `inventory.jsonl` names every".into());
    lines.push("single file and its fate. The receipts behind the archiving and letting-go".into());
    lines
        .push("decisions live in the archive's own ledger — the `.canon-ledger/` folder at".into());
    lines.push("the archive root, beside the content they concern.".into());
    match frame.drive_ledger {
        Some(n) if n > 0 => {
            lines.push(
                "`ledger/` here holds the receipts that lived on this drive itself: the".into(),
            );
            lines.push("record of what was lost here.".into());
        }
        Some(_) => lines.push("This drive kept no receipts of its own.".into()),
        None => {
            lines.push("The drive's own receipts could not be gathered — see the gaps in".into());
            lines.push("`README.md`.".into());
        }
    }
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
        // The registers overlap deliberately: a file copied to the archive
        // and later dismissed or lost here appears twice above — once as
        // chosen, once by the fate of the copy that stayed. Said exactly,
        // or the header reads as a partition it isn't. Every
        // archived-standing file is a copy (a moved file is absent), so the
        // overlap is copied minus still-standing; gated on a complete
        // moved/copied split — unrecorded rows would make the count a
        // guess.
        if account.archived_unrecorded == 0 {
            let overlap = (account.archived_copied - account.archived_standing).max(0);
            if overlap > 0 {
                lines.push(String::new());
                lines.push(if overlap == 1 {
                    "1 of the chosen files was copied, not moved — it appears again above by"
                        .into()
                } else {
                    format!(
                        "{} of the chosen files were copied, not moved — each appears again above by",
                        format_count(overlap)
                    )
                });
                lines.push(
                    "what became of the copy that stayed, so the lines sum past the total by"
                        .into(),
                );
                lines.push("exactly that much.".into());
            }
        }
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
    lines.push(if matches!(frame.drive_ledger, Some(n) if n > 0) {
        "live beside it — `inventory.jsonl`, `timeline.md`, `notes.md`, `ledger/` —".into()
    } else {
        "live beside it — `inventory.jsonl`, `timeline.md`, `notes.md` —".to_string()
    });
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
    account: &crate::core::domain::resolution::ResolutionAccount,
    archived_destinations: &LocationAggregate,
) -> Vec<String> {
    let mut cells: Vec<(String, Vec<String>)> = Vec::new();
    if account.archived_files > 0 {
        let mut cell = crate::story::file_noun(account.archived_files);
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
            crate::story::file_noun(account.covered),
            vec!["preserved by copies in the archive, archived from other places".into()],
        ));
    }
    if account.excluded > 0 {
        cells.push((
            crate::story::file_noun(account.excluded),
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
            crate::story::file_noun(account.deleted),
            vec!["deleted, as scans observed".into()],
        ));
    }
    if account.unexplained_missing > 0 {
        cells.push((
            crate::story::file_noun(account.unexplained_missing),
            vec!["went missing along the way, without a recorded reason".into()],
        ));
    }
    if account.unresolved > 0 {
        cells.push((
            crate::story::file_noun(account.unresolved),
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
fn gaps_paragraph(account: &crate::core::domain::resolution::ResolutionAccount) -> Vec<String> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::domain::resolution::ResolutionAccount;
    use crate::domain::root::Root;
    use crate::story::{ActDecision, ActGroup, LocationCount, PlaceStanding, StoryPlace};

    // `place`/`locations`/`act`/`report`/`report_with`/`zero_account` below
    // are deliberately forked with `story/ops/render.rs`'s copies, not
    // shared through a cross-subsystem test reach (the R5 inversion this
    // boundary exists to prevent) or a `core::testing` surface (would make
    // core depend on story's types, breaking the hub-never-depends-on-a-
    // spoke rule). Each builder constructs its domain type via an
    // exhaustive struct literal (no `..` spread), so a field added to the
    // type breaks both copies at compile time rather than letting them
    // silently drift apart in shape.
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

    #[allow(dead_code)]
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
            drive_ledger: Some(3),
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
        assert!(
            text.contains("- **returned to consideration** — a letting-go undone: the file was")
        );
        assert!(text.contains("3 files · let go   #57 · \"installer junk\""));
        assert!(text.contains("written by Canon v0.9.0"));
        assert!(text.contains("This is the one written at\nthe letting-go."));
    }

    #[test]
    fn the_trace_chain_names_the_archives_ledger() {
        // The which-ledger law: the paragraph says where each kind of
        // receipt lives — never that `ledger/` holds the cited decisions'
        // receipts (a source root's ledger only ever holds deletion
        // receipts).
        let text = compose_reference_telling(&report_with(place(""), full_account()), &frame());
        assert!(text.contains("The receipts behind the archiving and letting-go"));
        assert!(text.contains(
            "decisions live in the archive's own ledger — the `.canon-ledger/` folder at"
        ));
        assert!(
            text.contains("`ledger/` here holds the receipts that lived on this drive itself: the")
        );
        assert!(text.contains("record of what was lost here."));
        assert!(!text.contains("holds the original receipts"));
        assert!(text.contains("`inventory.jsonl`, `timeline.md`, `notes.md`, `ledger/` —"));
    }

    #[test]
    fn an_empty_drive_ledger_is_said_plainly() {
        let f = TellingFrame {
            drive_ledger: Some(0),
            ..frame()
        };
        let text = compose_reference_telling(&report_with(place(""), full_account()), &f);
        assert!(text.contains("This drive kept no receipts of its own."));
        assert!(!text.contains("`ledger/` here holds"));
        // The closing list of facts names only what stands in the book.
        assert!(text.contains("`inventory.jsonl`, `timeline.md`, `notes.md` —"));
        assert!(!text.contains("`notes.md`, `ledger/`"));
    }

    #[test]
    fn an_unreachable_drive_ledger_points_at_the_gaps() {
        let f = TellingFrame {
            drive_ledger: None,
            ..frame()
        };
        let text = compose_reference_telling(&report_with(place(""), full_account()), &f);
        assert!(text.contains("The drive's own receipts could not be gathered — see the gaps in"));
        assert!(!text.contains("`ledger/` here holds"));
        assert!(!text.contains("`notes.md`, `ledger/`"));
    }

    #[test]
    fn the_tally_admits_the_copied_overlap_exactly() {
        // 5 archived = 1 moved + 4 copied; 1 copy still stands here, so 3
        // copied-then-dismissed files appear twice above.
        let account = ResolutionAccount {
            archived_moved: 1,
            archived_copied: 4,
            archived_standing: 1,
            ..full_account()
        };
        let text = compose_reference_telling(&report_with(place(""), account), &frame());
        assert!(
            text.contains(
                "3 of the chosen files were copied, not moved — each appears again above by"
            ),
            "missing overlap sentence in:\n{text}"
        );
        assert!(text.contains("exactly that much."));
    }

    #[test]
    fn a_singular_overlap_reads_as_one_file() {
        let account = ResolutionAccount {
            archived_moved: 4,
            archived_copied: 1,
            archived_standing: 0,
            ..full_account()
        };
        let text = compose_reference_telling(&report_with(place(""), account), &frame());
        assert!(text
            .contains("1 of the chosen files was copied, not moved — it appears again above by"));
    }

    #[test]
    fn still_standing_copies_are_no_overlap() {
        // Every copy still stands here (archived_standing): each file is
        // counted once in the header's standing and once in the archived
        // cell — no double count, no sentence.
        let account = ResolutionAccount {
            archived_moved: 3,
            archived_copied: 2,
            archived_standing: 2,
            ..full_account()
        };
        let text = compose_reference_telling(&report_with(place(""), account), &frame());
        assert!(!text.contains("copied, not moved"));
    }

    #[test]
    fn an_unrecorded_split_omits_the_overlap_sentence() {
        // Pre-vocabulary receipts: the moved/copied split is incomplete —
        // the overlap would be a guess, so it is omitted, never guessed.
        let account = ResolutionAccount {
            archived_moved: 0,
            archived_copied: 3,
            archived_unrecorded: 2,
            archived_standing: 0,
            ..full_account()
        };
        let text = compose_reference_telling(&report_with(place(""), account), &frame());
        assert!(!text.contains("copied, not moved"));
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
}
