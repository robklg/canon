//! Archive's command surface: `canon cluster generate/refresh/status` and
//! `canon apply`. One declares an intended archive operation, the other
//! performs it, and they share almost nothing — so they stay separate files
//! rather than one module. What little they do share is here, where both can
//! reach it: text a user reads from either command about the same situation,
//! which must not be spelled twice.

pub(super) mod apply;
pub(super) mod cluster;

use std::path::Path;

use crate::archive::domain::LockStanding;
use crate::core::domain::format::shell_quote;

/// Whether a `cluster refresh` would run, as far as the caller telling the
/// user about one knows.
///
/// **Four states, because a turned-away refresh is two different situations
/// for the reader.** A remedy that sends the user straight at a command their
/// own manifest turns away is the same fault as naming a command that cannot
/// help — so the two commands that *have* resolved the recorded scope say
/// which, and the one that has not says so rather than asserting either.
///
/// Read that as *straight at*, not as *never mentions*: a refresh named
/// **after** the act that unblocks it is a step in a sequence, not a wrong
/// instruction, which is why the terminal rule's arm below can end in one.
///
/// The two refusals are kept apart because **only one of them states its own
/// way back**. A closed door is announced in the report with the act that
/// opens it (`canon roots unsuspend …`), so a remedy that falls silent there
/// leaves the user holding an act already. A recorded scope that kept nothing
/// is announced as `no sources known at <p> — skipped`, which names no act at
/// all — so falling silent there leaves the reader with nothing, and the
/// remedy owes the same pair `cluster refresh` itself prints when it refuses
/// for that very reason.
///
/// [`Unasked`](Self::Unasked) is `apply`'s. Apply carries out a lock already
/// built and checks that it still holds; it resolves no recorded scope, and a
/// manifest may record none. So at that end the question has no answer to
/// give, and this says that rather than assuming one. Its remedies speak about
/// the lock, which is what apply knows; a refresh named from there can still be
/// turned away, and the refresh states its own outcome and the acts that
/// answer it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RefreshStanding {
    /// It would run.
    Runs,
    /// It would be turned away by a closed door the recorded scope names.
    DoorClosed,
    /// It would be turned away by the source-existence policy's terminal
    /// rule: the recorded scope kept nothing.
    ScopeKeptNothing,
    /// This caller has not asked, and cannot.
    Unasked,
}

impl RefreshStanding {
    /// The acts to offer a reader whose sources are gone — empty where the way
    /// back is already on screen and repeating it would be noise.
    ///
    /// [`Unasked`](Self::Unasked) names the refresh: a remedy withheld on a
    /// question nobody asked would leave the user with no way forward at all,
    /// which is worse than naming one they may have to open a door for.
    fn way_back(self, manifest: &Path, set_aside: &[String]) -> Vec<String> {
        match self {
            RefreshStanding::Runs | RefreshStanding::Unasked => {
                vec![format!(
                    "refresh the manifest: {}",
                    refresh_command(manifest)
                )]
            }
            // The door line beside this one carries `canon roots unsuspend`.
            RefreshStanding::DoorClosed => Vec::new(),
            // The set-aside line beside this one carries no act, so this owes
            // one — the same pair the refresh prints when it refuses here, led
            // by the sentence that says which line they answer.
            RefreshStanding::ScopeKeptNothing => scope_kept_nothing_way_back(manifest, set_aside),
        }
    }

    /// Whether the lead runs on into the act, or closes as its own sentence.
    ///
    /// The refresh line completes the lead's *"if files are truly lost,"* and
    /// the two are one sentence across two lines. The scope edit is a
    /// different thought — the sources are a separate problem from a scope
    /// that resolves to nothing — so it stands on its own after a closed
    /// lead.
    fn runs_on_from_the_lead(self) -> bool {
        matches!(self, RefreshStanding::Runs | RefreshStanding::Unasked)
    }
}

/// A lock written before the measurement existed, in the words both readers
/// use for it: `apply`'s refusal and the report's `Lock:` line. One spelling,
/// because a report that stated the age in other words than the refusal that
/// follows it would read as two facts.
const LOCK_PREDATES_MEASUREMENT: &str =
    "built before lock files recorded the scopes they were built from";

/// The refresh, as a user would type it — the one spelling of the command
/// these remedies point at.
///
/// Trivial on its own, and worth owning anyway: the command's name was written
/// out by hand at every site that recommended it, which is how a rename
/// reaches nine sentences instead of one.
///
/// **The quoting belongs here, to the spelling, not to each sentence around
/// it** — a rule kept at the sentences is a rule some sentence will be written
/// without. A remedy exists to be typed, so a path the shell would not carry
/// verbatim yields a line that does not run as printed.
///
/// The quoting itself is **not this file's**: `core::domain::format::shell_quote`
/// owns it, and `ops/plan.rs`'s staleness remedy already runs manifest paths
/// through it for this very command. A spelling here that handled only spaces
/// would be a second, weaker answer to a question the tree has settled — and
/// weaker in a way that shows: it emits an unbalanced quote for a path carrying
/// an apostrophe, which is a directory name people really have.
fn refresh_command(manifest: &Path) -> String {
    format!(
        "canon cluster refresh {}",
        shell_quote(&manifest.display().to_string())
    )
}

/// The apply, as a user would type it — the sibling of [`refresh_command`],
/// and quoted by the same owner for the same reason.
///
/// It exists because the two invitations to apply drifted: `cluster generate`
/// quoted the manifest path it printed and `cluster status` did not, so one
/// command's suggestion ran and the other's did not, for the same manifest.
/// `resume` picks the form that finishes a run which already placed some of
/// its files. A parameter rather than a second function because the two differ
/// in one flag and must not differ in anything else — the flag's position
/// included, which is why it is composed here and not appended by the caller.
fn apply_command(manifest: &Path, resume: bool) -> String {
    let flag = if resume { "--resume " } else { "" };
    format!(
        "canon apply {flag}{}",
        shell_quote(&manifest.display().to_string())
    )
}

/// The way back from a lock that cannot be used as it stands: rebuild it.
///
/// One sentence for the three refusals that share one meaning — a lock whose
/// hash no longer matches, a lock written before the measurement, and a lock
/// that is not there — because they take the same act and differ only in what
/// the caller has already said about the cause.
///
/// **It takes no [`RefreshStanding`], deliberately.** All three callers are
/// `apply`, which is [`RefreshStanding::Unasked`] by construction, so the
/// parameter would have exactly one argument and the arm it never took would
/// return the empty string into the middle of a sentence — `Nothing was
/// moved. ` with nothing after it. A parameter whose live arm no caller
/// supplies is demand nobody has; the caller that needs one re-adds it with
/// itself as the evidence. `cluster status` composes no rebuild sentence
/// through here at all: its next step is the decision table's answer, which
/// asks the stronger question this one cannot.
fn rebuild_remedy(manifest: &Path) -> String {
    format!("Rebuild it: {}", refresh_command(manifest))
}

/// The way back from a manifest with no lock file beside it.
///
/// **Two absences, two remedies, told apart by the manifest's own record.** A
/// manifest still naming a lock hash lost a lock that existed, and a refresh
/// rebuilds it. A manifest recording the empty hash has no lock because its
/// last refresh matched nothing — a refresh there runs, matches nothing again
/// and returns the manifest unchanged, which is worse advice than none: it
/// does not even change the stated reason. What moves that manifest forward
/// is a different selection, so the remedy names the two fields that decide
/// one and puts the refresh after them.
///
/// The same fact `cluster status` reads to name the absence, read here to
/// name the way out of it — one record, two readers, neither guessing. Which
/// absence it is comes off [`LockStanding::absent`], the one spelling of that
/// question, rather than a second `is_empty()` at this end.
fn no_lock_remedy(manifest: &Path, recorded_lock_hash: &str) -> String {
    if LockStanding::absent(recorded_lock_hash).lost_a_lock_it_names() {
        rebuild_remedy(manifest)
    } else {
        format!(
            "The last refresh matched nothing, so there is no lock to carry across.\n\
             Edit the query or scope in the manifest, then {} to select again.",
            refresh_command(manifest)
        )
    }
}

/// The way back from a lock naming source files the world no longer has.
///
/// The lead is always said, because checking the volume is worth doing
/// whatever the manifest's own state is. What follows it is the standing's:
/// the refresh where one would run, the scope edit where the terminal rule
/// would turn one away, and nothing where a door is the reason — there, and
/// only there, the way back is already on screen with the door that closed
/// it.
///
/// The exact twin of this once stood in `apply`'s resume path and in `cluster
/// status`'s missing-source warning, one of them door-aware and one not.
fn source_lost_remedy(
    manifest: &Path,
    refresh: RefreshStanding,
    set_aside: &[String],
) -> Vec<String> {
    let way_back = refresh.way_back(manifest, set_aside);
    if way_back.is_empty() {
        return vec!["Check if the source volume is connected.".to_string()];
    }
    let lead = if refresh.runs_on_from_the_lead() {
        "Check if the source volume is connected. If files are truly lost,"
    } else {
        "Check if the source volume is connected."
    };
    let mut lines = vec![lead.to_string()];
    lines.extend(way_back);
    lines
}

/// `cluster status`'s whole answer for a recorded scope that kept nothing:
/// the sentence that binds the two acts to the line that caused them, then
/// the acts. The report reaches this state at two sites — its next step, and
/// the remedy for a lock naming sources the world has lost — and both read it
/// here, so the answer has one spelling.
///
/// **The lead is only where the two are apart.** `cluster refresh` prints
/// the two acts directly under its own `no sources known at …` refusal, where
/// a lead would say twice what the reader just read. `cluster status` prints
/// them at the foot of a report, several lines below the skipped scope line
/// and below a summary ending `All source files accounted for.` — which is
/// true of the sources the lock names and says nothing about the scope.
/// Without a lead the two acts read as advice about the files just counted,
/// which is the wrong subject entirely.
///
/// It names the consequence rather than repeating the cause: the skipped line
/// already said what happened, and what the reader does not know is that it
/// has cost them the rebuild.
fn scope_kept_nothing_way_back(manifest: &Path, set_aside: &[String]) -> Vec<String> {
    let mut lines = vec![
        "No sources are known under the scopes declared in this manifest, so the lock cannot be rebuilt:"
            .to_string(),
    ];
    lines.extend(scan_or_edit_scope(manifest, set_aside));
    lines
}

/// The way back from a recorded scope that kept nothing.
///
/// **Two answers, because Canon cannot tell which is the user's.** A set-aside
/// path sits inside a known root and holds no sources the index knows of, and
/// nothing on disk distinguishes *the files are there and were never scanned*
/// from *this is the wrong place*. Only the user knows which, so both are
/// offered and the scan comes first: it is the one that costs nothing to be
/// wrong about, and a scope naming a real folder Canon has not looked at is
/// the commoner of the two.
///
/// **The paths are named, not gestured at.** The command is there to be
/// pasted, and the reader has just been shown these paths on the skipped
/// line; a bare `canon scan` would make them retype what the report already
/// knows. Quoted by `refresh_command`'s own owner for the same reason.
///
/// Every command that meets this state says the same two lines: `cluster
/// refresh`'s own refusal, and `cluster status` both at its next step and in
/// the remedy for a lock naming sources the world has lost. `apply` does not,
/// because it never resolves a recorded scope — see [`RefreshStanding`].
///
/// Private, not `pub(super)`: a module's private items are already visible to
/// its descendants, so the two `cli/` files reach this and nothing in `ops`,
/// `domain` or `repo` can. An interface-layer formatter has no business being
/// reachable from a lower stratum, and the architecture test does not police
/// this file.
fn scan_or_edit_scope(manifest: &Path, set_aside: &[String]) -> Vec<String> {
    let edit = format!(
        "If not, edit meta.scope, then `{}` to rewrite the lock.",
        refresh_command(manifest)
    );
    // The terminal rule cannot fire on an empty set-aside list, so this arm is
    // unreachable from the two production callers. Kept rather than asserted
    // away: a remedy is the wrong place to panic, and the edit sentence alone
    // is still true.
    if set_aside.is_empty() {
        return vec![edit];
    }
    let paths: Vec<String> = set_aside.iter().map(|p| shell_quote(p)).collect();
    vec![
        format!(
            "If the files are there, `canon scan {}` to index them.",
            paths.join(" ")
        ),
        edit,
    ]
}

/// The way back from a pattern that cannot expand, where no rebuild would
/// change that.
///
/// **Two failures, two remedies, and neither is right for the other.** A
/// pattern that places files relative to the manifest's scope fails when there
/// is no scope to place them from, and nothing about facts helps: the scope
/// key is not a fact, so checking fact coverage sends the reader to look for
/// something that was never missing. A pattern naming a fact no source carries
/// fails for the opposite reason, and scoping the manifest does nothing for
/// it. Which one it is comes off the language facility's own reading of the
/// pattern, never off the wording of the refusal.
///
/// A pattern can fail both ways at once. The scope answer leads there, because
/// the scope must be settled whatever else is wrong, and each source's own
/// reason is printed above these lines either way.
///
/// Both commands say it because both meet the state: `apply` refuses on it,
/// and `cluster status` reaches it as a withheld step whose cause names no act
/// of its own. One composition, so the two cannot drift — which is what the
/// two invitations to apply did before [`apply_command`] existed.
fn adjust_pattern_remedy(needs_a_scope_it_lacks: bool) -> Vec<String> {
    if needs_a_scope_it_lacks {
        return vec![
            "The pattern places files relative to the scopes declared in the manifest, and this lock records none.".to_string(),
            "Use {source.rel_path} to keep each file's own folders, or add paths to meta.scope and refresh.".to_string(),
        ];
    }
    vec![
        "Adjust the pattern in your manifest, or import the facts it names.".to_string(),
        "Use 'canon facts' to see which sources carry them.".to_string(),
    ]
}

#[cfg(test)]
mod tests {
    /// The bypass this file exists to refuse: a remedy that spells a command
    /// and pastes a path into it on its own, rather than through the two
    /// spellings above.
    ///
    /// The composition pin next door checks the sentences it is handed, so a
    /// spelling nobody hands it is invisible to it. That is one rung too low
    /// for a remedy, which exists to be typed: a path the shell will not carry
    /// verbatim yields a line that does not run as printed, and the sentence
    /// most likely to carry one is the sentence nobody remembered.
    ///
    /// So the claim is structural instead: **every such spelling in archive's
    /// command surface is in this file**, and there are exactly three. A new
    /// one anywhere in `cli/` fails here, and the repair is to route it
    /// through [`refresh_command`], [`apply_command`], or
    /// [`scan_or_edit_scope`] — each of which quotes through the tree's own
    /// owner. Matched both directions, so a spelling that moves out of this
    /// file fails too, rather than leaving the list asserting a site that has
    /// gone.
    ///
    /// Deliberately narrow. It reads text, so it sees only a command name and
    /// an interpolation on one line; a spelling split across two lines, or
    /// built from a variable holding the verb, passes unseen. It also lets
    /// every `<manifest>`-style placeholder through, correctly — those paste
    /// nothing, so there is nothing to quote.
    ///
    /// Each file is read down to its `#[cfg(test)]` line and no further. What
    /// a test spells reaches no user, and the tests below assert on these very
    /// sentences — this one's own expected values included, so a scan reaching
    /// them would match itself.
    #[test]
    fn a_command_is_spelled_with_a_path_only_where_it_is_quoted() {
        let cli = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/archive/cli");
        let mut found: Vec<String> = Vec::new();
        let mut files = vec![cli.with_extension("rs")];
        for entry in std::fs::read_dir(&cli).expect("archive/cli is a directory") {
            files.push(entry.expect("readable entry").path());
        }
        files.sort();
        for file in &files {
            let text = std::fs::read_to_string(file).expect("readable source");
            let production = text.split("\n#[cfg(test)]").next().unwrap_or(&text);
            for line in production.lines() {
                let code = line.trim_start();
                if code.starts_with("//") || !code.contains('{') {
                    continue;
                }
                if ["canon apply", "canon cluster refresh", "canon scan"]
                    .iter()
                    .any(|verb| code.contains(verb))
                {
                    let name = file.file_name().expect("named file").to_string_lossy();
                    found.push(format!("{name}: {}", code.trim()));
                }
            }
        }
        assert_eq!(
            found,
            vec![
                r#"cli.rs: "canon cluster refresh {}","#.to_string(),
                r#"cli.rs: "canon apply {flag}{}","#.to_string(),
                r#"cli.rs: "If the files are there, `canon scan {}` to index them.","#.to_string(),
            ],
            "a command spelled with a path outside this file's three owners"
        );
    }
}
