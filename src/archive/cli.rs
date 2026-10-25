//! Archive's command surface: `canon cluster generate/refresh/status` and
//! `canon apply`. One declares an intended archive operation, the other
//! performs it, and they share almost nothing — so they stay separate files
//! rather than one module. What little they do share is here, where both can
//! reach it: text a user reads from either command about the same situation,
//! which must not be spelled twice.

pub(super) mod apply;
pub(super) mod cluster;

use std::path::Path;

use crate::archive::domain::ZoneRefusal;
use crate::archive::ops::pattern::PatternGaps;
use crate::core::domain::format::{shell_quote, with_visible_nulls};
use crate::core::domain::format_count;
use crate::core::domain::path::NotContained;
use crate::core::domain::time::zone::{RecordedZone, Unrecordable, UserZone, ZoneNameError};
use crate::expr::{ExpressionRefused, NoPlace, NoPlaceCause};

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
/// opens it (`canon roots unsuspend …`), and a zone or query line the refresh
/// would refuse in the refresh's own words, way back included — so a remedy
/// that falls silent there leaves the user holding an act already. A recorded
/// scope that kept nothing is announced as `no sources known at <p> — skipped`,
/// which names no act at all — so falling silent there leaves the reader with
/// nothing, and the remedy owes the same pair `cluster refresh` itself prints
/// when it refuses for that very reason.
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
    /// It would be turned away by a cause the report states with its way back:
    /// a closed door the recorded scope names, the manifest's zone, or a query
    /// that cannot run.
    WayBackStated,
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
    fn way_back(
        self,
        manifest: &Path,
        set_aside: &[String],
        query: Option<&ExpressionRefused>,
    ) -> Vec<String> {
        match self {
            RefreshStanding::Runs | RefreshStanding::Unasked => {
                vec![format!(
                    "refresh the manifest: {}",
                    refresh_command(manifest, false)
                )]
            }
            // The line beside this one carries the way back: `canon roots unsuspend`
            // for a door, the refresh's own words for the zone or a query line.
            RefreshStanding::WayBackStated => Vec::new(),
            // The set-aside line beside this one carries no act, so this owes
            // one — the same pair the refresh prints when it refuses here, led
            // by the sentence that says which line they answer.
            RefreshStanding::ScopeKeptNothing => {
                scope_kept_nothing_way_back(manifest, set_aside, query)
            }
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

/// A lock written before lock files recorded the time zone, in the words `apply`'s refusal
/// and the report's `Lock:` line share — the sibling of [`LOCK_PREDATES_MEASUREMENT`].
const LOCK_PREDATES_ZONE: &str = "built before lock files recorded the time zone";

/// The lock's zone as `apply` names it: the clock its placements read on.
fn lock_zone_line(zone: &RecordedZone) -> String {
    format!("Time zone: {} (from the lock)", zone.name())
}

/// A lock naming a zone this machine's zone data does not know: the cause, what the caller
/// says about it (`apply`: nothing was moved), and the three ways out — `apply`'s refusal
/// and `cluster status`'s line in one spelling. The third way names *the same zone*,
/// because any other known name would move where files go without the user meaning it to.
fn lock_zone_not_known_here(name: &str, manifest: &Path, outcome: &str) -> Vec<String> {
    let mut lead = format!(
        "This machine's time zone data does not know {name}, the zone the lock was built in."
    );
    if !outcome.is_empty() {
        lead.push(' ');
        lead.push_str(outcome);
    }
    vec![
        lead,
        "  update this machine's time zone data, or".to_string(),
        "  apply on the machine that built the lock, or".to_string(),
        format!(
            "  change the manifest's zone line to a name this machine knows for the same zone \
             (for example, an older spelling), then {}",
            refresh_command(manifest, false)
        ),
    ]
}

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
///
/// `edit` picks the form that opens the manifest in the editor before the
/// refresh reads it — the way back from a line that does not read. A parameter
/// for the reason [`apply_command`] gives for its own flag.
fn refresh_command(manifest: &Path, edit: bool) -> String {
    let flag = if edit { "--edit " } else { "" };
    format!(
        "canon cluster refresh {flag}{}",
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

/// The report, as a user would type it — the sibling of [`refresh_command`],
/// quoted by the same owner for the same reason. Named where a refusal has no
/// way back of its own to give, and the report is where the cause is stated.
fn status_command(manifest: &Path) -> String {
    format!(
        "canon cluster status {}",
        shell_quote(&manifest.display().to_string())
    )
}

/// `apply`'s refusal where no lock file stands beside the manifest — one sentence
/// for every cause of absence alike.
///
/// Apply carries out a lock already built; with none it has nothing to carry
/// out, and it infers nothing about why. Which absence it is — an empty
/// selection or a lock that has gone — is `cluster status`'s to state, so the
/// refusal names the report and no remedy of its own.
fn missing_lock_refusal(manifest: &Path) -> String {
    format!(
        "This manifest is missing its lock file. Nothing was moved. Run `{}` to inspect.",
        status_command(manifest)
    )
}

/// The way back from a lock that cannot be used as it stands: rebuild it.
///
/// One sentence for the three refusals that share one meaning — a lock whose
/// hash no longer matches, a lock with no measurement recorded, and a lock
/// with no time zone recorded — because they take the same act and differ
/// only in what the caller has already said about the cause.
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
    format!("Rebuild it: {}", refresh_command(manifest, false))
}

/// The way forward from a manifest whose last refresh selected no sources, as
/// `cluster status` names it.
///
/// A refresh over the same query and scope selects nothing again and returns
/// the manifest unchanged — a fixed point, not a remedy — so this names the two
/// fields that decide a selection and puts the refresh after them. Which
/// absence a lock is, and so whether this is owed, is the status table's
/// answer (`OwedWayBack::ChangeTheSelection`); a lost lock is rebuilt instead.
fn empty_selection_remedy(manifest: &Path) -> String {
    format!(
        "The last refresh selected no sources. Update the query or scope in the manifest, then \
         run `{}`.",
        refresh_command(manifest, false)
    )
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
    query: Option<&ExpressionRefused>,
) -> Vec<String> {
    let way_back = refresh.way_back(manifest, set_aside, query);
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
///
/// **A recorded query the refresh would also refuse takes its way back last,
/// here**, as at `cluster refresh`'s own exit for this cause: the scope's two
/// answers, then the query's, so the lines read as steps and the refresh the
/// query's line names comes after the step that lets it run. The report's
/// query block leaves its way back to this answer where the scope is what
/// turns the refresh away first; `query` is that refusal, and `None` anywhere
/// else.
fn scope_kept_nothing_way_back(
    manifest: &Path,
    set_aside: &[String],
    query: Option<&ExpressionRefused>,
) -> Vec<String> {
    let mut lines = vec![
        "No sources are known under the scopes declared in this manifest, so the lock cannot be rebuilt:"
            .to_string(),
    ];
    lines.extend(scan_or_edit_scope(manifest, set_aside));
    lines.extend(query.map(|refusal| query_way_back(refusal, manifest)));
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
        refresh_command(manifest, false)
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

/// Why this machine's zone cannot be written into a manifest, as the first clause of
/// either command's refusal: the `TZ` value Canon could not read where one was set, the
/// rule where `TZ` held one, and neither where neither is the cause.
fn machine_zone_cause(unrecordable: &Unrecordable) -> String {
    match unrecordable {
        Unrecordable::Unknown {
            unreadable_tz: None,
        } => "cannot tell this machine's time zone".to_string(),
        Unrecordable::Unknown {
            unreadable_tz: Some(tz),
        } => format!("TZ={tz} is not a time zone Canon can read"),
        Unrecordable::RuleString(rule) => {
            format!("TZ={rule} is a time zone rule with no name to record")
        }
        Unrecordable::UnnamedRules => {
            "this machine's time zone has rules but no name to record".to_string()
        }
    }
}

/// `cluster generate`'s refusal where this machine's zone cannot be recorded: the cause,
/// then the one way out — a run with `TZ` naming a zone.
fn generate_machine_zone_refusal(unrecordable: &Unrecordable) -> String {
    let set = match unrecordable {
        Unrecordable::Unknown {
            unreadable_tz: None,
        } => "TZ set",
        Unrecordable::Unknown { .. } | Unrecordable::RuleString(_) | Unrecordable::UnnamedRules => {
            "TZ set to a zone name"
        }
    };
    format!(
        "{}; run it with {set}, for example TZ=Europe/Amsterdam canon cluster generate …",
        machine_zone_cause(unrecordable)
    )
}

/// What `cluster generate` says about the zone it recorded.
fn zone_recorded_statement(zone: &RecordedZone) -> String {
    format!(
        "Time zone: {}, recorded in the manifest's zone line.",
        zone.name()
    )
}

/// Why a manifest's zone cannot be settled, in the words `cluster refresh` refuses with and
/// `cluster status` names as what stops the refresh. A name this machine does not know is
/// never called a typo: it may be newer than this machine's zone data.
fn zone_refusal(refusal: &ZoneRefusal) -> String {
    match refusal {
        ZoneRefusal::Line(ZoneNameError::NotKnownHere(name)) => format!(
            "this machine's time zone data does not know \"{name}\", the zone line in the \
             manifest — correct the zone line, or update this machine's time zone data"
        ),
        ZoneRefusal::Line(ZoneNameError::RuleString(_)) => {
            "the zone line is a time zone rule, not a zone name — write a zone name, for \
             example zone = \"Europe/Amsterdam\""
                .to_string()
        }
        ZoneRefusal::Line(ZoneNameError::Empty) => {
            "the zone line names no zone — write a zone name, or delete the line to take this \
             machine's"
                .to_string()
        }
        ZoneRefusal::Machine(unrecordable) => format!(
            "{}; write a zone line (for example zone = \"Europe/Amsterdam\") or set TZ",
            machine_zone_cause(unrecordable)
        ),
    }
}

/// The way back from sources the pattern gives no place, where no rebuild would change that.
///
/// **One line per cause the run met, each worded for that cause, and none is right for
/// another.** A pattern that places files relative to the manifest's scope fails where there is
/// no scope to place them from, and nothing about facts helps: the scope key is not a fact. A
/// stored fact a source does not carry is supplied by importing it; a value Canon works out and
/// a value that cannot be a name are not. A save time Canon cannot read is neither: the file has
/// one. A time too coarse for its modifier is met by a coarser modifier, which needs no refresh.
/// A destination that would leave its directory is met by an edit to the pattern alone. Which
/// ones the run met comes off the facility's typed causes (`ops::pattern::PatternGaps`), matched
/// with no catch-all arm, never off the wording of a sentence.
///
/// The scope answer stands alone where it is met, because the scope must be settled whatever
/// else is wrong. Each query entry is the cause's own (`NoPlaceCause::query_entry`), and every
/// line naming a refresh names it as a command, for this manifest.
///
/// Both commands say it because both meet the state: `apply` refuses on it, and `cluster
/// status` reaches it as a withheld step whose cause names no act of its own. One composition,
/// so the two cannot drift.
fn adjust_pattern_remedy(gaps: &PatternGaps, manifest: &Path) -> Vec<String> {
    let refresh = refresh_command(manifest, false);
    let scope = format!(
        "Use {{source.rel_path}} to keep each file's own directories, or add paths to meta.scope \
         and run `{refresh}`."
    );
    if gaps.needs_a_scope_it_lacks {
        return vec![scope];
    }
    let mut stored = Vec::new();
    let mut absent = Vec::new();
    let mut coarse = Vec::new();
    let (mut save_time, mut scoped, mut shape) = (false, false, false);
    let add = |entries: &mut Vec<String>, cause: &NoPlaceCause| {
        if let Some(entry) = cause.query_entry() {
            let entry = toml::Value::String(entry).to_string();
            if !entries.contains(&entry) {
                entries.push(entry);
            }
        }
    };
    for (cause, _) in &gaps.met {
        match cause {
            NoPlaceCause::NoFact { .. } => add(&mut stored, cause),
            NoPlaceCause::NoValue { .. } => add(&mut absent, cause),
            NoPlaceCause::TooCoarse { .. } => add(&mut coarse, cause),
            NoPlaceCause::UnreadableSaveTime => save_time = true,
            NoPlaceCause::NoScopeRecorded | NoPlaceCause::LockPredatesMeasurement => scoped = true,
            NoPlaceCause::ContainsSlash { .. }
            | NoPlaceCause::Empty { .. }
            | NoPlaceCause::DotName { .. }
            | NoPlaceCause::NoSegments { .. }
            | NoPlaceCause::NullChar { .. } => shape = true,
        }
    }
    let mut lines = Vec::new();
    if !stored.is_empty() {
        lines.push(format!(
            "Adjust the pattern in your manifest, or import the missing facts; or add {} to the \
             manifest's query to leave such sources out, then run `{refresh}`.",
            stored.join(" and ")
        ));
        lines.push("`canon facts` shows which sources have these facts".to_string());
    }
    if !absent.is_empty() {
        lines.push(format!(
            "Adjust the pattern in your manifest; or add {} to the manifest's query to leave such \
             sources out, then run `{refresh}`.",
            absent.join(" and ")
        ));
    }
    if save_time {
        lines.push(format!(
            "Add \"source.mtime?\" to the manifest's query to leave such sources out, or correct \
             the file's save time and scan again; then run `{refresh}`."
        ));
    }
    if scoped {
        lines.push(scope);
    }
    if !coarse.is_empty() {
        lines.push(format!(
            "Use a coarser modifier in the pattern, which takes effect at the next apply; or add \
             {} to the manifest's query to leave such sources out, then run `{refresh}`.",
            coarse.join(" and ")
        ));
    }
    if shape {
        lines.push(format!(
            "Adjust the pattern in your manifest; or leave such sources out with the manifest's \
             query, then run `{refresh}`."
        ));
    }
    if !gaps.not_contained.is_empty() {
        lines.push(ADJUST_THE_PATTERN.to_string());
    }
    lines
}

/// The way back from a destination that would leave its directory.
const ADJUST_THE_PATTERN: &str = "Adjust the pattern in your manifest.";

/// `apply`'s block for the sources the pattern gives no place: how many, each source with every
/// cause it met, then the way back for the causes met.
fn no_place_lines(
    failures: &[(String, NoPlace)],
    gaps: &PatternGaps,
    manifest: &Path,
) -> Vec<String> {
    let sources = if failures.len() == 1 {
        "1 source".to_string()
    } else {
        format!("{} sources", format_count(failures.len()))
    };
    let mut lines = vec![format!("Error: {sources} failed pattern expansion:")];
    for (path, no_place) in failures.iter().take(10) {
        let causes: Vec<String> = no_place.causes.iter().map(NoPlaceCause::sentence).collect();
        lines.push(format!("  {path}: {}", causes.join("; ")));
    }
    if failures.len() > 10 {
        lines.push(format!(
            "  ... and {} more",
            format_count(failures.len() - 10)
        ));
    }
    // The way back for destinations that would leave their directory follows their own block.
    let met = PatternGaps {
        not_contained: Vec::new(),
        ..gaps.clone()
    };
    let remedy = adjust_pattern_remedy(&met, manifest);
    if !remedy.is_empty() {
        lines.push(String::new());
    }
    lines.extend(remedy);
    lines
}

/// `apply`'s preflight block for destinations that would leave their directory: one heading per
/// way they would, each destination listed whole (a null character as `\0`), then the way back.
fn not_contained_lines(found: &[(String, String, NotContained)]) -> Vec<String> {
    let mut lines = Vec::new();
    let met: std::collections::BTreeSet<NotContained> =
        found.iter().map(|(_, _, why)| *why).collect();
    for why in met {
        let these: Vec<&(String, String, NotContained)> =
            found.iter().filter(|(_, _, w)| *w == why).collect();
        let one = these.len() == 1;
        let count = if one {
            "1 destination path".to_string()
        } else {
            format!("{} destination paths", format_count(these.len()))
        };
        let what = match (why, one) {
            (NotContained::ParentDirectory, true) => "contains a `..` directory name",
            (NotContained::ParentDirectory, false) => "contain a `..` directory name",
            (NotContained::NullChar, true) => "contains a null character",
            (NotContained::NullChar, false) => "contain a null character",
            (NotContained::Rooted, true) => "is absolute",
            (NotContained::Rooted, false) => "are absolute",
        };
        if !lines.is_empty() {
            lines.push(String::new());
        }
        lines.push(format!("Preflight failed: {count} {what}:"));
        lines.push(String::new());
        for (source, destination, _) in these.iter().take(10) {
            lines.push(format!(
                "  {} (from source: {source})",
                with_visible_nulls(destination)
            ));
        }
        if these.len() > 10 {
            lines.push(format!("  ... and {} more", format_count(these.len() - 10)));
        }
    }
    lines.push(String::new());
    lines.push(ADJUST_THE_PATTERN.to_string());
    lines
}

/// One destination that would leave its directory, named whole — what a sample destination
/// shows in its place before the user confirms.
fn not_contained_sentence(destination: &str, why: NotContained) -> String {
    let destination = with_visible_nulls(destination);
    match why {
        NotContained::ParentDirectory => {
            format!("the destination `{destination}` contains a `..` directory name")
        }
        NotContained::NullChar => {
            format!("the destination `{destination}` contains a null character")
        }
        NotContained::Rooted => format!("the destination `{destination}` is an absolute path"),
    }
}

/// `cluster status`'s counted line for destinations that would leave their directory: what the
/// sources share, since each has its own destination.
fn not_contained_count_line(why: NotContained, sources: usize) -> String {
    let count = if sources == 1 {
        "1 source".to_string()
    } else {
        format!("{} sources", format_count(sources))
    };
    let what = match why {
        NotContained::ParentDirectory => "contains a `..` directory name",
        NotContained::NullChar => "contains a null character",
        NotContained::Rooted => "is an absolute path",
    };
    format!("{count}: the destination {what}")
}

/// The clause both commands say about a `base_dir` that would leave the archive root, the value
/// quoted as the manifest specifies it.
fn base_dir_leaves(base_dir: &str) -> String {
    format!(
        "base_dir '{}' leaves the archive root — absolute paths, `..` components and null \
         characters are not allowed",
        with_visible_nulls(base_dir)
    )
}

/// `apply`'s refusal of a `base_dir` that would leave the archive root.
fn base_dir_refusal(base_dir: &str) -> String {
    format!(
        "Manifest {}.\nEdit base_dir in the manifest's [output] section to a directory inside \
         the archive root.",
        base_dir_leaves(base_dir)
    )
}

/// `cluster status`'s line for the same: the act it turns away, the cause, the way back.
fn base_dir_status_line(base_dir: &str) -> String {
    format!(
        "apply would be rejected: {}; edit base_dir in the manifest's [output] section",
        base_dir_leaves(base_dir)
    )
}

/// `cluster status`'s summary where no entry has a destination worked out, because of the
/// pattern or `base_dir`: the entries named once with why, and the source files, which were
/// checked.
fn no_destination_summary(
    count: usize,
    because: crate::archive::ops::status::NoDestination,
    missing: usize,
) -> String {
    use crate::archive::ops::status::NoDestination;
    let entries = if count == 1 { "entry" } else { "entries" };
    let why = match because {
        NoDestination::PatternUnreadable => "the pattern cannot be used as specified",
        NoDestination::BaseDirNotContained => "base_dir leaves the archive root",
    };
    format!(
        "Summary: {} {entries} with no destination computable: {why}; {} source files missing.",
        format_count(count),
        format_count(missing)
    )
}

/// `cluster status`'s line for a pattern it cannot read, before the pattern's own problems.
const PATTERN_UNREADABLE: &str =
    "apply would be rejected: the pattern cannot be used as specified — adjust the pattern in \
     your manifest";

/// What `cluster refresh` says about the zone a manifest had no line for.
fn zone_line_recorded_statement(zone: &RecordedZone) -> String {
    format!(
        "Recorded this machine's time zone in the manifest: {}",
        zone.name()
    )
}

/// What `cluster refresh` says when the zone it records differs from the one the old lock
/// was built in: in full where the query read an instant on the clock, since the change can
/// move what is selected; in one quiet clause where it read none.
fn zone_change_statement(
    old: &RecordedZone,
    new: &RecordedZone,
    query_read_an_instant: bool,
) -> String {
    let (old, new) = (old.name(), new.name());
    if query_read_an_instant {
        format!(
            "Time zone changed from {old} to {new}: times in the query are now in {new}, which \
             can change which sources a time query selects"
        )
    } else {
        format!("(time zone changed from {old} to {new})")
    }
}

/// What a manifest command says when it read times on the manifest's zone and this machine
/// keeps another.
fn manifest_clock_statement(zone: &RecordedZone, machine: &UserZone) -> String {
    format!(
        "times are in {}, as the manifest records (this machine: {})",
        zone.name(),
        machine.label()
    )
}

/// `cluster status`'s zone line where the lock carries a zone this machine reads: the zone
/// the destinations above were placed on.
fn status_lock_zone_line(zone: &RecordedZone) -> String {
    format!("Time zone: {} (the lock's)", zone.name())
}

/// `cluster status`'s zone line where the lock names a zone this machine does not know.
fn status_lock_zone_name_line(name: &str) -> String {
    format!("Time zone: {name} (the lock's)")
}

/// `cluster status`'s zone line where the lock records none — there is no lock, or it
/// predates the zone — and the manifest's line names one.
fn status_manifest_zone_line(zone: &RecordedZone, lock_present: bool) -> String {
    let lock = if lock_present {
        "the lock records none"
    } else {
        "no lock"
    };
    format!("Time zone: {} (the manifest's; {lock})", zone.name())
}

/// `cluster status`'s zone line where neither the lock nor the manifest records a zone.
fn status_no_zone_line_lead(zone: &RecordedZone, manifest: &Path) -> String {
    format!(
        "Time zone: none recorded; a refresh on this machine would record {}: {}",
        zone.name(),
        refresh_command(manifest, false)
    )
}

/// `cluster status`'s zone line where the lock records no zone and the refresh would refuse:
/// the manifest's line names none this machine can read, or there is no line and this
/// machine's zone cannot be written down.
fn status_zone_refusal_lead(refusal: &ZoneRefusal) -> String {
    let lead = match refusal {
        ZoneRefusal::Line(_) => "none this machine can read",
        ZoneRefusal::Machine(_) => "none recorded",
    };
    format!(
        "Time zone: {lead}; {}",
        refresh_would_refuse(&zone_refusal(refusal))
    )
}

/// The clause under the lock's zone where the manifest's line names another zone.
fn manifest_zone_differs(zone: &RecordedZone, manifest: &Path) -> String {
    format!(
        "the manifest's zone line says {}; {} brings the lock up to date",
        zone.name(),
        refresh_command(manifest, false)
    )
}

/// The clause under the lock's zone where the manifest's line was deleted.
fn manifest_zone_line_deleted(zone: &RecordedZone, manifest: &Path) -> String {
    format!(
        "the manifest has no zone line; a refresh on this machine would record {}: {}",
        zone.name(),
        refresh_command(manifest, false)
    )
}

/// The way back from a recorded query that cannot run, by cause, as `cluster refresh` and
/// `cluster status` state it: `--edit` where the query holds input Canon cannot read; the
/// conversion the refusal names, then a plain refresh, where a key holds two types — never
/// `--edit` for a query that is correct; and where both stand, one line taking both steps in
/// order, so neither meets a second refusal.
fn query_way_back(refusal: &ExpressionRefused, manifest: &Path) -> String {
    let edit = !refusal.is_answer();
    match (refusal.has_answer(), edit) {
        (true, true) => format!(
            "After converting, `{}` to fix the query and select again",
            refresh_command(manifest, true)
        ),
        (true, false) => format!(
            "After converting, `{}` to select again",
            refresh_command(manifest, false)
        ),
        (false, _) => format!(
            "`{}` to fix the query and select again",
            refresh_command(manifest, true)
        ),
    }
}

/// The recorded query's refusal as `cluster refresh` states it: the refusal — its first line,
/// then each cause under the typed line it sits in — and the way back.
fn query_refusal_block(refusal: &ExpressionRefused, manifest: &Path) -> String {
    format!("{refusal}\n{}", query_way_back(refusal, manifest))
}

/// The recorded query's refusal as `cluster status` states it: what the refresh would be
/// rejected for, then each cause under it four spaces further in, then the way back at the
/// causes' indent — after them, since with two causes there are two steps. Where the
/// scope's terminal rule is what turns the refresh away first, the way back is not named
/// here: the refresh it names would be turned away, so it follows the scope's own answer
/// instead ([`scope_kept_nothing_way_back`]), wherever the report owes that one.
fn query_status_lines(
    refusal: &ExpressionRefused,
    manifest: &Path,
    follows_the_scope: bool,
) -> Vec<String> {
    let mut lines = vec![refresh_would_refuse("the recorded query cannot run")];
    lines.extend(refusal.render("    "));
    if !follows_the_scope {
        lines.push(format!("    {}", query_way_back(refusal, manifest)));
    }
    lines
}

/// What stops a refresh — the manifest's zone or a query line — in the refresh's own words, as
/// a report states it: the act it turns away named first, as the door lines do.
fn refresh_would_refuse(cause: &str) -> String {
    format!("cluster refresh would be rejected: {cause}")
}

/// `cluster status`'s summary where it placed no entry, because the lock records no zone this
/// machine can read: in place of the counts, which would assert a classification nobody made
/// — every entry is unplaced or none is. Said once; the cause stands on the `Lock:` or zone line.
fn unplaced_statement(count: usize) -> String {
    let (entries, files) = if count == 1 {
        ("entry", "its source file")
    } else {
        ("entries", "their source files")
    };
    format!(
        "{} {entries} with no destination computable, and {files} not checked: the lock records no \
         time zone this machine can read.",
        format_count(count)
    )
}

#[cfg(test)]
mod tests {
    use std::path::Path;

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
    /// command surface is in this file**, and there are exactly four. A new
    /// one anywhere in `cli/` fails here, and the repair is to route it
    /// through [`refresh_command`], [`apply_command`], [`status_command`] or
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
                if [
                    "canon apply",
                    "canon cluster refresh",
                    "canon cluster status",
                    "canon scan",
                ]
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
                r#"cli.rs: "canon cluster refresh {flag}{}","#.to_string(),
                r#"cli.rs: "canon apply {flag}{}","#.to_string(),
                r#"cli.rs: "canon cluster status {}","#.to_string(),
                r#"cli.rs: "If the files are there, `canon scan {}` to index them.","#.to_string(),
            ],
            "a command spelled with a path outside this file's four owners"
        );
    }

    /// One pair per cause the run met, and every pair a run can meet says something; the
    /// scope pair stands alone where it is met; each way out that changes what the lock holds
    /// names the refresh.
    /// **Each cause met has its own way back, worded as the no-place table writes it**, the
    /// refresh named as a command for this manifest. Walked over every cause: the match below
    /// has no rest arm, so a new cause fails the build here until it joins the walk.
    #[test]
    fn the_pattern_remedy_prints_one_pair_per_cause_met() {
        use super::adjust_pattern_remedy;
        use crate::archive::ops::pattern::PatternGaps;
        use crate::core::domain::path::NotContained;
        use crate::core::domain::time::value::Precision;
        use crate::expr::{Modifier, NoPlaceCause};

        let manifest = Path::new("/m/trip.toml");
        let refresh = "`canon cluster refresh /m/trip.toml`";
        let met = |causes: Vec<NoPlaceCause>| PatternGaps {
            met: causes.into_iter().map(|c| (c, 1)).collect(),
            ..Default::default()
        };
        let every = [
            NoPlaceCause::NoFact {
                subject: "{content.Make}".into(),
                key: "content.Make".into(),
            },
            NoPlaceCause::NoValue {
                subject: "{source.ext}".into(),
                reason: None,
            },
            NoPlaceCause::UnreadableSaveTime,
            NoPlaceCause::NoScopeRecorded,
            NoPlaceCause::LockPredatesMeasurement,
            NoPlaceCause::TooCoarse {
                subject: "{content.Taken|month}".into(),
                modifier: Modifier::Month,
                precision: Precision::Year,
            },
            NoPlaceCause::ContainsSlash {
                subject: "{content.Make}".into(),
                value: "AC/DC".into(),
            },
            NoPlaceCause::Empty {
                subject: "{content.Make}".into(),
            },
            NoPlaceCause::DotName {
                subject: "{content.Make}".into(),
                value: "..".into(),
                in_path: false,
            },
            NoPlaceCause::NoSegments {
                subject: "{source.rel_path[1:]}".into(),
            },
            NoPlaceCause::NullChar {
                subject: "{content.Make}".into(),
            },
        ];
        let shape = format!(
            "Adjust the pattern in your manifest; or leave such sources out with the manifest's \
             query, then run {refresh}."
        );
        let scope = format!(
            "Use {{source.rel_path}} to keep each file's own directories, or add paths to \
             meta.scope and run {refresh}."
        );
        for cause in every {
            let expected: Vec<String> = match &cause {
                NoPlaceCause::NoFact { .. } => vec![
                    format!(
                        "Adjust the pattern in your manifest, or import the missing facts; or add \
                         \"content.Make?\" to the manifest's query to leave such sources out, then \
                         run {refresh}."
                    ),
                    "`canon facts` shows which sources have these facts".to_string(),
                ],
                NoPlaceCause::NoValue { .. } => vec![format!(
                    "Adjust the pattern in your manifest; or add \"source.ext?\" to the manifest's \
                     query to leave such sources out, then run {refresh}."
                )],
                NoPlaceCause::UnreadableSaveTime => vec![format!(
                    "Add \"source.mtime?\" to the manifest's query to leave such sources out, or \
                     correct the file's save time and scan again; then run {refresh}."
                )],
                NoPlaceCause::NoScopeRecorded | NoPlaceCause::LockPredatesMeasurement => {
                    vec![scope.clone()]
                }
                NoPlaceCause::TooCoarse { .. } => vec![format!(
                    "Use a coarser modifier in the pattern, which takes effect at the next apply; \
                     or add \"content.Taken|month?\" to the manifest's query to leave such sources \
                     out, then run {refresh}."
                )],
                NoPlaceCause::ContainsSlash { .. }
                | NoPlaceCause::Empty { .. }
                | NoPlaceCause::DotName { .. }
                | NoPlaceCause::NoSegments { .. }
                | NoPlaceCause::NullChar { .. } => vec![shape.clone()],
            };
            assert_eq!(
                adjust_pattern_remedy(&met(vec![cause.clone()]), manifest),
                expected,
                "{cause:?}"
            );
        }

        // A destination that would leave its directory: an edit to the pattern alone.
        let escaped = PatternGaps {
            not_contained: vec![(NotContained::ParentDirectory, 1)],
            ..Default::default()
        };
        assert_eq!(
            adjust_pattern_remedy(&escaped, manifest),
            ["Adjust the pattern in your manifest."]
        );

        // Where the pattern needs a scope the lock lacks, the scope's line stands alone: the
        // scope must be settled whatever else is wrong.
        let with_scope = PatternGaps {
            needs_a_scope_it_lacks: true,
            ..met(vec![NoPlaceCause::UnreadableSaveTime])
        };
        assert_eq!(adjust_pattern_remedy(&with_scope, manifest), [scope]);

        // Each query entry once, in the order met, joined with " and ".
        let two = met(vec![
            NoPlaceCause::TooCoarse {
                subject: "{content.b|day}".into(),
                modifier: Modifier::Day,
                precision: Precision::Month,
            },
            NoPlaceCause::TooCoarse {
                subject: "{content.a|month}".into(),
                modifier: Modifier::Month,
                precision: Precision::Year,
            },
        ]);
        assert!(adjust_pattern_remedy(&two, manifest)[0]
            .contains("add \"content.b|day?\" and \"content.a|month?\" to the manifest's query"));
    }

    /// Each entry a remedy names for the manifest's query reads, pasted into the query's TOML
    /// list as printed, as the cause's own entry: a key that needs quotes and a `{…}` specified
    /// with `"` both included.
    #[test]
    fn a_remedy_query_entry_pastes_into_the_manifest_as_printed() {
        use super::adjust_pattern_remedy;
        use crate::archive::ops::pattern::PatternGaps;
        use crate::expr::NoPlaceCause;

        let causes = [
            NoPlaceCause::NoFact {
                subject: "{'content.File Size'}".into(),
                key: "content.File Size".into(),
            },
            NoPlaceCause::NoValue {
                subject: "{\"content.File Size\"|year}".into(),
                reason: None,
            },
            NoPlaceCause::NoFact {
                subject: "{content.Make}".into(),
                key: "content.Make".into(),
            },
        ];
        for cause in causes {
            let gaps = PatternGaps {
                met: vec![(cause.clone(), 1)],
                ..Default::default()
            };
            let line = adjust_pattern_remedy(&gaps, Path::new("/m/trip.toml")).remove(0);
            let printed = line
                .split_once("add ")
                .and_then(|(_, rest)| rest.split_once(" to the manifest's query"))
                .map(|(entry, _)| entry)
                .unwrap_or_else(|| panic!("no query entry in {line}"));
            let query: toml::Value = toml::from_str(&format!("query = [{printed}]"))
                .unwrap_or_else(|e| panic!("{printed} is not a TOML string: {e}"));
            assert_eq!(
                query["query"][0].as_str(),
                cause.query_entry().as_deref(),
                "{line}"
            );
        }
    }

    /// `apply`'s count says "1 source" for one, and each source carries every cause it met.
    #[test]
    fn the_no_place_block_counts_its_sources() {
        use super::no_place_lines;
        use crate::archive::ops::pattern::PatternGaps;
        use crate::expr::{NoPlace, NoPlaceCause};

        let manifest = Path::new("/m/trip.toml");
        let no_fact = |subject: &str, key: &str| NoPlaceCause::NoFact {
            subject: subject.into(),
            key: key.into(),
        };
        let one = vec![(
            "/photos/a.jpg".to_string(),
            NoPlace {
                causes: vec![
                    no_fact("{content.Make}", "content.Make"),
                    no_fact("{content.Lens}", "content.Lens"),
                ],
            },
        )];
        let lines = no_place_lines(&one, &PatternGaps::default(), manifest);
        assert_eq!(lines[0], "Error: 1 source failed pattern expansion:");
        assert_eq!(
            lines[1],
            "  /photos/a.jpg: {content.Make} has no value; {content.Lens} has no value"
        );
        let two = [one.clone(), one].concat();
        assert_eq!(
            no_place_lines(&two, &PatternGaps::default(), manifest)[0],
            "Error: 2 sources failed pattern expansion:"
        );
    }

    /// `apply`'s preflight block for destinations that would leave their directory: one heading
    /// per way, the count in its own number, each destination whole with a null character as
    /// `\0`, then the way back.
    #[test]
    fn the_containment_block_names_each_destination_whole() {
        use super::not_contained_lines;
        use crate::core::domain::path::NotContained;

        let found = vec![
            (
                "/photos/README".to_string(),
                "../README".to_string(),
                NotContained::ParentDirectory,
            ),
            (
                "/photos/x".to_string(),
                "a\0b/x".to_string(),
                NotContained::NullChar,
            ),
            (
                "/photos/y".to_string(),
                "a\0b/y".to_string(),
                NotContained::NullChar,
            ),
        ];
        assert_eq!(
            not_contained_lines(&found),
            [
                "Preflight failed: 1 destination path contains a `..` directory name:",
                "",
                "  ../README (from source: /photos/README)",
                "",
                "Preflight failed: 2 destination paths contain a null character:",
                "",
                "  a\\0b/x (from source: /photos/x)",
                "  a\\0b/y (from source: /photos/y)",
                "",
                "Adjust the pattern in your manifest.",
            ]
        );
    }

    /// A `base_dir` that would leave the archive root is refused by `apply` in one sentence,
    /// the value quoted as the manifest specifies it — a null character as `\0` — and is no
    /// `ContainedPath`, so no plan can be built below it.
    #[test]
    fn a_base_dir_leaving_the_archive_root_is_refused_in_one_sentence() {
        use crate::core::domain::path::{ContainedPath, NotContained};
        assert_eq!(
            ContainedPath::new("../../tmp"),
            Err(NotContained::ParentDirectory)
        );
        assert_eq!(
            super::base_dir_refusal("../../tmp"),
            "Manifest base_dir '../../tmp' leaves the archive root — absolute paths, `..` \
             components and null characters are not allowed.\n\
             Edit base_dir in the manifest's [output] section to a directory inside the archive root."
        );
        assert!(super::base_dir_refusal("a\0b").starts_with("Manifest base_dir 'a\\0b' leaves"));
        assert_eq!(
            super::base_dir_status_line("../x"),
            "apply would be rejected: base_dir '../x' leaves the archive root — absolute paths, \
             `..` components and null characters are not allowed; edit base_dir in the \
             manifest's [output] section"
        );
    }

    #[test]
    fn an_absolute_base_dir_is_refused_in_one_sentence() {
        use crate::core::domain::path::{ContainedPath, NotContained};
        assert_eq!(
            ContainedPath::new("/tmp/elsewhere"),
            Err(NotContained::Rooted)
        );
        assert!(super::base_dir_refusal("/tmp/elsewhere")
            .starts_with("Manifest base_dir '/tmp/elsewhere' leaves the archive root"));
    }
}
