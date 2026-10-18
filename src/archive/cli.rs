//! Archive's command surface: `canon cluster generate/refresh/status` and
//! `canon apply`. One declares an intended archive operation, the other
//! performs it, and they share almost nothing — so they stay separate files
//! rather than one module. What little they do share is here, where both can
//! reach it: text a user reads from either command about the same situation,
//! which must not be spelled twice.

pub(super) mod apply;
pub(super) mod cluster;

use std::path::Path;

use crate::archive::domain::{LockStanding, ZoneRefusal};
use crate::archive::ops::pattern::PatternGaps;
use crate::core::domain::format::shell_quote;
use crate::core::domain::format_count;
use crate::core::domain::time::zone::{RecordedZone, Unrecordable, UserZone, ZoneNameError};
use crate::expr::ExpressionRefused;

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
    fn way_back(self, manifest: &Path, set_aside: &[String]) -> Vec<String> {
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
    format!("Rebuild it: {}", refresh_command(manifest, false))
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
            refresh_command(manifest, false)
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
            "the zone line holds a time zone rule, not a zone name — write a zone name, for \
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

/// The way back from a pattern that cannot expand, where no rebuild would
/// change that.
///
/// **One pair per cause the run met, and none is right for another.** A
/// pattern that places files relative to the manifest's scope fails when there
/// is no scope to place them from, and nothing about facts helps: the scope
/// key is not a fact, so checking fact coverage sends the reader to look for
/// something that was never missing. A pattern naming a fact no source carries
/// fails for the opposite reason, and scoping the manifest does nothing for
/// it. A save time Canon cannot read is neither: the file has one, so
/// importing facts cannot supply it. Which ones the run met comes off the
/// language facility's typed causes (`ops::pattern::PatternGaps`), never off
/// the wording of a refusal.
///
/// The scope answer stands alone where it is met, because the scope must be
/// settled whatever else is wrong. A value too coarse for its modifier gets a
/// pair naming each key and modifier that failed as the query entry that leaves
/// such files out. That entry and a corrected save time are both read when the
/// lock is built, so those two pairs name the refresh.
///
/// Both commands say it because both meet the state: `apply` refuses on it,
/// and `cluster status` reaches it as a withheld step whose cause names no act
/// of its own. One composition, so the two cannot drift — which is what the
/// two invitations to apply did before [`apply_command`] existed.
fn adjust_pattern_remedy(gaps: &PatternGaps) -> Vec<String> {
    if gaps.needs_a_scope_it_lacks {
        return vec![
            "The pattern places files relative to the scopes declared in the manifest, and this lock records none.".to_string(),
            "Use {source.rel_path} to keep each file's own folders, or add paths to meta.scope and refresh.".to_string(),
        ];
    }
    let mut lines = Vec::new();
    if gaps.fact_not_carried {
        lines
            .push("Adjust the pattern in your manifest, or import the facts it names.".to_string());
        lines.push("Use 'canon facts' to see which sources carry them.".to_string());
    }
    // Both ways out below change what the lock holds, a query edit and a save time alike
    // (placement reads the lock entry's), so each names the refresh that rebuilds it.
    if gaps.unreadable_save_time {
        lines.push("A save time Canon cannot read gives the pattern no value.".to_string());
        lines.push(
            "Add \"source.mtime?\" to the manifest's query to leave such files out, or correct the file's save time and scan again; then refresh."
                .to_string(),
        );
    }
    if !gaps.too_coarse.is_empty() {
        lines.push(
            "A time known only to a coarser grain than the pattern uses gives the pattern no value."
                .to_string(),
        );
        lines.push(coarse_way_out(gaps));
    }
    lines
}

/// The two ways out of a time too coarse for the pattern's modifier, one sentence for
/// every command that states the cause. A coarser modifier takes effect at the next apply
/// with no refresh, because the pattern is read when files are placed; a query entry
/// changes what the lock holds, so only that way names the refresh.
fn coarse_way_out(gaps: &PatternGaps) -> String {
    let entries: Vec<String> = gaps
        .too_coarse_chains()
        .iter()
        .map(|c| format!("\"{c}?\""))
        .collect();
    format!(
        "Use a coarser modifier in the pattern, which takes effect at the next apply; or add {} \
         to the manifest's query to leave such files out, then refresh.",
        entries.join(" and ")
    )
}

/// What `cluster refresh` says about times too coarse for the pattern: the counts
/// ([`coarse_value_counts`]), then the ways out once.
fn coarse_value_statements(gaps: &PatternGaps) -> Vec<String> {
    if gaps.too_coarse.is_empty() {
        return Vec::new();
    }
    let mut lines = coarse_value_counts(gaps);
    lines.push(coarse_way_out(gaps));
    lines
}

/// One line per key, modifier and grain a time was too coarse for, with how many sources
/// hold it — read by `cluster refresh` and `cluster status` alike; the ways out follow in
/// each command's own place.
fn coarse_value_counts(gaps: &PatternGaps) -> Vec<String> {
    gaps.too_coarse
        .iter()
        .map(|((chain, grain), count)| {
            let key = chain.split('|').next().unwrap_or(chain);
            let (sources, them) = if *count == 1 {
                ("source holds", "it")
            } else {
                ("sources hold", "them")
            };
            format!(
                "{} {sources} {key} only to the {}, so {{{chain}}} has no value for {them}",
                format_count(*count),
                grain.word()
            )
        })
        .collect()
}

/// What `cluster refresh` says when the lock it wrote cannot be read back for its statements:
/// the refresh itself is done, and what is missing is said.
fn statement_pass_failed(error: &anyhow::Error) -> String {
    format!(
        "Warning: the lock was written, but reading it back failed, so nothing is said about \
         whether its pattern uses a time that depends on the time zone or about its coarse values: {error:#}"
    )
}

/// What `cluster refresh` says about the zone a manifest had no line for.
fn zone_line_recorded_statement(zone: &RecordedZone) -> String {
    format!(
        "Recorded this machine's time zone in the manifest: {}",
        zone.name()
    )
}

/// What `cluster refresh` says when the zone it records differs from the one the old lock
/// was built in: in full where the run read an instant on the clock, since the change can
/// move what is selected and where it lands; in one quiet clause where it read none, or where
/// the pattern's reads are not known and the query read none.
fn zone_change_statement(old: &RecordedZone, new: &RecordedZone, instant_read: bool) -> String {
    let (old, new) = (old.name(), new.name());
    if instant_read {
        format!(
            "Time zone changed from {old} to {new}: times in the query and in folder names \
             are now in {new}, which can change which sources a time query selects and which \
             folder a file goes to"
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

/// The way back from a recorded query that cannot run, as `cluster refresh` and `cluster
/// status` state it.
fn query_way_back(manifest: &Path) -> String {
    format!(
        "`{}` to fix the query and select again",
        refresh_command(manifest, true)
    )
}

/// The recorded query's refusal as `cluster refresh` states it: the refusal — its first line,
/// then each cause under the typed line it sits in — and the way back.
fn query_refusal_block(refusal: &ExpressionRefused, manifest: &Path) -> String {
    format!("{refusal}\n{}", query_way_back(manifest))
}

/// The recorded query's refusal as `cluster status` states it: what the refresh would be
/// rejected for and the way back on one line, then each cause under it, four spaces further in.
fn query_status_lines(refusal: &ExpressionRefused, manifest: &Path) -> Vec<String> {
    let mut lines = vec![refresh_would_refuse(&format!(
        "the recorded query cannot run — {}",
        query_way_back(manifest)
    ))];
    lines.extend(refusal.render("    "));
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
        "{} {entries} with no destination computed, and {files} not checked: the lock records no \
         time zone this machine can read.",
        format_count(count)
    )
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
                r#"cli.rs: "canon cluster refresh {flag}{}","#.to_string(),
                r#"cli.rs: "canon apply {flag}{}","#.to_string(),
                r#"cli.rs: "If the files are there, `canon scan {}` to index them.","#.to_string(),
            ],
            "a command spelled with a path outside this file's three owners"
        );
    }

    /// One pair per cause the run met, and every pair a run can meet says something; the
    /// scope pair stands alone where it is met; each way out that changes what the lock holds
    /// names the refresh.
    #[test]
    fn the_pattern_remedy_prints_one_pair_per_cause_met() {
        use super::adjust_pattern_remedy;
        use crate::archive::ops::pattern::PatternGaps;
        use crate::core::domain::time::value::Precision;

        let save_time = [
            "A save time Canon cannot read gives the pattern no value.",
            "Add \"source.mtime?\" to the manifest's query to leave such files out, or correct the file's save time and scan again; then refresh.",
        ];
        let facts = [
            "Adjust the pattern in your manifest, or import the facts it names.",
            "Use 'canon facts' to see which sources carry them.",
        ];
        let coarse = |keys: &[&str]| -> PatternGaps {
            PatternGaps {
                too_coarse: keys
                    .iter()
                    .map(|k| ((k.to_string(), Precision::Year), 1))
                    .collect(),
                ..Default::default()
            }
        };

        let only_save_time = PatternGaps {
            unreadable_save_time: true,
            ..Default::default()
        };

        // A withheld step still answers: every cause a run can meet alone composes a way
        // back, so `cluster status` never ends on a cause with nothing after it. The
        // destructure names every cause with no rest, so a new one fails the build here
        // until it joins the walk below.
        let PatternGaps {
            needs_a_scope_it_lacks: _,
            unreadable_save_time: _,
            too_coarse: _,
            fact_not_carried: _,
            used_an_instant: _,
        } = PatternGaps::default();
        for alone in [
            PatternGaps::for_lock(true),
            only_save_time.clone(),
            coarse(&["content.DateTimeOriginal|month"]),
            PatternGaps {
                fact_not_carried: true,
                ..Default::default()
            },
        ] {
            assert!(!adjust_pattern_remedy(&alone).is_empty(), "{alone:?}");
        }

        assert_eq!(adjust_pattern_remedy(&only_save_time), save_time);

        let both = PatternGaps {
            unreadable_save_time: true,
            fact_not_carried: true,
            ..Default::default()
        };
        assert_eq!(adjust_pattern_remedy(&both), [facts, save_time].concat());

        let with_scope = PatternGaps {
            needs_a_scope_it_lacks: true,
            unreadable_save_time: true,
            fact_not_carried: true,
            ..coarse(&["content.DateTimeOriginal|month"])
        };
        let scope_alone = adjust_pattern_remedy(&with_scope);
        assert_eq!(scope_alone.len(), 2);
        assert!(
            scope_alone[1].contains("{source.rel_path}"),
            "{scope_alone:?}"
        );

        // A too-coarse failure gives both ways out: a coarser modifier, read at the next
        // apply, or each key and modifier it met as a query entry, in order, because that
        // entry is what the user types — and only that way names the refresh.
        assert_eq!(
            adjust_pattern_remedy(&coarse(&["content.DateTimeOriginal|month"])),
            [
                "A time known only to a coarser grain than the pattern uses gives the pattern no value.",
                "Use a coarser modifier in the pattern, which takes effect at the next apply; or add \"content.DateTimeOriginal|month?\" to the manifest's query to leave such files out, then refresh.",
            ]
        );
        assert_eq!(
            adjust_pattern_remedy(&coarse(&["content.b|day", "content.a|month"]))[1],
            "Use a coarser modifier in the pattern, which takes effect at the next apply; or add \"content.a|month?\" and \"content.b|day?\" to the manifest's query to leave such files out, then refresh."
        );
    }
}
