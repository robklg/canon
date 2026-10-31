//! `canon facts convert`: the read, the plan, `--list` and the act's result as printed. A value
//! prints through the printing rule for its own type: the read's as `canon facts` prints it, a
//! time on the user's clock; the plan's lost and removed values in their stored spelling. The
//! spelling example and `--list` show the stored spellings as written. Every key prints through
//! `printed_key`, every command through the shell quoting it runs under, and every statement
//! about a value and its type through the statement grammar.

use std::path::Path;

use anyhow::Result;

use crate::core::domain::config::{LedgerConfig, RecordingMode};
use crate::core::domain::decision::DecisionCommand;
use crate::core::domain::fact::{Count, FactType, Stored};
use crate::core::domain::format::shell_quote;
use crate::core::domain::format_count;
use crate::core::domain::time::zone::ReadTimesOn;
use crate::core::ops::decision::DecisionParams;
use crate::core::repo::Db;
use crate::expr::{
    convert_command, printed_key, type_words, AliasesNaming, Complement, Noun, Statement, Subject,
};
use crate::facts::domain::convert::{
    acting_command, bridge_clause, class_summary, duration_routes, facts_by_type,
    not_a_conversion_lines, parse_allow, place_parts, printed, stored_spelling, unreadable_line,
    valueless_parts, Class, ClassGroups, EmptyFiles, ListLine, ListMark, Outcome, Place, Places,
    ValueGroup, UNREADABLE_TIME,
};
use crate::facts::domain::{counted, plural, TypeWord};
use crate::facts::ops::convert::{
    execute_convert, plan_convert, read_key, ConvertOutcome, KeyRead, PlanRead,
};

/// What `canon facts convert` was given.
pub struct ConvertOptions {
    pub key: String,
    pub to: Option<TypeWord>,
    pub list: bool,
    /// `-0`: `--list`'s fields NUL-separated, each record NUL-ended, nothing escaped.
    pub null: bool,
    pub allow: Vec<String>,
    pub yes: bool,
    pub reason: Option<String>,
}

/// `canon facts convert`: the read without `--to`, the plan with it, `--list`, or the act with
/// `--yes`.
pub fn convert(
    db: &mut Db,
    options: &ConvertOptions,
    canon_home: &Path,
    command_line: &str,
    config: &LedgerConfig,
) -> Result<()> {
    let given = parse_allow(&options.allow).map_err(anyhow::Error::msg)?;
    let conn = db.conn_mut();
    let Some(target) = options.to else {
        let read = read_key(conn, &options.key, crate::user_zone())?;
        for line in read_lines(&read) {
            println!("{line}");
        }
        return Ok(());
    };
    if options.yes {
        let decision = DecisionParams {
            command: DecisionCommand::FactsConvert,
            scope: Vec::new(),
            command_line: command_line.to_string(),
            reason: options.reason.clone(),
            record_enabled: config.recording != RecordingMode::Off,
            receipt_enabled: false,
            ledger_config: config.clone(),
            zone: ReadTimesOn::of_user(crate::user_zone()),
        };
        let outcome = execute_convert(conn, &options.key, target, &given, Some(&decision))?;
        for line in act_lines(&outcome, config.recording == RecordingMode::Off) {
            println!("{line}");
        }
        if let ConvertOutcome::Converted(result) = &outcome {
            for warning in &result.warnings {
                eprintln!("{warning}");
            }
        }
        return Ok(());
    }
    let planned = plan_convert(conn, &options.key, target, canon_home)?;
    if options.list {
        // A reader that stops early (`| head`) closes the pipe: the list ends there, as `ls` ends.
        use std::io::Write;
        let _ = std::io::stdout()
            .lock()
            .write_all(list_text(&planned.list(), options.null).as_bytes());
    } else {
        for line in plan_lines(&planned, options.reason.as_deref()) {
            println!("{line}");
        }
    }
    Ok(())
}

// ============================================================================
// The act
// ============================================================================

/// What the act did: the summary, then the key's type now — or that it has none — and where
/// the record is.
fn act_lines(outcome: &ConvertOutcome, recording_off: bool) -> Vec<String> {
    let result = match outcome {
        ConvertOutcome::NothingToConvert { key, to } => {
            return vec![format!(
                "{}; nothing to convert.",
                statement(Subject::Key(key), Complement::OfType(*to))
            )]
        }
        ConvertOutcome::Converted(result) => result,
    };
    let mut lines = vec![result.summary.clone()];
    lines.push(if result.performed.remaining() == 0 {
        format!(
            "{} has no facts; its next import sets its type anew",
            printed_key(&result.key)
        )
    } else {
        format!(
            "{}.",
            statement(
                Subject::Key(&result.key),
                Complement::OfType(result.performed.to)
            )
        )
    });
    match result.decision_id {
        Some(id) => lines.push(format!(
            "`canon trail show {id}` to read this conversion later"
        )),
        None if recording_off => lines.push("not recorded (recording is off)".to_string()),
        // A record that failed to start says so in its warning.
        None => {}
    }
    lines
}

// ============================================================================
// Shared pieces
// ============================================================================

fn statement(subject: Subject, complement: Complement) -> String {
    Statement {
        subject,
        complement,
    }
    .to_string()
}

/// `the value of N sources: <places>`, or nothing where the facts are the value of no source.
fn value_of(places: &Places, name_roots: bool) -> Option<String> {
    (places.total() > 0).then(|| {
        format!(
            "the value of {}: {}",
            counted(places.total(), "source", "sources"),
            place_parts(places, name_roots).join(", ")
        )
    })
}

/// The fact on empty files' content and the empty files that show it, by place — the same
/// line in the read and the plan.
fn empty_files_line(empty: &EmptyFiles) -> String {
    format!(
        "1 fact on empty files' content: {} show it ({})",
        counted(empty.shown_by.total(), "empty file", "empty files"),
        place_parts(&empty.shown_by, false).join(", ")
    )
}

/// The modifier asking for values as `to` without changing anything stored.
fn modifier_line(to: FactType) -> Option<String> {
    let modifier = match to {
        FactType::Num => "number",
        FactType::Time => "datetime",
        FactType::Text | FactType::Path => return None,
    };
    Some(format!(
        "`|{modifier}` asks for these values as {} without changing anything stored",
        to.noun(Count::Many)
    ))
}

/// What the next import does for a key of the target type, one statement per kind of value.
fn next_import_statements(next: &[crate::facts::domain::convert::NextImport]) -> Vec<String> {
    next.iter()
        .map(|import| {
            statement(
                Subject::Arriving(&import.arriving),
                match import.outcome {
                    Outcome::Stored(t) => Complement::StoredAs(t),
                    Outcome::Skipped => Complement::Skipped,
                },
            )
        })
        .collect()
}

// ============================================================================
// The read
// ============================================================================

fn read_lines(read: &KeyRead) -> Vec<String> {
    let key = printed_key(&read.key);
    let holdings = &read.holdings;
    let mut lines = vec![
        format!("{key} (whole index, read-only)"),
        format!("Type: {}", type_words(&holdings.types)),
        "Holds:".to_string(),
    ];
    let width = holdings
        .per_type
        .iter()
        .map(|holding| holding.t.as_str().len())
        .max()
        .unwrap_or(0);
    let under = " ".repeat(2 + width + 2);
    for holding in &holdings.per_type {
        let mut first = counted(holding.facts, "fact", "facts");
        if let Some(value_of) = value_of(&holding.sources, false) {
            first = format!("{first}, {value_of}");
        }
        lines.push(format!("  {:<width$}  {first}", holding.t.as_str()));
        for part in valueless_parts(holding.hidden, holding.no_source) {
            lines.push(format!("{under}{part}"));
        }
        if holding.unreadable > 0 {
            lines.push(format!("{under}{}", unreadable_line(holding.unreadable)));
        }
        if let Some(empty) = holdings
            .empty_files
            .as_ref()
            .filter(|empty| empty.t == holding.t)
        {
            lines.push(format!("{under}{}", empty_files_line(empty)));
        }
        if !holding.top.is_empty() {
            let top: Vec<String> = holding
                .top
                .iter()
                .map(|(value, n)| format!("{value} ×{}", counted(*n, "fact", "facts")))
                .collect();
            lines.push(format!("{under}most frequent: {}", top.join(", ")));
        }
    }
    if holdings.shares_content {
        lines.push(
            "  (copies of one content share one fact, so a fact can be the value of several \
             sources)"
                .to_string(),
        );
    }
    lines.push(format!(
        "`canon facts --key {} --global --limit 0` to see the values `canon facts` counts",
        shell_quote(&key)
    ));

    let mut reachable = Vec::new();
    let mut unreachable = Vec::new();
    for target in &read.targets {
        match &target.reach {
            Ok(plan) if plan.counts.total() == 0 => {}
            Ok(plan) => reachable.push((target.to, plan)),
            Err(why) => unreachable.push((target.to, *why)),
        }
    }
    let width = read
        .targets
        .iter()
        .map(|target| target.to.as_str().len())
        .max()
        .unwrap_or(0);
    let under = " ".repeat(2 + width + 2);
    if !reachable.is_empty() {
        lines.push(String::new());
        lines.push("Can convert to:".to_string());
    }
    for (to, plan) in &reachable {
        lines.push(format!(
            "  {:<width$}  {}: {}",
            to.as_str(),
            facts_by_type(&plan.bridge.facts),
            class_summary(&plan.counts)
        ));
        let from_text = plan.bridge.facts.iter().any(|(t, _)| *t == FactType::Text);
        if from_text {
            if let Some(modifier) = modifier_line(*to) {
                let line = if plan.removes_most() {
                    format!(
                        "{} — text on purpose? {modifier}",
                        statement(Subject::MostOfThese, Complement::Not(*to))
                    )
                } else {
                    modifier
                };
                lines.push(format!("{under}{line}"));
            }
        }
        if let Some(durations) = plan.durations {
            lines.push(format!(
                "{under}{}:",
                statement(
                    Subject::Counted(durations, Noun::Fact),
                    Complement::WrittenLikeDurations
                )
            ));
            for route in duration_routes(&read.key, plan.duration_removal()) {
                lines.push(format!("{under}  {route}"));
            }
        }
        if *to == FactType::Text {
            let mut gives_up =
                next_import_statements(&crate::facts::domain::convert::next_import(FactType::Text));
            gives_up.push("original spellings are not restored".to_string());
            lines.push(format!("{under}{}", gives_up.join("; ")));
        }
    }
    if !unreachable.is_empty() {
        lines.push("Cannot convert to:".to_string());
    }
    for (to, why) in &unreachable {
        let mut reasons = not_a_conversion_lines(*why, *to).into_iter();
        lines.push(format!(
            "  {:<width$}  {}",
            to.as_str(),
            reasons.next().unwrap_or_default()
        ));
        for reason in reasons {
            lines.push(format!("{under}{reason}"));
        }
    }
    if !reachable.is_empty() {
        lines.push(String::new());
    }
    for (to, _) in &reachable {
        lines.push(format!(
            "{} to preview",
            convert_command(&read.key, Some(*to))
        ));
    }
    lines
}

// ============================================================================
// The plan
// ============================================================================

fn plan_lines(planned: &PlanRead, reason: Option<&str>) -> Vec<String> {
    let plan = &planned.plan;
    let key = printed_key(&planned.key);
    let to = plan.to;
    let head = format!("{key} → {to} (whole index, dry run)");
    if plan.counts.total() == 0 {
        return vec![
            format!(
                "{head}: {}; nothing to convert.",
                statement(Subject::Key(&planned.key), Complement::OfType(to))
            ),
            "Dry run: nothing changed.".to_string(),
        ];
    }

    let mut lines = vec![format!("{head}: {}", bridge_clause(&plan.bridge))];

    if plan.already > 0 {
        lines.push(format!(
            "  {} already, {}",
            statement(
                Subject::Counted(plan.already, Noun::Fact),
                Complement::OfType(to)
            ),
            plural(plan.already, "left as it is", "left as they are")
        ));
    }
    let classes = [
        ("unchanged", plan.counts.unchanged),
        ("lossy", plan.counts.lossy),
        ("removed", plan.counts.removed),
    ];
    let width = classes
        .iter()
        .filter(|(_, n)| *n > 0)
        .map(|(_, n)| format_count(*n).len())
        .max()
        .unwrap_or(0);
    for (class, n) in classes {
        if n == 0 {
            continue;
        }
        let mut line = format!("  {class:<9} {:>width$}", format_count(n));
        if class == "unchanged" {
            if let Some((before, after)) = &plan.spelling {
                line.push_str(&format!(
                    "   spelling only, e.g. {} → {}",
                    stored_spelling(before),
                    stored_spelling(after)
                ));
            }
        }
        lines.push(line);
        let groups = match class {
            "lossy" => &plan.lossy,
            "removed" => &plan.removed,
            _ => continue,
        };
        lines.extend(group_lines(groups, to));
    }
    if let Some((empty, _)) = &plan.empty_files {
        let shown = empty.shown_by.total() > empty.shown_by.no_longer_present;
        lines.push(if shown {
            format!(
                "  {}, so it changes for all of them",
                empty_files_line(empty)
            )
        } else {
            "  1 fact on empty files' content: no present empty file shows it; it converts with \
             the rest"
                .to_string()
        });
    }

    let changes_something = plan.counts.lossy > 0 || plan.counts.removed > 0;
    if plan.removes_every_fact() {
        lines.push(format!(
            "  every fact of {key} would be removed; the key would be forgotten, and its next \
             import sets its type anew"
        ));
    } else {
        if changes_something {
            lines.push(
                "  After changing the type, the lost and removed values cannot be reimported as \
                 your tool wrote them until this key's type is converted back."
                    .to_string(),
            );
        }
        if plan.durations.is_some() {
            lines.push(
                "  Values removed here that are written like durations land again when imported \
                 with the duration hint."
                    .to_string(),
            );
        }
        lines.push(format!(
            "  After: {}.",
            next_import_statements(&planned.next).join("; ")
        ));
    }
    match &planned.aliases {
        AliasesNaming::Names(names) if names.is_empty() => {}
        AliasesNaming::Names(names) => {
            let names: Vec<String> = names.iter().map(|name| format!("@{name}")).collect();
            lines.push(format!("  Aliases naming this key: {}.", names.join(", ")));
        }
        AliasesNaming::FileHasErrors => {
            lines.push("  Aliases naming this key: could not check aliases.toml.".to_string())
        }
    }
    if !plan.removes_every_fact() {
        lines.push(
            "  Manifests whose naming pattern uses this key place files by the new values at their \
             next apply."
                .to_string(),
        );
    }
    if changes_something {
        let preview = convert_command(&planned.key, Some(to));
        lines.push(format!(
            "{} --list` to see each fact with its source (not kept after)",
            preview.strip_suffix('`').unwrap_or(&preview)
        ));
    }
    lines.push(format!(
        "{} to convert",
        acting_command(&planned.key, to, &plan.counts, reason)
    ));
    if reason.is_none() {
        lines.push("  add --reason '<why>' to record why".to_string());
    }
    lines.push("Dry run: nothing changed.".to_string());
    lines
}

/// A lossy or removed class's distinct values, each stated with its reason, then the count of
/// the rest.
fn group_lines(groups: &ClassGroups, to: FactType) -> Vec<String> {
    let mut lines: Vec<String> = groups
        .shown
        .iter()
        .map(|group| format!("      {}", group_line(group, to)))
        .collect();
    if groups.more > 0 {
        lines.push(format!(
            "      and {} more distinct {}",
            format_count(groups.more),
            plural(groups.more, "value", "values")
        ));
    }
    lines
}

fn group_line(group: &ValueGroup, to: FactType) -> String {
    let what = match &group.value {
        Stored::Unreadable => UNREADABLE_TIME.to_string(),
        Stored::Readable(value) => {
            let text = stored_spelling(value);
            let subject = Subject::Value {
                text: &text,
                as_type: value.kind(),
            };
            match (group.loss, &group.becomes) {
                (Some(loss), Some(becomes)) => format!(
                    "{} → {}",
                    statement(subject, Complement::WithLoss(loss)),
                    printed(becomes)
                ),
                _ => statement(subject, Complement::Not(to)),
            }
        }
    };
    let mut parts = vec![format!("×{}", counted(group.facts, "fact", "facts"))];
    parts.extend(value_of(&group.sources, true));
    parts.extend(valueless_parts(group.hidden, group.no_source));
    let mut line = format!("{what}: {}", parts.join(", "));
    let uncovered = &group.uncovered;
    let after: Vec<String> = [
        (uncovered.own, "then show their own fact"),
        (
            uncovered.own_converted,
            "then show their own fact, converted",
        ),
        (uncovered.no_value, "then have no value"),
    ]
    .into_iter()
    .filter(|(n, _)| *n > 0)
    .map(|(n, then)| format!("{} {then}", format_count(n)))
    .collect();
    if !after.is_empty() {
        line.push_str(&format!(" ({})", after.join("; ")));
    }
    line
}

// ============================================================================
// --list
// ============================================================================

/// One record per lossy or removed fact and holding source: class, original value, what it
/// becomes, path, mark. Tab-separated with `\t`, `\n` and `\` written `\t`, `\n`, `\\` inside a
/// field; with `null`, NUL-separated and NUL-ended, nothing escaped.
fn list_text(lines: &[ListLine], null: bool) -> String {
    let mut out = String::new();
    for line in lines {
        let class = match line.class {
            Class::Lossy(_) => "lossy",
            Class::Removed(_) => "removed",
            Class::Unchanged => "unchanged",
        };
        let original = match &line.original {
            Stored::Readable(value) => stored_spelling(value),
            Stored::Unreadable => UNREADABLE_TIME.to_string(),
        };
        let becomes = line
            .becomes
            .as_ref()
            .map(stored_spelling)
            .unwrap_or_default();
        let mark = match &line.mark {
            ListMark::Place(Place::Counted) => String::new(),
            ListMark::Place(Place::NoLongerPresent) => "no longer present".to_string(),
            ListMark::Place(Place::Suspended { .. }) => "suspended".to_string(),
            ListMark::Place(Place::ArchiveCopy) => "archive copy".to_string(),
            ListMark::Place(Place::Excluded) => "excluded".to_string(),
            ListMark::Hidden => "hidden".to_string(),
            ListMark::EmptyFiles(n) => format!("empty files (shown by {})", format_count(*n)),
            ListMark::NoSource => "(no path: its sources were removed)".to_string(),
        };
        let fields = [
            class.to_string(),
            original,
            becomes,
            line.path.clone(),
            mark,
        ];
        if null {
            for field in fields {
                out.push_str(&field);
                out.push('\0');
            }
        } else {
            let escaped: Vec<String> = fields.iter().map(|field| escaped(field)).collect();
            out.push_str(&escaped.join("\t"));
            out.push('\n');
        }
    }
    out
}

/// A field with `\`, a tab and a newline written `\\`, `\t` and `\n`.
fn escaped(field: &str) -> String {
    field
        .replace('\\', "\\\\")
        .replace('\t', "\\t")
        .replace('\n', "\\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::domain::fact::FactValue;
    use crate::core::domain::time::zone::UserZone;
    use crate::core::testing::*;
    use crate::facts::domain::convert::{Ack, Acknowledgments, Why};

    /// The act on a key of `values`, to number, with the removal acknowledged.
    fn act(values: &[&str], recording: bool) -> ConvertOutcome {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/r", "source", false);
        for value in values {
            let id = insert_source(&conn, root, value, None);
            insert_fact(&conn, id, "content.N", value);
        }
        let removed = values.iter().filter(|v| v.parse::<f64>().is_err()).count();
        let decision = DecisionParams {
            command: DecisionCommand::FactsConvert,
            scope: Vec::new(),
            command_line: "canon facts convert".into(),
            reason: None,
            record_enabled: recording,
            receipt_enabled: false,
            ledger_config: LedgerConfig::default(),
            zone: ReadTimesOn::of_user(&UserZone::utc()),
        };
        execute_convert(
            &mut conn,
            "N",
            TypeWord::Type(FactType::Num),
            &Acknowledgments {
                loss: None,
                removal: Some(Ack::Count(removed as u64)),
            },
            Some(&decision),
        )
        .unwrap()
    }

    /// **The result line says what the key holds now**: its type where facts remain; where the
    /// act removed every fact, that it has none and its next import sets its type anew — never
    /// a type it no longer has; then where the record is, or that recording is off.
    #[test]
    fn every_fact_removed_forgets_the_key_in_the_result_line() {
        let kept = act_lines(&act(&["12", "misc"], true), false);
        assert_eq!(kept[kept.len() - 2], "content.N is of type number.");
        assert!(kept[kept.len() - 1].starts_with("`canon trail show "));
        assert!(kept[kept.len() - 1].ends_with("` to read this conversion later"));

        let emptied = act_lines(&act(&["misc", "other"], true), false);
        assert_eq!(
            emptied[emptied.len() - 2],
            "content.N has no facts; its next import sets its type anew"
        );
        assert!(emptied.iter().all(|line| !line.contains("is of type")));

        let off = act_lines(&act(&["12"], false), true);
        assert_eq!(off.last().unwrap(), "not recorded (recording is off)");
    }

    /// **A preview that removes every fact speaks of no new values**: it says the key would be
    /// forgotten and drops the recovery sentence, the next-import line and the manifests line; one
    /// that keeps facts says a manifest places files by the new values.
    #[test]
    fn a_plan_removing_every_fact_promises_no_new_values() {
        let plan_of = |values: &[&str]| {
            let mut conn = setup_test_db();
            let root = insert_root(&conn, "/r", "source", false);
            for value in values {
                let id = insert_source(&conn, root, value, None);
                insert_fact(&conn, id, "content.N", value);
            }
            let home = tempfile::tempdir().unwrap();
            let planned =
                plan_convert(&mut conn, "N", TypeWord::Type(FactType::Num), home.path()).unwrap();
            plan_lines(&planned, None)
        };

        let emptied = plan_of(&["misc", "other"]);
        assert!(emptied.contains(
            &"  every fact of content.N would be removed; the key would be forgotten, and its \
              next import sets its type anew"
                .to_string()
        ));
        assert!(
            emptied
                .iter()
                .all(|line| !line.starts_with("  After") && !line.starts_with("  Manifests")),
            "{emptied:#?}"
        );

        let kept = plan_of(&["12", "misc"]);
        assert!(
            kept.contains(
                &"  Manifests whose naming pattern uses this key place files by the new values at \
                  their next apply."
                    .to_string()
            ),
            "{kept:#?}"
        );
    }

    /// A field holding a tab, a newline and a backslash is escaped so the line splits back into
    /// its five fields; with `-0` each field stands as it is, NUL-separated and NUL-ended.
    #[test]
    fn a_list_line_splits_back_into_its_fields() {
        let line = ListLine {
            class: Class::Removed(Why::Unreadable),
            original: Stored::Readable(FactValue::Text("a\tb\nc\\d".into())),
            becomes: None,
            path: "/r/x\ty".into(),
            mark: ListMark::Place(Place::Suspended { root_id: 2 }),
        };
        let text = list_text(std::slice::from_ref(&line), false);
        let record = text.strip_suffix('\n').unwrap();
        let fields: Vec<&str> = record.split('\t').collect();
        assert_eq!(
            fields,
            ["removed", "a\\tb\\nc\\\\d", "", "/r/x\\ty", "suspended"]
        );
        let null = list_text(&[line], true);
        assert_eq!(null, "removed\0a\tb\nc\\d\0\0/r/x\ty\0suspended\0");
    }
}
