//! Scope display for the interface layer.
//!
//! Scope resolution lives in `core::ops::scope` (behavioral policy).
//! This module provides the display functions that format scope
//! information for terminal output — stdout for reports, stderr for lists.

use std::io::Write;

use crate::core::domain::root::{DoorVerb, ParkedPath, ParkedRoot, WayBack};
use crate::core::ops::scope::{ClosedDoor, ResolvedScope};

/// Print scope header for report commands (stdout).
///
/// Always prints — shows "Label: /path" when scoped, "Label: all roots" when global.
/// For multiple paths (≤2): "Label: /path1, /path2"
/// For multiple paths (>2): one per line indented.
/// Set-aside paths follow the scope itself.
pub fn print_report_scope(handle: &mut impl Write, label: &str, scope: &ResolvedScope) {
    if scope.is_global() {
        let _ = writeln!(handle, "{label}: all roots");
    } else if scope.prefixes.len() == 1 {
        let _ = writeln!(handle, "{label}: {}", scope.prefixes[0]);
    } else if scope.prefixes.len() <= 2 {
        let _ = writeln!(handle, "{label}: {}", scope.prefixes.join(", "));
    } else {
        let _ = writeln!(handle, "{label}:");
        for p in &scope.prefixes {
            let _ = writeln!(handle, "  {p}");
        }
    }
    write_parked(handle, scope, DoorVerb::SetAside);
    write_set_asides(handle, &scope.set_aside);
}

/// Print scope header for list/data commands (stderr handle).
///
/// Only prints when scoped — "scope: /path". Silent when global, except that
/// set-aside paths are stated whenever there are any.
pub fn print_list_scope(handle: &mut impl Write, scope: &ResolvedScope) {
    if !scope.is_global() {
        if scope.prefixes.len() == 1 {
            let _ = writeln!(handle, "scope: {}", scope.prefixes[0]);
        } else {
            let _ = writeln!(handle, "scope: {}", scope.prefixes.join(", "));
        }
    }
    if let Some(line) = parked_pause(&scope.pause) {
        let _ = writeln!(handle, "{line}");
    }
    write_parked(handle, scope, DoorVerb::SetAside);
    write_set_asides(handle, &scope.set_aside);
}

/// State what the scope boundary set aside, ahead of any ceremony display —
/// for effectful commands, whose scope channel is the ceremony itself. Runs
/// before the plan and before any confirmation, so `--yes` and `--dry-run`
/// see the same statement an interactive run does.
pub fn print_scope_set_asides(scope: &ResolvedScope, verb: DoorVerb) {
    let stdout = std::io::stdout();
    let mut handle = stdout.lock();
    write_parked(&mut handle, scope, verb);
    write_set_asides(&mut handle, &scope.set_aside);
}

/// State what the scope boundary set aside on stderr, for display paths
/// that carry no scope line of their own — a compact or machine-shaped
/// stdout stays exactly what it is, and the skip is still said.
pub fn eprint_scope_set_asides(scope: &ResolvedScope) {
    let mut handle = std::io::stderr().lock();
    write_parked(&mut handle, scope, DoorVerb::SetAside);
    write_set_asides(&mut handle, &scope.set_aside);
}

/// Where a command states its scope — and therefore where it states a closed
/// door. Report commands speak on stdout, list commands and every
/// machine-shaped stdout on stderr: the channel bends so that a stream stays
/// exactly what was asked for, and the statement never goes unsaid.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DoorChannel {
    Stdout,
    Stderr,
}

/// Above this many closed roots in one statement, the lines collapse to a
/// count and the way back becomes "see them all". The sweep's cap, reused —
/// a statement that names twenty roots is not a statement anyone reads.
const PARKED_ROOTS_NAMED_CAP: usize = 3;

/// Above this many paths on one root, the paths collapse to a count. Same
/// reason, one level down.
const PARKED_PATHS_NAMED_CAP: usize = 3;

/// The closed-door statement: one grammar, spelled once, for every door.
///
/// `<root> suspended — <verb>: <what> · <way back>` — the sweep's footer
/// grammar, generalised. `<what>` is `here` at the CWD door, the asked-for
/// path otherwise, or a count above the caps. The way back is
/// `canon roots unsuspend path:<root>` and only that, built from the argv it
/// runs so the printed line and the runnable command cannot drift.
///
/// **`--global` is offered to views at the CWD door, and nowhere else.** It
/// undoes nothing — it asks a different question, which is why the sweep's
/// "way back and only that" rule does not forbid it — but at an act's door a
/// second gesture would read as a route around the refusal. The rule is
/// carried here rather than passed in, so no caller can get it wrong.
///
/// Pure composition: no I/O, no terminal shaping.
pub fn closed_door_lines(places: &[ParkedPath], verb: DoorVerb, here: bool) -> Vec<String> {
    if places.is_empty() {
        return Vec::new();
    }
    let mut groups: Vec<(&ParkedPath, Vec<&str>)> = Vec::new();
    for place in places {
        match groups
            .iter_mut()
            .find(|(first, _)| first.root.root_id == place.root.root_id)
        {
            Some((_, paths)) => paths.push(&place.path),
            None => groups.push((place, vec![&place.path])),
        }
    }

    if groups.len() > PARKED_ROOTS_NAMED_CAP {
        // Too many doors to name one by one, so the statement counts them and
        // the way back becomes "see them all" — the sweep's cap, reused.
        return vec![format!(
            "{} — {}: {} · {}",
            counted(groups.len(), "suspended root"),
            verb.label(),
            counted(places.len(), "path"),
            WayBack::list_suspended().display(),
        )];
    }

    let gesture = if here && verb == DoorVerb::SetAside {
        " or --global"
    } else {
        ""
    };
    groups
        .iter()
        .map(|(first, paths)| {
            let what = if here {
                "here".to_string()
            } else if paths.len() > PARKED_PATHS_NAMED_CAP {
                counted(paths.len(), "path")
            } else {
                paths.join(", ")
            };
            format!("{}{gesture}", first.root.door_line(verb, &what))
        })
        .collect()
}

/// `N thing` / `N things`, with the project's thousands separators.
fn counted(count: usize, noun: &str) -> String {
    let plural = if count == 1 { "" } else { "s" };
    format!(
        "{} {noun}{plural}",
        crate::core::domain::format::format_count(count)
    )
}

/// Meet the door.
///
/// The spine derives whether a place is behind a door the user closed; this
/// is where the interface answers for it. An open door hands back the scope;
/// a closed one is stated on the command's own scope channel and the run ends
/// there — **exit 1 with no `Error:` prefix**, because nothing went wrong:
/// the user closed the door, and saying so is a legitimate answer
/// (`SurveyExit::FrameRefused`'s convention, spelled once here instead of
/// once per command).
///
/// The verb is the caller's permit class — a view sets aside, an act refuses.
/// Remembering views do not come through here: they read at the parked place
/// instead, which is a different arm and not an exit.
pub fn open_door(
    door: crate::core::ops::scope::Door,
    verb: DoorVerb,
    channel: DoorChannel,
) -> ResolvedScope {
    match door {
        crate::core::ops::scope::Door::Open(resolved) => resolved,
        crate::core::ops::scope::Door::Closed(closed) => {
            match channel {
                DoorChannel::Stdout => {
                    write_closed_door(&mut std::io::stdout().lock(), &closed, verb)
                }
                DoorChannel::Stderr => {
                    write_closed_door(&mut std::io::stderr().lock(), &closed, verb)
                }
            }
            std::process::exit(1);
        }
    }
}

/// What a **remembering** view states about the doors it is reading behind:
/// each pause and its way back, and no verb — it neither set aside nor
/// refused. It read.
///
/// `None` when nothing is parked. Every door is named, because a scope can
/// reach places on several closed roots and stating one would leave the rest
/// read behind a door nobody mentioned; above the cap they collapse to a
/// count, the same way a view's set-asides do.
pub fn parked_pause(roots: &[ParkedRoot]) -> Option<String> {
    match roots.len() {
        0 => None,
        n if n > PARKED_ROOTS_NAMED_CAP => Some(format!(
            "{} · {}",
            counted(n, "suspended root"),
            WayBack::list_suspended().display()
        )),
        _ => Some(
            roots
                .iter()
                .map(ParkedRoot::pause_line)
                .collect::<Vec<_>>()
                .join(", "),
        ),
    }
}

/// State a closed door, and beside it whatever the same ask kept nothing of
/// for the other reason. Two causes, two spellings, never one standing in for
/// the other.
pub fn write_closed_door(handle: &mut impl Write, door: &ClosedDoor, verb: DoorVerb) {
    for line in closed_door_lines(&door.places, verb, door.here) {
        let _ = writeln!(handle, "{line}");
    }
    write_set_asides(handle, &door.sourceless);
}

/// State the parked places a scope proceeded past, in the set-aside position.
pub fn write_parked(handle: &mut impl Write, scope: &ResolvedScope, verb: DoorVerb) {
    for line in closed_door_lines(&scope.parked, verb, false) {
        let _ = writeln!(handle, "{line}");
    }
}

/// The one spelling of a set-aside line. A path Canon knows no sources for
/// is named and marked skipped — never dropped, never counted as run.
///
/// Takes the paths rather than a [`ResolvedScope`] because the same statement
/// is owed at two doors: the argument door, where a scope comes back as a
/// `ResolvedScope`, and the manifest door, where `cluster refresh` holds a
/// `ScopeResolution` instead. One situation, one sentence — the partition it
/// came out of is not the sentence's business.
pub fn write_set_asides(handle: &mut impl Write, set_aside: &[String]) {
    for path in set_aside {
        let _ = writeln!(handle, "no sources known at {path} — skipped");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::domain::root::ParkedRoot;

    fn place(root_id: i64, root: &str, path: &str) -> ParkedPath {
        ParkedPath {
            path: path.to_string(),
            root: ParkedRoot {
                root_id,
                root_path: root.to_string(),
            },
        }
    }

    /// One grammar, in its three `<what>` forms: `here` at the CWD door, the
    /// asked-for path when it was named, and a count when one root carries
    /// more paths than a line should list.
    #[test]
    fn the_door_line_has_one_grammar() {
        let here = closed_door_lines(
            &[place(1, "/mnt/d1", "/mnt/d1/photos")],
            DoorVerb::SetAside,
            true,
        );
        assert_eq!(
            here,
            vec![
                "/mnt/d1 suspended — set aside: here · canon roots unsuspend path:/mnt/d1 or --global"
            ]
        );

        let named = closed_door_lines(
            &[place(1, "/mnt/d1", "/mnt/d1/photos")],
            DoorVerb::Refused,
            false,
        );
        assert_eq!(
            named,
            vec![
                "/mnt/d1 suspended — refused: /mnt/d1/photos · canon roots unsuspend path:/mnt/d1"
            ]
        );

        let many: Vec<ParkedPath> = (1..=4)
            .map(|n| place(1, "/mnt/d1", &format!("/mnt/d1/{n}")))
            .collect();
        assert_eq!(
            closed_door_lines(&many, DoorVerb::SetAside, false),
            vec!["/mnt/d1 suspended — set aside: 4 paths · canon roots unsuspend path:/mnt/d1"]
        );
    }

    /// `--global` is offered to a view standing at the door and to nothing
    /// else. It undoes nothing — it asks a different question — but at an
    /// act's door a second gesture would read as a route around the refusal.
    #[test]
    fn only_a_view_at_the_cwd_door_is_offered_global() {
        let one = [place(1, "/mnt/d1", "/mnt/d1")];
        assert!(closed_door_lines(&one, DoorVerb::SetAside, true)[0].ends_with(" or --global"));
        assert!(!closed_door_lines(&one, DoorVerb::Refused, true)[0].contains("--global"));
        assert!(!closed_door_lines(&one, DoorVerb::SetAside, false)[0].contains("--global"));
    }

    /// The sweep's cap, reused: above three closed roots the statement counts
    /// them and points at the listing rather than naming each.
    #[test]
    fn above_three_roots_the_lines_collapse() {
        let places: Vec<ParkedPath> = (1..=4)
            .map(|n| place(n, &format!("/mnt/d{n}"), &format!("/mnt/d{n}/x")))
            .collect();
        assert_eq!(
            closed_door_lines(&places, DoorVerb::SetAside, false),
            vec!["4 suspended roots — set aside: 4 paths · canon roots list --suspended"]
        );

        // At the cap itself, each door is still named.
        assert_eq!(
            closed_door_lines(&places[..3], DoorVerb::SetAside, false).len(),
            3
        );
    }

    /// Several places on one closed root are one door, and are stated as one
    /// line — the root is what the way back takes.
    #[test]
    fn places_on_one_root_are_one_line() {
        let places = vec![
            place(1, "/mnt/d1", "/mnt/d1/a"),
            place(1, "/mnt/d1", "/mnt/d1/b"),
        ];
        assert_eq!(
            closed_door_lines(&places, DoorVerb::SetAside, false),
            vec![
                "/mnt/d1 suspended — set aside: /mnt/d1/a, /mnt/d1/b · canon roots unsuspend path:/mnt/d1"
            ]
        );
    }

    /// A scope that kept nothing for two reasons states both, each in its own
    /// spelling. Collapsing them would put a false cause on one of the paths.
    #[test]
    fn a_closed_door_states_the_sourceless_paths_beside_it() {
        let door = ClosedDoor {
            places: vec![place(1, "/mnt/d1", "/mnt/d1/photos")],
            sourceless: vec!["/live/2012".to_string()],
            here: false,
            auto_include_archived: false,
        };
        let mut buf = Vec::new();
        write_closed_door(&mut buf, &door, DoorVerb::Refused);
        assert_eq!(
            String::from_utf8(buf).unwrap(),
            "/mnt/d1 suspended — refused: /mnt/d1/photos · canon roots unsuspend path:/mnt/d1\nno sources known at /live/2012 — skipped\n"
        );
    }

    /// A remembering view states the pause and no verb: it read.
    #[test]
    fn a_remembering_view_states_a_pause_without_a_verb() {
        let root = ParkedRoot {
            root_id: 1,
            root_path: "/mnt/d1".to_string(),
        };
        let line = parked_pause(std::slice::from_ref(&root)).unwrap();
        assert_eq!(
            line,
            "/mnt/d1 suspended · canon roots unsuspend path:/mnt/d1"
        );
        assert!(!line.contains("set aside") && !line.contains("refused"));
        assert_eq!(parked_pause(&[]), None, "nothing parked, nothing to say");

        // Every door is named: reading behind two and stating one would leave
        // the second read behind a door nobody mentioned.
        let second = ParkedRoot {
            root_id: 2,
            root_path: "/mnt/d2".to_string(),
        };
        let both = parked_pause(&[root.clone(), second.clone()]).unwrap();
        assert!(both.contains("/mnt/d1 suspended · "), "{both}");
        assert!(both.contains("/mnt/d2 suspended · "), "{both}");

        // And above the cap they collapse, like a view's set-asides.
        let many: Vec<ParkedRoot> = (1..=4)
            .map(|n| ParkedRoot {
                root_id: n,
                root_path: format!("/mnt/d{n}"),
            })
            .collect();
        assert_eq!(
            parked_pause(&many).unwrap(),
            "4 suspended roots · canon roots list --suspended"
        );
    }

    /// The scope printers state a parked place in the set-aside position —
    /// after the scope line, before anything the command lists.
    #[test]
    fn print_report_scope_states_the_parked_places_it_proceeded_past() {
        let scope = ResolvedScope {
            prefixes: vec!["/live".to_string()],
            set_aside: Vec::new(),
            parked: vec![place(1, "/mnt/d1", "/mnt/d1/photos")],
            pause: Vec::new(),
            from_cwd: false,
            auto_include_archived: false,
        };
        let mut buf = Vec::new();
        print_report_scope(&mut buf, "Coverage", &scope);
        assert_eq!(
            String::from_utf8(buf).unwrap(),
            "Coverage: /live\n/mnt/d1 suspended — set aside: /mnt/d1/photos · canon roots unsuspend path:/mnt/d1\n"
        );
    }

    #[test]
    fn print_report_scope_scoped() {
        let scope = ResolvedScope {
            prefixes: vec!["/a/b".to_string()],
            set_aside: Vec::new(),
            parked: Vec::new(),
            pause: Vec::new(),
            from_cwd: true,
            auto_include_archived: false,
        };
        let mut buf = Vec::new();
        print_report_scope(&mut buf, "Facts", &scope);
        assert_eq!(String::from_utf8(buf).unwrap(), "Facts: /a/b\n");
    }

    #[test]
    fn print_report_scope_global() {
        let scope = ResolvedScope {
            prefixes: Vec::new(),
            set_aside: Vec::new(),
            parked: Vec::new(),
            pause: Vec::new(),
            from_cwd: false,
            auto_include_archived: false,
        };
        let mut buf = Vec::new();
        print_report_scope(&mut buf, "Facts", &scope);
        assert_eq!(String::from_utf8(buf).unwrap(), "Facts: all roots\n");
    }

    #[test]
    fn print_report_scope_two_paths() {
        let scope = ResolvedScope {
            prefixes: vec!["/a/b".to_string(), "/c/d".to_string()],
            set_aside: Vec::new(),
            parked: Vec::new(),
            pause: Vec::new(),
            from_cwd: false,
            auto_include_archived: false,
        };
        let mut buf = Vec::new();
        print_report_scope(&mut buf, "Survey", &scope);
        assert_eq!(String::from_utf8(buf).unwrap(), "Survey: /a/b, /c/d\n");
    }

    #[test]
    fn print_report_scope_many_paths() {
        let scope = ResolvedScope {
            prefixes: vec!["/a".to_string(), "/b".to_string(), "/c".to_string()],
            set_aside: Vec::new(),
            parked: Vec::new(),
            pause: Vec::new(),
            from_cwd: false,
            auto_include_archived: false,
        };
        let mut buf = Vec::new();
        print_report_scope(&mut buf, "Facts", &scope);
        assert_eq!(
            String::from_utf8(buf).unwrap(),
            "Facts:\n  /a\n  /b\n  /c\n"
        );
    }

    #[test]
    fn print_report_scope_states_set_asides_after_the_scope_line() {
        let scope = ResolvedScope {
            prefixes: vec!["/a/b".to_string()],
            set_aside: vec!["/a/empty".to_string(), "/a/also-empty".to_string()],
            parked: Vec::new(),
            pause: Vec::new(),
            from_cwd: false,
            auto_include_archived: false,
        };
        let mut buf = Vec::new();
        print_report_scope(&mut buf, "Facts", &scope);
        assert_eq!(
            String::from_utf8(buf).unwrap(),
            "Facts: /a/b\n\
             no sources known at /a/empty — skipped\n\
             no sources known at /a/also-empty — skipped\n"
        );
    }

    #[test]
    fn print_list_scope_states_set_asides_on_its_handle() {
        let scope = ResolvedScope {
            prefixes: vec!["/a/b".to_string()],
            set_aside: vec!["/a/empty".to_string()],
            parked: Vec::new(),
            pause: Vec::new(),
            from_cwd: false,
            auto_include_archived: false,
        };
        let mut buf = Vec::new();
        print_list_scope(&mut buf, &scope);
        assert_eq!(
            String::from_utf8(buf).unwrap(),
            "scope: /a/b\nno sources known at /a/empty — skipped\n"
        );
    }

    /// A global scope says nothing about itself, but a set-aside is still a
    /// difference between what was asked and what ran.
    #[test]
    fn print_list_scope_is_silent_when_global_and_nothing_was_set_aside() {
        let scope = ResolvedScope {
            prefixes: Vec::new(),
            set_aside: Vec::new(),
            parked: Vec::new(),
            pause: Vec::new(),
            from_cwd: false,
            auto_include_archived: false,
        };
        let mut buf = Vec::new();
        print_list_scope(&mut buf, &scope);
        assert!(buf.is_empty());
    }
}
