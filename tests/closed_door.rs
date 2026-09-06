//! The closed-door census, run against the built binary.
//!
//! Suspension is the user's own act of closing the door on a root, and the
//! registry permits exactly four things behind it — opening, seeing,
//! remembering, testifying. Everything else meets the closed default. This
//! file is that conformance list, observed rather than read: every row is a
//! real invocation of `canon`, and its cell is an assertion on what came back
//! on which stream, with which exit code.
//!
//! **Why the binary and not the library.** The defects this epic repairs were
//! never visible from inside a function. Standing in a closed root widened
//! seven commands to the whole universe; naming one answered a false empty on
//! a machine stream; a refusal wore an `Error:` prefix that told the user
//! something had gone wrong when nothing had. Each of those is a property of
//! *what the process printed and returned*, and only a process can be asked.
//!
//! **What is asserted, and what is deliberately not.** Structure: the root
//! path is present, the way back is present, the exit code, the stream. The
//! exact wording lives in one place in the source and is free to improve; a
//! census that pinned it would fail every time a sentence got better, which is
//! the opposite of what a regression net is for. The fragments that carry
//! meaning rather than phrasing — `suspended`, the verb, the way back's
//! command — are asserted, because those are the grammar and not the wording.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

/// One fixture world: three roots and a canon home, all under a temp dir that
/// cleans itself up.
///
/// Deliberately built by driving the binary rather than by writing rows: a
/// census that seeded its own database would be asserting against a world
/// Canon never made.
struct Fixture {
    dir: tempfile::TempDir,
}

struct Run {
    stdout: String,
    stderr: String,
    code: i32,
}

impl Run {
    fn ok(&self) -> &Self {
        assert_eq!(
            self.code, 0,
            "expected success\n{}{}",
            self.stdout, self.stderr
        );
        self
    }
}

impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("temp dir");
        let fx = Fixture { dir };
        for sub in ["home", "src/sub", "arc/media", "live"] {
            std::fs::create_dir_all(fx.path(sub)).unwrap();
        }
        write(fx.path("src/a.jpg"), "alpha");
        write(fx.path("src/b.jpg"), "bravo");
        write(fx.path("src/sub/c.jpg"), "charlie");
        write(fx.path("arc/media/a.jpg"), "alpha");
        write(fx.path("arc/media/b.jpg"), "bravo");
        write(fx.path("live/d.jpg"), "delta");
        write(fx.path("live/a-copy.jpg"), "alpha");

        for (role, root) in [("source", "src"), ("archive", "arc"), ("source", "live")] {
            fx.run(&["scan", "--add", "--role", role, &fx.path_str(root)])
                .ok();
        }
        fx.run(&["note", &fx.path_str("src/sub"), "-m", "a thought about sub"])
            .ok();
        fx
    }

    fn path(&self, rel: &str) -> PathBuf {
        self.dir.path().join(rel)
    }

    /// The canonical form, which is what Canon stores and therefore what every
    /// statement names. On macOS the temp dir is reached through a symlink,
    /// so the un-canonicalized form would never appear in the output.
    fn path_str(&self, rel: &str) -> String {
        let p = self.path(rel);
        std::fs::canonicalize(&p)
            .unwrap_or(p)
            .to_string_lossy()
            .into_owned()
    }

    fn run(&self, args: &[&str]) -> Run {
        self.run_in(self.dir.path(), args)
    }

    fn run_in(&self, cwd: &Path, args: &[&str]) -> Run {
        let out: Output = Command::new(env!("CARGO_BIN_EXE_canon"))
            .args(args)
            .current_dir(cwd)
            .env("CANON_HOME", self.path("home"))
            .env("EDITOR", "true")
            .output()
            .expect("canon must run");
        Run {
            stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
            code: out.status.code().unwrap_or(-1),
        }
    }

    fn park(&self, rel: &str) {
        self.run(&["roots", "suspend", &format!("path:{}", self.path_str(rel))])
            .ok();
    }
}

fn write(path: PathBuf, body: &str) {
    std::fs::write(path, body).unwrap();
}

/// The one grammar, checked structurally: the door names its root, says
/// `suspended`, carries the surface's verb, and ends in a way back that runs.
fn assert_door_line(line: &str, root: &str, verb: &str) {
    assert!(line.contains(root), "the door names its root: {line}");
    assert!(line.contains("suspended"), "and says so: {line}");
    assert!(
        line.contains(&format!("— {verb}:")),
        "with the surface's own verb ({verb}): {line}"
    );
    assert!(
        line.contains(&format!("canon roots unsuspend path:{root}")),
        "and the way back, which is that and only that: {line}"
    );
}

/// **S1 — standing in a closed root, no path argument.**
///
/// Every one of these used to answer about the whole universe, silently. The
/// pause is now stated on the command's own scope channel and nothing is
/// listed; exit 1, and no `Error:` prefix, because nothing went wrong.
#[test]
fn a_view_standing_in_a_parked_root_states_the_door_and_lists_nothing() {
    let fx = Fixture::new();
    fx.park("src");
    let root = fx.path_str("src");
    let here = fx.path("src/sub");

    // Report commands state scope on stdout; list commands on stderr.
    for (cmd, on_stdout) in [
        ("facts", true),
        ("coverage", true),
        ("survey", true),
        ("ls", false),
        ("worklist", false),
    ] {
        let run = fx.run_in(&here, &[cmd]);
        assert_eq!(run.code, 1, "{cmd}: a whole-subject door exits 1");
        let (spoken, silent) = if on_stdout {
            (&run.stdout, &run.stderr)
        } else {
            (&run.stderr, &run.stdout)
        };
        assert_door_line(
            spoken.lines().next().unwrap_or_default(),
            &root,
            "set aside",
        );
        assert!(
            spoken.contains("here"),
            "{cmd}: the CWD door says `here`, not a path: {spoken}"
        );
        assert!(
            !spoken.contains("Error:"),
            "{cmd}: a refusal that is an answer wears no `Error:`: {spoken}"
        );
        assert!(
            silent.trim().is_empty(),
            "{cmd}: the other stream carries nothing: {silent}"
        );
    }
}

/// A machine stream stays exactly what was asked for: nothing on stdout, the
/// statement on stderr, and the exit code carrying the refusal. A consumer
/// that read an empty listing as "nothing here" would be wrong, and the code
/// is what stops it.
#[test]
fn a_machine_stream_at_a_parked_root_is_empty_and_says_so_on_stderr() {
    let fx = Fixture::new();
    fx.park("src");
    let here = fx.path("src/sub");

    for args in [vec!["worklist"], vec!["coverage", "--compact"]] {
        let run = fx.run_in(&here, &args);
        assert_eq!(run.code, 1, "{args:?}");
        assert!(run.stdout.is_empty(), "{args:?}: stdout: {}", run.stdout);
        assert_door_line(
            run.stderr.lines().next().unwrap_or_default(),
            &fx.path_str("src"),
            "set aside",
        );
    }
}

/// `--global` asks a different question and is answered as asked — it is the
/// one explicit way to widen, and the door does not take it away.
#[test]
fn global_from_a_parked_root_still_answers_globally() {
    let fx = Fixture::new();
    fx.park("src");
    let run = fx.run_in(&fx.path("src/sub"), &["ls", "--global"]);
    run.ok();
    assert!(run.stdout.contains("a-copy.jpg"), "{}", run.stdout);
}

/// **S1 — the acts.** An act behind a closed door is refused by name, and
/// `--yes` never gets past it: the door precedes the plan and the
/// confirmation by position, which is the whole reason a silent widening was
/// dangerous rather than merely wrong.
#[test]
fn an_act_standing_in_a_parked_root_is_refused_and_writes_nothing() {
    let fx = Fixture::new();
    fx.park("src");
    let root = fx.path_str("src");
    let here = fx.path("src/sub");
    let before = fx.run(&["trail", "--global"]).stdout;

    let acts: Vec<Vec<String>> = vec![
        vec!["exclude", "set", "--where", "source.ext = 'jpg'", "--yes"],
        vec!["exclude", "clear", "--where", "source.ext = 'jpg'", "--yes"],
        vec!["note", "-m", "a thought behind the door"],
        vec!["scan", "."],
    ]
    .into_iter()
    .map(|v| v.into_iter().map(String::from).collect())
    .collect();

    for act in &acts {
        let argv: Vec<&str> = act.iter().map(String::as_str).collect();
        let run = fx.run_in(&here, &argv);
        assert_eq!(run.code, 1, "{argv:?} must be refused");
        let spoken = format!("{}{}", run.stdout, run.stderr);
        assert_door_line(spoken.lines().next().unwrap_or_default(), &root, "refused");
        assert!(!spoken.contains("Error:"), "{argv:?}: {spoken}");
    }

    // `cluster generate` used to record its widened run as a **global** act.
    let run = fx.run_in(
        &here,
        &[
            "cluster",
            "generate",
            "--dest",
            &fx.path_str("arc"),
            "--where",
            "source.ext = 'jpg'",
            "--output",
            &fx.path("m.toml").to_string_lossy(),
        ],
    );
    assert_eq!(run.code, 1);
    assert_door_line(
        run.stdout.lines().next().unwrap_or_default(),
        &root,
        "refused",
    );
    assert!(!fx.path("m.toml").exists(), "no manifest is written");

    assert_eq!(
        before,
        fx.run(&["trail", "--global"]).stdout,
        "a refused act leaves no decision row behind"
    );
}

/// **S1 — remembering.** The trail, crossings and the note view read at the
/// parked place, with the pause stated once in the header and no verb: they
/// read, they did not set aside or refuse.
#[test]
fn remembering_reads_at_a_parked_root_with_the_pause_stated() {
    let fx = Fixture::new();
    fx.park("src");
    let root = fx.path_str("src");
    let here = fx.path("src/sub");

    let trail = fx.run_in(&here, &["trail"]);
    trail.ok();
    let header = trail.stdout.lines().next().unwrap_or_default();
    assert!(header.starts_with("Decision trail: "), "{header}");
    assert!(
        header.contains(&fx.path_str("src/sub")),
        "reads here: {header}"
    );
    assert!(
        !header.contains("all roots"),
        "never the universe: {header}"
    );
    assert!(header.contains(&format!("{root} suspended · ")), "{header}");
    assert!(
        header.contains(&format!("canon roots unsuspend path:{root}")),
        "{header}"
    );

    // `crossings` used to be the one surface that noticed — and it refused,
    // answering differently from the same place named on the command line.
    let crossings = fx.run_in(&here, &["trail", "crossings"]);
    crossings.ok();
    assert!(
        crossings.stdout.starts_with("Crossings: "),
        "{}",
        crossings.stdout
    );
    assert!(
        crossings.stdout.contains("suspended · "),
        "{}",
        crossings.stdout
    );

    // The note view reads here rather than falling through to the global list.
    let note = fx.run_in(&here, &["note"]);
    note.ok();
    assert!(
        note.stdout.contains("a thought about sub"),
        "{}",
        note.stdout
    );
    assert!(note.stderr.contains("suspended · "), "{}", note.stderr);
}

/// **S2 — naming the parked place.** The same place must answer the same way
/// named as it does stood in, and a named path is what the statement carries
/// in place of `here`.
#[test]
fn naming_a_parked_place_answers_the_way_standing_in_it_does() {
    let fx = Fixture::new();
    fx.park("src");
    let root = fx.path_str("src");
    let named = fx.path_str("src/sub");

    for (cmd, on_stdout) in [("facts", true), ("coverage", true), ("ls", false)] {
        let run = fx.run(&[cmd, &named]);
        assert_eq!(run.code, 1, "{cmd}");
        let spoken = if on_stdout { &run.stdout } else { &run.stderr };
        let line = spoken.lines().next().unwrap_or_default();
        assert_door_line(line, &root, "set aside");
        assert!(line.contains(&named), "the path as asked for: {line}");
        assert!(!line.contains("here"), "{line}");
    }

    // Remembering answers the same at both doors — the trail's own rule, now
    // holding through the door.
    let named_trail = fx.run(&["trail", "crossings", &named]);
    let stood_trail = fx.run_in(&fx.path("src/sub"), &["trail", "crossings"]);
    named_trail.ok();
    stood_trail.ok();
    assert_eq!(
        named_trail.stdout.lines().next(),
        stood_trail.stdout.lines().next(),
        "the same place, the same answer"
    );
}

/// A closed door among live keepers is a set-aside, not a refusal: the rest
/// runs, exit 0, and the door is stated in the set-aside position — before
/// anything the command lists.
#[test]
fn a_parked_path_beside_a_live_one_is_set_aside_and_stated_first() {
    let fx = Fixture::new();
    fx.park("src");
    let run = fx.run(&["ls", &fx.path_str("src"), &fx.path_str("live")]);
    run.ok();
    assert!(
        run.stdout.contains("a-copy.jpg"),
        "the keeper ran: {}",
        run.stdout
    );
    let stated = run
        .stderr
        .lines()
        .find(|l| l.contains("suspended"))
        .unwrap_or_default();
    assert_door_line(stated, &fx.path_str("src"), "set aside");
}

/// **The roots door.** `rm` is an act and meets the closed default; `comment`
/// is the label on the door and stays permitted. Both used to say
/// "No root for path" about a root that plainly exists — the false cause this
/// story retires.
#[test]
fn the_roots_door_names_a_parked_root_rather_than_calling_it_absent() {
    let fx = Fixture::new();
    fx.park("src");
    let root = fx.path_str("src");

    let rm = fx.run(&["roots", "rm", &format!("path:{root}"), "--yes"]);
    assert_eq!(rm.code, 1);
    let spoken = format!("{}{}", rm.stdout, rm.stderr);
    assert_door_line(spoken.lines().next().unwrap_or_default(), &root, "refused");
    assert!(!spoken.contains("No root for path"), "{spoken}");

    let comment = fx.run(&["roots", "comment", &format!("path:{root}"), "a label"]);
    comment.ok();
    assert!(
        !comment.stdout.contains("No root for path"),
        "{}",
        comment.stdout
    );
}

/// **The load-bearing locations.** `compare`'s two sides and
/// `survey --other`'s reference cannot be set aside without changing the
/// question, so a closed door on any of them refuses the whole ask — never a
/// false zero, never a residual list omitting what stands there.
#[test]
fn a_load_bearing_location_behind_a_door_refuses_the_whole_ask() {
    let fx = Fixture::new();
    fx.park("src");
    let root = fx.path_str("src");

    for args in [
        vec![
            "compare".to_string(),
            fx.path_str("src"),
            fx.path_str("live"),
        ],
        vec![
            "compare".to_string(),
            fx.path_str("live"),
            fx.path_str("src"),
        ],
        vec![
            "survey".to_string(),
            fx.path_str("live"),
            "--other".to_string(),
            fx.path_str("src"),
        ],
    ] {
        let argv: Vec<&str> = args.iter().map(String::as_str).collect();
        let run = fx.run(&argv);
        assert_eq!(run.code, 1, "{argv:?}");
        let spoken = format!("{}{}", run.stdout, run.stderr);
        assert_door_line(spoken.lines().next().unwrap_or_default(), &root, "refused");
        assert!(!spoken.contains("Error:"), "{argv:?}: {spoken}");
        assert!(
            !spoken.contains("0 of"),
            "{argv:?}: never a false count: {spoken}"
        );
    }
}

/// Above three closed doors in one statement the lines collapse to a count,
/// and the way back becomes the listing — the sweep's cap, reused.
#[test]
fn above_three_parked_roots_the_statement_collapses_to_a_count() {
    let fx = Fixture::new();
    let mut named = vec!["ls".to_string()];
    for n in 1..=4 {
        let rel = format!("p{n}");
        std::fs::create_dir_all(fx.path(&rel)).unwrap();
        write(fx.path(&format!("{rel}/f.jpg")), "x");
        fx.run(&["scan", "--add", "--role", "source", &fx.path_str(&rel)])
            .ok();
        fx.park(&rel);
        named.push(fx.path_str(&rel));
    }
    let argv: Vec<&str> = named.iter().map(String::as_str).collect();
    let run = fx.run(&argv);
    assert_eq!(run.code, 1);
    let line = run.stderr.lines().next().unwrap_or_default();
    assert!(
        line.starts_with("4 suspended roots — set aside: 4 paths · "),
        "{line}"
    );
    assert!(line.ends_with("canon roots list --suspended"), "{line}");
}

/// **An unknown place behind a closed door is still said to be unknown** — and
/// names the door beside it. The gate runs there: it reads sources, notes,
/// extractions and decisions, and a door hides none of them, so a typo behind
/// one must not render a plausible empty view of a place Canon has never heard
/// of.
#[test]
fn a_place_with_no_history_behind_a_door_is_stated_not_rendered() {
    let fx = Fixture::new();
    fx.park("src");
    let root = fx.path_str("src");

    let run = fx.run(&["trail", &format!("{root}/nosuchdir")]);
    assert_eq!(
        run.code, 1,
        "stdout: {}\nstderr: {}",
        run.stdout, run.stderr
    );
    assert!(
        run.stdout.is_empty(),
        "nothing is rendered for it: {}",
        run.stdout
    );
    let line = run.stderr.lines().next().unwrap_or_default();
    assert!(line.starts_with("No history known at "), "{line}");
    assert!(line.contains(&format!("{root} suspended · ")), "{line}");
    assert!(
        line.contains(&format!("canon roots unsuspend path:{root}")),
        "{line}"
    );

    // A place that does hold history reads, as remembering must.
    fx.run(&["trail", &fx.path_str("src/sub")]).ok();
}

/// A path named beside a parked one, which Canon has no sources for, is still
/// stated. Two causes in one ask, and one standing in for the other is the
/// false-cause family this epic exists to end.
#[test]
fn a_sourceless_path_beside_a_parked_one_is_still_stated() {
    let fx = Fixture::new();
    std::fs::create_dir_all(fx.path("live/empty")).unwrap();
    fx.park("src");

    let run = fx.run(&["trail", &fx.path_str("src"), &fx.path_str("live/empty")]);
    let spoken = format!("{}{}", run.stdout, run.stderr);
    assert!(
        spoken.contains(&fx.path_str("live/empty")),
        "the sourceless companion is named, not dropped: {spoken}"
    );
}

/// Reading behind two closed doors names both. Stating the first would leave
/// the second read behind a door nobody mentioned.
#[test]
fn a_reading_behind_two_doors_names_both() {
    let fx = Fixture::new();
    fx.park("src");
    fx.park("live");

    let run = fx.run(&["trail", &fx.path_str("src"), &fx.path_str("live")]);
    run.ok();
    let header = run.stdout.lines().next().unwrap_or_default();
    assert!(
        header.contains(&format!("{} suspended · ", fx.path_str("src"))),
        "{header}"
    );
    assert!(
        header.contains(&format!("{} suspended · ", fx.path_str("live"))),
        "{header}"
    );
}

/// **A destination inside a parked archive names the door.** This was the last
/// active-only lookup: `cluster generate --dest` answered "not inside any
/// registered archive root" about an archive root that plainly is registered.
#[test]
fn a_destination_in_a_parked_archive_names_the_door() {
    let fx = Fixture::new();
    fx.park("arc");
    let root = fx.path_str("arc");

    let run = fx.run(&[
        "cluster",
        "generate",
        &fx.path_str("live"),
        "--dest",
        &format!("{root}/out"),
        "--where",
        "source.ext = 'jpg'",
        "--output",
        &fx.path("m.toml").to_string_lossy(),
    ]);
    assert_eq!(run.code, 1);
    let spoken = format!("{}{}", run.stdout, run.stderr);
    assert_door_line(spoken.lines().next().unwrap_or_default(), &root, "refused");
    assert!(!spoken.contains("not inside any registered"), "{spoken}");
    assert!(!spoken.contains("Error:"), "{spoken}");
    assert!(!fx.path("m.toml").exists());
}

/// **A parked place named beside a live one is read, on both branches.**
///
/// Remembering has no "set aside" register: the permit says knowledge Canon
/// already holds still reads, and a live keeper standing beside a parked place
/// changes nothing about that. Setting it aside instead made the same place
/// answer differently depending on what was named next to it — against the
/// trail's own rule that a place answers the same way named or stood in.
///
/// What survives from this row's first shape is the part that was always
/// true: **the human and `--jsonl` branches must agree about what was asked.**
/// They have disagreed twice in this story's history, once in each direction.
#[test]
fn a_parked_place_named_beside_a_live_one_is_read_on_both_branches() {
    let fx = Fixture::new();
    fx.park("src");
    let parked = fx.path_str("src");

    // What the parked place answers on its own is the standard both branches
    // are held to below.
    let alone = fx.run(&["trail", &parked]);
    alone.ok();
    // Compared by decision id, not by rendered row: the scope column is
    // relative when one root is in view and absolute when two are, which is a
    // difference in how a row is drawn rather than in which rows there are.
    let ids_in = |text: &str| -> Vec<String> {
        text.lines()
            .filter_map(|l| l.strip_prefix('#'))
            .filter_map(|l| l.split_whitespace().next())
            .map(str::to_string)
            .collect()
    };
    let its_own = ids_in(&alone.stdout);
    assert!(!its_own.is_empty(), "the fixture must give it a story");

    let human = fx.run(&["trail", &parked, &fx.path_str("live")]);
    human.ok();
    let mixed = ids_in(&human.stdout);
    for id in &its_own {
        assert!(
            mixed.contains(id),
            "decision #{id} is read at the parked place alone and must survive a \
             live sibling:\n{}",
            human.stdout
        );
    }
    assert!(
        human.stdout.contains(&format!("{parked} suspended · ")),
        "with the pause in the header, not a set-aside line:\n{}",
        human.stdout
    );
    assert!(
        !human.stdout.contains("— set aside:") && !human.stderr.contains("— set aside:"),
        "a place the trail reads is never also announced as skipped"
    );

    // The two branches agree: the machine one carries the parked place's
    // decisions on stdout and the pause on stderr.
    let machine = fx.run(&["trail", "--jsonl", &parked, &fx.path_str("live")]);
    machine.ok();
    assert!(
        machine.stdout.contains("\"command\":\"roots_suspend\""),
        "the parked place's own decisions reach the stream:\n{}",
        machine.stdout
    );
    assert!(
        machine.stderr.contains(&format!("{parked} suspended · ")),
        "and the pause is on stderr:\n{}",
        machine.stderr
    );

    // `crossings` reaches the same door through the same `open_scope`, and it
    // is the one surface in this subsystem whose own suspended arm was retired
    // this story — so it is held to the mixed door too, not only to the CWD
    // and named-alone ones.
    let crossings = fx.run(&["trail", "crossings", &parked, &fx.path_str("live")]);
    crossings.ok();
    assert!(
        crossings
            .stdout
            .lines()
            .next()
            .unwrap_or_default()
            .contains(&format!("{parked} suspended · ")),
        "crossings states the door it reads behind:\n{}",
        crossings.stdout
    );
    assert!(
        !crossings.stdout.contains("— set aside:") && !crossings.stderr.contains("— set aside:"),
        "and never announces as skipped a place it reads"
    );
}

/// A parked **root top** named beside a live path keeps the root-top
/// exemption: a root Canon has been told about is a place it knows by
/// definition, whichever arm the path arrived through. Exempting it only when
/// it is named alone would make the same root answer differently beside a
/// sibling — the shape this whole conjugation exists to remove.
///
/// Run with recording off, which is where the gap is reachable: with recording
/// on, `scan --add` leaves a decision scoped at the root and the gate reads it
/// as evidence, so the root top survives for a reason that has nothing to do
/// with the exemption.
#[test]
fn a_parked_root_top_beside_a_live_path_keeps_its_exemption() {
    let fx = Fixture::new();
    std::fs::write(
        fx.path("home/config.toml"),
        "[ledger]\nrecording = \"Off\"\n",
    )
    .unwrap();
    std::fs::create_dir_all(fx.path("bare")).unwrap();
    fx.run(&["scan", "--add", "--role", "source", &fx.path_str("bare")])
        .ok();
    fx.park("bare");
    let bare = fx.path_str("bare");

    let alone = fx.run(&["trail", &bare]);
    let beside = fx.run(&["trail", &bare, &fx.path_str("live")]);
    alone.ok();
    beside.ok();
    for run in [&alone, &beside] {
        assert!(
            run.stdout.contains(&format!("{bare} suspended · ")),
            "the root top is in the view and states its door:\n{}",
            run.stdout
        );
    }
}

#[test]
fn a_reading_behind_a_door_keeps_the_order_it_was_asked_in() {
    let fx = Fixture::new();
    fx.park("src");
    let root = fx.path_str("src");
    let sub = fx.path_str("src/sub");

    let header = |a: &str, b: &str| -> String {
        let run = fx.run(&["trail", a, b]);
        run.ok();
        run.stdout.lines().next().unwrap_or_default().to_string()
    };

    // The root's own top is exempt from the gate and its subdirectory is not,
    // so the two take different paths through it and must still come back in
    // the order they were named.
    assert!(
        header(&sub, &root).starts_with(&format!("Decision trail: {sub}, {root} ")),
        "{}",
        header(&sub, &root)
    );
    assert!(
        header(&root, &sub).starts_with(&format!("Decision trail: {root}, {sub} ")),
        "{}",
        header(&root, &sub)
    );
}

/// A machine-shaped survey states what it set aside even when the selection
/// comes back empty. The header is suppressed because stdout is a stream, not
/// because there is nothing to say: what was set aside is a difference between
/// what was asked and what ran, and an empty result is unrelated to it.
#[test]
fn a_machine_survey_states_its_set_asides_even_when_nothing_matched() {
    let fx = Fixture::new();
    fx.park("src");
    let root = fx.path_str("src");

    let run = fx.run(&[
        "survey",
        "--detail",
        "unique",
        "--null",
        "--where",
        "source.ext = 'nosuchext'",
        &fx.path_str("live"),
        &root,
    ]);
    assert!(
        run.stdout.is_empty(),
        "the stream stays a stream: {}",
        run.stdout
    );
    assert!(
        run.stderr
            .contains(&format!("{root} suspended — set aside:")),
        "an empty result is not a reason to go silent: {:?}",
        run.stderr
    );
}
