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

    /// The one surface that takes its subjects on stdin rather than as
    /// arguments — `import-facts`, whose door is per record.
    fn run_with_stdin(&self, args: &[&str], input: &str) -> Run {
        use std::io::Write as _;
        let mut child = Command::new(env!("CARGO_BIN_EXE_canon"))
            .args(args)
            .current_dir(self.dir.path())
            .env("CANON_HOME", self.path("home"))
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("canon must run");
        child
            .stdin
            .take()
            .expect("stdin")
            .write_all(input.as_bytes())
            .unwrap();
        let out = child.wait_with_output().expect("canon must finish");
        Run {
            stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
            code: out.status.code().unwrap_or(-1),
        }
    }

    /// A manifest and its lock, generated from `live` into `arc` while every
    /// door is still open — the state the manifest door's rows start from.
    fn manifest(&self) -> PathBuf {
        let manifest = self.path("m.toml");
        self.run(&[
            "cluster",
            "generate",
            &self.path_str("live"),
            "--dest",
            &format!("{}/out", self.path_str("arc")),
            "--where",
            "source.ext = 'jpg'",
            "--output",
            &manifest.to_string_lossy(),
        ])
        .ok();
        manifest
    }

    /// A manifest with **no recorded scope** — generated globally, so the only
    /// way it can reach behind a door is through its lock's own entries.
    fn global_manifest(&self) -> PathBuf {
        let manifest = self.path("g.toml");
        self.run(&[
            "cluster",
            "generate",
            "--global",
            "--dest",
            &format!("{}/out", self.path_str("arc")),
            "--where",
            "source.ext = 'jpg'",
            "--allow",
            "archived,duplicates",
            "--output",
            &manifest.to_string_lossy(),
        ])
        .ok();
        manifest
    }

    /// The id the index gave a source, for the record-shaped surfaces that
    /// name one.
    fn source_id(&self, rel: &str) -> i64 {
        let run = self.run(&["worklist", "--global"]);
        run.ok();
        let needle = self.path_str(rel);
        for line in run.stdout.lines() {
            let v: serde_json::Value = serde_json::from_str(line).expect("jsonl");
            if v["path"].as_str() == Some(needle.as_str()) {
                return v["source_id"].as_i64().expect("source_id");
            }
        }
        panic!("no source at {needle} in the worklist");
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

// ===========================================================================
// S3 — the manifest door, and the acts reached through a record
// ===========================================================================

/// **A refresh through a closed door leaves the lock exactly where it was.**
///
/// This was the epic's one destructive silence: a manifest whose root had been
/// parked refreshed to "No sources matched the query" and *removed its lock
/// file* — the work of building the selection lost, with a false cause printed
/// over it. The lock's bytes are compared before and after, which is what
/// fails if the guard ever moves below the selection.
#[test]
fn a_refresh_through_a_parked_root_leaves_the_lock_standing() {
    let fx = Fixture::new();
    let manifest = fx.manifest();
    let lock = manifest.with_extension("lock");
    let before = std::fs::read(&lock).expect("the lock exists before");
    let manifest_before = std::fs::read(&manifest).unwrap();
    fx.park("live");
    let root = fx.path_str("live");

    let run = fx.run(&["cluster", "refresh", &manifest.to_string_lossy()]);

    assert_eq!(run.code, 1, "a door refuses: {}{}", run.stdout, run.stderr);
    let spoken = format!("{}{}", run.stdout, run.stderr);
    assert_door_line(spoken.lines().next().unwrap_or_default(), &root, "refused");
    assert!(!spoken.contains("Error:"), "{spoken}");
    assert!(
        !spoken.contains("No sources matched"),
        "never the false cause: {spoken}"
    );
    assert!(
        spoken.contains("unchanged"),
        "the refusal says what it left alone: {spoken}"
    );
    assert_eq!(
        before,
        std::fs::read(&lock).unwrap(),
        "the lock is byte-identical"
    );
    assert_eq!(manifest_before, std::fs::read(&manifest).unwrap());
}

/// **The scope refuses on its own, with every lock entry standing in the
/// open.** A manifest scoped across two roots whose entries all come from one
/// of them: park the *other*, and nothing the lock holds is behind a door —
/// only the declaration is. The refusal still fires, once, and the lock is
/// untouched.
///
/// This is the shape that separates the two causes, which no fixture where the
/// scope and the entries share a root can. Its mirror is
/// `an_apply_invitation_stands_where_the_door_is_not_its_own`, where the same
/// world lets the apply through.
#[test]
fn a_parked_scope_line_refuses_a_refresh_whose_entries_stand_in_the_open() {
    let fx = Fixture::new();
    let manifest = fx.path("two-roots.toml");
    // Only `live` holds a file named d.jpg, so every lock entry stands there
    // and `src` — named in the scope — contributes none.
    fx.run(&[
        "cluster",
        "generate",
        &fx.path_str("src"),
        &fx.path_str("live"),
        "--dest",
        &format!("{}/out", fx.path_str("arc")),
        "--where",
        "filename = 'd.jpg'",
        "--output",
        &manifest.to_string_lossy(),
    ])
    .ok();
    let lock = manifest.with_extension("lock");
    let before = std::fs::read(&lock).expect("the lock exists before");
    fx.park("src");
    let root = fx.path_str("src");

    let run = fx.run(&["cluster", "refresh", &manifest.to_string_lossy()]);

    assert_eq!(run.code, 1, "{}{}", run.stdout, run.stderr);
    let spoken = format!("{}{}", run.stdout, run.stderr);
    let doors: Vec<&str> = spoken
        .lines()
        .filter(|l| l.contains(&format!("{root} suspended")))
        .collect();
    assert_eq!(doors.len(), 1, "one door, one line: {spoken}");
    assert_door_line(doors[0], &root, "refused");
    // Not decorative: `parked_doors` composes place reaches only, so this
    // fails the day a lock cause is wired back into the refresh's door —
    // which is the defect the repair removed.
    assert!(
        !spoken.contains("lock entr") && !spoken.contains("origin of"),
        "nothing in the lock is behind this door: {spoken}"
    );
    assert_eq!(before, std::fs::read(&lock).unwrap(), "the lock stands");
}

/// **A status behind a door states it, and points at no act that door turns
/// away.** Here both ends are exercised one at a time, and each is a door
/// `apply` meets — so the invitation is withheld and the way back, stated with
/// the door itself, is what is left. Which steps a door withholds is the
/// separate question `a_status_withholds_only_the_steps_its_own_door_turns_away`
/// asks; this row asks that the door is *stated*, once per root, at each end.
#[test]
fn a_status_behind_a_door_states_it_and_names_no_apply() {
    let fx = Fixture::new();
    let manifest = fx.manifest();

    // The origin end.
    fx.park("live");
    let source_root = fx.path_str("live");
    let run = fx.run(&["cluster", "status", &manifest.to_string_lossy()]);
    run.ok();
    assert!(
        run.stdout.contains(&format!("{source_root} suspended")),
        "the entries' door is stated: {}",
        run.stdout
    );
    assert_eq!(
        run.stdout
            .lines()
            .filter(|l| l.contains(&format!("{source_root} suspended")))
            .count(),
        1,
        "one door, one line — this manifest reaches it through its scope and \
         its lock alike: {}",
        run.stdout
    );
    assert!(
        run.stdout.contains("1 pending"),
        "the fixture must have something to invite an apply for — every status \
         prints the word `pending`, so the count is what makes this a guard: {}",
        run.stdout
    );
    assert!(!run.stdout.contains("To apply:"), "{}", run.stdout);
    assert!(!run.stdout.contains("canon apply"), "{}", run.stdout);

    // And the destination end, on its own.
    fx.run(&["roots", "unsuspend", &format!("path:{source_root}")])
        .ok();
    fx.park("arc");
    let dest_root = fx.path_str("arc");
    let run = fx.run(&["cluster", "status", &manifest.to_string_lossy()]);
    run.ok();
    assert!(
        run.stdout.contains(&format!("{dest_root} suspended")),
        "{}",
        run.stdout
    );
    assert!(!run.stdout.contains("To apply:"), "{}", run.stdout);
}

/// **A lock entry behind a door is `apply`'s to refuse, and a refresh simply
/// rebuilds without it.**
///
/// A record is not a naming. The lock is Canon's snapshot of where sources
/// stood when the selection was last made; a refresh is asking again, and the
/// open world it asks about no longer holds the parked root. So the rebuild
/// leaves those entries out and refuses nothing — while `apply`, which would
/// carry them across, still refuses, and the report says so in apply's own
/// conditional and points at the rebuild that clears them.
///
/// Refusing the refresh here would leave a user with a lock they can neither
/// apply nor rebuild, and no way forward but to unsuspend a root they closed
/// on purpose.
#[test]
fn a_refresh_rebuilds_a_lock_without_what_stands_behind_a_door() {
    let fx = Fixture::new();
    let manifest = fx.global_manifest();
    let lock = manifest.with_extension("lock");
    let before = std::fs::read_to_string(&lock).expect("the lock exists before");
    assert!(
        before.contains("/c.jpg"),
        "the fixture's lock must hold an entry on the root about to be parked: {before}"
    );
    fx.park("src");
    let root = fx.path_str("src");

    // The report: apply's door, in apply's conditional, with the rebuild
    // offered as the act that clears it.
    let status = fx.run(&["cluster", "status", &manifest.to_string_lossy()]);
    status.ok();
    let door = status
        .stdout
        .lines()
        .find(|l| l.contains("suspended"))
        .unwrap_or_default();
    assert_door_line(door, &root, "apply would refuse");
    assert!(door.contains("origin of"), "what stands behind it: {door}");
    assert!(!status.stdout.contains("To apply:"), "{}", status.stdout);
    assert!(
        status
            .stdout
            .contains("To rebuild the lock: canon cluster refresh"),
        "the act that brings the apply within reach is the step: {}",
        status.stdout
    );

    // And the rebuild runs, leaving the parked root's content out of the new
    // lock — the act ignoring parked content, silently.
    let refresh = fx.run(&["cluster", "refresh", &manifest.to_string_lossy()]);
    refresh.ok();
    let spoken = format!("{}{}", refresh.stdout, refresh.stderr);
    assert!(
        !spoken.contains("suspended"),
        "a lock entry is no door of the refresh's: {spoken}"
    );
    let after = std::fs::read_to_string(&lock).expect("the lock still exists");
    assert!(
        !after.contains(&root),
        "the parked root's entries are not in the new lock: {after}"
    );
    assert!(
        after.contains("d.jpg"),
        "and the open world's still are: {after}"
    );
}

/// **A lock standing wholly behind doors is still pointed at the rebuild** —
/// because what a rebuild would find is a question about the open world, not
/// about the lock.
///
/// Here every entry in the lock stands on the parked root, and the open world
/// has gained a file the manifest's query matches. The report cannot know that
/// without running the selection it does not run, so it names the act that
/// runs; the rebuild then finds the new content and the apply becomes
/// available. A predicate over the lock — "every entry is parked, so the
/// rebuild would empty it" — withholds exactly here, and is what this row
/// refuses.
///
/// The other direction of that same guess is worse: it invites a rebuild that
/// empties a partly-applied lock, whose entries have left the source root for
/// the archive and so count as neither parked nor present. Both are why the
/// narrowing reaches only structural facts.
#[test]
fn a_lock_wholly_behind_a_door_is_still_pointed_at_the_rebuild() {
    let fx = Fixture::new();
    let manifest = fx.path("d.toml");
    // Scoped nowhere, matching one file, which stands on `live`.
    fx.run(&[
        "cluster",
        "generate",
        "--global",
        "--dest",
        &format!("{}/out", fx.path_str("arc")),
        "--where",
        "filename = 'd.jpg'",
        "--output",
        &manifest.to_string_lossy(),
    ])
    .ok();
    fx.park("live");

    // The open world gains a match the lock has never seen.
    write(fx.path("src/d.jpg"), "delta");
    fx.run(&["scan", &fx.path_str("src")]).ok();

    let status = fx.run(&["cluster", "status", &manifest.to_string_lossy()]);
    status.ok();
    assert!(
        status
            .stdout
            .lines()
            .any(|l| l.contains("suspended") && l.contains("apply would refuse")),
        "the door is stated: {}",
        status.stdout
    );
    assert!(
        status
            .stdout
            .contains("To rebuild the lock: canon cluster refresh"),
        "and the act that runs is named: {}",
        status.stdout
    );

    // Which was the right thing to name: the rebuild finds the open world's
    // copy, and the apply the report withheld becomes available.
    fx.run(&["cluster", "refresh", &manifest.to_string_lossy()])
        .ok();
    let after = fx.run(&["cluster", "status", &manifest.to_string_lossy()]);
    after.ok();
    assert!(
        after.stdout.contains("To apply: canon apply"),
        "the rebuild brought the apply within reach: {}",
        after.stdout
    );
    assert!(
        !after.stdout.contains("suspended"),
        "and nothing stands behind a door any more: {}",
        after.stdout
    );
}

/// **Apply refuses at either end, `--dry-run` included, and writes nothing.**
/// The destination end was silent before this story: a rehearsal printed
/// sample destinations inside a parked archive, and a real run would have
/// written files and a receipt there.
#[test]
fn apply_refuses_at_either_end_of_a_closed_door() {
    let fx = Fixture::new();
    let manifest = fx.manifest();

    for (park, expect_root) in [("arc", "arc"), ("live", "live")] {
        fx.park(park);
        let root = fx.path_str(expect_root);
        let manifest_arg = manifest.to_string_lossy().into_owned();
        for extra in [vec![], vec!["--dry-run"], vec!["--resume"]] {
            let mut argv = vec!["apply", manifest_arg.as_str(), "--yes"];
            argv.extend(extra.iter().copied());
            let run = fx.run(&argv);
            assert_eq!(run.code, 1, "{argv:?}: {}{}", run.stdout, run.stderr);
            let spoken = format!("{}{}", run.stdout, run.stderr);
            let door = spoken
                .lines()
                .find(|l| l.contains("suspended"))
                .unwrap_or_default();
            assert_door_line(door, &root, "refused");
            // The `<what>` slot's role vocabulary: how the act reached the
            // root, in the act's own nouns — the destination, or the origin
            // and the sources this run would have carried from it.
            let reach = if expect_root == "arc" {
                "destination"
            } else {
                "origin of"
            };
            assert!(door.contains(reach), "{argv:?}: {door}");
            assert!(!spoken.contains("Error:"), "{argv:?}: {spoken}");
            assert!(
                !spoken.contains("regenerate the manifest"),
                "a way back that does not run: {spoken}"
            );
        }
        fx.run(&["roots", "unsuspend", &format!("path:{root}")])
            .ok();
    }

    assert!(
        !fx.path("arc/out").exists(),
        "nothing was written into the archive"
    );
    // Read from the machine stream, and by the recorded **command**: the
    // human timeline's own column carries the transition word (`archived`),
    // never the command, so a text scan for "apply" there passes whether or
    // not a row was written — a guard that cannot fail against the defect it
    // names. The doors were opened and closed in between, so the whole trail
    // cannot be compared before and after either.
    let trail = fx.run(&["trail", "--global", "--jsonl"]);
    trail.ok();
    let commands: Vec<String> = trail
        .stdout
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter_map(|v| v["command"].as_str().map(str::to_string))
        .collect();
    assert!(
        commands.iter().any(|c| c == "cluster_generate"),
        "the fixture must have rows to look through: {commands:?}"
    );
    assert!(
        !commands.iter().any(|c| c == "apply"),
        "a refused apply leaves no decision row behind: {commands:?}"
    );
}

/// **A record naming a parked source is set aside and counted** — a batch
/// surface takes the closed default per record, so one parked root never
/// breaks an import, and the fact is never written behind the door.
#[test]
fn an_import_record_behind_a_door_is_counted_not_written() {
    let fx = Fixture::new();
    let behind = fx.source_id("src/a.jpg");
    let keeper = fx.source_id("live/d.jpg");
    fx.park("src");

    let input = format!(
        "{{\"source_id\":{behind},\"basis_rev\":0,\"facts\":{{\"Make\":\"Canon\"}}}}\n\
         {{\"source_id\":{keeper},\"basis_rev\":0,\"facts\":{{\"Make\":\"Canon\"}}}}\n"
    );
    let run = fx.run_with_stdin(&["import-facts"], &input);
    run.ok();

    let spoken = format!("{}{}", run.stdout, run.stderr);
    assert!(
        spoken.contains("1 skipped (on suspended roots)"),
        "counted, and stated: {spoken}"
    );
    assert!(
        spoken.contains("1 facts imported"),
        "the batch ran on: {spoken}"
    );

    // And nothing was written behind the door.
    let facts = fx.run(&["facts", &fx.path_str("live"), "--key", "content.Make"]);
    facts.ok();
    assert!(facts.stdout.contains("Canon"), "{}", facts.stdout);
}

/// **Object exclusion counts its parked sharers rather than filtering them,
/// on both of its paths.**
///
/// The act is about the object everywhere and the stamp reaches every source
/// sharing it — stamp-set is receipt-set — so what is owed is the statement:
/// a fact about what this act *does*, which is the one thing a count behind a
/// door is owed for. It stands under the sources line it qualifies, on the
/// preview and on the executing path alike, because a `--yes` user is the one
/// who most needs it and a preview they skipped is no place to keep it.
///
/// What the run *searched* is not stated: an act ignores parked content
/// silently, and a wide dismissal whose own statement is already true owes no
/// standing footer about the rooms it never entered.
#[test]
fn object_exclusion_states_how_far_behind_a_door_it_reaches() {
    let fx = Fixture::new();
    fx.park("src");
    let root = fx.path_str("src");

    // The preview path. `a.jpg` stands in all three roots; `live` is where the
    // ask is made.
    let run = fx.run(&[
        "exclude",
        "set-object",
        &fx.path_str("live"),
        "--where",
        "source.ext = 'jpg'",
    ]);
    run.ok();
    let lines: Vec<&str> = run.stdout.lines().collect();
    let share = lines
        .iter()
        .position(|l| l.contains("of which 1 on suspended roots"))
        .unwrap_or_else(|| panic!("the reach is counted: {}", run.stdout));
    assert!(
        lines[share.saturating_sub(1)].contains("sources"),
        "and it stands under the sources line it is a share of: {}",
        run.stdout
    );
    assert!(
        !run.stdout.contains(&root),
        "what it did not search is not a footer: {}",
        run.stdout
    );

    // And the executing path, where the act really happens.
    let executed = fx.run(&[
        "exclude",
        "set-object",
        "--global",
        "--where",
        // The live copy of content that also stands behind the door: selected
        // from an open root, and its object shared with a parked source.
        "filename = 'a-copy.jpg'",
        "--yes",
    ]);
    executed.ok();
    assert!(
        executed.stdout.contains("of which 1 on suspended roots"),
        "the count is owed where the act happens: {}",
        executed.stdout
    );
    assert!(
        executed.stdout.contains("Excluded"),
        "and the act proceeded, as an act about content everywhere must: {}",
        executed.stdout
    );
}

/// **Retirement opens the door first — and the review still reads.** The
/// ceremony is an act and meets the closed default; the review is remembering,
/// so `--dry-run` renders it and closes by naming the door instead of the
/// acknowledgment hint, which would name a run the door turns away.
#[test]
fn retire_dry_run_reads_a_parked_root_and_a_real_run_refuses() {
    let fx = Fixture::new();
    fx.park("src");
    let root = fx.path_str("src");
    let before = fx.run(&["trail", "--global"]).stdout;

    let review = fx.run(&["roots", "retire", &format!("path:{root}"), "--dry-run"]);
    review.ok();
    assert!(
        review.stdout.contains("Retirement review:"),
        "remembering reads: {}",
        review.stdout
    );
    assert!(
        review.stdout.contains(&format!(
            "{root} suspended · canon roots unsuspend path:{root}"
        )),
        "and closes with the pause, in the verbless form: {}",
        review.stdout
    );
    assert!(
        !review.stdout.contains("To retire anyway:"),
        "the hint names an act the door turns away: {}",
        review.stdout
    );

    let real = fx.run(&["roots", "retire", &format!("path:{root}"), "--yes"]);
    assert_eq!(real.code, 1);
    let spoken = format!("{}{}", real.stdout, real.stderr);
    // **The subject's own door answers first**, ahead of the review and ahead
    // of the shelf's: the user named this root, so this root's door is the
    // answer to what they asked, and its way back is the one that unblocks
    // them. Asserted on the *first* line, which is what distinguishes the
    // ordering — a door stated last would pass a "somewhere in the output"
    // check under either arrangement.
    assert_door_line(spoken.lines().next().unwrap_or_default(), &root, "refused");
    assert!(
        !spoken.contains("Retirement review:"),
        "a refused act shows no review: {spoken}"
    );
    assert!(!spoken.contains("Error:"), "{spoken}");
    assert!(
        !fx.path("arc/retired").exists(),
        "nothing was bound: the door precedes the bind"
    );
    assert_eq!(
        before,
        fx.run(&["trail", "--global"]).stdout,
        "and no decision row was opened"
    );

    // And with the shelf closed too, the subject's door is still the one that
    // speaks: the other root's way back would not unblock what was asked.
    fx.park("arc");
    let both = fx.run(&["roots", "retire", &format!("path:{root}"), "--yes"]);
    assert_eq!(both.code, 1);
    let spoken = format!("{}{}", both.stdout, both.stderr);
    assert_door_line(spoken.lines().next().unwrap_or_default(), &root, "refused");
    assert!(
        !spoken.contains("needs an archive root"),
        "the shelf's absence is not the answer to a question about this root: {spoken}"
    );
}

/// **Closing a door is a recorded act, and takes a reason like any other.**
/// The registry gives "why did I park this" a home in the act of parking, and
/// the trail is where it answers.
#[test]
fn suspending_a_root_records_its_reason() {
    let fx = Fixture::new();
    let root = fx.path_str("src");

    let run = fx.run(&[
        "roots",
        "suspend",
        &format!("path:{root}"),
        "--reason",
        "steering the sweep; revisit after the phone import",
    ]);
    run.ok();

    let trail = fx.run(&["trail", "--global"]);
    trail.ok();
    assert!(
        trail.stdout.contains("steering the sweep"),
        "the trail answers why: {}",
        trail.stdout
    );

    let opened = fx.run(&[
        "roots",
        "unsuspend",
        &format!("path:{root}"),
        "--reason",
        "back to work on it",
    ]);
    opened.ok();
    let trail = fx.run(&["trail", "--global"]);
    trail.ok();
    assert!(
        trail.stdout.contains("back to work on it"),
        "{}",
        trail.stdout
    );
}

/// **A refresh remedy is withheld where a refresh would refuse, and nowhere
/// else.**
///
/// A report must name no command the door turns away. `cluster refresh`
/// re-queries from the manifest's recorded **scope** and rewrites files that
/// live where the user put them, so a parked **scope line** turns it away and
/// a parked destination does not — and neither does a parked lock entry, which
/// the rebuild simply leaves out. The manifest here is scoped to the root that
/// gets parked, which is what makes this the refusing case. The remedy under
/// the source-lost warning is a refresh, so it follows that split, and it is
/// one sentence across two lines: withheld, the lead goes with it rather than
/// trailing off mid-thought.
///
/// A lost source is what raises this remedy, and it also makes the report
/// account for nothing — so no apply invitation arises here either way. That
/// half of the rule is `an_apply_invitation_stands_where_the_door_is_not_its_own`
/// below, which is the state where it can actually be asked.
#[test]
fn a_status_withholds_only_the_steps_its_own_door_turns_away() {
    for (park, remedy_expected) in [("arc", true), ("live", false)] {
        let fx = Fixture::new();
        let manifest = fx.manifest();
        // A lost source is what raises the remedy at all.
        std::fs::remove_file(fx.path("live/a-copy.jpg")).unwrap();
        std::fs::remove_file(fx.path("live/d.jpg")).unwrap();
        fx.park(park);

        let run = fx.run(&["cluster", "status", &manifest.to_string_lossy()]);
        run.ok();
        assert!(
            run.stdout.contains("source files are missing"),
            "{park}: the fixture must reach the warning: {}",
            run.stdout
        );
        assert_eq!(
            run.stdout
                .contains("refresh the manifest: canon cluster refresh"),
            remedy_expected,
            "{park}: the remedy follows its own act, not any door: {}",
            run.stdout
        );
        // Whichever way it went, the sentence is whole: the lead never stands
        // without the remedy it introduces.
        assert!(
            !run.stdout.contains("If files are truly lost,\n\n")
                && !run.stdout.trim_end().ends_with("If files are truly lost,"),
            "{park}: a sentence was cut in half: {}",
            run.stdout
        );
    }
}

/// **An apply invitation stands where the closed door is not apply's own.**
///
/// `apply` carries out a lock: it transfers from the lock's entries into the
/// destination, and reads no recorded scope. So a manifest scoped across two
/// roots whose entries all stand on one of them applies perfectly well when
/// the *other* is closed — the door is stated, and the invitation stands.
/// Withholding it there would keep the working command from the user who needs
/// it, which is the same fault as naming one that refuses.
#[test]
fn an_apply_invitation_stands_where_the_door_is_not_its_own() {
    let fx = Fixture::new();
    let manifest = fx.path("two-roots.toml");
    // Scoped across both source roots; only `live` holds a file named d.jpg,
    // so every lock entry stands there and `src` contributes none.
    fx.run(&[
        "cluster",
        "generate",
        &fx.path_str("src"),
        &fx.path_str("live"),
        "--dest",
        &format!("{}/out", fx.path_str("arc")),
        "--where",
        "filename = 'd.jpg'",
        "--output",
        &manifest.to_string_lossy(),
    ])
    .ok();
    fx.park("src");
    let parked = fx.path_str("src");

    let run = fx.run(&["cluster", "status", &manifest.to_string_lossy()]);
    run.ok();

    // The door is stated — seeing is owed whatever the step turns out to be.
    let door = run
        .stdout
        .lines()
        .find(|l| l.contains("suspended"))
        .unwrap_or_default();
    // Status states the door in the act's own conditional: this one is
    // `cluster refresh`'s, whose re-query reads the recorded scope.
    assert_door_line(door, &parked, "cluster refresh would refuse");

    // And the step stands, because this door is not one an apply meets.
    assert!(
        run.stdout.contains("To apply: canon apply"),
        "the invitation is owed here: {}",
        run.stdout
    );

    // Which is true: the apply runs.
    let applied = fx.run(&["apply", &manifest.to_string_lossy(), "--yes"]);
    applied.ok();
    assert!(
        fx.path("arc/out").exists(),
        "the report invited an act that works"
    );
}

// ============================================================================
// E — testifying: what a closed door does NOT stop
// ============================================================================
//
// The registry's fourth permit. A parked root's copies still ground claims
// about other places: custody stands, and redundancy points. What changes is
// that every claim resting on them says so — **stated, never headlined**,
// which is a rule about presentation and not about whether the evidence
// counts.

/// **E1 — coverage counts a parked archive's copies, and says what rests
/// there.**
///
/// The count does not shrink: parking never un-archives, and a source whose
/// only copy stands behind a door is covered. What is added is the one fact a
/// user reading this before releasing a drive would otherwise have to open a
/// door to find out.
#[test]
fn coverage_counts_parked_copies_and_says_what_rests_on_them() {
    let fx = Fixture::new();
    // `src/sub/c.jpg` has no copy in `arc`; give it one in a second archive,
    // then close that archive's door.
    std::fs::create_dir_all(fx.path("arc2")).unwrap();
    write(fx.path("arc2/c.jpg"), "charlie");
    fx.run(&["scan", "--add", "--role", "archive", &fx.path_str("arc2")])
        .ok();
    let parked = fx.path_str("arc2");
    fx.park("arc2");

    let run = fx.run(&["coverage", "--global"]);
    run.ok();

    let archived = run
        .stdout
        .lines()
        .find(|l| l.contains("Archived:") && l.contains("only on suspended"))
        .unwrap_or_else(|| panic!("no qualified archived line in:\n{}", run.stdout));
    assert!(
        archived.contains(&parked),
        "the segment names the root the claim rests on: {archived}"
    );
    assert!(
        !archived.contains("canon roots unsuspend"),
        "testimony is stated, not offered as a way back: {archived}"
    );
}

/// **E2 — a wholly open archive says nothing about doors.**
///
/// The segment is suppressed at zero. "0 only on suspended roots" is a
/// sentence about a situation that is not happening, and a view that says it
/// every time stops being read.
#[test]
fn an_open_world_never_mentions_a_door() {
    let fx = Fixture::new();
    let run = fx.run(&["coverage", "--global"]);
    run.ok();
    assert!(!run.stdout.contains("only on suspended"), "{}", run.stdout);
}

/// **E3 — a parked `--archive` is answered and marked, not refused.**
///
/// This was refused by name for as long as the outward side filtered those
/// copies out, because an answer would have been a zero where content stands.
/// The filter is gone, so the answer is owed.
#[test]
fn a_parked_archive_named_as_the_frame_is_answered() {
    let fx = Fixture::new();
    let parked = fx.path_str("arc");
    fx.park("arc");

    for args in [
        vec!["coverage", "--global", "--archive"],
        vec!["survey", "--archive"],
    ] {
        let mut argv = args.clone();
        let spec = format!("path:{parked}");
        argv.push(&spec);
        let scope = fx.path_str("src");
        if argv[0] == "survey" {
            argv.insert(1, &scope);
        }
        let run = fx.run(&argv);
        run.ok();
        assert!(
            run.stdout.contains("(suspended)"),
            "the frame is marked ({argv:?}):\n{}",
            run.stdout
        );
        assert!(
            !run.stdout.contains("canon roots unsuspend"),
            "and not refused ({argv:?}):\n{}",
            run.stdout
        );
    }
}

/// **E4 — survey's outward side counts parked evidence, marks it, and never
/// leads with it.**
///
/// The inversion this repairs: with `live` parked, the same content used to
/// read `1 unique here` and `No related locations found.` — survey's answer
/// changed by an act that changed only attention.
#[test]
fn survey_counts_parked_evidence_and_never_leads_with_it() {
    let fx = Fixture::new();
    // Give the root about to be parked a second shared file, so it carries a
    // strictly stronger relation than the live location added below. A tie
    // would let a sort that ignores standing pass this row by accident.
    write(fx.path("live/b-copy.jpg"), "bravo");
    fx.run(&["scan", &fx.path_str("live")]).ok();
    let parked = fx.path_str("live");
    fx.park("live");

    let run = fx.run(&["survey", &fx.path_str("src")]);
    run.ok();

    assert!(
        run.stdout.contains("Related locations:"),
        "the parked copy is still a copy:\n{}",
        run.stdout
    );
    let cited = run
        .stdout
        .lines()
        .find(|l| l.contains(&parked))
        .unwrap_or_else(|| panic!("no citation of {parked} in:\n{}", run.stdout));
    assert!(
        cited.contains("(suspended)"),
        "and the citation says so: {cited}"
    );
    assert!(
        !run.stdout.contains("0 unique here\n  1 unique"),
        "sanity: {}",
        run.stdout
    );

    // A second, live location carrying the *weaker* relation — one shared
    // file against the parked location's two. It still leads, because
    // standing orders ahead of the relation. Built here rather than assumed:
    // the base fixture yields one related location, so the assertion below
    // would otherwise never run at all.
    std::fs::create_dir_all(fx.path("live2")).unwrap();
    write(fx.path("live2/a-copy.jpg"), "alpha");
    fx.run(&["scan", "--add", "--role", "source", &fx.path_str("live2")])
        .ok();
    let live2 = fx.path_str("live2");

    let run = fx.run(&["survey", &fx.path_str("src")]);
    run.ok();
    let order: Vec<&str> = run
        .stdout
        .lines()
        .skip_while(|l| !l.contains("Related locations:"))
        .filter(|l| l.contains("overlap ("))
        .collect();
    assert!(
        order.len() > 1,
        "the fixture must offer a choice to order:\n{}",
        run.stdout
    );
    assert!(
        order[0].contains(&live2),
        "the live location leads even carrying the weaker relation: {order:?}"
    );
    assert!(
        order[0].contains(" 1 of 3 overlap"),
        "sanity: the leading location is the weaker one: {order:?}"
    );
    assert!(
        order.last().unwrap().contains("(suspended)")
            && order.last().unwrap().contains(" 2 of 3 overlap"),
        "and the parked one trails despite the stronger relation: {order:?}"
    );
}

/// **E7 — a detail view answers about a place the way the summary just did.**
///
/// The same run must not mark a location in its summary and leave it bare a
/// few lines down, nor let it lead one list while trailing the other. Before
/// the outward side counted parked evidence a parked archive could not reach
/// these views at all, so this is the widening's own conformance row.
#[test]
fn a_detail_view_marks_and_orders_the_places_the_summary_named() {
    let fx = Fixture::new();
    // The parked archive holds all three; the live one holds two, so the
    // parked place carries the strictly stronger relation and must still
    // trail. A tie would let a stable count-sort pass this by accident.
    std::fs::create_dir_all(fx.path("arc2")).unwrap();
    write(fx.path("arc2/a.jpg"), "alpha");
    write(fx.path("arc2/b.jpg"), "bravo");
    write(fx.path("arc2/c.jpg"), "charlie");
    fx.run(&["scan", "--add", "--role", "archive", &fx.path_str("arc2")])
        .ok();
    let parked = fx.path_str("arc2");
    fx.park("arc2");

    let run = fx.run(&["survey", &fx.path_str("src"), "--detail", "archived"]);
    run.ok();

    let cited: Vec<&str> = run
        .stdout
        .lines()
        .filter(|l| l.trim_start().starts_with("Archived at "))
        .collect();
    assert!(cited.len() > 1, "no choice to order:\n{}", run.stdout);
    assert!(
        !cited[0].contains(&parked),
        "a parked location never leads a detail view either: {cited:?}"
    );
    let parked_line = cited
        .iter()
        .find(|l| l.contains(&parked))
        .unwrap_or_else(|| panic!("the parked archive is not cited:\n{}", run.stdout));
    assert!(
        parked_line.contains("(suspended)"),
        "and it carries the same mark the summary gave it: {parked_line}"
    );
}

/// **E5 — the story and the readiness review say what a claim rests on, from
/// one account.**
///
/// Both are lenses over the same fetch, so the number they print cannot
/// disagree; and the where-line marks the place it names.
#[test]
fn the_story_and_the_review_state_the_evidence_behind_a_door() {
    let fx = Fixture::new();
    std::fs::create_dir_all(fx.path("arc2")).unwrap();
    write(fx.path("arc2/c.jpg"), "charlie");
    fx.run(&["scan", "--add", "--role", "archive", &fx.path_str("arc2")])
        .ok();
    let parked = fx.path_str("arc2");
    fx.park("arc2");
    let subject = format!("path:{}", fx.path_str("src"));

    let story = fx.run(&["roots", "story", &subject]);
    story.ok();
    let where_line = story
        .stdout
        .lines()
        .find(|l| l.contains("copies stand in") && l.contains(&parked))
        .unwrap_or_else(|| panic!("no covered-where line naming {parked}:\n{}", story.stdout));
    assert!(
        where_line.contains("(suspended)"),
        "the where-line marks the place it names: {where_line}"
    );
    assert!(
        story
            .stdout
            .lines()
            .any(|l| l.starts_with("Standing:") && l.contains("only on suspended")),
        "the closing totals carry it too:\n{}",
        story.stdout
    );

    let review = fx.run(&["roots", "retire", &subject, "--dry-run"]);
    let covered = review
        .stdout
        .lines()
        .find(|l| l.trim_start().starts_with("covered"))
        .unwrap_or_else(|| panic!("no covered line in:\n{}", review.stdout));
    assert!(
        covered.contains("only on suspended") && covered.contains(&parked),
        "the review reads the same account: {covered}"
    );
}

/// **E6 — the composition card marks a parked origin; the history beside it
/// does not.**
///
/// The card states what stands here *now*, so a door in front of an origin is
/// what a reader about to go looking needs. The act line above it records what
/// happened *then*, and a standing true only now is not true of the past.
#[test]
fn the_card_marks_a_parked_origin_and_the_history_does_not() {
    let fx = Fixture::new();
    let manifest = fx.manifest();
    fx.run(&["apply", &manifest.to_string_lossy(), "--yes"])
        .ok();
    let origin = fx.path_str("live");
    fx.park("live");

    let run = fx.run(&["trail", &format!("{}/out", fx.path_str("arc"))]);
    run.ok();

    let card_line = run
        .stdout
        .lines()
        .position(|l| l.contains("Standing here"))
        .unwrap_or_else(|| panic!("no composition card in:\n{}", run.stdout));
    let card: Vec<&str> = run.stdout.lines().skip(card_line).collect();
    assert!(
        card.iter().any(|l| l.contains("(suspended)")),
        "the card marks the parked origin:\n{}",
        card.join("\n")
    );

    let history: Vec<&str> = run.stdout.lines().take(card_line).collect();
    assert!(
        history.iter().any(|l| l.contains(&origin)),
        "the act line still names the origin:\n{}",
        history.join("\n")
    );
    assert!(
        !history.iter().any(|l| l.contains("(suspended)")),
        "but says nothing about a door that closed afterwards:\n{}",
        history.join("\n")
    );

    // The `drew from:` block is the other past-tense surface, and the one
    // that shares the marker's own spelling — so it is the one that would
    // actually change if the rule were dropped.
    let apply_id = history
        .iter()
        .find(|l| l.contains("archived"))
        .and_then(|l| l.split_whitespace().next())
        .map(|chip| chip.trim_start_matches('#').to_string())
        .unwrap_or_else(|| panic!("no apply row in:\n{}", history.join("\n")));
    let show = fx.run(&["trail", "show", &apply_id]);
    show.ok();
    let drew: Vec<&str> = show
        .stdout
        .lines()
        .filter(|l| l.contains(&origin))
        .collect();
    assert!(!drew.is_empty(), "no origin lines in:\n{}", show.stdout);
    assert!(
        !drew.iter().any(|l| l.contains("(suspended)")),
        "a record of what happened then carries no standing from now: {drew:?}"
    );
}
