//! Binding on the built binary: every command that reads `--where` reads its filter through
//! binding before anything runs, so a value that cannot mean anything for its key stops the
//! command with nothing listed, planned, prompted, recorded or written — under `--dry-run`
//! and `--yes` alike, and before the early returns a command takes when it has nothing to do.
//!
//! One test per path that reads a filter. Each asserts the exit code, the `Error:` channel,
//! the broken clause named, no source listed, and no decision recorded; the manifest paths
//! assert the manifest and its lock unchanged to the byte.
//!
//! Beside them, what a typed value means on the user's clock — the New Year rows in two zones,
//! the pairs of spellings — what `worklist --emit` writes pasting back, and which channel a
//! refusal prints on, with or without `Error:`.

use std::path::PathBuf;
use std::process::Command;

/// The refused filter: `garbage` is no number, and `NOT` must not turn it into every source.
const REFUSED: &str = "NOT source.size=garbage";

struct Fixture {
    dir: tempfile::TempDir,
    /// The user's zone every run reads its clock in.
    zone: &'static str,
}

struct Run {
    stdout: String,
    stderr: String,
    code: i32,
}

impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("temp dir");
        let fx = Fixture { dir, zone: "UTC" };
        for sub in ["home", "src/sub", "arc"] {
            std::fs::create_dir_all(fx.path(sub)).unwrap();
        }
        std::fs::write(fx.path("src/a.jpg"), "alpha").unwrap();
        std::fs::write(fx.path("src/sub/b.jpg"), "alpha").unwrap();
        std::fs::write(fx.path("arc/c.jpg"), "charlie").unwrap();
        for (role, root) in [("source", "src"), ("archive", "arc")] {
            let run = fx.run(&["scan", "--add", "--role", role, &fx.path_str(root)]);
            assert_eq!(run.code, 0, "{}{}", run.stdout, run.stderr);
        }
        fx
    }

    fn path(&self, rel: &str) -> PathBuf {
        self.dir.path().join(rel)
    }

    fn path_str(&self, rel: &str) -> String {
        let p = self.path(rel);
        std::fs::canonicalize(&p)
            .unwrap_or(p)
            .to_string_lossy()
            .into_owned()
    }

    fn run(&self, args: &[&str]) -> Run {
        self.run_with_input(args, None)
    }

    fn run_with_input(&self, args: &[&str], input: Option<&str>) -> Run {
        use std::io::Write;
        let mut child = Command::new(env!("CARGO_BIN_EXE_canon"))
            .args(args)
            .current_dir(self.dir.path())
            .env("CANON_HOME", self.path("home"))
            .env("TZ", self.zone)
            .env("EDITOR", "true")
            .env("VISUAL", "true")
            .stdin(if input.is_some() {
                std::process::Stdio::piped()
            } else {
                std::process::Stdio::null()
            })
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("canon must run");
        if let Some(text) = input {
            child
                .stdin
                .take()
                .unwrap()
                .write_all(text.as_bytes())
                .unwrap();
        }
        let out = child.wait_with_output().expect("canon must finish");
        Run {
            stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
            code: out.status.code().unwrap_or(-1),
        }
    }

    /// How many decisions the trail holds.
    fn decisions(&self) -> usize {
        let run = self.run(&["trail", "--global", "--jsonl"]);
        run.stdout
            .lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .filter(|v| v["type"] == "decision")
            .count()
    }

    /// Run a command that reads `filter` and assert it was refused before anything ran.
    fn assert_refused(&self, args: &[&str]) {
        let before = self.decisions();
        let run = self.run(args);
        let all = format!("{}{}", run.stdout, run.stderr);
        assert_ne!(run.code, 0, "{args:?} must fail:\n{all}");
        assert!(
            run.stderr
                .starts_with("Error: cannot run the filter as given\n"),
            "{args:?}:\n{all}"
        );
        assert!(
            run.stderr.contains("source.size=garbage"),
            "{args:?}:\n{all}"
        );
        assert!(
            !all.contains("a.jpg") && !all.contains("b.jpg"),
            "{args:?} listed a source:\n{all}"
        );
        assert!(!all.contains("Proceed?"), "{args:?} prompted:\n{all}");
        assert_eq!(
            self.decisions(),
            before,
            "{args:?} recorded a decision:\n{all}"
        );
    }
}

#[test]
fn ls_binds_before_listing() {
    let fx = Fixture::new();
    fx.assert_refused(&["ls", "--global", "--where", REFUSED]);
}

#[test]
fn ls_duplicates_binds_before_grouping() {
    let fx = Fixture::new();
    fx.assert_refused(&["ls", "--global", "--duplicates", "--where", REFUSED]);
}

#[test]
fn worklist_binds_before_emitting() {
    let fx = Fixture::new();
    fx.assert_refused(&["worklist", "--global", "--where", REFUSED]);
}

#[test]
fn coverage_binds_before_counting() {
    let fx = Fixture::new();
    fx.assert_refused(&["coverage", "--global", "--where", REFUSED]);
}

#[test]
fn compare_binds_before_comparing() {
    let fx = Fixture::new();
    let (src, arc) = (fx.path_str("src"), fx.path_str("arc"));
    fx.assert_refused(&["compare", &src, &arc, "--where", REFUSED]);
}

#[test]
fn survey_binds_before_surveying_and_before_its_all_archive_statement() {
    let fx = Fixture::new();
    fx.assert_refused(&["survey", "--global", "--where", REFUSED]);
    let arc = fx.path_str("arc");
    fx.assert_refused(&["survey", &arc, "--where", REFUSED]);
}

#[test]
fn facts_binds_before_reporting() {
    let fx = Fixture::new();
    fx.assert_refused(&["facts", "--global", "--where", REFUSED]);
}

#[test]
fn facts_delete_binds_before_planning_or_deleting() {
    let fx = Fixture::new();
    let src = fx.path_str("src");
    fx.assert_refused(&[
        "facts",
        "delete",
        "--on",
        "source",
        "content.k",
        &src,
        "--where",
        REFUSED,
    ]);
    fx.assert_refused(&[
        "facts",
        "delete",
        "--on",
        "source",
        "content.k",
        &src,
        "--where",
        REFUSED,
        "--yes",
    ]);
}

#[test]
fn exclude_set_binds_under_dry_run_and_yes() {
    let fx = Fixture::new();
    let src = fx.path_str("src");
    fx.assert_refused(&["exclude", "set", &src, "--where", REFUSED, "--dry-run"]);
    fx.assert_refused(&["exclude", "set", &src, "--where", REFUSED, "--yes"]);
}

#[test]
fn exclude_clear_binds_before_its_nothing_to_clear_return() {
    let fx = Fixture::new();
    let src = fx.path_str("src");
    // Nothing is excluded, so a filter that bound would answer "No excluded sources match".
    fx.assert_refused(&["exclude", "clear", &src, "--where", REFUSED, "--dry-run"]);
    fx.assert_refused(&["exclude", "clear", &src, "--where", REFUSED, "--yes"]);
}

#[test]
fn exclude_duplicates_binds_under_dry_run_and_yes() {
    let fx = Fixture::new();
    let (src, sub) = (fx.path_str("src"), fx.path_str("src/sub"));
    for flag in ["--dry-run", "--yes"] {
        fx.assert_refused(&[
            "exclude",
            "duplicates",
            &src,
            "--prefer",
            &sub,
            "--where",
            REFUSED,
            flag,
        ]);
    }
}

#[test]
fn exclude_set_object_binds_under_preview_and_yes() {
    let fx = Fixture::new();
    let src = fx.path_str("src");
    fx.assert_refused(&["exclude", "set-object", &src, "--where", REFUSED]);
    fx.assert_refused(&["exclude", "set-object", &src, "--where", REFUSED, "--yes"]);
}

#[test]
fn cluster_generate_binds_before_writing_a_manifest() {
    let fx = Fixture::new();
    let manifest = fx.path("m.toml");
    let (src, dest) = (fx.path_str("src"), format!("{}/out", fx.path_str("arc")));
    fx.assert_refused(&[
        "cluster",
        "generate",
        &src,
        "--dest",
        &dest,
        "--where",
        REFUSED,
        "--output",
        &manifest.to_string_lossy(),
    ]);
    assert!(!manifest.exists(), "no manifest is written");
    assert!(
        !manifest.with_extension("lock").exists(),
        "no lock is written"
    );
}

/// A manifest's recorded query is read the same way: `cluster refresh` refuses it and
/// leaves the manifest and its lock exactly as they were; `cluster status` names the cause.
#[test]
fn a_recorded_query_binds_at_refresh_and_status() {
    let fx = Fixture::new();
    let manifest = fx.path("m.toml");
    let (src, dest) = (fx.path_str("src"), format!("{}/out", fx.path_str("arc")));
    let run = fx.run(&[
        "cluster",
        "generate",
        &src,
        "--dest",
        &dest,
        "--where",
        "source.ext=jpg",
        "--allow",
        "duplicates",
        "--output",
        &manifest.to_string_lossy(),
    ]);
    assert_eq!(run.code, 0, "{}{}", run.stdout, run.stderr);
    let config = std::fs::read_to_string(&manifest).unwrap();
    let edited = config.replace(
        r#"query = ["source.ext=jpg"]"#,
        r#"query = ["NOT source.size=garbage"]"#,
    );
    assert_ne!(
        config, edited,
        "the query line is where the test expects it:\n{config}"
    );
    std::fs::write(&manifest, &edited).unwrap();
    let lock = manifest.with_extension("lock");
    let lock_before = std::fs::read(&lock).unwrap();
    let decisions = fx.decisions();

    let refresh = fx.run(&["cluster", "refresh", &manifest.to_string_lossy()]);
    assert_ne!(refresh.code, 0, "{}{}", refresh.stdout, refresh.stderr);
    assert!(
        refresh.stderr.contains("source.size=garbage"),
        "{}",
        refresh.stderr
    );
    assert!(
        refresh.stderr.contains("is not a number"),
        "{}",
        refresh.stderr
    );
    assert_eq!(std::fs::read_to_string(&manifest).unwrap(), edited);
    assert_eq!(std::fs::read(&lock).unwrap(), lock_before);
    assert_eq!(fx.decisions(), decisions);

    let status = fx.run(&["cluster", "status", &manifest.to_string_lossy()]);
    assert!(
        status.stdout.contains("cluster refresh would be rejected")
            && status.stdout.contains("is not a number"),
        "{}{}",
        status.stdout,
        status.stderr
    );
}

// ---------------------------------------------------------------------------
// Typed values on the user's clock, and what the program prints, on the built binary.
// ---------------------------------------------------------------------------

/// A save time no row below asks about: noon UTC on 1 January 2022, 2022 in every zone here.
const OLD: i64 = 1_641_038_400;

/// Named files, each saved at its instant (seconds since 1970), scanned as one source root;
/// every run reads its clock in `zone`.
fn photos(zone: &'static str, files: &[(&str, i64)]) -> Fixture {
    let dir = tempfile::tempdir().expect("temp dir");
    let fx = Fixture { dir, zone };
    for sub in ["home", "src"] {
        std::fs::create_dir_all(fx.path(sub)).unwrap();
    }
    for (name, saved) in files {
        let path = fx.path(&format!("src/{name}"));
        std::fs::write(&path, name).unwrap();
        let at = std::time::UNIX_EPOCH + std::time::Duration::from_secs(*saved as u64);
        std::fs::File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_modified(at)
            .unwrap();
    }
    let run = fx.run(&["scan", "--add", "--role", "source", &fx.path_str("src")]);
    assert_eq!(run.code, 0, "{}{}", run.stdout, run.stderr);
    fx
}

impl Fixture {
    /// Each file's source id, read off `worklist`.
    fn source_ids(&self) -> std::collections::HashMap<String, i64> {
        let run = self.run(&["worklist", "--global"]);
        run.stdout
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .map(|v| {
                let path = v["path"].as_str().unwrap();
                let name = path.rsplit('/').next().unwrap().to_string();
                (name, v["source_id"].as_i64().unwrap())
            })
            .collect()
    }

    /// Import `facts` (a JSON object) onto each named file.
    fn import(&self, rows: &[(&str, serde_json::Value)]) {
        let ids = self.source_ids();
        let input: String = rows
            .iter()
            .map(|(name, facts)| {
                serde_json::json!({"source_id": ids[*name], "basis_rev": 0, "facts": facts})
                    .to_string()
                    + "\n"
            })
            .collect();
        let run = self.run_with_input(&["import-facts"], Some(&input));
        assert_eq!(run.code, 0, "{}{}", run.stdout, run.stderr);
    }

    /// The file names `ls --where <filter>` lists; the filter must run.
    fn listed(&self, filter: &str) -> std::collections::BTreeSet<String> {
        let run = self.run(&["ls", "--global", "--where", filter]);
        assert_eq!(
            run.code, 0,
            "{} in {}: {}{}",
            filter, self.zone, run.stdout, run.stderr
        );
        run.stdout
            .lines()
            .filter(|line| line.contains("/src/"))
            .map(|line| line.rsplit('/').next().unwrap().to_string())
            .collect()
    }
}

/// The names, as a set, for comparing with `listed`.
fn names(of: &[&str]) -> std::collections::BTreeSet<String> {
    of.iter().map(|n| n.to_string()).collect()
}

/// New Year and typed dates, in `Europe/Amsterdam` and in
/// `America/New_York`: a file saved at 00:30 on 1 January 2024 on the user's clock is 2024
/// under `source.mtime=2024`, `|year`, `ls -l` and `facts --key source.mtime`; a typed date is that day on the user's
/// clock for every kind of time; a typed time with an offset is translated to the user's local
/// time; each pair of spellings selects the same sources; and a value known
/// only to the year is found by asking for no month.
#[test]
fn the_new_year_rows_hold_on_each_users_clock() {
    // (zone, 00:30 on 1 January 2024 there, 23:30 on 31 December 2023 there)
    for (zone, new_year, eve) in [
        ("Europe/Amsterdam", 1_704_065_400, 1_704_061_800),
        ("America/New_York", 1_704_087_000, 1_704_083_400),
    ] {
        let fx = photos(
            zone,
            &[
                ("saved.jpg", new_year),
                ("eve.jpg", eve),
                ("ny.jpg", OLD),
                ("utc.jpg", OLD),
                ("clock.jpg", OLD),
                ("year.jpg", OLD),
                ("month.jpg", OLD),
                ("day.jpg", OLD),
                ("second.jpg", OLD),
            ],
        );
        let time = |text: &str| serde_json::json!({"taken": {"value": text, "type": "datetime"}});
        fx.import(&[
            ("ny.jpg", time("2023-12-31T21:00:00-05:00")),
            ("utc.jpg", time("2023-12-31T23:30:00Z")),
            ("clock.jpg", time("2023:12:31 21:00:00")),
            ("year.jpg", time("2024")),
            ("month.jpg", time("2024-06")),
            ("day.jpg", time("2024-06-15")),
            ("second.jpg", time("2024-06-15T14:30:05")),
        ]);

        // The file saved at 00:30 on 1 January 2024 on the user's clock.
        for filter in [
            "source.mtime=2024",
            "source.mtime|year=2024",
            "source.mtime=2024-01-01",
            "source.mtime>=2024",
        ] {
            assert_eq!(fx.listed(filter), names(&["saved.jpg"]), "{zone}: {filter}");
        }
        assert_eq!(
            fx.listed("source.mtime=2023"),
            names(&["eve.jpg"]),
            "{zone}"
        );
        let long = fx.run(&["ls", "-l", "--global", "--where", "source.mtime=2024"]);
        assert!(long.stdout.contains("2024"), "{zone}: {}", long.stdout);
        let report = fx.run(&["facts", "--global", "--key", "source.mtime"]);
        let row_2024: Vec<&str> = report
            .stdout
            .lines()
            .filter(|line| line.starts_with("2024 "))
            .collect();
        assert!(
            row_2024.len() == 1 && row_2024[0].split_whitespace().nth(1) == Some("1"),
            "{zone}: {}",
            report.stdout
        );

        // The New Year search: a typed date is that day on the user's clock.
        let (new_years_day, new_years_eve, typed_offset) = match zone {
            // 21:00 at -05:00 is 03:00 on 1 January here, and 23:30 UTC is 00:30.
            "Europe/Amsterdam" => (
                names(&["ny.jpg", "utc.jpg"]),
                names(&["clock.jpg"]),
                names(&["ny.jpg"]),
            ),
            // Here both instants are still 31 December, and the typed minute is 21:00 on the
            // user's clock — the minute the clock reading names as written.
            _ => (
                names(&[]),
                names(&["ny.jpg", "utc.jpg", "clock.jpg"]),
                names(&["ny.jpg", "clock.jpg"]),
            ),
        };
        assert_eq!(fx.listed("taken=2024-01-01"), new_years_day, "{zone}");
        assert_eq!(fx.listed("taken=2023-12-31"), new_years_eve, "{zone}");
        assert_eq!(
            fx.listed(r#"taken="2023-12-31 21:00 -05:00""#),
            typed_offset,
            "{zone}"
        );
        assert_eq!(
            fx.listed("taken=2023-12-31T21:00-05:00"),
            typed_offset,
            "{zone}"
        );

        // One meaning, two spellings.
        let in_2024: Vec<&str> = match zone {
            "Europe/Amsterdam" => vec![
                "ny.jpg",
                "utc.jpg",
                "year.jpg",
                "month.jpg",
                "day.jpg",
                "second.jpg",
            ],
            _ => vec!["year.jpg", "month.jpg", "day.jpg", "second.jpg"],
        };
        for (plain, modified, expected) in [
            ("taken=2024", "taken|year=2024", in_2024),
            (
                "taken=2024-06",
                "taken|yearmonth=2024-06",
                vec!["month.jpg", "day.jpg", "second.jpg"],
            ),
            (
                "taken=2024-06-15",
                "taken|date=2024-06-15",
                vec!["day.jpg", "second.jpg"],
            ),
        ] {
            assert_eq!(fx.listed(plain), names(&expected), "{zone}: {plain}");
            assert_eq!(fx.listed(modified), names(&expected), "{zone}: {modified}");
        }

        // A value known only to the year.
        assert_eq!(
            fx.listed("taken=2024 AND NOT taken|month?"),
            names(&["year.jpg"]),
            "{zone}"
        );
    }
}

/// What `worklist --emit` writes pastes back: the exact text bare against a text key is the
/// type mismatch, refused with its way out; quoted it selects; bare against a time key it
/// selects that point.
#[test]
fn what_worklist_emits_pastes_back() {
    let fx = photos("Europe/Amsterdam", &[("a.jpg", OLD), ("b.jpg", OLD)]);
    let written = "2024-06-15T14:30:05.45+02:00";
    fx.import(&[(
        "a.jpg",
        serde_json::json!({"stamp": written, "taken": {"value": written, "type": "datetime"}}),
    )]);
    let emit = fx.run(&[
        "worklist",
        "--global",
        "--emit",
        "content.stamp",
        "--emit",
        "content.taken",
    ]);
    let emitted: Vec<serde_json::Value> = emit
        .stdout
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .filter(|v: &serde_json::Value| v["path"].as_str().unwrap().ends_with("/a.jpg"))
        .collect();
    let stamp = emitted[0]["facts"]["content.stamp"].as_str().unwrap();
    let taken = emitted[0]["facts"]["content.taken"].as_str().unwrap();

    let bare = format!("stamp={stamp}");
    let refused = fx.run(&["ls", "--global", "--where", &bare]);
    assert_ne!(refused.code, 0, "{bare} must be refused");
    assert!(
        refused
            .stderr
            .starts_with("Error: cannot run the filter as given\n")
            && refused.stderr.contains(&bare)
            && refused.stderr.contains(&format!(r#"stamp="{stamp}""#)),
        "{}",
        refused.stderr
    );
    assert_eq!(fx.listed(&format!(r#"stamp="{stamp}""#)), names(&["a.jpg"]));
    assert_eq!(fx.listed(&format!("taken={taken}")), names(&["a.jpg"]));
}

/// A refusal that is an answer about what a key is — a key holding two types — prints as it
/// stands, without `Error:`, on the command's own channel: stderr for a list command,
/// stdout for an effectful one; anything holding input Canon cannot read prints with
/// `Error:` on stderr, the answer beside it. Each exits non-zero and runs nothing.
#[test]
fn an_answer_prints_without_error_on_the_commands_channel() {
    let fx = photos("UTC", &[("a.jpg", OLD), ("b.jpg", OLD)]);
    fx.import(&[
        ("a.jpg", serde_json::json!({"mixed": "abc"})),
        ("b.jpg", serde_json::json!({"mixed": "def"})),
    ]);
    // Import keeps a key to one type, so the second type is written as an older Canon could.
    let db = rusqlite::Connection::open(fx.path("home/canon.db")).unwrap();
    let changed = db
        .execute(
            "UPDATE facts SET value_text = NULL, value_num = 5 \
             WHERE key = 'content.mixed' AND value_text = 'def'",
            [],
        )
        .unwrap();
    assert_eq!(changed, 1);
    drop(db);
    let answer = "mixed=5: mixed holds text and numbers; a filter compares one type only.";
    let way_out = "  `canon facts convert content.mixed`";

    let listed = fx.run(&["ls", "--global", "--where", "mixed=5"]);
    assert_ne!(listed.code, 0);
    assert!(listed.stdout.is_empty(), "{}", listed.stdout);
    assert!(
        listed.stderr.starts_with(answer) && listed.stderr.contains(way_out),
        "{}",
        listed.stderr
    );
    assert!(!listed.stderr.contains("Error:"), "{}", listed.stderr);

    std::fs::create_dir_all(fx.path("arc")).unwrap();
    let run = fx.run(&["scan", "--add", "--role", "archive", &fx.path_str("arc")]);
    assert_eq!(run.code, 0, "{}{}", run.stdout, run.stderr);
    let manifest = fx.path("m.toml");
    let generated = fx.run(&[
        "cluster",
        "generate",
        &fx.path_str("src"),
        "--dest",
        &format!("{}/out", fx.path_str("arc")),
        "--where",
        "mixed=5",
        "--output",
        &manifest.to_string_lossy(),
    ]);
    assert_ne!(generated.code, 0);
    assert!(
        generated.stdout.contains(answer)
            && generated.stdout.contains(way_out)
            && !generated.stdout.contains("Error:"),
        "{}{}",
        generated.stdout,
        generated.stderr
    );
    assert!(!generated.stderr.contains("Error:"), "{}", generated.stderr);
    assert!(!manifest.exists());

    let both = fx.run(&[
        "ls",
        "--global",
        "--where",
        "mixed=5 AND source.size=garbage",
    ]);
    assert_ne!(both.code, 0);
    assert!(both.stdout.is_empty(), "{}", both.stdout);
    assert!(
        both.stderr
            .starts_with("Error: cannot run the filter as given\n")
            && both.stderr.contains(answer)
            && both.stderr.contains("source.size=garbage"),
        "{}",
        both.stderr
    );
}

// ---------------------------------------------------------------------------
// How a refusal points at what was typed, on the built binary.
// ---------------------------------------------------------------------------

/// Run `args` and assert it was refused before anything ran: nothing on stdout, a non-zero
/// exit, no decision recorded. The refusal's text is returned.
fn refused(fx: &Fixture, args: &[&str]) -> String {
    let before = fx.decisions();
    let run = fx.run(args);
    assert_ne!(run.code, 0, "{args:?}:\n{}{}", run.stdout, run.stderr);
    assert!(run.stdout.is_empty(), "{args:?}:\n{}", run.stdout);
    assert_eq!(fx.decisions(), before, "{args:?} recorded a decision");
    run.stderr
}

/// A `ls` run that must succeed; its stdout.
fn listed(fx: &Fixture, filter: &str) -> String {
    let run = fx.run(&["ls", "--global", "--where", filter]);
    assert_eq!(run.code, 0, "{filter}: {}{}", run.stdout, run.stderr);
    run.stdout
}

/// **A refusal points at each problem**: under the first line, the typed line and a caret
/// under the first character of each problem with its sentence, every problem of the run in
/// order of position, each `--where` echoing its own text; a filter over several lines shows
/// only the line holding the problem; `facts --key` takes the key's first line.
#[test]
fn a_refusal_points_at_each_problem_with_a_caret() {
    let fx = photos("UTC", &[("a.jpg", OLD)]);
    let stderr = refused(
        &fx,
        &[
            "ls",
            "--global",
            "--where",
            "filename=a%b AND source.size=big",
            "--where",
            "source.ext=jpg AND Foo=1",
        ],
    );
    assert_eq!(
        stderr,
        "Error: cannot run the filter as given\n\
         \x20 filename=a%b AND source.size=big\n\
         \x20           ^ `%` needs quotes around the value: filename=\"a%b\"\n\
         \x20 filename=a%b AND source.size=big\n\
         \x20                              ^ source.size holds numbers; \"big\" is not a number.\n\
         \x20   Type a number: 1024, -5, 3.14, 1e20\n\
         \x20 source.ext=jpg AND Foo=1\n\
         \x20                    ^ `Foo` is an unknown key\n\
         \x20   `canon facts` lists the keys\n"
    );
    let stderr = refused(
        &fx,
        &[
            "ls",
            "--global",
            "--where",
            "filename=x\nAND source.size=big",
        ],
    );
    assert_eq!(
        stderr,
        "Error: cannot run the filter as given\n\
         \x20 AND source.size=big\n\
         \x20                 ^ source.size holds numbers; \"big\" is not a number.\n\
         \x20   Type a number: 1024, -5, 3.14, 1e20\n"
    );
    let stderr = refused(&fx, &["facts", "--global", "--key", "source.ext|yeer"]);
    assert!(
        stderr.starts_with(
            "Error: cannot use the key as given\n  source.ext|yeer\n            ^ `|yeer` is \
             not a modifier — Modifiers: "
        ),
        "{stderr}"
    );
}

/// **Plain values**: a value is read by the reading rules — a character that cannot stand bare
/// needs quotes, with the value quoted as the fix (the other quote mark where that needs no
/// doubling, a lone `]` among them); a space ends a value; in a list, a space between items is
/// named; a key of another type reads the value as its type; an unknown key is only unknown.
/// A lowercase `or`, a capital `AND` and letters of any language are values.
#[test]
fn plain_values_are_read_by_the_reading_rules() {
    let fx = photos("UTC", &[("a.jpg", OLD)]);
    for (filter, line) in [
        (
            "filename=a]b",
            "^ `]` needs quotes around the value: filename=\"a]b\"",
        ),
        (
            "filename=O'Brien",
            "^ `'` needs quotes around the value: filename=\"O'Brien\"",
        ),
        (
            "filename=O\"Brien",
            "^ `\"` needs quotes around the value: filename='O\"Brien'",
        ),
        (
            "filename IN (a.jpg b.jpg)",
            "^ values in a list are separated by commas\n",
        ),
        ("Model=EOS 5D", "^ a space separates values\n"),
        (
            "source.size=4%",
            "^ source.size holds numbers; \"4%\" is not a number.",
        ),
        ("Foo=a%b", "^ `Foo` is an unknown key\n"),
    ] {
        let stderr = refused(&fx, &["ls", "--global", "--where", filter]);
        assert!(stderr.contains(line), "{filter}:\n{stderr}");
        assert!(
            filter.starts_with("filename") || !stderr.contains("needs quotes"),
            "{filter}:\n{stderr}"
        );
    }
    for filter in ["filename=or", "filename=AND", "filename=照片1.jpg"] {
        listed(&fx, filter);
    }
}

/// **A wildcard after `=`**: named as a wildcard the operator does not take, with the glob and
/// the exact text; after `!=` the glob is `!~`; in a list, no fix; a key of another type reads
/// the value as its type.
#[test]
fn a_wildcard_after_equals_is_named_with_both_readings() {
    let fx = photos("UTC", &[("a.jpg", OLD)]);
    for (filter, line) in [
        (
            "filename=IMG*",
            "^ `*` is a wildcard, but `=` does not take wildcards. As a glob: filename~IMG*   As \
             exact text: filename=\"IMG*\"\n",
        ),
        (
            "filename!=IMG?",
            "^ `?` is a wildcard, but `!=` does not take wildcards. As a glob: filename!~IMG?   \
             As exact text: filename!=\"IMG?\"\n",
        ),
        (
            "filename IN (IMG*)",
            "^ `*` is a wildcard, but IN does not take wildcards\n",
        ),
        (
            "source.size=1*",
            "^ source.size holds numbers; \"1*\" is not a number.",
        ),
    ] {
        let stderr = refused(&fx, &["ls", "--global", "--where", filter]);
        assert!(stderr.contains(line), "{filter}:\n{stderr}");
    }
}

/// The empty-key sentences: an empty `--key`, and a `--group-by` list with nothing after or
/// before a comma.
#[test]
fn the_empty_key_sentences_name_where_a_key_is_missing() {
    let fx = photos("UTC", &[("a.jpg", OLD)]);
    let stderr = refused(&fx, &["facts", "--global", "--key", ""]);
    assert_eq!(
        stderr,
        "Error: cannot use the key as given\n  the key is empty\n"
    );
    for (grouping, line) in [
        ("Make,", "       ^ a key is missing after `,`\n"),
        (",Make", "  ^ a key is missing before `,`\n"),
    ] {
        let stderr = refused(
            &fx,
            &[
                "facts",
                "--global",
                "--key",
                "source.ext",
                "--group-by",
                grouping,
            ],
        );
        assert!(
            stderr.starts_with("Error: cannot use the key as given\n")
                && stderr.contains(&format!("\n  {grouping}\n{line}")),
            "{grouping}:\n{stderr}"
        );
    }
    let run = fx.run(&[
        "facts",
        "--global",
        "--key",
        "source.ext",
        "--group-by",
        "Make, source.size|bucket(60,300)",
    ]);
    assert_eq!(run.code, 0, "{}{}", run.stdout, run.stderr);
}

/// A report names its key as the key was read, spaces around it dropped, in the plain header
/// and in the grouped one alike.
#[test]
fn a_report_names_its_key_as_read_in_both_headers() {
    let fx = photos("UTC", &[("a.jpg", OLD)]);
    fx.import(&[(
        "a.jpg",
        serde_json::json!({"Make": "Canon", "Model": "EOS"}),
    )]);
    let plain = fx.run(&["facts", "--global", "--key", " Make "]);
    assert_eq!(plain.code, 0, "{}{}", plain.stdout, plain.stderr);
    let header = format!("{:<40} {:>10} {:>10}\n", "Make", "Count", "Coverage");
    assert!(plain.stdout.contains(&header), "{}", plain.stdout);
    let grouped = fx.run(&[
        "facts",
        "--global",
        "--key",
        " Make ",
        "--group-by",
        "Model ",
    ]);
    assert_eq!(grouped.code, 0, "{}{}", grouped.stdout, grouped.stderr);
    assert!(
        grouped.stdout.contains("\nMake (grouped by Model)\n"),
        "{}",
        grouped.stdout
    );
}

/// `worklist`'s `--emit` names and its filter are refused together, the filter's first line
/// leading and each name on its own line after the filter's blocks.
#[test]
fn worklist_names_an_emit_name_beside_a_refused_filter() {
    let fx = photos("UTC", &[("a.jpg", OLD)]);
    fx.import(&[("a.jpg", serde_json::json!({"Rating": 5}))]);
    let stderr = refused(
        &fx,
        &[
            "worklist",
            "--global",
            "--where",
            "Rating=big",
            "--emit",
            "source.extension",
        ],
    );
    assert_eq!(
        stderr,
        "Error: cannot run the filter as given\n\
         \x20 Rating=big\n\
         \x20        ^ Rating holds numbers; \"big\" is not a number.\n\
         \x20   Type a number: 1024, -5, 3.14, 1e20\n\
         \x20 `source.extension` is an unknown key\n\
         \x20   `canon facts` lists the keys\n"
    );
}

/// A quoted word is never a status word: `"archived"?` asks whether the stored fact
/// `content.archived` has a value, and `NOT "archived"?` selects every source without it —
/// where `NOT archived?`, the status word, selects every source not archived.
#[test]
fn a_quoted_status_word_names_a_stored_fact() {
    let fx = photos("UTC", &[("a.jpg", OLD), ("b.jpg", OLD)]);
    fx.import(&[("a.jpg", serde_json::json!({"archived": "yes"}))]);
    let quoted = listed(&fx, "\"archived\"?");
    assert!(
        quoted.contains("a.jpg") && !quoted.contains("b.jpg"),
        "{quoted}"
    );
    let not_quoted = listed(&fx, "NOT \"archived\"?");
    assert!(
        !not_quoted.contains("a.jpg") && not_quoted.contains("b.jpg"),
        "{not_quoted}"
    );
    let status = listed(&fx, "NOT archived?");
    assert!(
        status.contains("a.jpg") && status.contains("b.jpg"),
        "{status}"
    );
}

/// An effectful command refused for a value that needs quotes records nothing, under `--yes`.
#[test]
fn a_value_that_needs_quotes_stops_exclude_set_under_yes() {
    let fx = Fixture::new();
    let stderr = refused(
        &fx,
        &[
            "exclude",
            "set",
            "--global",
            "--where",
            "filename=a%b",
            "--yes",
        ],
    );
    assert!(
        stderr.starts_with("Error: cannot run the filter as given\n"),
        "{stderr}"
    );
}

/// A manifest generated with `source.ext=jpg`, its recorded query then edited to `query`.
fn manifest_with_query(fx: &Fixture, query: &str) -> PathBuf {
    let manifest = fx.path("m.toml");
    let (src, dest) = (fx.path_str("src"), format!("{}/out", fx.path_str("arc")));
    let run = fx.run(&[
        "cluster",
        "generate",
        &src,
        "--dest",
        &dest,
        "--where",
        "source.ext=jpg",
        "--allow",
        "duplicates",
        "--output",
        &manifest.to_string_lossy(),
    ]);
    assert_eq!(run.code, 0, "{}{}", run.stdout, run.stderr);
    let config = std::fs::read_to_string(&manifest).unwrap();
    let edited = config.replace(
        r#"query = ["source.ext=jpg"]"#,
        &format!("query = [{query:?}]"),
    );
    assert_ne!(
        config, edited,
        "the query line is where expected:\n{config}"
    );
    std::fs::write(&manifest, edited).unwrap();
    manifest
}

/// **A recorded query is refused as specified**: `cluster refresh` gives the filter's first
/// line for a manifest, the caret blocks, the way back alone after them and the abort; `cluster
/// status` names the refresh it would reject and the way back on one line, the blocks under it.
#[test]
fn a_recorded_query_is_refused_as_specified_at_refresh_and_nested_at_status() {
    let fx = Fixture::new();
    let manifest = manifest_with_query(&fx, "source.ext =");
    let m = manifest.to_string_lossy();
    let refresh = fx.run(&["cluster", "refresh", &m]);
    assert_ne!(refresh.code, 0);
    assert_eq!(
        refresh.stderr,
        format!(
            "Options from manifest: allow duplicates\n\
             Error: cannot run the filter as specified\n\
             \x20 source.ext =\n\
             \x20             ^ a value is missing after `=`\n\
             `canon cluster refresh --edit {m}` to fix the query and select again\n\
             Refresh aborted — {m} and its lock file are unchanged.\n"
        )
    );
    let status = fx.run(&["cluster", "status", &m]);
    let block = format!(
        "\ncluster refresh would be rejected: the recorded query cannot run — `canon cluster \
         refresh --edit {m}` to fix the query and select again\n\
         \x20   source.ext =\n\
         \x20               ^ a value is missing after `=`\n"
    );
    assert!(
        status.stdout.contains(&block),
        "{}{}",
        status.stdout,
        status.stderr
    );
    assert!(!status.stdout.contains("Error:"), "{}", status.stdout);
}

/// **The query block stands at the query's place in every arm of a refusing refresh**: after a
/// parked scope's door lines (stdout, no `Error:`, exit 1) and after "no sources known" for a
/// scope that kept nothing — the block whole, then the abort once.
#[test]
fn the_refresh_query_block_stands_at_the_querys_place_in_every_arm() {
    let order = |out: &str, parts: &[&str]| {
        let at: Vec<usize> = parts
            .iter()
            .map(|p| out.find(p).unwrap_or_else(|| panic!("{p} missing:\n{out}")))
            .collect();
        assert!(
            at.windows(2).all(|w| w[0] < w[1]),
            "{parts:?} out of order:\n{out}"
        );
        assert_eq!(out.matches("Refresh aborted").count(), 1, "{out}");
    };
    let block = [
        "cannot run the filter as specified\n  source.ext =\n              ^ a value is missing \
         after `=`\n`canon cluster refresh --edit ",
        "Refresh aborted",
    ];

    let fx = Fixture::new();
    let manifest = manifest_with_query(&fx, "source.ext =");
    let suspended = fx.run(&["roots", "suspend", &format!("path:{}", fx.path_str("src"))]);
    assert_eq!(
        suspended.code, 0,
        "{}{}",
        suspended.stdout, suspended.stderr
    );
    let refresh = fx.run(&["cluster", "refresh", &manifest.to_string_lossy()]);
    assert_eq!(refresh.code, 1, "{}{}", refresh.stdout, refresh.stderr);
    assert!(!refresh.stdout.contains("Error:") && !refresh.stderr.contains("Error:"));
    order(&refresh.stdout, &["suspended", block[0], block[1]]);

    let fx = Fixture::new();
    std::fs::create_dir_all(fx.path("src/empty")).unwrap();
    let manifest = manifest_with_query(&fx, "source.ext =");
    let config = std::fs::read_to_string(&manifest).unwrap();
    let edited: Vec<String> = config
        .lines()
        .map(|line| {
            if line.starts_with("scope = ") {
                format!("scope = [{:?}]", fx.path_str("src/empty"))
            } else {
                line.to_string()
            }
        })
        .collect();
    assert_ne!(
        edited.join("\n") + "\n",
        config,
        "the scope line is where expected"
    );
    std::fs::write(&manifest, edited.join("\n") + "\n").unwrap();
    let refresh = fx.run(&["cluster", "refresh", &manifest.to_string_lossy()]);
    assert_ne!(refresh.code, 0, "{}{}", refresh.stdout, refresh.stderr);
    order(
        &refresh.stderr,
        &["no sources known at", block[0], block[1]],
    );
}
