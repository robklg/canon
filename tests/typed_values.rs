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

/// A refusal that is an answer about what a key is — a key of mixed type — prints as it
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
    let answer =
        "mixed=5: mixed is of mixed type (text and number); a filter supports one type only\n";
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

/// **A key of mixed type is named with its types on every surface that names one**: a filter
/// ("a filter supports one type only") and a report's modifier ("a modifier supports one type
/// only"), each with its counts and the convert line; the `facts` listing, `cluster generate`'s
/// warning and the manifest's comment, one key named with its types and several counted, then
/// each named. The convert line names the full stored key, quoted for the shell only.
#[test]
fn a_mixed_type_key_is_named_with_its_types() {
    let fx = photos("UTC", &[("a.jpg", OLD), ("b.jpg", OLD)]);
    fx.import(&[
        (
            "a.jpg",
            serde_json::json!({"Model": "EOS", "File Size": "380 KB"}),
        ),
        (
            "b.jpg",
            serde_json::json!({"Model": "R5", "File Size": "41 KB"}),
        ),
    ]);
    // Import keeps a key to one type, so the second type is written as an older Canon could.
    let second_type = |key: &str, text: &str| {
        let db = rusqlite::Connection::open(fx.path("home/canon.db")).unwrap();
        let changed = db
            .execute(
                "UPDATE facts SET value_text = NULL, value_num = 5 \
                 WHERE key = ?1 AND value_text = ?2",
                [key, text],
            )
            .unwrap();
        assert_eq!(changed, 1);
    };
    second_type("content.Model", "R5");

    let counts = "  text: 1 fact   numbers: 1 fact   (every fact of the key, on every root)";
    let listed = fx.run(&["ls", "--global", "--where", "Model=EOS"]);
    assert_eq!(
        listed.stderr,
        format!(
            "Model=EOS: Model is of mixed type (text and number); a filter supports one type \
             only\n{counts}\nTo see its values and convert it to one type:\n  `canon facts \
             convert content.Model`\n"
        )
    );
    let report = fx.run(&["facts", "--global", "--key", "Model|lowercase"]);
    assert_ne!(report.code, 0);
    let said = format!("{}{}", report.stdout, report.stderr);
    assert!(
        said.contains(
            "Model|lowercase: Model is of mixed type (text and number); a modifier supports one \
             type only\n"
        ),
        "{said}"
    );

    let statement = "content.Model is of mixed type (text and number); filters and patterns \
                     support one type only.\nTo see its values and convert it to one type:\n  \
                     `canon facts convert content.Model`\n";
    let keys = fx.run(&["facts", "--global"]);
    assert_eq!(keys.code, 0, "{}{}", keys.stdout, keys.stderr);
    assert!(keys.stdout.contains(statement), "{}", keys.stdout);

    std::fs::create_dir_all(fx.path("arc")).unwrap();
    let run = fx.run(&["scan", "--add", "--role", "archive", &fx.path_str("arc")]);
    assert_eq!(run.code, 0, "{}{}", run.stdout, run.stderr);
    let manifest = fx.path("m.toml");
    let generate = |output: &std::path::Path| {
        fx.run(&[
            "cluster",
            "generate",
            &fx.path_str("src"),
            "--dest",
            &format!("{}/out", fx.path_str("arc")),
            "--output",
            &output.to_string_lossy(),
        ])
    };
    let generated = generate(&manifest);
    assert_eq!(
        generated.code, 0,
        "{}{}",
        generated.stdout, generated.stderr
    );
    assert!(generated.stderr.contains(statement), "{}", generated.stderr);
    let written = std::fs::read_to_string(&manifest).unwrap();
    assert!(
        written.contains(
            "#     To see its values and convert it to one type:\n\
             #       `canon facts convert content.Model`\n"
        ),
        "{written}"
    );

    second_type("content.File Size", "41 KB");
    let several = "2 keys are of mixed type; filters and patterns support one type only:\n  \
                   \"content.File Size\" (text and number)\n  content.Model (text and number)\n\
                   To see their values and convert each to one type:\n  `canon facts convert \
                   'content.File Size'`\n  `canon facts convert content.Model`\n";
    let keys = fx.run(&["facts", "--global"]);
    assert!(keys.stdout.contains(several), "{}", keys.stdout);
    let generated = generate(&fx.path("m2.toml"));
    assert_eq!(
        generated.code, 0,
        "{}{}",
        generated.stdout, generated.stderr
    );
    assert!(generated.stderr.contains(several), "{}", generated.stderr);
    let written = std::fs::read_to_string(fx.path("m2.toml")).unwrap();
    assert!(
        written.contains("#       `canon facts convert 'content.File Size'`\n"),
        "{written}"
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
         \x20                              ^ source.size is of type number; `big` is not a number\n\
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
         \x20                 ^ source.size is of type number; `big` is not a number\n\
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
            "^ source.size is of type number; `4%` is not a number",
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
            "^ source.size is of type number; `1*` is not a number",
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
         \x20        ^ Rating is of type number; `big` is not a number\n\
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
/// status` names the refresh it would reject, the blocks under it and the way back after them.
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
        "\ncluster refresh would be rejected: the recorded query cannot run\n\
         \x20   source.ext =\n\
         \x20               ^ a value is missing after `=`\n\
         \x20   `canon cluster refresh --edit {m}` to fix the query and select again\n"
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
/// parked scope's door lines and after "no sources known" for a scope that kept nothing — the
/// block whole, then the abort once. Beside the door the query holds input Canon cannot read,
/// so the door lines stay on stdout with no `Error:`, and the refusal goes to stderr with
/// `Error:` before the query block; exit 1.
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
    assert!(refresh.stdout.contains("suspended"), "{}", refresh.stdout);
    assert!(!refresh.stdout.contains("Error:"), "{}", refresh.stdout);
    assert!(
        !refresh.stdout.contains("Refresh aborted"),
        "{}",
        refresh.stdout
    );
    assert!(
        refresh.stderr.contains(&format!("\nError: {}", block[0])),
        "{}",
        refresh.stderr
    );
    assert_eq!(
        refresh.stderr.matches("Error:").count(),
        1,
        "{}",
        refresh.stderr
    );
    order(&refresh.stderr, &["Error: ", block[0], block[1]]);

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

/// A manifest whose recorded query is `query`, over two files of different content where
/// `Model` holds text on `a.jpg` and a number on `b.jpg` — a key of two types.
fn mixed_model_manifest(query: &str) -> (Fixture, PathBuf) {
    let fx = photos("UTC", &[("a.jpg", OLD), ("b.jpg", OLD)]);
    fx.import(&[
        ("a.jpg", serde_json::json!({"Model": "EOS"})),
        ("b.jpg", serde_json::json!({"Model": "R5"})),
    ]);
    // Import keeps a key to one type, so the second type is written as an older Canon could.
    let db = rusqlite::Connection::open(fx.path("home/canon.db")).unwrap();
    let changed = db
        .execute(
            "UPDATE facts SET value_text = NULL, value_num = 5 \
             WHERE key = 'content.Model' AND value_text = 'R5'",
            [],
        )
        .unwrap();
    assert_eq!(changed, 1);
    drop(db);
    std::fs::create_dir_all(fx.path("arc")).unwrap();
    let run = fx.run(&["scan", "--add", "--role", "archive", &fx.path_str("arc")]);
    assert_eq!(run.code, 0, "{}{}", run.stdout, run.stderr);
    let manifest = manifest_with_query(&fx, query);
    (fx, manifest)
}

/// **A refused refresh names the way back that fits the cause**, and `cluster status` names the
/// same one after the blocks. A value that is not a number is input Canon cannot read: `Error:`
/// on stderr, and `--edit`. A key holding two types is an answer: on stdout with no `Error:`,
/// the conversion the refusal names and then a plain refresh, never `--edit`. Both at once:
/// `Error:`, and one line taking both steps. The manifest and its lock are unchanged to the
/// byte; status exits 0.
#[test]
fn a_refused_refresh_names_the_way_back_by_cause() {
    let typo = "source.size = garbage";
    let two_types = "Model = EOS";
    let both = "Model = EOS AND source.size = garbage";
    for query in [typo, two_types, both] {
        let (fx, manifest) = mixed_model_manifest(query);
        let m = manifest.to_string_lossy().into_owned();
        let lock = manifest.with_extension("lock");
        let (config, lock_bytes) = (
            std::fs::read(&manifest).unwrap(),
            std::fs::read(&lock).unwrap(),
        );
        let edit = format!("`canon cluster refresh --edit {m}` to fix the query and select again");
        let convert_then_refresh =
            format!("After converting, `canon cluster refresh {m}` to select again");
        let convert_then_edit =
            format!("After converting, `canon cluster refresh --edit {m}` to fix the query and select again");
        let way_back = match query {
            q if q == typo => edit.clone(),
            q if q == two_types => convert_then_refresh.clone(),
            _ => convert_then_edit.clone(),
        };

        let refresh = fx.run(&["cluster", "refresh", &m]);
        assert_ne!(refresh.code, 0, "{query}");
        let said = format!("{}{}", refresh.stdout, refresh.stderr);
        // Whole lines: the plain `--edit` line is part of the combined one.
        assert_eq!(lines_saying(&said, &way_back), 1, "{query}: {said}");
        for other in [&edit, &convert_then_refresh, &convert_then_edit] {
            if *other != way_back {
                assert_eq!(lines_saying(&said, other), 0, "{query}: {said}");
            }
        }
        if query == two_types {
            assert!(!said.contains("Error:"), "{said}");
            assert!(!said.contains("--edit"), "{said}");
            assert!(
                refresh.stdout.contains("Model is of mixed type")
                    && refresh
                        .stdout
                        .contains("`canon facts convert content.Model`")
                    && refresh.stdout.contains(&way_back)
                    && refresh.stdout.contains("Refresh aborted"),
                "{said}"
            );
            let convert = refresh.stdout.find("canon facts convert").unwrap();
            assert!(convert < refresh.stdout.find(&way_back).unwrap(), "{said}");
        } else {
            assert!(!refresh.stdout.contains("Error:"), "{query}: {said}");
            assert_eq!(
                refresh.stderr.matches("Error:").count(),
                1,
                "{query}: {said}"
            );
            assert!(refresh.stderr.contains(&way_back), "{query}: {said}");
        }
        assert_eq!(std::fs::read(&manifest).unwrap(), config, "{query}");
        assert_eq!(std::fs::read(&lock).unwrap(), lock_bytes, "{query}");

        let status = fx.run(&["cluster", "status", &m]);
        assert_eq!(
            status.code, 0,
            "{query}: {}{}",
            status.stdout, status.stderr
        );
        let lead = "cluster refresh would be rejected: the recorded query cannot run\n";
        let (_, after) = status
            .stdout
            .split_once(lead)
            .unwrap_or_else(|| panic!("{query}: {}", status.stdout));
        let line = format!("\n    {way_back}\n");
        assert!(after.contains(&line), "{query}: {}", status.stdout);
        assert!(
            !status.stdout.contains("Error:"),
            "{query}: {}",
            status.stdout
        );
        for other in [&edit, &convert_then_refresh, &convert_then_edit] {
            if *other != way_back {
                assert_eq!(
                    lines_saying(&status.stdout, other),
                    0,
                    "{query}: {}",
                    status.stdout
                );
            }
        }
    }
}

/// Rewrite a manifest's `meta.scope` to the given paths.
fn set_scope(manifest: &std::path::Path, paths: &[String]) {
    let config = std::fs::read_to_string(manifest).unwrap();
    let edited: Vec<String> = config
        .lines()
        .map(|line| {
            if line.starts_with("scope = ") {
                format!("scope = {paths:?}")
            } else {
                line.to_string()
            }
        })
        .collect();
    std::fs::write(manifest, edited.join("\n") + "\n").unwrap();
}

/// The lines of `out` equal, once trimmed, to `line`.
fn lines_saying(out: &str, line: &str) -> usize {
    out.lines().filter(|l| l.trim() == line).count()
}

/// **Beside a scope that kept nothing, the scope's way back comes first and no refresh it turns
/// away is named.** A manifest scoped to a folder Canon has no sources under, whose query names
/// a key of two types or holds a value Canon cannot read: `cluster refresh` prints `no sources
/// known`, the scan-or-edit pair, the query's refusal and its way back last, then the abort.
/// `cluster status` states the query's refusal without its way back; where the report owes the
/// scope's way back — no lock, so no step is named — the scope's two answers come first and the
/// query's way back last, once, as in the remedy for a source file the world has lost. Where the
/// report names a step (`To apply`), neither refresh cause owes an act and none is named.
#[test]
fn a_scope_that_kept_nothing_leads_the_steps_and_no_turned_away_refresh_is_named() {
    for query in ["Model = EOS", "source.size = garbage"] {
        let (fx, manifest) = mixed_model_manifest(query);
        std::fs::create_dir_all(fx.path("src/empty")).unwrap();
        set_scope(&manifest, &[fx.path_str("src/empty")]);
        let m = manifest.to_string_lossy().into_owned();
        let way_back = if query == "Model = EOS" {
            format!("After converting, `canon cluster refresh {m}` to select again")
        } else {
            format!("`canon cluster refresh --edit {m}` to fix the query and select again")
        };

        let refresh = fx.run(&["cluster", "refresh", &m]);
        assert_ne!(refresh.code, 0, "{}{}", refresh.stdout, refresh.stderr);
        let mut rest = refresh.stderr.as_str();
        for part in [
            "Error: no sources known at",
            "If the files are there",
            "If not, edit meta.scope",
            query,
            way_back.as_str(),
            "Refresh aborted",
        ] {
            rest = rest
                .split_once(part)
                .map(|(_, after)| after)
                .unwrap_or_else(|| panic!("{part} missing or out of order:\n{}", refresh.stderr));
        }
        assert!(
            !rest.trim_end().contains('\n'),
            "the abort ends it:\n{}",
            refresh.stderr
        );

        // Entries pending: the step is the apply, and neither refresh cause owes an act.
        let status = fx.run(&["cluster", "status", &m]);
        assert_eq!(status.code, 0, "{}{}", status.stdout, status.stderr);
        let out = status.stdout.as_str();
        assert!(
            out.contains("no sources known at")
                && out.contains("cluster refresh would be rejected: the recorded query cannot run")
                && out.contains("To apply:"),
            "{query}: {out}"
        );
        assert!(
            !out.contains("canon cluster refresh"),
            "{query}: a step is named, so no refresh is: {out}"
        );

        std::fs::rename(fx.path("src/a.jpg"), fx.path("a.moved")).unwrap();
        // A source file lost: its remedy carries the scope's way back, and the query's after it.
        let lost = fx.run(&["cluster", "status", &m]);
        std::fs::rename(fx.path("a.moved"), fx.path("src/a.jpg")).unwrap();
        let out = lost.stdout.as_str();
        assert_eq!(lines_saying(out, &way_back), 1, "{query}: {out}");
        let tail = out
            .split_once("Check if the source volume is connected.")
            .map(|(_, after)| after.trim())
            .unwrap_or_else(|| panic!("{query}: no remedy:\n{out}"));
        assert!(
            tail.split_once("If not, edit meta.scope")
                .is_some_and(|(_, after)| after.trim_end().ends_with(way_back.as_str())),
            "{query}: the query's way back follows the scope's:\n{out}"
        );
        // No lock: no step is named, so the scope's way back is owed, and the query's follows it.
        std::fs::remove_file(manifest.with_extension("lock")).unwrap();
        let status = fx.run(&["cluster", "status", &m]);
        assert_eq!(status.code, 0, "{}{}", status.stdout, status.stderr);
        let out = status.stdout.as_str();
        assert_eq!(lines_saying(out, &way_back), 1, "{query}: {out}");
        let mut rest = out;
        for part in [
            "no sources known at",
            "cluster refresh would be rejected: the recorded query cannot run",
            "No sources are known under the scopes declared in this manifest",
            "If the files are there",
            "If not, edit meta.scope",
            way_back.as_str(),
        ] {
            rest = rest
                .split_once(part)
                .map(|(_, after)| after)
                .unwrap_or_else(|| panic!("{query}: {part} missing or out of order:\n{out}"));
        }
        assert!(
            rest.trim().is_empty(),
            "{query}: the query's way back ends it:\n{out}"
        );
    }
}

/// **A closed door outranks the scope that kept nothing, so the query's block names its own way
/// back at status as the refresh does.** Scope: a suspended root and a folder Canon has no
/// sources under. The refresh is turned away at the door, whose line names `canon roots
/// unsuspend`; once it is open the scope keeps that root's sources and the query is what turns the
/// refresh away. Both commands name the query's way back, once.
#[test]
fn beside_a_door_the_query_names_its_way_back_though_the_scope_kept_nothing() {
    let (fx, manifest) = mixed_model_manifest("Model = EOS");
    std::fs::create_dir_all(fx.path("other")).unwrap();
    std::fs::write(fx.path("other/c.jpg"), "c").unwrap();
    let other = fx.path_str("other");
    let by_path = format!("path:{other}");
    for args in [
        ["scan", "--add", "--role", "source", other.as_str()].as_slice(),
        ["roots", "suspend", by_path.as_str()].as_slice(),
    ] {
        let run = fx.run(args);
        assert_eq!(run.code, 0, "{}{}", run.stdout, run.stderr);
    }
    std::fs::create_dir_all(fx.path("src/empty")).unwrap();
    set_scope(&manifest, &[fx.path_str("other"), fx.path_str("src/empty")]);
    let m = manifest.to_string_lossy().into_owned();
    let way_back = format!("After converting, `canon cluster refresh {m}` to select again");

    let refresh = fx.run(&["cluster", "refresh", &m]);
    assert_eq!(refresh.code, 1, "{}{}", refresh.stdout, refresh.stderr);
    assert_eq!(
        lines_saying(&refresh.stdout, &way_back),
        1,
        "{}{}",
        refresh.stdout,
        refresh.stderr
    );

    let status = fx.run(&["cluster", "status", &m]);
    assert_eq!(status.code, 0, "{}{}", status.stdout, status.stderr);
    let out = status.stdout.as_str();
    assert!(out.contains("canon roots unsuspend"), "{out}");
    assert_eq!(lines_saying(out, &way_back), 1, "{out}");
}

/// **A closed door beside a key holding two types is an answer**: the door lines, the refusal,
/// the plain way back and the abort, all on stdout, with no `Error:`; exit 1.
#[test]
fn a_door_beside_a_two_type_key_is_an_answer() {
    let (fx, manifest) = mixed_model_manifest("Model = EOS");
    let m = manifest.to_string_lossy().into_owned();
    let suspended = fx.run(&["roots", "suspend", &format!("path:{}", fx.path_str("src"))]);
    assert_eq!(
        suspended.code, 0,
        "{}{}",
        suspended.stdout, suspended.stderr
    );

    let refresh = fx.run(&["cluster", "refresh", &m]);
    assert_eq!(refresh.code, 1, "{}{}", refresh.stdout, refresh.stderr);
    let way_back = format!("After converting, `canon cluster refresh {m}` to select again");
    let mut rest = refresh.stdout.as_str();
    for part in [
        "suspended",
        "Model is of mixed type",
        &way_back,
        "Refresh aborted",
    ] {
        rest = rest
            .split_once(part)
            .map(|(_, after)| after)
            .unwrap_or_else(|| panic!("{part} missing or out of order:\n{}", refresh.stdout));
    }
    assert!(!refresh.stdout.contains("Error:"), "{}", refresh.stdout);
    assert!(!refresh.stderr.contains("Error:"), "{}", refresh.stderr);
    assert!(!refresh.stderr.contains("mixed type"), "{}", refresh.stderr);
    assert!(!refresh.stdout.contains("--edit"), "{}", refresh.stdout);
}

/// Scan an archive root beside the fixture's sources, for the manifest commands.
fn with_archive(fx: &Fixture) {
    std::fs::create_dir_all(fx.path("arc")).unwrap();
    let run = fx.run(&["scan", "--add", "--role", "archive", &fx.path_str("arc")]);
    assert_eq!(run.code, 0, "{}{}", run.stdout, run.stderr);
}

/// The line directly under the first line of `out` that starts, once trimmed, with `count`.
fn under<'a>(out: &'a str, count: &str) -> &'a str {
    let mut lines = out.lines();
    lines
        .find(|l| l.trim_start().starts_with(count))
        .unwrap_or_else(|| panic!("{count:?} missing:\n{out}"));
    lines
        .next()
        .unwrap_or_else(|| panic!("nothing under {count:?}:\n{out}"))
}

/// **Every command that acts on a `--where` selection says how much of "everything else" has
/// no value**, directly under its count: `Make != Canon` selects five sources, four with no
/// `Make`. On `exclude set` under `--dry-run`, at the confirmation answered `n` and under
/// `--yes`; on `exclude clear`, `exclude duplicates` (the one copy it would exclude), `exclude
/// set-object` (counted in objects), `cluster generate` (on the screen and in the manifest),
/// `cluster refresh`, and `facts delete` in its own count, dry run and `--yes`. The trail lists
/// the head alone; `trail show` gives the note under it.
#[test]
fn the_plan_says_how_much_of_everything_else_has_no_value() {
    let names = ["a.jpg", "b.jpg", "c.jpg", "d.jpg", "e.jpg"];
    let files: Vec<(&str, i64)> = names.iter().map(|n| (*n, OLD)).collect();
    let fx = photos("UTC", &files);
    // A second copy of `e.jpg`'s content, for the duplicates command.
    std::fs::create_dir_all(fx.path("src/dup")).unwrap();
    std::fs::write(fx.path("src/dup/e.jpg"), "e.jpg").unwrap();
    let run = fx.run(&["scan", &fx.path_str("src")]);
    assert_eq!(run.code, 0, "{}{}", run.stdout, run.stderr);
    fx.import(&[
        ("a.jpg", serde_json::json!({"Make": "Canon"})),
        ("b.jpg", serde_json::json!({"Make": "Nikon"})),
    ]);
    with_archive(&fx);
    let src = fx.path_str("src");
    let filter = "Make != Canon";
    let note = "  of which 4 have no Make";

    let dry = fx.run(&["exclude", "set", "--where", filter, "--dry-run", &src]);
    assert_eq!(
        under(&dry.stdout, "Would exclude 5 sources"),
        note,
        "{}",
        dry.stdout
    );
    let declined = fx.run_with_input(&["exclude", "set", "--where", filter, &src], Some("n\n"));
    assert_eq!(
        under(&declined.stderr, "Will exclude 5 sources"),
        note,
        "{}{}",
        declined.stdout,
        declined.stderr
    );
    let objects = fx.run(&["exclude", "set-object", "--where", filter, &src]);
    assert_eq!(
        under(&objects.stdout, "Would exclude 4 objects"),
        "  of which 3 objects have no Make",
        "{}",
        objects.stdout
    );
    let prefer = fx.path_str("src/dup");
    let duplicates = fx.run(&[
        "exclude",
        "duplicates",
        "--where",
        filter,
        "--prefer",
        &prefer,
        "--dry-run",
        &src,
    ]);
    assert_eq!(
        under(&duplicates.stdout, "Would exclude 1 sources"),
        "  of which 1 has no Make",
        "{}",
        duplicates.stdout
    );

    let manifest = fx.path("m.toml");
    let m = manifest.to_string_lossy().into_owned();
    let dest = format!("{}/out", fx.path_str("arc"));
    let generated = fx.run(&[
        "cluster",
        "generate",
        &src,
        "--dest",
        &dest,
        "--where",
        filter,
        "--allow",
        "duplicates",
        "--no-edit",
        "--output",
        &m,
    ]);
    assert_eq!(
        under(&generated.stdout, "Generated manifest"),
        note,
        "{}",
        generated.stdout
    );
    let written = std::fs::read_to_string(&manifest).unwrap();
    assert!(
        written.contains("\n# 5 sources\n#   of which 4 have no Make\n"),
        "{written}"
    );
    let refreshed = fx.run(&["cluster", "refresh", &m]);
    assert_eq!(
        under(&refreshed.stdout, "Refreshed lock file"),
        note,
        "{}",
        refreshed.stdout
    );

    let acted = fx.run(&["exclude", "set", "--where", filter, "--yes", &src]);
    assert_eq!(
        under(&acted.stdout, "Excluded 5 sources"),
        note,
        "{}",
        acted.stdout
    );
    let listed = fx.run(&["trail", "--global"]);
    let line = listed
        .stdout
        .lines()
        .find(|l| l.contains("Excluded 5 sources"))
        .unwrap_or_else(|| panic!("{}", listed.stdout));
    assert!(line.ends_with("Excluded 5 sources"), "{line}");
    assert!(!listed.stdout.contains("of which"), "{}", listed.stdout);
    let jsonl = fx.run(&["trail", "--global", "--jsonl"]);
    let id = jsonl
        .stdout
        .lines()
        .map(|l| serde_json::from_str::<serde_json::Value>(l).unwrap())
        .find(|v| v["command"] == "exclude_set")
        .and_then(|v| v["id"].as_i64())
        .unwrap_or_else(|| panic!("{}", jsonl.stdout));
    let shown = fx.run(&["trail", "show", &id.to_string()]);
    assert_eq!(under(&shown.stdout, "summary:"), note, "{}", shown.stdout);

    let cleared = fx.run(&["exclude", "clear", "--where", filter, "--yes", &src]);
    assert_eq!(
        under(&cleared.stdout, "Cleared exclusions for 5 sources"),
        note,
        "{}",
        cleared.stdout
    );

    let deletes = [
        "facts",
        "delete",
        "--on",
        "object",
        "content.Make",
        "--where",
        filter,
        &src,
    ];
    let facts_note = "  from 5 matching sources, 4 of which have no Make";
    let would = fx.run(&deletes);
    assert_eq!(
        under(&would.stdout, "Would delete 1"),
        facts_note,
        "{}",
        would.stdout
    );
    let mut yes = deletes.to_vec();
    yes.push("--yes");
    let deleted = fx.run(&yes);
    assert_eq!(
        under(&deleted.stdout, "Deleted 1"),
        facts_note,
        "{}",
        deleted.stdout
    );
}

/// **A stored time coarser than the date typed is named where the comparison could not place
/// it**: two values known only to 2024 against `2024-06`. Under `Taken < 2024-06` they are not
/// selected; under `NOT Taken >= 2024-06` and `Taken != 2024-06` they are; under `Taken IN
/// (2024-06)` nothing is, and the line stands under the "no sources" sentence — on `exclude
/// set --dry-run` and `--yes` alike. Where only coarse values stand, `Taken < 2024-06` empties
/// the selection, and `cluster refresh` says so on the screen and in the manifest.
#[test]
fn the_spanning_line_names_values_the_comparison_could_not_place() {
    let line = |direction: &str| {
        format!("  2 sources have a time that spans 2024-06 (coarser than a month): 2 {direction}")
    };
    let rows = [
        (
            "Taken < 2024-06",
            Some("Would exclude 1 sources"),
            Some("Excluded 1 source"),
            "not selected",
        ),
        (
            "NOT Taken >= 2024-06",
            Some("Would exclude 3 sources"),
            Some("Excluded 3 sources"),
            "selected",
        ),
        (
            "Taken != 2024-06",
            Some("Would exclude 4 sources"),
            Some("Excluded 4 sources"),
            "selected",
        ),
        ("Taken IN (2024-06)", None, None, "not selected"),
    ];
    for (filter, would, did, direction) in rows {
        for flag in ["--dry-run", "--yes"] {
            let fx = photos(
                "UTC",
                &[
                    ("a.jpg", OLD),
                    ("b.jpg", OLD),
                    ("c.jpg", OLD),
                    ("d.jpg", OLD),
                ],
            );
            fx.import(&[
                ("a.jpg", serde_json::json!({"Taken": {"value": "2024", "type": "datetime"}})),
                ("b.jpg", serde_json::json!({"Taken": {"value": "2024", "type": "datetime"}})),
                ("c.jpg", serde_json::json!({"Taken": {"value": "2024-03-01T10:00:00", "type": "datetime"}})),
                ("d.jpg", serde_json::json!({"Taken": {"value": "2024-08-01T10:00:00", "type": "datetime"}})),
            ]);
            let run = fx.run(&[
                "exclude",
                "set",
                "--where",
                filter,
                flag,
                &fx.path_str("src"),
            ]);
            let count = match (flag, would, did) {
                ("--dry-run", Some(count), _) | ("--yes", _, Some(count)) => count,
                _ => "No sources to exclude",
            };
            assert_eq!(
                under(&run.stdout, count),
                line(direction),
                "{filter} {flag}: {}",
                run.stdout
            );
        }
    }

    let fx = photos("UTC", &[("a.jpg", OLD), ("b.jpg", OLD)]);
    fx.import(&[
        (
            "a.jpg",
            serde_json::json!({"Taken": {"value": "2024", "type": "datetime"}}),
        ),
        (
            "b.jpg",
            serde_json::json!({"Taken": {"value": "2024", "type": "datetime"}}),
        ),
    ]);
    let set = fx.run(&[
        "exclude",
        "set",
        "--where",
        "Taken < 2024-06",
        "--dry-run",
        &fx.path_str("src"),
    ]);
    assert_eq!(
        under(&set.stdout, "No sources to exclude"),
        line("not selected"),
        "{}",
        set.stdout
    );
    with_archive(&fx);
    let manifest = manifest_with_query(&fx, "Taken < 2024-06");
    let refreshed = fx.run(&["cluster", "refresh", &manifest.to_string_lossy()]);
    assert_eq!(
        under(&refreshed.stdout, "No sources matched the query"),
        line("not selected"),
        "{}",
        refreshed.stdout
    );
    let written = std::fs::read_to_string(&manifest).unwrap();
    assert!(
        written.contains(&format!(
            "\n# 0 sources\n# {}\n# 0 have no archived copy\n",
            line("not selected")
        )),
        "{written}"
    );
}

/// **On `exclude set-object --yes` the suspended share stays under the line it qualifies, and
/// the notes follow it**: the head, then the copies on suspended roots, then the "of which"
/// line in objects.
#[test]
fn set_object_states_the_suspended_share_before_the_notes_under_yes() {
    let fx = photos("UTC", &[("a.jpg", OLD), ("b.jpg", OLD), ("c.jpg", OLD)]);
    fx.import(&[
        ("a.jpg", serde_json::json!({"Make": "Canon"})),
        ("b.jpg", serde_json::json!({"Make": "Nikon"})),
    ]);
    // A copy of `c.jpg`'s content on a root the user then parks.
    std::fs::create_dir_all(fx.path("other")).unwrap();
    std::fs::write(fx.path("other/c.jpg"), "c.jpg").unwrap();
    let other = fx.path_str("other");
    let parked = format!("path:{other}");
    for args in [
        ["scan", "--add", "--role", "source", other.as_str()].as_slice(),
        ["roots", "suspend", parked.as_str()].as_slice(),
    ] {
        let run = fx.run(args);
        assert_eq!(run.code, 0, "{}{}", run.stdout, run.stderr);
    }

    let run = fx.run(&[
        "exclude",
        "set-object",
        "--where",
        "Make != Canon",
        "--yes",
        &fx.path_str("src"),
    ]);
    assert_eq!(run.code, 0, "{}{}", run.stdout, run.stderr);
    let lines: Vec<&str> = run.stdout.lines().collect();
    let head = lines
        .iter()
        .position(|l| l.starts_with("Excluded 2 objects"))
        .unwrap_or_else(|| panic!("{}", run.stdout));
    assert_eq!(
        lines.get(head + 1..head + 3),
        Some(
            &[
                "  of which 1 on suspended roots",
                "  of which 1 object has no Make"
            ][..]
        ),
        "{}",
        run.stdout
    );
}

/// Replace the manifest's `pattern = ` line by `pattern`, as a user editing it does, and return
/// the line written.
fn set_pattern(manifest: &std::path::Path, pattern: &str) -> String {
    let line = format!("pattern = {pattern:?}");
    let config = std::fs::read_to_string(manifest).unwrap();
    let edited: Vec<String> = config
        .lines()
        .map(|l| {
            if l.starts_with("pattern = ") {
                line.clone()
            } else {
                l.to_string()
            }
        })
        .collect();
    std::fs::write(manifest, edited.join("\n") + "\n").unwrap();
    line
}

/// A key of mixed type, `Model`: text on `a.jpg`, a number on `b.jpg`; and `Taken` on `a.jpg`,
/// a time known only to the year.
fn import_a_mixed_model(fx: &Fixture) {
    fx.import(&[
        (
            "a.jpg",
            serde_json::json!({"Model": "EOS", "Taken": {"value": "2024", "type": "datetime"}}),
        ),
        ("b.jpg", serde_json::json!({"Model": "R5"})),
    ]);
    // Import keeps a key to one type, so the second type is written as an older Canon could.
    let db = rusqlite::Connection::open(fx.path("home/canon.db")).unwrap();
    let changed = db
        .execute(
            "UPDATE facts SET value_text = NULL, value_num = 5 \
             WHERE key = 'content.Model' AND value_text = 'R5'",
            [],
        )
        .unwrap();
    assert_eq!(changed, 1);
}

/// The patterns a refresh must not read: one per kind of problem a pattern can have, and one
/// that leaves a source with no place.
const BROKEN_PATTERNS: [&str; 6] = [
    // A reading problem.
    "{source.mtime|yeer}/{filename}",
    // An unknown key.
    "{content.Nothing}/{filename}",
    // A modifier the key's type does not take.
    "{source.ext|year}/{filename}",
    // A key of mixed type.
    "{content.Model}/{filename}",
    // A problem in the literal text.
    "a//{filename}",
    // A time too coarse for its modifier: `a.jpg` gets no place.
    "{content.Taken|month}/{filename}",
];

/// **`cluster refresh` never reads the pattern**: under a valid pattern and under each broken
/// one it exits 0, writes the same lock, writes the pattern's text back byte for byte, and
/// prints the same lines — so no line names the pattern, a count of sources with no place, or
/// a placement.
#[test]
fn refresh_never_reads_the_pattern() {
    let fx = Fixture::new();
    import_a_mixed_model(&fx);
    let manifest = manifest_with_query(&fx, "source.ext = jpg");
    let m = manifest.to_string_lossy().into_owned();
    let lock = manifest.with_extension("lock");

    set_pattern(&manifest, "{filename}");
    let valid = fx.run(&["cluster", "refresh", &m]);
    assert_eq!(valid.code, 0, "{}{}", valid.stdout, valid.stderr);
    let valid_lock = std::fs::read(&lock).unwrap();

    for pattern in BROKEN_PATTERNS {
        let line = set_pattern(&manifest, pattern);
        let run = fx.run(&["cluster", "refresh", &m]);
        assert_eq!(run.code, 0, "{pattern}: {}{}", run.stdout, run.stderr);
        assert_eq!(std::fs::read(&lock).unwrap(), valid_lock, "{pattern}");
        let written = std::fs::read_to_string(&manifest).unwrap();
        assert!(
            written.lines().any(|l| l == line),
            "{pattern}: the pattern line comes back as written:\n{written}"
        );
        assert_eq!(run.stdout, valid.stdout, "{pattern}");
        assert_eq!(run.stderr, valid.stderr, "{pattern}");
    }
}

/// **A broken pattern beside a broken query: the refresh refuses for the query alone** — the
/// same refusal, word for word, it gives under a valid pattern.
#[test]
fn a_refresh_with_a_broken_pattern_and_query_refuses_for_the_query_alone() {
    let fx = Fixture::new();
    let manifest = manifest_with_query(&fx, "source.ext =");
    let m = manifest.to_string_lossy().into_owned();

    set_pattern(&manifest, "{filename}");
    let query_alone = fx.run(&["cluster", "refresh", &m]);
    assert_ne!(query_alone.code, 0);

    set_pattern(&manifest, "{source.mtime|yeer}/{filename}");
    let config = std::fs::read_to_string(&manifest).unwrap();
    let both = fx.run(&["cluster", "refresh", &m]);
    assert_eq!(both.code, query_alone.code);
    assert_eq!(both.stdout, query_alone.stdout);
    assert_eq!(both.stderr, query_alone.stderr);
    assert!(!both.stderr.contains("yeer"), "{}", both.stderr);
    assert_eq!(std::fs::read_to_string(&manifest).unwrap(), config);
}

/// **`cluster generate` writes the default pattern and no placement warning**, even where the
/// manifest is written at the destination itself — the one place a default pattern meets it.
#[test]
fn generate_writes_the_default_and_no_placement_warning() {
    let fx = Fixture::new();
    let dest = format!("{}/out.d", fx.path_str("arc"));
    let run = fx.run(&[
        "cluster",
        "generate",
        &fx.path_str("src"),
        "--dest",
        &dest,
        "--allow",
        "duplicates",
        "--output",
        &dest,
    ]);
    assert_eq!(run.code, 0, "{}{}", run.stdout, run.stderr);
    let written = std::fs::read_to_string(&dest).unwrap();
    assert!(
        written
            .lines()
            .any(|l| l == r#"pattern = "{scope.rel_path}""#),
        "{written}"
    );
    assert!(!run.stderr.contains("Warning"), "{}", run.stderr);
    assert!(!run.stdout.contains("Warning"), "{}", run.stdout);
}

// ---------------------------------------------------------------------------
// Import fits the key's type, on the built binary.
// ---------------------------------------------------------------------------

impl Fixture {
    /// Import one record per named file, each record's facts given as the JSON text a tool
    /// wrote, so a number keeps its spelling; the run is returned, not checked.
    fn import_written(&self, rows: &[(&str, &str)]) -> Run {
        let ids = self.source_ids();
        let input: String = rows
            .iter()
            .map(|(name, facts)| {
                format!(
                    "{{\"source_id\":{},\"basis_rev\":0,\"facts\":{facts}}}\n",
                    ids[*name]
                )
            })
            .collect();
        self.run_with_input(&["import-facts"], Some(&input))
    }

    /// One key's stored rows, each as the source or content it is stored on and its stored
    /// columns, in that order.
    fn stored_rows(&self, key: &str) -> Vec<Vec<Option<String>>> {
        let db = rusqlite::Connection::open(self.path("home/canon.db")).unwrap();
        let mut statement = db
            .prepare(
                "SELECT entity_id, value_text, value_num, value_time, time_nanos, time_form, \
                 time_offset, time_precision FROM facts WHERE key = ?1 ORDER BY entity_id",
            )
            .unwrap();
        statement
            .query_map([key], |row| {
                (0..8)
                    .map(|i| {
                        row.get::<_, rusqlite::types::Value>(i).map(|v| match v {
                            rusqlite::types::Value::Null => None,
                            rusqlite::types::Value::Integer(n) => Some(n.to_string()),
                            rusqlite::types::Value::Real(x) => Some(x.to_string()),
                            rusqlite::types::Value::Text(t) => Some(t),
                            rusqlite::types::Value::Blob(_) => Some("blob".to_string()),
                        })
                    })
                    .collect()
            })
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap()
    }
}

/// **A time handed out by `worklist --emit` comes back the same through a processor**, in
/// `Europe/Amsterdam` and in `America/New_York`: every form (an offset, `Z`, `-00:00`, none)
/// and every precision (year, month, day, minute, second, a fraction) emitted, passed through a
/// processor that hands each line on unchanged, and imported with no hint, is stored with the
/// same form, offset, precision and fraction, and `facts --key` prints the same.
#[test]
fn a_time_handed_out_by_emit_comes_back_the_same_through_a_processor() {
    let written = [
        "2024-06-15T14:30:05+02:00",
        "2024-06-15T12:30:05Z",
        "2024-06-15T12:30:05-00:00",
        "2024-06-15T14:30:05",
        "2024",
        "2024-06",
        "2024-06-15",
        "2024-06-15T14:30",
        "2024-06-15T14:30:05.45+02:00",
    ];
    let names: Vec<String> = (0..written.len()).map(|i| format!("t{i}.jpg")).collect();
    for zone in ["Europe/Amsterdam", "America/New_York"] {
        let files: Vec<(&str, i64)> = names.iter().map(|n| (n.as_str(), OLD)).collect();
        let fx = photos(zone, &files);
        let rows: Vec<(&str, serde_json::Value)> = names
            .iter()
            .zip(written)
            .map(|(name, w)| {
                (
                    name.as_str(),
                    serde_json::json!({"taken": {"value": w, "type": "datetime"}}),
                )
            })
            .collect();
        fx.import(&rows);
        let stored = fx.stored_rows("content.taken");
        assert_eq!(stored.len(), written.len(), "{zone}");
        let report = |fx: &Fixture| {
            let run = fx.run(&["facts", "--global", "--key", "content.taken"]);
            assert_eq!(run.code, 0, "{}", run.stderr);
            // Rows with equal counts are listed in no fixed order.
            run.stdout
                .lines()
                .map(str::to_string)
                .collect::<std::collections::BTreeSet<_>>()
        };
        let reported = report(&fx);

        let emitted = fx.run(&["worklist", "--global", "--emit", "content.taken"]);
        assert_eq!(emitted.code, 0, "{}", emitted.stderr);
        let back = fx.run_with_input(&["import-facts"], Some(&emitted.stdout));
        assert_eq!(back.code, 0, "{zone}: {}{}", back.stdout, back.stderr);
        assert_eq!(
            back.stdout, "Read 9 records: imported 9 facts.\n",
            "{zone}: {}",
            emitted.stdout
        );

        assert_eq!(fx.stored_rows("content.taken"), stored, "{zone}");
        assert_eq!(report(&fx), reported, "{zone}");
    }
}

/// **Each skipped value is counted under its cause, and each key and cause gets one line with
/// its way out**, on the built binary: the summary names every cause's count, and stderr holds
/// one line per key and cause in key order, each as the way-out table words it — and nothing
/// for a null value or a source fact passed back, which the summary counts.
#[test]
fn import_states_each_cause_with_its_count_and_way_out() {
    let names = ["a.jpg", "b.jpg", "c.jpg", "d.jpg", "e.jpg"];
    let files: Vec<(&str, i64)> = names.iter().map(|n| (*n, OLD)).collect();
    let fx = photos("UTC", &files);
    let setup = fx.import_written(&[
        (
            "a.jpg",
            r#"{"Width":1920,"Count":5,"Taken":{"value":"2024","type":"datetime"},"When":{"value":"2024","type":"datetime"},"Label":"x","Model":"EOS"}"#,
        ),
        ("b.jpg", r#"{"Model":"R5"}"#),
    ]);
    assert_eq!(setup.code, 0, "{}{}", setup.stdout, setup.stderr);
    // Import keeps a key to one type, so the second type is written as an older Canon could.
    let db = rusqlite::Connection::open(fx.path("home/canon.db")).unwrap();
    db.execute(
        "UPDATE facts SET value_text = NULL, value_num = 5 \
         WHERE key = 'content.Model' AND value_text = 'R5'",
        [],
    )
    .unwrap();
    drop(db);

    let run = fx.import_written(&[
        (
            "b.jpg",
            r#"{"Width":"wide","Count":9007199254740993,"Taken":"soon","When":true,"Label":{"value":"2024","type":"datetime"},"Rating":{"value":"twelve","type":"number"},"Model":"Z","source.foo":1,"source.size":5,"Nothing":null,"Note":"ok"}"#,
        ),
        (
            "c.jpg",
            r#"{"Width":"wide","Count":9007199254740993,"Taken":"soon","When":true,"Label":{"value":"2024","type":"datetime"},"Rating":{"value":"twelve","type":"number"},"Model":"Z","source.foo":1}"#,
        ),
        (
            "d.jpg",
            r#"{"Width":"01234","Count":[1,2],"Taken":2019,"When":{"value":"5","type":"duration"},"Label":{"value":"x","type":"bogus"},"Lens":"a","content.Lens":"b"}"#,
        ),
        (
            "e.jpg",
            r#"{"Width":"01234","Count":[1,2],"Taken":2019,"When":{"value":"5","type":"duration"},"Label":{"value":"x","type":"bogus"},"Lens":"a","content.Lens":"b"}"#,
        ),
    ]);
    assert_eq!(run.code, 0, "{}{}", run.stdout, run.stderr);
    assert_eq!(
        run.stdout,
        "Read 4 records: imported 1 fact; skipped 30 values: 4 that do not read as their key's \
         type, 2 that would lose what the text carried, 2 that are numbers for a time key, \
         2 that are numbers Canon cannot hold exactly, 2 that are true or false for a time key, \
         2 that are lists or objects for a number or time key, 4 with a hint their key does not \
         take, 2 that do not read as their hint, 2 with a hint Canon does not read, 2 for keys \
         of mixed type, 4 for keys named twice in one record, 2 under names reserved for \
         source facts; 1 null value (nothing stored); 1 value for a source fact, which Canon \
         records itself.\n"
    );
    let lines = [
        "content.Count: 2 values, such as 9007199254740993, are numbers Canon cannot hold \
         exactly (--verbose lists each); `canon facts convert content.Count --to text` to keep \
         values like these as written",
        "content.Count: 2 values, such as [1,2], are lists or objects, and the key holds \
         numbers (--verbose lists each); a number key does not take lists or objects: import \
         them under a new key, or have the tool write numbers",
        "content.Label: 2 values, such as \"2024\", are hinted datetime, and the key holds text \
         (--verbose lists each); `canon facts convert content.Label --to time` if this key \
         should hold times; otherwise have the tool write text",
        "content.Label: 2 values, such as \"x\", carry the hint `bogus`, which Canon does not \
         read (--verbose lists each); Canon reads three type hints, datetime, duration and \
         number; have the tool drop this one, and the key decides",
        "content.Lens: 4 values, such as \"a\", are for a key named twice in one record \
         (--verbose lists each); have the tool write each key under one name (a key passed \
         back by `worklist --emit` under the name it was emitted as)",
        "content.Model: 2 values, such as \"Z\", are for a key of mixed type (--verbose lists \
         each); these values arrive when the pipeline runs again after the key is converted",
        "To see its values and convert it to one type:",
        "  `canon facts convert content.Model`",
        "content.Rating: 2 values, such as \"twelve\", do not read as their number hint \
         (--verbose lists each); have the tool correct these values, or omit them",
        "content.Taken: 2 values, such as \"soon\", do not read as a time (--verbose lists \
         each); have the tool write a time Canon reads (see \"How Canon interprets a time\" in \
         the facts documentation) or omit values that are no time (an empty EXIF date such as \
         0000:00:00 00:00:00 is best omitted), or `canon facts convert content.Taken` to see \
         the other ways",
        "content.Taken: 2 values, such as 2019, are numbers, and the key holds times \
         (--verbose lists each); have the tool write the time as text (a year as \"2019\"), or, \
         for a year, write the number with the datetime hint",
        "content.When: 2 values, such as true, are true or false, and the key holds times \
         (--verbose lists each); a time key does not take true or false: import them under a \
         new key, or have the tool write times",
        "content.When: 2 values, such as \"5\", are hinted duration, and the key holds times \
         (--verbose lists each); a duration is not a time: import durations under a new key",
        "content.Width: 2 values, such as \"wide\", do not read as a number (--verbose lists \
         each); have the tool write numbers or omit values that are no number, or `canon facts \
         convert content.Width` to see the other ways",
        "content.Width: 2 values, such as \"01234\", would lose what the text carried \
         (--verbose lists each); `canon facts convert content.Width --to text` to keep values \
         like these as written, or have the tool write them as JSON numbers",
        "source.foo: 2 values, such as 1, are under a name starting with `source.`, which only \
         Canon's own source facts have (--verbose lists each); have the tool name them another \
         way (the key printed is the name as the tool wrote it)",
        "Updating query statistics...",
    ];
    assert_eq!(run.stderr, lines.join("\n") + "\n");
    assert_eq!(fx.stored_rows("content.Note").len(), 1);
    assert_eq!(fx.stored_rows("content.Width").len(), 1, "only the setup's");
}

/// **A JSON number for a text key lands as text, in the spelling the tool wrote**: `1.10`
/// stays `1.10`, never `1.1`.
#[test]
fn a_number_written_1_10_stays_1_10_in_a_text_key() {
    let fx = photos("UTC", &[("a.jpg", OLD), ("b.jpg", OLD)]);
    let run = fx.import_written(&[("a.jpg", r#"{"Version":"v1"}"#)]);
    assert_eq!(run.code, 0, "{}{}", run.stdout, run.stderr);
    let run = fx.import_written(&[("b.jpg", r#"{"Version":1.10}"#)]);
    assert_eq!(run.stdout, "Read 1 record: imported 1 fact.\n");
    let stored: std::collections::BTreeSet<Option<String>> = fx
        .stored_rows("content.Version")
        .into_iter()
        .map(|row| row[1].clone())
        .collect();
    assert_eq!(
        stored,
        [Some("1.10".to_string()), Some("v1".to_string())].into()
    );
    assert_eq!(fx.listed(r#"Version="1.10""#), names(&["b.jpg"]));
}

/// **A key of mixed type takes nothing**, of any type, until it is converted: each value is
/// counted, the line names the conversion, and the key's rows are as they were.
#[test]
fn an_import_into_a_mixed_key_takes_nothing() {
    // Files of their own content, so each fact is stored on its own content.
    let fx = photos("UTC", &[("a.jpg", OLD), ("b.jpg", OLD)]);
    import_a_mixed_model(&fx);
    let before = fx.stored_rows("content.Model");
    assert_eq!(before.len(), 2);
    let run = fx.import_written(&[
        ("a.jpg", r#"{"Model":"EOS2"}"#),
        ("b.jpg", r#"{"Model":7}"#),
    ]);
    assert_eq!(run.code, 0, "{}{}", run.stdout, run.stderr);
    assert_eq!(
        run.stdout,
        "Read 2 records: imported 0 facts; skipped 2 values: 2 for keys of mixed type.\n"
    );
    assert!(
        run.stderr.starts_with(
            "content.Model: 2 values, such as \"EOS2\", are for a key of mixed type (--verbose \
             lists each); these values arrive when the pipeline runs again after the key is \
             converted\nTo see its values and convert it to one type:\n  `canon facts convert \
             content.Model`\n"
        ),
        "{}",
        run.stderr
    );
    assert_eq!(fx.stored_rows("content.Model"), before);
}

/// **A large integer through `canonargs --type number` is skipped, never changed**: the
/// records are the JSON `canonargs` writes for `9007199254740993` and `1920` (a number passed
/// on as written, pinned in `canonargs` itself), imported into a number key. The first is
/// skipped as a number Canon cannot hold exactly, and counted; the second is stored.
#[test]
fn a_large_integer_through_canonargs_is_skipped_never_changed() {
    let fx = photos("UTC", &[("a.jpg", OLD), ("b.jpg", OLD), ("c.jpg", OLD)]);
    let run = fx.import_written(&[("a.jpg", r#"{"Serial":5}"#)]);
    assert_eq!(run.code, 0, "{}{}", run.stdout, run.stderr);
    let run = fx.import_written(&[
        ("b.jpg", r#"{"Serial":9007199254740993}"#),
        ("c.jpg", r#"{"Serial":1920}"#),
    ]);
    assert_eq!(
        run.stdout,
        "Read 2 records: imported 1 fact; skipped 1 value: 1 that is a number Canon cannot \
         hold exactly.\n"
    );
    assert!(
        run.stderr.starts_with(
            "content.Serial: 1 value, 9007199254740993, is a number Canon cannot hold exactly; \
             `canon facts convert content.Serial --to text` to keep values like these as \
             written\n"
        ),
        "{}",
        run.stderr
    );
    let numbers: std::collections::BTreeSet<Option<String>> = fx
        .stored_rows("content.Serial")
        .into_iter()
        .map(|row| row[2].clone())
        .collect();
    assert_eq!(
        numbers,
        [Some("1920".to_string()), Some("5".to_string())].into()
    );
}
