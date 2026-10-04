//! Times read on the user's clock, through the built binary.
//!
//! One temp canon home: a file whose save time is 23:30 UTC on New Year's Eve 2023, and
//! seven files each carrying one written form of a time under the key `Taken`. The
//! reports are run under two zones, and every expected value is read off the display
//! table below — the instant on the user's clock, the clock reading as written, a value
//! never padded to a grain nobody wrote — never captured from a run.

use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, SystemTime};

/// 2023-12-31T23:30:00Z.
const NEW_YEARS_EVE: u64 = 1704065400;

/// The written forms, each with what it prints, its `|year` and its `|date` in Amsterdam
/// and in New York. `None` is no value: a time known only to the year has no date.
struct Row {
    written: &'static str,
    amsterdam: (&'static str, &'static str, Option<&'static str>),
    new_york: (&'static str, &'static str, Option<&'static str>),
}

const ROWS: [Row; 7] = [
    Row {
        written: "2023-12-31T21:00:00-05:00",
        amsterdam: ("2024-01-01 03:00:00", "2024", Some("2024-01-01")),
        new_york: ("2023-12-31 21:00:00", "2023", Some("2023-12-31")),
    },
    Row {
        written: "2023-12-31T23:30:00Z",
        amsterdam: ("2024-01-01 00:30:00", "2024", Some("2024-01-01")),
        new_york: ("2023-12-31 18:30:00", "2023", Some("2023-12-31")),
    },
    Row {
        written: "2023-12-31T23:30:00-00:00",
        amsterdam: ("2024-01-01 00:30:00", "2024", Some("2024-01-01")),
        new_york: ("2023-12-31 18:30:00", "2023", Some("2023-12-31")),
    },
    Row {
        written: "2023-07-14T21:00:00+02:00",
        amsterdam: ("2023-07-14 21:00:00", "2023", Some("2023-07-14")),
        new_york: ("2023-07-14 15:00:00", "2023", Some("2023-07-14")),
    },
    Row {
        written: "2023:12:31 21:00:00",
        amsterdam: ("2023-12-31 21:00:00", "2023", Some("2023-12-31")),
        new_york: ("2023-12-31 21:00:00", "2023", Some("2023-12-31")),
    },
    Row {
        written: "2024",
        amsterdam: ("2024", "2024", None),
        new_york: ("2024", "2024", None),
    },
    Row {
        written: "2024-06-15T14:30:05.45+02:00",
        amsterdam: ("2024-06-15 14:30:05", "2024", Some("2024-06-15")),
        new_york: ("2024-06-15 08:30:05", "2024", Some("2024-06-15")),
    },
];

struct Home {
    dir: tempfile::TempDir,
}

struct Run {
    stdout: String,
    stderr: String,
}

impl Home {
    fn new() -> Self {
        let home = Home {
            dir: tempfile::tempdir().unwrap(),
        };
        std::fs::create_dir_all(home.path("home")).unwrap();
        std::fs::create_dir_all(home.path("photos")).unwrap();
        for i in 0..ROWS.len() {
            std::fs::write(home.path(&format!("photos/f{i}.jpg")), format!("file {i}")).unwrap();
        }
        let saved = home.path("photos/saved.jpg");
        std::fs::write(&saved, "saved").unwrap();
        std::fs::File::options()
            .write(true)
            .open(&saved)
            .unwrap()
            .set_modified(SystemTime::UNIX_EPOCH + Duration::from_secs(NEW_YEARS_EVE))
            .unwrap();

        let photos = home.path_str("photos");
        home.run("UTC", &["scan", "--add", "--role", "source", &photos], None);

        let mut input = String::new();
        for (i, row) in ROWS.iter().enumerate() {
            let id = home.source_id(&format!("photos/f{i}.jpg"));
            let record = serde_json::json!({
                "source_id": id,
                "basis_rev": 0,
                "facts": {"Taken": {"value": row.written, "type": "datetime"}},
            });
            input.push_str(&format!("{record}\n"));
        }
        let run = home.run("UTC", &["import-facts"], Some(&input));
        assert!(
            run.stdout.contains("7 facts imported"),
            "{}{}",
            run.stdout,
            run.stderr
        );
        home
    }

    fn path(&self, rel: &str) -> PathBuf {
        self.dir.path().join(rel)
    }

    /// The canonical form, which is what Canon stores and prints: on macOS the temp
    /// dir is reached through a symlink.
    fn path_str(&self, rel: &str) -> String {
        let p = self.path(rel);
        std::fs::canonicalize(&p)
            .unwrap_or(p)
            .to_string_lossy()
            .into_owned()
    }

    fn run(&self, tz: &str, args: &[&str], stdin: Option<&str>) -> Run {
        use std::io::Write as _;
        let mut child = Command::new(env!("CARGO_BIN_EXE_canon"))
            .args(args)
            .current_dir(self.dir.path())
            .env("CANON_HOME", self.path("home"))
            .env("TZ", tz)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("canon must run");
        child
            .stdin
            .take()
            .unwrap()
            .write_all(stdin.unwrap_or("").as_bytes())
            .unwrap();
        let out = child.wait_with_output().expect("canon must finish");
        let run = Run {
            stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
        };
        assert!(
            out.status.success(),
            "canon {args:?} failed: {}{}",
            run.stdout,
            run.stderr
        );
        run
    }

    fn source_id(&self, rel: &str) -> i64 {
        let run = self.run("UTC", &["worklist", "--global"], None);
        let needle = self.path_str(rel);
        for line in run.stdout.lines() {
            let v: serde_json::Value = serde_json::from_str(line).expect("jsonl");
            if v["path"].as_str() == Some(needle.as_str()) {
                return v["source_id"].as_i64().expect("source_id");
            }
        }
        panic!("no source at {needle} in the worklist");
    }

    /// `facts --key <key>`'s rows as (value, count), sorted.
    fn distribution(&self, tz: &str, key: &str) -> Vec<(String, usize)> {
        let run = self.run(tz, &["facts", "--global", "--key", key], None);
        let rows = run
            .stdout
            .lines()
            .skip_while(|line| !line.starts_with('─'))
            .skip(1);
        let mut parsed: Vec<(String, usize)> = rows
            .filter_map(|line| {
                let words: Vec<&str> = line.split_whitespace().collect();
                if words.len() < 3 {
                    return None;
                }
                let count = words[words.len() - 2].parse().ok()?;
                Some((words[..words.len() - 2].join(" "), count))
            })
            .collect();
        parsed.sort();
        parsed
    }
}

/// The rows a report should print, counted from the table: one per written form, and one
/// more under `(no value)` for the save-time file, which carries no `Taken`.
fn expected(values: impl Iterator<Item = Option<String>>) -> Vec<(String, usize)> {
    let mut counts = std::collections::BTreeMap::new();
    for value in values {
        *counts
            .entry(value.unwrap_or_else(|| "(no value)".to_string()))
            .or_insert(0) += 1;
    }
    // The save-time file carries no `Taken`.
    *counts.entry("(no value)".to_string()).or_insert(0) += 1;
    let mut rows: Vec<(String, usize)> = counts.into_iter().collect();
    rows.sort();
    rows
}

#[test]
fn every_written_form_reads_on_the_users_clock() {
    let home = Home::new();

    for (tz, pick) in [
        (
            "Europe/Amsterdam",
            (|row: &Row| row.amsterdam) as fn(&Row) -> (&str, &str, Option<&str>),
        ),
        ("America/New_York", |row: &Row| row.new_york),
    ] {
        let printed = expected(ROWS.iter().map(|r| Some(pick(r).0.to_string())));
        assert_eq!(home.distribution(tz, "Taken"), printed, "facts in {tz}");

        let year = expected(ROWS.iter().map(|r| Some(pick(r).1.to_string())));
        assert_eq!(home.distribution(tz, "Taken|year"), year, "|year in {tz}");

        let date = expected(ROWS.iter().map(|r| pick(r).2.map(str::to_string)));
        assert_eq!(home.distribution(tz, "Taken|date"), date, "|date in {tz}");
    }
}

#[test]
fn the_save_time_reads_on_the_users_clock() {
    let home = Home::new();
    let saved = home.path_str("photos/saved.jpg");

    for (tz, year, column) in [
        ("Europe/Amsterdam", "2024", "Jan  1 2024"),
        ("America/New_York", "2023", "Dec 31 2023"),
    ] {
        let years = home.distribution(tz, "source.mtime|year");
        assert!(years.contains(&(year.to_string(), 1)), "{tz}: {years:?}");

        let ls = home.run(tz, &["ls", "--global", "-l"], None);
        let line = ls
            .stdout
            .lines()
            .find(|line| line.ends_with(&saved))
            .unwrap_or_else(|| panic!("{}", ls.stdout));
        assert!(line.contains(column), "{tz}: {line}");

        let selected = home.run(
            tz,
            &[
                "ls",
                "--global",
                "--where",
                &format!("source.mtime|year = {year}"),
            ],
            None,
        );
        assert!(
            selected.stdout.contains(&saved),
            "{tz}: {}",
            selected.stdout
        );
        assert!(
            !selected.stderr.contains("time zone"),
            "no notice for a zone Canon reads: {}",
            selected.stderr
        );
    }
}

/// A `TZ` Canon cannot read is named once on stderr, and times are read in UTC.
#[test]
fn a_zone_canon_cannot_read_is_named_and_utc_is_read() {
    let home = Home::new();
    let run = home.run(
        "Europe/Amsterdm",
        &["facts", "--global", "--key", "source.mtime|year"],
        None,
    );
    assert_eq!(
        run.stderr,
        "TZ=Europe/Amsterdm is not a time zone Canon can read; times are read in UTC\n"
    );
    assert!(
        run.stdout.lines().any(|line| line.starts_with("2023 ")),
        "{}",
        run.stdout
    );
}
