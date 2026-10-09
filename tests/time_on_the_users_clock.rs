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

/// The outcome of a run that may refuse.
struct Outcome {
    success: bool,
    stdout: String,
    stderr: String,
}

impl Home {
    /// [`Home::run`], for a run that may refuse: its exit and both streams, asserted on by
    /// the caller.
    fn attempt(&self, tz: &str, args: &[&str]) -> Outcome {
        let out = Command::new(env!("CARGO_BIN_EXE_canon"))
            .args(args)
            .current_dir(self.dir.path())
            .env("CANON_HOME", self.path("home"))
            .env("TZ", tz)
            .stdin(std::process::Stdio::null())
            .output()
            .expect("canon must run");
        Outcome {
            success: out.status.success(),
            stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
        }
    }

    /// An archive root beside the photos, and a manifest made under `tz` over the save-time
    /// file alone, placing by `{source.mtime|year}`.
    fn manifest_made_in(&self, tz: &str) -> String {
        let manifest = self.manifest_over(tz, &["photos/saved.jpg"]);
        self.edit_manifest(
            &manifest,
            "pattern = ",
            "pattern = \"{source.mtime|year}/{filename}\"",
        );
        manifest
    }

    /// An archive root beside the photos, and a manifest made under `tz` over `files`.
    fn manifest_over(&self, tz: &str, files: &[&str]) -> String {
        std::fs::create_dir_all(self.path("archive")).unwrap();
        let archive = self.path_str("archive");
        self.run(tz, &["scan", "--add", "--role", "archive", &archive], None);
        let manifest = format!("{}/cluster.toml", self.path_str(""));
        let scope: Vec<String> = files.iter().map(|f| self.path_str(f)).collect();
        let mut args = vec!["cluster", "generate"];
        args.extend(scope.iter().map(String::as_str));
        args.extend([
            "--dest",
            archive.as_str(),
            "-o",
            manifest.as_str(),
            "--no-edit",
        ]);
        let made = self.run(tz, &args, None);
        assert!(
            made.stdout.contains(&format!("Times are read on {tz}")),
            "{}{}",
            made.stdout,
            made.stderr
        );
        manifest
    }

    /// Replace the manifest line starting with `starts` by `line`, as a user editing it does.
    fn edit_manifest(&self, manifest: &str, starts: &str, line: &str) {
        let text = std::fs::read_to_string(manifest).unwrap();
        let edited: String = text
            .lines()
            .map(|l| {
                if l.starts_with(starts) {
                    format!("{line}\n")
                } else {
                    format!("{l}\n")
                }
            })
            .collect();
        std::fs::write(manifest, edited).unwrap();
    }
}

/// **The New Year places on the manifest's zone on any machine.** A manifest recorded in
/// Amsterdam, refreshed and applied in New York, places the file saved at 23:30 UTC on New
/// Year's Eve 2023 under 2024 — Amsterdam's year — and says which clock it read. With the
/// zone line edited to New York and refreshed, the same file places under 2023.
#[test]
fn the_new_year_places_on_the_manifests_zone_on_any_machine() {
    for (zone_line, year) in [(None, "2024"), (Some("America/New_York"), "2023")] {
        let home = Home::new();
        let manifest = home.manifest_made_in("Europe/Amsterdam");
        if let Some(zone) = zone_line {
            home.edit_manifest(&manifest, "zone = ", &format!("zone = \"{zone}\""));
        }

        let refreshed = home.run("America/New_York", &["cluster", "refresh", &manifest], None);
        match zone_line {
            None => assert!(
                refreshed.stdout.contains(
                    "times read on Europe/Amsterdam, as the manifest records (this machine: America/New_York)"
                ),
                "{}",
                refreshed.stdout
            ),
            Some(_) => assert!(
                refreshed
                    .stdout
                    .contains("Time zone changed from Europe/Amsterdam to America/New_York"),
                "{}",
                refreshed.stdout
            ),
        }

        let dry = home.run("America/New_York", &["apply", &manifest, "--dry-run"], None);
        let lock_zone = zone_line.unwrap_or("Europe/Amsterdam");
        assert!(
            dry.stderr
                .contains(&format!("Time zone: {lock_zone} (from the lock)")),
            "{}",
            dry.stderr
        );

        home.run("America/New_York", &["apply", &manifest, "--yes"], None);
        assert!(
            home.path(&format!("archive/{year}/saved.jpg")).exists(),
            "placed under {year}"
        );
    }
}

/// **An old lock is refused in friendly words**: a lock written before lock files recorded
/// the zone is refused by `apply` naming the rebuild, never as a parse error — and a
/// manifest command prints no unknown-zone notice, since it reads the recorded zone.
#[test]
fn an_old_lock_is_refused_in_friendly_words() {
    use sha2::{Digest, Sha256};
    let home = Home::new();
    let manifest = home.manifest_made_in("Europe/Amsterdam");

    // Take the zone out of the lock's header and re-point the manifest at the new bytes, so
    // the refusal is the zone's and not the hash's.
    let lock = manifest.replace(".toml", ".lock");
    let text = std::fs::read_to_string(&lock).unwrap();
    let mut lines: Vec<String> = text.lines().map(str::to_string).collect();
    let mut header: serde_json::Value = serde_json::from_str(&lines[0]).unwrap();
    header.as_object_mut().unwrap().remove("zone");
    lines[0] = header.to_string();
    let old = lines.join("\n") + "\n";
    std::fs::write(&lock, &old).unwrap();
    let hash = format!("{:x}", Sha256::digest(old.as_bytes()));
    home.edit_manifest(
        &manifest,
        "lock_hash = ",
        &format!("lock_hash = \"{hash}\""),
    );

    let refused = home.attempt("America/New_York", &["apply", &manifest, "--yes"]);
    assert!(!refused.success);
    let said = format!("{}{}", refused.stdout, refused.stderr);
    assert!(
        said.contains("This lock file was built before lock files recorded the time zone."),
        "{said}"
    );
    assert!(said.contains("canon cluster refresh"), "{said}");
    assert!(!said.contains("Failed to parse"), "{said}");
}

/// **A manifest command prints no unknown-zone notice**: it reads the manifest's recorded
/// zone, so "times are read in UTC" would be false there — on a machine whose `TZ` Canon
/// cannot read, `cluster refresh` and `cluster status` say nothing of it.
#[test]
fn manifest_commands_print_no_unknown_zone_notice() {
    let home = Home::new();
    let manifest = home.manifest_made_in("Europe/Amsterdam");
    for args in [
        ["cluster", "refresh", manifest.as_str()],
        ["cluster", "status", manifest.as_str()],
    ] {
        let run = home.run("Europe/Amsterdm", &args, None);
        assert!(
            !run.stderr.contains("times are read in UTC"),
            "{args:?}: {}",
            run.stderr
        );
    }
}

/// **Camera dates place on the manifest's zone too**, the other half of the New Year check: an
/// instant written with `-05:00` and one written in UTC are 1 January 2024 in Amsterdam, and a
/// clock reading with no zone places as written — on a New York machine, from a manifest
/// recording Amsterdam.
#[test]
fn a_camera_date_places_on_the_manifests_zone_and_a_clock_reading_as_written() {
    let home = Home::new();
    let manifest = home.manifest_over(
        "Europe/Amsterdam",
        &["photos/f0.jpg", "photos/f1.jpg", "photos/f4.jpg"],
    );
    home.edit_manifest(
        &manifest,
        "pattern = ",
        "pattern = \"{content.Taken|date}/{filename}\"",
    );
    home.run("America/New_York", &["cluster", "refresh", &manifest], None);
    home.run("America/New_York", &["apply", &manifest, "--yes"], None);
    for placed in [
        "2024-01-01/f0.jpg",
        "2024-01-01/f1.jpg",
        "2023-12-31/f4.jpg",
    ] {
        assert!(home.path(&format!("archive/{placed}")).exists(), "{placed}");
    }
}

/// **A refresh states the scope's refusal first**: a scope line on a suspended root, beside a
/// zone line Canon cannot read, prints the door, then the zone's cause, then the abort once —
/// exit 1 with no `Error:`, nothing written.
#[test]
fn a_refresh_states_the_door_before_the_zone_cause() {
    let home = Home::new();
    let manifest = home.manifest_made_in("Europe/Amsterdam");
    home.edit_manifest(&manifest, "zone = ", "zone = \"Europe/Amsterdm\"");
    let before = std::fs::read_to_string(&manifest).unwrap();
    let photos = home.path_str("photos");
    home.run(
        "UTC",
        &["roots", "suspend", &format!("path:{photos}")],
        None,
    );

    let refused = home.attempt("UTC", &["cluster", "refresh", &manifest]);
    assert!(!refused.success);
    let out = refused.stdout;
    let door = out.find("suspended").unwrap_or_else(|| panic!("{out}"));
    let zone = out
        .find("Europe/Amsterdm")
        .unwrap_or_else(|| panic!("{out}"));
    let aborted = out
        .find("Refresh aborted")
        .unwrap_or_else(|| panic!("{out}"));
    assert!(door < zone && zone < aborted, "{out}");
    assert_eq!(out.matches("Refresh aborted").count(), 1, "{out}");
    assert!(!refused.stderr.contains("Error:"), "{}", refused.stderr);
    assert_eq!(std::fs::read_to_string(&manifest).unwrap(), before);
}

impl Home {
    /// The ids of the decisions recorded under `command`, oldest first, read from the trail.
    fn decision_ids(&self, command: &str) -> Vec<i64> {
        let run = self.run("UTC", &["trail", "--global", "--jsonl"], None);
        let mut ids: Vec<i64> = run
            .stdout
            .lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .filter(|v| v["type"] == "decision" && v["command"] == command)
            .filter_map(|v| v["id"].as_i64())
            .collect();
        ids.sort();
        ids
    }

    /// `trail show <id>`'s `read times on:` value, checked to sit on the line after `command:`.
    fn shown_zone(&self, id: i64) -> String {
        let run = self.run("UTC", &["trail", "show", &id.to_string()], None);
        let lines: Vec<&str> = run.stdout.lines().collect();
        let at = lines
            .iter()
            .position(|l| l.starts_with("  read times on: "))
            .unwrap_or_else(|| panic!("no read times on line:\n{}", run.stdout));
        assert!(
            at > 0 && lines[at - 1].starts_with("  command:"),
            "read times on: must follow command:\n{}",
            run.stdout
        );
        lines[at]
            .strip_prefix("  read times on: ")
            .expect("found by its prefix")
            .to_string()
    }

    /// The `[meta].zone` of decision `id`'s receipt, found by its file name anywhere under
    /// the archive root.
    fn receipt_zone(&self, id: i64, command: &str) -> String {
        let name = format!("{id:06}-{command}.toml");
        let mut stack = vec![self.path("archive")];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    stack.push(path);
                } else if path.file_name().is_some_and(|n| n == name.as_str()) {
                    let receipt: toml::Value =
                        std::fs::read_to_string(&path).unwrap().parse().unwrap();
                    return receipt["meta"]["zone"]
                        .as_str()
                        .expect("[meta].zone")
                        .to_string();
                }
            }
        }
        panic!("no receipt {name} under the archive");
    }
}

/// **A manifest's run records the manifest's zone**: a manifest made in Amsterdam, refreshed
/// and applied on a New York machine, records `Europe/Amsterdam` on the generate, refresh and
/// apply rows and in apply's receipt — the clock their times were read on, never the machine's.
#[test]
fn a_manifest_run_records_the_manifests_zone() {
    let home = Home::new();
    let manifest = home.manifest_made_in("Europe/Amsterdam");
    home.run("America/New_York", &["cluster", "refresh", &manifest], None);
    home.run("America/New_York", &["apply", &manifest, "--yes"], None);

    for command in ["cluster_generate", "cluster_refresh", "apply"] {
        let ids = home.decision_ids(command);
        assert_eq!(ids.len(), 1, "{command}: {ids:?}");
        assert_eq!(home.shown_zone(ids[0]), "Europe/Amsterdam", "{command}");
    }
    let apply = home.decision_ids("apply")[0];
    assert_eq!(home.receipt_zone(apply, "apply"), "Europe/Amsterdam");
}

/// **A record states the clock its command read**: `exclude set --where` on a New York
/// machine records `America/New_York` on its row and in its receipt; on a machine whose `TZ`
/// Canon cannot read, both say UTC and that the zone is unknown.
#[test]
fn an_exclusion_records_the_users_zone() {
    for (tz, said) in [
        ("America/New_York", "America/New_York"),
        (
            "Europe/Amsterdm",
            "UTC (this machine's time zone is unknown)",
        ),
    ] {
        let home = Home::new();
        std::fs::create_dir_all(home.path("archive")).unwrap();
        let archive = home.path_str("archive");
        home.run(
            "UTC",
            &["scan", "--add", "--role", "archive", &archive],
            None,
        );
        let photos = home.path_str("photos");
        home.run(
            tz,
            &[
                "exclude",
                "set",
                &photos,
                "--where",
                "filename = 'f0.jpg'",
                "--yes",
            ],
            None,
        );

        let ids = home.decision_ids("exclude_set");
        assert_eq!(ids.len(), 1, "{tz}: {ids:?}");
        assert_eq!(home.shown_zone(ids[0]), said, "{tz}");
        assert_eq!(home.receipt_zone(ids[0], "exclude_set"), said, "{tz}");
    }
}

/// **The trail refuses a date it cannot read with its own refusal**: a one-digit month is not
/// in the one list, so `trail --on 2024-1-5` exits non-zero naming the flag and the forms.
#[test]
fn the_trail_refuses_a_date_outside_the_one_list() {
    let home = Home::new();
    let refused = home.attempt(
        "Europe/Amsterdam",
        &["trail", "--global", "--on", "2024-1-5"],
    );
    assert!(!refused.success);
    assert!(
        refused
            .stderr
            .contains("cannot read the date for --on\n  \"2024-1-5\" is not in the right format."),
        "{}",
        refused.stderr
    );
    assert!(
        refused.stderr.contains("2024, 2024-06, 2024-06-15"),
        "{}",
        refused.stderr
    );
}
