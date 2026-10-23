//! A manifest's pattern, run against the built binary: what `apply` refuses and what
//! `cluster status` states, observed on a scratch library rather than read from the code.
//!
//! **Why the binary.** A destination is the one decision a user cannot undo after a move, and
//! whether a refusal moved nothing, wrote nothing and recorded nothing is a property of the
//! process and the disk, which only a process run can show.

use std::path::PathBuf;
use std::process::Command;

/// A source root holding `README` at its top and three files under `sub`, an archive root, and
/// a canon home, under a temp dir that cleans itself up.
struct Library {
    dir: tempfile::TempDir,
}

struct Run {
    stdout: String,
    stderr: String,
    code: i32,
}

impl Run {
    fn all(&self) -> String {
        format!("{}{}", self.stdout, self.stderr)
    }
}

impl Library {
    fn new() -> Self {
        let lib = Library {
            dir: tempfile::tempdir().unwrap(),
        };
        for dir in ["home", "photos/sub", "archive"] {
            std::fs::create_dir_all(lib.path(dir)).unwrap();
        }
        for (file, body) in [
            ("photos/README", "a"),
            ("photos/sub/x.jpg", "b"),
            ("photos/sub/y.jpg", "c"),
            ("photos/sub/photo..jpg", "d"),
        ] {
            std::fs::write(lib.path(file), body).unwrap();
        }
        lib.ok(&["scan", "--add", "--role", "source", &lib.path_str("photos")]);
        lib.ok(&[
            "scan",
            "--add",
            "--role",
            "archive",
            &lib.path_str("archive"),
        ]);
        lib
    }

    fn path(&self, rel: &str) -> PathBuf {
        self.dir.path().join(rel)
    }

    /// The canonical form, which is what Canon stores and prints: on macOS the temp dir is
    /// reached through a symlink.
    fn path_str(&self, rel: &str) -> String {
        let p = self.path(rel);
        std::fs::canonicalize(&p)
            .unwrap_or(p)
            .to_string_lossy()
            .into_owned()
    }

    fn run(&self, args: &[&str], stdin: &str) -> Run {
        use std::io::Write as _;
        let mut child = Command::new(env!("CARGO_BIN_EXE_canon"))
            .args(args)
            .current_dir(self.dir.path())
            .env("CANON_HOME", self.path("home"))
            .env("TZ", "UTC")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("canon must run");
        child
            .stdin
            .take()
            .unwrap()
            .write_all(stdin.as_bytes())
            .unwrap();
        let out = child.wait_with_output().expect("canon must finish");
        Run {
            stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
            code: out.status.code().unwrap_or(-1),
        }
    }

    fn ok(&self, args: &[&str]) -> Run {
        let run = self.run(args, "");
        assert_eq!(run.code, 0, "canon {args:?}:\n{}", run.all());
        run
    }

    /// Each source's id, by its path below `photos/`.
    fn source_id(&self, rel: &str) -> i64 {
        let run = self.ok(&["worklist", &self.path_str("photos")]);
        run.stdout
            .lines()
            .map(|l| serde_json::from_str::<serde_json::Value>(l).unwrap())
            .find(|r| {
                r["path"]
                    .as_str()
                    .unwrap()
                    .ends_with(&format!("/photos/{rel}"))
            })
            .unwrap_or_else(|| panic!("no source {rel}"))["source_id"]
            .as_i64()
            .unwrap()
    }

    /// Import `key` with `value` (JSON) on each source named.
    fn import(&self, key: &str, values: &[(&str, serde_json::Value)]) {
        let mut input = String::new();
        for (rel, value) in values {
            let record = serde_json::json!({
                "source_id": self.source_id(rel),
                "basis_rev": 0,
                "facts": { key: value },
            });
            input.push_str(&format!("{record}\n"));
        }
        let run = self.run(&["import-facts"], &input);
        assert_eq!(run.code, 0, "{}", run.all());
    }

    /// A manifest over `photos`, into `archive`, its pattern and base_dir as given.
    fn manifest(&self, pattern: &str, base_dir: &str) -> String {
        let manifest = self.path_str("").to_string() + "/m.toml";
        if !std::path::Path::new(&manifest).exists() {
            self.ok(&[
                "cluster",
                "generate",
                &self.path_str("photos"),
                "--dest",
                &self.path_str("archive"),
                "-o",
                &manifest,
                "--no-edit",
            ]);
        }
        let text = std::fs::read_to_string(&manifest).unwrap();
        let toml_string = |s: &str| toml::Value::String(s.to_string()).to_string();
        let edited: String = text
            .lines()
            .map(|l| {
                if l.starts_with("pattern = ") {
                    format!("pattern = {}\n", toml_string(pattern))
                } else if l.starts_with("base_dir = ") {
                    format!("base_dir = {}\n", toml_string(base_dir))
                } else {
                    format!("{l}\n")
                }
            })
            .collect();
        std::fs::write(&manifest, edited).unwrap();
        manifest
    }

    /// Every file under the archive root, its ledger left out.
    fn archive_files(&self) -> Vec<String> {
        let root = self.path("archive");
        let mut found = Vec::new();
        let mut stack = vec![root.clone()];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).unwrap() {
                let path = entry.unwrap().path();
                if path.file_name().is_some_and(|n| n == ".canon-ledger") {
                    continue;
                }
                if path.is_dir() {
                    stack.push(path);
                } else {
                    found.push(path.strip_prefix(&root).unwrap().display().to_string());
                }
            }
        }
        found.sort();
        found
    }

    fn apply_decisions(&self) -> usize {
        let run = self.ok(&["trail", "--global", "--jsonl"]);
        run.stdout
            .lines()
            .filter(|l| l.contains("\"apply\""))
            .count()
    }
}

/// **One `apply` run per cause a source can have no place for**, each refused with its
/// sentence and the way back for it, and each moving nothing, writing nothing and recording
/// nothing.
#[test]
fn apply_refuses_each_source_with_no_place_by_its_cause() {
    let lib = Library::new();
    lib.import(
        "Make",
        &[
            ("README", "AC/DC".into()),
            ("sub/x.jpg", "".into()),
            ("sub/y.jpg", "..".into()),
            ("sub/photo..jpg", "a\u{0}b".into()),
        ],
    );
    lib.import(
        "Taken",
        &[(
            "README",
            serde_json::json!({"value": "2024", "type": "datetime"}),
        )],
    );
    let refresh = format!("`canon cluster refresh {}/m.toml`", lib.path_str(""));
    let rows: [(&str, &str, &str); 7] = [
        (
            "{content.Make}/{filename}",
            "README: {content.Make} is `AC/DC`: a file or directory name cannot contain `/`",
            "Adjust the pattern in your manifest; or leave such sources out with the manifest's query",
        ),
        (
            "{content.Make}/{filename}",
            "x.jpg: {content.Make} is empty: a file or directory name cannot be empty",
            "Adjust the pattern in your manifest; or leave such sources out",
        ),
        (
            "{content.Make}/{filename}",
            "y.jpg: {content.Make} is `..`: a file or directory name cannot be `..`",
            "Adjust the pattern in your manifest; or leave such sources out",
        ),
        (
            "{content.Make}/{filename}",
            "photo..jpg: {content.Make} contains a null character: a file or directory name cannot contain one",
            "Adjust the pattern in your manifest; or leave such sources out",
        ),
        (
            "{content.Taken|month}/{filename}",
            "README: {content.Taken|month} has no value: the time is known only to the year",
            "Use a coarser modifier in the pattern, which takes effect at the next apply; or add \"content.Taken|month?\"",
        ),
        (
            "{content.Taken}/{filename}",
            "x.jpg: {content.Taken} has no value",
            "Adjust the pattern in your manifest, or import the missing facts; or add \"content.Taken?\"",
        ),
        (
            "{source.rel_path[1:]}",
            "README: {source.rel_path[1:]} has no segments: a file name cannot be empty",
            "Adjust the pattern in your manifest; or leave such sources out",
        ),
    ];
    for (pattern, sentence, remedy) in rows {
        let manifest = lib.manifest(pattern, "");
        let run = lib.run(&["apply", &manifest, "--yes"], "");
        let all = run.all();
        assert_eq!(run.code, 1, "{pattern}: {all}");
        assert!(all.contains(sentence), "{pattern}: {sentence}\n{all}");
        assert!(all.contains(remedy), "{pattern}: {remedy}\n{all}");
        assert!(all.contains(&refresh), "{all}");
        assert!(
            all.trim_end()
                .ends_with("Error: Aborting due to pattern expansion failures"),
            "{all}"
        );
        assert!(lib.archive_files().is_empty(), "{pattern}: nothing moved");
    }
    assert_eq!(lib.apply_decisions(), 0, "no decision recorded");

    // "1 source" for one, "N sources" for more.
    let manifest = lib.manifest("{source.rel_path[1:]}", "");
    let one = lib.run(&["apply", &manifest, "--yes"], "");
    assert!(
        one.all()
            .contains("Error: 1 source failed pattern expansion:"),
        "{}",
        one.all()
    );
    let manifest = lib.manifest("{content.Make}/{filename}", "");
    let four = lib.run(&["apply", &manifest, "--yes"], "");
    assert!(
        four.all()
            .contains("Error: 4 sources failed pattern expansion:"),
        "{}",
        four.all()
    );
}

/// **A pattern `apply` cannot read is refused whole**: its first line, every problem in the
/// caret format, exit 1, nothing planned, nothing moved, no decision.
#[test]
fn apply_refuses_a_pattern_it_cannot_read_with_every_problem() {
    let lib = Library::new();
    let manifest = lib.manifest("{Rating}/{source.size|year}//x/", "");
    let run = lib.run(&["apply", &manifest, "--yes"], "");
    assert_eq!(run.code, 1);
    let lines: Vec<&str> = run.stderr.lines().collect();
    assert_eq!(
        lines[0],
        "Error: cannot use the pattern as specified",
        "{}",
        run.all()
    );
    for sentence in [
        "^ `Rating` is an unknown key",
        "^ |year works on times or text; source.size is of type number",
        "^ an empty name between `//` cannot be a directory name",
        "^ a pattern cannot end with `/`",
    ] {
        assert!(run.stderr.contains(sentence), "{sentence}\n{}", run.all());
    }
    assert!(
        !run.all().contains("Running preflight checks"),
        "nothing planned"
    );
    assert!(lib.archive_files().is_empty());
    assert_eq!(lib.apply_decisions(), 0);
}

/// **A destination that would leave its directory is refused at preflight**, each one listed
/// whole, a null character as `\0`; and where the same run also has sources with no place, both
/// blocks print, the no-place list first, and the run ends on one line.
#[test]
fn apply_refuses_a_destination_that_would_leave_its_directory() {
    let lib = Library::new();
    let manifest = lib.manifest("{source.rel_path[:-1]}../{filename}", "");
    let run = lib.run(&["apply", &manifest, "--yes"], "");
    let all = run.all();
    assert_eq!(run.code, 1, "{all}");
    assert!(
        all.contains("Preflight failed: 1 destination path contains a `..` directory name:"),
        "{all}"
    );
    assert!(all.contains("  ../README (from source: "), "{all}");
    assert!(
        all.contains("Adjust the pattern in your manifest."),
        "{all}"
    );
    assert!(
        all.trim_end().ends_with(
            "Error: Aborting due to destination paths with a `..` directory name or a null character"
        ),
        "{all}"
    );
    assert!(lib.archive_files().is_empty());

    // A null character in the pattern's own text reaches every destination.
    let manifest = lib.manifest("a\u{0}b/{filename}", "");
    let run = lib.run(&["apply", &manifest, "--yes"], "");
    let all = run.all();
    assert!(
        all.contains("Preflight failed: 4 destination paths contain a null character:"),
        "{all}"
    );
    assert!(all.contains("  a\\0b/README (from source: "), "{all}");
    // Wherever either command shows the pattern or a path, a null character is `\0`.
    assert!(!all.contains('\0'), "{all:?}");
    let status = lib.run(&["cluster", "status", &manifest], "").all();
    assert!(status.contains("Pattern: a\\0b/{filename}"), "{status}");
    assert!(!status.contains('\0'), "{status:?}");

    // Both in one run: a source with no place, and one whose destination leaves.
    lib.import("Make", &[("README", "Canon".into())]);
    let manifest = lib.manifest("{source.rel_path[:-1]}../{content.Make}", "");
    let run = lib.run(&["apply", &manifest, "--yes"], "");
    let all = run.all();
    let no_place = all.find("failed pattern expansion:").expect(&all);
    let escaped = all.find("Preflight failed:").expect(&all);
    assert!(no_place < escaped, "the no-place list comes first: {all}");
    assert!(
        all.trim_end()
            .ends_with("Error: Aborting due to pattern expansion failures"),
        "{all}"
    );
    assert_eq!(all.matches("Error: Aborting").count(), 1, "{all}");
    assert_eq!(lib.apply_decisions(), 0);
}

/// **`cluster status` states a pattern it cannot read**, under its own line and in the caret
/// format, still checks every source file, says no destination was computed, and exits 0.
#[test]
fn status_states_a_pattern_it_cannot_read() {
    let lib = Library::new();
    let manifest = lib.manifest("{Rating}/{filename}", "");
    std::fs::remove_file(lib.path("photos/sub/x.jpg")).unwrap();
    let run = lib.run(&["cluster", "status", &manifest], "");
    let out = run.all();
    assert_eq!(run.code, 0, "{out}");
    assert!(
        out.contains(
            "apply would be rejected: the pattern cannot be used as specified — adjust the \
             pattern in your manifest"
        ),
        "{out}"
    );
    assert!(out.contains("    {Rating}/{filename}"), "{out}");
    assert!(out.contains("^ `Rating` is an unknown key"), "{out}");
    assert!(
        out.contains(
            "Summary: 4 entries with no destination computable: the pattern cannot be used as \
             specified; 1 source files missing."
        ),
        "the source files were checked: {out}"
    );
    assert!(!out.contains("To apply"), "{out}");

    // With no lock there are no entries to count as having no destination: the summary is the
    // one an absent lock gets whatever the pattern says.
    std::fs::remove_file(lib.path("m.lock")).unwrap();
    let run = lib.run(&["cluster", "status", &manifest], "");
    let out = run.all();
    assert!(out.contains("^ `Rating` is an unknown key"), "{out}");
    assert!(
        out.contains("Summary: 0 at destination, 0 pending, 0 source files missing."),
        "{out}"
    );
    assert!(!out.contains("with no destination computable"), "{out}");
}

/// **`cluster status` counts each cause once**, then lists the first sources; a destination
/// that would leave its directory is counted by what the sources share and never looked for on
/// disk — a file planted where it would land is not reported as at the destination.
#[test]
fn status_counts_each_cause_and_never_looks_outside_the_destination() {
    let lib = Library::new();
    lib.import(
        "Make",
        &[("README", "AC/DC".into()), ("sub/x.jpg", "".into())],
    );
    let manifest = lib.manifest("{content.Make}/{filename}", "");
    let run = lib.run(&["cluster", "status", &manifest], "");
    let out = run.all();
    assert_eq!(run.code, 0, "{out}");
    assert!(out.contains("4 of 4 entries have no destination:"), "{out}");
    for line in [
        "  1 source: {content.Make} contains `/`: a file or directory name cannot contain `/`",
        "  1 source: {content.Make} is empty: a file or directory name cannot be empty",
        "  2 sources: {content.Make} has no value",
    ] {
        assert!(out.contains(line), "{line}\n{out}");
    }
    assert!(
        out.contains("`canon facts` shows which sources have these facts"),
        "{out}"
    );
    // An entry the pattern gives no place is counted as that, never as pending, and the
    // lock's source files are not called accounted for.
    assert!(
        out.contains(
            "Summary: 0 at destination, 0 pending, 4 with no destination computable, 0 source \
             files missing."
        ),
        "{out}"
    );
    assert!(!out.contains("All source files accounted for"), "{out}");

    let manifest = lib.manifest("{source.rel_path[:-1]}../{filename}", "out");
    // Where `../README` under `out` would land: inside the archive root, outside `out`.
    std::fs::write(lib.path("archive/README"), "a").unwrap();
    let run = lib.run(&["cluster", "status", &manifest], "");
    let out = run.all();
    assert!(
        out.contains("  1 source: the destination contains a `..` directory name"),
        "{out}"
    );
    assert!(
        out.contains("Adjust the pattern in your manifest."),
        "{out}"
    );
    assert!(
        out.contains("Summary: 0 at destination, 3 pending, 1 with no destination computable,"),
        "never looked for on disk: {out}"
    );
}

/// **A `base_dir` that would leave the archive root**: `cluster status` states it as the act it
/// turns away, works out no destination, and still checks the source files; `apply` refuses it
/// in one sentence before anything is planned.
#[test]
fn a_base_dir_that_would_leave_the_archive_root() {
    let lib = Library::new();
    let manifest = lib.manifest("{filename}", "../x");
    let run = lib.run(&["cluster", "status", &manifest], "");
    let out = run.all();
    assert_eq!(run.code, 0, "{out}");
    // The destination shown is the archive root alone, never a path the next line refuses.
    let destination = out
        .lines()
        .find(|l| l.starts_with("Destination: "))
        .unwrap_or_else(|| panic!("{out}"));
    assert!(destination.ends_with("/archive"), "{out}");
    assert!(
        out.contains(
            "apply would be rejected: base_dir '../x' leaves the archive root — absolute \
             paths, `..` components and null characters are not allowed; edit base_dir in the \
             manifest's [output] section"
        ),
        "{out}"
    );
    assert!(
        out.contains(
            "Summary: 4 entries with no destination computable: base_dir leaves the archive \
             root; 0 source files missing."
        ),
        "{out}"
    );
    let run = lib.run(&["apply", &manifest, "--yes"], "");
    assert_eq!(run.code, 1);
    assert!(
        run.stderr.starts_with(
            "Error: Manifest base_dir '../x' leaves the archive root — absolute paths, `..` \
             components and null characters are not allowed."
        ),
        "{}",
        run.all()
    );
    assert!(lib.archive_files().is_empty());
}

/// **`cluster status` runs none of `apply`'s preflight checks**: with a file standing where a
/// directory must go, status names `apply` as the step, and `apply --dry-run` refuses.
#[test]
fn status_runs_none_of_applys_preflight_checks() {
    let lib = Library::new();
    let manifest = lib.manifest("sub/{filename}", "");
    std::fs::write(lib.path("archive/sub"), "a file where a directory must go").unwrap();
    let status = lib.run(&["cluster", "status", &manifest], "");
    assert!(
        status.all().contains("To apply: canon apply"),
        "{}",
        status.all()
    );
    let dry = lib.run(&["apply", &manifest, "--dry-run"], "");
    assert_eq!(dry.code, 1, "{}", dry.all());
    assert!(dry.all().contains("Preflight failed"), "{}", dry.all());
}

/// **The recorded destination is the one a scan finds**: with `base_dir = "out/./x"` and a
/// pattern leaving a `.` in the middle of a destination, the file on disk, the index row and the
/// receipt all name `out/x/…`, and a scan of the archive finds that path unchanged.
#[test]
fn the_recorded_destination_is_the_one_a_scan_finds() {
    let lib = Library::new();
    let manifest = lib.manifest("a/{source.rel_path[:-1]}./{filename}", "out/./x");
    lib.ok(&["apply", &manifest, "--yes"]);
    let files = lib.archive_files();
    assert!(files.contains(&"out/x/a/README".to_string()), "{files:?}");
    assert!(
        files.contains(&"out/x/a/sub./x.jpg".to_string()),
        "{files:?}"
    );

    // The index row.
    let indexed = lib.ok(&["ls", &lib.path_str("archive")]);
    assert!(
        indexed.stdout.contains("out/x/a/README"),
        "{}",
        indexed.all()
    );
    assert!(!indexed.stdout.contains("/./"), "{}", indexed.all());

    // The receipt.
    let mut receipts = Vec::new();
    let mut stack = vec![lib.path("archive/.canon-ledger")];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else {
                receipts.push(std::fs::read_to_string(path).unwrap());
            }
        }
    }
    let receipt = receipts.concat();
    assert!(
        receipt.contains("destination_rel_path = \"out/x/a/README\""),
        "{receipt}"
    );
    assert!(!receipt.contains("/./"), "{receipt}");

    // A scan finds the same path: nothing new, nothing gone.
    let scanned = lib.ok(&["scan", &lib.path_str("archive")]);
    let applied = lib.archive_files().len();
    assert!(
        scanned.all().contains(&format!(
            "Scanned {applied} files: 0 new, 0 updated, 0 moved, {applied} unchanged, 0 missing"
        )),
        "{}",
        scanned.all()
    );
}
