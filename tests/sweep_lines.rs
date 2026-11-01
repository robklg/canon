//! Every `→` line a real sweep prints, run as printed.
//!
//! The round-trip law parses each line against the argument definitions, which
//! proves it is a command and never that the command runs: survey's refusal of
//! an archive scope happens after parsing. This file is the pin parsing cannot
//! be. It reads the lines off a real board and hands each to a shell, so the
//! quoting is the quoting the user would paste.

use std::path::Path;
use std::process::Command;

fn canon(home: &Path, cwd: &Path, args: &[&str]) -> String {
    let out = Command::new(env!("CARGO_BIN_EXE_canon"))
        .env("CANON_HOME", home)
        .current_dir(cwd)
        .args(args)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "canon {args:?}\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

/// Six distinct files of 400 KB each, big enough for a board under `--all`.
fn write_files(dir: &Path, tag: &str) {
    std::fs::create_dir_all(dir).unwrap();
    for i in 0..6 {
        let first = format!("{tag}-{i}");
        let mut content = first.into_bytes();
        content.resize(400 * 1024, b'x');
        std::fs::write(dir.join(format!("{tag}-{i}.bin")), content).unwrap();
    }
}

fn copy_files(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        std::fs::copy(entry.path(), to.join(entry.file_name())).unwrap();
    }
}

/// The shape of a real library: two source roots whose content also stands in
/// one archive folder, a folder mirrored across two source roots, and a folder
/// standing once on a source root and once in the archive. Run from a directory
/// outside every root, as a user reading a universe-wide board is.
#[test]
fn every_printed_line_runs_as_printed() {
    let dir = tempfile::tempdir().unwrap();
    let top = std::fs::canonicalize(dir.path()).unwrap();
    let home = top.join("home");
    let elsewhere = top.join("elsewhere");
    std::fs::create_dir(&elsewhere).unwrap();

    write_files(&top.join("drive-a/photos"), "a");
    write_files(&top.join("drive-b/photos"), "b");
    copy_files(&top.join("drive-a/photos"), &top.join("archive/kept"));
    copy_files(&top.join("drive-b/photos"), &top.join("archive/kept"));
    write_files(&top.join("drive-c/set"), "c");
    copy_files(&top.join("drive-c/set"), &top.join("drive-d/set"));
    write_files(&top.join("drive-e/solo"), "e");
    copy_files(&top.join("drive-e/solo"), &top.join("archive/solo"));

    for (root, role) in [
        ("drive-a", "source"),
        ("drive-b", "source"),
        ("drive-c", "source"),
        ("drive-d", "source"),
        ("drive-e", "source"),
        ("archive", "archive"),
    ] {
        let path = top.join(root);
        canon(
            &home,
            &elsewhere,
            &["scan", "--add", "--role", role, path.to_str().unwrap()],
        );
    }

    let board = canon(&home, &elsewhere, &["sweep", "--all"]);
    let lines: Vec<&str> = board
        .lines()
        .filter_map(|line| line.trim_start().strip_prefix("→ "))
        .collect();
    assert!(lines.len() >= 2, "an empty board proves nothing:\n{board}");

    let archive_folder = top.join("archive/kept");
    assert!(
        lines.iter().any(|line| line.contains("--other")
            && line.contains(archive_folder.to_str().unwrap())),
        "a line compares against the archive folder:\n{board}"
    );

    let bin_dir = Path::new(env!("CARGO_BIN_EXE_canon")).parent().unwrap();
    let path = format!(
        "{}:{}",
        bin_dir.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    for line in lines {
        let out = Command::new("sh")
            .args(["-c", line])
            .env("PATH", &path)
            .env("CANON_HOME", &home)
            .current_dir(&elsewhere)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "`{line}` must run as printed\n{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
    }
}
