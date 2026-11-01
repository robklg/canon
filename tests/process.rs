//! What the process itself does, read from the built binary.
//!
//! A version flag, a closed pipe and the order a scan writes its record in are
//! properties of what the process printed, wrote and how it ended, so only a
//! process can be asked.

use std::path::Path;
use std::process::{Command, Output, Stdio};

fn canon(home: &Path, args: &[&str]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_canon"));
    command.env("CANON_HOME", home).args(args);
    command
}

/// Parsing answers `--version` before anything opens the database, so a home
/// that does not exist is not created by asking.
#[test]
fn the_version_flag_prints_the_package_version() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path().join("no-home-yet");

    let out = canon(&home, &["--version"]).output().unwrap();

    assert_eq!(out.status.code(), Some(0));
    assert_eq!(
        String::from_utf8_lossy(&out.stdout).trim(),
        format!("canon {}", env!("CARGO_PKG_VERSION"))
    );
    assert!(!home.exists(), "the flag touches no state");
}

/// Runs `args` with stdout already closed at its far end. The reader goes
/// before the spawn: closed after it, the reader would race the child.
#[cfg(unix)]
fn run_with_a_closed_stdout(home: &Path, args: &[&str]) -> Output {
    let (reader, writer) = std::io::pipe().unwrap();
    drop(reader);
    canon(home, args)
        .stdout(writer)
        .stderr(Stdio::piped())
        .output()
        .unwrap()
}

/// A closed pipe ends the command the way it ends any other Unix filter.
#[cfg(unix)]
#[test]
fn a_closed_pipe_ends_the_command_without_a_panic() {
    use std::os::unix::process::ExitStatusExt;

    let dir = tempfile::tempdir().unwrap();

    let out = run_with_a_closed_stdout(dir.path(), &["roots", "list"]);

    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!stderr.contains("panicked"), "{stderr}");
    assert_eq!(out.status.signal(), Some(13), "{stderr}");
}

fn run_ok(home: &Path, args: &[&str]) -> Output {
    let out = canon(home, args).output().unwrap();
    assert!(
        out.status.success(),
        "canon {args:?}\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    out
}

/// A scanned root with two files, one of them then deleted from disk.
#[cfg(unix)]
fn root_that_lost_a_file(dir: &Path) -> (std::path::PathBuf, std::path::PathBuf) {
    let home = dir.join("home");
    let root = dir.join("root");
    std::fs::create_dir(&root).unwrap();
    std::fs::write(root.join("kept.txt"), "kept").unwrap();
    std::fs::write(root.join("lost.txt"), "lost").unwrap();
    run_ok(
        &home,
        &["scan", "--add", "--role", "source", root.to_str().unwrap()],
    );
    std::fs::remove_file(root.join("lost.txt")).unwrap();
    (home, root)
}

/// A relative path with no current directory to resolve it against is refused
/// by name on both arms, and leaves no row. A process cannot be left without a
/// current directory inside a unit test, so this one runs through a shell. The
/// allowed case is the same relative form from a directory that exists.
#[cfg(unix)]
#[test]
fn a_scan_from_a_directory_that_is_gone_writes_no_row() {
    let dir = tempfile::tempdir().unwrap();
    let top = std::fs::canonicalize(dir.path()).unwrap();
    let home = top.join("home");
    let gone = top.join("gone");

    for flags in ["", "--missing"] {
        std::fs::create_dir(&gone).unwrap();
        let line = format!(
            "cd '{gone}' && rmdir '{gone}' && exec '{canon}' scan {flags} relative",
            gone = gone.display(),
            canon = env!("CARGO_BIN_EXE_canon"),
        );
        let out = Command::new("sh")
            .args(["-c", &line])
            .env("CANON_HOME", &home)
            .output()
            .unwrap();
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert_eq!(out.status.code(), Some(1), "{stderr}");
        assert!(
            stderr.contains("current directory is unavailable"),
            "{stderr}"
        );
    }
    let trail = run_ok(&home, &["trail", "--global"]);
    let trail = String::from_utf8_lossy(&trail.stdout);
    assert!(!trail.contains("#1"), "{trail}");

    let root = top.join("root");
    std::fs::create_dir(&root).unwrap();
    let added = canon(&home, &["scan", "--add", "--role", "source", "."])
        .current_dir(&root)
        .output()
        .unwrap();
    assert!(added.status.success());
    let trail = run_ok(&home, &["trail", "--global"]);
    let trail = String::from_utf8_lossy(&trail.stdout);
    assert!(trail.contains("#1"), "{trail}");
}

/// The warning that a receipt could not be written is the only sign the record
/// itself failed, and stderr is still the terminal under `| head`. It is said
/// before the summary line, where a closed pipe ends the process.
#[cfg(unix)]
#[test]
fn a_closed_pipe_still_says_a_receipt_could_not_be_written() {
    let dir = tempfile::tempdir().unwrap();
    let (home, root) = root_that_lost_a_file(dir.path());
    // A file where the ledger directory has to go.
    std::fs::write(root.join(".canon-ledger"), "in the way").unwrap();

    let out = run_with_a_closed_stdout(&home, &["scan", root.to_str().unwrap()]);

    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("receipt"), "{stderr}");
}

/// A closed pipe costs the summary line, never the record: the deletion's
/// receipt stands and the row is settled, because the line prints last.
#[cfg(unix)]
#[test]
fn a_closed_pipe_leaves_a_scans_record_whole() {
    let dir = tempfile::tempdir().unwrap();
    let (home, root) = root_that_lost_a_file(dir.path());

    run_with_a_closed_stdout(&home, &["scan", root.to_str().unwrap()]);

    let receipts = std::fs::read_dir(root.join(".canon-ledger"))
        .expect("the deletion's receipt stands under the root")
        .count();
    assert_eq!(receipts, 1);
    let trail = run_ok(&home, &["trail", "--global"]);
    let trail = String::from_utf8_lossy(&trail.stdout);
    assert!(trail.contains("scan"), "{trail}");
    assert!(!trail.contains("[started]"), "{trail}");

    // Allowed: with an open stdout the same scan states its summary.
    let dir = tempfile::tempdir().unwrap();
    let (home, root) = root_that_lost_a_file(dir.path());
    let out = run_ok(&home, &["scan", root.to_str().unwrap()]);
    assert!(
        String::from_utf8_lossy(&out.stdout).contains("missing"),
        "{}",
        String::from_utf8_lossy(&out.stdout)
    );
}
