//! Globs on the built binary: a glob after `~` read as a shell reads it — `*` any run, `?` one
//! visible character, sets in brackets, quoted parts literal — on a library holding the name
//! shapes these rules are about, both stored forms of `é` among them.
//!
//! Each selection row lists exactly the files its filter names. Each refusal row runs on `ls`
//! and on `exclude set`, under `--dry-run` and `--yes`, and asserts the exit code, the first
//! line, the caret block, nothing on stdout and no decision recorded. The names are generic
//! shapes, never anyone's own files.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::process::Command;

/// Every name shape the rows are about, as paths under the source root. The two stored forms
/// of `Café.jpg` sit in two directories, since a filesystem may hold only one of them per
/// directory.
const NAMES: &[&str] = &[
    "IMG_0001.jpg",
    "IMG_1.jpg",
    "IMG_*",
    "IMG*.jpg",
    "Holiday [2024].jpg",
    "Holiday 2024.jpg",
    "*star.jpg",
    "star.jpg",
    "icon@2x.png",
    "icon.png",
    "photo(1).jpg",
    "O'Brien.jpg",
    "John's.jpg",
    "Mary's.jpg",
    "my photo.jpg",
    "my_photo.jpg",
    "say \"hi\".jpg",
    "apple.jpg",
    "banana.jpg",
    "R&B#1.mp3",
    "a%b[c.txt",
    "\u{1F44D}\u{1F3FD}.jpg",
    "trips/My Pictures/beach.jpg",
    "[Disc 1]/track.mp3",
    "nfc/Caf\u{e9}.jpg",
    "nfd/Cafe\u{301}.jpg",
];

struct Library {
    dir: tempfile::TempDir,
}

struct Run {
    stdout: String,
    stderr: String,
    code: i32,
}

impl Library {
    /// The library, scanned as one source root.
    fn new() -> Library {
        let dir = tempfile::tempdir().expect("temp dir");
        let lib = Library { dir };
        std::fs::create_dir_all(lib.path("home")).unwrap();
        for (n, name) in NAMES.iter().enumerate() {
            let path = lib.path("src").join(name);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, format!("content {n}")).unwrap();
        }
        let src = lib.path_str("src");
        let run = lib.run(&["scan", "--add", "--role", "source", &src]);
        assert_eq!(run.code, 0, "{}{}", run.stdout, run.stderr);
        lib
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
        let out = Command::new(env!("CARGO_BIN_EXE_canon"))
            .args(args)
            .current_dir(self.dir.path())
            .env("CANON_HOME", self.path("home"))
            .env("TZ", "UTC")
            .stdin(std::process::Stdio::null())
            .output()
            .expect("canon must run");
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

    /// The paths under the root that `ls --where <filter>` lists; the filter must run.
    fn listed(&self, filter: &str) -> BTreeSet<String> {
        let run = self.run(&["ls", "--global", "--where", filter]);
        assert_eq!(run.code, 0, "{filter}: {}{}", run.stdout, run.stderr);
        let src = format!("{}/", self.path_str("src"));
        run.stdout
            .lines()
            .filter_map(|line| line.strip_prefix(&src))
            .map(str::to_string)
            .collect()
    }
}

fn names(of: &[&str]) -> BTreeSet<String> {
    of.iter().map(|n| n.to_string()).collect()
}

/// **Each glob selects exactly the files it names**: quoted parts literal and joined to the
/// unquoted ones, `?` one visible character on both stored forms, typed text compared as stored,
/// a space inside quotes part of the glob, two apostrophes closing each other as in a
/// shell, and a quoted glob its text — with `!~` of it everything else.
#[test]
fn every_glob_selects_exactly_the_files_it_names() {
    let lib = Library::new();
    let rows: &[(&str, &[&str])] = &[
        (r#"filename~*"[2024]"*"#, &["Holiday [2024].jpg"]),
        (r#"filename~"*"*"#, &["*star.jpg"]),
        (r#"filename~IMG"_1".jpg"#, &["IMG_1.jpg"]),
        (r#"filename~*"@"2x*"#, &["icon@2x.png"]),
        (
            "filename~Caf?.jpg",
            &["nfc/Caf\u{e9}.jpg", "nfd/Cafe\u{301}.jpg"],
        ),
        // Typed text compares exactly as stored: `Cafe` begins only the decomposed form.
        ("filename~Cafe*", &["nfd/Cafe\u{301}.jpg"]),
        ("filename~?.jpg", &["\u{1F44D}\u{1F3FD}.jpg"]),
        (
            r#"source.rel_path~*/"My Pictures"/*"#,
            &["trips/My Pictures/beach.jpg"],
        ),
        ("filename~[^a]* AND filename~*an*", &["banana.jpg"]),
        // One glob, as a shell reads it: the text `s* OR filename~*Mary` sits between the
        // two apostrophes, and no name holds it.
        ("filename~*John's* OR filename~*Mary's*", &[]),
        (
            r#"filename~*"'"*"#,
            &["John's.jpg", "Mary's.jpg", "O'Brien.jpg"],
        ),
        (r#"source.rel_path~"[Disc 1]"/*"#, &["[Disc 1]/track.mp3"]),
        (r#"filename~"IMG*.jpg""#, &["IMG*.jpg"]),
        (r#"filename~"IMG_*""#, &["IMG_*"]),
    ];
    for (filter, expected) in rows {
        assert_eq!(lib.listed(filter), names(expected), "{filter}");
    }
    // A set holding an underscore or a quote mark.
    assert_eq!(
        lib.listed(r#"filename~*[_"]*"#),
        names(&[
            "IMG_*",
            "IMG_0001.jpg",
            "IMG_1.jpg",
            "my_photo.jpg",
            "say \"hi\".jpg",
        ])
    );
    let everything = names(NAMES);
    let mut rest = everything.clone();
    rest.remove("IMG*.jpg");
    assert_eq!(lib.listed(r#"filename!~"IMG*.jpg""#), rest);
}

/// **A glob Canon cannot read stops the command**, on `ls` and on `exclude set` under
/// `--dry-run` and `--yes`, which records a decision when it runs:
/// a character that needs quotes, a set or quote never closed, a glob on a number or a time, a
/// backslash, and a part needing quotes before a stop — each with its caret, nothing listed,
/// nothing recorded.
#[test]
fn every_glob_refusal_stops_ls_and_exclude_set() {
    let lib = Library::new();
    let src = lib.path_str("src");
    let block = |filter: &str, at: usize, sentence: &str| {
        format!("  {filter}\n  {}^ {sentence}\n", " ".repeat(at))
    };
    let part = |c: char| format!("`{c}` needs quotes; quote that part, but not * ? [ ]");
    let rows: Vec<(&str, String)> = vec![
        ("filename~*#1*", block("filename~*#1*", 10, &part('#'))),
        (
            "filename~[abc",
            block("filename~[abc", 9, "this set is unclosed"),
        ),
        (
            "filename~O'Brien*",
            block("filename~O'Brien*", 10, "this quote is unclosed"),
        ),
        // A space inside a set ends it: a forgotten `]` never takes in the clause after it.
        (
            "filename~[abc OR filename~x.jpg]",
            block(
                "filename~[abc OR filename~x.jpg]",
                9,
                "this set is unclosed",
            ),
        ),
        (
            "filename~*[ _]*",
            block("filename~*[ _]*", 10, "this set is unclosed"),
        ),
        (
            "source.mtime~2024*",
            block(
                "source.mtime~2024*",
                13,
                "`~` works on text; source.mtime is of type time",
            ),
        ),
        (
            "source.size!~1*",
            block(
                "source.size!~1*",
                13,
                "`!~` works on text; source.size is of type number",
            ),
        ),
        (r"filename~a\b*", block(r"filename~a\b*", 10, &part('\\'))),
        (
            "filename~a%b[c",
            block("filename~a%b[c", 10, &part('%'))
                + &block("filename~a%b[c", 12, "this set is unclosed"),
        ),
    ];
    for (filter, blocks) in rows {
        for args in [
            vec!["ls", "--global", "--where", filter],
            vec!["exclude", "set", &src, "--where", filter, "--dry-run"],
            vec!["exclude", "set", &src, "--where", filter, "--yes"],
        ] {
            let before = lib.decisions();
            let run = lib.run(&args);
            assert_eq!(run.code, 1, "{args:?}:\n{}{}", run.stdout, run.stderr);
            assert_eq!(
                run.stderr,
                format!("Error: cannot run the filter as given\n{blocks}"),
                "{args:?}"
            );
            assert!(run.stdout.is_empty(), "{args:?}:\n{}", run.stdout);
            assert_eq!(lib.decisions(), before, "{args:?} recorded a decision");
        }
    }
}
