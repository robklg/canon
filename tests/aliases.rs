//! Aliases on the built binary: a canon home whose `aliases.toml` the rows write, a source root
//! of a few files and an archive root. An alias is read where a condition starts or a key
//! stands, the file only once a use is met; a refusal stops the command with nothing recorded;
//! a manifest records what each alias stood for. The names are generic shapes.

use std::path::PathBuf;
use std::process::Command;

struct Library {
    dir: tempfile::TempDir,
}

struct Run {
    stdout: String,
    stderr: String,
    code: i32,
}

impl Library {
    /// `a.jpg`, `icon@2x.png` and `notes/c.txt` in one source root, `a.jpg` with a content fact,
    /// and an empty archive root.
    fn new() -> Library {
        let dir = tempfile::tempdir().expect("temp dir");
        let lib = Library { dir };
        std::fs::create_dir_all(lib.path("home")).unwrap();
        std::fs::create_dir_all(lib.path("archive")).unwrap();
        for (name, content) in [
            ("a.jpg", "a"),
            ("icon@2x.png", "bb"),
            ("notes/c.txt", "ccc"),
        ] {
            let path = lib.path("src").join(name);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, content).unwrap();
        }
        for (root, role) in [("src", "source"), ("archive", "archive")] {
            let run = lib.run(
                &["scan", "--add", "--role", role, &lib.path_str(root)],
                None,
            );
            assert_eq!(run.code, 0, "{}{}", run.stdout, run.stderr);
        }
        let worklist = lib.run(&["worklist", "--global"], None);
        let id = worklist
            .stdout
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .find(|v| v["path"].as_str().unwrap().ends_with("/a.jpg"))
            .expect("a.jpg is listed")["source_id"]
            .as_i64()
            .unwrap();
        let input = serde_json::json!({"source_id": id, "basis_rev": 0, "facts": {"Model": "X100"}})
            .to_string() + "\n";
        let run = lib.run(&["import-facts"], Some(&input));
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

    fn aliases_file(&self) -> PathBuf {
        self.path("home").join("aliases.toml")
    }

    fn write_aliases(&self, content: &str) {
        std::fs::write(self.aliases_file(), content).unwrap();
    }

    fn run(&self, args: &[&str], input: Option<&str>) -> Run {
        use std::io::Write;
        let mut child = Command::new(env!("CARGO_BIN_EXE_canon"))
            .args(args)
            .current_dir(self.dir.path())
            .env("CANON_HOME", self.path("home"))
            .env("TZ", "UTC")
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

    /// The file names `ls --where <filter>` lists; the filter must run.
    fn listed(&self, filter: &str) -> Vec<String> {
        let run = self.run(&["ls", "--global", "--where", filter], None);
        assert_eq!(run.code, 0, "{filter}: {}{}", run.stdout, run.stderr);
        let src = format!("{}/", self.path_str("src"));
        let mut names: Vec<String> = run
            .stdout
            .lines()
            .filter_map(|line| line.strip_prefix(&src))
            .map(str::to_string)
            .collect();
        names.sort();
        names
    }

    /// How many decisions the trail holds.
    fn decisions(&self) -> usize {
        let run = self.run(&["trail", "--global", "--jsonl"], None);
        run.stdout
            .lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .filter(|v| v["type"] == "decision")
            .count()
    }
}

const ALIASES: &str = "image = \"source.ext=jpg OR source.ext=png\"\n\
                       model = \"Model\"\n\
                       yr = \"source.mtime|year\"\n\
                       f = \"source.rel_path\"\n\
                       x = \"source.size~1*\"\n";

/// **An alias selects what it stands for**, where a condition starts and where a key stands —
/// a filter alias whole, negated, a key alias compared, continued by a modifier or an accessor.
#[test]
fn an_alias_selects_what_it_stands_for() {
    let lib = Library::new();
    lib.write_aliases(ALIASES);
    assert_eq!(lib.listed("@image"), ["a.jpg", "icon@2x.png"]);
    assert_eq!(lib.listed("NOT @image"), ["notes/c.txt"]);
    assert_eq!(lib.listed("@model=X100"), ["a.jpg"]);
    assert_eq!(lib.listed("@model|lowercase=x100"), ["a.jpg"]);
    assert_eq!(lib.listed("@f[0]=notes"), ["notes/c.txt"]);
    assert_eq!(lib.listed("@yr>2000 AND @image"), ["a.jpg", "icon@2x.png"]);
}

/// **The file is read only once a use is met**: with a broken aliases file present, a filter
/// with no alias runs or is refused for what it is — an unquoted `@` in a value needs quotes,
/// in a glob's set it is a member — and a filter with one meets the file's refusal.
#[test]
fn a_filter_with_no_alias_never_reads_the_file() {
    let lib = Library::new();
    lib.write_aliases("image = \"x?\"\nnested = \"@image\"\n");
    let run = lib.run(&["ls", "--global", "--where", "filename=icon@2x.png"], None);
    assert_eq!(run.code, 1);
    assert_eq!(
        run.stderr,
        "Error: cannot run the filter as given\n  \
         filename=icon@2x.png\n               \
         ^ `@` needs quotes around the value: filename=\"icon@2x.png\"\n"
    );
    assert_eq!(lib.listed("filename~*[@]*"), ["icon@2x.png"]);
    let run = lib.run(&["ls", "--global", "--where", "@image"], None);
    assert_eq!(run.code, 1);
    assert_eq!(
        run.stderr,
        format!(
            "Error: the aliases file {} has entries that are not valid\n  \
             line 2: @nested: @image\n                   \
             ^ an alias cannot contain another alias\n",
            lib.aliases_file().display()
        )
    );
    assert!(run.stdout.is_empty(), "{}", run.stdout);
}

/// **An alias Canon refuses stops an effectful command**, under `--dry-run` and `--yes`, with
/// nothing recorded: a problem inside an alias drawn under its use, a broken file, a missing
/// file.
#[test]
fn a_refused_alias_stops_exclude_set_and_records_nothing() {
    let lib = Library::new();
    let src = lib.path_str("src");
    let inside = "Error: cannot run the filter as given\n  @x\n  \
                  ^ in alias `@x`: `~` works on text; source.size is of type number\n"
        .to_string();
    let broken = format!(
        "Error: the aliases file {} has entries that are not valid\n  \
         line 1: @x: \n             ^ the filter is empty\n",
        lib.aliases_file().display()
    );
    let missing = format!(
        "Error: cannot run the filter as given\n  @x\n  \
         ^ `@x` is an unknown alias; there is no aliases file at {}\n",
        lib.aliases_file().display()
    );
    for (file, expected) in [
        (Some(ALIASES), inside),
        (Some("x = \"\"\n"), broken),
        (None, missing),
    ] {
        match file {
            Some(content) => lib.write_aliases(content),
            None => std::fs::remove_file(lib.aliases_file()).unwrap(),
        }
        for mode in ["--dry-run", "--yes"] {
            let before = lib.decisions();
            let run = lib.run(&["exclude", "set", &src, "--where", "@x", mode], None);
            assert_eq!(run.code, 1, "{mode}:\n{}{}", run.stdout, run.stderr);
            assert_eq!(run.stderr, expected, "{mode}");
            assert!(run.stdout.is_empty(), "{mode}:\n{}", run.stdout);
            assert_eq!(lib.decisions(), before, "{mode} recorded a decision");
        }
    }
}

/// **A manifest records what each alias stood for**: a filter alias in parentheses, with the
/// filter as given in an `# Original:` line; its query reads back as the selection it made.
#[test]
fn cluster_generate_records_each_alias_replaced() {
    let lib = Library::new();
    lib.write_aliases(ALIASES);
    let manifest = lib.path("m.toml");
    let dest = format!("{}/out", lib.path_str("archive"));
    let run = lib.run(
        &[
            "cluster",
            "generate",
            "--global",
            "--where",
            "@image AND @yr>2000",
            "--dest",
            &dest,
            "-o",
            &manifest.to_string_lossy(),
            "--no-edit",
        ],
        None,
    );
    assert_eq!(run.code, 0, "{}{}", run.stdout, run.stderr);
    let text = std::fs::read_to_string(&manifest).unwrap();
    assert!(text.contains("# Original: @image AND @yr>2000"), "{text}");
    assert!(
        text.contains(r#""(source.ext=jpg OR source.ext=png) AND source.mtime|year>2000""#),
        "{text}"
    );
    let status = lib.run(&["cluster", "status", &manifest.to_string_lossy()], None);
    assert_eq!(status.code, 0, "{}{}", status.stdout, status.stderr);
}

/// **A report reads a key alias as the key it stands for**, its header showing the key as
/// typed and then that key's kind, as every header does: no label for a content fact.
#[test]
fn facts_reads_a_key_alias_and_shows_it_as_typed() {
    let lib = Library::new();
    lib.write_aliases(ALIASES);
    let header = |key: &str| {
        let run = lib.run(&["facts", "--global", "--key", key], None);
        assert_eq!(run.code, 0, "{key}: {}{}", run.stdout, run.stderr);
        run.stdout
            .lines()
            .find(|line| line.contains("Count"))
            .unwrap_or_else(|| panic!("{key}: {}", run.stdout))
            .split("  ")
            .next()
            .unwrap()
            .to_string()
    };
    assert_eq!(header("@model"), "@model");
    assert_eq!(header("@yr"), "@yr (source fact)");
    let run = lib.run(
        &[
            "facts",
            "--global",
            "--key",
            "source.ext",
            "--group-by",
            "@yr",
        ],
        None,
    );
    assert_eq!(run.code, 0, "{}{}", run.stdout, run.stderr);
    assert!(
        run.stdout.contains("source.ext (grouped by @yr)"),
        "{}",
        run.stdout
    );
}
