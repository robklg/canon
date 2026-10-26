//! **The aliases file, read**: the half of aliases that needs the outside world. The file is
//! read and handed to the pure check ([`check_aliases`]) the first time a reading
//! meets an alias use, and never otherwise.

use anyhow::{Context, Result};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::expr::domain::alias::{check_aliases, CheckedAliases, RawEntry};
use crate::expr::domain::problem::cut;

/// **Where a canon home keeps its aliases**: the one spelling of the file's name.
pub(in crate::expr) fn aliases_file(canon_home: &Path) -> PathBuf {
    canon_home.join("aliases.toml")
}

/// **Load and check the aliases file at `path`**: a file that is not there is no aliases at
/// all; one that cannot be read, or that TOML cannot read as names with text values, is refused
/// with the reader's own message beneath; every entry is then checked, and a broken one refuses
/// the file. Each entry carries the line its name stands on.
pub(in crate::expr) fn load_aliases(path: &Path) -> Result<CheckedAliases> {
    let cannot_be_read = || format!("the aliases file {} cannot be read", path.display());
    let content = match std::fs::read_to_string(path) {
        Ok(content) => content,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(check_aliases(path, false, Vec::new())?);
        }
        Err(e) => return Err(e).with_context(cannot_be_read),
    };
    let table: BTreeMap<toml::Spanned<String>, String> =
        toml::from_str(&content).with_context(cannot_be_read)?;
    let raw = table
        .into_iter()
        .map(|(name, value)| RawEntry {
            line: 1 + cut(&content, 0, name.span().start).matches('\n').count(),
            name: name.into_inner(),
            value,
        })
        .collect();
    Ok(check_aliases(path, true, raw)?)
}

/// What the aliases file says about a key: the aliases naming it, or that the file could not
/// be checked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AliasesNaming {
    Names(Vec<String>),
    FileHasErrors,
}

/// **The aliases naming a stored key** — awareness for a command about to change the key, never
/// a gate: a file that cannot be read or checked is `FileHasErrors`, for the caller to state; a
/// file that is not there names nothing.
pub fn aliases_naming_key(canon_home: &Path, key: &str) -> AliasesNaming {
    match load_aliases(&aliases_file(canon_home)) {
        Ok(aliases) => AliasesNaming::Names(aliases.naming_key(key)),
        Err(_) => AliasesNaming::FileHasErrors,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(content: &[u8]) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("aliases.toml");
        std::fs::write(&path, content).unwrap();
        (dir, path)
    }

    fn refusal(path: &Path) -> String {
        format!("{:?}", load_aliases(path).expect_err("refused"))
    }

    /// An entry's line is the line its name stands on, whatever TOML form its value takes — a
    /// basic string, a multi-line literal string, a quoted name — and an entry after a
    /// multi-line value is counted from the lines that value spans.
    #[test]
    fn the_line_is_where_the_entry_starts() {
        let (_dir, path) = file(
            b"# my aliases\n\
              a = \"x?\"\n\
              \n\
              b = '''\n\
              x?\n\
              AND y'''\n\
              \"my name\" = 'z?'\n\
              c = \"\"\n",
        );
        let text = refusal(&path);
        assert!(text.contains("  line 4: AND y\n"), "{text}");
        assert!(text.contains("  line 7: @my name: z?\n"), "{text}");
        assert!(text.contains("  line 8: @c: \n"), "{text}");
        assert!(!text.contains("line 2:"), "{text}");
    }

    /// A broken file read from disk: every broken entry under its line, in the order of the
    /// lines — `half` on line 9 after `nested` on line 8, though its name sorts first — and an
    /// empty value drawn as any problem.
    #[test]
    fn every_broken_entry_is_shown_with_its_line() {
        let (_dir, path) = file(
            b"image = \"content.mime IN (image/jpeg, image/png)\"\n\
              \n\n\n\n\n\n\
              nested = \"@image\"\n\
              half = \"source.ext IN jpg\"\n",
        );
        assert_eq!(
            load_aliases(&path).expect_err("refused").to_string(),
            format!(
                "the aliases file {} has entries that are not valid\n\
                 \x20 line 8: @nested: @image\n\
                 \x20                  ^ an alias cannot contain another alias\n\
                 \x20 line 9: @half: source.ext IN jpg\n\
                 \x20                              ^ `(` is missing after IN",
                path.display()
            )
        );
        let (_dir, path) = file(b"a = \"x?\"\nx = \"\"\n");
        let drawn = load_aliases(&path).expect_err("refused").to_string();
        assert_eq!(
            drawn.lines().skip(1).collect::<Vec<_>>(),
            ["  line 2: @x: ", "             ^ the filter is empty"]
        );
    }

    /// The check reads nothing from the library: entries naming keys no source has, or a
    /// comparison no key's type could take, load — the check takes no database at all.
    #[test]
    fn the_file_check_reads_nothing_from_the_library() {
        let (_dir, path) = file(b"a = \"Nowhere=5\"\nb = \"Never\"\nc = \"source.size~x*\"\n");
        let aliases = load_aliases(&path).expect("loads");
        assert!(aliases.exists());
        for name in ["a", "b", "c"] {
            assert!(aliases.get(name).is_some(), "{name}");
        }
    }

    /// A file TOML cannot read — not TOML, a value that is not text, a table — is refused with
    /// "cannot be read" and TOML's own message beneath; so is a file that is not UTF-8, with the
    /// system's.
    #[test]
    fn a_file_toml_cannot_read_is_refused_with_the_readers_line() {
        let first = |path: &Path| format!("the aliases file {} cannot be read", path.display());
        for content in [&b"image = "[..], b"n = 5\n", b"[a]\nb = \"x?\"\n"] {
            let (_dir, path) = file(content);
            let text = refusal(&path);
            assert!(text.starts_with(&first(&path)), "{text}");
            assert!(text.contains("Caused by:\n    TOML parse error"), "{text}");
        }
        let (_dir, path) = file(b"a = \"\xff\"\n");
        let text = refusal(&path);
        assert!(text.starts_with(&first(&path)), "{text}");
        assert!(
            text.contains("Caused by:\n    stream did not contain valid UTF-8"),
            "{text}"
        );
    }

    /// A key alias reading as the key and a filter alias naming it under `NOT` are named; an
    /// alias naming another key is not; a broken file is stated, never an error; no file names
    /// nothing.
    #[test]
    fn the_aliases_naming_a_key() {
        let (dir, _path) = file(
            b"taken = \"DateTimeOriginal\"\nundated = \"NOT DateTimeOriginal? AND source.ext=jpg\"\nmake = \"Make\"\nfull = \"content.DateTimeOriginal|year\"\n",
        );
        assert_eq!(
            aliases_naming_key(dir.path(), "content.DateTimeOriginal"),
            AliasesNaming::Names(vec!["taken".into(), "undated".into(), "full".into()])
        );
        assert_eq!(
            aliases_naming_key(dir.path(), "content.Width"),
            AliasesNaming::Names(Vec::new())
        );
        let (broken, _path) = file(b"bad = \"a AND\"\n");
        assert_eq!(
            aliases_naming_key(broken.path(), "content.DateTimeOriginal"),
            AliasesNaming::FileHasErrors
        );
        let none = tempfile::tempdir().unwrap();
        assert_eq!(
            aliases_naming_key(none.path(), "content.DateTimeOriginal"),
            AliasesNaming::Names(Vec::new())
        );
    }
}
