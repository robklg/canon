use anyhow::{bail, Context, Result};
use std::io::{self, Write};

/// Display "Proceed? [y/N]" and wait for user input.
/// Returns Ok(true) to proceed, Ok(false) if declined.
/// When `yes` is true, returns Ok(true) without prompting.
///
/// When showing confirmation content, gate it behind `!yes` before calling this function.
/// This ensures `--yes` skips both the content and the prompt.
pub fn confirm(yes: bool) -> Result<bool> {
    if yes {
        return Ok(true);
    }
    eprint!("Proceed? [y/N] ");
    io::stderr().flush()?;

    let mut input = String::new();
    io::stdin().read_line(&mut input)?;

    if input.trim().eq_ignore_ascii_case("y") {
        Ok(true)
    } else {
        eprintln!("Aborted.");
        Ok(false)
    }
}

/// Ask a yes/no question ("<question> [y/N]"); no is a plain answer, not an
/// abort. Callers gate on `--yes` themselves — this never auto-answers.
pub fn ask(question: &str) -> Result<bool> {
    eprint!("{question} [y/N] ");
    io::stderr().flush()?;
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    Ok(input.trim().eq_ignore_ascii_case("y"))
}

/// Open the user's editor (`$VISUAL`, then `$EDITOR` — the git precedent:
/// when prose is expected, open an editor) on `initial` in a temp file and
/// return what came back. `Ok(None)` when no editor is configured; a
/// non-zero editor exit is an error the caller handles explicitly.
pub fn edit_in_editor(initial: &str, name: &str) -> Result<Option<String>> {
    let editor = std::env::var("VISUAL")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .or_else(|| {
            std::env::var("EDITOR")
                .ok()
                .filter(|v| !v.trim().is_empty())
        });
    let Some(editor) = editor else {
        return Ok(None);
    };
    let path = std::env::temp_dir().join(format!("canon-{}-{name}", std::process::id()));
    std::fs::write(&path, initial)
        .with_context(|| format!("Could not write the draft to {}", path.display()))?;
    // The draft may hold personal prose; keep it out of other users' reach
    // while it sits in the shared temp dir.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
    }
    // The variable may carry arguments ("code --wait"); run it through sh
    // with the path as a positional argument so quoting stays the shell's.
    let status = std::process::Command::new("sh")
        .arg("-c")
        .arg(format!("{editor} \"$1\""))
        .arg("sh")
        .arg(&path)
        .status()
        .with_context(|| format!("Could not launch the editor ({editor})"))?;
    let read_back = std::fs::read_to_string(&path);
    if !status.success() {
        // An exit status is not the person's words. If the editor saved
        // changes before failing (a plugin dying at exit, a crash after
        // :wq), the saved text is the edit — keep it and say so. Only a
        // draft the editor left untouched is safe to drop.
        return match read_back {
            Ok(text) if text != initial => {
                let _ = std::fs::remove_file(&path);
                eprintln!(
                    "The editor exited with an error ({status}); your saved edits were kept."
                );
                Ok(Some(text))
            }
            _ => {
                let _ = std::fs::remove_file(&path);
                bail!("The editor exited with an error ({status}); the draft is unchanged.")
            }
        };
    }
    let text = read_back.with_context(|| {
        format!(
            "Could not read the edited draft back from {}",
            path.display()
        )
    })?;
    let _ = std::fs::remove_file(&path);
    Ok(Some(text))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One test, scenarios in sequence — `edit_in_editor` reads `$VISUAL`,
    /// and parallel tests mutating process env would race each other.
    #[test]
    fn edit_in_editor_never_destroys_saved_words() {
        // The editor saves, then exits nonzero (a plugin dying at exit, a
        // crash after :wq): the saved text is the edit — kept, not lost.
        std::env::set_var("VISUAL", "sh -c 'printf kept > \"$0\"; exit 3'");
        let out = edit_in_editor("draft", "test-fail-after-save").unwrap();
        assert_eq!(out.as_deref(), Some("kept"));

        // The editor fails without touching the draft: an error, and the
        // message claims no more than it knows — the draft is unchanged.
        std::env::set_var("VISUAL", "false");
        let err = edit_in_editor("draft", "test-fail-untouched").unwrap_err();
        assert!(err.to_string().contains("draft is unchanged"), "{err:#}");

        // A clean edit still round-trips.
        std::env::set_var("VISUAL", "sh -c 'printf edited > \"$0\"'");
        let out = edit_in_editor("draft", "test-clean").unwrap();
        assert_eq!(out.as_deref(), Some("edited"));

        std::env::remove_var("VISUAL");
    }
}
