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
    // The variable may carry arguments ("code --wait"); run it through sh
    // with the path as a positional argument so quoting stays the shell's.
    let status = std::process::Command::new("sh")
        .arg("-c")
        .arg(format!("{editor} \"$1\""))
        .arg("sh")
        .arg(&path)
        .status()
        .with_context(|| format!("Could not launch the editor ({editor})"))?;
    if !status.success() {
        let _ = std::fs::remove_file(&path);
        bail!("The editor exited without saving ({status})");
    }
    let text = std::fs::read_to_string(&path).with_context(|| {
        format!(
            "Could not read the edited draft back from {}",
            path.display()
        )
    })?;
    let _ = std::fs::remove_file(&path);
    Ok(Some(text))
}
