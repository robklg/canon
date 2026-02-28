use anyhow::Result;
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
        println!("Aborted.");
        Ok(false)
    }
}
