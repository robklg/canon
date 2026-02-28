use anyhow::Result;
use std::io::{self, Write};

/// Format a number with thousands separators (e.g., 3847 → "3,847").
pub fn format_count(n: usize) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

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
