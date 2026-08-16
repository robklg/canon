use std::io::Write;

use crate::core::domain::format::cap_path;

/// Progress indicator for long-running operations
pub struct Progress {
    total: usize,
    interval: usize,
}

impl Progress {
    pub fn new(total: usize) -> Self {
        Self {
            total,
            interval: std::cmp::max(total / 20, 1),
        }
    }

    pub fn update(&self, i: usize) {
        if i > 0 && i.is_multiple_of(self.interval) {
            let pct = (i * 100) / self.total;
            eprint!("\r  {}% ({}/{})\x1b[K", pct, i, self.total);
            let _ = std::io::stderr().flush();
        }
    }

    pub fn update_with_name(&self, i: usize, name: &str) {
        if i > 0 && i.is_multiple_of(self.interval) {
            let pct = (i * 100) / self.total;
            // Truncate long filenames to keep the line readable.
            let display_name = cap_path(name, 40);
            eprint!("\r  {}% ({}/{}) {}\x1b[K", pct, i, self.total, display_name);
            let _ = std::io::stderr().flush();
        }
    }

    pub fn finish(&self) {
        if self.total > self.interval {
            eprint!("\r  100% ({}/{})\x1b[K\n", self.total, self.total);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Filenames are user data, and any of them can reach the progress line.
    /// Trimming one to fit the terminal must never be able to stop a scan or an
    /// apply partway through, whatever alphabet the name is written in.
    #[test]
    fn long_multibyte_names_do_not_stop_the_run() {
        let progress = Progress::new(20);

        // Long enough to be trimmed, in a script where no character is one
        // byte — so a cut measured in bytes lands inside a character.
        progress.update_with_name(1, &"文".repeat(50));
        // Just past the trim threshold, where the cut is nearest the start.
        progress.update_with_name(1, &"文".repeat(41));
        // Mixed widths, so the cut point depends on where the wide runs sit.
        progress.update_with_name(1, &format!("{}{}", "a".repeat(30), "→".repeat(20)));
        // Short enough to pass through untouched.
        progress.update_with_name(1, "café.jpg");
    }

    /// The trimmed name is bounded in characters, which is what fits a terminal
    /// column — bounding it in bytes would leave wide scripts overflowing.
    #[test]
    fn trimmed_name_is_bounded_in_characters() {
        let trimmed = cap_path(&"文".repeat(50), 40);
        assert_eq!(trimmed.chars().count(), 40);
        assert!(trimmed.starts_with("..."));
    }
}
