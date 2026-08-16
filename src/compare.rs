use anyhow::Result;
use std::path::Path;

use crate::core::domain::IncludeSet;
use crate::core::repo::Db;
use crate::ops;

pub struct CompareOptions {
    pub include: IncludeSet,
    pub verbose: bool,
}

pub fn run(
    db: &mut Db,
    path_a: &Path,
    path_b: &Path,
    filter_strs: &[String],
    options: &CompareOptions,
) -> Result<bool> {
    let run =
        ops::compare::run_compare(db.conn_mut(), path_a, path_b, filter_strs, &options.include)?;

    // Print header
    {
        use std::io::Write;
        let stdout = std::io::stdout();
        let mut handle = stdout.lock();
        let _ = writeln!(handle, "Comparing:");
        let _ = writeln!(handle, "  A: {}", run.prefix_a);
        let _ = writeln!(handle, "  B: {}", run.prefix_b);
        if options.include.includes_excluded() {
            let _ = writeln!(handle, "  [including excluded]");
        }
        let _ = writeln!(handle);
    }

    // Report unhashed and contentless files
    if run.total_unhashed > 0 {
        eprintln!(
            "Skipped {} unhashed files (use `canon worklist` to hash them)",
            run.total_unhashed
        );
    }
    if run.total_contentless > 0 {
        let noun = if run.total_contentless == 1 {
            "empty file"
        } else {
            "empty files"
        };
        eprintln!(
            "Skipped {} {noun} (no content to compare)",
            run.total_contentless
        );
    }
    if run.total_unhashed > 0 || run.total_contentless > 0 {
        eprintln!();
    }

    // Print summary (always show all lines, even if count is 0)
    {
        use std::io::Write;
        let stdout = std::io::stdout();
        let mut handle = stdout.lock();

        let _ = writeln!(handle, "Files in both (by content): {}", run.in_both_count);

        // Print only in A
        let _ = writeln!(handle, "Only in A: {}", run.only_in_a_paths.len());
        if options.verbose {
            for path in &run.only_in_a_paths {
                if writeln!(handle, "  {path}").is_err() {
                    break;
                }
            }
        }

        // Print only in B
        let _ = writeln!(handle, "Only in B: {}", run.only_in_b_paths.len());
        if options.verbose {
            for path in &run.only_in_b_paths {
                if writeln!(handle, "  {path}").is_err() {
                    break;
                }
            }
        }
    }

    // Visibility hint: if excluded? was used but --include excluded wasn't set
    if run.used_excluded && !options.include.includes_excluded() && run.total_excluded > 0 {
        eprintln!(
            "({} excluded sources hidden, use --include excluded to show)",
            run.total_excluded
        );
    }

    Ok(run.is_identical)
}
