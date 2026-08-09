use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;

use crate::ops::scope::parse_root_spec_any;
use crate::repo::{self, Db};
use crate::story::domain::place::StoryParams;
use crate::story::ops::render::story_lines;

/// `canon roots story` — the judgment instrument: a source root's
/// resolution story as a path-ordered map of places, where you acted and
/// what no decision ever touched. Read-only in the fullest sense: no
/// decision row, no receipt, no cache — fresh per run; exits 0 (a report —
/// the verdict belongs to `retire --dry-run`).
pub fn story(db: &Db, spec: &str, limit: usize, all: bool) -> Result<()> {
    let conn = db.conn();
    let roots = repo::root::fetch_all(conn)?;
    // `_any`: a suspended root's story reads fine — as last observed.
    let root_id = parse_root_spec_any(&roots, spec)?;
    let report = crate::story::ops::report::compute_story(conn, root_id, &StoryParams::default())?;

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let cap = if all { usize::MAX } else { limit };
    for line in story_lines(&report, cap, now) {
        println!("{line}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    #[test]
    fn story_argv_parses_through_the_real_cli() {
        for argv in [
            vec!["canon", "roots", "story", "path:/mnt/old-disk"],
            vec!["canon", "roots", "story", "id:3", "--limit", "10"],
            vec!["canon", "roots", "story", "id:3", "--all"],
        ] {
            crate::Cli::try_parse_from(&argv)
                .unwrap_or_else(|e| panic!("must parse: {argv:?}\n{e}"));
        }
        // --limit and --all are contradictory intents.
        assert!(crate::Cli::try_parse_from([
            "canon", "roots", "story", "id:3", "--limit", "5", "--all"
        ])
        .is_err());
    }
}
