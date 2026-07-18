//! ledger command — maintain the extraction ledger index.
//!
//! Interface layer only: parse args, call ops::ledger, format the coverage
//! report. `canon ledger reindex` is a maintenance command, not a content
//! decision — it writes no decision row; its printed report is its record.

use anyhow::Result;

use crate::domain::format::format_count;
use crate::ops::ledger::{reindex_extractions, ReindexParams, ReindexResult};
use crate::repo::Db;

pub fn run_reindex(db: &mut Db, dry_run: bool) -> Result<()> {
    let result = reindex_extractions(db.conn_mut(), &ReindexParams { dry_run })?;
    print!("{}", format_report(&result, dry_run));

    // Nonzero only when nothing at all could be processed — every scanned
    // decision landed in a skip bucket and nothing was (or would be) indexed.
    let nothing_processed =
        result.scanned > 0 && result.indexed.is_empty() && result.already_current.is_empty();
    if nothing_processed {
        std::process::exit(1);
    }

    Ok(())
}

fn format_report(result: &ReindexResult, dry_run: bool) -> String {
    let mut out = String::new();
    out.push_str("Ledger reindex: extraction index\n");
    out.push_str(&format!(
        "Scanned {} apply decision{}.\n\n",
        format_count(result.scanned),
        if result.scanned == 1 { "" } else { "s" }
    ));

    let indexed_label = if dry_run { "would index" } else { "indexed" };
    let rows_note = if dry_run {
        String::new()
    } else {
        format!(" ({} rows)", format_count(result.rows_written))
    };
    out.push_str(&format!(
        "  {indexed_label}:  {} decisions{rows_note}\n",
        format_count(result.indexed.len())
    ));
    out.push_str(&format!(
        "  already current:  {}\n",
        format_count(result.already_current.len())
    ));

    push_category(&mut out, "no receipt", &result.no_receipt);
    push_category(&mut out, "unreachable", &result.unreachable);
    push_category(&mut out, "malformed", &result.malformed);
    if !result.unknown_source_roots.is_empty() {
        push_category(
            &mut out,
            "partial (unmatched source root)",
            &result.unknown_source_roots,
        );
    }

    if dry_run {
        out.push_str("\n(dry run — nothing written)\n");
    }

    if !result.unreachable.is_empty() {
        out.push_str(
            "\nUnreachable receipts are retried on the next run; nothing is concluded from absence.\n",
        );
    }

    out
}

/// Print one category line (`  label:  N`) plus a `#id  detail` line per
/// entry. Zero-entry categories print the count only — no empty block.
fn push_category(out: &mut String, label: &str, entries: &[(i64, String)]) {
    out.push_str(&format!("  {label}:  {}\n", format_count(entries.len())));
    for (id, detail) in entries {
        out.push_str(&format!("    #{id}  {detail}\n"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_result() -> ReindexResult {
        ReindexResult {
            scanned: 4,
            indexed: vec![1, 2],
            rows_written: 3,
            already_current: vec![3],
            no_receipt: vec![],
            unreachable: vec![],
            malformed: vec![],
            unknown_source_roots: vec![],
        }
    }

    #[test]
    fn report_zero_entry_categories_print_count_only() {
        let result = base_result();
        let report = format_report(&result, false);
        assert!(report.contains("no receipt:  0"));
        assert!(report.contains("unreachable:  0"));
        assert!(report.contains("malformed:  0"));
        assert!(!report.contains('#'));
    }

    #[test]
    fn report_includes_reasons_per_category() {
        let mut result = base_result();
        result.no_receipt = vec![(12, "recording mode had receipts off".to_string())];
        result.unreachable = vec![(
            87,
            "root path not present (offline?): /Volumes/x".to_string(),
        )];
        let report = format_report(&result, false);
        assert!(report.contains("#12  recording mode had receipts off"));
        assert!(report.contains("#87  root path not present (offline?): /Volumes/x"));
    }

    #[test]
    fn report_unknown_source_roots_reported_as_partial() {
        let mut result = base_result();
        result.unknown_source_roots = vec![(55, "/vol/gone".to_string())];
        let report = format_report(&result, false);
        assert!(report.contains("partial (unmatched source root):  1"));
        assert!(report.contains("#55  /vol/gone"));
    }

    #[test]
    fn report_closing_line_only_when_unreachable_present() {
        let result = base_result();
        let report = format_report(&result, false);
        assert!(!report.contains("Unreachable receipts are retried"));

        let mut with_unreachable = base_result();
        with_unreachable.unreachable = vec![(1, "reason".to_string())];
        let report = format_report(&with_unreachable, false);
        assert!(report.contains("Unreachable receipts are retried"));
    }

    #[test]
    fn report_dry_run_uses_would_index_and_nothing_written_note() {
        let result = base_result();
        let report = format_report(&result, true);
        assert!(report.contains("would index"));
        assert!(report.contains("(dry run — nothing written)"));
        assert!(!report.contains("rows)"));
    }

    #[test]
    fn report_non_dry_run_shows_rows_written() {
        let result = base_result();
        let report = format_report(&result, false);
        assert!(report.contains("(3 rows)"));
    }
}
