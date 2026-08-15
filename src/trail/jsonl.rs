//! Trail's JSONL-rendering stratum: `--jsonl`'s machine-output completeness
//! contract, one typed object per line, view-independent (a decision
//! serializes identically wherever it was surfaced from).

use std::io::{self, Write};

use anyhow::Result;
use serde::Serialize;

use crate::core::domain::extraction::DecisionExtraction;
use crate::trail::domain::timeline::TimelineEvent;
use crate::trail::{TrailResult, TrailView};

#[derive(Serialize)]
struct JsonDecisionEvent<'a> {
    r#type: &'static str,
    id: i64,
    command: &'a str,
    created_at: i64,
    status: &'a str,
    count_attempted: Option<i64>,
    count_completed: Option<i64>,
    count_failed: Option<i64>,
    count_skipped: Option<i64>,
    reason: Option<&'a str>,
    scope: Option<&'a [String]>,
    summary: Option<&'a str>,
    receipt_root_id: Option<i64>,
    receipt_rel_path: Option<&'a str>,
    /// Additive; absent (not `[]`) for a decision with no extraction rows.
    /// Always the *full* row set for the decision, independent of which
    /// view/lens surfaced it (JSONL is a machine-output completeness
    /// contract, not a scoped-touching one).
    #[serde(skip_serializing_if = "Option::is_none")]
    extractions: Option<Vec<JsonExtraction<'a>>>,
}

#[derive(Serialize)]
struct JsonExtraction<'a> {
    root: &'a str,
    rel_prefix: &'a str,
    files: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    bytes: Option<i64>,
    destination: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    disposition: Option<&'static str>,
}

#[derive(Serialize)]
struct JsonNoteEvent<'a> {
    r#type: &'static str,
    created_at: i64,
    root_id: i64,
    rel_path: &'a str,
    text: &'a str,
}

#[derive(Serialize)]
pub(super) struct JsonRetiredScopeEvent<'a> {
    pub(super) r#type: &'static str,
    pub(super) root_path: &'a str,
    pub(super) retired_at: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) reason: Option<&'a str>,
    pub(super) book: &'a str,
    pub(super) decision_id: i64,
}

fn json_extractions(rows: Option<&[DecisionExtraction]>) -> Option<Vec<JsonExtraction<'_>>> {
    rows.map(|rows| {
        rows.iter()
            .map(|r| JsonExtraction {
                root: &r.root_path,
                rel_prefix: &r.rel_prefix,
                files: r.files,
                bytes: r.bytes,
                destination: &r.destination_path,
                disposition: r.disposition.map(|d| d.as_str()),
            })
            .collect()
    })
}

pub(super) fn print_jsonl(result: &TrailResult) -> Result<()> {
    let stdout = io::stdout();
    let mut handle = stdout.lock();
    let events: Box<dyn Iterator<Item = &TimelineEvent>> = match &result.view {
        TrailView::Recent(events) => Box::new(events.iter()),
        TrailView::Days(days) => Box::new(days.iter().flat_map(|d| d.events.iter())),
    };
    for event in events {
        let json = match event {
            TimelineEvent::Decision(d) => serde_json::to_string(&JsonDecisionEvent {
                r#type: "decision",
                id: d.id,
                command: &d.command,
                created_at: d.created_at,
                status: &d.status,
                count_attempted: d.count_attempted,
                count_completed: d.count_completed,
                count_failed: d.count_failed,
                count_skipped: d.count_skipped,
                reason: d.reason.as_deref(),
                scope: d.scope.as_deref(),
                summary: d.summary.as_deref(),
                receipt_root_id: d.receipt_root_id,
                receipt_rel_path: d.receipt_rel_path.as_deref(),
                extractions: json_extractions(result.extractions_all.get(&d.id).map(Vec::as_slice)),
            })?,
            TimelineEvent::Note(n) => serde_json::to_string(&JsonNoteEvent {
                r#type: "note",
                created_at: n.created_at,
                root_id: n.root_id,
                rel_path: &n.rel_path,
                text: &n.text,
            })?,
        };
        writeln!(handle, "{json}")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::domain::extraction::OriginDisposition;
    use crate::notes::Note;

    fn mk_note(text: &str) -> Note {
        Note {
            id: 1,
            root_id: 2,
            rel_path: "a/b".to_string(),
            text: text.to_string(),
            created_at: 1000,
        }
    }

    #[test]
    fn jsonl_note_event_shape() {
        let note = mk_note("check the RAW files");
        let json = serde_json::to_string(&JsonNoteEvent {
            r#type: "note",
            created_at: note.created_at,
            root_id: note.root_id,
            rel_path: &note.rel_path,
            text: &note.text,
        })
        .unwrap();
        assert_eq!(
            json,
            r#"{"type":"note","created_at":1000,"root_id":2,"rel_path":"a/b","text":"check the RAW files"}"#
        );
    }

    #[test]
    fn jsonl_retired_scope_event_shape() {
        // Under --jsonl the retired-scope statement is one typed object —
        // stdout stays machine-clean on this path too.
        let json = serde_json::to_string(&JsonRetiredScopeEvent {
            r#type: "retired_scope",
            root_path: "/vol/gone",
            retired_at: 1000,
            reason: Some("drive failing"),
            book: "/archive/retired/gone",
            decision_id: 42,
        })
        .unwrap();
        assert_eq!(
            json,
            r#"{"type":"retired_scope","root_path":"/vol/gone","retired_at":1000,"reason":"drive failing","book":"/archive/retired/gone","decision_id":42}"#
        );

        // A reasonless retirement omits the field, never nulls it.
        let json = serde_json::to_string(&JsonRetiredScopeEvent {
            r#type: "retired_scope",
            root_path: "/vol/gone",
            retired_at: 1000,
            reason: None,
            book: "/archive/retired/gone",
            decision_id: 42,
        })
        .unwrap();
        assert!(!json.contains("reason"));
    }

    #[test]
    fn jsonl_decision_event_shape() {
        let json = serde_json::to_string(&JsonDecisionEvent {
            r#type: "decision",
            id: 61,
            command: "exclude_duplicates",
            created_at: 1000,
            status: "completed",
            count_attempted: Some(315),
            count_completed: Some(210),
            count_failed: None,
            count_skipped: Some(105),
            reason: Some("redundant backup"),
            scope: None,
            summary: Some("Excluded 210 duplicates"),
            receipt_root_id: Some(3),
            receipt_rel_path: Some(".canon-ledger/000061-exclude_duplicates.toml"),
            extractions: None,
        })
        .unwrap();
        assert!(json.starts_with(r#"{"type":"decision","id":61,"command":"exclude_duplicates""#));
        assert!(json.contains(r#""reason":"redundant backup""#));
        assert!(!json.contains("extractions"));
    }

    #[test]
    fn jsonl_decision_event_extractions_present_when_some() {
        let row = crate::core::domain::extraction::DecisionExtraction {
            decision_id: 61,
            root_id: 1,
            root_path: "/vol/photos".to_string(),
            rel_prefix: "2016/italy".to_string(),
            files: 47,
            bytes: Some(3_900_000),
            destination_root_id: Some(9),
            destination_path: "/archive/2016/Italy".to_string(),
            disposition: Some(OriginDisposition::Retained),
        };
        let json = serde_json::to_string(&JsonDecisionEvent {
            r#type: "decision",
            id: 61,
            command: "apply",
            created_at: 1000,
            status: "completed",
            count_attempted: None,
            count_completed: None,
            count_failed: None,
            count_skipped: None,
            reason: None,
            scope: None,
            summary: None,
            receipt_root_id: None,
            receipt_rel_path: None,
            extractions: json_extractions(Some(&[row])),
        })
        .unwrap();
        assert!(json.contains(r#""extractions":[{"root":"/vol/photos""#));
        assert!(json.contains(r#""rel_prefix":"2016/italy""#));
        assert!(json.contains(r#""files":47"#));
        assert!(json.contains(r#""bytes":3900000"#));
        assert!(json.contains(r#""destination":"/archive/2016/Italy""#));
        assert!(json.contains(r#""disposition":"retained""#));
    }

    #[test]
    fn jsonl_decision_event_carries_no_view_dependent_card_or_rollup_data() {
        // JSONL is a completeness contract, not a scoped-touching one: the
        // same decision must serialize identically wherever it was surfaced
        // from. The card and the three rollups are all view-dependent, so
        // none of their vocabulary may appear in a machine-output event.
        let json = serde_json::to_string(&JsonDecisionEvent {
            r#type: "decision",
            id: 61,
            command: "apply",
            created_at: 1000,
            status: "completed",
            count_attempted: None,
            count_completed: None,
            count_failed: None,
            count_skipped: None,
            reason: None,
            scope: None,
            summary: None,
            receipt_root_id: None,
            receipt_rel_path: None,
            extractions: None,
        })
        .unwrap();
        for leaked in [
            "standing",
            "Standing",
            "rearranged",
            "Rearranged",
            "origins",
            "arrived",
            "Arrived",
            "indexed_here",
            "untracked",
        ] {
            assert!(!json.contains(leaked), "{leaked} leaked into JSONL: {json}");
        }
    }
}
