//! Fixtures shared across the retirement ops test modules — genuinely
//! cross-cutting helpers only; a helper used by a single test file lives
//! there instead.

use std::path::Path;

use rusqlite::Connection;

use crate::core::domain::config::{LedgerConfig, RecordingMode};
use crate::core::domain::extraction::{DecisionExtraction, OriginDisposition};
use crate::core::domain::time::zone::{ReadTimesOn, UserZone};
use crate::core::ops::root_story::fetch_root_story;
use crate::core::repo;
use crate::core::repo::db::open_in_memory_for_test;
use crate::core::repo::insert_test_root;
use crate::retire::ops::compile::{compile_book, CompileParams, CompiledBook};
use crate::retire::ops::frame::TellingArtifact;
use crate::retire::ops::{
    begin_ceremony, plan_bind, readiness_lens, CeremonyParams, RetireCeremony,
};
use crate::story::StoryParams;

pub(super) fn test_telling() -> TellingArtifact {
    TellingArtifact {
        text: "# a test telling\n".to_string(),
        hand_edited: false,
        params: StoryParams::default(),
    }
}

/// A bound retirement of a (since-removed) root: decision with an
/// artifact reference + a scope-row path snapshot — the shape the
/// ceremony's `begin`/`bind` leave behind.
pub(super) fn insert_bound_retirement(
    conn: &Connection,
    retired_root_path: &str,
    created_at: i64,
    receipt_root_id: i64,
    receipt_rel_path: Option<&str>,
    reason: Option<&str>,
) -> i64 {
    conn.execute(
        "INSERT INTO decisions
             (command, command_line, status, canon_version, created_at, reason,
              receipt_root_id, receipt_rel_path)
             VALUES ('roots_retire', 'canon roots retire', 'completed', 'test', ?1, ?2,
                     ?3, ?4)",
        rusqlite::params![
            created_at,
            reason,
            receipt_rel_path.map(|_| receipt_root_id),
            receipt_rel_path
        ],
    )
    .unwrap();
    let decision_id = conn.last_insert_rowid();
    conn.execute(
        "INSERT INTO decision_scopes (decision_id, root_id, root_path, rel_prefix)
             VALUES (?1, 999, ?2, '')",
        rusqlite::params![decision_id, retired_root_path],
    )
    .unwrap();
    decision_id
}

pub(super) fn insert_source(
    conn: &Connection,
    root_id: i64,
    rel_path: &str,
    object_id: Option<i64>,
    present: bool,
    excluded: bool,
    decision_id: Option<i64>,
) -> i64 {
    conn.execute(
            "INSERT INTO sources (root_id, rel_path, object_id, device, inode, size, mtime,
                                  partial_hash, scanned_at, last_seen_at, present, excluded, decision_id)
             VALUES (?, ?, ?, 0, 0, 1000, 0, 'hash', 0, 0, ?, ?, ?)",
            rusqlite::params![
                root_id,
                rel_path,
                object_id,
                present as i64,
                excluded as i64,
                decision_id
            ],
        )
        .unwrap();
    conn.last_insert_rowid()
}

pub(super) fn insert_object(conn: &Connection, hash: &str) -> i64 {
    conn.execute(
        "INSERT INTO objects (hash_type, hash_value) VALUES ('sha256', ?)",
        [hash],
    )
    .unwrap();
    conn.last_insert_rowid()
}

pub(super) fn insert_decision(conn: &Connection, command: &str, created_at: i64) -> i64 {
    conn.execute(
        "INSERT INTO decisions (command, command_line, status, canon_version, created_at)
             VALUES (?1, 'test', 'completed', '0', ?2)",
        rusqlite::params![command, created_at],
    )
    .unwrap();
    conn.last_insert_rowid()
}

pub(super) fn scope(conn: &Connection, decision_id: i64, root_id: i64) {
    conn.execute(
        "INSERT INTO decision_scopes (decision_id, root_id, root_path, rel_prefix)
             VALUES (?1, ?2, '/r', '')",
        rusqlite::params![decision_id, root_id],
    )
    .unwrap();
}

pub(super) fn extraction_from(conn: &Connection, decision_id: i64, root_id: i64, files: i64) {
    repo::decision::replace_extractions(
        conn,
        &[DecisionExtraction {
            decision_id,
            root_id,
            root_path: "/r".to_string(),
            rel_prefix: String::new(),
            files,
            bytes: Some(files * 100),
            destination_root_id: Some(999),
            destination_path: "/archive/dest".to_string(),
            disposition: Some(OriginDisposition::Relocated),
        }],
    )
    .unwrap();
}

pub(super) fn ledger_config() -> LedgerConfig {
    LedgerConfig::default()
}

pub(super) fn set_decision_extras(
    conn: &Connection,
    decision_id: i64,
    summary: Option<&str>,
    reason: Option<&str>,
) {
    conn.execute(
        "UPDATE decisions SET summary = ?2, reason = ?3 WHERE id = ?1",
        rusqlite::params![decision_id, summary, reason],
    )
    .unwrap();
}

pub(super) fn set_decision_receipt(
    conn: &Connection,
    decision_id: i64,
    root_id: i64,
    rel_path: &str,
) {
    conn.execute(
        "UPDATE decisions SET receipt_root_id = ?2, receipt_rel_path = ?3 WHERE id = ?1",
        rusqlite::params![decision_id, root_id, rel_path],
    )
    .unwrap();
}

pub(super) fn compile_to(conn: &Connection, root_id: i64, dest: &Path) -> CompiledBook {
    compile_on(conn, root_id, dest, UserZone::utc())
}

/// [`compile_to`] with the binder's clock named.
pub(super) fn compile_on(
    conn: &Connection,
    root_id: i64,
    dest: &Path,
    zone: UserZone,
) -> CompiledBook {
    let story = fetch_root_story(conn, root_id).unwrap();
    let origins = crate::retire::ops::compile::collect_origins(conn, &story).unwrap();
    compile_book(
        conn,
        &story,
        &CompileParams {
            reason: Some("story complete".to_string()),
            now: 1_753_000_000,
            read_times_on: ReadTimesOn::of_user(&zone),
            zone,
            dest_dir: dest.to_path_buf(),
            ceremony_decision_id: None,
            telling: Some(test_telling()),
            origins: &origins,
        },
    )
    .unwrap()
}

pub(super) fn inventory_lines(dir: &Path) -> Vec<serde_json::Value> {
    std::fs::read_to_string(dir.join("inventory.jsonl"))
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

/// The drive the copies lifecycle reproduces: `photos/` holds five files an
/// apply chose into the archive's `photos/`, and seven copies of them stand
/// in `backup/` (five) and `docs/` (two) — content in the archive that
/// nobody chose from there.
pub(super) struct Copies {
    pub(super) conn: Connection,
    pub(super) src: tempfile::TempDir,
    pub(super) arch: tempfile::TempDir,
    pub(super) root_id: i64,
    pub(super) apply_id: i64,
}

/// What the copies fixture varies.
#[derive(Clone, Copy)]
pub(super) struct CopiesShape {
    /// The apply left a receipt on disk (`Full`), or none (`Records`).
    pub(super) receipt: bool,
    /// The apply moved the chosen files (no origin row stands) instead of
    /// copying them.
    pub(super) moved: bool,
    /// The seven copies stand on the drive.
    pub(super) copies: bool,
}

/// Build [`Copies`]: the archive side is what an apply leaves — its copies
/// stamped with the apply's id, its extraction rows aggregated by the real
/// `build_extraction_rows`, and, with `receipt`, its receipt in the archive's
/// ledger naming each chosen file.
pub(super) fn copies_fixture(shape: CopiesShape) -> Copies {
    use crate::core::domain::extraction::{build_extraction_rows, ExtractionItem};

    let conn = open_in_memory_for_test();
    let src = tempfile::tempdir().unwrap();
    let arch = tempfile::tempdir().unwrap();
    let src_path = src.path().to_str().unwrap().to_string();
    let arch_path = arch.path().to_str().unwrap().to_string();
    let root_id = insert_test_root(&conn, &src_path, "source", false);
    let archive_id = insert_test_root(&conn, &arch_path, "archive", false);
    let apply_id = insert_decision(&conn, "apply", 200);

    let chosen: Vec<String> = (1..=5).map(|n| format!("photos/p{n}.jpg")).collect();
    let mut objects = Vec::new();
    for (n, rel) in chosen.iter().enumerate() {
        let object = insert_object(&conn, &format!("photo{n}"));
        objects.push(object);
        insert_source(
            &conn,
            archive_id,
            rel,
            Some(object),
            true,
            false,
            Some(apply_id),
        );
        if !shape.moved {
            insert_source(&conn, root_id, rel, Some(object), true, false, None);
        }
    }
    if shape.copies {
        for (n, object) in objects.iter().enumerate() {
            let rel = format!("backup/p{}.jpg", n + 1);
            insert_source(&conn, root_id, &rel, Some(*object), true, false, None);
        }
        for (n, object) in objects.iter().take(2).enumerate() {
            let rel = format!("docs/p{}.jpg", n + 1);
            insert_source(&conn, root_id, &rel, Some(*object), true, false, None);
        }
    }

    let items: Vec<ExtractionItem> = chosen
        .iter()
        .map(|rel| ExtractionItem {
            source_root: &src_path,
            source_rel_path: rel,
            destination_rel_path: rel,
            size: 1000,
        })
        .collect();
    let disposition = if shape.moved {
        OriginDisposition::Relocated
    } else {
        OriginDisposition::Retained
    };
    let roots = repo::root::fetch_all(&conn).unwrap();
    let (rows, unknown) = build_extraction_rows(
        &items,
        &roots,
        (Some(archive_id), &arch_path),
        Some(disposition),
        apply_id,
    );
    assert!(unknown.is_empty());
    repo::decision::replace_extractions(&conn, &rows).unwrap();

    if shape.receipt {
        let receipt_rel = format!(".canon-ledger/{apply_id:06}-apply.toml");
        std::fs::create_dir_all(arch.path().join(".canon-ledger")).unwrap();
        let mut body = format!(
            "[meta]\ndecision_id = {apply_id}\norigin_disposition = \"{}\"\n\n\
             [meta.locus]\npath = \"{arch_path}\"\nid = {archive_id}\n",
            disposition.as_str()
        );
        for (n, rel) in chosen.iter().enumerate() {
            body.push_str(&format!(
                "\n[[items]]\nsource_root = \"{src_path}\"\nsource_rel_path = \"{rel}\"\n\
                 destination_rel_path = \"{rel}\"\nsize = 1000\nhash = \"sha256:photo{n}\"\n"
            ));
        }
        std::fs::write(arch.path().join(&receipt_rel), body).unwrap();
        set_decision_receipt(&conn, apply_id, archive_id, &receipt_rel);
    }

    Copies {
        conn,
        src,
        arch,
        root_id,
        apply_id,
    }
}

/// A root exercising every fate at once: covered-enriched (archived from
/// here), covered plain, excluded, unresolved hashed + unhashed, deleted,
/// unexplained, and a receipt-recovered moved entry.
pub(super) fn every_fate_fixture() -> (Connection, tempfile::TempDir, tempfile::TempDir, i64) {
    let conn = open_in_memory_for_test();
    let src_dir = tempfile::tempdir().unwrap();
    let arch_dir = tempfile::tempdir().unwrap();
    let src_path = src_dir.path().to_str().unwrap().to_string();
    let arch_path = arch_dir.path().to_str().unwrap().to_string();
    let root_id = insert_test_root(&conn, &src_path, "source", false);
    let archive_id = insert_test_root(&conn, &arch_path, "archive", false);

    let copied = insert_object(&conn, "copiedhash");
    let plain = insert_object(&conn, "plainhash");
    let uncovered = insert_object(&conn, "uncoveredhash");
    let moved = insert_object(&conn, "movedhash");
    insert_source(
        &conn,
        archive_id,
        "a/copied.jpg",
        Some(copied),
        true,
        false,
        None,
    );
    insert_source(
        &conn,
        archive_id,
        "a/plain.jpg",
        Some(plain),
        true,
        false,
        None,
    );
    insert_source(
        &conn,
        archive_id,
        "a/moved.jpg",
        Some(moved),
        true,
        false,
        None,
    );

    insert_source(
        &conn,
        root_id,
        "copied.jpg",
        Some(copied),
        true,
        false,
        None,
    );
    insert_source(&conn, root_id, "plain.jpg", Some(plain), true, false, None);
    let exclude = insert_decision(&conn, "exclude_set", 400);
    set_decision_extras(&conn, exclude, None, Some("duplicate"));
    insert_source(&conn, root_id, "junk.jpg", None, true, true, Some(exclude));
    insert_source(
        &conn,
        root_id,
        "loose.jpg",
        Some(uncovered),
        true,
        false,
        None,
    );
    insert_source(&conn, root_id, "unhashed.jpg", None, true, false, None);
    let scan = insert_decision(&conn, "scan", 500);
    insert_source(
        &conn,
        root_id,
        "deleted.jpg",
        None,
        false,
        false,
        Some(scan),
    );
    insert_source(&conn, root_id, "vanished.jpg", None, false, false, None);

    let apply = insert_decision(&conn, "apply", 600);
    extraction_from(&conn, apply, root_id, 2);
    let receipt_rel = ".canon-ledger/000001-apply.toml";
    std::fs::create_dir_all(arch_dir.path().join(".canon-ledger")).unwrap();
    std::fs::write(
        arch_dir.path().join(receipt_rel),
        format!(
            r#"
[meta]
decision_id = {apply}
origin_disposition = "relocated"

[meta.locus]
path = "{arch_path}"
id = {archive_id}

[[items]]
source_root = "{src_path}"
source_rel_path = "copied.jpg"
destination_rel_path = "a/copied.jpg"
size = 1000
hash = "sha256:copiedhash"
mtime = 0

[[items]]
source_root = "{src_path}"
source_rel_path = "gone/moved.jpg"
destination_rel_path = "a/moved.jpg"
size = 555
hash = "sha256:movedhash"
mtime = 1700000000
"#
        ),
    )
    .unwrap();
    set_decision_receipt(&conn, apply, archive_id, receipt_rel);

    (conn, src_dir, arch_dir, root_id)
}

pub(super) const CEREMONY_NOW: i64 = 1_753_000_000;

fn config_with(recording: RecordingMode) -> LedgerConfig {
    LedgerConfig {
        recording,
        ..LedgerConfig::default()
    }
}

pub(super) fn begin_with(
    conn: &Connection,
    root_id: i64,
    recording: RecordingMode,
) -> RetireCeremony {
    begin_in(
        conn,
        root_id,
        recording,
        UserZone::utc(),
        ReadTimesOn::of_user(&UserZone::utc()),
    )
}

/// [`begin_with`] with the binder's clock and the clock the records state
/// named separately — the interface builds both from one zone.
pub(super) fn begin_in(
    conn: &Connection,
    root_id: i64,
    recording: RecordingMode,
    zone: UserZone,
    read_times_on: ReadTimesOn,
) -> RetireCeremony {
    let story = fetch_root_story(conn, root_id).unwrap();
    let review = readiness_lens(&story);
    let config = config_with(recording);
    let plan = plan_bind(&story, &config, CEREMONY_NOW, &zone).unwrap();
    begin_ceremony(
        conn,
        story,
        &review,
        plan,
        CeremonyParams {
            reason: Some("story complete".to_string()),
            now: CEREMONY_NOW,
            zone,
            command_line: "canon roots retire".to_string(),
            config,
            read_times_on,
        },
    )
}
