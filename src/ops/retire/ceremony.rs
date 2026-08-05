//! The ceremony: plan, begin, bind, release — ops owns the ordering.

use super::*;

use crate::domain::story::StoryParams;
use crate::ops::telling::TellingArtifact;

// ---------------------------------------------------------------------------
// The ceremony — plan, begin, bind
// ---------------------------------------------------------------------------

/// Where the book will land — computed before any confirmation, so the
/// interface can state a replacement as awareness, never surprise.
#[derive(Debug)]
pub struct BindPlan {
    pub shelf: PathBuf,
    /// `retired/<name>` — the book's final home.
    pub final_dir: PathBuf,
    /// `retired/.compiling-<name>` — the compile target. Inside the shelf so
    /// the placement rename never crosses a filesystem boundary (atomic).
    pub temp_dir: PathBuf,
    pub ledger_root_id: i64,
    /// A same-root book already stands at `final_dir` (a prior
    /// aborted-after-bind run) — this run replaces it with a fresh compile.
    pub replaces_existing: bool,
    /// `false` → first use: the shelf directory and its README are generated.
    pub shelf_exists: bool,
}

/// Resolve the shelf and the book's name. Collision handling is keyed on the
/// standing book's own `meta.toml` identity — deliberately disk-keyed, never
/// a decision-row lookup, so convergence survives any recording-mode history:
/// the same root's book is replaced (a re-run converges); a different root's
/// book pushes this one to a `-2`/`-3`… sibling; a directory that cannot be
/// identified refuses the ceremony.
pub fn plan_bind(story: &RootStory, config: &LedgerConfig, now: i64) -> Result<BindPlan> {
    let (ledger_root_id, ledger_root_path) =
        ops::receipt::resolve_ledger_root(&story.roots, config).ok_or_else(|| {
            anyhow::anyhow!(
            "Retirement needs an archive root to hold the record — no archive root is registered"
        )
        })?;
    let shelf = PathBuf::from(&ledger_root_path).join(SHELF_DIR);
    let base = book_dir_name(&story.root.path, &iso_date(now));

    let mut chosen: Option<(String, bool)> = None;
    for attempt in 1u32..=99 {
        let name = if attempt == 1 {
            base.clone()
        } else {
            format!("{base}-{attempt}")
        };
        let candidate = shelf.join(&name);
        if !candidate.exists() {
            chosen = Some((name, false));
            break;
        }
        let standing_root = read_book_root_path(&candidate)?;
        if standing_root == story.root.path {
            chosen = Some((name, true));
            break;
        }
    }
    let Some((name, replaces_existing)) = chosen else {
        bail!(
            "No free book name beside {} after 99 attempts — the shelf needs attention",
            shelf.join(&base).display()
        );
    };

    Ok(BindPlan {
        final_dir: shelf.join(&name),
        temp_dir: shelf.join(format!(".compiling-{name}")),
        shelf_exists: shelf.is_dir(),
        shelf,
        ledger_root_id,
        replaces_existing,
    })
}

/// The identity half of a standing book's `meta.toml` — read only to answer
/// "whose book is this?" during collision planning. Deliberately separate
/// from `verify_book`'s `MetaDoc`: different question, different tolerance.
#[derive(Deserialize)]
struct IdentityProbe {
    identity: IdentityProbePath,
}

#[derive(Deserialize)]
struct IdentityProbePath {
    path: String,
}

fn read_book_root_path(dir: &Path) -> Result<String> {
    let refusal = || {
        format!(
            "A directory stands at {} but cannot be identified as a book — refusing to replace what cannot be identified; move it aside and re-run",
            dir.display()
        )
    };
    let raw = std::fs::read_to_string(dir.join("meta.toml")).with_context(refusal)?;
    let probe: IdentityProbe = toml::from_str(&raw).with_context(refusal)?;
    Ok(probe.identity.path)
}

/// The ceremony's parameters, held for the life of one invocation — the
/// config is read once, so recording modes cannot change mid-ceremony.
pub struct CeremonyParams {
    /// The user's `--reason`: bound onto the book and the decision row.
    pub reason: Option<String>,
    pub now: i64,
    pub command_line: String,
    pub config: LedgerConfig,
}

/// The ceremony across its movements: one decision spanning bind and
/// release, with the interface's confirmations between them. Ops owns the
/// ceremony policy (ordering, recording, verification gating); the interface
/// owns the prompts and the printing.
pub struct RetireCeremony {
    // Fields are pub(super) for the retire module's own tests only —
    // outside `ops/retire/` the ceremony's internals stay sealed.
    pub(super) story: RootStory,
    pub(super) plan: BindPlan,
    pub(super) recorder: DecisionRecorder,
    reason: Option<String>,
    now: i64,
    /// Review-time basis for the release movement's world-moved re-check.
    snapshot_source_count: i64,
    snapshot_max_decision_id: Option<i64>,
}

/// Start the ceremony's two-phase decision (status: `started` — an
/// interrupted ceremony is findable from here on). Called after the first
/// confirmation; a declined prompt records nothing.
pub fn begin_ceremony(
    conn: &Connection,
    story: RootStory,
    review: &ReadinessReview,
    plan: BindPlan,
    params: CeremonyParams,
) -> RetireCeremony {
    let decision = DecisionParams {
        command: DecisionCommand::RootsRetire,
        scope: vec![DecisionScope::new(
            story.root.id,
            story.root.path.clone(),
            String::new(),
        )],
        command_line: params.command_line,
        reason: params.reason.clone().filter(|r| !r.trim().is_empty()),
        record_enabled: params.config.recording != RecordingMode::Off,
        // The book is the decision's artifact — retire never writes a receipt
        // file. The pointer columns are recorded separately at bind, gated on
        // recording alone.
        receipt_enabled: false,
        ledger_config: params.config,
    };
    let recorder = DecisionRecorder::start(conn, &decision, None);

    RetireCeremony {
        story,
        plan,
        recorder,
        reason: params.reason,
        now: params.now,
        snapshot_source_count: review.snapshot_source_count,
        snapshot_max_decision_id: review.snapshot_max_decision_id,
    }
}

/// The placed, verified book — what the bind movement hands the interface
/// for the inspection window.
#[derive(Debug)]
pub struct BoundBook {
    pub dir: PathBuf,
    pub entry_count: i64,
    pub gaps: Vec<String>,
    pub ledger_files: Option<usize>,
    pub replaced_previous: bool,
    pub warnings: Vec<String>,
}

impl RetireCeremony {
    /// Compose the reference-voiced telling over the ceremony's own story —
    /// one fetch: the gate, the book, and the telling read the same world
    /// by construction. The interface offers this draft to the user's
    /// editor before binding; the finalized text comes back through `bind`.
    pub fn compose_telling(&self, conn: &Connection) -> Result<String> {
        let params = StoryParams::default();
        let report = ops::story::report_over(conn, &self.story, &params)?;
        // Where the chosen content lives now: the recorded destinations,
        // aggregated the same way every "where" line is.
        let dirs: Vec<(&str, i64)> = self
            .story
            .extractions
            .iter()
            .map(|e| (e.destination_path.as_str(), e.files))
            .collect();
        let bases: Vec<&str> = self
            .story
            .roots
            .iter()
            .filter(|r| r.role == "archive")
            .map(|r| r.path.as_str())
            .collect();
        // What stands in the drive's own ledger — the same discriminant and
        // source the bind's gather reads, so the paragraph never claims
        // receipts the gather won't find.
        let drive_ledger = if self.story.reachable {
            Some(super::compile::count_files(
                &Path::new(&self.story.root.path).join(".canon-ledger"),
            )?)
        } else {
            None
        };
        let frame = ops::telling::TellingFrame {
            title: ops::telling::suggested_title(
                &self.story.root.path,
                self.story.root.comment.as_deref(),
            ),
            retirement_reason: self.reason.clone(),
            bound_on: self.now,
            canon_version: env!("CARGO_PKG_VERSION").to_string(),
            archived_destinations: crate::domain::story::aggregate_locations(
                &dirs,
                &bases,
                params.where_cap,
            ),
            drive_ledger,
        };
        Ok(ops::telling::compose_reference_telling(&report, &frame))
    }

    /// The bind movement: shelf, compile to temp, verify, place, pointer.
    ///
    /// Verify-before-touch is the load-bearing order: the book is verified at
    /// its temp name, so a standing book is never touched until the fresh one
    /// is proven whole — and the placement rename is what commits a book, so
    /// anything still at a temp name was structurally never placed.
    ///
    /// The telling is a required parameter: a ceremony structurally cannot
    /// bind a dossier.
    pub fn bind(&mut self, conn: &Connection, telling: TellingArtifact) -> Result<BoundBook> {
        if !self.plan.shelf_exists {
            std::fs::create_dir_all(&self.plan.shelf).with_context(|| {
                format!(
                    "Could not create the shelf at {}",
                    self.plan.shelf.display()
                )
            })?;
        }
        write_shelf_readme(&self.plan.shelf)?;

        // A leftover temp dir is a compile that was never placed — removable
        // without ceremony.
        if self.plan.temp_dir.exists() {
            std::fs::remove_dir_all(&self.plan.temp_dir).with_context(|| {
                format!(
                    "Could not clear the leftover compile at {}",
                    self.plan.temp_dir.display()
                )
            })?;
        }

        let compiled = compile_book(
            conn,
            &self.story,
            &CompileParams {
                reason: self.reason.clone(),
                now: self.now,
                dest_dir: self.plan.temp_dir.clone(),
                ceremony_decision_id: self.recorder.decision_id(),
                telling: Some(telling),
            },
        )?;
        verify_book(&self.plan.temp_dir).with_context(|| {
            format!(
                "The compiled book failed verification — nothing was placed; the compile is kept for inspection at {}",
                self.plan.temp_dir.display()
            )
        })?;

        place_book(&self.plan)?;

        let name = self
            .plan
            .final_dir
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        self.recorder.record_artifact_pointer(
            conn,
            self.plan.ledger_root_id,
            &format!("{SHELF_DIR}/{name}"),
        );

        Ok(BoundBook {
            dir: self.plan.final_dir.clone(),
            entry_count: compiled.entry_count,
            gaps: compiled.gaps,
            ledger_files: compiled.ledger_files,
            replaced_previous: self.plan.replaces_existing,
            warnings: self.recorder.take_warnings(),
        })
    }

    /// The release movement: one `BEGIN IMMEDIATE` transaction holding the
    /// world-moved re-check, the removal, and the decision's completion — the
    /// re-check's reads must be authoritative against concurrent writers, and
    /// the transaction is short.
    pub fn release(&mut self, conn: &Connection) -> Result<ReleaseOutcome> {
        let book_display = self.plan.final_dir.display().to_string();
        let tx = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)?;

        if let Some(detail) = self.world_moved(&tx)? {
            // Nothing was written — dropping the transaction rolls back.
            drop(tx);
            let summary = format!(
                "The book is bound at {book_display}; the world moved before release ({detail}) — the root remains in the index"
            );
            self.recorder.complete_db(
                conn,
                DecisionStatus::Partial,
                DecisionCounts {
                    attempted: Some(self.snapshot_source_count),
                    completed: None,
                    failed: None,
                    skipped: None,
                },
                &summary,
            );
            return Ok(ReleaseOutcome::WorldMoved {
                detail,
                warnings: self.recorder.take_warnings(),
            });
        }

        let removed = ops::roots::remove_root_data(&tx, self.story.root.id)?;
        let summary = format!(
            "Retired {}: {} sources released; the story is bound at {book_display}",
            self.story.root.path,
            format_count(removed.deleted_sources),
        );
        self.recorder.complete_db(
            &tx,
            DecisionStatus::Completed,
            DecisionCounts {
                attempted: Some(self.snapshot_source_count),
                completed: Some(removed.deleted_sources),
                failed: None,
                skipped: None,
            },
            &summary,
        );
        tx.commit()?;

        Ok(ReleaseOutcome::Released {
            deleted_sources: removed.deleted_sources,
            deleted_notes: removed.deleted_notes,
            summary,
            warnings: self.recorder.take_warnings(),
        })
    }

    /// Has the world moved since the review? Two cheap aggregates against the
    /// review-time snapshots — computed over SQL exactly as `readiness_lens`
    /// derived them from the fetched rows, so equality means "same world".
    /// The ceremony's own decision is excluded: its recording must not read
    /// as the world moving (a concurrent process's decision has a different
    /// id and correctly trips the check).
    fn world_moved(&self, conn: &Connection) -> Result<Option<String>> {
        let count = repo::source::count_all_by_root(conn, self.story.root.id)?;
        if count != self.snapshot_source_count {
            return Ok(Some(format!(
                "the root held {} source rows at review, {} now",
                format_count(self.snapshot_source_count),
                format_count(count)
            )));
        }
        let max = repo::decision::max_decision_id_touching_root(
            conn,
            self.story.root.id,
            self.recorder.decision_id(),
        )?;
        if max != self.snapshot_max_decision_id {
            return Ok(Some(
                "another decision touched this root since the review".to_string(),
            ));
        }
        Ok(None)
    }

    /// The declined second confirmation: the book stands, the root stays.
    /// The decision completes `partial` — a findable state the rm guard and
    /// a re-run both read (the re-run converges by replacing the book).
    pub fn abandon(&mut self, conn: &Connection) -> AbandonResult {
        let summary = format!(
            "The book is bound at {}; the root remains in the index (release declined)",
            self.plan.final_dir.display()
        );
        self.recorder.complete_db(
            conn,
            DecisionStatus::Partial,
            DecisionCounts {
                attempted: Some(self.snapshot_source_count),
                completed: None,
                failed: None,
                skipped: None,
            },
            &summary,
        );
        AbandonResult {
            summary,
            warnings: self.recorder.take_warnings(),
        }
    }

    /// Record a failed bind as `interrupted` — fix-forward: the failure is
    /// findable in the trail, the root untouched. The error itself is the
    /// caller's to propagate; this only records. Returns drained warnings.
    pub fn interrupt(&mut self, conn: &Connection, error: &str) -> Vec<String> {
        let summary = format!("Retirement interrupted during bind: {error}");
        self.recorder.complete_db(
            conn,
            DecisionStatus::Interrupted,
            DecisionCounts {
                attempted: Some(self.snapshot_source_count),
                completed: None,
                failed: None,
                skipped: None,
            },
            &summary,
        );
        self.recorder.take_warnings()
    }
}

/// The release movement's outcome. `WorldMoved` is a ceremony outcome, not a
/// program error: the ceremony stops (root intact, book standing, decision
/// `partial`) and asks to be re-run.
pub enum ReleaseOutcome {
    Released {
        /// Counts carried per the typed-result convention; the summary is
        /// the composed narration the interface prints.
        #[allow(dead_code)]
        deleted_sources: i64,
        #[allow(dead_code)]
        deleted_notes: usize,
        summary: String,
        warnings: Vec<String>,
    },
    WorldMoved {
        detail: String,
        warnings: Vec<String>,
    },
}

pub struct AbandonResult {
    pub summary: String,
    pub warnings: Vec<String>,
}

/// Commit the verified temp compile to its final name. In the replacement
/// case the standing book steps aside first, so there is never a moment with
/// a partial book at the final name — the only non-atomic instant has both
/// the old book (aside) and the new one (placed) present.
fn place_book(plan: &BindPlan) -> Result<()> {
    if !plan.replaces_existing {
        return std::fs::rename(&plan.temp_dir, &plan.final_dir)
            .with_context(|| format!("Could not place the book at {}", plan.final_dir.display()));
    }

    let name = plan
        .final_dir
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let aside = plan.shelf.join(format!(".replaced-{name}"));
    // A leftover aside dir is a book a prior swap already replaced —
    // removable, same standing as a leftover temp.
    if aside.exists() {
        std::fs::remove_dir_all(&aside)?;
    }
    std::fs::rename(&plan.final_dir, &aside).with_context(|| {
        format!(
            "Could not move the standing book aside at {}",
            plan.final_dir.display()
        )
    })?;
    std::fs::rename(&plan.temp_dir, &plan.final_dir).with_context(|| {
        format!(
            "Could not place the book at {} (the previous book is at {})",
            plan.final_dir.display(),
            aside.display()
        )
    })?;
    std::fs::remove_dir_all(&aside)
        .with_context(|| format!("Could not remove the replaced book at {}", aside.display()))?;
    Ok(())
}

/// The shelf explains itself once; the README is never rewritten.
fn write_shelf_readme(shelf: &Path) -> Result<()> {
    let path = shelf.join("README.md");
    if path.exists() {
        return Ok(());
    }
    let text = "\
# The Shelf

This directory holds the books of retired roots — drives and folders whose
complete story was compiled by `canon roots retire` before their index was
removed.

Each book is a plain directory: open its `README.md` and start reading. Books
are self-contained — inventory, decisions, notes, and receipts in plain,
stable formats — and are meant to outlive Canon itself. No database and no
tool is needed to read them.

Keep this directory with your archive. Deleting a book deletes the only
reviewable story of a root that is already gone.
";
    std::fs::write(&path, text)
        .with_context(|| format!("Could not write the shelf README at {}", path.display()))
}
