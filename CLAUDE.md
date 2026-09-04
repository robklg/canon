# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

> **What belongs in this file.** Every sentence is one of three things: a **map fact**,
> where a thing lives and what it is; a **rule**, what must hold, the one place it is spoken,
> and where it does not apply; or a **why**, one sentence shaped as the failure the rule
> prevents. A rule may name its owner and one verifying test, never a list. Nothing else
> belongs: no dates, no record of who decided a thing, no superseded claims kept as history,
> no bare counts, no private paths or record filenames. The test for a doubtful sentence is
> the reader's: a session that has never seen how this project is run acts the same without
> it, so it goes. The provenance that leaves lives in the commit that introduced the sentence.
>
> It is a map, not an inventory: to find a specific function, grep its module.

## Build Commands

```bash
cargo build          # Build the project
cargo run -- <cmd>   # Run with subcommand
cargo test           # Run all tests
cargo clippy         # Run linter
cargo fmt            # Format code
```

## Project Overview

Canon is a CLI tool for organizing large media libraries into a "canonical archive". It manages files scattered across multiple backups and devices, handling duplicates and providing structured organization.

### Core Concepts

- **Root**: A scanned folder, identified by canonical path. Has a role: `source` or `archive`.
- **Source**: A file on disk (root + relative path) — one source per path, so hardlink twins are ordinary sources sharing one object. Path is the identity axis and content evidence is the arbiter; **device and inode are a nomination hint, never identity** (the physical-identity law, `scan/domain.rs::same_physical_file`). A relocating claim additionally needs disk truth — old-path absence verified against the storage the row was observed on, device serving as mount-presence evidence — and a write-time re-check.
- **Object**: Content identified by hash (sha256). Multiple sources can map to one object.
- **Facts**: Arbitrary key-value metadata (EAV model). Source facts are tied to a file path; object facts to a content hash.

### Domain Language

**Use the ubiquitous language.** Canon's domain vocabulary — imported below from its registry — is
the language of this project: use its terms as defined, in code, docs and discussion. Don't coin or
redefine domain terms outside the registry; if new work seems to need a new term, say so in the
record rather than minting it in passing.

@docs/LANGUAGE.md

## Architecture at a Glance

A **strict layered architecture** in a **feature-first tree** (the Feature-First Structure ADR).
The subsystem is the unit of locality: each is a top-level directory holding its own
`domain`/`repo`/`ops`/`cli` strata, sealed behind a pinned barrel in its front-door file (a flat
`src/<name>.rs` for most, the subsystem's own `mod.rs` for `retire` and `story`). `core` is the
exception at the other end: the shared spine, not a subsystem, and deliberately unsealed. The
four-layer law below is unchanged by that shape and applies inside every subsystem.

**How to read the map**: the spine and the facility first, then the subsystems, then what stays
flat at the crate root. The subsystem block is in no meaningful order — read it as a list, not a
sequence. Each bullet names strata and what the barrel seals; the *why* of a subsystem lives in
its own nested CLAUDE.md.

- **`core/`** — The cross-cutting spine of the feature-first tree. Two things live here side by side. **Shared substrate** multiple subsystems independently compute over, never a subsystem's finished output: `domain::resolution` (the two-register resolution account: `ResolutionAccount`, `classify_present`/`classify_absent`, `build_account`), `domain::folder_tree` (`FolderTree` — interned folder-tree topology, shared by the boundary walks; only the skeleton is shared, never the emission rules), `ops::root_story` (`RootStory`, `fetch_root_story` — the one structural fetch of a root's complete world-state). Alongside them the **provenance spine**: `ops::receipt` — receipt placement, path computation, the derived `[meta]` table, and the generic writer that puts any body on disk — and `ops::decision`, the two-phase recorder that brackets every effectful action. With them the two access layers every subsystem reaches through: `ops::scope`, the one scope-resolution pipeline (CWD defaulting, `--global`, root membership, source existence), and `ops::fs`, structured access to files on disk — the second data plane, parallel to `repo`. Beside them, the **pure domain layer** — the fundamental nouns every subsystem speaks: structs + predicate functions, no I/O, unit-testable. And the **shared repository layer** — the database access more than one subsystem needs: SQL lives here, returns domain types, batch operations; a subsystem's own exclusive SQL lives in its own `repo` stratum instead. Beside them, **`testing`** — the shared fixtures every stratum's tests build a database with, `cfg(test)`-only. See `src/core/CLAUDE.md`. Modules: `domain/source`, `domain/root`, `domain/object`, `domain/fact`, `domain/config`, `domain/decision`, `domain/format`, `domain/scope`, `domain/path`, `domain/include`, `domain/resolution`, `domain/fate`, `domain/extraction`, `domain/folder_tree`, `ops/root_story`, `ops/receipt`, `ops/decision`, `ops/scope`, `ops/fs`, `ops/ledger`, `repo/db`, `repo/source`, `repo/root`, `repo/object`, `repo/decision`, `repo/fact`, `testing/helpers`.
- **`expr/`** — The expression facility: the language's one home, and the only unit that is a facility rather than a command — nothing here owns a CLI surface, and every subsystem that takes a **selection** consumes it (`archive`, `compare`, `coverage`, `exclude`, `facts`, `ls`, `survey`, `worklist`); the rest take a scope or a root instead and never speak the language. Its barrel is recorded in three groups: the items consumers name, the parameter types that complete their surface, and the one item reached past the language. See `src/expr/CLAUDE.md`. Modules: `domain` (the language as pure logic — `domain/key` the key vocabulary and its parsing, `domain/transform` accessors/modifiers, `domain/pattern` pattern parse+evaluate, `domain/vantage` the place a scope-relative path measures from, `domain/filter` the `--where` grammar, AST and comparisons, `domain/cache` the prefetched fact cache, `domain/value` fact resolution, `domain/alias` alias rewriting), `ops` (the language applied — `ops/filter` filtering run against a database, `ops/pattern` the shaping half's prefetch, where the context-supplied law is applied, `ops/selection` the shared selector every command resolves through, `ops/alias` the aliases file), `repo` (the facility's SQL — two prefetches that fill the cache, two point reads).
- **`ls/`** — The ls instrument: `cli.rs` (`canon ls`, both display modes), `ops.rs` (duplicate grouping by content identity), `repo.rs` (the instrument's own SQL — the one batched read behind the duplicates mode). Sealed behind a barrel of `run` and `show_duplicates`. See `src/ls/CLAUDE.md`.
- **`worklist/`** — The worklist instrument: `cli.rs` (`canon worklist`), `ops.rs` (the JSONL entry build). Sealed behind a barrel of `run`. See `src/worklist/CLAUDE.md`.
- **`coverage/`** — The coverage instrument: `cli.rs` (`canon coverage`), `ops.rs` (the scoped and per-root computations, and archive-root resolution). Sealed behind a barrel of `run` plus `compute_per_root` for the contentless canary. See `src/coverage/CLAUDE.md`.
- **`compare/`** — The compare instrument: `cli.rs` (`canon compare`), `ops.rs` (the comparison itself, which builds its own object maps rather than routing through the shared index — a law site in its own right). Sealed behind a barrel of `run` and `CompareOptions`, plus `run_compare` for the contentless canary. See `src/compare/CLAUDE.md`.
- **`retire/`** — The retirement subsystem: `domain/` (the readiness verdict, the book's fate model), `ops/` (the readiness review, the ceremony, the book compile, the shelf listing, the bound telling's frame), `cli.rs` (`canon roots retire`/`retired`). See `src/retire/CLAUDE.md`.
- **`story/`** — The story subsystem: `domain/` (the place splitter, act grouping, location aggregation), `ops/` (the compute op — the third lens over `RootStory`; the place-walk renderer, both voicings), `cli.rs` (`canon roots story`). See `src/story/CLAUDE.md`.
- **`trail/`** — The trail subsystem: `domain/` (the timeline merge, the two-claims placement law, the crossings selection rule, the composition card, the decision-id shape predicate), `ops/` (`compute`/`show`/`place`/`crossings` — the scope/time-lens reads, the composition-card fetch, the evidence gate, and the counterpart door's computation), `repo/` (trail's own SQL), `cli.rs`/`render.rs`/`jsonl.rs` (`canon trail`/`trail show`/`trail crossings`; `cli.rs` parses and dispatches, the other two are the output voices). See `src/trail/CLAUDE.md`.
- **`sweep/`** — The sweep subsystem: `domain/` (the structural computation — five pipeline stages, `universe`/`weights`/`discovery`/`localization`/`assembly` — plus the reduction lens), `ops.rs` (the sweep's compute op — owns its fetch), `cli.rs` (`canon sweep`). Sealed behind a barrel of `run`. See `src/sweep/CLAUDE.md`.
- **`survey/`** — The survey subsystem: `domain/` (the analysis lens — scope discovery, only-here/uniqueness counting, location classification — plus the in-memory object index that grounds content comparisons in objects, `object_id → sources`), `ops/` (`compute`/`orchestrate` — the pure computation and the fetch/resolution wiring around it), `cli.rs`/`render.rs` (`canon survey`; CLI-shape validation and orchestration wiring, and the presentation functions). See `src/survey/CLAUDE.md`.
- **`exclude/`** — The exclude subsystem: `domain.rs` (duplicate detection), `repo.rs` (the exclusion-transition SQL plus `ObjectReceiptSource`, nested `source`/`object` modules), `ops.rs` + `ops/` (six strata — `types`/`plan`/`receipt`/`runner`/`execute`/`single` — plan/execute pairs around `run_exclusion`, the subsystem's one transaction site), `cli.rs` (`canon exclude set/clear/duplicates/set-object/clear-object`). See `src/exclude/CLAUDE.md`.
- **`scan/`** — The scan subsystem: `domain.rs` (the physical-identity law and the two reconciliation arms that consume it, deterministic move pairing, the roots-never-nest overlap check), `repo.rs` (the scan-transition SQL plus `ReceiptSource`, nested `source`/`root`/`fact` modules), `ops/` (five strata — `types`/`pipeline`/`receipt`/`candidates`/`hash` — the walk pipeline, deletion-receipt writing, root-candidate discovery, hashing), `cli.rs` (`canon scan`). Sealed behind a barrel of `run` and `find_candidates`, plus `same_physical_file` — the physical-identity law — and its parameter types `IdentityClaim`/`FileObservation`, the three serving the contentless canary. See `src/scan/CLAUDE.md`.
- **`roots/`** — The roots subsystem: no domain stratum (roots' logic lives on the shared `Root`), `repo.rs` (the roots-exclusive SQL, nested `root`/`note` modules), `ops.rs` (plan/execute for removal, suspend/unsuspend flips), `cli.rs` (`canon roots list/rm/comment/suspend/unsuspend`). The barrel adds `plan_remove`/`remove_root_data` to the commands, reached by retirement's sibling edge. See `src/roots/CLAUDE.md`.
- **`notes/`** — The notes subsystem: `domain.rs` (the note struct, the descendant-contract path logic), `repo.rs` (notes' SQL, reached directly by sibling subsystems), `ops.rs` (view/list/clear behaviors, the survey context), `cli.rs` (`canon note`). See `src/notes/CLAUDE.md`.
- **`facts/`** — The facts subsystem, carved at item grain: `domain.rs` (the import-vocabulary half of the fact noun — `FactValueType`/`SourceFact`/`normalize_fact_key`/`is_content_fact`; `FactEntry` and the noun's rump stay in `core/domain/fact.rs`), `repo.rs` (the facts-exclusive SQL; `batch_fetch_for_sources`/`batch_fetch_key_for_sources` stay in `core/repo/fact.rs`), `ops/report.rs` + `ops/maintain.rs` (distribution/enumeration reads vs. delete/prune plan-execute pairs), `ops/import.rs` (`canon import-facts`'s per-record logic), `cli.rs` + `cli/import.rs` (the two commands, kept as separate files). Its barrel is entirely `main.rs`-facing. A leaf subsystem: zero sibling reach either direction. See `src/facts/CLAUDE.md`.
- **`archive/`** — The archive subsystem: `domain.rs` (the manifest format — pure, no I/O), `ops/` (seven strata — `plan`/`execute` for apply, `generate`/`status` for cluster, over the shared `pattern` and `manifest`, plus `receipt` for apply's own document shapes; the internal edges form a DAG, `execute → plan → pattern`, `execute → receipt`, and `generate`/`status` → `manifest`), `repo.rs` (the SQL only apply performs), `cli/` (`canon apply`, `canon cluster generate/refresh/status`, kept as separate files). See `src/archive/CLAUDE.md`.
- **The crate root** (flat `src/*.rs`) — three kinds of file, and nothing else. **Front doors**: every subsystem but `retire` and `story` opens through a flat file here (`archive.rs`, `compare.rs`, …), holding only private `mod` declarations and the barrel's `pub use` list — no logic; those two put the identical barrel in their own `mod.rs` instead, differing in file placement only. **`core` is not a subsystem and has no barrel** — `core/mod.rs` declares its strata `pub mod`, deliberately: the spine's job is to be reachable, so it is exempt from the seal and carries no pin row. **Entry and utilities**: `main.rs` (entry/clap, canon-home resolution, alias dispatch), `ceremony.rs` (`confirm()`, `ask()`, and `edit_in_editor()` — the last is what retirement's editor offer runs on), `scope.rs` (scope display), `progress.rs`. **One flat command**: `ledger.rs` (`canon ledger reindex`) — the only command still parsing, computing and formatting in one file at this level. (`contentless_law_tests.rs` also sits here — the canary belongs to no single subsystem because it reads across all of them.)

### Where the subsystem conventions live

Every directory under `src/` carries a nested CLAUDE.md holding the rules that govern it. This
table is the index for the ones carrying substantial bodies of law lifted out of this file;
follow it before changing behavior in any of these areas. It is not a partition — a rule that
several subsystems depend on stays *here*, as cross-cutting law, and this file still carries
surface-level conventions for commands whose depth lives elsewhere.

| Look for | In |
|---|---|
| What `core/` may and may not hold, and why each spine member earns its place | `src/core/CLAUDE.md` |
| The generate/apply workflow, the manifest format and version gate, the comment sections and their split parser contract, apply's non-transactional recovery model | `src/archive/CLAUDE.md` |
| The observe/reconcile/persist stages, the outcome/DB-action table, the mount-stability deletion-detection guard | `src/scan/CLAUDE.md` |
| The trail's timeline merge, the two-claims placement law, the composition card, both op families | `src/trail/CLAUDE.md` — the fate-vocabulary law and the extraction *round-trip* law live in `src/core/CLAUDE.md`, which core consumes independently of trail; the extraction ledger's **row-claim** laws are below, under Extraction ledger conventions, being cross-cutting |
| The sweep's lens separation law, the disjointness invariant, the floor two-walk merge | `src/sweep/CLAUDE.md` |
| Survey's two modes, the detail views, the asymmetric-visibility designed deviation, the in-memory object index | `src/survey/CLAUDE.md` |
| The retirement readiness gate, book compile, ceremony orchestration, shelf listing, and the bound telling's *frame* (`story.md`'s structure around the place walk) | `src/retire/CLAUDE.md` |
| The place splitter, act grouping, location aggregation, and the place-walk renderer — the bound telling's *walk and voicing* half | `src/story/CLAUDE.md` |
| The facility's sealed surface, its barrel groups, and the mechanics of writing against it | `src/expr/CLAUDE.md` |

The other subsystems (`exclude`, `facts`, `notes`, `roots`, `ls`, `worklist`, `coverage`,
`compare`) each carry a nested CLAUDE.md too; theirs record subsystem-local hazards rather than
laws lifted out of this file.

### Commands

- `scan` — Index directories or single files. A directory is walked, a named file observed singly; the file-grain path infers no absence (`src/scan/CLAUDE.md`)
- `roots` — List, suspend/unsuspend, comment, remove roots. `rm`'s ceremony states the root's story standing as fact (no artifact: what removal destroys, plus a `canon roots retire` pointer; bound: where the book lives) — awareness, never a gate (`RemoveRootPlan.retirement`) (`src/roots/CLAUDE.md`)
- `roots retire` — The full retirement ceremony: readiness review (identity, two-register resolution account, gap facts, asymmetric verdict) → confirm → the telling composes with one editor offer → bind (compile to temp on the shelf with `story.md` inside, verify, place by rename, pointer) → inspection window → confirm → release (world-moved-checked removal in one transaction). `--dry-run` reviews only; NOT READY needs `--allow unresolved`; `--yes` skips both confirmations, never the verification or the ordering (`src/retire/CLAUDE.md`)
- `roots retired` — The shelf listing: the retired fleet, one line per book (the union-listing law; `src/retire/CLAUDE.md`)
- `roots story` — The judgment instrument between passes: a source root's resolution story as a path-ordered map of places — acts as place-grain slices in the what/why register, and the never-decided at equal standing. `--limit N`/`--all` with counted omissions; read-only, fresh per run, no decision row; an archive-role target is refused toward `trail`. The readiness review points here on both verdict paths (`src/story/CLAUDE.md`)
- `worklist` — Output sources as JSONL for external processing
- `import-facts` — Import facts from JSONL on stdin
- `ls` — List sources matching filters (`-l` long format; `--duplicates` groups by hash)
- `facts` — Fact coverage and value distribution (`--key` supports modifiers/accessors)
- `coverage` — Archive coverage statistics
- `compare` — Compare two folders by content hash
- `survey` — Survey a selection for archive status, related locations, unique content (orientation/affinity modes, detail views); a scope resolving entirely inside archive roots is stated, not surveyed (`SurveyExit::FrameRefused`), decided from containing-root roles at resolution and never from a zero count; a mixed scope narrows to the source side and names what it set aside (`src/survey/CLAUDE.md`)
- `sweep` — Universe-wide reduction-opportunities leaderboard (`--limit N`/`--all`); the finder seat — no paths, no filters, read-only (`src/sweep/CLAUDE.md`)
- `cluster generate` / `cluster refresh` — Generate/refresh a manifest from matching sources
- `apply` — Apply a manifest to copy/move/rename files
- `exclude set/clear/duplicates/set-object/clear-object` — Manage source exclusions
- `note` — Annotate locations with timestamped notes (add, view, list, clear; surfaces in survey). Notes hold *thoughts*; the trail holds *actions*
- `trail` — Read the decision trail through four doors: the scope lens ("what did I do here?", chronological, closing with whole-history rollups over disjoint row sets and, when scoped, the present-tense "Standing here" composition card), the time lens (`--today`/`--since`/`--on`, day-grouped with fate rollups), `show <id>` (one decision's detail, its receipt pointers and where it drew from), and `crossings` (the counterpart door: the relation between two places). Notes interleave into the timeline unless `--no-notes`; `--jsonl` is available at each door. Which rows a view claims is the two-claims placement law's; a place nothing records is stated rather than rendered, and the same place must answer the same way whether it was named or is where you are standing (`src/trail/CLAUDE.md`)
- `ledger reindex [--dry-run]` — Rebuild the extraction ledger (`decision_extractions`) from apply receipts on disk, upgrading pre-precision rows to directory precision; global, whole-history, no decision row (index maintenance, not a content decision)

## Layered Architecture

```
┌─────────────────────────────────────────────────────────────┐
│ Interface: main.rs, front doors, <sub>/{cli,render,jsonl}.rs │
│ - CLI parsing (clap), output formatting, ceremony presentation│
│ - Directory walk creation; the ONLY layer touching stdio     │
└─────────────────────────────────────────────────────────────┘
                          ▼
┌─────────────────────────────────────────────────────────────┐
│ Operations: core/ops/ + <sub>/ops.rs or <sub>/ops/           │
│ - Typed operations + results, shared sub-ops (select_sources)│
│ - Ceremony policy, transaction boundaries; no stdio          │
└─────────────────────────────────────────────────────────────┘
          ┌───────────────┼───────────────┐
          ▼               ▼               ▼
┌──────────────────┐ ┌──────────────┐ ┌──────────────────────┐
│ Repository       │ │ Domain       │ │ Filesystem           │
│ core/repo/ +     │ │ core/domain/ │ │ (core/ops/fs)        │
│ <subsystem>/repo │ │ + <sub>/dom. │ │ copy/rename/hash/    │
│ ALL db access;   │ │ pure logic,  │ │ metadata; no DB, no  │
│ returns domain   │ │ no I/O       │ │ terminal             │
│ types; SQL here  │ │              │ │                      │
└──────────────────┘ └──────────────┘ └──────────────────────┘
```

**The layers are strata, not directories — and a stratum is a file or a directory, whichever the
subsystem needs.** `ops.rs` and `ops/` are the same stratum; so are `repo.rs` and `repo/`. Every
subsystem may hold each layer, so "Repository" means `core/repo/` *plus* every
`<subsystem>/repo`, and "ALL db access" is a claim about those together: SQL lives in a repo
stratum and nowhere else. A repo stratum lives in `archive`, `exclude`, `expr`, `facts`, `ls`,
`notes`, `retire`, `roots`, `scan` and `trail`, each holding the SQL only that subsystem
performs. **Where the SQL goes is a question of who needs it, not of which layer it is**: shared
by two or more subsystems → `core/repo`; exclusive to one → that subsystem's own `repo`.

**Layer Responsibilities (STRICT):**

| Layer | Allowed | NOT Allowed |
|-------|---------|-------------|
| **Interface** | CLI parsing, output formatting, ceremony presentation, terminal I/O, directory walk creation | Business logic, source selection, ceremony policy, computation, filesystem data ops, direct repo calls |
| **Operations** | Composing domain + repo + fs into behaviors, typed results, ceremony policy, transactions, composing text that is itself a record or artifact value | stdout/stderr/stdin, CLI arg types, terminal-shaped presentation (color, width, prompts, progress), direct SQL, direct filesystem data ops |
| **core/ops/fs** | Filesystem data ops: copy, rename, validate, hash, metadata | Database access, terminal I/O, business decisions |
| **Repo** | DB queries returning domain types, batch ops | Business logic, transaction management, filesystem access |
| **Domain** | Pure functions, structs, predicates, business logic | Any I/O (database, filesystem, network) |

**Where composed text lives — the second-consumer criterion.** The load-bearing line is **the
terminal, not the string**: the interface is the only layer that touches stdio, and the only one
that may shape output *for a screen* — color, width, prompts, progress. Composing the text itself
belongs to whichever layer keeps it spoken once:

- **One consumer, the screen → interface.** Text that exists only to be printed is presentation.
- **A second, durable consumer — a record, an artifact, a file → operations**, because the alternative is two spellings of one meaning. `summary` is composed in ops because the same string is the printed line *and* the decision record's summary *and* the trail's narration (one composition, three uses). `story/ops/render.rs` composes the place walk because it is both `canon roots story`'s output *and* the book's `story.md`, reached from `retire/ops/frame.rs` — a compile path the interface cannot serve.

Forcing such a renderer into the interface would split the book's text from the terminal's, which
is re-deriving one meaning per surface. What is enforced is **no stdio outside the interface**
(the architecture test's stdio-macro rule). A composition whose only consumer is the screen but
which sits in ops is a record-on-sight finding, not a build failure.

Canon has **two data planes** — Sources (DB-indexed, via `repo`) and Files (on disk, via
`core/ops/fs`); the operations layer orchestrates both. The separation is strict because pure
domain functions can be unit-tested with known inputs/outputs, so core-logic bugs (path matching,
scope resolution) are caught by tests, not users.

Most query commands select through the expression facility's `select_sources()`. Two commands
intentionally keep custom selection: `survey` (asymmetric visibility model) and `cluster generate`
(post-filtering for archive status). **A new command gets its own subsystem** — a directory with
its own strata and a pinned barrel, never a flat file that does everything. The architecture test
enforces this deny-by-default: a subsystem directory with no pin row fails the build.

## Core Invariants & Rules

The load-bearing rules. Violating them causes subtle bugs that the test suite is designed to catch.

**Path handling — SQL NEVER constructs or compares paths.**
- Repo returns `Source` objects with `root_path` populated (via JOIN).
- Domain computes paths via `Source::path()` and compares via `path_is_under()`. Path resolution comes in a pure half and a fallback half, same names in both: `core/domain/path.rs`'s `resolve_path()`/`resolve_paths()` soft-match against known roots and return `None` when nothing matches (offline, no filesystem access); `core/ops/scope.rs`'s wrappers of the same names fall back to `fs::canonicalize` for the unmatched ones.
- **The one sanctioned exception — rel_path *boundary membership* inside a query — is spelled exactly once**: `core::repo::db::path_at_or_under_sql()` / `path_strictly_under_sql()` (string comparison, never LIKE — `_`/`%` in a real path must match literally). Never hand-spell a `rel_path LIKE` prefix pattern; route through the helpers, because two boundary defects came from hand-spelled variants: a missing separator bound, then wildcard leakage in the deletion path.
- **The positional-probe bypass fails the build** (`tests/architecture.rs::the_containment_probe_is_spelled_only_inside_the_path_law`, the path law's registered verifying test). The fingerprint it refuses is the **positional probe**: take a byte prefix, then read the one element sitting at the prefix's length and check it against a separator. That is `path_is_under` re-derived, and a second spelling drifts silently — correct the day it is written, divergent the day the owner is repaired. Homes: `core/domain/path.rs` (the owner, which spells none today and is named prospectively) and the guard's own file. Exemptions are named with the verb each serves and an exact count, matched both directions, so a second spelling inside an exempt file fails too. **What is and is not build-refused, since the two adjacent claims must not contradict**: this guard refuses *one bypass idiom*. The law's own semantics stay **pinned**, by `path_is_under`'s unit tests; the SQL boundary spellings stay unsealed, exactly as the bullet above says. The guard's aperture is deliberately narrow and its blind spots are enumerated in its own doc comment — a bare `starts_with` used alone, the idiom split across two statements, the slice form, and the separator-concatenation family (`notes::domain::relative_to_scope` is a live production instance, agreeing with the owner and recorded rather than silent). Widening to every path-ish prefix test was weighed and declined: `Path::starts_with` and `str::starts_with` are indistinguishable to a text scan, so the wide aperture asserts a false equality. The scan covers `tests/` as well as `src/`.
- Operations resolve scope via `core::ops::scope::resolve_scope()` — the single pipeline for CWD defaulting, `--global`, root-membership + source-existence validation. **A subsystem's `cli` stratum receives pre-resolved `&[String]` prefixes and goes straight to `ScopeMatch::classify_all()` — no re-resolution.**
- Interface parses CLI path args, calls `resolve_scope()`, formats scope display. File-accessing commands (scan) use `fs::canonicalize` directly (hard resolution — path must exist).

**The scope-boundary honesty policy** (`core::ops::scope`) — at the scope boundary, what was asked and what ran is never a silent difference. The source-existence gate returns a named `ScopePartition` (`kept` / `set_aside`) rather than a verdict, and `resolve_scope` carries the set-asides forward as data on `ResolvedScope` (`set_aside`) beside its kept `prefixes`; the policy is spoken once in `apply_source_existence_policy` and never re-derived per command. The four rules: root-level paths (empty rel) are always kept; a **single** sourceless path errors (the "scan first" contract); **several** paths with ≥1 keeper proceed with the rest set aside; **several** with no keeper error naming every path. Root membership is the harder failure and keeps its precedence. Rendering is interface-only, one spelling (`scope.rs::write_set_asides` — `no sources known at <p> — skipped`), on each command's own scope channel: stdout for report commands, stderr for list commands, and in the ceremony immediately after resolution for effectful ones (`exclude set/clear/set-object`, `cluster generate`, `facts delete`) — before any plan display or confirmation, so `--yes` and `--dry-run` state it by position. A display mode that renders a bare stream and carries no scope header of its own (`coverage --compact`, `survey --detail unique`) states it on **stderr** via `scope::eprint_scope_set_asides`, so a machine-shaped stdout stays exactly what was asked for — the channel bends, the statement never goes unsaid. **A set-aside never becomes a `DecisionScope`** (`a_set_aside_never_becomes_a_decision_scope`). **Carve-outs**, spelled as the caller list of `validate_sources_exist` (the surviving abort spelling): `compare`'s two sides, `exclude duplicates`' scope and prefer paths, `survey --other`'s reference location — a location load-bearing to the question, where proceeding without it changes the question rather than narrowing it. `scan` is disk-facing and has no source gate; `trail` conjugates the policy through its own evidence gate instead (see `src/trail/CLAUDE.md`).

**The gate is on the explicit-path arm only, and a consumer must never read "came from `resolve_scope`" as "met the gate".** `resolve_scope` has three arms and only the first is gated: explicit paths go through `apply_source_existence_policy`; `--global` yields no prefixes to gate; and the **CWD-defaulting arm hands the current directory over ungated** — deliberately, because defaulting to CWD is a context switch rather than a claim about content, and a folder created since the last scan is a legitimate place to be standing. So a prefix leaving `resolve_scope` can be sourceless, and a downstream resolution that asks the index about it will find nothing there. (`resolve_history_scope` likewise skips the gate, for its own stated reason.) Every arm is named here because reading "gated" as universal is what let an `expect` panic `cluster generate` on its commonest invocation — no arguments, from a folder not yet scanned. The honest disposition for a sourceless prefix is the policy's own: set aside and state.

**The policy is conjugated at a second door**, the manifest's: `core::ops::scope::resolve_recorded_scope` partitions a recorded scope into confirmed / set-aside / unrooted, `cluster refresh` and `cluster status` state the set-asides through the same `write_set_asides` spelling, and the terminal rule — a scope that kept nothing must never look like a narrowing — is raised by refresh through the same `no_sources_known` sentence the argument door uses. Two differences, both deliberate: the resolution itself never errors (it classifies; every disposition is a caller's), and an **all-unrooted** scope is not the terminal case — it kept nothing either, but a refresh is the way back from a manifest naming a root that is gone, so refusing it would strand the user (`an_all_unrooted_scope_is_not_the_terminal_rule`).

**The form-tolerance rule** (`core::domain::path::normalization_candidates`) — a resolved prefix is always emitted in the byte-form the index stores; the argument bends, stored bytes are never rewritten (provenance stays derivable from disk alone). Candidates are as-given → NFC → NFD, deduped, with an ASCII fast path. The as-given-first ordering is load-bearing and must not be tidied: on a normalization-*sensitive* filesystem two spellings can be two genuinely different directories, and trying the path as written before any bend is what makes each resolve to itself. **Four integration points, and the rule has two halves.** The *root* half asks which known root owns a whole prefix and needs no database; the *remainder* half asks which byte-form of the below-root remainder the index knows sources under, and is spoken exactly once, in `core::ops::scope::stored_form_of_rel`. Two points are in the argument pipeline: `domain::path::resolve_path` tries candidates against root containment, and the source-existence gate (`stored_form_with_sources`) asks the remainder half, rebuilding onto the matched root, so root and content may each carry whichever form their disk gave. A path sourceless under every candidate is genuinely sourceless — only then does the boundary policy see it. The other two are at the **manifest door**, where text never passes through the argument pipeline at all: `core::domain::scope::attribute_prefix` runs the root half (candidates of the **whole** prefix, first match wins, so the form that matched the root is the form the prefix carries on), and `core::ops::scope::resolve_recorded_scope` then runs the remainder half through the same `stored_form_of_rel` the argument door uses — which is what makes the two doors incapable of drifting apart (`the_two_doors_agree_on_the_same_paths`). **Order is not interchangeable**: a prefix whose root portion is written in the other form must match its root before its remainder can be asked about. A remainder no candidate confirms is **set aside**, not obeyed and not dropped. A prefix rootless under every candidate is genuinely rootless and the recorded-scope resolution's own partition carries it.

**Key domain invariants** (`core/domain/source.rs`):
- `is_excluded()` checks BOTH source-level AND object-level exclusion.
- `matches_scope()` edge case: `/a/bc` is NOT under `/a/b`.
- `path()` handles empty `rel_path` (returns just `root_path`).

**SQL batching — any `WHERE ... IN (...)` MUST handle large ID lists** (SQLite var limit ~999–32K). Either chunk (`for chunk in ids.chunks(1000)`, see `core/repo/source.rs`) or use a temp table (`core::repo::db::populate_temp_sources()` + JOIN, see `core/repo/fact.rs`).

**Test databases — always use `core::repo::open_in_memory_for_test()`.** It builds the production schema (all constraints/indexes/CHECKs). Do NOT define custom test schemas — they drift and mask bugs. Insert helpers must provide all NOT NULL columns lacking defaults (notably `size`, `mtime`, `partial_hash`, `scanned_at`, `last_seen_at`, `device`, `inode` for sources).

**Test co-location — the fixture-sharing criterion**: tests live in-file with the code they exercise; test files externalize into a stratum's `tests/` directory with a shared `fixtures.rs` exactly when multiple files share test machinery — DB builders and pure helpers alike; **layer is never the criterion**. A self-contained corpus stays in-file even beside externalized siblings (`story/domain/place.rs` beside `splitter.rs`'s externalized corpus).

**A pin asserts the chartered claim, never the observed output**: a guard test's expected value is derived from the spec's claim, not captured from the run; and a guard or anti-drift test is **red-smoked against the defect it names** — deliberately plant the defect and watch the test fail — because an assertion that cannot fail against its own defect is not a defense. A pin keyed to output the code cannot even produce is the specimen this rule exists to prevent.

**Subsystem barrels are pinned**: the architecture test's `SUBSYSTEM_BARREL_ITEMS` table asserts each subsystem's exact `pub use` surface and seals every stratum `mod` (bare-private at a front door, where `super` is the crate root; `pub(super)` at most below it), deny-by-default both directions — a subsystem directory without a pin row fails the build, so a new subsystem cannot arrive unsealed, and a stale pin fails with it. Changing a barrel means editing its pin in the same commit. **What a pinned barrel carries — the parameter-type rule** (this is the domain name code comments cite): items consumers name, plus **parameter types of exported items** (a caller cannot factor a helper without naming them) — never return-type-only courtesies, because inference lets a caller leave those unnamed, so carrying them records demand that doesn't exist; when real demand arrives, re-adding is one pin edit with the consumer as evidence.

**Comment citations are build-enforced** (`tests/citations.rs`): a comment naming a file (`foo.rs`) or a module path (`a::b::c`), and a `Modules:` inventory line in any CLAUDE.md, must resolve against the current tree — prose about structure goes stale invisibly when code moves, and the stale line is never in the diff that moved it. Fix the prose; widen the scan's tolerance only for a genuinely legitimate citation shape. Every `src/` subdirectory owes a CLAUDE.md and an absent one fails the walk, because a subsystem that documents nothing is one no reader can find their way into.

**The register ceiling is build-enforced** (`tests/registers.rs`): each tracked CLAUDE.md, and the domain-language register beside them, carries a word ceiling and a cap on distinct backticked identifiers that name a test. Shrinking is free; growing means raising the number in the same commit, where a reviewer sees the raise beside the prose that needed it. Matched both directions — a tracked register with no row fails, a row naming no register fails — which is what makes prose expensive to add and free to remove, the opposite of how a document about code drifts on its own.

**The law roster is build-enforced** (`tests/laws.rs`): the register of Canon's named laws, each row carrying its owner, its verifying test, its reach, its authority and the date it was decided. Matched **both directions** — a law name spelled in the root file or any subsystem CLAUDE.md that resolves to no row fails the build, and a row whose owner or verifier has gone fails with it. The walk is depth-1 and reads prose only: a name spelled in a nested CLAUDE.md deeper than a subsystem's own, or in a code comment, is out of its reach. Names the register does not carry live on a named unqualified list, each with the reason — either that the key is not a law's own name, or that a law's carrier is short a piece. The list is exact-match in both directions, so it can neither grow silently nor keep an entry whose prose was fixed. Adding a law means adding its row in the same commit. The roster rides the CLAUDE.md walk `tests/citations.rs` uses, spelled once in `tests/common/mod.rs`. Deliberately **not** checked: that a row is cited in prose — a meaning spoken once in code with a pinned battery is a law whether or not anyone has written about it. The remaining gap is an *owner with no row*, which no build-grade check reaches from here.

**Concurrency** — users may run multiple canon processes at once. Repo functions do NOT manage transactions; operations/commands establish scope. Weigh transaction scope (atomicity vs. contention) and idempotency (re-run after partial failure often reduces the need for transactions).

**`decision_id` set/preserve rule** — `sources.decision_id` records the decision that caused the *most recent* state transition. Set on `New` reconciliations, `insert_destination()`/`update_location()` (apply), exclusion transitions (`set_excluded`/`batch_set_excluded`/`set_decision_id_by_object`), and **deletion transitions** (`mark_missing(conn, ids, now, Some(decision_id))` — a source going `present=1 → 0`, whether via the scan sweep or `--missing`). Scan `Modified`/`Moved`/`Unchanged` paths **omit the column from their UPDATE** — they preserve the existing value (`mark_missing` with `None` likewise preserves, e.g. when recording is off). A standing *present* path is never `New`: replacement (same path, new inode) joins the `Modified` family and preserves like every other observation, so a scan never overwrites the last performed act on a path that stood the whole time. `New` reaches an existing row only through **revival** (it was `present=0`). **One principled exception on the New path**: a revive at an *excluded* row preserves both the exclusion and its `decision_id` (`CASE WHEN excluded = 1`) — a source exclusion survives every scan-observed standing change (a dismissal is undone only by `exclude clear`'s recorded `restored` act, never as a scan side effect), and the row must keep pointing at the judgment that governs it, or story/trail/book narration would find excluded content citing a scan. (Not total: an excluded row *deleted* is stamped by the scan — `classify_absent` needs the Observe-family stamp — so an excluded → deleted → reappeared row stands excluded while citing the scan, its reason reachable via the deletion receipt's `previous_decision_id`; known, accepted, don't "fix" either side.) Apply's `insert_destination` still clears exclusion at its destination — claiming is a performed, recorded act.

## Conventions & Patterns

**User docs register** — anything written into `docs/src` follows the charter in `docs/STYLE.md` (page tiers, register criteria). Read it before writing or editing doc pages.

**The Standard Pattern** (domain → repo → ops → interface):
```rust
// Operations compose repo fetch → domain predicates → typed result:
let selection = select_sources(conn, &params)?;   // use crate::expr::{select_sources, SelectionParams};
// selection.sources, selection.source_ids(), selection.excluded_count
```
1. Domain: struct + pure predicates. 2. Repo: batch fetch/write, returns domain types, SQL here. 3. Ops: compose into a typed result. 4. Interface: parse args, call ops, format.

**Operations layer conventions:**
- Read ops take `&mut Connection` (filter temp tables); no transaction management.
- Write ops take `&mut Db` and **own their transactions**.
- Result types are concrete structs per operation (no generic containers). No stdio.
- **Plan/execute** separates computation from side effects (reference: `exclude::ops`). Plan returns a typed struct with display + confirmation data; execute performs writes, composes a `summary`, optionally records a decision, returns a typed result. The interface decides what happens between (dry-run, confirmation, immediate).

**Repo return-type conventions:**

| Operation | Returns | Example |
|---|---|---|
| Create | Domain object (fetch after insert) | `create()` → `Root` |
| Get-or-Create | Domain object | `get_or_create()` → `Object` |
| Fetch/Read | Domain object(s) | `fetch_all()` → `Vec<Root>` |
| Mutation | `Result<()>` | `set_excluded()` → `()` |

Creation fns return domain objects so the caller immediately has usable data (the `insert_destination()` pattern).

**Decision & receipt conventions** (`core/ops/decision.rs`, `core/ops/receipt.rs`):
- **`decision.rs` depends on `receipt.rs`, never the reverse.** `receipt.rs` is decision-agnostic (format + writers + placement).
- Two-phase recorder: `start()` INSERTs "started"; `complete()`/`complete_with_receipt()` UPDATEs outcome and finalizes `.incomplete` → `.toml`. Execute fns take `Option<&DecisionParams>` (`None` skips recording, used in tests).
- **Scope is a typed where-contract, not free-form strings.** `DecisionParams.scope: Vec<DecisionScope>` (`core/domain/scope.rs`) — each scope is *decomposed to a known root* (`root_id` + stored canonical `root_path` + `rel_prefix`); empty = global. Constructing one requires a matching root (via `DecisionScope::decompose(prefixes, roots)`, or `new()` when the caller already holds its root), so a bare `"."` or a root-relative string is **unrepresentable** — an invariant the type carries rather than convention, because a raw `"."` once reached a record. The recorder is a pure sink: it *derives* the `decisions.scope` display column and `meta.scope` from `display_path()`, and the `decision_scopes` index rows from `index_pair()` — callers never supply raw scope strings. Decomposition happens at the scope-resolution boundary (the caller has `roots` + resolved prefixes), never inside the recorder.
- The recorder **collects warnings in a `Vec<String>`, never writes to stderr.** Execute fns **drain `take_warnings()` into their result struct** (`Exclude*Result`/`ApplyResult` `warnings` field); the interface prints them. A **0-item plan skips recording** (no decision row, no dangling receipt).
- **Placement principle: a receipt lives at the locus of the action's effect.** apply → destination archive root; deletion → the **source root** where the file was lost; exclusion → archive ledger root (the dismissal judgment must outlive the source drive it clears; the archive ledger is the enduring home of resolution records). (The Receipt Placement Principle ADR.)
- Receipt placement (`ReceiptPlacement`): `Targeted` (apply — mirrors the destination under `.canon-ledger/` per `layout`) and `LedgerRoot` (flat at a root's `.canon-ledger/`, layout-independent). `LedgerRoot` serves **both** exclusion (via `resolve_ledger_root()` → archive root) and **deletion** (pointed at the **source root** that lost files) — same mechanics, different which-root policy; no new variant.
- **A receipt states its own what + where in `[meta]`** (self-describing receipts): `transition`/`posture` (registered vocabulary), `[meta.locus] { path, id }`, and apply-only `origin_disposition` (`retained`/`relocated`). All **derived, never a per-writer literal**: each writer names a **`ReceiptKind`** (`core/ops/receipt.rs`) — the single authority mapping to `(family, aspect)` (→ `fate_transition`/`fate_posture`) and, for apply, origin disposition. `receipt_meta(decision_id, status, summary, locus, kind, manifest)` is the one derivation site. **Locus is the receipt's where, from placement** (`ReceiptPlacement::locus_root()`), not from scope — orthogonal to the `DecisionScope` typing; it's **required and gated on placement** (a placement-less receipt is written nowhere, so the build is skipped). `ReceiptLocus` is **`ReceiptMeta`'s last field** so the struct reads in the order the file is written — TOML puts a sub-table after its parent's scalar keys, and the serializer arranges that itself (a trailing scalar does *not* fail to serialize, pinned by `a_scalar_declared_after_a_nested_table_still_serializes`). **The body shapes belong to the commands that write them** — apply's in `archive/ops/receipt.rs`, exclude's in `exclude/ops/receipt.rs`, scan's in `scan/ops/types.rs`; `write_receipt` is generic and never inspects a body, so `[meta]` is the only part any two receipts share. All fields additive under `receipt_version = 1` (no schema change, no version bump). `ReceiptKind`'s family mapping must agree with `decision_family(command)` — the integrity test in `core/ops/receipt.rs` enforces it.
- **Granularity invariant**: subjects that can span roots carry **per-item** root identity (apply `source_root`; exclusion/object `root`); the **locus** root is always **meta-level**. Receipt-level-only identity (no per-item root — the deletion receipt) is valid exactly where single-root-ness is guaranteed by construction (deletion receipts are coalesced one-per-root, so items inherit the meta locus).
- **Stamp-set = receipt-set** (presence-axis constraint): object-level exclusion stamps every source sharing the object, present or not (`set_decision_id_by_object`); the receipt must list that same set. Capture it in-transaction **before** stamping (`fetch_object_sharers_for_receipt`) so `previous_decision_id` is pre-stamp; tombstones are marked `present = false` in the receipt entry. Never let a stamp touch rows the receipt doesn't list.
- **Receipts record content fates only.** The receipt gate is the per-item *source* state transition — never intentionality, never data class. Annotation ops (`note clear`, `facts delete`) are decision-row-only: notes/facts are the user's scaffolding, not assets, and their removal is housekeeping, not a fate. A note-clear receipt is not owed and must not be re-derived. A 0-item clear records nothing (0-item convention).
- **One decision may emit several receipts** — a scan deleting across N source roots writes one source-local receipt per root, all under the one decision. The single `decisions.receipt_*` columns can't hold that, so each per-root receipt is linked via `decision_scopes.receipt_rel_path` (`set_scope_receipt` upserts; the DB index is rebuildable, disk is the truth). `scan::ops::receipt::write_deletion_receipts` coalesces same-root entries so each root writes exactly one receipt.
- **`previous_decision_id`** lives in the receipt item (the source's `decision_id` before the op — captured **pre-flip** for deletion). Because that id is also the predecessor receipt's filename stem (`{decision_id:06}-{command}.toml`), the chain is walkable from disk alone.
- **A receipt claim is registered prospectively and settled at the decision's last act.** `DecisionRecorder::start()` writes the receipt columns before the file they name exists, so the recorder — which registered the claim — is what settles it: `settle_receipt_claim` finalizes a receipt that was written and retracts one that was not, leaving the columns citing only an artifact that is on disk. It is **not** DB completion: exclude commits its row inside its transaction and writes the receipt after the commit, so retracting at `complete_db` would clear a claim about to become true; and apply's pre-transfer refusals settle both of the row's prospective claims in one act — the run reaches a last act, it just declines to do anything. A failed *finalize* retracts like a failed write, the artifact being equally absent. The recorder tracks what the row asserts (`ReceiptClaim`: `Unclaimed`/`Registered`/`Written`/`Finalized`/`Artifact`); only the `start()`-registered slot is retractable, and `Finalized` is terminal so settling twice cannot un-claim a receipt that is on disk. Two neighbours share the same columns and are exempt by construction: scan's `write_placed_receipt` (post-hoc, never registers) and retirement's `record_artifact_pointer` (the book exists independently of any receipt → `Artifact`). **A reader of empty columns states what it observes, never a cause** — the row cannot tell receipts-off from a write that did not complete, and the book binds that reason forever. On every *completing* path the absence also explains itself from the row's own counts and status; a refused run says so in its own status, and a run that never reached a last act at all says only that no receipt was recorded. The prospective-claim settlement law owns this, verified by `a_failed_receipt_write_clears_the_claim`.
- **A status claim is registered prospectively and settled at the same last act.** `insert_started` writes `started` before anything happens, so the row makes a claim about a run that has not finished — the same prospective shape as the receipt columns beside it. Settling means writing a terminal: confirmed (`completed`), corrected (`partial`/`interrupted`) or **retracted** (`refused` — `DecisionRecorder::refuse`, the one terminal the recorder owns whole, because a refusal carries no caller-specific information: always empty counts, always the receipt claim retracted in the same act). What survives on a row reading `started` is therefore exactly one thing: a run that never reached a last act at all — killed, crashed, power lost — which is the recovery signal the write-path atomicity rules name and `docs/src/concepts/decisions.md` publishes. **Which word a non-refusal outcome deserves stays the caller's**, legitimately — the caller is what knows its own results — so scan's uncounted interruption and retirement's snapshotted decline are **declared projections**, each carrying a test that says so, never funnelled through a shared `interrupt()`. Tracked as `StatusClaim` (`Unclaimed`/`Registered`/`Settled`); a failed UPDATE leaves the claim registered (the row really is still `started`) and pushes a warning — which reaches the user only where the caller drains it, and apply's two refusal paths `bail!` without draining, so there the record of a failed settlement is the row itself. `Settled` never walks back; it is also **not proof the row settled** where the UPDATE ran inside a transaction that then failed to commit. Retirement's release settles again on the connection after such a rollback, so the claim and the row converge; the narrower case where that second settling `UPDATE` itself fails is unread. **Still open**: the general `?` window between `start()` and `complete()` at `cluster generate`/`refresh`, `roots rm`/`suspend`/`unsuspend`, `facts delete` and the prunes, and `import-facts` — *which* status each failure deserves is per-command product judgment, not recorder work. Retirement's release is closed: every *returning* exit after the bind records `partial` — book bound, root still in the index — settling at one site, its wrapper's `Err` arm, on the connection rather than the transaction. `exclude` and `notes` are immune by construction (recorder on `&tx`; a `?` rolls the row back). Every terminal is reachable by name, with no `_` arm (`every_terminal_status_is_reachable_by_name`).

**Extraction ledger conventions** (`decision_extractions`, `core/domain/extraction.rs`, `core/ops/ledger.rs`) — the trail's outbound direction:
- **Aggregate-only, never per-item** (the consumption-readiness razor): one row per `(decision_id, root_id, origin dir, destination dir)` — directory-precision placements, never a per-item copy (no filenames). Per-item detail stays in the apply receipt on disk; a `kind`-tagged `decision_scopes` row was rejected precisely to keep this table's meaning crisp.
- **The uniform row claim**: every row asserts its files lie *under* its recorded origin location and were placed *under* its recorded destination location. Precise rows make the claim tightly (containing directories, via `parent_dir`); legacy pre-precision rows hold lossy common prefixes and make the same claim slackly. Matching and counting treat both identically (`placement_in_view`): a row surfaces exactly where the view contains its location, and wherever it surfaces its count is exact — which is why legacy rows need no special case, only conservatism (silent below their prefix until `ledger reindex` upgrades them from receipts).
- **`replace_extractions`, never row-level upsert**: the writer deletes each covered decision's rows then inserts the new set. A row-level upsert on the widened PK would leave a legacy coarse row standing beside freshly precise rows — a silent double count. Repo does no transaction; **callers wrap the pair** (`unchecked_transaction` — apply around its one write, reindex per decision) so a concurrent trail read never sees a half-replaced decision.
- **One aggregation helper, two callers**: `core::domain::extraction::build_extraction_rows` is called by both the forward path (`archive::ops::execute::execute_apply`, right after the transfer loop) and the backfill path (`core::ops::ledger::reindex_extractions`). Sharing the code — not just the shape — is what makes a backfilled row structurally indistinguishable from a forward-recorded one; the extraction round-trip law is a test, not a convention held by discipline alone.
- **Forward recording gates on `record_enabled`, not `receipt_enabled`.** `Records`-mode decisions still get a live extraction ledger; only backfilling an *old* `Records`-mode decision (no receipt was ever written) is impossible by construction — reported as `no_receipt`, never guessed.
- **`core/ops/ledger.rs` owns lenient receipt-read types** (`ApplyReceiptDoc` etc.) — `core/ops/receipt.rs` stays writer-only and decision-agnostic; its types must never grow the `Option`-tolerance a maintenance-path reader needs for pre-vocabulary receipts. This is the one place that reads receipt files back — every query path (`trail/ops/compute.rs`) still reads DB projections only. Its apply-driving tests live in `archive/ops/receipt.rs`, not beside it: they name archive's transfer types, and a core test may not name a subsystem.
- **`ledger reindex` records no decision row.** Index maintenance is not a content decision; the printed coverage report is its own record. Every decision it scans lands in exactly one bucket (`indexed`/`already_current`/`no_receipt`/`unreachable`/`malformed`) — self-explaining gaps, never silent.

**The contentless law** (`core/domain/source.rs::is_contentless`, `core/repo/object.rs`, `survey/domain/object_index.rs`; the Contentless Law ADR carries the rationale, including every does-NOT-apply site):
- **Identity claims about empty content are vacuous.** An empty source is all shape, no content — the standing *contentless*: never covered, never unresolved, never blocking; stated, never silent; carried with its place. Surfaces say the plain referent ("empty files") beside the word. **The law governs identity claims, never transitions**: a receipt origin names one specific path going to one destination — nothing vacuous — so an applied empty file is *archived, fate-wise* (the book's origin check precedes the Contentless fate arm; extraction rows and act lines count applied empties). Contentless is the standing where no act claimed the path.
- **One vocabulary point**: `Source::is_contentless()` is the only place `size == 0` is written. **The law in SQL**: `batch_check_archived`, `batch_find_archive_paths`, `batch_find_archive_info_by_hash`, `batch_check_archived_from_root` all require `s.size > 0` on the archive side (documented SQL projections of the predicate) — this converts every archived-ness consumer at once: `archived?`, coverage, ceremony contexts, readiness/story/book, cluster's skip set (empty files travel with their folders), apply's archive conflicts. **The law in the index**: `ObjectIndex::build()` refuses contentless sources (survey inherits). Compare's own object maps (`compare::ops::select_and_build_map`) skip contentless with a counted note the same way — it builds its maps directly rather than routing through the SQL or the index, but it lives in ops like every other law site; no interface-layer exception remains. **The object-exclusion guards** (`exclude::ops::plan`/`exclude::ops::single`) are law sites too: `plan_set_objects` sets empties aside (counted) and `check_set_object_by_file` refuses an empty path toward explicit `--hash` — identity-keyed exclusion would dismiss every empty file in the universe via the one shared object; both route through the predicate.
- **Classifier precedence** (`classify_present`): excluded > contentless > archived > covered > unresolved. Exclusion first (judgment covers shape); contentless precedes every identity test *including the hash test* (an unhashed empty source is Contentless, never `Unresolved{unhashed}` — empties on never-enriched roots must not block retirement). The **archived/covered split's evidence** is the extraction-linked object join (`batch_check_archived_from_root`: archive copy present AND stamped by a decision whose extraction rows draw from this root) — never stamp families: apply stamps destination rows, not the origin root's. `Archived ⊆ archived` holds by construction.
- **Where the law does NOT apply — do not "fix" these**: `ls`/worklist/facts counting (existence, not identity); apply's path-collision checks; exclusion anywhere; scan/reconcile; the sweep's counted set-aside; hashing/enrichment (empty files stay indexed and hashed — identity computed, never load-bearing). Coverage's denominator is `coverable_sources()` (hashed − contentless) — a fully-covered root with empties reads 100%.
- **The canary** (`src/contentless_law_tests.rs`): one module, every surface; a regression on any single site fails exactly one named assertion. Extend it when a new surface consumes content identity.

**Summary composition** — every effectful execute fn returns a typed result with a `summary: String`. Ops composes the message; the interface prints it; the same string is the decision record's summary, which the trail renders as the per-line narration (one composition, three uses).

**Ceremony** (`ceremony.rs`) — `confirm(yes)` shared prompt ("Proceed? [y/N]"); content gated behind `!yes` (skipped with `--yes`). `core::domain::format::format_count()` adds thousands separators.

**CLI conventions** — `canon roots` and `canon roots list` must behave identically; add flags to both. **A refusal that is itself a legitimate answer exits non-zero without `Error:` noise** — a NOT READY retirement, `SurveyExit::FrameRefused`, `TrailExit::PlaceUnknown` — because prefixing an answer with `Error:` tells the user something went wrong when nothing did.

**Fact keys** — use `BuiltinKey::from_str()` to detect built-ins (don't string-match); stored facts fall back to the facts table. Key normalization (adding the optional `content.` prefix) is the facility's own business and is not reachable from outside it — hand raw keys to the barrel and let `ParsedFactKey::parse()` normalize them. Reference: `expr/domain/key.rs`, `expr/domain/value.rs:get_builtin_value()`.

## Domain & CLI Concepts

### CLI Flag Vocabulary

Three unified flags:
- **`--include`** (query commands `ls`, `facts`, `coverage`, `worklist`, `compare`, `survey`): expands what you see — `excluded`, `archived`, `all` (comma-separated, repeatable). Always safe. Compare/survey accept only `excluded`.
- **`--allow`** (effectful `cluster generate`, `apply`, `import-facts`): acknowledges non-default source selection ("I'm aware, proceed"). Per-command values. Not on `cluster refresh` (reads manifest `[options]`). **An acknowledgment belongs to the invocation that needs it**: the manifest's recorded `allow` feeds `cluster refresh`, the one command that re-selects from it; `apply` re-selects nothing and takes only `duplicates` from it (the acknowledgment travels with the content it is about to transfer), asking for everything else on its own flags. The archived half being unread at apply is the rule, not an omission.
- **`--global`** (scope-taking `ls`, `facts`, `coverage`, `worklist`, `survey`, `trail`, `cluster generate`, `exclude set/clear/set-object`): operate on all roots, bypassing CWD defaulting. Only meaningful with no explicit paths. Not on `compare`.

Provenance flags:
- **`--no-receipt`** (global): suppresses receipt file generation for this invocation (DB recording still happens per `RecordingMode`). Per-invocation opt-out.
- **`--reason`** (effectful `exclude set/clear/duplicates/set-object`, `apply`, `scan`, `roots rm`): attaches user reasoning to the decision record. Optional, no prompting; empty = no reason.

**Recording modes** (`$CANON_HOME/config.toml`, `ledger.recording`): `Full` (DB + receipt files), `Records` (DB only), `Off` (nothing).

**CWD scope defaulting** — scope-taking commands default to CWD when no paths given and CWD is inside a known root. Inside an archive root, `--include archived` auto-enables. CWD not under any root → global (silent). `--global` forces global from inside a root. Effectful commands show scope/count/root-breakdown in confirmation.

**Scope display** — report commands (`survey`, `facts`, `coverage`, `compare`, `trail`) show scope on stdout ("Facts: /path" or "Facts: all roots"); list commands (`ls`, `worklist`) show scope on stderr when scoped, silent when global.

**Non-root error** — an explicit path not under any known root errors immediately; CWD-not-in-root falls back to global silently.

**`ls` display** — `--duplicates` is the sole display mode (groups by hash). Status filtering via `--where` predicates (`archived?`, `NOT hashed?`, etc.). With `--include`, `ls -l` shows a status column: `E` (source-excluded), `X` (object-excluded), `A` (archive source), or blank.

### Canon Home

All state under one "canon home": default `~/.canon/`, override `CANON_HOME` env or `--canon-home` flag (precedence: flag > env > default). Contains `canon.db`, `aliases.toml`, `config.toml`.

`config.toml`:
```toml
[ledger]
recording = "Full"   # Full | Records | Off
layout = "Central"   # Central (root-top .canon-ledger/, mirroring the affected path) | Alongside (.canon-ledger/ beside the affected content). Both place within a root — every receipt is root-anchored on disk.
root = 5             # Optional: root id of the archive ledger root, where `LedgerRoot`-placed receipts (exclusion) land. Default: the lowest-id active archive root.
```
Missing/empty config is fine — `LedgerConfig::default()` is `recording = Full`, `layout = Central`, `root` unset.
`root` is **advisory, never fatal** (`core::ops::receipt::resolve_ledger_root`): an id that is missing, suspended, or names a source root falls through to the default rather than failing the command — receipt placement must not be blockable by a stale config line.

### Expression Aliases

Named `--where` aliases in `$CANON_HOME/aliases.toml` (e.g. `image = "content.mime IN (...)"`). Usage: `canon ls --where "@image AND @tens"`. The `@name` syntax expands **before** filter parsing (the engine never sees `@`); each value is paren-wrapped; `@` inside quoted strings is literal. Expansion happens in `main.rs`; pure logic in `expr/domain/alias.rs`, file I/O in `expr/ops/alias.rs`.

### Filter Expressions (`expr/domain/filter.rs` grammar, `expr/ops/filter.rs` evaluation)

`--where` supports full boolean logic. **Operators**: `=`, `!=`, `~`/`!~` (glob), `>`, `<`, `>=`, `<=`, `IN (...)`, `NOT IN (...)`, `?` (exists). `=`/`!=` are case-sensitive (use `|lowercase`). **Glob** (`~`): `*`, `?`, `[abc]`, `[a-z]`, `[!abc]`.

**Status predicates** — `archived?`, `hashed?`, `excluded?`, `enriched?` — computed state, not stored facts. Recognized before `content.` normalization. Boolean-only (`?` and `NOT ... ?`); comparison operators error. `Expr::Status(StatusPredicate)` in the AST, evaluated via batch-prefetched `HashSet`s. `apply_filters()` returns `FilterResult { source_ids, UsedStatus }`; `UsedStatus` propagates through `Selection`/`SurveyResult` for visibility-mismatch hints.

**Modifiers** (`|`): `source.mtime|year`, `source.ext|lowercase`, etc. **Path accessors** (Python-style): `source.rel_path[-1]|stem`. **Content prefix optional** — bare keys normalize to `content.*` (`Make` → `content.Make`); applies to `--where`, `--key`, `--group-by`. **Not** manifest patterns, which read their key through `parse_key_and_accessor` and never normalize: `{Make}` is unknown where `{content.Make}` resolves. Built-in keys (`source.*`, `filename`) and explicit-prefix keys unchanged. Built-in derived facts (`filename`, `source.ext`) are hardcoded in `expr/ops/filter.rs`. Any imported fact is filterable.

### Manifest Patterns (`expr/domain/pattern.rs`)

`{expr}` syntax. **Path accessors**: `{source.rel_path[-1]}` (filename), `[0]`, `[1:3]`, `[:-1]`. **Modifiers**: `|year`, `|month`, `|stem`, `|ext`, `|short`. **Aliases** (patterns only, see `canon facts --show-aliases`): `{filename}` → `{source.rel_path[-1]}`, etc. Example: `pattern = "{content.DateTimeOriginal|yearmonth}/{filename}"`.

**`{scope.rel_path}` measures from the vantage** — the deepest directory containing every scope
that lies in the source's own root. One scope is its own vantage; siblings share their parent, so
each scope's own name survives at the destination; scopes in several roots each get their own, and
it can never climb above a root. Derived once per run (`expr::ScopeVantage`) from the resolved
recorded scope, and never re-derived per reader.

**The lock carries the measurement, and that is where placement is settled.** The config is
editable intent; the lock is the settled decision. A vantage is constructed **only where a
manifest is written** — `cluster generate` and `cluster refresh`, through one shared
`archive::ops::generate::measure_entries` — which writes each entry's `scope_rel_path` into the
lock beside the file it names. The two readers, `apply` and `cluster status`, consume that recorded
value: neither resolves a scope, builds a vantage, or consults roots for placement. `apply`'s own
claim that it *selects nothing and carries out a lock already built* is true of placement too.

The consequences worth knowing. Editing `meta.scope` takes effect on the next `cluster refresh`,
like the filters beside it, rather than changing **placement** without changing **selection** —
half an edit landing silently. Editing `output.pattern` takes effect immediately, because a
pattern is how you want things *named* rather than a property of the selection. Evaluation has
**two** refusals, which is how many causes an absent measurement can have. Both are facts about
the run, and they take different answers, so an evaluation that could not tell them apart would
prescribe a remedy that may not work: **a lock written before the measurement existed**, which
`apply` refuses outright — whatever the pattern says, because such a lock also carries no scope
for the decision record, and a scoped act written down as a global one is the silence this
mechanism exists to close — and **a manifest that records no scope**, where there was nothing to
measure from and no refresh will give it one. Which of the two an entry is in is a property of the
lock file, never of the entry (`archive::ops::manifest::LockFile::unmeasured_reason`), and the
`expr::Unmeasured` the reader hands to evaluation is what carries it. `cluster status` asks the
same question before it names a next step, and says nothing where nothing would help. There is
no fallback: a destination is the one decision a user cannot un-decide after a move.

The recorded scope both writers measure from is **resolved first, once per run**
(`core::ops::scope::resolve_recorded_scope`): form-tolerantly in both halves, carrying a prefix that
matches no known root, and setting aside one whose remainder no byte-form confirms. Only
**confirmed** scopes reach the vantage, the lock header and the decision record — so a line naming
a place Canon cannot confirm does not drag the measurement, and the surviving lines place
correctly instead. What each writer does about a carried line is stated, never silent: a refresh
says so, keeps the line verbatim in the file, and continues; a status reports it. Pattern-only: a
`--where` clause has no access to a manifest's scope, so the key is a plain const rather than a
built-in. Details and pins in `src/expr/CLAUDE.md`; the manifest and lock side is in
`src/archive/CLAUDE.md`.

### Root Specs

`--root`/`--archive`: `id:N` (by DB ID) or `path:/foo/bar` (by path). Parse via `RootSpec::parse()`.

### Database

Default `$CANON_HOME/canon.db`. Key tables: `roots`, `sources`, `objects`, `facts`, `notes`, `decisions`, `decision_scopes`.

**Schema evolution via `rusqlite_migration` (`PRAGMA user_version`).** The migration set *owns* the schema (`core/repo/db::migrations()`): migration 1 is the frozen `SCHEMA` baseline (`IF NOT EXISTS` throughout, so pre-adoption DBs upgrade in place); each later entry is an **append-only** `M::up` delta (e.g. an `ALTER TABLE` adding a column). **Never reorder or edit a released migration; never re-add a column to `SCHEMA`** — `user_version` tracks progress and rewriting history desyncs existing DBs. Both `open_with_options()` and `open_in_memory_for_test()` route through `to_latest()`, so tests run the real migrated schema. Pinned `rusqlite_migration = "1.3.x"` (rusqlite ^0.32; 2.x needs rusqlite ≥0.34).

- `roots`: `suspended` (hide from ops), `comment`, `last_scanned_at`.
- `sources.decision_id` (nullable, no FK): the decision behind the most recent state transition (see the set/preserve rule above).
- `decisions`: `receipt_root_id`/`receipt_rel_path` (receipt location, set only when a receipt was actually written — registered prospectively under `RecordingMode::Full` without `--no-receipt`, and retracted at the decision's last act if the write never happened); `status` (`started`/`completed`/`partial`/`interrupted`/`refused`); `created_at` and `command` are indexed.
- `decision_scopes` `(decision_id, root_id, root_path, rel_prefix, receipt_rel_path)`: durable root-based scope index. **Populated** at `DecisionRecorder::start()` by projecting the already-typed `DecisionParams.scope` (`Vec<DecisionScope>`) — one row per scoped root/prefix, no re-derivation. Scan/import additionally record roots that didn't exist at `start()` via `record_scopes(&[DecisionScope])` at completion, which both inserts the index rows **and** backfills the `decisions.scope` display column for those roots. `receipt_rel_path` (nullable) links each source-local deletion receipt to its root (`set_scope_receipt`), so one decision can carry N per-root receipts. `root_path` (nullable) is a **write-time snapshot** of the root's path (same precedent as `decision_extractions`), projected via `DecisionScope::index_row()` — read paths render it snapshot-first (live-roots join second, marked `(removed)` fallback last) so scope references survive `roots rm`. A row with no snapshot is recovered from the decision's `scope` display column via `core::domain::scope::recover_root_path`, the suffix-direction inverse of `display_path()` (`recover_is_the_inverse_of_display_path`) — NULL-over-guess: ambiguity stays NULL, never a wrong path.
- `decision_extractions` `(decision_id, root_id, root_path, rel_prefix, files, bytes, destination_root_id, destination_path, disposition)`, PK `(decision_id, root_id, rel_prefix, destination_path)`: the trail's placement index — what an `apply` decision drew from each (source root, origin dir) into each destination dir, aggregate-only at directory precision (see the Extraction ledger conventions above). Written via `core::repo::decision::replace_extractions` (delete-then-insert per covered decision, callers wrap in a transaction) so forward recording and `ledger reindex` converge without leaving stale coarse rows. `root_path`/`destination_path` are write-time snapshots — they render even after a root is removed.

## Workflow Models

### Design Principles

External tools for hashing/metadata (via JSONL worklist/import). Incremental workflow (scan → enrich → cluster → apply). Human-editable manifests (`.toml`). `basis_rev` tracks file state changes for staleness.

## When Adding New Features

1. Predicate / business logic → domain layer (pure function).
2. Database access → repo layer (returns domain types).
3. Composed behavior (selection, scope resolution, computation, ceremony policy) → ops layer.
4. Filesystem ops (copy, hash, validate, metadata) → core/ops/fs.
5. Interface modules ONLY parse args, call operations, format output.
6. Refactoring existing commands → extract behavioral logic to ops.
7. Scope resolution is ops (`core::ops::scope`, behavioral policy); scope *display* is interface (`scope.rs`, presentation).

## Architecture Decisions

**Each accepted ADR's *rule* is distilled below; its *rationale* stays in the ADR itself.** An ADR
is cited by title, never by slug or date, and only an accepted one contributes a binding rule.

- **Operations Layer** — composed behaviors live in an `ops` stratum as typed plan/execute functions; the interface only parses/calls/formats. *Binding rules are the Layered Architecture + Operations-layer conventions above.*
- **Provenance Write-Path Atomicity**:
  - Exclude execute paths **own a transaction** over their DB writes; the receipt file is written **after** commit (transaction = DB mutations; receipt = artifact after commit). Single-target exclude paths use `conn_mut()`.
  - Two-phase (`started`/`completed`) recording exists to bracket **non-rollback-able side effects (the filesystem)**; pure-DB ops should be atomic and need no `started`-row evidence.
  - Apply is **non-transactional by design** (FS+DB); recovery is fix-forward (real `interrupted()`, status on disk, findable `started` rows).
  - `ReceiptMeta` carries `status` (interrupted/partial receipts self-describe on disk).
- **Provenance Consumption Readiness**:
  - **The DB is a rebuildable index over durable receipts.** Consumption is served by indices/projections (`decisions`, `decision_scopes`, optional `previous_decision_id` column) — never a second copy of receipt *content*.
  - **Gating is two-layer**: recording = every effectful action; receipt + `decision_id` = per-item state transition ("receipt recording rule = decision_id update rule"). "Decision" is the generic name for any effectful op.
  - **Disposition-fate completeness**: each of the three terminal fates (archived / excluded / deleted) must, by default, leave a durable, decision-linked record.
  - **Self-explaining gaps**: provenance gaps must never be silent — every opt-out (`--no-receipt`, `recording = off`, no archive root) is honored *and* recorded in the decision row, so absence ≠ loss.
- **Receipt Placement Principle**:
  - A receipt lives at the **locus of the action's effect**: apply → destination archive root (`Targeted`); deletion → the **source root** where the file was lost; exclusion → archive ledger root (`LedgerRoot` — the dismissal judgment outlives the source drive it clears, and object exclusions span roots, needing one stable anchor).
  - A source-root event's receipt **never defaults to an archive root** — a deletion with no archive root must still be recorded (completeness). A single decision may emit **multiple receipts** (one per affected root); deletion tracks them in `decision_scopes.receipt_rel_path` (durable truth on disk; DB index rebuildable).
  - Exclusion's archive-ledger placement holds even though deletion's is source-local: exclusion is a DB-side judgment needing no mount, so source-local receipts would be unwritable for disconnected/suspended roots, while deletion is scan-observed and mounted by construction. **Corollary**: when the locus of effect ceases to exist (retirement), the record moves to where the surviving content lives — the book on the shelf at the archive.
- **The Contentless Law** — identity claims about empty content are vacuous. *Binding rules are the contentless-law conventions above* (the one predicate, the SQL projections, the index law, the classifier precedence, the does-NOT-apply table); the rationale per site lives in the ADR.
- **Feature-First Structure** — the subsystem is the unit of locality: subsystems are top-level modules with internal layer strata and their own CLAUDE.md; the four-layer law is unchanged and enforced by a **deny-by-default architecture test** in three rule tiers: a closed sanctioned-plumbing list (interface may hold/open `Db`, never move data through repo fns), named documented exceptions, and an exact-match, severity-tagged drift baseline matched both directions — new drift fails the build, and a repair must delete its entry. `core/` holds the spine (fundamental nouns, the provenance spine as a unit, shared repo/test machinery — entry: ≥2 consumers with rationale in hand; exit rule for single-consumer code). **`// AUDIT:` marks a counted, standing exception to a structural rule, accepted temporarily and owed a repair.** Two recognized classes: **(a) temporarily widened visibility** (count-only — the widening is its own visible artifact) and **(b) a hazard a move worsened without touching the code** — code that merely sat in a rule-exempt directory now sits in a file that *claims* a stratum, so an exception the old home never had to state becomes a false claim in the new one. A class-(b) site is marked **and pinned by a test wherever the exception is nameable** — one failing if a second instance appears, if the marker is dropped while the hazard stands, or if the site disappears, which is what the repair looks like; marker and pin are retired by the change that retires the reason for them, and the pin *and its matcher* are red-smoked. Prose alone is never a disposition for class (b).
- **Load-Bearing Mechanics — the Spoken-Once Doctrine** — **every load-bearing meaning is spoken once — as a noun, a verb, a grammar, or a law — and every other appearance is a declared projection**; re-derivation is refused at the highest available rung. Binding rules: **the assurance ladder** (unrepresentable → build-refused → pinned → canaried → named-watch), graded on two axes — *criticality sets the floor* (effectful/claim-bearing never below pinned grade; a shared mechanic takes its most critical consumer's rung), *understanding sets the ceiling* (an unripe concept is never over-frozen; assurance comes from pins over existing spellings). **The minting razor**: a concept carrying a statable invariant gets its type or owner on first sight; plain duplication keeps extract-on-second-use; recognition is structural on sight, relocation may wait for the second use. **Conjugations are single-derived and exhaustively consumed** — no `_` arms on law matches, no `..` rests on durable roll-ups. **Ownership follows what a thing is** (verb/noun/plane/law) — consumer traces verify, never decide; the exit rule governs tellers' finished output; core entry has three paths (named by a law or an ADR; data-plane membership — a closed set, canonical on sight; measured at ≥2 consumers). **The path law restated**: containment has one owner; the SQL boundary spellings are its registered projections carried as bound values. **The law roster** is the semantic register — a map of laws, never an inventory of habits; every matcher red-smoked; laws die with their reasons; claims about laws resolve to their verifying tests. **Never route around a law — route through its owner or its `// AUDIT:` exception valve.** **Recognition — speaking a load-bearing semantic once, giving it one owner, pinning it, and describing it in its owner's own CLAUDE.md — happens in the same change that discovers it**, repairs to un-lawed semantics included; a law's **reach** is a recorded field on its roster row, defaulting to the unit that recognized it. **The carrier is the owner plus its verifying test plus a roster row** — prose is a citation, never a carrier, so a named law with no owner in code is a description.
- **The Physical-Identity Law** — **path is the primary identity axis, content evidence is the arbiter, and (device, inode) is a corroborating hint, never identity**; spoken once in `scan/domain.rs::same_physical_file`, graded by `IdentityClaim` (`SamePath` vs `Relocation` — the grade follows what the claim would do when wrong, not what the file is). Binding rules: a `Relocation` refuses contentless candidates and unanchored (no-partial-hash) claims; a Moved verdict clears three independent gates — disk truth with the storage-agreement narrowing (a `NotFound` is absence only when the candidate root's current device equals the row's stored device; device is mount-presence evidence, its one sanctioned job), corroboration at `Relocation` grade, and the in-transaction re-check; corroboration filters, never nominates (inode is the only nominating thread; false Moved is the forbidden class); anything failing a gate degrades conservatively to New (+ Missing), preserving a retroactive join via the receipt chain; pairing is deterministic (`resolve_moves`), nomination-agnostic by design. Pinned, with the corroboration battery in `src/scan/CLAUDE.md`. The accepted costs and the three-way-coincidence residual live in the ADR.
