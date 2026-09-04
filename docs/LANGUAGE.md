# Canon Domain Language

Canon's ubiquitous language: use its terms as defined here, in code, specs, docs and
discussion. A term is defined here or it is not a domain term. Meaning, not mechanics;
fundamental nouns only.

Some terms are deliberate metaphors (the story, the book, the shelf). Two guards keep them
honest: on surfaces, a metaphor always travels with its concrete referent (a path, a file —
never a bare "consult the shelf"); on disk, artifacts stay plainly named for the reader who
has no Canon — the disk speaks plainly to strangers, the domain speaks warmly to users.

The registry join: where the engine realizes a term in one nameable owner — a type, a
predicate, a law — the entry may carry a `realized-by:` reference naming it. A pointer, never
a definition; meaning stays here, mechanics stay in the code.

## The user's world

- **Universe** — all of the user's scattered digital content Canon is aware of; the goal is to resolve it.
- **Resolution** — the outcome of content being archived or excluded; the universe shrinks from both directions until nothing uncertain remains. Two layers: **coverage** — the evidence Canon verifies — and the judgment that integrates it, which is the user's alone. Canon can prove unresolved; it never certifies resolved.
- **Activities** — the six modes of work: gather, discover, orient, assemble, archive, with triage weaving through.
- **Orientation** — mapping what a place *is* by how its content relates to the rest of the universe, before intent forms.
- **Assembly** — the iterative work of gathering related content into a coherent collection; Canon's central human work.
- **Triage** — the decision to dismiss. Narrower than everyday "triage": keeping is assembly's claiming, not triage.
- **Collection** — a coherent set of related content being assembled toward a canonical home.
- **Canonical home** — the proper destination content earns in the archive; final in custody, not frozen in form — arrangement may mature through later curation passes.
- **Custody** — the state Canon owes every kept item: identified, safely in the archive, accounted for, its decisions recorded. Canon's mission ends at custody.
- **Curation** — the optional, judgment-driven work above custody: selecting the best, naming events, forming collections for enjoyment. The user judges; Canon carries judgments (as facts) and moves content in bulk — it never judges, and never entertains.
- **Residual** — what remains unaccounted for: at the universe level, after assembly claims and triage dismisses; in a survey comparison, content found only here, not at the reference.

## The engine's nouns

- **Root** — a scanned folder with a role: source or archive. A folder, period: what holds it — a whole drive, a mount, a corner of one — is the user's business; Canon knows only the path and what it observes there.
- **Source** — a file on disk (root + relative path); say sources, not files.
- **Object** — content identified by hash; many sources can map to one object.
- **Perceptual identity** — a second, probabilistic layer of content identity: visual equivalence across byte-different objects. It informs and points — it never carries exact identity's guarantees (the trust ceiling); direction not yet chosen.
- **Fact** — key-value metadata about a source or an object.
- **Enrichment** — external tools computing facts; Canon provides the infrastructure, the tools the knowledge.
- **Expression** — the language for speaking about content in terms of facts. One vocabulary, two halves: the **asking half** (filters) picks content out of the universe; the **shaping half** (patterns) shapes where kept content lands. An expression selects and shapes; it never warrants — no filter certifies resolution.
- **Expression facility** — the language's one home in the engine: vocabulary, transforms, both halves, aliases — and Selection, the language applied — live behind it. A language rule is spoken exactly once, there; the rest of the engine consumes finished results, never re-derives a rule.
- **Scope** — the path-prefix boundary an operation works within.
- **Selection** — the set of sources an operation reports over or acts on; the expression language applied within a scope. Exploring and acting share it, so what you act on is what you saw.
- **Exclusion** — consciously dismissing a source (or an object everywhere) from consideration; triage's letting-go. The safe dismissal: nothing is destroyed, only released from attention — and it can be restored. Being redundant beside the archive is one reason to let go, not the defining one.

## Fates & provenance

- **Standing** — where content presently sits in the resolution story: indexed, present, covered, contentless, missing, archived, excluded — and, with its root, suspended (the door closed on its root — see Suspension). The present-tense fact about a source or a place. Transitions are changes of standing; the trail records the changes, the provenance card states standing now. A counterpart's standing — archived above merely-present above suspended — is what makes acting on a finding safe.
- **Suspension** — the user's own act of closing the door on a root: everything about it closed by default, kept safe, until the same hand opens it again. Root grain. The act speaks only of the user's attention: whether the root's path answers on disk right now is a separate fact, observed and never commanded, and everything inside keeps exactly the standing it had — the content's resolution story pauses where it stood and resumes there. The closed door permits exactly four things, and nothing else: **opening it** — unsuspension, the user's own hand, where every resumption begins, retirement's ceremony included; **seeing it** — the pause is always visible: stated when the user names the parked place (never silence, never a widened view, never a false "empty"), counted where its absence would mislead ("including N on suspended roots"), listed on asking; **remembering** — knowledge Canon already holds still reads (the trail, the story, the notes, the books): a pause of attention doesn't make Canon forget; **testifying** — its copies still ground claims about other places: custody stands (parking never un-archives), redundancy points and never warrants a dismissal — stated, never headlined. Everything not on this list meets the closed default: an act is refused by name with the way back stated; a view sets the root aside.
- **Covered** — an evidence-standing: content verified present in the archive by identity, though not archived from here. A containment fact — precise about content, silent about shape and worth (a folder can be covered object-by-object yet stand verbatim nowhere); evidence for the resolution judgment, never the judgment itself.
- **Unresolved** — present content carrying no resolution evidence: neither covered nor excluded. The one side Canon can judge: NOT READY is provable; "ready" never is — the asymmetric verdict.
- **Contentless** — the standing of an empty source: all shape, no content. Content identity has nothing to identify, so coverage can say nothing about it — never covered (any empty file anywhere would hollowly cover them all), never unresolved (there is no content to lose), never blocking. It is stated, never silent, and it travels with its place: carried where its folder goes, resolved with the place's own fate. Surfaces say the plain referent — empty files — beside the word.
- **Fate** — a terminal outcome for content: archived, excluded, or deleted; each leaves a durable, decision-linked record.
- **Transition** — the change in a source's standing that a decision performed or observed, from first being indexed through moves and modifications to the terminal fates; the per-item unit a receipt records. The *what* of provenance — stable across the command surfaces (the *how*) that produce it. Canonical vocabulary: *indexed*, *moved*, *modified*, *archived*, *excluded*, *restored* (exclusion undone — dismissed content returned to consideration), *deleted*. Receipts record the fates and restored — the standing changes no re-scan could recover; moves and modifications are re-observable and live in the trail.
- **Decision** — any effectful operation Canon records: what was done, to what, when, optionally why.
- **Observed vs. performed** — the two postures of a decision: Canon performing a change, or Canon observing a change the world made. The trail records both; an observation must never read as an act.
- **Receipt** — the durable on-disk record of a decision's per-item effects, placed at the locus of the action's effect.
- **Ledger** — the on-disk home of receipts; disk is the truth, the database a rebuildable index.
- **Decision trail** — the queryable history of decisions.
- **Crossing** — a recorded movement across the boundary of a place in view: what arrived here from elsewhere, what left here for elsewhere — the trail's relation between two places, read from the decision record, with an origin end and a destination end. Distinct from the sweep's **counterpart**, which binds places by where matching content *is*; a crossing binds them by what *moved*, when, and why. realized-by: `trail::domain::placement::row_aspect`.
- **Provenance card** — the present-tense composition statement of an archive location: what stands there *now* and where each part came from, read from the last transitions of the surviving sources. A statement of state, not an event log — deleted content is absent (the trail holds the loss), curation renames don't erase origin, and origin-unknown content shows plainly as first-indexed-here. Distinct from the trail ("what happened here") and from item lineage ("what happened to this one thing" — direction not yet chosen).
- **Retirement** — closing the book on a fully resolved root: its complete story compiled into the book, placed on the shelf, its index removed, whatever held it free to go. Its two movements: the **bind** — the story compiled, verified, placed — and the **release** — the index removed; between them, the user inspects the standing book.
- **Story** — a root's complete narrative: everything it ever held, each part's standing and fate, the decisions and their reasons. The story exists all along — the trail and the ledger tell it live; retirement binds it into the book. The name is a promise: wherever Canon tells a story, it must *read* as one — self-explanatory, needing no index. How Canon tells a story will evolve with its versions; the promise doesn't — and a story once told stands alone: reading it never needs the teller.
- **Book** — the bound story of a retired root: what it held, the fate of every part, the reasons — readable forever, without Canon. The register is completion, never mourning: a finished story, not an obituary. "Retirement artifact" names the same thing mechanically; in the domain, say the book. Bound history, distinct from the ledger — the live record home of roots still in play. A book once bound is never rewritten — retirement is letting go, and rewriting the story would unmake it; improvements bind forward into future books. Repair of an actual defect is the one reserved exception: healed as a repair, never improved as a rewrite.
- **Shelf** — the visible place in the archive where the books stand; browsing it reads the retired fleet — finished stories, counted, the north star's scoreboard. A shelf, not a graveyard. A place and a reading, not a command name. Written once per book, read forever: the shelf is immutable history.

## Workflow & interface

- **Survey** — outward-looking comparison from a shaped selection; affinity is its assembly-oriented lens.
- **Overlap group** — a maximal set of disjoint locations bound by substantial shared content; the unit of folder-level reduction. Never "cluster" — that word belongs to assembly.
- **Reduction opportunity** — an overlap group framed for dismissal (the triage lens): what acting returns, the counterpart's standing, what the residual risks. The structural thing is the group; the opportunity is what it means when triage is the question.
- **Sweep** — the finder gesture: one fresh, universe-wide pass surfacing the ranked places where one decision resolves the most. Survey's unanchored counterpart — the sweep finds, survey judges, the user decides, the ceremony records. A finding tips toward dismissal *or* claiming; the sweep points, it never verdicts.
- **Subject** — the place a relation statement is about: the side being read for resolution. A single finding's headline is its subject.
- **Counterpart** — the place bound to a subject by one relation claim: the where-else of the matched content. A role in a binding claim — not survey's related locations, which are a discovered neighborhood; the handoff maps a counterpart onto survey's `--other`.
- **Hub** — a counterpart shared by many subjects — the star shape real duplication takes. A hub reads from the shared side: its headline is the counterpart, not any one subject.
- **Note** — a timestamped, location-level annotation (breadcrumb); Canon holding awareness for the user. Notes hold *thoughts* — note-to-self, questions, hunches ("what did I think about this?") — never the record of actions, which is the decision trail's job ("what did I do?").
- **Manifest** — the human-editable declaration of an intended archive operation; the ceremony of settling.
- **Ceremony** — the structured moment around an effectful action; awareness, not anxiety.
- **Assembly artifact** — the future working artifact holding live assembly state; direction not yet chosen.
