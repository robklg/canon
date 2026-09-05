# expr/ — the expression facility

The language for speaking about content in terms of facts, and everything that applies it.
One vocabulary, two halves — **filters ask**, **patterns shape** — plus the pieces that turn
either half into an answer. It is a *facility*, not a command: nothing here owns a CLI
surface, and every subsystem that takes a **selection** consumes it.

**The load-bearing promise**: a language rule is spoken exactly once, here. The rest of the
engine consumes finished results — it never re-derives a rule. When a rule looks like it
needs restating somewhere else, that is the signal something belongs in this facility.

**Modules**: `domain/key`, `domain/transform`, `domain/pattern`, `domain/vantage`,
`domain/filter`, `domain/cache`, `domain/value`, `domain/alias`, `ops/filter`,
`ops/pattern`, `ops/selection`, `ops/alias`, `repo`.

## Strata

**`domain/`** — the language as pure logic, no I/O anywhere. `key` is the key vocabulary and
how a written key is read. `transform` holds the modifiers and path accessors a value can be
put through. `pattern` parses and evaluates the shaping half, and `vantage` owns the one rule
that half cannot answer per source — see The scope vantage below. `filter` is the asking
half: the `--where` grammar, its syntax tree, the comparisons it reduces to, and the glob
matcher. `cache` is the shape a prefetched fact takes on its way to a comparison — it sits in
domain, not ops, because putting a return type in the layer above the function that returns it
is a repo-to-ops inversion. `value` resolves a key to a value for one source. `alias` rewrites
the `@name` shorthands a filter may be written with.

**`ops/`** — the language applied. `filter` runs parsed filters against real rows, against a
prefetched cache rather than a query per source. `pattern` is the shaping half's one trip to
storage — the prefetch that reads a pattern's stored facts, and the place the context-supplied
law is applied rather than merely stated. `selection` is the set of sources an operation
reports over or acts on — it lives here rather than beside any one command because exploring
and acting must resolve to the same set. `alias` is the half of alias expansion that needs the
outside world: the rewriting rules are a domain concern, reading the file they rewrite from is
this one.

**`repo.rs`** — what the language needs read out of the database before it can be evaluated:
`prefetch_facts` and `prefetch_status_data` fill the cache, `is_known_key` and `get_fact_value`
each answer one question about one key.

## The front door

`src/expr.rs` re-exports the whole of the facility from outside, in three groups it names.
The surface is pinned exactly — `subsystem_barrels_seal_to_their_pinned_surfaces`
(`tests/architecture.rs`) — so changing it means editing the pin in the same commit.
Everything else is private, and the strata are private modules: how the facility divides
itself is its own business, and rearranging it must not be visible to any caller.

- **Named externally** — what the rest of the engine actually writes down.
- **Completing the surface** — `PathAccessor` and `ModifierCall`, the *parameter* types of
  `apply_accessor` and `apply_modifier`, carried while nothing names them because the
  constraint is structural: a caller that factors out a helper taking one must be able to name
  it. Return types are absent under the parameter-type rule — inference always lets a caller
  leave one unnamed, so carrying it would record demand that does not exist. The
  `#[allow(unused_imports)]` sits on the re-export statement rather than on the file, so a
  re-export that goes dead for any other reason still says so.
- **Reached past the language** — `get_fact_value`, a point read one caller still asks
  directly. It closes when that caller is rewritten to ask the language instead.

**Every inbound reach is at the front door.** Nothing outside names `expr::domain::…` or
`expr::ops::…`, and nothing inside reaches a sibling subsystem's internals in either
direction. A sibling may not bind the facility's own name (`use crate::expr;`) — the boundary
checks cannot see through a bare `expr::` prefix.

## The scope vantage

`{scope.rel_path}` means *the shape I was looking at*. What it measures from is the **vantage**:
the deepest directory containing every scope that lies in the source's own root. One directory
scope is its own vantage, and one **file** scope is the directory that file sits in — the same
sentence, since a file's deepest containing directory is its parent; siblings share their
parent, so each scope's own name survives at the destination and two siblings cannot collide;
scopes in several roots each get their own; and it can never climb above a root, because every
scope in a root is under it (`the_vantage_never_rises_above_its_root`, `domain/vantage.rs`).

`domain/vantage.rs` owns it, and it is derived **once per run**, from an already-resolved
recorded scope the caller hands in — what "the scope" means when there is more than one is not
a question each reader answers for itself.

**Two questions it must not answer for itself are shut at its signature.** `ScopeVantage::new`
takes `&[DirectoryLocation]` — `core::domain::scope`'s measured-from register — so neither raw
manifest text nor a resolved *item* path can be folded, and both are compile errors rather than
guesses. Which root owns a prefix is the path law's question, answered once in
`core::domain::scope`/`core::ops::scope`; whether a scope names a directory or one item is the
index's, answered once at the manifest door. What is left here is this type's own question
and nothing else. It also sees only the resolution's **confirmed** scopes: a line the index could
not confirm reaches it as nothing at all, so it cannot drag a common prefix above the sibling
that did confirm.

The item half is why the signature is a `DirectoryLocation` and not a scope. Fold an item path
and `common_path_prefix` has no divergent component to drop, so the vantage becomes the file
itself, `path_strip_prefix` yields `""`, and every entry aims at the destination directory —
surfacing at `apply` as a destination conflict over a blank path, at the far end of the
pipeline from its cause. Two or more files always leave their directory behind, so the
multi-file case worked by coincidence rather than by rule; both are now answered by the one
rule. The neighbour table (`the_grain_table_answers_every_neighbour_by_one_rule`) keeps the
degenerate row where it can only be read beside the rows that make it obviously wrong.

**A vantage is constructed only where a manifest is written** — `cluster generate` and
`cluster refresh`, through one shared measure function — which records each entry's measured
path into the lock file. Pattern evaluation therefore **reads** the settled value rather than
deriving one: `EvalContext` carries `scope_rel: Option<&str>`, not a vantage, and `get_value`'s
`SCOPE_REL_PATH` arm is a lookup. The strip happens at write time, through the path law's own
`path_strip_prefix`.

**An absent measurement is refused for exactly two reasons, kept apart because only one has a
remedy**: a lock written before the measurement was recorded in it is rebuilt by
`cluster refresh`, while a manifest that records no scope has nothing to measure from and no
refresh will give it one — telling that user to refresh sends them round a loop. The facility
cannot tell them apart on its own: both are an absent field on an entry, and the difference is
a property of the **lock file**. So the caller hands it in, as `Unmeasured`
(`domain/pattern.rs`), beside the measurement itself — `EvalContext::set_scope_rel` takes both,
because half of that pair is a state no caller means to be in. Pinned by
`an_unmeasured_entry_is_refused_by_the_reason_it_is_unmeasured`, which also asserts the *other*
message does not appear.

**There is no fallback**, on purpose: a destination is the one decision a user cannot un-decide
after a move, so the alternative to refusing is inventing one — and a refusal naming a remedy
that cannot work is one step short of that.

Where it does **not** apply: `scope.rel_path` is pattern-only and never reaches a filter — a
`--where` clause has no access to a manifest's scope, which is why `SCOPE_REL_PATH` is a plain
const rather than a `BuiltinKey` variant. The facility itself never reads the manifest or the
lock; the consumer hands it the measured value.

## The context-supplied law

**A key the pattern evaluation context answers is never read from the facts table.**

Why it has to be a law rather than a habit: if a stored fact reaches evaluation wearing a
computed key's name, it wins, and where files land changes. That is the one decision a user
cannot un-decide after a move, so this sits on the destination-deciding path and is graded
accordingly.

Two owners, one on each side of the seam.

- **The set side** — `PatternFacts` (`domain/pattern.rs`), the wrapper the fetch hands to
  evaluation. Its invariant *is* the law: nothing in it is a key the context supplies. Rung:
  **unrepresentable** — the only public way to build one is `from_entries`, which is
  `#[cfg(test)]`, so the invariant is refused by the build rather than held by anyone
  remembering it.
- **The fetch side** — `is_context_supplied` (`domain/pattern.rs`), applied by
  `prefetch_pattern_facts` (`ops/pattern.rs`). Rung: **pinned**.

**The predicate is derived, not listed — but only half of it.** The built-in half reads
`BuiltinKey::is_computed`, which reads `get_builtin_value`'s own arms; a hand-written list gets
`content.hash.sha256` wrong, because it is a built-in *and* a genuinely stored fact, and
`hash`/`hash_short` the same way. The other half — `SCOPE_REL_PATH` and `OBJECT_HASH` — is
named literally, because neither is a `BuiltinKey`, and it is pinned rather than derived.
`a_key_is_computed_exactly_when_the_resolver_answers_it` (`domain/value.rs`) holds the derived
half over the whole enum and is deliberately not widened to the other two: they are not
`BuiltinKey`s, so asserting the equality over them would assert something false.

**The rule leaves the facility as a fetch, never as a predicate.** `is_context_supplied` is
`pub(in crate::expr)`; what callers get is `prefetch_pattern_facts`, which applies it for them.
Exporting a predicate for three callers to remember to call reproduces exactly the failure this
repaired, so a wrong site is made impossible rather than merely detectable — and
`the_context_supplied_set_is_spelled_only_inside_expr` (`tests/architecture.rs`) catches a
spelling appearing anywhere outside.

**Reach: this facility, argued rather than defaulted.** The asking half needs no such rule —
`resolve_fact_value` tries the built-in first and falls back to stored facts, so shadowing is
already structurally impossible there. No canon-wide reach is claimed.

Where it does **not** apply, and these are not oversights — three sites outside spell the same
bytes for three different verbs, and unifying them would be a false equality:
`facts/domain.rs` reserves an import namespace (it refuses `source.*` on the way *in* — same
bytes, opposite direction); `facts/ops/maintain.rs` protects facts from deletion (a different
set, `source.*` *and* `policy.*`); `worklist/ops.rs` routes an entity lookup (source table vs
object table — a dispatch switch, not a supply claim). They are the matcher's stated
exemptions, each with its verb named there.

One narrowing to know about: the law drops only what the context actually supplies, never the
whole `source.` namespace. A `source.*` key that is not a built-in is supplied by nothing and
shadows nothing, so it is fetched like any other stored fact — pinned by
`a_key_the_context_does_not_supply_is_fetched` (`ops/pattern.rs`). No Canon writer can create
such a row, which is why the widening is unreachable in practice and pinned anyway.

## The one pinned exception

`ops/filter.rs`'s `check_fact_compare` queries the database from the operations stratum
instead of going through the repository beside it. It carries an `// AUDIT:` marker and is
pinned by `the_facility_leaves_sql_to_its_repo_but_for_one_pinned_exception`
(`tests/architecture.rs`), which asserts the set of SQL-speaking functions in `ops/` is exactly
this one *and* that it still carries its marker. The pin fails if a second such function
appears, if the marker is dropped while the hazard stands, or if the site disappears — which is
what the repair looks like.

Moving the function whole into `repo` would not help: that puts comparison logic in the
repository instead, breaking the same rule from the other side. What retires the exception is
deriving built-in values in **one** place rather than two — here and in the value surface the
rest of the engine reads through — after which there is nothing left for it to do. The marker
and the pin are retired by the same commit that lands that repair, which is what stops the
repair landing quietly.

## Mechanics

None of these is visible from the code that trips it.

- **A barrel re-exporting the facility's own repo stratum must be fully qualified** —
  `pub use crate::expr::repo::f;`, never `pub use repo::f;`. A front door classifies as
  interface code, and the scanner's textual check reads a bare `repo::` as the shared
  repository layer.
- **`use crate::expr::repo;` is refused outright** — it binds the bare name `repo`, so a
  later `repo::…` in that file would read as the shared layer. Import the items in full
  instead of aliasing the module.
- **The front door, the barrel pin and the strata seal land in one commit**, because each of
  the three makes the other two mean something different.
- **A stratum front door's doc comment goes stale the moment a module joins it** — the `//!`
  and the `mod` line are edited in different commits, so nothing puts them in one diff.
