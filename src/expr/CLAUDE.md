# expr/ — the expression facility

The language for speaking about content in terms of facts, and everything that applies it.
One vocabulary, two halves — **filters ask**, **patterns shape** — plus the pieces that turn
either half into an answer. It is a *facility*, not a command: nothing here owns a CLI
surface, and every subsystem that takes a **selection** consumes it.

**The load-bearing promise**: a language rule is spoken exactly once, here. The rest of the
engine consumes finished results — it never re-derives a rule. When a rule looks like it
needs restating somewhere else, that is the signal something belongs in this facility.

**Modules**: `domain/key`, `domain/read`, `domain/problem`, `domain/transform`,
`domain/pattern`, `domain/vantage`, `domain/filter`, `domain/glob`, `domain/bind`, `domain/print`,
`domain/cache`, `domain/value`, `domain/alias`, `ops/filter`, `ops/bind`, `ops/pattern`, `ops/selection`, `ops/alias`,
`ops/value`, `repo`.

## Strata

**`domain/`** — the language as pure logic, no I/O anywhere. `key` is the key vocabulary and
how a written key is read. `read` is the reader — see The reader below — and `problem` what
it records where a text cannot be read, and how a refusal draws it. `transform` holds the modifiers and path accessors a value can be
put through. `pattern` parses and evaluates the shaping half, and `vantage` owns the one rule
that half cannot answer per source — see The scope vantage below. `filter` is the asking
half: the syntax tree the reader builds and the comparisons it reduces to. `glob` is a glob's
own grammar and its matcher. `bind` reads each clause's value as its key's type — the modifier table, the
bare-value classes, the refusal and its ways out — and `print` is the printing rule. `cache`
is the shape a prefetched fact takes on its way to a comparison — it sits in domain, not ops,
because putting a return type in the layer above the function that returns it is a
repo-to-ops inversion. `value` resolves a key to a value for one source. `alias` rewrites
the `@name` shorthands a filter may be written with.

**`ops/`** — the language applied. `bind` reads the stored keys' types and binds a filter or
a report's keys, or refuses them. `filter` runs bound filters against real rows, against a
prefetched cache rather than a query per source. `pattern` is the shaping half's one trip to
storage — the prefetch that reads a pattern's stored facts, and the place the context-supplied
law is applied rather than merely stated. `selection` is the set of sources an operation
reports over or acts on — it lives here rather than beside any one command because exploring
and acting must resolve to the same set. `alias` is the half of alias expansion that needs the
outside world: the rewriting rules are a domain concern, reading the file they rewrite from is
this one. `value` reads a stored key's one value for a batch of sources, and any key's as an
expression reads it (`fetch_named_values`, for `worklist --emit`).

**`repo.rs`** — what the language needs read out of the database before it can be evaluated:
`prefetch_facts` and `prefetch_status_data` fill the cache, `fetch_source_row` reads one source's
row, and
`fetch_key_rows` reads a key's rows per source, on the source and on its content.

## The front door

`src/expr.rs` re-exports the whole of the facility from outside: the items the rest of the
engine writes down. The surface is pinned exactly — `subsystem_barrels_seal_to_their_pinned_surfaces`
(`tests/architecture.rs`) — so changing it means editing the pin in the same commit.
Everything else is private, and the strata are private modules: how the facility divides
itself is its own business, and rearranging it must not be visible to any caller. No item is
carried only as a parameter type today; under the parameter-type rule one would join when an
exported item takes a type nothing outside names, and return types never do.

**Every inbound reach is at the front door.** Nothing outside names `expr::domain::…` or
`expr::ops::…`, and nothing inside reaches a sibling subsystem's internals in either
direction. A sibling may not bind the facility's own name (`use crate::expr;`) — the boundary
checks cannot see through a bare `expr::` prefix.

## The reader

**What may stand at a position decides what is read there**, and where it cannot be read, that
position's sentence follows (`domain/read.rs`: `read_filter`, and `read_keys` for `--key` and
`--group-by`). A keyword is one only in exact capitals and only where one may stand; elsewhere
the word is a value or a key's name. A status word (`archived`, `hashed`, `excluded`, `enriched`)
is one only unquoted and in lower case, and takes `?` alone, refused with an operator, an accessor
or a modifier; quoted or in other case it is a key name (`"archived"?` asks about
`content.archived`; `status_words_count_only_in_lower_case_and_only_with_a_question_mark`). A key is read by one grammar, `domain/key.rs::read_key`,
whose caller says what may follow it. A problem is recorded where it is met, as a byte range,
and reading goes on: after a problem in a value at the value's end, after any other at the next
`AND` or `OR` of the same group or the `)` closing it, quotes and closed parentheses
stepped over. Reading **stops** only where everything after a point is inside it — an unclosed quote,
`[` or `|bucket(` — and nothing is recorded after a stop. The reader words nothing that depends
on a key's type: a value that cannot stand bare carries its first such character
(`NeedsQuotes`, a wildcard outranking any other), and binding words it. `domain/problem.rs`
draws a problem: the typed line that contains it, a caret counted in terminal columns, the
sentence, its rule line, and a fix from a closed list (`Fix`). Pins:
`a_keyword_is_a_keyword_only_where_one_may_stand`,
`a_grammar_problem_resumes_at_the_next_and_or_or_closing_paren` and
`reading_stops_at_an_unclosed_quote`.

**A glob** after `~` or `!~` is read once into pieces by `domain/glob.rs::read_glob`, so quoted
text stays literal; it ends where any value ends, and a set a space reaches is unclosed. Text
compares as stored; `?` and a set member take one visible character (an extended grapheme
cluster), only from where one starts. Each unquoted part holding a character a plain value
cannot hold records its first; binding words it, or the reader where reading stops later in the
glob, both through `needs_quotes_problem`. `!~` reads as `NOT` of the glob. Pins:
`every_sentence_of_the_glob_grammar`, `inside_a_set_a_quote_is_a_member_and_a_space_ends_it`,
`question_mark_takes_one_visible_character`, `the_matcher_stays_polynomial`.

**Who reads through it**: binding (`--where`, `facts --key` and `--group-by`, a manifest's
recorded query), the printing rule and alias sorting. Its tree is `domain/filter.rs`'s `Expr`.

## Binding

**The binding law**: between reading and evaluation every `--where` clause reads its value once,
as its key's type, and a value that cannot mean anything for the key is refused by name before
anything is selected — never a silent false that `NOT` turns into every row. Owner
`ops/bind.rs` (`read_filters`), over the pure `domain/bind.rs`. `apply_filters` takes only
`BoundFilters`, which only `read_filters` makes, so an unbound filter cannot run; `read_report`
reads a `facts` report's `--key`, `--group-by` and `--where` the same way. A text with a problem
the reader met never runs, though what of it could be read is bound, so its causes are named too
(`a_filter_with_a_reading_problem_never_runs`). A key's type is read
from all its rows (`core::repo::fact::key_types`), so parking, scope and presence never move it.
**Text is text**: against a text key, a bare value shaped like a number or a time is refused, and
quoted it is text; against a number or time key, a quoted value means the same as a bare one.
A `source.` name that is no source fact is an unknown key wherever a key stands, `?` and report
keys included (`a_source_name_that_is_no_source_fact_is_an_unknown_key_wherever_it_stands`).
Pins: `a_value_that_cannot_mean_anything_is_refused_before_selection` (the verifier),
`every_negated_operator_selects_what_its_not_form_selects` (`!=` is `NOT =`, a source with no
value included) and `a_type_that_changes_mid_run_refuses_the_run`.

**A key's type changing while a run reads it** is checked one way, `domain/bind.rs::check_read_as`,
by the filter and by a `facts` report: a key is read as its one type, as having no facts, or as
any type where no value is compared or read through a chain (a bare `?`, a bare report key), and
a value that is not what the key was read as stops the run as an answer. A report key carries it
as `ParsedFactKey`'s `read_as`. Pinned on the filter by `a_type_that_changes_mid_run_refuses_the_run`
and `a_key_that_gains_its_first_facts_mid_run_refuses_the_run`, on the report by
`a_type_that_changes_mid_report_refuses_the_report`.

What a modifier reads and gives is one table, `result_of`, read by binding and agreeing with the
chain. Every problem and cause is collected into one `ExpressionRefused`, its causes matched with
no `_` arm (a command's refused key names join it, the filter's first line leading:
`unknown_keys`, `join`), and each is drawn under the typed line it sits in, a caret under the value, the
key's name, the `|` of a modifier or the `[` of an accessor; a key of two types keeps its
`clause: statement` line. Where the key gives text, an unquoted value's character that needs
quotes, or a wildcard after `=`, `!=` or in a list, is binding's sentence; any other type reads
the value as that type. A glob binds only where the key gives text or a path; for any other
result its one cause is the key's type statement (`Gives::statement`, one writer), so no
comparison meets a glob. Ways out rewrite only what they change. **A way out makes what it rewrites bind**: a way out is one value, `Way` — what it
does and what it rewrites to — and every one that rewrites is bound before it is shown, a
filter's as a clause, a report's as a key, and dropped if it would be refused again
(`keep_ways_that_bind`). Pinned at the filter by `every_way_out_binds_the_whole_clause`, at the
report by `a_report_key_is_read_and_checked_like_a_filter_key`. A fix from the closed list
(`Fix`) is checked the same way: the quoted value, or the glob and the exact text, both or
neither, read back and bound before it is shown; a quoted name only where it names a key with
facts (`every_fix_shown_reads_back_and_binds`, `a_quoted_name_is_offered_only_for_a_key_with_facts`). **Held by discipline**: a builder
offers a rewrite through `Way::to`, never as hint text, and a new caller of `bind_key` calls the
check. The conversion pointer rewrites nothing and is gated where it is built. A cause about what a key is — two types, or a
type that changed, or a key that gained its first facts, while the run read it — is an answer and prints without `Error:`
(`ExpressionRefused::is_answer`); every other cause is input Canon cannot read. Evaluation compares
text by code point, a glob through its matcher, numbers with a tolerance, times through the time noun's period table, a
modifier's part as a whole number, a bucket's label by equality.

**Where it does not apply**: a pattern, which reads its keys through `parse_key_and_accessor`
and meets a wrong-type modifier as a gap when `apply` evaluates it; a manifest's recorded query
is bound like `--where`, read as specified with no aliases, its refusal's first line saying so
(`ExpressionRefused::into_specified`). It holds what expansion produced, so it is read as expanded
text: an `@name` where a key belongs is named as an alias by the reader, which also meets one
left inside an alias's own value; after `=` an `@` needs quotes, and after `~` it is part of the
glob. The alias name has one
spelling, `domain/alias.rs::alias_name_at`.

## The printing rule

`domain/print.rs` writes a value or a key so that, typed into `--where`, it reads back as
itself: bare where the reader reads the bare text back the same, double-quoted otherwise. The
writer asks the reader — the filter reader (`reads_as_one_bare_value`, `reads_as_one_bare_key`),
alias expansion's `@` test and the binder's bare-value class — and keeps no character list (`every_printed_value_reads_back_as_itself`, and every
printable one- and two-character value checked against the reader). A refusal's clauses and
ways out print through it. So does the statement that keys hold two types and its way out
(`mixed_type_statement`, `mixed_type_way_out`), which `facts`' listing, `cluster`'s summary and
a manifest's comments read: the key prints through the rule, and its `canon facts convert`
command is quoted again for the shell. **Where it does not apply**: a `facts` row and a pattern's
folder name print text as it is, number- or time-shaped text bare.

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

**An absent measurement is refused for exactly two reasons, and each states its own.** A lock
written before the measurement was recorded in it is one; a lock built while the manifest
declared no scope is the other — the lock's fact both times, never the manifest's, which may
have declared a scope since. The facility cannot tell them apart on its own: both are an absent field on an
entry, and the difference is a property of the **lock file**. So the caller hands it in, as
`Unmeasured` (`domain/pattern.rs`), beside the measurement itself — `EvalContext::set_scope_rel`
takes both, because half of that pair is a state no caller means to be in. Pinned by
`an_unmeasured_entry_is_refused_by_the_reason_it_is_unmeasured`, which also asserts the *other*
message does not appear.

**Neither message names a remedy**, which is the rule rather than an omission. A refresh does
rebuild an old lock, but reaches an applicable manifest only where the recorded scope confirms
something to measure from — a fact about the manifest this facility never sees. The reason is
what it can warrant; the remedy belongs to whoever holds both facts.

**There is no fallback**, on purpose: a destination is the one decision a user cannot un-decide
after a move, so the alternative to refusing is inventing one — and a refusal naming a remedy
that cannot work is one step short of that.

Where it does **not** apply: `scope.rel_path` is pattern-only and never reaches a filter — a
`--where` clause has no access to a manifest's scope, which is why `SCOPE_REL_PATH` is a plain
const rather than a `Key`. The facility itself never reads the manifest or the
lock; the consumer hands it the measured value.

**Whether a pattern names that key is the facility's answer, not the consumer's**
(`Pattern::names_scope_rel_path`). A consumer needs it to tell two expansion failures apart — a
scope it has not got, and a fact no source carries — which take opposite remedies; without it the
advice is wrong for half the readers. Answered here because the alternative is a consumer matching
on the key's spelling or on the refusal's wording, which is a second copy of a language rule
living outside the language.

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

**The predicate is derived, not listed — but only half of it.** The derived half reads
`Key::is_computed`, which reads `source_row_value`'s own arms; a hand-written list gets
`hash`/`hash_short` wrong, built-in keys the source row cannot answer. The other half —
`SCOPE_REL_PATH` and `OBJECT_HASH` — is named literally, because neither is a `Key`, and it is
pinned rather than derived. `a_key_is_computed_exactly_when_the_resolver_answers_it`
(`domain/value.rs`) holds the derived half and is deliberately not widened to the other two:
asserting the equality over them would assert something false.

**The rule leaves the facility as a fetch, never as a predicate.** `is_context_supplied` is
`pub(in crate::expr)`; what callers get is `prefetch_pattern_facts`, which applies it for them.
Exporting a predicate for three callers to remember to call reproduces exactly the failure this
repaired, so a wrong site is made impossible rather than merely detectable — and
`the_context_supplied_set_is_spelled_only_inside_expr` (`tests/architecture.rs`) catches a
spelling appearing anywhere outside.

**Reach: this facility, argued rather than defaulted.** The asking half needs no such rule —
`resolve_fact_value` tries the computed key first and falls back to stored facts, so shadowing is
already structurally impossible there. No canon-wide reach is claimed.

One narrowing to know about: the law drops only what the context actually supplies, never the
whole `source.` namespace. A `source.*` key that is not a source fact is supplied by nothing and
shadows nothing, so it is fetched like any other stored fact — pinned by
`a_key_the_context_does_not_supply_is_fetched` (`ops/pattern.rs`). No Canon writer can create
such a row, which is why the widening is unreachable in practice and pinned anyway.

## The per-source comparison reads its row through `repo`

`ops/filter.rs` speaks no SQL. A key the source row computes, compared or tested for existence,
is resolved per source, and the row it is resolved from — the source's attributes — comes from
`repo.rs`'s `fetch_source_row`. Every other key, the content hash included, is compared on the source's one
value, read from the prefetched cache (`FactCache::value`). The comparison stays in `ops`;
moving it into `repo` would put comparison logic in the repository instead.

## Source-row values are derived once

`source_row_value` (`domain/value.rs`) is the one place a value is derived from a source row,
a built-in key's through a source fact's arm where it is a short name for one (`size`, `ext`), which
`BuiltinKey::source_fact` alone says — the size buckets, the year grouping and the save-time cause read it too; `--where`,
`facts --key`, `--emit` and `{source.*}` read it. Readers
split computed from stored by the key (`Key::is_computed()`, pinned equal to the resolver's answer on a readable source), never
by its `None` or by listing: a computed key the row cannot answer has no value. Outside patterns, `hash` reads as `content.hash.sha256` and `hash_short` as
`content.hash.sha256|short` (`BuiltinKey::reading`); patterns keep the lock's `object.hash`.
Pinned per reader. Not bound: pattern aliases' own modifier chains.

**The key-name law**: every command taking a key's name reads it through `read_key_name`
(`domain/key.rs`) — the source fact, built-in key and content fact it names — and applies its
own rule: an expression and `--emit` take them in that order, import and `facts delete` the
content reading alone. A consumer then decides by the key read (`ParsedFactKey::key`), never
the text, since quoting never changes which key. Verifier
`a_key_is_read_by_the_name_import_stored_it_under` (`facts/domain.rs`); `--emit`'s leg is in
`worklist/ops.rs`. Not patterns (`pattern_key`).

## The source-value law

**A source has one value for a stored key**: its content's fact; the source's own only where the
content has none. `source_value` (`domain/value.rs`) decides it. Readers reach it
through `FactCache::value` (the filter), `fetch_source_values` (`ops/value.rs`: patterns, `facts`
reports, and `worklist --emit` through `fetch_named_values`), or `source_values` (`domain/value.rs`), which `cluster generate`
hands the facts it already holds for a source; none outside pairs the two rows or picks
between them, and which row a fact sits on is the fact noun's (`FactEntry::is_on_source`). Why:
a filter matching either row selected sources the report beside it never counted.

Pinned by `one_source_one_value_in_filter_report_pattern_and_worklist`, which asks the four
surfaces, an empty file among its sources; `cluster generate`'s facts comment by its own
`the_coverage_comment_reads_one_value_per_source`.

Where it does **not** apply: a key's type (`core::repo::fact::key_types` reads every row); `?` on a
hash key, true too for a source linked to content with no stored hash; a computed key, never
read from the facts table.

## The clock is a parameter

Times read through the frame rule on the clock the facility is handed — `SelectionParams.clock`,
`EvalContext`'s `clock`, the display and report functions' `zone` — never the machine's; a
manifest's run is handed the zone the manifest or its lock records. A modifier answers `Modified`: a value, or
no value and its `NoValueCause` (too coarse, text reading as no time or number, no segment),
which every reader treats as a missing fact, `?` included; a pattern refuses a too-coarse
time with `PatternNoValue`, a typed cause the archive's remedy reads, never its sentence. A typed
`--where` time is translated to that zone's local time once per run.

A modifier's result is typed — a number part a number; `|date`, `|yearmonth` and `|datetime` a
time with no offset; `|time` a time of day, which has no date — and printed one way,
`domain/transform.rs::rendered`, which patterns place, `facts` reports and a later text modifier
reads.

**A time of day has no date.** `|time` gives a reading on a reference day, so only `|hour`,
`|minute`, `|second` and `|time` may read it (`Modifier::reads_a_time_of_day`, one rule):
binding refuses the rest and the chain errors on them, so no pattern places a date read off
that day (`the_table_and_the_chain_agree_on_a_time_of_day`). A text modifier after `|time`
reads its printed form, so `|time|lowercase|year` reads text and has no value.

**A run says whether it read an instant on the clock** (`Selection.read_an_instant`,
`EvalContext::read_an_instant`), by one rule, `domain/transform.rs::reads_an_instant`: an
instant compared or printed bare or through a time modifier, text a time modifier reads as one,
and a typed time with an offset; never a clock reading, a number, an accessor or a modifier refusing a time. Pinned by
`the_instant_rule_reads_only_what_reads_a_clock`; a facility answer, with no roster row.

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
