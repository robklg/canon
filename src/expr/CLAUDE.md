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
`domain/row`, `domain/cache`, `domain/value`, `domain/alias`, `domain/statement`, `ops/filter`, `ops/bind`, `ops/pattern`, `ops/selection`, `ops/alias`,
`ops/value`, `ops/notes`, `repo`.

## Strata

**`domain/`** — the language as pure logic, no I/O anywhere. `key` is the key vocabulary and
how a written key is read. `read` is the reader — see The reader below — and `problem` what
it records where a text cannot be read, and how a refusal draws it. `transform` holds the modifiers and path accessors a value can be
put through. `pattern` reads, checks and evaluates the shaping half — see A pattern below — and
`vantage` owns the one rule that half cannot answer per source — see The scope vantage below. `filter` is the asking
half: the syntax tree the reader builds and the comparisons it reduces to. `glob` is a glob's
own grammar and its matcher. `bind` reads each clause's value as its key's type — the modifier table, the
input types, the refusal — and `print` is the printing rule. `row` is a report's rows — see The
read-back round trip below. `statement` is the statement grammar — see The statement grammar below. `cache`
is the shape a prefetched fact takes on its way to a comparison — it sits in domain, not ops,
because putting a return type in the layer above the function that returns it is a
repo-to-ops inversion. `value` resolves a key to a value for one source. `alias` checks
the aliases file's entries and writes the text a manifest records for a filter that used one.

**`ops/`** — the language applied. `bind` reads the stored keys' types and binds a filter or
a report's keys, or refuses them. `filter` runs bound filters against real rows, against a
prefetched cache rather than a query per source. `pattern` is the shaping half's trips to
storage — `read_pattern`, binding a pattern against the stored keys' types, and the prefetch that
reads its stored facts, the place the context-supplied law is applied rather than merely stated. `selection` is the set of sources an operation
reports over or acts on — it lives here rather than beside any one command because exploring
and acting must resolve to the same set. `notes` is what a selecting run recorded beside its
evaluation and the lines an acting command's plan states from it — see Selection notes below. `alias` reads the aliases file, the first time a reading meets
an alias, and hands its entries to the check. `value` reads a stored key's one value for a batch of sources, any key's as an
expression reads it (`fetch_named_values`, for `worklist --emit`), a report key's typed
(`fetch_row_values`), and how many sources have a value for each stored key
(`count_values_by_key`).

**`repo.rs`** — what the language needs read out of the database before it can be evaluated:
`prefetch_facts` and `prefetch_status_data` fill the cache, `fetch_source_row` reads one source's
row,
`fetch_key_rows` reads a key's rows per source, on the source and on its content, and
`fetch_all_key_rows` every stored key's, one source at a time.

## The front door

`src/expr.rs` re-exports the whole of the facility from outside: the items the rest of the
engine writes down. The surface is pinned exactly — `subsystem_barrels_seal_to_their_pinned_surfaces`
(`tests/architecture.rs`) — so changing it means editing the pin in the same commit.
Everything else is private, and the strata are private modules: how the facility divides
itself is its own business, and rearranging it must not be visible to any caller. One item is
carried only as a parameter type, `NoteMarks`: `SelectionNotes::of` takes it and no consumer
names it, so it rides with `#[allow(unused_imports)]` under the parameter-type rule; return
types never join that way. `fetch_source_values` rides the same way for another reason: its one
reader outside is the source-value law's verifier, a test.

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
whose caller says what may end a bare name: in a filter a space, where a key stands alone only
what may follow it there (`[` or `|`, and `,` in `--group-by`), so a space inside is the name's.
There a `|`, or a `[` where an accessor may follow, after spaces is still the key's, after a name,
a quoted name or a key alias (`spaces_before_transforms`); the forgotten-comma line keeps a spaced
`|` or `[` with its part. A problem is recorded where it is met, as a byte range,
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
recorded query, a pattern's `{…}`), the printing rule and the aliases file's check. Its tree is `domain/filter.rs`'s `Expr`.

**An alias** is `@` and an alias name where a condition starts or a key stands; elsewhere `@`
follows its position's rule. A command hands `ExpressionText` (its texts, and where their aliases
come from); the file is loaded at the first use, which stops a reading made without it, and that
text is read again with it. A
filter alias reads as its value's tree, and only `AND`, `OR`, `)` or the end may follow it; a key
alias reads as its key, the key grammar continuing where that key ended. A value is one key or one
filter, with no alias (`read_alias_value`); the file check, `domain/alias.rs::check_aliases`,
reads nothing from the library and draws no fix, and one broken entry refuses every use. Places
inside an alias stay in the value's coordinates; drawing puts the caret under the use, prefixes
"in alias `@x`: " and shows nothing that rewrites text (`BindCause::at_use`, pinned by
`every_place_inside_an_alias_points_at_its_use`). The alias name has one spelling,
`domain/alias.rs::alias_name_at`.

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
**Text is text**: against a text or path key, a bare value that reads as a number, a time or a time
of day is refused, and quoted it is text — what it reads as, never how it looks (`input_type`);
against a number or time key, a quoted value means the same as a bare one. A typed number is read
form first, fit second (`core::domain::number`): held exactly as written, or refused.
A `source.` name that is no source fact is an unknown key wherever a key stands, `?` and report
keys included (`a_source_name_that_is_no_source_fact_is_an_unknown_key_wherever_it_stands`), and
so is a key with no facts wherever a value is read, a report key included; only `?` binds one.
Pins: `a_value_that_cannot_mean_anything_is_refused_before_selection` (the verifier),
`every_negated_operator_selects_what_its_not_form_selects` (`!=` is `NOT =`, a source with no
value included) and `a_type_that_changes_mid_run_refuses_the_run`.

**A key's type changing while a run reads it** is checked one way, `domain/bind.rs::check_read_as`,
by the filter and by a `facts` report: a key is read as its one type, as having no facts (a `?`
alone), or as any type where no value is compared or read through a chain (a bare `?`, a bare
report key), and
a value that is not what the key was read as stops the run as an answer. A report key carries it
as `ParsedFactKey`'s `read_as`. Pinned on the filter by `a_type_that_changes_mid_run_refuses_the_run`
and `a_key_that_gains_its_first_facts_mid_run_refuses_the_run`, on the report by
`a_type_that_changes_mid_report_refuses_the_report`.

What a modifier reads and gives is one table, `result_of`, read by binding and agreeing with the
chain; a `?` on a key with no facts walks it too, its first step from any type. Every problem and cause is
collected into one `ExpressionRefused`, its causes matched with no `_` arm (`unknown_keys` and
`join` add a command's refused names). Each is a `Problem` drawn under its typed line, a caret under
the value, the key's name, a modifier's `|` or an accessor's `[`: one sentence naming the key as
typed up to the problem (`Gives::statement`, one writer), the rule line, and a fix from the closed
list (`Fix`); a key of mixed type keeps its `clause: statement` line and its counts. Where the key
gives text, a character that needs quotes, or a wildcard after `=`, `!=` or in a list, is binding's
sentence; any other type reads the value as that type. A glob binds only where the key gives text
or a path, its one cause otherwise the type statement. **A fix shown reads back and binds**: each
fix is read back and bound before it is shown, a filter's as a clause, a report's as a key
(`keep_fix_that_binds`), and a `--group-by` item of known keys with no commas between them as a
key list (`Fix::GroupingKeys`); a filter's quoted name only where it names a key with facts (`every_fix_shown_reads_back_and_binds`,
`a_quoted_name_is_offered_only_for_a_key_with_facts`). No
other filter is ever offered. A type mismatch on a key of type text carries the bottom line
(`Conversion`), printed once, last, with each stored key's convert command
(`the_conversion_line_is_printed_once_naming_each_stored_key`). **Held by discipline**: a new caller
of `bind_key` calls the check, and a new reader of a key list `keep_grouping_keys_that_bind`. A cause about what a key is — mixed type, a type that changed or a
key that gained its first facts while the run read it — is an answer, printed without `Error:`
(`ExpressionRefused::is_answer`). Evaluation compares text by code point, a glob through its matcher, numbers
exactly, times through the time noun's period table, a modifier's part as a whole number, a bucket's
label by equality.

A manifest's recorded query is bound like `--where`, read as specified with no aliases, its refusal's first line saying so,
and an `@name` where an alias would stand refused by name. What a manifest records is
`BoundFilters::recorded`, built in the reading that bound the filter: each use replaced by a
filter alias's value in parentheses or a key alias's key, so the recorded query selects what the
typed text selected (`the_recorded_text_selects_what_the_typed_text_selected`).

## A pattern

**The one key grammar** reads every `{…}`: `domain/key.rs::read_key` at its own position,
`Grammar::Pattern`, which only the `}` may follow, so a key reads the same in a filter, a report
key and a pattern, the `content.` default included (verifier
`a_key_reads_the_same_in_a_filter_a_report_and_a_pattern`). `read_pattern_text` reads the literal
text — a `}` with no `{`, a leading or trailing `/`, `//`, a whole name `.` or `..` — and each
`{…}`, read as `--key` reads its argument: a space inside a bare name is the name's, spaces at
the ends of the `{…}` are not, and a quoted name may hold a `}`. `@name` is refused, a status word is not a key, and `scope.rel_path` binds as a
path. **Binding** is `ops/pattern.rs::read_pattern`, through `bind_pattern_key`
(`Reads::Pattern`): a key with no facts is unknown, a key of mixed type one of the pattern's
problems and never an answer (`ExpressionRefused::is_answer`). Every problem and cause is in one
refusal, first line "cannot use the pattern as specified" (`every_sentence_of_the_pattern_grammar`).
`BoundPattern` is made only there, and `evaluate` takes nothing else.

**Where a value lands is decided by its type** (`evaluate`): a path value makes one directory per
segment (`domain/transform.rs::path_segments`, the accessor's rule), a path with no segments none,
the `/` joining it going with it only where keeping it would leave `//` or a leading `/`; any other
value, in its one printed form, makes exactly one name and has no place where it holds `/`, is
empty, is `.` or `..`, or holds a null character; a path with no segments leaves no file name
where, had every other `{…}` given an ordinary name, the last name it stands in would be empty
or `.`. The finished text is a `core::domain::path::ContainedPath`, the type the
destination containment law is carried by, or the source is `NotPlaced::NotContained`
(`where_a_value_lands`).

**A source with no place carries every cause of every `{…}`** (`NoPlace`): each `NoPlaceCause`
has its sentence naming the `{…}` as specified, a value's saying "a file or directory name",
never which; its counted line (`count_line`, naming `/` where the sentence would quote a value);
and the query entry its remedy names (`query_entry`, read back as a filter by
`every_remedy_query_entry_binds_as_a_filter`). A remedy is composed by cause, never wording; a
key's type changing while the run reads it is `NotPlaced::TypeChanged`, an answer, never a source
with no place (`a_type_change_during_apply_stops_the_run_as_an_answer`).

## The printing rule

`domain/print.rs` writes a value or a key so that, typed into `--where`, it reads back as
itself: bare where the reader reads the bare text back the same, double-quoted otherwise. The
writer asks the reader — the filter reader (`reads_as_one_bare_value`, `reads_as_one_bare_key`)
and the binder's input type — and keeps no character list (`every_printed_value_reads_back_as_itself`, and every
printable one- and two-character value checked against the reader). A refusal's rule lines print
through it. So does the statement that keys are of mixed type, with their types, and its way out
(`mixed_type_statement`, `mixed_type_way_out`), which a refused filter, `facts`' listing,
`cluster`'s summary, a manifest's comments and `import-facts`' skip lines read: the key prints
through the rule, its control characters shown as pictures in `mixed_type_statement` (a refused
filter's own sentence, `Gives::statement`, prints the key as typed), and its `canon facts convert` command (`convert_command`) names the full stored
key, quoted for the shell only, and the type to convert to where its reader names one
(`the_convert_command_is_quoted_for_the_shell_only`). Every bucket label is spelled and recognized
here (`bucket_label`, `is_plain_bucket_label`, `threshold_bucket_labels`), the modifier handing over
only a `BucketRange`, so a report, a rejection and a directory name say one label. A report's
value prints against what its key gives (`printed_as`): every kind a chain gives, a part's word
and a bucket's label bare. A key's name as the user spelled it (`printed_name`, a report's
header) prints bare where it reads back as that same name, never swapped for the full stored key. Its inverse
for a name, `printed_name_read`, reads Canon's quotes off by `read_quoted`, which the key-name
reader applies to a typed name (`a_printed_key_reads_back_as_its_name`). **Where it does
not apply**: a pattern's directory name prints text as it is, bare.

## The read-back round trip

**The read-back round trip**: every row a report prints carries its filter as a typed result
(`RowFilter`, exact or wider), spelled only in `domain/row.rs` — a report spells no `--where`
text. A row's cell and the value in its filter come from one constructor (`ParsedFactKey::row`),
so the two cannot drift. Pasted as printed onto the same selection in the same zone, an unmarked
row selects exactly the sources it counted; a coarse time beside finer ones in its period is
marked `+` and its exact filter adds `AND NOT <key>|<next finer part>?` (`exactly`, read off
`RowValue::grain` and `contained`, the time noun's period table); text or a path holding a control
character is marked `!`, shown in control pictures (`pictured`), its filter a glob of quoted parts
around a bare `?` per visible character holding one — wider. A key of mixed type's rows claim no
filter. A report's value is read typed (`RowValue`) and counted under its row (`RowIdentity`: its
type and its printed form, so on a key of mixed type the text `"2019"` and the number `2019` are
two rows); values are ordered by type, then the type's own order, then printed form and type, so
two values are equal in order only where they are one row and ties print the same every run; a report key carries what its rows
are (`Rows`) and how it was spelled, which its header prints (`printed`), and a report's default is
added as a modifier (`with_modifier`). Verifier `every_printed_row_selects_what_it_counted`
(`facts`' tests, the report that consumes it). **Where it does not apply**: `ls -l`, `worklist`
and `facts convert`'s own output, which make no paste-back promise.

## The statement grammar

`domain/statement.rs` is the one way to state a fact about a value and its type: a `Statement`
is a `Subject`, `is` or `are`, and a `Complement`, or a subject and `cannot be` and a participle.
The verb comes from the subject's count alone, and no other verb can be written. A value subject
prints through the printing rule and a key subject through `printed_key`, so no caller prints
either another way. The type nouns and the loss names are core's (`FactType::noun`, `Loss::name`).
Where the problem sits and who fixes it stay the caller's: what can be done about a type — that
it cannot change, a route through another type, a modifier that asks without changing anything
stored — is help beside a statement, never one. Sentences about acts and their history are
outside it too.
Pins: `every_complement_reads_in_both_forms` and
`a_value_subject_prints_through_the_printing_rule`. **Where it does not apply**: these
statements about a value and its type are spelled in their own files and are not built from it —
`Gives::statement` in `domain/bind.rs`, `mixed_type_statement` and `mixed_type_way_out` in
`domain/print.rs`, the modifier no-value reasons in `domain/transform.rs`, the pattern statements in
`ops/pattern.rs`, `import-facts`' skip lines, `facts delete`'s duration answer, and the number and time reading errors in core.

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
deriving one: `EvalContext` carries `scope_rel: Option<&str>`, not a vantage, and evaluation's
`scope.rel_path` arm is a lookup. The strip happens at write time, through the path law's own
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
(`BoundPattern::names_scope_rel_path`). A consumer needs it to tell two expansion failures apart — a
scope it has not got, and a fact no source carries — which take opposite remedies; without it the
advice is wrong for half the readers. Answered here because the alternative is a consumer matching
on the key's spelling or on a sentence's wording, which is a second copy of a language rule
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
`SCOPE_REL_PATH` and `CONTENT_HASH` — is named literally: the first names no key, the second the
source row cannot answer though the lock entry recorded it, which `{hash}`, `{hash_short}` and
`{content.hash.sha256}` read; it is pinned rather than derived. `a_key_is_computed_exactly_when_the_resolver_answers_it`
(`domain/value.rs`) holds the derived half and is deliberately not widened to the other two:
asserting the equality over them would assert something false.

**The rule leaves the facility as a fetch, never as a predicate.** `is_context_supplied` is
`pub(in crate::expr)`; what callers get is `prefetch_pattern_facts`, which applies it for them.
Exporting a predicate for three callers to remember to call reproduces exactly the failure this
repaired, so a wrong site is made impossible rather than merely detectable — and
`the_context_supplied_set_is_spelled_only_inside_expr` (`tests/architecture.rs`) catches a
spelling appearing anywhere outside.

**Reach: this facility, argued rather than defaulted.** The asking half needs no such rule —
the filter's compare and a report's `row_value` read a computed key from the source row only, so
shadowing is already structurally impossible there. No canon-wide reach is claimed.

One narrowing to know about: the law drops only what the context actually supplies, never the
whole `source.` namespace. A `source.*` key that is not a source fact is supplied by nothing and
shadows nothing, so it is fetched like any other stored fact — pinned by
`a_key_the_context_does_not_supply_is_fetched` (`ops/pattern.rs`). No Canon writer can create
such a row, which is why the widening is unreachable in practice and pinned anyway.

## The per-source comparison reads its row through `repo`

`ops/filter.rs` speaks no SQL. A key the source row computes, compared or tested for existence,
is resolved per source, and the row it is resolved from — the source's attributes — comes from
`repo.rs`'s `fetch_source_row`, read at most once per source for the evaluation and the notes
together (`noting_reads_each_source_row_once`). Every other key, the content hash included, is compared on the source's one
value, read from the prefetched cache (`FactCache::value`). The comparison stays in `ops`;
moving it into `repo` would put comparison logic in the repository instead.

## Source-row values are derived once

`source_row_value` (`domain/value.rs`) is the one place a value is derived from a source row,
a built-in key's through a source fact's arm where it is a short name for one (`size`, `ext`), which
`BuiltinKey::source_fact` alone says — the size buckets, the year grouping and the save-time cause read it too; `--where`,
`facts --key`, `--emit` and `{source.*}` read it. Readers
split computed from stored by the key (`Key::is_computed()`, pinned equal to the resolver's answer on a readable source), never
by its `None` or by listing: a computed key the row cannot answer has no value. `hash` reads as `content.hash.sha256` and `hash_short` as
`content.hash.sha256|short` (`BuiltinKey::reading`) everywhere, a pattern reading that key off
the hash the lock entry recorded. Pinned per reader.

**The key-name law**: every command taking a key's name reads it through `read_key_name`
(`domain/key.rs`), declaring how (`NameReading`): a name a person typed has Canon's printed
quoting read off (`facts delete`, `facts convert`, `--emit`), a tool's or an already-read name
is read as written (import, `Key::read`). It gives the source fact, built-in key and content fact
the name names, and each command applies its own rule: an expression and `--emit` take them in that order, import and `facts delete` the
content reading alone, and `facts convert` the content reading with its own refusals. A consumer then decides by the key read (`ParsedFactKey::key`), never
the text, since quoting never changes which key. Verifier
`a_key_is_read_by_the_name_import_stored_it_under` (`facts/domain.rs`); `--emit`'s leg is in
`worklist/ops.rs`, and both readings are pinned at the owner (`a_name_is_read_as_its_caller_declares`).

## Selection notes

**The selection notes law**: an acting command's plan counts what its selection's own
evaluation decided, by one rule, in `ops/notes.rs`. A key is negated when its comparison sits
under an odd number of negations in the bound tree — each `NOT`, `!=`, `!~` and `NOT IN` counting
one, since the reader reads each negated operator as `NOT` of its positive form — and `of which N
have no <key>` counts, among the sources the command acts on, those `?` on the key as written
does not select; on `exclude set-object` an object counts when none of its selected sources has
a value. A time compared against a typed period finer than the time spans it when the boundary
falls strictly inside its period (`ClockSpan::spans`, the time noun's, beside the period table
and pinned against it); its line counts "selected" over what the command acts on and "not
selected" over what the filter did not select, so a source the filter selected and the command
leaves alone is in neither. Why: a count taken over the whole selection reports values entering
an act that does not touch them.

The marks are taken in the selecting loop itself, over the same cache and row read, through
`apply_filters_noting` where `SelectionParams.notes` asks: a second evaluation could read values
changed in between, and the line would disagree with the act. The lines are composed here once,
since the decision record, the receipt and the manifest carry them — the key as typed, the period
through the printing rule, no line at zero — and joined under a count by `SelectionNotes::under`. Verifier `the_notes_count_what_the_selection_decided`.

Where it does **not** apply: a command that does not act never asks (`notes: false`) — `ls`,
`facts`, `coverage`, `worklist`, `compare`, `survey`, and `cluster status`, which selects
nothing; nor single-target exclusion, which no filter selects. A bare `NOT key?` gives no line,
since it already selects exactly what has no value.

## The source-value law

**A source has one value for a stored key**: its content's fact; the source's own only where the
content has none. `source_value` (`domain/value.rs`) decides it. Readers reach it
through `FactCache::value` (the filter), `fetch_source_values` (`ops/value.rs`: patterns, `facts`
reports through `fetch_row_values`, and `worklist --emit` through `fetch_named_values`),
`count_values_by_key` (the key list's counts, what a bare `<key>?` selects), or `source_values` (`domain/value.rs`), which `cluster generate`
hands the facts it already holds for a source; none outside pairs the two rows or picks
between them, and which row a fact sits on is the fact noun's (`FactEntry::is_on_source`). Why:
a filter matching either row selected sources the report beside it never counted.

Pinned by `one_source_one_value_in_filter_report_pattern_and_worklist`, which asks the four
surfaces, an empty file among its sources; `cluster generate`'s facts comment by its own
`the_coverage_comment_reads_one_value_per_source`.

A content row Canon cannot read still decides, so the source has no value. Where it does **not**
apply: a key's type (`core::repo::fact::key_types` reads every row); a
computed key, never read from the facts table.

## The clock is a parameter

Times read through the frame rule on the clock the facility is handed — `SelectionParams.clock`,
`EvalContext`'s `clock`, the display and report functions' `zone` — never the machine's; a
manifest's run is handed the zone the manifest or its lock records. A modifier answers `Modified`: a value, or
no value and its `NoValueCause`, which every reader treats as a missing fact, `?` included; a slice
always has a value; a pattern gives a too-coarse
time `NoPlaceCause::TooCoarse`, a typed cause the archive's remedy reads, never its sentence. A typed
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
`Placement::used_an_instant`), and the chain answers it at the step that reads
(`apply_chain`'s `Chained`): an instant compared or printed with no transform, a time modifier
given an instant or text that reads as one — after an accessor or another modifier too — and a
typed time with an offset; never a clock reading or a number. Pinned by
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
