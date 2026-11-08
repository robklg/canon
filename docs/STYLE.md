# Style

The style guide for every sentence a user reads: Canon's printed output (messages,
hints, warnings, help text) and the user docs (`docs/src`), for anyone writing or
reviewing them.

User-facing text says what a user needs to use the program and can't see by using
it. A detail belongs only if knowing it changes what they do or what they can rely
on.

The docs are a reference: structure, tables, and sample outputs carry the weight,
and the prose stays plain. A separate user guide is planned; warmth belongs there.

## Page tiers

- **Concept pages** (`concepts/`) explain what a thing is and may explain why it is
  that way, with restraint. A measure of sentiment is allowed where the concept
  itself carries weight.
- **Command and reference pages** are functional. They assume the concepts section
  has been read: link to the concept, don't re-explain it.
- A command page found explaining a concept is a signal that a concept page is
  missing or incomplete. The fix is moving the explanation, not polishing it in
  place.

## Criteria

1. **No stated sentiment.** The emotion lives in the user's experience, not the
   prose. Outside concept pages, at most one such sentence may survive per page, and
   only if cutting it would make a deliberate design read as a missing feature.
2. **No design rationale.** Pages say what a thing is and how to read it; why it was
   built that way lives in specs and ADRs. Exception: rationale that changes how the
   user reads the output.
3. **Don't define by negation** ("not X, but Y") unless the reader actually arrives
   holding the misconception.
4. **Don't answer questions nobody asks.** No reassurances against conflicts the
   reader has never seen.
5. **Sample outputs are the documentation.** Prose around them shrinks. Output blocks
   must match what the binary prints — never edit one to improve its wording; that is
   a CLI change, to be raised separately.
6. **Contract language survives, in plain terms.** Behavioral guarantees ("omitted,
   never guessed", "identification is not verification", counted omissions) are
   load-bearing, not decoration. Keep the guarantee; say it plainly.
7. **Accuracy over eloquence.** Every statement of behavior must match today's
   behavior. A rewrite must not introduce factual claims the original didn't make;
   any it can't avoid must be verified against the source before landing.
8. **General shapes.** No drive-shaped or photo-shaped wording where the feature is
   general; examples represent the general case.
9. **Em dashes** stay out of running prose in the docs. They are fine in tables,
   lists, and literal output, separating a term from its definition.
10. **Today's product only.** No promises about where Canon is heading; the docs
    describe what exists.
11. **Full clauses, not compressed ones.** No elliptical phrases such as "read as in X"
    or "as per X": say what is shared, as in "follows the same rules as X". Never use
    "read" to mean parsed or interpreted.
12. **Write a changed sentence again**, from what the user needs, as if it were the
    first version. A fix makes details feel important to whoever just worked on it:
    the edge case that was fixed, or behaviour that shows as soon as the command
    runs, like a table sorted by date. The user hasn't seen the fix, and a clause
    added to the old sentence tends to record it.
