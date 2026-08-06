# Managing Roots

To track files in Canon, first you add and scan [roots](../../concepts/roots.md).
This makes these sources available for further enrichment or archive operations.
You can suspend roots to temporarily mask them from Canon commands.

Adding new roots, or scanning existing is performed through the [scan](scan.md) command.

Managing roots, such as suspending or listing them is done with [canon roots](roots.md).

[`canon roots story`](story.md) reads a root's resolution story between triage
passes: where you acted and why, and what no decision ever touched. When you judge it
resolved, [`canon roots retire`](retire.md) binds that story into a book and releases
the root from the index.
