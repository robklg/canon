# Managing Sources

After scanning and enriching, these commands control which sources archiving operations consider, and annotate locations along the way.

[`exclude`](exclude.md) marks sources to skip during `cluster generate` and `apply`: temporary or system files, known duplicates beside a preferred copy, files below a size threshold. Excluding deletes nothing, and exclusions can be cleared at any time.

[`note`](note.md) annotates locations with timestamped observations. Notes surface automatically in [`survey`](../query/survey.md) output when you revisit a location.
