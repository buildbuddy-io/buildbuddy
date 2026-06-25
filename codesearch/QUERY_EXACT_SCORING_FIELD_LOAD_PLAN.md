# Query Exact Scoring Field Load Follow-Up

## Context

After adding field-length stats and scanning the `sta` section sequentially, the Linux query run looked like:

```text
Retrieved 6 posting lists with 453345 doc IDs in 57.444583ms [0.39 MB]
Read 22372 stored-field keys [2.67 MB]
Read 93014 field-stats keys in 38.963458ms [8.10 MB]
Scored 1598 docs in 97.155209ms
Completed search in 216.51825ms
```

The stats scan is now cheap enough to keep. Exact scoring remains a moderate cost: about 97ms of the 216ms total, or roughly 45%.

## Follow-Up

Reduce stored-field reads during exact scoring.

For regexp queries over `content` and `filename`, exact scoring should only need the fields referenced by the query scorer, not every stored field for each candidate document. Loading only the needed fields should reduce the `22372 stored-field keys` read for 1598 exact-scored docs.

Expected impact is moderate rather than transformative. If exact scoring drops from about 97ms to 50-70ms, the total query time would likely move from about 216ms to roughly 170-190ms on this run. The main value is better scaling on broad queries and repos with more stored metadata fields.
