# Codesearch PR Split

1. **Index Profiling Infrastructure**  
   Add `codesearch/indexprofile`, wire `-index_profile` / `-index_profile_detailed`, and record indexing phases, counters, posting-list buckets, value-byte top lists, and TF storage estimates.

2. **Query Performance Logging**  
   Expand search logs to include posting-list bytes, stored-field key/byte reads, index-scoring duration, docs scored, and total scoring time.

3. **Posting List Builder Fast Path**  
   Introduce a sorted-slice `BuilderList` for indexing-time posting-list construction, avoiding hot-loop roaring mutations while preserving normal posting-list behavior.

4. **Sparse Ngram Tokenizer Fast Paths**  
   Add `NgramString`, ASCII line handling, compact ASCII ngram dedupe, and `sparse.Set.Index` to reduce allocation/conversion cost during indexing.

5. **Term Frequency Collection**  
   Teach `SparseNgramTokenizer` to track per-field ngram frequencies and expose them via `ForEachTermFrequency` / stats so indexing can add one posting per unique ngram with a frequency count.

6. **Counted Posting List Encoding**  
   Extend posting lists with `Frequency(docID)`, persist non-1 frequencies using the current counted format (`CSTF\x03`) with dense or bitset count encoding, and remove legacy compatibility paths.

7. **Index Writer Uses Dense Counts**  
   Update indexing to consume tokenizer term-frequency maps, write counted postings, and profile posting-list key/value byte sizes and TF estimates.

8. **Index-Backed Search Scoring**  
   Change `RawQuery`/`DocumentMatch`/`Scorer` so scoring uses posting-list frequencies directly, without loading full documents during scoring.

9. **Single Scoring Path Cleanup**  
   Remove exact-scoring/fallback complexity and score all candidates from indexed frequencies with fixed-length BM25-style saturation.

10. **Stored Token Count Sidecars Cleanup Decision**  
    We added stored `field:token_count` keys while exploring exact scoring, but the latest scorer no longer uses them. Either remove this entirely before PRs, or keep it as a separate explicit feature only if future exact/doc-length scoring is still desired.

11. **Parallel Indexing CLI**  
    Add `-parallelism`, task fanout, multiple independent index writers, cancellation on worker errors, and final latest-SHA writes.

12. **Profile Output Cleanup**  
    Remove noisy "top content posting lists by doc frequency" output and keep the more actionable value-bytes view. This can be folded into the profiling PR.
