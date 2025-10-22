# Codesearch Result Quality Test Harness

## Harness

The harness is in [rater.go](rater.go). See comments in that file for background on scoring
methodology.

## Scoring methodology

The harness uses
[Normalized Discounted Cumulative Gain (nDCG)](https://en.wikipedia.org/wiki/Discounted_cumulative_gain).
The score for a single query is based on the relevance of each result in the result set, and the
position of the result in the result set. More relevant results that appear earlier in the list
will score higher.

The relevance of each actual result is determined by looking up that result in a list of provided
"ideal results", provided with each test case.

If an actual result is not found in the ideal results, that result either does not contribute to
scoring, or, if the `penalize_unmatched` option is set, contributes negatively to scoring. This
allows evaluation of queries with small result sets, where the full set of expected results can
be specified, as well as queries with large result sets, where it is only feasible to list the most
relevant expected results.

## Annotating queries

Creating new test cases involves finding a target query, collecting the expected "ideal results",
and adding this information to [ratings.json](ratings.json). The format is protojson,
and the file contains a single `Suite` message from
[search_rating.proto](/proto/search_rating.proto).

When adding new cases:

1. Choose a query, check out the relevant repo at the commit SHA specified in
   the test suite, and use `grep` (or some other tool) to manually find the expected responses.

   - **Don't** use codesearch itself to find the expected responses - bugs in codesearch could lead
     to encoding incorrect results in the golden data.

2. Create an `IdealResult` object describing each result that you want to contribute to scoring.
   Give each result a relevance score. The scores can be any float values. A higher relevance value
   indicates that a result should show up earlier in the result set if ranking is ideal.

   The range of relevance values matters within a single test case, but does not need to be uniform
   across test cases (scores are normalized per test case). However, here are some default rules to
   follow unless there is a reason to deviate:

   1. A relevance of 3.0 is an "excellent" result, and should the first or second result.
   2. A relevance of 2.0 is a "good" result, and should be within the first few results.
   3. A relevance of 1.0 is a "fine" result, but should probably not be in the first few results.
   4. A relevance of 0.5 is a "poor" result - it's a valid match, but should be towards the end
      of the results (e.g. generated code, or low value test usages of a function).

   Note that the order of the `IdealResults` in [ratings.json](ratings.json) is
   irrelevant. The provided relevance values indicate how each result will contribute to the
   overall score.

## Full vs. partial result set matching

If the given query has a small number of expected results, `penalized_unmatched` should be set to
true for the test case. This will result in a negative score contribution from any actual results
that are not found in the expected results. An example of this situation is scoring a query where
there are only 2-3 results. In this case, we can exhaustively list the expected results, and
scoring should suffer if false positives are included.

If the given query has a large number of expected results (too many to reasonably list in the
test case), the most relevant results should be included, and `penalize_unmatched` should be set to
false. In this case, the actual query will score higher if relevant results are included early in
the result set, but will not be directly penalized for other results being included. An example of
this situation might be a very commonly used function - the test case may list the function's
definition with a high relevance, but no other results. In this case, as long as the actual query
returned the function definition early in the results, it would score well.
