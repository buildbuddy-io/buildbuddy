package searcher

import (
	"container/heap"
	"context"
	"math"
	"runtime"
	"sort"
	"time"

	"github.com/buildbuddy-io/buildbuddy/codesearch/performance"
	"github.com/buildbuddy-io/buildbuddy/codesearch/types"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"golang.org/x/sync/errgroup"
)

type CodeSearcher struct {
	ctx         context.Context
	indexReader types.IndexReader
	log         log.Logger
}

func New(ctx context.Context, ir types.IndexReader) types.Searcher {
	subLog := log.NamedSubLogger("searcher")
	return &CodeSearcher{ctx: ctx, indexReader: ir, log: subLog}
}

// importRankAlpha is the fixed strength of the always-on import-rank boost:
// final = text * (1 + importRankAlpha * rank). The scale is fixed (no
// corpus-wide max) so scores stay stable under incremental index updates.
const importRankAlpha = 0.5

// importRankBoost returns the multiplier applied to a document's score for its
// import in-degree signal: 1 + importRankAlpha * min(1, log2(1+inDegree)/20).
// Returns 1 when the signal is unresolved or absent (in-degree 0) — e.g. on
// older indexes with no import fields — so the boost is a safe no-op there.
// The signal is resolved only over the rescore window (see scoreDocs), so the
// boost is applied during rescoring rather than the cheap all-candidates pass.
func importRankBoost(docMatch types.DocumentMatch) float64 {
	rank := math.Min(1, math.Log2(1+docMatch.Signal(types.SignalImportInDegree))/20)
	return 1 + importRankAlpha*rank
}

func (c *CodeSearcher) retrieveDocs(candidateDocIDs []uint64) []types.Document {
	docs := make([]types.Document, len(candidateDocIDs))

	for i, docID := range candidateDocIDs {
		doc := c.indexReader.GetStoredDocument(docID)
		docs[i] = doc
	}
	return docs
}

func truncate(results []uint64, numResults, offset int) []uint64 {
	if offset >= len(results) {
		return nil
	}
	rest := results[offset:]
	if len(rest) > numResults {
		rest = rest[:numResults]
	}
	return rest
}

type scoredDoc struct {
	docID uint64
	match types.DocumentMatch
	score float64
}

// lowerScoredDoc orders docs from weakest to strongest: ascending score,
// breaking ties toward the larger doc ID so that equal-scored docs surface in
// ascending doc ID order.
func lowerScoredDoc(a, b scoredDoc) bool {
	if a.score == b.score {
		return a.docID > b.docID
	}
	return a.score < b.score
}

// topDocsHeap is a min-heap whose root is the weakest doc in the current top
// set, so a stronger doc can replace it in O(log n).
type topDocsHeap []scoredDoc

func (h topDocsHeap) Len() int           { return len(h) }
func (h topDocsHeap) Less(i, j int) bool { return lowerScoredDoc(h[i], h[j]) }
func (h topDocsHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *topDocsHeap) Push(x any)        { *h = append(*h, x.(scoredDoc)) }
func (h *topDocsHeap) Pop() any {
	old := *h
	doc := old[len(old)-1]
	*h = old[:len(old)-1]
	return doc
}

func pushOrReplaceTopDoc(h *topDocsHeap, doc scoredDoc, limit int) {
	if doc.score <= 0 {
		return
	}
	if h.Len() < limit {
		heap.Push(h, doc)
		return
	}
	if lowerScoredDoc((*h)[0], doc) {
		(*h)[0] = doc
		heap.Fix(h, 0)
	}
}

// minDocsToRescore is the minimum phase-1 candidate window handed to the
// rescore pass, regardless of the requested page size. Over-fetching beyond
// the result window lets the exact scorer promote docs the cheap scorer
// ranked below the page boundary, and keeps pages full when rescoring drops
// false positives. Sized in line with other engines' rescore windows (Solr
// reranks 200 docs by default, Vespa 100 per content node).
//
// It also bounds correctness for fully-unindexable queries: when a pattern
// produces no ngrams (e.g. `filepath:zo` alone compiles to (:all)), the cheap
// scorer gives every doc equal credit, so the window is just the first
// rescoreLimit docs by doc ID. A matching doc beyond that window is never
// rescored and is silently dropped (the "window exhausted" Warning fires, but
// only in server logs). Rescoring adaptively until the page fills would remove
// the bound at the cost of unbounded stored-doc reads.
const minDocsToRescore = 200

// rescoreDocs runs the exact (stored-document) scorer over docs and drops any
// doc the exact scorer rejects — docs whose ngrams all matched but whose
// contents don't actually match the query score 0 here. Document fetch and
// regexp matching make this the expensive pass, so docs are scored
// concurrently — but bounded to GOMAXPROCS, so a large window (deep
// pagination) can't fan out thousands of simultaneous Pebble reads and DFA
// scans against the same DB.
func (c *CodeSearcher) rescoreDocs(scorer types.Scorer, docs []scoredDoc) ([]scoredDoc, error) {
	rescored := make([]scoredDoc, len(docs))
	var g errgroup.Group
	g.SetLimit(runtime.GOMAXPROCS(0))
	for i, d := range docs {
		g.Go(func() error {
			// TODO(jdelfino): We throw away the stored document here, but then
			// re-fetch it in retrieveDocs if this doc survives. Rescore now
			// fetches up to a full window (~minDocsToRescore) of stored docs per
			// relevance query, so saving and plumbing it back is worth doing.
			doc := c.indexReader.GetStoredDocument(d.docID)
			// The scorer owns scoring policy: Rescore returns the exact text
			// score (0 = not a true match, dropped below). The searcher only
			// layers on the import-rank boost.
			final := scorer.Rescore(d.match, doc)
			if final > 0 {
				final *= importRankBoost(d.match)
			}
			rescored[i] = scoredDoc{docID: d.docID, match: d.match, score: final}
			return nil
		})
	}
	// No closure returns an error today (GetStoredDocument doesn't surface
	// one), but the errgroup is the seam to propagate fetch errors once it
	// does, instead of silently scoring a missing doc as 0.
	if err := g.Wait(); err != nil {
		return nil, err
	}

	results := rescored[:0]
	for _, d := range rescored {
		if d.score > 0 {
			results = append(results, d)
		}
	}
	if dropped := len(rescored) - len(results); dropped > 0 {
		log.Infof("Rescoring dropped %d zero-scored docs", dropped)
	}
	return results, nil
}

func (c *CodeSearcher) scoreDocs(scorer types.Scorer, matches []types.DocumentMatch, numResults, offset int) ([]uint64, error) {
	start := time.Now()
	docsScored := 0

	defer func() {
		tracker := performance.TrackerFromContext(c.ctx)
		if tracker == nil {
			return
		}
		tracker.TrackOnce(performance.TOTAL_SCORING_DURATION, int64(time.Since(start)))
		tracker.TrackOnce(performance.TOTAL_DOCS_SCORED_COUNT, int64(docsScored))
	}()

	if numResults <= 0 {
		// Count-only request: nothing to rank or rescore.
		return nil, nil
	}

	// The rescore window over-fetches minDocsToRescore docs past the requested
	// page so false-positive drops don't leave it short, and bounds the per-doc
	// signal/rescore work to O(window) no matter how many candidates matched.
	rescoreLimit := offset + max(numResults, minDocsToRescore)

	if scorer.Skip() {
		// Filter-only query (no text relevance to score on). Rank by the import
		// in-degree signal so popular files surface first — but only over a
		// bounded window, since a broad filter (lang:go, repo:...) can match
		// tens of thousands of docs and the user sees one page. The window is
		// the lowest doc IDs; matches beyond it keep doc-ID order, so a popular
		// file whose doc ID falls past the window won't surface (acceptable for
		// a ranking nicety with no text signal to prune by).
		sort.Slice(matches, func(i, j int) bool {
			return matches[i].Docid() < matches[j].Docid()
		})
		window := matches
		if len(window) > rescoreLimit {
			window = window[:rescoreLimit]
		}
		if err := c.indexReader.ResolveSignals(window, types.SignalImportInDegree); err != nil {
			return nil, err
		}
		// Stable sort so equal in-degrees (e.g. all zero on an index without
		// import data) keep the ascending doc-ID order established above.
		sort.SliceStable(window, func(i, j int) bool {
			return importRankBoost(window[i]) > importRankBoost(window[j])
		})
		docIDs := make([]uint64, len(matches))
		for i, match := range matches {
			docIDs[i] = match.Docid()
		}
		return truncate(docIDs, numResults, offset), nil
	}

	// Prepare computes candidate-set statistics for scoring; do it after the
	// guards above so a count-only or filter-only request pays nothing.
	scorer.Prepare(matches)

	// Scoring uses only index-side data (term frequencies and field lengths),
	// so it is cheap enough to run on every match; the heap keeps just the
	// docs that can make it into the rescore window.
	topDocs := make(topDocsHeap, 0, rescoreLimit)
	for _, match := range matches {
		doc := scoredDoc{docID: match.Docid(), match: match, score: scorer.Score(match)}
		docsScored++
		pushOrReplaceTopDoc(&topDocs, doc, rescoreLimit)
	}

	window := []scoredDoc(topDocs)

	// Resolve the import in-degree signal only over the window we'll rescore,
	// so the boost (applied in rescoreDocs) is bounded to O(window) reads
	// rather than O(candidates).
	windowMatches := make([]types.DocumentMatch, len(window))
	for i, d := range window {
		windowMatches[i] = d.match
	}
	if err := c.indexReader.ResolveSignals(windowMatches, types.SignalImportInDegree); err != nil {
		return nil, err
	}

	results, err := c.rescoreDocs(scorer, window)
	if err != nil {
		return nil, err
	}
	if len(results) < offset+numResults && len(topDocs) == rescoreLimit {
		// The window was full, so deeper candidates may exist that were never
		// rescored. If this fires often, consider rescoring adaptively until
		// the page fills instead of using a fixed window.
		log.Warningf("Rescore window exhausted: %d of %d candidates survived for a request of %d results",
			len(results), rescoreLimit, offset+numResults)
	}
	sort.Slice(results, func(i, j int) bool {
		return lowerScoredDoc(results[j], results[i])
	})

	resultDocIDs := make([]uint64, len(results))
	for i, result := range results {
		resultDocIDs[i] = result.docID
	}
	return truncate(resultDocIDs, numResults, offset), nil
}

func (c *CodeSearcher) Search(q types.Query, numResults, offset int) ([]types.Document, error) {
	searchStart := time.Now()

	docidMatches, err := c.indexReader.RawQuery(q.SQuery())
	if err != nil {
		return nil, err
	}

	// The import-rank boost is applied during rescoring over the top-K window
	// (scoreDocs), not here over every candidate: resolving the signal per
	// match would read an import_id and load a posting list for every one of
	// potentially tens of thousands of candidates.
	topDocIDs, err := c.scoreDocs(q.Scorer(), docidMatches, numResults, offset)
	if err != nil {
		return nil, err
	}
	docs := c.retrieveDocs(topDocIDs)

	if tracker := performance.TrackerFromContext(c.ctx); tracker != nil {
		tracker.TrackOnce(performance.TOTAL_SEARCH_DURATION, int64(time.Since(searchStart)))
	}
	return docs, nil
}
