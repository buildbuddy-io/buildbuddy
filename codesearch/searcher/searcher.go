package searcher

import (
	"container/heap"
	"context"
	"sort"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/codesearch/performance"
	"github.com/buildbuddy-io/buildbuddy/codesearch/types"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

// minDocsToRescore is the minimum phase-1 candidate window handed to the
// rescore pass, regardless of the requested page size. Over-fetching beyond
// the result window lets the exact scorer promote docs the cheap scorer
// ranked below the page boundary, and keeps pages full when rescoring drops
// false positives. Sized in line with other engines' rescore windows (Solr
// reranks 200 docs by default, Vespa 100 per content node).
const minDocsToRescore = 200

type CodeSearcher struct {
	ctx         context.Context
	indexReader types.IndexReader
	log         log.Logger
}

func New(ctx context.Context, ir types.IndexReader) types.Searcher {
	subLog := log.NamedSubLogger("searcher")
	return &CodeSearcher{ctx: ctx, indexReader: ir, log: subLog}
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

// rescoreDocs runs the exact (stored-document) scorer over docs and drops any
// doc the exact scorer rejects — docs whose ngrams all matched but whose
// contents don't actually match the query score 0 here. Document fetch and
// regexp matching make this the expensive pass, so docs are scored in
// parallel.
func (c *CodeSearcher) rescoreDocs(scorer types.Scorer, docs []scoredDoc) []scoredDoc {
	rescored := make([]scoredDoc, len(docs))
	var wg sync.WaitGroup
	for i, d := range docs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// TODO(jdelfino): We throw away the stored document here, but then re-fetch it
			// if this document makes the cut. Save it and plumb it back out to improve
			// performance.
			doc := c.indexReader.GetStoredDocument(d.docID)
			rescored[i] = scoredDoc{docID: d.docID, match: d.match, score: scorer.Rescore(d.match, doc)}
		}()
	}
	wg.Wait()

	results := rescored[:0]
	for _, d := range rescored {
		if d.score > 0 {
			results = append(results, d)
		}
	}
	if dropped := len(rescored) - len(results); dropped > 0 {
		log.Infof("Rescoring dropped %d zero-scored docs", dropped)
	}
	return results
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

	if scorer.Skip() {
		docIDs := make([]uint64, 0, len(matches))
		for _, match := range matches {
			docIDs = append(docIDs, match.Docid())
		}
		sort.Slice(docIDs, func(i, j int) bool {
			return docIDs[i] < docIDs[j]
		})
		return truncate(docIDs, numResults, offset), nil
	}

	if numResults <= 0 {
		return nil, nil
	}

	// Scoring uses only index-side data (term frequencies and field lengths),
	// so it is cheap enough to run on every match; the heap keeps just the
	// docs that can make it into the rescore window.
	rescoreLimit := max(offset+numResults, minDocsToRescore)
	topDocs := make(topDocsHeap, 0, rescoreLimit)
	for _, match := range matches {
		doc := scoredDoc{docID: match.Docid(), match: match, score: scorer.Score(match)}
		docsScored++
		pushOrReplaceTopDoc(&topDocs, doc, rescoreLimit)
	}

	results := c.rescoreDocs(scorer, []scoredDoc(topDocs))
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
