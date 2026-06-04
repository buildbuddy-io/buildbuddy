package searcher

import (
	"container/heap"
	"context"
	"slices"
	"sort"
	"time"

	"github.com/buildbuddy-io/buildbuddy/codesearch/performance"
	"github.com/buildbuddy-io/buildbuddy/codesearch/types"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/priority_queue"
)

const maxDocsToScore = 100_000

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

type candidateDoc struct {
	docID      uint64
	match      types.DocumentMatch
	upperBound float64
}

type scoredDoc struct {
	docID uint64
	score float64
}

func lowestScoredDoc(pq priority_queue.PriorityQueue[scoredDoc]) (scoredDoc, bool) {
	if len(pq) == 0 {
		return scoredDoc{}, false
	}
	// priority_queue is a max heap, so the lowest priority item is one of the leaves.
	minIndex := len(pq) / 2
	for i := minIndex + 1; i < len(pq); i++ {
		if pq[i].Value().score < pq[minIndex].Value().score {
			minIndex = i
		}
	}
	return pq[minIndex].Value(), true
}

func pushOrReplaceTopDoc(pq *priority_queue.PriorityQueue[scoredDoc], doc scoredDoc, limit int) {
	if doc.score <= 0 || limit <= 0 {
		return
	}
	if pq.Len() < limit {
		heap.Push(pq, priority_queue.NewItem(doc, doc.score))
		return
	}
	minDoc, ok := lowestScoredDoc(*pq)
	if !ok {
		return
	}
	if doc.score > minDoc.score || (doc.score == minDoc.score && doc.docID < minDoc.docID) {
		pq.RemoveItemWithMinPriority()
		heap.Push(pq, priority_queue.NewItem(doc, doc.score))
	}
}

func (c *CodeSearcher) scoreDocs(scorer types.Scorer, matches []types.DocumentMatch, numResults, offset int) ([]uint64, error) {
	start := time.Now()

	allDocIDs := make([]uint64, 0)
	for _, match := range matches {
		allDocIDs = append(allDocIDs, match.Docid())
	}
	slices.Sort(allDocIDs)
	docIDs := slices.Compact(allDocIDs)
	numDocs := len(docIDs)
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
		return truncate(docIDs, numResults, offset), nil
	}

	if numResults <= 0 {
		return nil, nil
	}

	candidates := make([]candidateDoc, 0, numDocs)
	for _, match := range matches {
		upperBound := scorer.UpperBoundScore(match)
		if upperBound <= 0 {
			continue
		}
		candidates = append(candidates, candidateDoc{
			docID:      match.Docid(),
			match:      match,
			upperBound: upperBound,
		})
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].upperBound == candidates[j].upperBound {
			return candidates[i].docID < candidates[j].docID
		}
		return candidates[i].upperBound > candidates[j].upperBound
	})

	resultLimit := offset + numResults
	topDocs := make(priority_queue.PriorityQueue[scoredDoc], 0, resultLimit)
	heap.Init(&topDocs)
	quitScoringEarly := false
	for _, candidate := range candidates {
		if topDocs.Len() >= resultLimit {
			minDoc, ok := lowestScoredDoc(topDocs)
			if ok && candidate.upperBound <= minDoc.score {
				break
			}
		}
		if docsScored > maxDocsToScore {
			quitScoringEarly = true
			break
		}
		doc := c.indexReader.GetStoredDocument(candidate.docID)
		score := scorer.Score(candidate.match, doc)
		pushOrReplaceTopDoc(&topDocs, scoredDoc{docID: candidate.docID, score: score}, resultLimit)
		docsScored += 1
	}
	if quitScoringEarly {
		log.Warningf("Stopped scoring after %d (max) docs", maxDocsToScore)
	}

	results := make([]scoredDoc, topDocs.Len())
	for i, item := range topDocs {
		results[i] = item.Value()
	}
	sort.Slice(results, func(i, j int) bool {
		if results[i].score == results[j].score {
			return results[i].docID < results[j].docID
		}
		return results[i].score > results[j].score
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
