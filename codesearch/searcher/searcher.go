package searcher

import (
	"context"
	"slices"
	"sort"
	"time"

	"github.com/buildbuddy-io/buildbuddy/codesearch/performance"
	"github.com/buildbuddy-io/buildbuddy/codesearch/types"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
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

func dropZeroScores(docIDs []uint64, scoreMap map[uint64]float64) []uint64 {
	// Precondition: docIDs is sorted in descending order of score.
	for i, docID := range docIDs {
		if scoreMap[docID] <= 0.0 {
			log.Infof("Dropping %d zero scores", len(docIDs)-i)
			return docIDs[:i]
		}
	}
	return docIDs
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

	scoreMap := make(map[uint64]float64, numDocs)

	quitScoringEarly := false
	for _, match := range matches {
		docID := match.Docid()
		if docsScored > maxDocsToScore {
			quitScoringEarly = true
			scoreMap[docID] = 0.0
			continue
		}
		scoreMap[docID] = scorer.Score(match)
		docsScored += 1
	}
	if quitScoringEarly {
		log.Warningf("Stopped scoring after %d (max) docs", maxDocsToScore)
	}

	sort.Slice(docIDs, func(i, j int) bool {
		return scoreMap[docIDs[i]] > scoreMap[docIDs[j]]
	})

	docIDs = truncate(docIDs, numResults, offset)
	return dropZeroScores(docIDs, scoreMap), nil
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
