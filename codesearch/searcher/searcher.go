package searcher

import (
	"context"
	"runtime"
	"slices"
	"sort"
	"sync"
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

func (c *CodeSearcher) retrieveDocs(candidateDocIDs []uint64) ([]types.Document, error) {
	docs := make([]types.Document, len(candidateDocIDs))

	for i, docID := range candidateDocIDs {
		doc, err := c.indexReader.GetStoredDocument(docID)
		if err != nil {
			return nil, err
		}
		docs[i] = doc
	}
	return docs, nil
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

func (c *CodeSearcher) scoreDocs(scorer types.Scorer, matches []types.DocumentMatch, numResults, offset int) ([]uint64, error) {
	start := time.Now()

	allDocIDs := make([]uint64, 0)
	for _, match := range matches {
		allDocIDs = append(allDocIDs, match.Docid())
	}
	slices.Sort(allDocIDs)
	docIDs := slices.Compact(allDocIDs)
	numDocs := len(docIDs)

	defer func() {
		tracker := performance.TrackerFromContext(c.ctx)
		if tracker == nil {
			return
		}
		tracker.TrackOnce(performance.TOTAL_SCORING_DURATION, int64(time.Since(start)))
		tracker.TrackOnce(performance.TOTAL_DOCS_SCORED_COUNT, int64(numDocs))
	}()

	if scorer.Skip() {
		return truncate(docIDs, numResults, offset), nil
	}

	scoreMap := make(map[uint64]float64, numDocs)
	var mu sync.Mutex

	// TODO(tylerw): use a priority-queue; stop iteration early.
	g := new(errgroup.Group)
	g.SetLimit(runtime.GOMAXPROCS(0))

	for _, match := range matches {
		docID := match.Docid()
		g.Go(func() error {
			doc, err := c.indexReader.GetStoredDocument(docID)
			if err != nil {
				return err
			}

			score := scorer.Score(match, doc)
			mu.Lock()
			scoreMap[docID] = score
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		log.Errorf("error: %s", err)
	}

	sort.Slice(docIDs, func(i, j int) bool {
		return scoreMap[docIDs[i]] > scoreMap[docIDs[j]]
	})

	return truncate(docIDs, numResults, offset), nil
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
	docs, err := c.retrieveDocs(topDocIDs)
	if err != nil {
		return nil, err
	}
	if tracker := performance.TrackerFromContext(c.ctx); tracker != nil {
		tracker.TrackOnce(performance.TOTAL_SEARCH_DURATION, int64(time.Since(searchStart)))
	}
	return docs, nil
}
