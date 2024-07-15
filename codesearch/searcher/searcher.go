package searcher

import (
	"runtime"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/codesearch/types"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"golang.org/x/sync/errgroup"
)

type CodeSearcher struct {
	indexReader types.IndexReader
	log         log.Logger
}

func New(ir types.IndexReader) types.Searcher {
	subLog := log.NamedSubLogger("searcher")
	return &CodeSearcher{indexReader: ir, log: subLog}
}

func (c *CodeSearcher) retrieveDocs(candidateDocIDs []uint64) ([]types.Document, error) {
	start := time.Now()
	docs := make([]types.Document, len(candidateDocIDs))
	g := new(errgroup.Group)
	g.SetLimit(runtime.GOMAXPROCS(0))

	for i, docID := range candidateDocIDs {
		docID := docID
		i := i
		g.Go(func() error {
			doc, err := c.indexReader.GetStoredDocument(docID)
			if err != nil {
				return err
			}
			docs[i] = doc
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	c.log.Infof("Fetching %d docs took %s", len(candidateDocIDs), time.Since(start))
	return docs, nil
}

func (c *CodeSearcher) scoreDocs(scorer types.Scorer, matches []types.DocumentMatch, numResults int) ([]uint64, error) {
	start := time.Now()

	allDocIDs := make([]uint64, 0)
	for _, match := range matches {
		allDocIDs = append(allDocIDs, match.Docid())
	}
	slices.Sort(allDocIDs)
	docIDs := slices.Compact(allDocIDs)
	numDocs := len(docIDs)

	defer func() {
		c.log.Infof("Scoring %d docs took %s", numDocs, time.Since(start))
	}()

	if scorer.Skip() {
		if len(docIDs) > numResults {
			docIDs = docIDs[:numResults]
		}
		return docIDs, nil
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

	if len(docIDs) > numResults {
		docIDs = docIDs[:numResults]
	}

	return docIDs, nil
}

func (c *CodeSearcher) Search(q types.Query) ([]types.Document, error) {
	searchStart := time.Now()

	scorer := q.GetScorer()
	squery := q.SQuery()

	fieldDocidMatches, err := c.indexReader.RawQuery([]byte(squery))
	if err != nil {
		return nil, err
	}

	topDocIDs, err := c.scoreDocs(scorer, fieldDocidMatches, q.NumResults())
	if err != nil {
		return nil, err
	}
	docs, err := c.retrieveDocs(topDocIDs)
	if err != nil {
		return nil, err
	}
	c.log.Infof("Search took %s", time.Since(searchStart))
	return docs, nil
}
