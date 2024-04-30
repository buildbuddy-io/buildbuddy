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
	c.log.Infof("Fetching docs took %s", time.Since(start))
	return docs, nil
}

func (c *CodeSearcher) scoreDocs(scorer types.Scorer, fieldDocidMatches map[string][]uint64, numResults int) ([]uint64, error) {
	start := time.Now()

	allDocIDs := make([]uint64, 0)
	for _, docIDs := range fieldDocidMatches {
		allDocIDs = append(allDocIDs, docIDs...)
	}
	slices.Sort(allDocIDs)

	numDocs := len(allDocIDs)
	docIDs := slices.Compact(allDocIDs)

	scoreMap := make(map[uint64]float64, numDocs)
	var mu sync.Mutex

	docFields := make(map[uint64][]string, numDocs)
	for fieldName, docs := range fieldDocidMatches {
		if len(fieldName) == 0 {
			continue
		}
		for _, docid := range docs {
			docFields[docid] = append(docFields[docid], fieldName)
		}
	}

	// TODO(tylerw): use a priority-queue; stop iteration early.
	g := new(errgroup.Group)
	g.SetLimit(runtime.GOMAXPROCS(0))

	for _, docID := range docIDs {
		docID := docID
		g.Go(func() error {
			doc, err := c.indexReader.GetStoredDocument(docID, docFields[docID]...)
			if err != nil {
				return err
			}
			score := scorer.Score(doc)
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

	c.log.Infof("Scoring %d docs took %s", numDocs, time.Since(start))
	return docIDs, nil
}

func (c *CodeSearcher) Search(q types.Query) ([]types.Document, error) {
	searchStart := time.Now()

	scorer := q.GetScorer()
	squery := q.SQuery()

	candidateDocIDs, err := c.indexReader.RawQuery([]byte(squery))
	if err != nil {
		return nil, err
	}

	topDocIDs, err := c.scoreDocs(scorer, candidateDocIDs, q.NumResults())
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
