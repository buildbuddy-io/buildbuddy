package searcher

import (
	"bufio"
	"bytes"
	"fmt"
	"regexp"
	"regexp/syntax"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/codesearch/query"
	"github.com/buildbuddy-io/buildbuddy/codesearch/result"
	"github.com/buildbuddy-io/buildbuddy/codesearch/types"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/go-enry/go-enry/v2"
	"golang.org/x/sync/errgroup"
)

type Searcher interface {
	Search(q string, numResults int) ([]*result.Result, error)
}

type CodeSearcher struct {
	indexReader types.IndexReader
	log         log.Logger
}

func New(ir types.IndexReader) Searcher {
	subLog := log.NamedSubLogger("searcher")
	return &CodeSearcher{indexReader: ir, log: subLog}
}

// remove these one-off methods
var nl = []byte{'\n'}

func countNL(b []byte) int {
	r := b
	n := 0
	for {
		i := bytes.IndexByte(r, '\n')
		if i < 0 {
			break
		}
		n++
		r = r[i+1:]
	}
	return n
}

func extractLine(buf []byte, lineNumber int) []byte {
	s := bufio.NewScanner(bytes.NewReader(buf))
	currentLine := 0
	for s.Scan() {
		currentLine++
		if currentLine == lineNumber {
			return s.Bytes()
		}
	}
	return nil
}

// formalize this?
type region struct {
	startOffset int
	endOffset   int
	lineNumber  int
}

type reScorer struct {
	re *regexp.Regexp
}

func (s *reScorer) match(buf []byte) []region {
	matchIndexes := s.re.FindAllIndex(buf, -1)
	results := make([]region, len(matchIndexes))
	for i, pair := range matchIndexes {
		results[i] = region{
			startOffset: pair[0],
			endOffset:   pair[1],
			lineNumber:  countNL(buf[:pair[0]]) + 1,
		}
	}
	return results
}

func (s *reScorer) Score(doc types.Document) float64 {
	docScore := 0.0
	for _, fieldName := range doc.Fields() {
		field := doc.Field(fieldName)
		if len(field.Contents()) == 0 {
			continue
		}
		matchingRegions := s.match(field.Contents())
		f_qi_d := float64(len(matchingRegions))
		D := float64(len(strings.Fields(string(field.Contents()))))
		k1, b := bm25Params(field.Name())
		fieldScore := (f_qi_d * (k1 + 1)) / (f_qi_d + k1*(1-b+b*D))
		docScore += fieldScore
	}
	return docScore
}

func (s *reScorer) makeResults(docs []types.Document) []*result.Result {
	results := make([]*result.Result, len(docs))
	for i, doc := range docs {
		r := &result.Result{}
		for _, fieldName := range doc.Fields() {
			field := doc.Field(fieldName)
			if field.Name() == "filename" {
				r.Filename = string(field.Contents())
			}
			for _, region := range s.match(field.Contents()) {
				r.MatchCount += 1
				line := extractLine(field.Contents(), region.lineNumber)
				r.Snippets = append(r.Snippets, []byte(fmt.Sprintf("%d: %s\n", region.lineNumber, line)))
			}
		}
		results[i] = r
	}
	return results
}

func (c *CodeSearcher) parse(q string) (*reScorer, []byte, error) {
	c.log.Infof("raw query: [%s]", q)

	// A list of s-expression strings that must be satisfied by
	// the query. (added to the query with AND)
	requiredSClauses := make([]string, 0)
	regexOpts := []string{
		"(?m)", // always use multiline mode.
	}

	// match `case:yes` or `case:y`
	caseMatcher := regexp.MustCompile(`case:(yes|y)`)
	if caseMatcher.MatchString(q) {
		q = caseMatcher.ReplaceAllString(q, "")
	} else {
		// otherwise default to case-insensitive
		regexOpts = append(regexOpts, "(?i)")
	}

	// match `file:test.js`, `f:test.js`, and `path:test.js`
	fileMatcher := regexp.MustCompile(`(?:file:|f:|path:)(?P<filepath>[[:graph:]]+)`)
	fileMatch := fileMatcher.FindStringSubmatch(q)
	if len(fileMatch) == 2 {
		q = fileMatcher.ReplaceAllString(q, "")
		syn, err := syntax.Parse(fileMatch[1], syntax.Perl)
		if err != nil {
			return nil, nil, err
		}
		subQ := query.RegexpQuery(syn).SQuery("filename")
		requiredSClauses = append(requiredSClauses, subQ)
	}

	// match `lang:go`, `lang:java`, etc.
	// the list of supported languages (and their aliases) is here:
	// https://github.com/github-linguist/linguist/blob/master/lib/linguist/languages.yml
	langMatcher := regexp.MustCompile(`(?:lang:)(?P<lang>[[:graph:]]+)`)
	langMatch := langMatcher.FindStringSubmatch(q)
	if len(langMatch) == 2 {
		q = langMatcher.ReplaceAllString(q, "")
		lang, ok := enry.GetLanguageByAlias(langMatch[1])
		if ok {
			subQ := fmt.Sprintf("(:eq lang %s)", strconv.Quote(strings.ToLower(lang)))
			requiredSClauses = append(requiredSClauses, subQ)
		} else {
			return nil, nil, status.InvalidArgumentErrorf("unknown lang %q", langMatch[1])
		}
	}
	q = strings.TrimSpace(q)
	q = strings.Join(regexOpts, "") + q
	c.log.Infof("parsed query: [%s]", q)

	syn, err := syntax.Parse(q, syntax.Perl)
	if err != nil {
		return nil, nil, err
	}

	// Annoyingly, we have to compile the regexp in the normal way too.
	re, err := regexp.Compile(q)
	if err != nil {
		return nil, nil, err
	}

	scorer := &reScorer{re}
	queryObj := query.RegexpQuery(syn)
	squery := queryObj.SQuery(types.AllFields)

	if len(requiredSClauses) > 0 {
		clauses := strings.Join(requiredSClauses, " ")
		squery = "(:and " + squery + clauses + ")"
	}
	c.log.Infof("squery: %q", squery)
	return scorer, []byte(squery), nil
}

func bm25Params(fieldName string) (k1 float64, b float64) {
	switch fieldName {
	case "filename":
		return 1.2, 0.8
	default:
		return 1.4, 0.9
	}
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

func (c *CodeSearcher) scoreDocs(scorer types.Scorer, candidateDocIDs []uint64, numResults int) ([]uint64, error) {
	start := time.Now()
	numCandidateDocIDs := len(candidateDocIDs)
	scoreMap := make(map[uint64]float64, len(candidateDocIDs))
	var mu sync.Mutex

	// TODO(tylerw): use a priority-queue; stop iteration early.
	g := new(errgroup.Group)
	g.SetLimit(runtime.GOMAXPROCS(0))

	for _, docID := range candidateDocIDs {
		docID := docID
		g.Go(func() error {
			doc, err := c.indexReader.GetStoredDocument(docID, "filename", "content")
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

	sort.Slice(candidateDocIDs, func(i, j int) bool {
		return scoreMap[candidateDocIDs[i]] > scoreMap[candidateDocIDs[j]]
	})

	if len(candidateDocIDs) > numResults {
		candidateDocIDs = candidateDocIDs[:numResults]
	}

	c.log.Infof("Scoring %d docs took %s", numCandidateDocIDs, time.Since(start))
	return candidateDocIDs, nil
}

func (c *CodeSearcher) Search(rawQ string, numResults int) ([]*result.Result, error) {
	searchStart := time.Now()

	scorer, squery, err := c.parse(rawQ)
	if err != nil {
		return nil, err
	}

	candidateDocIDs, err := c.indexReader.RawQuery([]byte(squery))
	if err != nil {
		return nil, err
	}

	candidateDocIDs, err = c.scoreDocs(scorer, candidateDocIDs, numResults)
	if err != nil {
		return nil, err
	}
	docs, err := c.retrieveDocs(candidateDocIDs)
	if err != nil {
		return nil, err
	}
	results := scorer.makeResults(docs)
	c.log.Infof("Search took %s", time.Since(searchStart))

	return results, nil
}
