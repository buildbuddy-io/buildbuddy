package query

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"regexp/syntax"
	"strconv"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/codesearch/dfa"
	"github.com/buildbuddy-io/buildbuddy/codesearch/filters"
	"github.com/buildbuddy-io/buildbuddy/codesearch/token"
	"github.com/buildbuddy-io/buildbuddy/codesearch/types"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

const (
	// TODO(tylerw): These should not be defined in this file.
	// Find a way to specify them from the indexer / searcher?
	filenameField = "filename"
	contentField  = "content"
)

var (
	nl = []byte{'\n'}

	_ types.Query             = (*ReQuery)(nil)
	_ types.HighlightedRegion = (*regionMatch)(nil)
	_ types.Scorer            = (*fieldScorer)(nil)
)

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

type region struct {
	startOffset int
	endOffset   int
	lineNumber  int
}

func match(re *dfa.Regexp, buf []byte) []region {
	results := make([]region, 0)
	var (
		lineno    = 1
		count     = 0
		beginText = true
		endText   = true
	)

	end := len(buf)
	chunkStart := 0
	for chunkStart < end {
		m1 := re.Match(buf[chunkStart:end], beginText, endText) + chunkStart
		beginText = false
		if m1 < chunkStart {
			break
		}
		lineStart := bytes.LastIndex(buf[chunkStart:m1], nl) + 1 + chunkStart
		lineEnd := m1 + 1
		if lineEnd > end {
			lineEnd = end
		}
		lineno += countNL(buf[chunkStart:lineStart])
		results = append(results, region{
			startOffset: lineStart,
			endOffset:   lineEnd,
			lineNumber:  lineno,
		})
		count++
		lineno++
		chunkStart = lineEnd
	}
	lineno += countNL(buf[chunkStart:end])
	return results
}

type scorerOp int

const (
	Match scorerOp = iota
	Or
	And
	Noop
)

// fieldScorer scores documents based on how well they match the given scorers.
// Scorers can be combined using AND and OR operations, which allows scoring to mirror
// the structure of queries.
// TODO(jdelfino): simplify to just be an array of scorers? or leave it generalized?
type fieldScorer struct {
	op        scorerOp
	fieldName string
	weight    int
	matcher   *dfa.Regexp
	children  []*fieldScorer
}

func (fs *fieldScorer) Skip() bool {
	return fs.op == Noop
}

func (fs *fieldScorer) scoreInternal(doc types.Document) (matchCount, tokenCount int) {
	// We adapt BM25 scoring to work with multiple fields by counting matches and tokens from each
	// field, summing them up, then running BM25 on those numbers. Fields can be weighted - the
	// counts from a field (both token and match) are duplicated `weight` times.
	// This approach is meant to match the method describe here:
	// https://www.researchgate.net/publication/221613382_Simple_BM25_extension_to_multiple_weighted_fields

	switch fs.op {
	case Match:
		contents := doc.Field(fs.fieldName).Contents()
		m := match(fs.matcher.Clone(), contents)
		matchCount = len(m) * fs.weight
		tokenCount = len(strings.Fields(string(contents))) * fs.weight
		return matchCount, tokenCount
	case Or:
		// Sum up the scores of all children. Arguably, we could take the highest score,
		// but it seems more useful to boost queries that match more terms.
		matchCount = 0.0
		tokenCount = 0.0
		for _, child := range fs.children {
			m, t := child.scoreInternal(doc)
			matchCount += m
			tokenCount += t
		}
		return matchCount, tokenCount
	case And:
		// Same os OR, but if any child scores 0, the overall score is 0.
		matchCount = 0.0
		tokenCount = 0.0

		for _, child := range fs.children {
			m, t := child.scoreInternal(doc)
			if m == 0.0 {
				return 0.0, 0.0
			}
			matchCount += m
			tokenCount += t
		}
		return matchCount, tokenCount
	case Noop:
		return 1.0, 1.0
	default:
		log.Warningf("Unknown scorer operation %d", fs.op)
		return 0.0, 0.0 // Should never happen
	}
}

func bm25Score(f_qi_d, D float64) float64 {
	// See https://en.wikipedia.org/wiki/Okapi_BM25#The_ranking_function for
	// the formula for BM25 scoring. k1 and b are left at "default" values here, and haven't been
	// tuned.
	k1 := 1.2
	b := 0.75
	return (f_qi_d * (k1 + 1)) / (f_qi_d + k1*(1-b+b*D))
}

func newNoopScorer() *fieldScorer {
	return &fieldScorer{
		op: Noop,
	}
}

func newFieldScorer(fieldName string, weight int, matcher *dfa.Regexp) *fieldScorer {
	return &fieldScorer{
		op:        Match,
		fieldName: fieldName,
		weight:    weight,
		matcher:   matcher,
	}
}

func andScorers(a *fieldScorer, b *fieldScorer) *fieldScorer {
	if a.op == Noop {
		return b
	}
	if b.op == Noop {
		return a
	}
	return &fieldScorer{
		op:       And,
		children: []*fieldScorer{a, b},
	}
}

func orScorers(a *fieldScorer, b *fieldScorer) *fieldScorer {
	if a.op == Noop {
		return b
	}
	if b.op == Noop {
		return a
	}
	return &fieldScorer{
		op:       Or,
		children: []*fieldScorer{a, b},
	}
}

func (fs *fieldScorer) Score(docMatch types.DocumentMatch, doc types.Document) float64 {
	matchCount, tokenCount := fs.scoreInternal(doc)
	return bm25Score(float64(matchCount), float64(tokenCount))
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

type reHighlighter struct {
	contentMatcher *dfa.Regexp
}

type regionMatch struct {
	field  types.Field
	region region
}

func (rm regionMatch) FieldName() string {
	return rm.field.Name()
}

func makeLine(line []byte, lineNumber int) string {
	return fmt.Sprintf("%d: %s\n", lineNumber, line)
}

func (rm regionMatch) Line() int {
	return rm.region.lineNumber
}

func (rm regionMatch) CustomSnippet(linesBefore, linesAfter int) string {
	lineNumber := rm.region.lineNumber
	snippetText := ""

	firstLine := max(lineNumber-linesBefore, 1)
	lastLine := lineNumber + linesAfter

	for n := firstLine; n <= lastLine; n++ {
		buf := extractLine(rm.field.Contents(), n)
		if len(bytes.TrimSuffix(buf, []byte{'\n'})) == 0 && n < lineNumber {
			// Skip blank lines before the matched line.
			continue
		}
		snippetText += makeLine(buf, n)
	}
	return snippetText
}

func (rm regionMatch) String() string {
	return rm.CustomSnippet(0, 0)
}

func (h *reHighlighter) Highlight(doc types.Document) []types.HighlightedRegion {
	results := make([]types.HighlightedRegion, 0)

	field := doc.Field(contentField)
	if h.contentMatcher != nil {
		for _, region := range match(h.contentMatcher.Clone(), field.Contents()) {
			region := region
			results = append(results, types.HighlightedRegion(regionMatch{
				field:  field,
				region: region,
			}))
		}
	}

	// Minor hack: If there are no matching content regions, add a fake region that matches the first
	// line of the file. This way filter-only queries will be able to display a highlighted region.
	// Note that we can rely on scoring to have filtered out non-matches, so it's safe to
	// assume this doc is a match.
	if len(results) == 0 {
		results = append(results, types.HighlightedRegion(regionMatch{
			field: field,
			region: region{
				lineNumber:  1,
				startOffset: 0,
				endOffset:   0,
			},
		}))
	}
	return results
}

type ReQuery struct {
	ctx    context.Context
	log    log.Logger
	parsed string
	squery string

	scorer         *fieldScorer
	contentMatcher *dfa.Regexp
}

func expressionToSquery(expr string, fieldName string) (string, error) {
	syn, err := syntax.Parse(expr, syntax.Perl)
	if err != nil {
		return "", err
	}
	return RegexpQuery(syn).SQuery(fieldName), nil
}

func NewReQuery(ctx context.Context, q string) (*ReQuery, error) {
	subLog := log.NamedSubLogger("regexp-query")
	subLog.Infof("raw query: [%s]", q)

	// A list of s-expression strings that must be satisfied by the query.
	// (added to the query with AND)
	sClauses := make([]string, 0)

	// Regex options that will be applied to the main query only.
	regexFlags := "m" // always use multiline mode.

	q, caseSensitive := filters.ExtractCaseSensitivity(q)
	if !caseSensitive {
		regexFlags += "i"
	}

	q, filename := filters.ExtractFilenameFilter(q)
	scorer := newNoopScorer()
	var contentMatcher *dfa.Regexp

	if len(filename) > 0 {
		subQ, err := expressionToSquery(filename, filenameField)
		if err != nil {
			return nil, status.InvalidArgumentError(err.Error())
		}
		sClauses = append(sClauses, subQ)
		fileMatchRe, err := dfa.Compile(filename)
		if err != nil {
			return nil, status.InvalidArgumentError(err.Error())
		}
		// Weight 2 because explicit filename matches should be more impactful to ranking than
		// non-explicit filename matches.
		scorer = andScorers(scorer, newFieldScorer(filenameField, 2, fileMatchRe))
	}

	q, lang := filters.ExtractLanguageFilter(q)
	if len(lang) > 0 {
		subQ := fmt.Sprintf("(:eq language %s)", strconv.Quote(strings.ToLower(lang)))
		sClauses = append(sClauses, subQ)
	}

	q, repo := filters.ExtractRepoFilter(q)
	if len(repo) > 0 {
		subQ := fmt.Sprintf("(:eq repo %s)", strconv.Quote(repo))
		sClauses = append(sClauses, subQ)
	}

	q = strings.TrimSpace(q)
	if len(q) > 0 {
		flagString := "(?" + regexFlags + ")"

		r := csv.NewReader(strings.NewReader(q))
		r.Comma = ' '
		queryTerms, err := r.Read()
		if err != nil {
			return nil, err
		}
		for _, qTerm := range queryTerms {
			expr := flagString + strings.TrimSuffix(strings.TrimPrefix(qTerm, `"`), `"`)
			syn, err := syntax.Parse(expr, syntax.Perl)
			if err != nil {
				return nil, status.InvalidArgumentError(err.Error())
			}
			// TODO(jdelfino): This should really be derived from the tokenizer specified on the
			// field in the schema.
			subQContent := RegexpQuery(syn, token.WithMaxNgramLength(6), token.WithLowerCase(true)).SQuery(contentField)
			subQFilename := RegexpQuery(syn).SQuery(filenameField)
			sClauses = append(sClauses, "(:or "+subQContent+" "+subQFilename+")")
		}

		// Build a content matcher that will match any of the query terms.
		re, err := reForQueryTerms(queryTerms, flagString)
		if err != nil {
			return nil, err
		}

		scorer = andScorers(scorer,
			orScorers(
				// Weight 2 because content matches should be more important than non-explicit
				// filename matches.
				newFieldScorer(contentField, 2, re),
				newFieldScorer(filenameField, 1, re),
			))
		contentMatcher = re

	}
	subLog.Infof("parsed query: [%s]", q)

	squery := ""
	if len(sClauses) == 1 {
		squery = sClauses[0]
	} else if len(sClauses) > 1 {
		squery = "(:and " + strings.Join(sClauses, " ") + ")"
	}

	req := &ReQuery{
		ctx:            ctx,
		log:            subLog,
		squery:         squery,
		parsed:         q,
		scorer:         scorer,
		contentMatcher: contentMatcher,
	}
	return req, nil
}

func reForQueryTerms(queryTerms []string, flags string) (*dfa.Regexp, error) {
	// Build a regexp that matches any of the query terms.
	for i, qTerm := range queryTerms {
		queryTerms[i] = "(" + qTerm + ")"
	}
	q := flags + strings.Join(queryTerms, "|")
	re, err := dfa.Compile(q)
	if err != nil {
		return nil, status.InvalidArgumentError(err.Error())
	}
	return re, nil
}

func (req *ReQuery) SQuery() string {
	return req.squery
}

func (req *ReQuery) ParsedQuery() string {
	return req.parsed
}

func (req *ReQuery) Scorer() types.Scorer {
	return req.scorer
}

func (req *ReQuery) Highlighter() types.Highlighter {
	return &reHighlighter{req.contentMatcher}
}

// TESTONLY: return content matcher to verify regexp params.
func (req *ReQuery) TestOnlyContentMatcher() *dfa.Regexp {
	return req.contentMatcher
}
