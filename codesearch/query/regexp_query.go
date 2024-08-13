package query

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"regexp"
	"regexp/syntax"
	"strconv"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/codesearch/dfa"
	"github.com/buildbuddy-io/buildbuddy/codesearch/token"
	"github.com/buildbuddy-io/buildbuddy/codesearch/types"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/go-enry/go-enry/v2"
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
	_ types.Scorer            = (*reScorer)(nil)
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

type reScorer struct {
	fieldMatchers map[string]*dfa.Regexp
	skip          bool
}

func (s *reScorer) Skip() bool {
	return s.skip
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

func (s *reScorer) Score(docMatch types.DocumentMatch, doc types.Document) float64 {
	docScore := 0.0
	for _, fieldName := range docMatch.FieldNames() {
		re, ok := s.fieldMatchers[fieldName]
		if !ok {
			continue
		}
		field := doc.Field(fieldName)
		if len(field.Contents()) == 0 {
			continue
		}
		matchingRegions := match(re.Clone(), field.Contents())
		f_qi_d := float64(len(matchingRegions))
		D := float64(len(strings.Fields(string(field.Contents()))))
		k1, b := bm25Params(field.Name())
		fieldScore := (f_qi_d * (k1 + 1)) / (f_qi_d + k1*(1-b+b*D))
		docScore += fieldScore
	}
	return docScore
}

func bm25Params(fieldName string) (k1 float64, b float64) {
	switch fieldName {
	case filenameField:
		return 1.2, 0.8
	default:
		return 1.4, 0.9
	}
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
	fieldMatchers map[string]*dfa.Regexp
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
	matcher, ok := h.fieldMatchers[contentField]
	if ok {
		for _, region := range match(matcher.Clone(), field.Contents()) {
			region := region
			results = append(results, types.HighlightedRegion(regionMatch{
				field:  field,
				region: region,
			}))
		}
	}

	// HACK: if there are no matching regions, add a fake one that matches
	// the first line of the file. This way filter-only queries will be able
	// to display a highlighted region.
	if len(results) == 0 && h.fieldMatchers[contentField] == nil {
		field := doc.Field(contentField)
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
	ctx           context.Context
	log           log.Logger
	parsed        string
	squery        []byte
	numResults    int
	fieldMatchers map[string]*dfa.Regexp
}

func expressionToSquery(expr string, fieldName string) (string, error) {
	syn, err := syntax.Parse(expr, syntax.Perl)
	if err != nil {
		return "", err
	}
	return RegexpQuery(syn).SQuery(fieldName), nil
}

func NewReQuery(ctx context.Context, q string, numResults int) (*ReQuery, error) {
	subLog := log.NamedSubLogger("regexp-query")
	subLog.Infof("raw query: [%s]", q)

	// A list of s-expression strings that must be satisfied by the query.
	// (added to the query with AND)
	requiredSClauses := make([]string, 0)

	// Regex options that will be applied to the main query only.
	regexFlags := "m" // always use multiline mode.

	// Regexp matches (for highlighting) by fieldname.
	fieldMatchers := make(map[string]*dfa.Regexp)

	// Match `case:yes` or `case:y` and enable case-sensitive searches.
	caseMatcher := regexp.MustCompile(`case:(yes|y|no|n)`)
	caseMatch := caseMatcher.FindStringSubmatch(q)
	if len(caseMatch) == 2 {
		q = caseMatcher.ReplaceAllString(q, "")
		if strings.HasPrefix(caseMatch[1], "n") {
			regexFlags += "i"
		}
	} else {
		// otherwise default to case-insensitive
		regexFlags += "i"
	}

	// match `file:test.js`, `f:test.js`, and `path:test.js`
	fileMatcher := regexp.MustCompile(`(?:file:|f:|path:)(?P<filepath>[[:graph:]]+)`)
	fileMatch := fileMatcher.FindStringSubmatch(q)

	if len(fileMatch) == 2 {
		q = fileMatcher.ReplaceAllString(q, "")
		subQ, err := expressionToSquery(fileMatch[1], filenameField)
		if err != nil {
			return nil, err
		}
		requiredSClauses = append(requiredSClauses, subQ)
		fileMatchRe, err := dfa.Compile(fileMatch[1])
		if err != nil {
			return nil, err
		}
		fieldMatchers[filenameField] = fileMatchRe
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
			subQ := fmt.Sprintf("(:eq language %s)", strconv.Quote(strings.ToLower(lang)))
			requiredSClauses = append(requiredSClauses, subQ)
		} else {
			return nil, status.InvalidArgumentErrorf("unknown lang %q", langMatch[1])
		}
	}

	q = strings.TrimSpace(q)
	sQueries := make([]string, 0)
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
				return nil, err
			}
			subQ := RegexpQuery(syn, token.WithMaxNgramLength(6), token.WithLowerCase(true)).SQuery(contentField)
			sQueries = append(sQueries, subQ)
		}

		// Build a content matcher that will match any of the query terms.
		for i, qTerm := range queryTerms {
			queryTerms[i] = "(" + qTerm + ")"
		}
		q = flagString + strings.Join(queryTerms, "|")
		re, err := dfa.Compile(q)
		if err != nil {
			return nil, err
		}
		fieldMatchers[contentField] = re

		// If there is a content matcher, and there is not already a
		// filename matcher, allow filenames that match the query too.
		if _, ok := fieldMatchers[filenameField]; !ok {
			fieldMatchers[filenameField] = re
		}
	}
	subLog.Infof("parsed query: [%s]", q)

	squery := ""
	if len(sQueries) == 1 {
		squery = sQueries[0]
	} else if len(sQueries) > 1 {
		squery = "(:and " + strings.Join(sQueries, " ") + ")"
	}

	if len(requiredSClauses) > 0 {
		var clauses string
		if len(requiredSClauses) == 1 {
			clauses = requiredSClauses[0]
		} else {
			clauses = strings.Join(requiredSClauses, " ")
		}
		squery = "(:and " + squery + " " + clauses + ")"
	}
	subLog.Infof("squery: %q", squery)

	req := &ReQuery{
		ctx:           ctx,
		log:           subLog,
		squery:        []byte(squery),
		parsed:        q,
		numResults:    numResults,
		fieldMatchers: fieldMatchers,
	}
	return req, nil
}

func (req *ReQuery) SQuery() []byte {
	return req.squery
}

func (req *ReQuery) ParsedQuery() string {
	return req.parsed
}

func (req *ReQuery) NumResults() int {
	return req.numResults
}

func (req *ReQuery) GetScorer() types.Scorer {
	return &reScorer{
		fieldMatchers: req.fieldMatchers,
		skip:          len(req.fieldMatchers) == 0,
	}
}

func (req *ReQuery) GetHighlighter() types.Highlighter {
	return &reHighlighter{req.fieldMatchers}
}

// TESTONLY: return field matchers to verify regexp params.
func (req *ReQuery) TestOnlyFieldMatchers() map[string]*dfa.Regexp {
	return req.fieldMatchers
}
