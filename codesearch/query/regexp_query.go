package query

import (
	"bufio"
	"bytes"

	"fmt"
	"regexp"
	"regexp/syntax"
	"strconv"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/codesearch/types"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	"github.com/go-enry/go-enry/v2"
)

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

type region struct {
	startOffset int
	endOffset   int
	lineNumber  int
}

type reScorer struct {
	re *regexp.Regexp
}

func match(re *regexp.Regexp, buf []byte) []region {
	matchIndexes := re.FindAllIndex(buf, -1)
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
		matchingRegions := match(s.re, field.Contents())
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
	case "filename":
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
	re *regexp.Regexp
}

type regionMatch struct {
	fieldName string
	snippet   string
}

func (rm regionMatch) FieldName() string {
	return rm.fieldName
}
func (rm regionMatch) String() string {
	return rm.snippet
}

func (h *reHighlighter) Highlight(doc types.Document) []types.HighlightedRegion {
	results := make([]types.HighlightedRegion, 0)
	for _, fieldName := range doc.Fields() {
		field := doc.Field(fieldName)
		for _, region := range match(h.re, field.Contents()) {
			line := extractLine(field.Contents(), region.lineNumber)
			results = append(results, types.HighlightedRegion(regionMatch{
				fieldName: fieldName,
				snippet:   fmt.Sprintf("%d: %s\n", region.lineNumber, line),
			}))
		}
	}
	return results
}

type ReQuery struct {
	log        log.Logger
	squery     []byte
	numResults int
	re         *regexp.Regexp
}

func NewReQuery(q string, numResults int) (types.Query, error) {
	subLog := log.NamedSubLogger("searcher")
	subLog.Infof("raw query: [%s]", q)

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
			return nil, err
		}
		subQ := RegexpQuery(syn).SQuery("filename")
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
			return nil, status.InvalidArgumentErrorf("unknown lang %q", langMatch[1])
		}
	}
	q = strings.TrimSpace(q)
	q = strings.Join(regexOpts, "") + q
	subLog.Infof("parsed query: [%s]", q)

	syn, err := syntax.Parse(q, syntax.Perl)
	if err != nil {
		return nil, err
	}

	// Annoyingly, we have to compile the regexp in the normal way too.
	re, err := regexp.Compile(q)
	if err != nil {
		return nil, err
	}
	queryObj := RegexpQuery(syn)
	squery := queryObj.SQuery(types.AllFields)
	if len(requiredSClauses) > 0 {
		clauses := strings.Join(requiredSClauses, " ")
		squery = "(:and " + squery + clauses + ")"
	}
	subLog.Infof("squery: %q", squery)

	req := &ReQuery{
		log:        subLog,
		squery:     []byte(squery),
		numResults: numResults,
		re:         re,
	}
	return req, nil
}

func (req *ReQuery) SQuery() []byte {
	return req.squery
}

func (req *ReQuery) NumResults() int {
	return req.numResults
}

func (req *ReQuery) GetScorer() types.Scorer {
	return &reScorer{req.re}
}

func (req *ReQuery) GetHighlighter() types.Highlighter {
	return &reHighlighter{req.re}
}
