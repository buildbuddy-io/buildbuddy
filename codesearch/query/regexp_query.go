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

	// allSQuery is what RegexpQuery compiles a pattern to when it can't
	// extract any ngrams from it (e.g. the pattern is shorter than the
	// minimum ngram length, or matches too generally). Such a clause doesn't
	// filter the candidate set at all.
	allSQuery = "(:all)"
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

	// filteredByIndex is true when this matcher contributed real ngrams to
	// the squery, meaning the index already filtered the candidate set
	// through it. Only then does a missing posting prove a doc doesn't
	// match; for unindexable patterns (too short/general to produce ngrams)
	// a missing posting just means "unverified".
	filteredByIndex bool

	// avgFieldLen maps each scored field to its mean length over the
	// candidate set, computed by Prepare on the root scorer. It supplies
	// the avgdl in BM25's per-field length normalization.
	avgFieldLen map[string]float64
}

func (fs *fieldScorer) collectFieldNames(out map[string]bool) {
	if fs.op == Match {
		out[fs.fieldName] = true
	}
	for _, child := range fs.children {
		child.collectFieldNames(out)
	}
}

// Prepare computes per-field average lengths across the candidate set so
// Score can normalize each field's term frequency against that field's own
// typical length.
func (fs *fieldScorer) Prepare(matches []types.DocumentMatch) {
	names := make(map[string]bool)
	fs.collectFieldNames(names)
	sums := make(map[string]float64, len(names))
	counts := make(map[string]int, len(names))
	for _, m := range matches {
		for name := range names {
			if l := m.FieldLength(name); l > 0 {
				sums[name] += float64(l)
				counts[name]++
			}
		}
	}
	fs.avgFieldLen = make(map[string]float64, len(names))
	for name, n := range counts {
		fs.avgFieldLen[name] = sums[name] / float64(n)
	}
}

func (fs *fieldScorer) Skip() bool {
	return fs.op == Noop
}

// fieldScore returns the subtree's score: each matched field contributes
// weight_f * sat(tf_f / B_f), where B_f = 1 - b + b*(len_f/avglen_f)
// normalizes the field's length against that field's candidate-set average
// and sat is BM25's saturating transform. Saturation is applied PER FIELD,
// before weighting and summation, because fields have very different
// term-frequency scales (content ngram frequencies run 10-100x symbol or
// filename frequencies); pooling raw frequencies across fields would leave a
// short field's evidence invisible inside an already-saturated pool. Fields
// are weighted to mirror the method described here:
// https://www.researchgate.net/publication/221613382_Simple_BM25_extension_to_multiple_weighted_fields
func (fs *fieldScorer) fieldScore(docMatch types.DocumentMatch, avgLens map[string]float64) float64 {
	switch fs.op {
	case Match:
		if docMatch == nil {
			return 0
		}
		posting := docMatch.Posting(fs.fieldName)
		if posting == nil {
			if !fs.filteredByIndex {
				// Candidates were never filtered through this matcher's
				// ngrams, so a missing posting means "unverified", not "no
				// match". Contribute neutral evidence so an And doesn't
				// veto the doc, and let Rescore make the exact call.
				return float64(fs.weight) * bm25Sat(1)
			}
			// The index filtered candidates through this matcher's ngrams,
			// so a missing posting means the doc truly doesn't match.
			return 0
		}
		tf := float64(posting.Frequency())
		if tf == 0 {
			return 0
		}
		fieldLen := float64(docMatch.FieldLength(fs.fieldName))
		// A field always holds at least as many tokens as any one term's
		// occurrences, so tf is a lower bound on the true field length.
		// Docs indexed before field lengths were stored report 0; without
		// this floor they'd skip length normalization entirely and
		// outscore reindexed docs.
		if fieldLen < tf {
			fieldLen = tf
		}
		norm := 1.0
		if avg := avgLens[fs.fieldName]; avg > 0 {
			norm = 1 - bm25B + bm25B*(fieldLen/avg)
		}
		return float64(fs.weight) * bm25Sat(tf/norm)
	case Or:
		// Pool the children: a doc matching the term in several fields
		// accumulates evidence from each.
		total := 0.0
		for _, child := range fs.children {
			total += child.fieldScore(docMatch, avgLens)
		}
		return total
	case And:
		// Gate on any clause contributing nothing (AND semantics), summing
		// the rest. Gating lives here, not in Score, so it holds at any
		// nesting depth (e.g. an And nested under an Or).
		total := 0.0
		for _, child := range fs.children {
			s := child.fieldScore(docMatch, avgLens)
			if s == 0 {
				return 0
			}
			total += s
		}
		return total
	case Noop:
		return 1
	default:
		log.Warningf("Unknown scorer operation %d", fs.op)
		return 0 // Should never happen
	}
}

const (
	// Standard BM25 constants, untuned. See
	// https://en.wikipedia.org/wiki/Okapi_BM25#The_ranking_function.
	bm25K1 = 1.2
	bm25B  = 0.75
)

// bm25Sat applies BM25's saturating transform to a length-normalized term
// frequency. Per-field length normalization happens in fieldScore, so no
// document-length term appears here.
func bm25Sat(tf float64) float64 {
	if tf <= 0 {
		return 0
	}
	return (tf * (bm25K1 + 1)) / (tf + bm25K1)
}

func newNoopScorer() *fieldScorer {
	return &fieldScorer{
		op: Noop,
	}
}

func newFieldScorer(fieldName string, weight int, matcher *dfa.Regexp, filteredByIndex bool) *fieldScorer {
	return &fieldScorer{
		op:              Match,
		fieldName:       fieldName,
		weight:          weight,
		matcher:         matcher,
		filteredByIndex: filteredByIndex,
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

func (fs *fieldScorer) Score(docMatch types.DocumentMatch) float64 {
	return fs.fieldScore(docMatch, fs.avgFieldLen)
}

// rescoreFieldScore mirrors fieldScore, but computes exact match counts by
// running the field matchers against the stored document contents instead of
// approximating with index-side term frequencies.
func (fs *fieldScorer) rescoreFieldScore(docMatch types.DocumentMatch, doc types.Document, avgLens map[string]float64) float64 {
	switch fs.op {
	case Match:
		if fs.matcher == nil {
			// A matcher-less scorer has nothing to verify against the stored
			// document (e.g. exact keyword-field lookups, which produce no
			// trigram false positives, and whose fields aren't stored), so
			// the index-side score is already the exact score.
			return fs.fieldScore(docMatch, avgLens)
		}
		contents := doc.Field(fs.fieldName).Contents()
		tf := float64(len(match(fs.matcher.Clone(), contents)))
		if tf == 0 {
			return 0
		}
		fieldLen := 0.0
		if docMatch != nil {
			fieldLen = float64(docMatch.FieldLength(fs.fieldName))
		}
		// Same floor as fieldScore: docs indexed before field lengths were
		// stored report FieldLength 0.
		if fieldLen < tf {
			fieldLen = tf
		}
		norm := 1.0
		if avg := avgLens[fs.fieldName]; avg > 0 {
			norm = 1 - bm25B + bm25B*(fieldLen/avg)
		}
		return float64(fs.weight) * bm25Sat(tf/norm)
	case Or:
		total := 0.0
		for _, child := range fs.children {
			total += child.rescoreFieldScore(docMatch, doc, avgLens)
		}
		return total
	case And:
		// Gate at any depth, mirroring fieldScore.
		total := 0.0
		for _, child := range fs.children {
			s := child.rescoreFieldScore(docMatch, doc, avgLens)
			if s == 0 {
				return 0
			}
			total += s
		}
		return total
	case Noop:
		return 1
	default:
		log.Warningf("Unknown scorer operation %d", fs.op)
		return 0 // Should never happen
	}
}

func (fs *fieldScorer) Rescore(docMatch types.DocumentMatch, doc types.Document) float64 {
	return fs.rescoreFieldScore(docMatch, doc, fs.avgFieldLen)
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
	var snippetText strings.Builder

	firstLine := max(lineNumber-linesBefore, 1)
	lastLine := lineNumber + linesAfter

	for n := firstLine; n <= lastLine; n++ {
		buf := extractLine(rm.field.Contents(), n)
		if len(bytes.TrimSuffix(buf, []byte{'\n'})) == 0 && n < lineNumber {
			// Skip blank lines before the matched line.
			continue
		}
		snippetText.WriteString(makeLine(buf, n))
	}
	return snippetText.String()
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

// symbolsWeight is the BM25 field weight for symbol-definition matches: an
// identifier-shaped query term additionally matches the symbols field (the
// tree-sitter extracted declaration names) at this weight, so a file that
// declares the queried name scores above files that merely use it. Documents
// indexed without the field contribute no symbols postings, so this is safe
// against older indexes.
const symbolsWeight = 2

// identifierTerm reports whether the query term is a bare identifier (word
// characters only, no regex metacharacters), returning it lowercased to match
// the keyword tokenizer's normalization.
func identifierTerm(qTerm string) (string, bool) {
	term := strings.TrimSuffix(strings.TrimPrefix(qTerm, `"`), `"`)
	if term == "" {
		return "", false
	}
	for _, r := range term {
		if r != '_' && (r < '0' || r > '9') && (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') {
			return "", false
		}
	}
	return strings.ToLower(term), true
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
		scorer = andScorers(scorer, newFieldScorer(filenameField, 2, fileMatchRe, subQ != allSQuery))
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
		hasIdentifierTerm := false
		// The term scorers below cover all terms at once (via an OR'd
		// matcher), so a field counts as index-filtered if any term
		// contributed real ngrams for it.
		contentFiltered := false
		filenameFiltered := false
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
			contentFiltered = contentFiltered || subQContent != allSQuery
			filenameFiltered = filenameFiltered || subQFilename != allSQuery
			clause := subQContent + " " + subQFilename
			if id, ok := identifierTerm(qTerm); ok {
				// Scoring evidence only: any doc whose symbols contain the
				// term also matches the content ngram clause, so the candidate
				// set is unchanged.
				clause += fmt.Sprintf(" (:eq %s %s)", types.SymbolsField, strconv.Quote(id))
				hasIdentifierTerm = true
			}
			sClauses = append(sClauses, "(:or "+clause+")")
		}

		// Build a content matcher that will match any of the query terms.
		re, err := reForQueryTerms(queryTerms, flagString)
		if err != nil {
			return nil, err
		}

		contentScorer := orScorers(
			// Weight 2 because content matches should be more important than non-explicit
			// filename matches.
			newFieldScorer(contentField, 2, re, contentFiltered),
			newFieldScorer(filenameField, 1, re, filenameFiltered),
		)
		if hasIdentifierTerm {
			// Declaring the queried name is the strongest content evidence.
			// No matcher: keyword-field postings are exact, and the field is
			// not stored, so Rescore reuses the index-side score.
			contentScorer = orScorers(
				newFieldScorer(types.SymbolsField, symbolsWeight, nil, true),
				contentScorer,
			)
		}
		scorer = andScorers(scorer, contentScorer)
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
