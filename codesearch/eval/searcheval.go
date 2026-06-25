// searcheval compares codesearch result quality against cs.opensource.google.
//
// It runs a fixed query set (queries.jsonl) against a local index (by
// shelling out to the codesearch cli binary) and against Google's public Code
// Search instance (the Grimoire JSON-RPC endpoint that backs
// cs.opensource.google), then reports ranking-agreement and target-recall
// metrics.
//
// Google responses are cached on disk (one JSON file per query id) and only
// fetched when missing, with a delay between network requests, so repeated
// eval runs never hit the API.
//
// Usage:
//
//	bb build //codesearch/cmd/cli //codesearch/eval:searcheval
//	bazel-bin/codesearch/eval/searcheval_/searcheval \
//	  -cli=bazel-bin/codesearch/cmd/cli/cli_/cli \
//	  -index_dir=$PWD/codesearch/eval/index_go -namespace=eval \
//	  -queries=$PWD/codesearch/eval/queries.jsonl \
//	  -cache_dir=$PWD/codesearch/eval/cache/google \
//	  -report_dir=$PWD/codesearch/eval/reports
package main

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	cliPath   = flag.String("cli", "bazel-bin/codesearch/cmd/cli/cli_/cli", "Path to the codesearch cli binary")
	indexDir  = flag.String("index_dir", "", "Path to the local index directory")
	namespace = flag.String("namespace", "eval", "Index namespace")
	queries   = flag.String("queries", "", "Path to queries.jsonl")
	cacheDir  = flag.String("cache_dir", "", "Directory holding cached Google responses (one file per query id)")
	reportDir = flag.String("report_dir", "", "Directory to write results.jsonl and summary.md")

	numResults   = flag.Int("results", 50, "Results to retrieve per query from each engine")
	stripPrefix  = flag.String("strip_prefix", "eval/corpus/go/", "Path marker; local result filenames are trimmed through its last occurrence so they match Google's repo-relative paths (works for both absolute and relative index paths)")
	cliExtraArgs = flag.String("cli_extra_args", "", "Extra space-separated args passed to `cli search` (e.g. \"-rescore_mode=gate\")")

	zoektPath      = flag.String("zoekt", "", "Path to a zoekt CLI binary; empty disables the zoekt comparison")
	zoektIndexDir  = flag.String("zoekt_index_dir", "", "Path to the zoekt index directory")
	zoektExtraArgs = flag.String("zoekt_extra_args", "", "Extra space-separated args passed to zoekt (e.g. \"-bm25\")")

	allowFetch = flag.Bool("allow_fetch", false, "Permit network requests to Google for cache misses. Off by default: a cache miss is an error so we never hit Google's API accidentally.")
	maxFetches = flag.Int("max_fetches", 150, "Safety cap on network requests per run")
	fetchDelay = flag.Duration("fetch_delay", 2500*time.Millisecond, "Delay between network requests")
	ossProject = flag.String("oss_project", "go", "Grimoire ossProject scope")
	repoName   = flag.String("repo_name", "go", "Grimoire repositoryName scope")
	// This is the referer-restricted public key served in the cs.opensource.google
	// page HTML, not a secret.
	apiKey = flag.String("api_key", "AIzaSyBsu036b7m6R75fTF6ZTbuv5J8lm9f_2d4", "Grimoire API key")
)

type evalQuery struct {
	ID          string   `json:"id"`
	Category    string   `json:"category"`
	Query       string   `json:"query"`
	GoogleQuery string   `json:"google_query,omitempty"` // override when syntaxes diverge
	Targets     []string `json:"targets,omitempty"`      // acceptable "right answer" files
	SkipGoogle  bool     `json:"skip_google,omitempty"`
}

type queryResult struct {
	ID                  string   `json:"id"`
	Category            string   `json:"category"`
	Query               string   `json:"query"`
	Targets             []string `json:"targets,omitempty"`
	Local               []string `json:"local"`
	Google              []string `json:"google"`
	Zoekt               []string `json:"zoekt,omitempty"`
	LocalErr            string   `json:"local_err,omitempty"`
	GoogleErr           string   `json:"google_err,omitempty"`
	ZoektErr            string   `json:"zoekt_err,omitempty"`
	LocalTargetRank     int      `json:"local_target_rank"`  // 1-based; 0 = not found
	GoogleTargetRank    int      `json:"google_target_rank"` // 1-based; 0 = not found
	ZoektTargetRank     int      `json:"zoekt_target_rank"`  // 1-based; 0 = not found
	Jaccard10           float64  `json:"jaccard10"`
	GoogleTop1LocalRank int      `json:"google_top1_local_rank"` // where our ranking puts Google's #1
}

func main() {
	flag.Parse()
	for name, v := range map[string]string{"index_dir": *indexDir, "queries": *queries, "cache_dir": *cacheDir, "report_dir": *reportDir} {
		if v == "" {
			fatalf("missing required flag -%s", name)
		}
	}
	if _, err := os.Stat(*cliPath); err != nil {
		fatalf("cli binary not found at %q (build it with `bb build //codesearch/cmd/cli` or pass -cli)", *cliPath)
	}
	qs, err := loadQueries(*queries)
	if err != nil {
		fatalf("loading queries: %s", err)
	}
	if err := os.MkdirAll(*cacheDir, 0755); err != nil {
		fatalf("%s", err)
	}
	if err := os.MkdirAll(*reportDir, 0755); err != nil {
		fatalf("%s", err)
	}

	fetches := 0
	results := make([]*queryResult, 0, len(qs))
	for i, q := range qs {
		r := &queryResult{ID: q.ID, Category: q.Category, Query: q.Query, Targets: q.Targets}

		files, err := localSearch(q.Query)
		if err != nil {
			r.LocalErr = err.Error()
		}
		r.Local = files

		if !q.SkipGoogle {
			gfiles, fetched, err := googleResults(q, &fetches)
			if err != nil {
				r.GoogleErr = err.Error()
			}
			r.Google = gfiles
			if fetched {
				fmt.Fprintf(os.Stderr, "[%d/%d] fetched %q from Google (%d results)\n", i+1, len(qs), q.ID, len(gfiles))
				time.Sleep(*fetchDelay)
			}
		}

		if *zoektPath != "" {
			zfiles, err := zoektSearch(q.Query)
			if err != nil {
				r.ZoektErr = err.Error()
			}
			r.Zoekt = zfiles
		}

		r.LocalTargetRank = rankOf(r.Local, q.Targets)
		r.GoogleTargetRank = rankOf(r.Google, q.Targets)
		r.ZoektTargetRank = rankOf(r.Zoekt, q.Targets)
		r.Jaccard10 = jaccard(top(r.Local, 10), top(r.Google, 10))
		if len(r.Google) > 0 {
			r.GoogleTop1LocalRank = rankOf(r.Local, r.Google[:1])
		}
		results = append(results, r)
	}

	if err := writeResults(filepath.Join(*reportDir, "results.jsonl"), results); err != nil {
		fatalf("%s", err)
	}
	summary := summarize(results)
	if err := os.WriteFile(filepath.Join(*reportDir, "summary.md"), []byte(summary), 0644); err != nil {
		fatalf("%s", err)
	}
	fmt.Println(summary)
	fmt.Fprintf(os.Stderr, "wrote %s and %s (%d network fetches)\n",
		filepath.Join(*reportDir, "results.jsonl"), filepath.Join(*reportDir, "summary.md"), fetches)
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

func loadQueries(path string) ([]evalQuery, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var qs []evalQuery
	seen := make(map[string]bool)
	for i, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "//") {
			continue
		}
		var q evalQuery
		if err := json.Unmarshal([]byte(line), &q); err != nil {
			return nil, fmt.Errorf("line %d: %s", i+1, err)
		}
		if q.ID == "" || q.Query == "" {
			return nil, fmt.Errorf("line %d: id and query are required", i+1)
		}
		if seen[q.ID] {
			return nil, fmt.Errorf("line %d: duplicate id %q", i+1, q.ID)
		}
		seen[q.ID] = true
		qs = append(qs, q)
	}
	return qs, nil
}

// Local search (shells out to the codesearch cli)

// resultLineRE matches the file-header lines `cli search` prints, e.g.:
//
//	"src/fmt/print.go" [42 matches]
var resultLineRE = regexp.MustCompile(`^"(.+)" \[\d+ matches\]$`)

func localSearch(pat string) ([]string, error) {
	args := []string{
		"search",
		"-index_dir", *indexDir,
		"-namespace", *namespace,
		"-results", strconv.Itoa(*numResults),
		"-snippets", "0",
	}
	if *cliExtraArgs != "" {
		args = append(args, strings.Fields(*cliExtraArgs)...)
	}
	args = append(args, pat)

	cmd := exec.Command(*cliPath, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("cli search failed: %s: %.300s", err, stderr.String())
	}

	var files []string
	for line := range strings.SplitSeq(stdout.String(), "\n") {
		m := resultLineRE.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		name, err := strconv.Unquote(`"` + m[1] + `"`)
		if err != nil {
			name = m[1]
		}
		if i := strings.LastIndex(name, *stripPrefix); i >= 0 {
			name = name[i+len(*stripPrefix):]
		}
		files = append(files, name)
	}
	return files, nil
}

// zoektSearch shells out to the zoekt CLI in list mode (-l), which prints one
// repo-relative filename per line in rank order.
func zoektSearch(pat string) ([]string, error) {
	args := []string{"-index_dir", *zoektIndexDir, "-l"}
	if *zoektExtraArgs != "" {
		args = append(args, strings.Fields(*zoektExtraArgs)...)
	}
	args = append(args, pat)

	cmd := exec.Command(*zoektPath, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("zoekt failed: %s: %.300s", err, stderr.String())
	}

	var files []string
	seen := make(map[string]bool)
	for line := range strings.SplitSeq(stdout.String(), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || seen[line] {
			continue
		}
		seen[line] = true
		files = append(files, line)
		if len(files) >= *numResults {
			break
		}
	}
	return files, nil
}

// Google (Grimoire) search with disk cache

// rateLimited is set when Google returns HTTP 429; all later fetches in the
// run are skipped so we don't pile on.
var rateLimited bool

type cacheEntry struct {
	Query      string          `json:"query"`
	OssProject string          `json:"oss_project"`
	RepoName   string          `json:"repo_name"`
	PageSize   int             `json:"page_size"`
	FetchedAt  string          `json:"fetched_at"`
	Response   json.RawMessage `json:"response"`
}

type grimoireResponse struct {
	SearchResults []struct {
		FileSearchResult struct {
			FileSpec struct {
				Path string `json:"path"`
			} `json:"fileSpec"`
		} `json:"fileSearchResult"`
	} `json:"searchResults"`
	EstimatedResultCount string `json:"estimatedResultCount"`
	Error                *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// googleResults returns Google's ranked file list for q, using the disk cache
// and fetching from the network only on a miss. The bool reports whether a
// network fetch happened.
func googleResults(q evalQuery, fetches *int) ([]string, bool, error) {
	gq := q.Query
	if q.GoogleQuery != "" {
		gq = q.GoogleQuery
	}
	cachePath := filepath.Join(*cacheDir, q.ID+".json")

	if data, err := os.ReadFile(cachePath); err == nil {
		var entry cacheEntry
		if err := json.Unmarshal(data, &entry); err != nil {
			return nil, false, fmt.Errorf("corrupt cache entry %s: %s", cachePath, err)
		}
		if entry.Query == gq && entry.OssProject == *ossProject && entry.RepoName == *repoName {
			files, err := parseGrimoire(entry.Response)
			return files, false, err
		}
		// Query text changed; fall through and refetch.
	}

	if !*allowFetch {
		return nil, false, fmt.Errorf("no cached Google response for %q; rerun with -allow_fetch to fill the cache", q.ID)
	}
	if rateLimited {
		return nil, false, fmt.Errorf("skipped: rate limited earlier in this run; rerun later to fill the cache")
	}
	if *fetches >= *maxFetches {
		return nil, false, fmt.Errorf("fetch budget (-max_fetches=%d) exhausted", *maxFetches)
	}
	*fetches++
	raw, err := grimoireSearch(gq)
	if err != nil {
		// On a rate-limit response, stop fetching for the rest of the run
		// rather than sending ~100 more doomed requests.
		if strings.Contains(err.Error(), "HTTP 429") {
			rateLimited = true
		}
		return nil, true, err
	}
	entry := cacheEntry{
		Query:      gq,
		OssProject: *ossProject,
		RepoName:   *repoName,
		PageSize:   *numResults,
		FetchedAt:  time.Now().UTC().Format(time.RFC3339),
		Response:   raw,
	}
	data, err := json.MarshalIndent(entry, "", " ")
	if err != nil {
		return nil, true, err
	}
	if err := os.WriteFile(cachePath, data, 0644); err != nil {
		return nil, true, err
	}
	files, err := parseGrimoire(raw)
	return files, true, err
}

func parseGrimoire(raw json.RawMessage) ([]string, error) {
	var resp grimoireResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, err
	}
	if resp.Error != nil {
		return nil, fmt.Errorf("grimoire error %d: %s", resp.Error.Code, resp.Error.Message)
	}
	var files []string
	seen := make(map[string]bool)
	for _, sr := range resp.SearchResults {
		path := sr.FileSearchResult.FileSpec.Path
		if path == "" || seen[path] {
			continue
		}
		seen[path] = true
		files = append(files, path)
	}
	return files, nil
}

// grimoireSearch issues one search RPC against the endpoint backing
// cs.opensource.google. The RPC is a single-part multipart/mixed "batch"
// envelope around POST /v1/contents/search (the google-api-javascript-client
// batch transport the web UI uses).
func grimoireSearch(q string) (json.RawMessage, error) {
	payload := map[string]any{
		"queryString": q,
		"searchOptions": map[string]any{
			"enableDiagnostics":    false,
			"exhaustive":           false,
			"numberOfContextLines": 1,
			"pageSize":             *numResults,
			"pageToken":            "",
			"pathPrefix":           "",
			"repositoryScope":      map[string]any{"root": map[string]any{"ossProject": *ossProject, "repositoryName": *repoName}},
			// Counterintuitively, false returns zero results; the web UI
			// sends true. Cross-branch duplicates are deduped by path.
			"retrieveMultibranchResults": true,
			"savedQuery":                 "",
			"scoringModel":               "",
			"showPersonalizedResults":    false,
			"suppressGitLegacyResults":   false,
		},
		"snippetOptions": map[string]any{
			"numberOfContextLines":   1,
			"minSnippetLinesPerFile": 10,
			"minSnippetLinesPerPage": 60,
		},
	}
	innerBody, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	boundary := "batch" + randHex(12)
	var body bytes.Buffer
	for _, line := range []string{
		"--" + boundary,
		"Content-Type: application/http",
		"Content-ID: <response-" + boundary + "+gapiRequest@googleapis.com>",
		"",
		"POST /v1/contents/search?alt=json&key=" + *apiKey,
		"sessionid: " + randHex(8),
		"actionid: " + randHex(8),
		"X-JavaScript-User-Agent: google-api-javascript-client/1.1.0",
		"X-Requested-With: XMLHttpRequest",
		"Content-Type: application/json",
		"X-Goog-Encode-Response-If-Executable: base64",
		"",
		string(innerBody),
		"--" + boundary + "--",
		"",
	} {
		body.WriteString(line + "\r\n")
	}

	url := "https://grimoireoss-pa.clients6.google.com/batch?%24ct=multipart%2Fmixed%3B%20boundary%3D" + boundary
	req, err := http.NewRequest(http.MethodPost, url, &body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "text/plain; charset=UTF-8")
	req.Header.Set("Origin", "https://cs.opensource.google")
	req.Header.Set("Referer", "https://cs.opensource.google/")
	req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	text, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d: %.300s", resp.StatusCode, text)
	}
	// The batch response wraps the inner JSON in multipart framing; extract
	// the outermost JSON object.
	start := bytes.IndexByte(text, '{')
	end := bytes.LastIndexByte(text, '}')
	if start < 0 || end < start {
		return nil, fmt.Errorf("no JSON in response: %.300s", text)
	}
	return json.RawMessage(text[start : end+1]), nil
}

func randHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

// Metrics

func top(files []string, n int) []string {
	if len(files) > n {
		return files[:n]
	}
	return files
}

// rankOf returns the 1-based rank of the first file in files that appears in
// targets, or 0 if none does.
func rankOf(files, targets []string) int {
	if len(targets) == 0 {
		return 0
	}
	tset := make(map[string]bool, len(targets))
	for _, t := range targets {
		tset[t] = true
	}
	for i, f := range files {
		if tset[f] {
			return i + 1
		}
	}
	return 0
}

func jaccard(a, b []string) float64 {
	if len(a) == 0 && len(b) == 0 {
		return 0
	}
	set := make(map[string]int, len(a))
	for _, f := range a {
		set[f] = 1
	}
	inter := 0
	for _, f := range b {
		if set[f] == 1 {
			set[f] = 2
			inter++
		}
	}
	union := len(set)
	for _, f := range b {
		if _, ok := set[f]; !ok {
			union++
			set[f] = 0
		}
	}
	return float64(inter) / float64(union)
}

func writeResults(path string, results []*queryResult) error {
	var buf bytes.Buffer
	for _, r := range results {
		line, err := json.Marshal(r)
		if err != nil {
			return err
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}
	return os.WriteFile(path, buf.Bytes(), 0644)
}

type agg struct {
	n, nTargets                      int
	localR1, localR5, localR10       int
	googleR1, googleR5, googleR10    int
	zoektR1, zoektR5, zoektR10       int
	localMRR, googleMRR, zoektMRR    float64
	jaccardSum                       float64
	googleTop1InLocal5, nWithBoth    int
	localErrs, googleErrs, zoektErrs int
}

func (a *agg) add(r *queryResult) {
	a.n++
	if r.LocalErr != "" {
		a.localErrs++
	}
	if r.GoogleErr != "" {
		a.googleErrs++
	}
	if r.ZoektErr != "" {
		a.zoektErrs++
	}
	if len(r.Targets) > 0 {
		a.nTargets++
		addRank := func(rank int, r1, r5, r10 *int, mrr *float64) {
			if rank >= 1 {
				*mrr += 1.0 / float64(rank)
				if rank <= 1 {
					*r1++
				}
				if rank <= 5 {
					*r5++
				}
				if rank <= 10 {
					*r10++
				}
			}
		}
		addRank(r.LocalTargetRank, &a.localR1, &a.localR5, &a.localR10, &a.localMRR)
		addRank(r.GoogleTargetRank, &a.googleR1, &a.googleR5, &a.googleR10, &a.googleMRR)
		addRank(r.ZoektTargetRank, &a.zoektR1, &a.zoektR5, &a.zoektR10, &a.zoektMRR)
	}
	if len(r.Local) > 0 && len(r.Google) > 0 {
		a.nWithBoth++
		a.jaccardSum += r.Jaccard10
		if r.GoogleTop1LocalRank >= 1 && r.GoogleTop1LocalRank <= 5 {
			a.googleTop1InLocal5++
		}
	}
}

func pct(num, den int) string {
	if den == 0 {
		return "-"
	}
	return fmt.Sprintf("%.0f%%", 100*float64(num)/float64(den))
}

func ratio(num float64, den int) string {
	if den == 0 {
		return "-"
	}
	return fmt.Sprintf("%.2f", num/float64(den))
}

func summarize(results []*queryResult) string {
	byCat := make(map[string]*agg)
	var catOrder []string
	total := &agg{}
	for _, r := range results {
		a := byCat[r.Category]
		if a == nil {
			a = &agg{}
			byCat[r.Category] = a
			catOrder = append(catOrder, r.Category)
		}
		a.add(r)
		total.add(r)
	}

	var b strings.Builder
	b.WriteString("# Search quality: local codesearch vs cs.opensource.google\n\n")
	b.WriteString("Target metrics are over queries with labeled target files (n_t). ")
	b.WriteString("`g@1∈L5` = how often Google's top result appears in our top 5; `J@10` = mean Jaccard overlap of top-10 file sets.\n\n")
	withZoekt := *zoektPath != ""
	if withZoekt {
		b.WriteString("| category | n | n_t | local R@1 | local R@5 | local MRR | google R@1 | google R@5 | google MRR | zoekt R@1 | zoekt R@5 | zoekt MRR | J@10 | g@1∈L5 |\n")
		b.WriteString("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|\n")
	} else {
		b.WriteString("| category | n | n_t | local R@1 | local R@5 | local MRR | google R@1 | google R@5 | google MRR | J@10 | g@1∈L5 |\n")
		b.WriteString("|---|---|---|---|---|---|---|---|---|---|---|\n")
	}
	row := func(name string, a *agg) {
		zoektCols := ""
		if withZoekt {
			zoektCols = fmt.Sprintf(" %s | %s | %s |",
				pct(a.zoektR1, a.nTargets), pct(a.zoektR5, a.nTargets), ratio(a.zoektMRR, a.nTargets))
		}
		fmt.Fprintf(&b, "| %s | %d | %d | %s | %s | %s | %s | %s | %s |%s %s | %s |\n",
			name, a.n, a.nTargets,
			pct(a.localR1, a.nTargets), pct(a.localR5, a.nTargets), ratio(a.localMRR, a.nTargets),
			pct(a.googleR1, a.nTargets), pct(a.googleR5, a.nTargets), ratio(a.googleMRR, a.nTargets),
			zoektCols,
			ratio(a.jaccardSum, a.nWithBoth), pct(a.googleTop1InLocal5, a.nWithBoth))
	}
	for _, cat := range catOrder {
		row(cat, byCat[cat])
	}
	row("**total**", total)

	if total.localErrs > 0 || total.googleErrs > 0 || total.zoektErrs > 0 {
		fmt.Fprintf(&b, "\n%d local errors, %d google errors, %d zoekt errors (see results.jsonl).\n", total.localErrs, total.googleErrs, total.zoektErrs)
	}

	// Worst target misses, most useful queries to look at first.
	b.WriteString("\n## Queries where we miss the target but Google finds it\n\n")
	misses := 0
	for _, r := range results {
		if len(r.Targets) > 0 && r.GoogleTargetRank >= 1 && (r.LocalTargetRank == 0 || r.LocalTargetRank > 2*r.GoogleTargetRank+3) {
			fmt.Fprintf(&b, "- `%s` (%s): local rank %s, google rank %d, target %s\n",
				r.Query, r.ID, rankStr(r.LocalTargetRank), r.GoogleTargetRank, r.Targets[0])
			misses++
		}
	}
	if misses == 0 {
		b.WriteString("(none)\n")
	}
	return b.String()
}

func rankStr(r int) string {
	if r == 0 {
		return "miss"
	}
	return fmt.Sprintf("%d", r)
}
