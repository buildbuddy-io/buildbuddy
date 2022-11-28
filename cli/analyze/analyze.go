package analyze

import (
	"bytes"
	"flag"
	"fmt"
	"os/exec"
	"sort"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/bazelisk"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/workspace"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	bqpb "github.com/buildbuddy-io/buildbuddy/proto/bazel_query"
)

const (
	// The default max duration to look back in git history when considering
	// file change history.
	defaultLookbackDuration = 4 * 7 * 24 * time.Hour // 4 weeks

	// The max number of targets to display for the cost analysis.
	targetLimit = 20
)

var (
	flags = flag.NewFlagSet("analyze", flag.ContinueOnError)

	longestPathFlag = flags.Bool("longest_path", false, "Show the longest path in the build graph.")

	costFlag     = flags.Bool("cost", false, "Analyze dependency costs for the target.")
	lookbackFlag = flags.Duration("lookback", defaultLookbackDuration, "How far back to look in git history to determine the number of edits.")

	usage = `
usage: bb ` + flags.Name() + ` [PATTERN]

Analyzes the dependency graph for the given PATTERN, attempting to identify
opportunities for restructuring your build in order to improve performance.

The simplest usage is to run all analyses for all targets in the current repo,
using the default //... as PATTERN argument:

    bb ` + flags.Name() + `

There are a few different analysis types available:

    bb ` + flags.Name() + ` PATTERN --longest_path

Shows the longest path in the dependency graph. Long dependency chains cannot
be built in parallel, and slow down the whole build.

    bb ` + flags.Name() + ` PATTERN --cost [--lookback=672h]

Prints a cost metric for all transitive dependencies of a target in the current
repository, based on the git change history of each dependency as well as the
number of transitive dependencies.

Targets with the highest cost are good candidates for splitting up into more
granular targets. For example, it might make sense to split up a large,
frequently changing utility library into smaller utilities, or split up a large
piece of business logic into separate API and implementation targets.

The cost of a target is computed roughly as the number of cache invalidations
caused by changes to any of the target's source files. This is approximated by
multiplying the number of "affected" targets by the number of changes to source
files in the last 4 weeks. The "affected" target set includes the target itself
as well as any targets that depend on it, either directly or indirectly.

The lookback duration defaults to 4 weeks but can be controlled using the
--lookback flag.
`
)

func HandleAnalyze(args []string) (int, error) {
	cmd, idx := arg.GetCommandAndIndex(args)
	if cmd != flags.Name() {
		return -1, nil
	}
	if err := arg.ParseFlagSet(flags, args[idx+1:]); err != nil {
		if err == flag.ErrHelp {
			log.Print(usage)
			return 1, nil
		}
		return -1, err
	}

	// TODO: Support more than one target
	if len(flags.Args()) > 1 {
		log.Print(usage)
		return 1, nil
	}

	// Run all analyses if none are explicitly requested.
	if !*costFlag && !*longestPathFlag {
		log.Printf("No analysis flags were set. Enabling `--cost` and `--longest_path` analyses.")
		*costFlag = true
		*longestPathFlag = true
	}

	pattern := "//..."
	if len(flags.Args()) > 0 {
		pattern = flags.Args()[0]
	}
	log.Printf("Querying dependency graph for %q ...", pattern)
	graph, err := queryGraph(pattern)
	if err != nil {
		log.Print(err)
		return 1, nil
	}

	if *longestPathFlag {
		// Compute the longest dependency path
		longestPath := graph.LongestPath()
		log.Printf("Longest dependency path (length %d):", len(longestPath)-1)
		printRow("LENGTH", "TARGET")
		for i, target := range longestPath {
			printRow(i, target)
		}
	}

	if *costFlag {
		log.Printf("Computing target metrics...")
		targetMetrics, err := computeTargetMetrics(graph)
		if err != nil {
			log.Printf("Failed to compute target metrics: %s", err)
			return 1, nil
		}

		topTargets := make([]string, 0, len(targetMetrics))
		for name := range targetMetrics {
			topTargets = append(topTargets, name)
		}
		// Sort in decreasing order of cost.
		sort.Slice(topTargets, func(i, j int) bool {
			ci, cj := targetMetrics[topTargets[i]].Cost(), targetMetrics[topTargets[j]].Cost()
			if ci != cj {
				return ci > cj
			}
			// Break ties using lexicographical name ordering.
			ni, nj := topTargets[i], topTargets[j]
			return ni < nj
		})
		// Print top targets.
		printRow("RANK", "CHANGED", "AFFECTS", "COST", "TARGET")
		for i := 0; i < len(topTargets) && i < targetLimit; i++ {
			t := topTargets[i]
			m := targetMetrics[t]
			printRow(i+1, m.SourceFileChangeCount, m.AffectedTargetCount, m.Cost(), t)
		}
	}
	return 0, nil
}

func printRow(values ...any) {
	// NOTE: row data is printed to stdout, while all other output (log.Printf
	// etc.) is printed to stderr. This allows redirecting stdout to a file
	// and then processing it as a TSV (tab-separated value) file.
	cols := make([]string, 0, len(values))
	for _, v := range values {
		cols = append(cols, fmt.Sprint(v))
	}
	fmt.Printf("%s\n", strings.Join(cols, "\t"))
}

// TargetMetrics holds metrics for a single target.
type TargetMetrics struct {
	// SourceFileChangeCount is the total number of changes to any source files
	// in this target within the lookback window.
	SourceFileChangeCount int

	// AffectedTargetCount is the number of targets affected by this target due
	// to transitive dependency edges, including the target itself.
	AffectedTargetCount int
}

// Cost returns how expensive the target is, taking into account the number of
// changes to the target as well as its number of affected targets.
func (m *TargetMetrics) Cost() int {
	return m.SourceFileChangeCount * m.AffectedTargetCount
}

func computeTargetMetrics(graph *DependencyGraph) (map[string]*TargetMetrics, error) {
	ws, err := workspace.Path()
	if err != nil {
		return nil, err
	}

	srcFilePaths, err := getAllSourceFilePaths(ws)
	if err != nil {
		return nil, fmt.Errorf("failed to compute source file paths: %s", err)
	}

	startTime := time.Now().Add(-*lookbackFlag)

	type targetResult struct {
		RuleName      string
		TargetMetrics *TargetMetrics

		Err error
	}
	compute := func(rule *bqpb.Rule) *targetResult {
		srcFileChangeCount := 0
		for _, input := range rule.RuleInput {
			srcPath := sourcePathFromLabel(input)
			if !srcFilePaths[srcPath] {
				continue
			}
			c, err := gitChangeCount(ws, srcPath, startTime)
			if err != nil {
				return &targetResult{Err: fmt.Errorf("failed to compute git change count for %q: %s", srcPath, err)}
			}
			srcFileChangeCount += c
		}
		AffectedTargetCount := graph.AffectedTargetCount(*rule.Name)
		return &targetResult{
			RuleName: *rule.Name,
			TargetMetrics: &TargetMetrics{
				SourceFileChangeCount: srcFileChangeCount,
				AffectedTargetCount:   AffectedTargetCount,
			},
		}
	}

	// Start some workers in parallel to compute target results. The parallelism
	// here lets us get more throughput for the `git log` calls for computing
	// source file change history.
	const numWorkers = 8
	resultsCh := make(chan *targetResult, numWorkers)
	eg := errgroup.Group{}
	rulesCh := bufferedChanOf(mapValues(graph.Rules))
	for i := 0; i < numWorkers; i++ {
		eg.Go(func() error {
			for rule := range rulesCh {
				res := compute(rule)
				if res.Err != nil {
					return res.Err
				}
				resultsCh <- res
			}
			return nil
		})
	}

	// Read results and populate them in a map.
	metrics := map[string]*TargetMetrics{}
	var metricsErr error
	metricsDone := make(chan struct{})
	go func() {
		defer close(metricsDone)
		for result := range resultsCh {
			if result.Err != nil {
				metricsErr = result.Err
				return
			}
			metrics[result.RuleName] = result.TargetMetrics
		}
	}()

	start := time.Now()
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	log.Debugf("Computed target metrics in %s", time.Since(start))
	close(resultsCh)
	<-metricsDone
	if metricsErr != nil {
		return nil, metricsErr
	}

	return metrics, nil
}

func sourcePathFromLabel(label string) string {
	relTarget := strings.TrimPrefix(label, "//")
	srcPath := strings.Replace(relTarget, ":", "/", 1)
	return srcPath
}

func gitChangeCount(repoRoot, path string, since time.Time) (int, error) {
	lines, err := getOutputLines(repoRoot, "git", "log", "--pretty=oneline", "--since="+since.Format(time.RFC3339Nano), "--", path)
	if err != nil {
		return 0, fmt.Errorf("git log failed: %s", err)
	}
	return len(lines), nil
}

// EdgeSet represents edges in a dependency graph.
type EdgeSet struct {
	// Outgoing represents all outgoing dependency edges.
	// Outgoing[n][m] is true iff n depends on m.
	Outgoing map[string]map[string]bool

	// Incoming represents all incoming dependency edges.
	// Incoming[m][n] is true iff n depends on m.
	Incoming map[string]map[string]bool
}

// Remove removes an edge from n to m.
func (s *EdgeSet) Remove(n, m string) bool {
	if _, ok := s.Outgoing[n]; !ok {
		return false
	}
	if _, ok := s.Incoming[m]; !ok {
		return false
	}
	if !s.Outgoing[n][m] || !s.Incoming[m][n] {
		return false
	}
	delete(s.Outgoing[n], m)
	delete(s.Incoming[m], n)
	return true
}

type DependencyGraph struct {
	Query  *bqpb.QueryResult
	Rules  map[string]*bqpb.Rule
	DepsOn map[string][]*bqpb.Rule

	affectedTargetCount map[string]int
}

func queryGraph(target string) (*DependencyGraph, error) {
	// Note, intersecting with //... limits us only to targets in the current
	// workspace. Tracking changes to external repos is complicated, so we're
	// excluding them for now.
	q := fmt.Sprintf("deps(%s) intersect //...", target)
	bazelArgs := []string{"query", "--output=proto", q}
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	opts := &bazelisk.RunOpts{
		Stdout: stdout,
		Stderr: stderr,
	}
	exitCode, err := bazelisk.Run(bazelArgs, opts)
	if err != nil {
		return nil, err
	}
	if exitCode != 0 {
		fmt.Print(stderr.String())
		return nil, fmt.Errorf("bazelisk query failed (exit code %d)", exitCode)
	}

	res := &bqpb.QueryResult{}
	if err := proto.Unmarshal(stdout.Bytes(), res); err != nil {
		return nil, err
	}

	graph := &DependencyGraph{
		Query:  res,
		Rules:  map[string]*bqpb.Rule{},
		DepsOn: map[string][]*bqpb.Rule{},

		affectedTargetCount: map[string]int{},
	}
	// Index rules by name.
	for _, t := range graph.Query.Target {
		r := t.Rule
		// Ignore non-Rule messages.
		if r == nil {
			continue
		}
		graph.Rules[r.GetName()] = r
	}
	// Index incoming dependency edges for more efficient graph traversal.
	// This is needed since the Rules returned by bazel query only list their
	// outgoing dependency edges.
	for _, r := range graph.Rules {
		for _, input := range r.RuleInput {
			// Don't index inputs from external repos.
			if strings.HasPrefix(input, "@") {
				continue
			}
			graph.DepsOn[input] = append(graph.DepsOn[input], r)
		}
	}
	return graph, nil
}

// AffectedTargetCount returns the number of targets that have transitive deps
// on a given target, counting the target itself.
func (g *DependencyGraph) AffectedTargetCount(name string) int {
	// Run a DFS on the graph of incoming dependencies, keeping track of nodes
	// already visited.

	// TODO: avoid a full graph traversal on each function call. Note: it's not
	// as simple as memoizing this func and then just summing the func's output
	// for all directly affected targets, since those targets can have an
	// overlapping set of transitively affected targets.
	frontier := []string{name}
	visited := map[string]bool{}
	for len(frontier) > 0 {
		name := frontier[len(frontier)-1]
		frontier = frontier[:len(frontier)-1]
		if visited[name] {
			continue
		}
		visited[name] = true
		for _, r := range g.DepsOn[name] {
			frontier = append(frontier, *r.Name)
		}
	}

	return len(visited)
}

// LongestPath returns the longest dependency path from one node in the graph to
// any other node.
func (g *DependencyGraph) LongestPath() []string {
	longestPathLength := 0
	var longestPathEnd *string

	length := map[string]int{}
	pred := map[string]*string{}
	e := g.EdgeSet()
	for _, m := range g.TopologicalSort() {
		for n := range e.Incoming[m] {
			l := length[n] + 1
			if l > length[m] {
				length[m] = l
				n := n
				pred[m] = &n
			}
			if l > longestPathLength {
				longestPathLength = l
				m := m
				longestPathEnd = &m
			}
		}
	}
	if longestPathEnd == nil {
		return nil
	}
	path := []string{*longestPathEnd}
	for {
		p := pred[path[len(path)-1]]
		if p == nil {
			break
		}
		path = append(path, *p)
	}
	reverseSlice(path)
	return path
}

// Roots returns all labels in the graph which have no incoming dependency
// edges.
func (g *DependencyGraph) Roots() []string {
	var roots []string
	for name := range g.Rules {
		if len(g.DepsOn[name]) == 0 {
			roots = append(roots, name)
		}
	}
	return roots
}

func (g *DependencyGraph) EdgeSet() *EdgeSet {
	outgoing := map[string]map[string]bool{}
	incoming := map[string]map[string]bool{}
	s := g.Roots()
	for len(s) > 0 {
		n := s[len(s)-1]
		s = s[:len(s)-1]
		if _, ok := outgoing[n]; ok {
			continue // already visited
		}
		rule := g.Rules[n]
		if rule == nil {
			continue
		}
		for _, m := range rule.RuleInput {
			if outgoing[n] == nil {
				outgoing[n] = map[string]bool{}
			}
			if incoming[m] == nil {
				incoming[m] = map[string]bool{}
			}
			outgoing[n][m] = true
			incoming[m][n] = true

			s = append(s, m)
		}
	}
	return &EdgeSet{Outgoing: outgoing, Incoming: incoming}
}

// TopologicalSort returns all node labels in the graph sorted so that each node
// is sorted before any of its transitive deps.
func (g *DependencyGraph) TopologicalSort() []string {
	// https://en.wikipedia.org/wiki/Topological_sorting#Kahn's_algorithm
	var out []string
	s := g.Roots()
	e := g.EdgeSet()
	for len(s) > 0 {
		n := s[len(s)-1]
		s = s[:len(s)-1]
		out = append(out, n)
		for m := anyKey(e.Outgoing[n]); m != nil; m = anyKey(e.Outgoing[n]) {
			e.Remove(n, *m)
			if len(e.Incoming[*m]) == 0 {
				s = append(s, *m)
			}
		}
	}
	return out
}

func getAllSourceFilePaths(dir string) (map[string]bool, error) {
	lines, err := getOutputLines(dir, "git", "ls-files", "--exclude-standard")
	if err != nil {
		return nil, fmt.Errorf("git ls-files failed: %s", err)
	}
	return makeSet(lines), nil
}

func getOutputLines(dir, executable string, args ...string) ([]string, error) {
	cmd := exec.Command(executable, args...)
	cmd.Dir = dir
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("%s", strings.TrimSpace(stderr.String()))
	}
	output := strings.TrimRight(stdout.String(), "\n")
	if output == "" {
		return nil, nil
	}
	return strings.Split(output, "\n"), nil
}

// anyKey returns an arbitrary key from the given map, or nil if the map is
// empty.
func anyKey[K comparable, V any](m map[K]V) *K {
	for k := range m {
		key := k
		return &key
	}
	return nil
}

// makeSet converts a slice to a set represented as a boolean-valued map.
func makeSet[T comparable](values []T) map[T]bool {
	set := map[T]bool{}
	for _, v := range values {
		set[v] = true
	}
	return set
}

// reverseSlice reverses the order of the elements in the given slice.
func reverseSlice[T any](a []T) {
	for i := 0; i < len(a)/2; i++ {
		j := len(a) - i - 1
		a[i], a[j] = a[j], a[i]
	}
}

// bufferedChanOf returns a channel pre-populated with the list of values from
// the given slice. This is useful to avoid having to start and manage a
// goroutine to stream the values from a slice onto the channel, but comes at
// the cost of creating a copy of the slice.
func bufferedChanOf[T any](slice []T) <-chan T {
	ch := make(chan T, len(slice))
	for _, val := range slice {
		ch <- val
	}
	close(ch)
	return ch
}

// mapValues returns a slice containing all the values from the given map. Note
// that values aren't guaranteed to be unique and are returned in no particular
// order.
func mapValues[K comparable, V any](m map[K]V) []V {
	var out []V
	for _, v := range m {
		out = append(out, v)
	}
	return out
}
