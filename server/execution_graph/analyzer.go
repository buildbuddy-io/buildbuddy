package execution_graph

import (
	"container/heap"
	"slices"
	"sort"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	egpb "github.com/buildbuddy-io/buildbuddy/proto/execution_graph"
	egapb "github.com/buildbuddy-io/buildbuddy/proto/execution_graph_analysis"
)

const (
	// AnalyzerVersion identifies the analysis algorithm. Bump when the
	// analysis output changes incompatibly; it is part of the storage key,
	// so bumping it causes invocations to be re-analyzed.
	//
	// v2: added FactorDrag.critical_path_millis.
	// v3: added NodeDrag/EdgeDrag.new_critical_path.
	// v4: added the Process factor, runner / flaky-test factors, retry
	// edges, and target dependency drags.
	AnalyzerVersion = 4

	// Per-node and per-edge new critical paths are only stored when the
	// critical path has at most this many nodes: storing them is quadratic in
	// the path length.
	maxNewPathStorageLen = 512

	// DefaultMaxFactors is the default cap on stored factor drags.
	DefaultMaxFactors = 100

	// A root node is only made to depend on the synthetic analysis-phase node
	// if it started at least this close to (or after) the end of the analysis
	// phase. Nodes that started earlier (e.g. the workspace status action)
	// genuinely did not wait for analysis.
	rootDepToleranceMillis = 250

	// Factor names for overhead groups and Bazel phases.
	bazelOverheadFactor  = "Bazel overhead"
	remoteOverheadFactor = "Remote overhead"
)

// Options configures Analyze.
type Options struct {
	InvocationID string

	// InvocationDurationMillis is the total wall duration of the invocation,
	// used as the baseline for relative drag.
	InvocationDurationMillis int64

	// BuildStartTimestampMillis is the epoch timestamp of the build's Started
	// event. Zero disables synthetic phase nodes.
	BuildStartTimestampMillis int64

	// ActionsExecutionStartMillis is BuildMetrics.TimingMetrics
	// .actions_execution_start_in_ms: the offset from build start until the
	// first action execution. Zero disables the startup/analysis nodes.
	ActionsExecutionStartMillis int64

	// AnalysisPhaseMillis is BuildMetrics.TimingMetrics
	// .analysis_phase_time_in_ms.
	AnalysisPhaseMillis int64

	// WallTimeMillis is BuildMetrics.TimingMetrics.wall_time_in_ms. Zero
	// disables the finalization node.
	WallTimeMillis int64

	// MaxFactors caps the number of stored factor drags (component, overhead
	// and phase factors are always kept). Defaults to DefaultMaxFactors.
	MaxFactors int
}

type component int

const (
	compQueue component = iota
	compSetup
	compParse
	compFetch
	compNetwork
	compProcess
	compUpload
	compProcessOutputs
	compOther
	compRetry
	compDiscoverInputs
	numComponents
)

// Keys used in Node.component_millis, matching execution_graph.Metrics field
// names.
var componentKeys = [numComponents]string{
	"queue", "setup", "parse", "fetch", "network", "process", "upload",
	"process_outputs", "other", "retry", "discover_inputs",
}

// Factor display names for standalone component factors. Process time is
// additionally attributed via mnemonic / rule class / target factors.
var componentFactorNames = [numComponents]string{
	"Queue", "Setup", "Parse", "Fetch", "Network", "Process", "Upload",
	"Outputs", "Other", "Retry", "Discover inputs",
}

const (
	// Parent factor covering the full duration of test attempts that failed
	// and were retried; "Flaky Test/<target>" children cover single targets.
	flakyTestFactor = "Flaky Test"

	// Mnemonic identifying test executions.
	testRunnerMnemonic = "TestRunner"
)

// Overhead group attribution, per subcomponent. Queue, network and other are
// standalone and belong to neither group.
var overheadGroup = map[component]string{
	compParse:          bazelOverheadFactor,
	compProcessOutputs: bazelOverheadFactor,
	compDiscoverInputs: bazelOverheadFactor,
	compFetch:          remoteOverheadFactor,
	compSetup:          remoteOverheadFactor,
	compUpload:         remoteOverheadFactor,
	compRetry:          remoteOverheadFactor,
}

func componentsOf(m *egpb.Metrics) [numComponents]int64 {
	return [numComponents]int64{
		int64(m.GetQueueMillis()),
		int64(m.GetSetupMillis()),
		int64(m.GetParseMillis()),
		int64(m.GetFetchMillis()),
		int64(m.GetNetworkMillis()),
		int64(m.GetProcessMillis()),
		int64(m.GetUploadMillis()),
		int64(m.GetProcessOutputsMillis()),
		int64(m.GetOtherMillis()),
		int64(m.GetRetryMillis()),
		int64(m.GetDiscoverInputsMillis()),
	}
}

// graphNode is the analyzer's internal node representation. Nodes are stored
// in a slice in topological order; deps refer to earlier slice positions.
type graphNode struct {
	outIndex      int32
	targetLabel   string
	mnemonic      string
	ruleClass     string
	description   string
	identifier    string
	runner        string
	runnerSubtype string
	startMillis   int64
	durMillis     int64
	deps          []int
	// Position of the earlier attempt this node is a retry of, or -1.
	retryOfPos int
	components [numComponents]int64
	synthetic  bool
}

type graph struct {
	nodes []*graphNode
}

// Analyze computes the critical path and node / edge / factor drags for the
// given execution graph log nodes.
func Analyze(logNodes []*egpb.Node, opts *Options) (*egapb.ExecutionGraphAnalysis, error) {
	if len(logNodes) == 0 {
		return nil, status.InvalidArgumentError("execution graph log contains no nodes")
	}
	maxFactors := opts.MaxFactors
	if maxFactors <= 0 {
		maxFactors = DefaultMaxFactors
	}

	g, err := buildGraph(logNodes, opts)
	if err != nil {
		return nil, err
	}

	cpLen, cpPath := g.longestPath(nil, nil)
	onCP := make(map[int]bool, len(cpPath))
	for _, pos := range cpPath {
		onCP[pos] = true
	}

	analysis := &egapb.ExecutionGraphAnalysis{
		Version:                  AnalyzerVersion,
		InvocationId:             opts.InvocationID,
		InvocationDurationMillis: opts.InvocationDurationMillis,
		NumNodes:                 int32(len(g.nodes)),
		CriticalPath:             g.pathProto(cpLen, cpPath),
	}
	for _, n := range g.nodes {
		analysis.NumEdges += int64(len(n.deps))
	}

	storeNewPaths := len(cpPath) <= maxNewPathStorageLen

	// Node drags, in critical path order.
	for _, pos := range cpPath {
		nodeLen, nodePath := g.longestPath(func(p int) int64 {
			if p == pos {
				return g.nodes[p].durMillis
			}
			return 0
		}, nil)
		nd := &egapb.NodeDrag{
			NodeIndex:  g.nodes[pos].outIndex,
			DragMillis: cpLen - nodeLen,
		}
		if storeNewPaths {
			nd.NewCriticalPath = g.pathProto(nodeLen, nodePath)
		}
		analysis.NodeDrags = append(analysis.NodeDrags, nd)
	}

	// Edge drags, in critical path order.
	for i := 1; i < len(cpPath); i++ {
		dep, node := cpPath[i-1], cpPath[i]
		edgeLen, edgePath := g.longestPath(nil, func(d, n int) bool { return d == dep && n == node })
		ed := &egapb.EdgeDrag{
			DepIndex:   g.nodes[dep].outIndex,
			NodeIndex:  g.nodes[node].outIndex,
			DragMillis: cpLen - edgeLen,
		}
		if storeNewPaths {
			ed.NewCriticalPath = g.pathProto(edgeLen, edgePath)
		}
		analysis.EdgeDrags = append(analysis.EdgeDrags, ed)
	}

	analysis.FactorDrags = g.factorDrags(cpLen, onCP, maxFactors)
	analysis.TargetDepDrags = g.targetDepDrags(cpLen, cpPath, storeNewPaths)

	// Node summaries, in topological order.
	for _, n := range g.nodes {
		np := &egapb.Node{
			Index:                n.outIndex,
			TargetLabel:          n.targetLabel,
			Mnemonic:             n.mnemonic,
			RuleClass:            n.ruleClass,
			Description:          n.description,
			Identifier:           n.identifier,
			Runner:               n.runner,
			RunnerSubtype:        n.runnerSubtype,
			StartTimestampMillis: n.startMillis,
			DurationMillis:       n.durMillis,
			Synthetic:            n.synthetic,
		}
		if n.retryOfPos >= 0 {
			np.RetryOfIndex = proto.Int32(g.nodes[n.retryOfPos].outIndex)
		}
		for _, dep := range n.deps {
			np.DepIndex = append(np.DepIndex, g.nodes[dep].outIndex)
		}
		for c, v := range n.components {
			if v > 0 {
				if np.ComponentMillis == nil {
					np.ComponentMillis = make(map[string]int64)
				}
				np.ComponentMillis[componentKeys[c]] = v
			}
		}
		analysis.Nodes = append(analysis.Nodes, np)
	}
	return analysis, nil
}

// buildGraph converts log nodes into the internal topologically-ordered
// representation and injects synthetic phase nodes (startup, analysis,
// finalization) derived from BES timing data.
func buildGraph(logNodes []*egpb.Node, opts *Options) (*graph, error) {
	g := &graph{}

	// Synthetic startup + analysis chain, prepended so that positions stay
	// topologically ordered.
	analysisPos := -1
	analysisEndMillis := int64(0)
	if opts.BuildStartTimestampMillis > 0 && opts.ActionsExecutionStartMillis > 0 {
		startupDur := max(0, opts.ActionsExecutionStartMillis-opts.AnalysisPhaseMillis)
		analysisDur := opts.ActionsExecutionStartMillis - startupDur
		g.nodes = append(g.nodes, &graphNode{
			description: "Bazel startup",
			mnemonic:    "BazelPhase",
			startMillis: opts.BuildStartTimestampMillis,
			durMillis:   startupDur,
			retryOfPos:  -1,
			synthetic:   true,
		})
		g.nodes = append(g.nodes, &graphNode{
			description: "Analysis phase",
			mnemonic:    "BazelPhase",
			startMillis: opts.BuildStartTimestampMillis + startupDur,
			durMillis:   analysisDur,
			deps:        []int{0},
			retryOfPos:  -1,
			synthetic:   true,
		})
		analysisPos = 1
		analysisEndMillis = opts.BuildStartTimestampMillis + opts.ActionsExecutionStartMillis
	}

	// Bazel writes nodes to the log asynchronously, so on parallel builds
	// the file is not in dependency order — and with change-pruned actions
	// included, a dependency's index can even be numerically greater than
	// its dependent's. Order the nodes topologically ourselves.
	logNodes, err := topoSort(logNodes)
	if err != nil {
		return nil, err
	}

	posByLogIndex := make(map[int32]int, len(logNodes))
	maxLogIndex := int32(-1)
	for _, ln := range logNodes {
		n := &graphNode{
			outIndex:      ln.GetIndex(),
			targetLabel:   ln.GetTargetLabel(),
			mnemonic:      ln.GetMnemonic(),
			ruleClass:     ln.GetRuleClass(),
			description:   ln.GetDescription(),
			identifier:    lastPartition(ln.GetIdentifier()),
			runner:        ln.GetRunner(),
			runnerSubtype: ln.GetRunnerSubtype(),
			startMillis:   ln.GetMetrics().GetStartTimestampMillis(),
			durMillis:     int64(ln.GetMetrics().GetDurationMillis()),
			retryOfPos:    -1,
			components:    componentsOf(ln.GetMetrics()),
		}
		for _, dep := range ln.GetDependentIndex() {
			pos, ok := posByLogIndex[dep]
			if !ok {
				return nil, status.InvalidArgumentErrorf("node %d depends on unknown node %d", ln.GetIndex(), dep)
			}
			n.deps = append(n.deps, pos)
		}
		// A retry can only start once the earlier attempt has finished, so
		// treat retry_of as a dependency edge.
		if ln.RetryOf != nil {
			pos, ok := posByLogIndex[ln.GetRetryOf()]
			if !ok {
				return nil, status.InvalidArgumentErrorf("node %d is a retry of unknown node %d", ln.GetIndex(), ln.GetRetryOf())
			}
			n.retryOfPos = pos
			if !slices.Contains(n.deps, pos) {
				n.deps = append(n.deps, pos)
			}
		}
		// Execution can only start once analysis has produced the action, so
		// root nodes depend on the analysis phase — except nodes that
		// demonstrably started before analysis ended (e.g. the workspace
		// status action).
		if len(n.deps) == 0 && analysisPos >= 0 && n.startMillis >= analysisEndMillis-rootDepToleranceMillis {
			n.deps = append(n.deps, analysisPos)
		}
		if _, ok := posByLogIndex[ln.GetIndex()]; ok {
			return nil, status.InvalidArgumentErrorf("duplicate node index %d", ln.GetIndex())
		}
		posByLogIndex[ln.GetIndex()] = len(g.nodes)
		maxLogIndex = max(maxLogIndex, ln.GetIndex())
		g.nodes = append(g.nodes, n)
	}

	// Synthetic finalization node: Bazel work after the last action, e.g. BEP
	// finish and output download. Depends on every sink node.
	if opts.BuildStartTimestampMillis > 0 && opts.WallTimeMillis > 0 {
		lastEnd := int64(0)
		hasDependent := make([]bool, len(g.nodes))
		for _, n := range g.nodes {
			lastEnd = max(lastEnd, n.startMillis+n.durMillis)
			for _, dep := range n.deps {
				hasDependent[dep] = true
			}
		}
		finDur := opts.BuildStartTimestampMillis + opts.WallTimeMillis - lastEnd
		if finDur > 0 {
			fin := &graphNode{
				description: "Finalization",
				mnemonic:    "BazelPhase",
				startMillis: lastEnd,
				durMillis:   finDur,
				retryOfPos:  -1,
				synthetic:   true,
			}
			for pos, dependent := range hasDependent {
				if !dependent {
					fin.deps = append(fin.deps, pos)
				}
			}
			g.nodes = append(g.nodes, fin)
		}
	}

	// Assign output indexes to synthetic nodes, after all log indexes.
	next := maxLogIndex + 1
	for _, n := range g.nodes {
		if n.synthetic {
			n.outIndex = next
			next++
		}
	}
	return g, nil
}

// longestPath runs the longest-path DP over the graph and returns the length
// and the path (as slice positions, in dependency order).
//
// sub, if non-nil, is subtracted from each node's duration (clamped at zero).
// skipEdge, if non-nil, drops dependency edges for which it returns true.
func (g *graph) longestPath(sub func(int) int64, skipEdge func(dep, node int) bool) (int64, []int) {
	finish := make([]int64, len(g.nodes))
	pred := make([]int, len(g.nodes))
	end := -1
	for i, n := range g.nodes {
		dur := n.durMillis
		if sub != nil {
			dur = max(0, dur-sub(i))
		}
		best, bestPred := int64(0), -1
		for _, dep := range n.deps {
			if skipEdge != nil && skipEdge(dep, i) {
				continue
			}
			if finish[dep] > best {
				best, bestPred = finish[dep], dep
			}
		}
		finish[i] = best + dur
		pred[i] = bestPred
		if end < 0 || finish[i] > finish[end] {
			end = i
		}
	}
	var path []int
	for pos := end; pos >= 0; pos = pred[pos] {
		path = append(path, pos)
	}
	// Reverse into dependency order.
	for i, j := 0, len(path)-1; i < j; i, j = i+1, j-1 {
		path[i], path[j] = path[j], path[i]
	}
	return finish[end], path
}

func (g *graph) pathProto(length int64, path []int) *egapb.CriticalPath {
	cp := &egapb.CriticalPath{DurationMillis: length}
	for _, pos := range path {
		cp.NodeIndex = append(cp.NodeIndex, g.nodes[pos].outIndex)
	}
	return cp
}

// factorSpec describes one factor: a named slice of time across all nodes.
type factorSpec struct {
	name  string
	ftype egapb.FactorType
	// slice returns the factor's time within the node at the given position.
	slice func(int) int64
	total int64
	// cpTotal is the factor's time summed over critical-path nodes. A factor
	// with no time on the critical path cannot have drag.
	cpTotal int64
}

// factorDrags computes the drag (and resulting new critical path) of every
// factor.
func (g *graph) factorDrags(cpLen int64, onCP map[int]bool, maxFactors int) []*egapb.FactorDrag {
	specs := g.factorSpecs(onCP)

	var drags []*egapb.FactorDrag
	for _, spec := range specs {
		if spec.total == 0 {
			continue
		}
		fd := &egapb.FactorDrag{
			Factor:             spec.name,
			Type:               spec.ftype,
			TotalMillis:        spec.total,
			CriticalPathMillis: spec.cpTotal,
		}
		if spec.cpTotal > 0 {
			newLen, newPath := g.longestPath(spec.slice, nil)
			fd.DragMillis = cpLen - newLen
			fd.NewCriticalPath = g.pathProto(newLen, newPath)
		}
		// Per-value factors (mnemonic / rule class / target) are only
		// interesting when they carry drag; all other factor types are
		// always reported.
		if fd.DragMillis > 0 || alwaysKeepFactorType(spec.ftype) {
			drags = append(drags, fd)
		}
	}

	sort.Slice(drags, func(i, j int) bool {
		if drags[i].DragMillis != drags[j].DragMillis {
			return drags[i].DragMillis > drags[j].DragMillis
		}
		return drags[i].Factor < drags[j].Factor
	})

	// Cap the per-value factors; always-kept factor types are bounded.
	if len(drags) > maxFactors {
		kept := make([]*egapb.FactorDrag, 0, maxFactors)
		var dropped int
		for _, fd := range drags {
			if len(kept)+remainingAlwaysKeep(drags, dropped+len(kept)) >= maxFactors && !alwaysKeepFactorType(fd.Type) {
				dropped++
				continue
			}
			kept = append(kept, fd)
		}
		drags = kept
	}
	return drags
}

// alwaysKeepFactorType returns whether factors of this type are reported even
// with zero drag and are exempt from the factor cap. Per-value factor types
// (mnemonic / rule class / target) are not: there can be arbitrarily many.
func alwaysKeepFactorType(t egapb.FactorType) bool {
	switch t {
	case egapb.FactorType_COMPONENT, egapb.FactorType_OVERHEAD, egapb.FactorType_PHASE,
		egapb.FactorType_RUNNER, egapb.FactorType_RUNNER_SUBTYPE, egapb.FactorType_FLAKY_TEST:
		return true
	default:
		return false
	}
}

// remainingAlwaysKeep counts always-kept factors at or after index i.
func remainingAlwaysKeep(drags []*egapb.FactorDrag, i int) int {
	count := 0
	for ; i < len(drags); i++ {
		if alwaysKeepFactorType(drags[i].Type) {
			count++
		}
	}
	return count
}

func (g *graph) factorSpecs(onCP map[int]bool) []*factorSpec {
	var specs []*factorSpec

	// Standalone component factors.
	for c := component(0); c < numComponents; c++ {
		if componentFactorNames[c] == "" {
			continue
		}
		specs = append(specs, &factorSpec{
			name:  componentFactorNames[c],
			ftype: egapb.FactorType_COMPONENT,
			slice: func(pos int) int64 { return g.nodes[pos].components[c] },
		})
	}

	// Overhead groups.
	for _, group := range []string{bazelOverheadFactor, remoteOverheadFactor} {
		specs = append(specs, &factorSpec{
			name:  group,
			ftype: egapb.FactorType_OVERHEAD,
			slice: func(pos int) int64 {
				var total int64
				for c, grp := range overheadGroup {
					if grp == group {
						total += g.nodes[pos].components[c]
					}
				}
				return total
			},
		})
	}

	// Process time attributed by mnemonic, rule class and target.
	dims := []struct {
		ftype egapb.FactorType
		get   func(*graphNode) string
	}{
		{egapb.FactorType_MNEMONIC, func(n *graphNode) string { return n.mnemonic }},
		{egapb.FactorType_RULE_CLASS, func(n *graphNode) string { return n.ruleClass }},
		{egapb.FactorType_TARGET, func(n *graphNode) string { return n.targetLabel }},
	}
	for _, dim := range dims {
		seen := make(map[string]bool)
		for _, n := range g.nodes {
			val := dim.get(n)
			if val == "" || n.synthetic || seen[val] {
				continue
			}
			seen[val] = true
			specs = append(specs, &factorSpec{
				name:  val,
				ftype: dim.ftype,
				slice: func(pos int) int64 {
					if dim.get(g.nodes[pos]) == val && !g.nodes[pos].synthetic {
						return g.nodes[pos].components[compProcess]
					}
					return 0
				},
			})
		}
	}

	// Synthetic phase nodes are factors of their own.
	for pos, n := range g.nodes {
		if !n.synthetic {
			continue
		}
		specs = append(specs, &factorSpec{
			name:  n.description,
			ftype: egapb.FactorType_PHASE,
			slice: func(p int) int64 {
				if p == pos {
					return g.nodes[p].durMillis
				}
				return 0
			},
		})
	}

	// Runner factors cover the full duration of a runner's steps (a runner
	// owns the whole spawn lifecycle, so runners partition the build's
	// time). "<runner>/<subtype>" children roll up to "<runner>"
	// structurally, but drags are computed independently.
	seenRunner := make(map[string]bool)
	for _, n := range g.nodes {
		if n.runner == "" || seenRunner[n.runner] {
			continue
		}
		seenRunner[n.runner] = true
		runner := n.runner
		specs = append(specs, &factorSpec{
			name:  runner,
			ftype: egapb.FactorType_RUNNER,
			slice: func(pos int) int64 {
				if g.nodes[pos].runner == runner {
					return g.nodes[pos].durMillis
				}
				return 0
			},
		})
		seenSubtype := make(map[string]bool)
		for _, m := range g.nodes {
			if m.runner != runner || m.runnerSubtype == "" || seenSubtype[m.runnerSubtype] {
				continue
			}
			seenSubtype[m.runnerSubtype] = true
			subtype := m.runnerSubtype
			specs = append(specs, &factorSpec{
				name:  runner + "/" + subtype,
				ftype: egapb.FactorType_RUNNER_SUBTYPE,
				slice: func(pos int) int64 {
					if g.nodes[pos].runner == runner && g.nodes[pos].runnerSubtype == subtype {
						return g.nodes[pos].durMillis
					}
					return 0
				},
			})
		}
	}

	// Flaky test factors cover spawns that were re-executed later in a retry
	// chain. Bazel links ALL spawns of a test action via retry_of — including
	// follow-up spawns like test.xml generation — so retry_of alone doesn't
	// identify wasted attempts. The identifier (the action digest) does:
	// within a chain, a spawn whose identifier appears again later was
	// re-executed, so it (and its follow-ups) belonged to a failed attempt.
	// Spawns without identifiers (no remote cache/execution) can't be
	// matched and are never marked.
	flaky := make([]bool, len(g.nodes))
	flakyTargets := make(map[string]bool)
	chainRoot := make(map[int]int)
	chainMembers := make(map[int][]int)
	for pos, n := range g.nodes {
		if n.retryOfPos < 0 {
			continue
		}
		root, ok := chainRoot[n.retryOfPos]
		if !ok {
			root = n.retryOfPos
			chainMembers[root] = []int{root}
		}
		chainRoot[pos] = root
		chainMembers[root] = append(chainMembers[root], pos)
	}
	for _, members := range chainMembers {
		// Positions ascend, so later spawns come later in the list.
		lastByIdentifier := make(map[string]int, len(members))
		for _, pos := range members {
			identifier := g.nodes[pos].identifier
			if identifier == "" {
				continue
			}
			if prev, ok := lastByIdentifier[identifier]; ok && g.nodes[prev].mnemonic == testRunnerMnemonic {
				flaky[prev] = true
				if target := g.nodes[prev].targetLabel; target != "" {
					flakyTargets[target] = true
				}
			}
			lastByIdentifier[identifier] = pos
		}
	}
	flakySlice := func(match func(*graphNode) bool) func(int) int64 {
		return func(pos int) int64 {
			if flaky[pos] && match(g.nodes[pos]) {
				return g.nodes[pos].durMillis
			}
			return 0
		}
	}
	if len(flakyTargets) > 0 {
		specs = append(specs, &factorSpec{
			name:  flakyTestFactor,
			ftype: egapb.FactorType_FLAKY_TEST,
			slice: flakySlice(func(*graphNode) bool { return true }),
		})
		for target := range flakyTargets {
			specs = append(specs, &factorSpec{
				name:  flakyTestFactor + "/" + target,
				ftype: egapb.FactorType_FLAKY_TEST,
				slice: flakySlice(func(n *graphNode) bool { return n.targetLabel == target }),
			})
		}
	}

	// Compute totals and critical-path time in one pass per spec.
	for _, spec := range specs {
		for pos := range g.nodes {
			v := spec.slice(pos)
			spec.total += v
			if onCP[pos] {
				spec.cpTotal += v
			}
		}
	}
	return specs
}

// nodeDeps returns a node's dependency indexes, including retry_of (a retry
// can only start once the earlier attempt has finished).
func nodeDeps(ln *egpb.Node) []int32 {
	deps := ln.GetDependentIndex()
	if ln.RetryOf != nil && !slices.Contains(deps, ln.GetRetryOf()) {
		deps = append(slices.Clone(deps), ln.GetRetryOf())
	}
	return deps
}

// topoSort orders log nodes so that every node's dependencies (including
// retry_of) come before it, breaking ties by index so the output is
// deterministic. Returns an InvalidArgument error on duplicate indexes,
// references to absent nodes, or dependency cycles.
func topoSort(logNodes []*egpb.Node) ([]*egpb.Node, error) {
	byIndex := make(map[int32]*egpb.Node, len(logNodes))
	for _, ln := range logNodes {
		if _, ok := byIndex[ln.GetIndex()]; ok {
			return nil, status.InvalidArgumentErrorf("duplicate node index %d", ln.GetIndex())
		}
		byIndex[ln.GetIndex()] = ln
	}
	indegree := make(map[int32]int, len(logNodes))
	dependents := make(map[int32][]int32, len(logNodes))
	for _, ln := range logNodes {
		for _, dep := range nodeDeps(ln) {
			if _, ok := byIndex[dep]; !ok {
				return nil, status.InvalidArgumentErrorf("node %d depends on unknown node %d", ln.GetIndex(), dep)
			}
			indegree[ln.GetIndex()]++
			dependents[dep] = append(dependents[dep], ln.GetIndex())
		}
	}
	ready := &int32Heap{}
	for _, ln := range logNodes {
		if indegree[ln.GetIndex()] == 0 {
			heap.Push(ready, ln.GetIndex())
		}
	}
	sorted := make([]*egpb.Node, 0, len(logNodes))
	for ready.Len() > 0 {
		index := heap.Pop(ready).(int32)
		sorted = append(sorted, byIndex[index])
		for _, dependent := range dependents[index] {
			indegree[dependent]--
			if indegree[dependent] == 0 {
				heap.Push(ready, dependent)
			}
		}
	}
	if len(sorted) != len(logNodes) {
		return nil, status.InvalidArgumentErrorf("execution graph contains a dependency cycle (%d nodes involved)", len(logNodes)-len(sorted))
	}
	return sorted, nil
}

type int32Heap []int32

func (h int32Heap) Len() int            { return len(h) }
func (h int32Heap) Less(i, j int) bool  { return h[i] < h[j] }
func (h int32Heap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *int32Heap) Push(x interface{}) { *h = append(*h, x.(int32)) }
func (h *int32Heap) Pop() interface{} {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}

// targetDepDrags computes, for each target with a step on the critical path,
// how much shorter the critical path would be if no step had to wait for any
// of the target's steps (i.e. all dependency edges out of the target are
// removed).
func (g *graph) targetDepDrags(cpLen int64, cpPath []int, storeNewPaths bool) []*egapb.TargetDepDrag {
	seen := make(map[string]bool)
	var drags []*egapb.TargetDepDrag
	for _, pos := range cpPath {
		target := g.nodes[pos].targetLabel
		if target == "" || g.nodes[pos].synthetic || seen[target] {
			continue
		}
		seen[target] = true
		newLen, newPath := g.longestPath(nil, func(dep, node int) bool {
			return g.nodes[dep].targetLabel == target && !g.nodes[dep].synthetic
		})
		if cpLen-newLen <= 0 {
			continue
		}
		td := &egapb.TargetDepDrag{
			TargetLabel: target,
			DragMillis:  cpLen - newLen,
		}
		if storeNewPaths {
			td.NewCriticalPath = g.pathProto(newLen, newPath)
		}
		drags = append(drags, td)
	}
	sort.Slice(drags, func(i, j int) bool {
		if drags[i].DragMillis != drags[j].DragMillis {
			return drags[i].DragMillis > drags[j].DragMillis
		}
		return drags[i].TargetLabel < drags[j].TargetLabel
	})
	return drags
}

// lastPartition returns the last '/'-separated component of s.
func lastPartition(s string) string {
	if i := strings.LastIndexByte(s, '/'); i >= 0 {
		return s[i+1:]
	}
	return s
}
