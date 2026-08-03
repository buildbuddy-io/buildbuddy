package execution_graph_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/execution_graph"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	egpb "github.com/buildbuddy-io/buildbuddy/proto/execution_graph"
	egapb "github.com/buildbuddy-io/buildbuddy/proto/execution_graph_analysis"
)

// The test fixture is a real execution graph log from a six-genrule build
// executed remotely, plus one internal workspace status action:
//
//	index  target                       mnemonic                    duration  deps
//	0      (internal_platform)          BazelWorkspaceStatusAction  569 ms    -
//	1      //execgraph_demo:base        Genrule                     4057 ms   -
//	2      //execgraph_demo:lone        Genrule                     6593 ms   -
//	3      //execgraph_demo:fast_b     Genrule                     3987 ms   1
//	4      //execgraph_demo:mid_c       Genrule                     4954 ms   3
//	5      //execgraph_demo:slow_a      Genrule                     10910 ms  1
//	6      //execgraph_demo:final       Genrule                     3998 ms   2,4,5
//
// The critical path is base -> slow_a -> final (18965 ms); the parallel
// branch base -> fast_b -> mid_c -> final is 1969 ms shorter.
const fixturePath = "testdata/execution_graph_dump.proto.zst"

func parseFixture(t *testing.T) []*egpb.Node {
	f, err := os.Open(fixturePath)
	require.NoError(t, err)
	defer f.Close()
	nodes, err := execution_graph.ParseCompressedLog(f, 0)
	require.NoError(t, err)
	return nodes
}

func factorByName(t *testing.T, analysis *egapb.ExecutionGraphAnalysis, name string) *egapb.FactorDrag {
	for _, fd := range analysis.GetFactorDrags() {
		if fd.GetFactor() == name {
			return fd
		}
	}
	return nil
}

func TestParseCompressedLog(t *testing.T) {
	nodes := parseFixture(t)
	require.Len(t, nodes, 7)
	assert.Equal(t, "BazelWorkspaceStatusAction", nodes[0].GetMnemonic())
	assert.Equal(t, "//execgraph_demo:base", nodes[1].GetTargetLabel())
	assert.Equal(t, int32(4057), nodes[1].GetMetrics().GetDurationMillis())
	assert.Equal(t, []int32{2, 4, 5}, nodes[6].GetDependentIndex())
}

func TestParseCompressedLog_MaxNodes(t *testing.T) {
	f, err := os.Open(fixturePath)
	require.NoError(t, err)
	defer f.Close()
	_, err = execution_graph.ParseCompressedLog(f, 3)
	require.Error(t, err)
}

func TestAnalyze_CriticalPath(t *testing.T) {
	nodes := parseFixture(t)
	analysis, err := execution_graph.Analyze(nodes, &execution_graph.Options{
		InvocationID:             "test-invocation",
		InvocationDurationMillis: 19056,
	})
	require.NoError(t, err)

	assert.Equal(t, int32(7), analysis.GetNumNodes())
	assert.Equal(t, int64(6), analysis.GetNumEdges())
	assert.Equal(t, int64(18965), analysis.GetCriticalPath().GetDurationMillis())
	assert.Equal(t, []int32{1, 5, 6}, analysis.GetCriticalPath().GetNodeIndex())

	require.Len(t, analysis.GetNodeDrags(), 3)
	assert.Equal(t, int64(4057), analysis.GetNodeDrags()[0].GetDragMillis()) // base
	assert.Equal(t, int64(1969), analysis.GetNodeDrags()[1].GetDragMillis()) // slow_a
	assert.Equal(t, int64(3998), analysis.GetNodeDrags()[2].GetDragMillis()) // final

	require.Len(t, analysis.GetEdgeDrags(), 2)
	assert.Equal(t, int32(1), analysis.GetEdgeDrags()[0].GetDepIndex())
	assert.Equal(t, int32(5), analysis.GetEdgeDrags()[0].GetNodeIndex())
	assert.Equal(t, int64(1969), analysis.GetEdgeDrags()[0].GetDragMillis()) // base -> slow_a
	assert.Equal(t, int64(1969), analysis.GetEdgeDrags()[1].GetDragMillis()) // slow_a -> final

	// Every node and edge drag records the critical path that would result
	// from zeroing the node / removing the edge.
	for i, nd := range analysis.GetNodeDrags() {
		require.NotNil(t, nd.GetNewCriticalPath(), "node drag %d new critical path", i)
		assert.Equal(t, int64(18965)-nd.GetDragMillis(), nd.GetNewCriticalPath().GetDurationMillis())
	}
	for i, ed := range analysis.GetEdgeDrags() {
		require.NotNil(t, ed.GetNewCriticalPath(), "edge drag %d new critical path", i)
		assert.Equal(t, int64(18965)-ed.GetDragMillis(), ed.GetNewCriticalPath().GetDurationMillis())
	}
	// Zeroing slow_a hands the lead to the fast_b -> mid_c branch.
	assert.Equal(t, []int32{1, 3, 4, 6}, analysis.GetNodeDrags()[1].GetNewCriticalPath().GetNodeIndex())
	// Removing base -> slow_a lets slow_a start immediately, so the longest
	// chain also runs through the fast_b branch.
	assert.Equal(t, []int32{1, 3, 4, 6}, analysis.GetEdgeDrags()[0].GetNewCriticalPath().GetNodeIndex())

	// Target dependency drags: how much shorter would the path be if nothing
	// waited on the target? Only critical-path targets with non-zero drag
	// are reported, sorted by descending drag. (final has no dependents.)
	require.Len(t, analysis.GetTargetDepDrags(), 2)
	assert.Equal(t, "//execgraph_demo:base", analysis.GetTargetDepDrags()[0].GetTargetLabel())
	assert.Equal(t, int64(4057), analysis.GetTargetDepDrags()[0].GetDragMillis())
	// With no deps on base, the longest chain is slow_a -> final.
	assert.Equal(t, []int32{5, 6}, analysis.GetTargetDepDrags()[0].GetNewCriticalPath().GetNodeIndex())
	assert.Equal(t, "//execgraph_demo:slow_a", analysis.GetTargetDepDrags()[1].GetTargetLabel())
	assert.Equal(t, int64(1969), analysis.GetTargetDepDrags()[1].GetDragMillis())
	assert.Equal(t, []int32{1, 3, 4, 6}, analysis.GetTargetDepDrags()[1].GetNewCriticalPath().GetNodeIndex())
}

func TestAnalyze_FactorDrags(t *testing.T) {
	nodes := parseFixture(t)
	analysis, err := execution_graph.Analyze(nodes, &execution_graph.Options{InvocationID: "test-invocation"})
	require.NoError(t, err)

	for _, tc := range []struct {
		factor string
		ftype  egapb.FactorType
		drag   int64
		total  int64
		cpTime int64
	}{
		// Process time attributed by mnemonic / rule class / target. Zeroing
		// all Genrule process time leaves the parallel branches' overhead as
		// the longest path, so drag is less than the time on the critical
		// path.
		{"Genrule", egapb.FactorType_MNEMONIC, 14085, 27182, 15089},
		{"genrule rule", egapb.FactorType_RULE_CLASS, 14085, 27182, 15089},
		{"//execgraph_demo:base", egapb.FactorType_TARGET, 2029, 2029, 2029},
		{"//execgraph_demo:final", egapb.FactorType_TARGET, 3029, 3029, 3029},
		// slow_a's process drag is capped by the parallel branch's float.
		{"//execgraph_demo:slow_a", egapb.FactorType_TARGET, 1969, 10031, 10031},
		// Standalone components, including process itself.
		{"Process", egapb.FactorType_COMPONENT, 14085, 27751, 15089},
		{"Queue", egapb.FactorType_COMPONENT, 186, 379, 186},
		{"Setup", egapb.FactorType_COMPONENT, 808, 1335, 808},
		{"Parse", egapb.FactorType_COMPONENT, 368, 736, 368},
		{"Fetch", egapb.FactorType_COMPONENT, 7, 7, 7},
		{"Network", egapb.FactorType_COMPONENT, 1088, 2058, 1088},
		{"Upload", egapb.FactorType_COMPONENT, 579, 1147, 579},
		{"Outputs", egapb.FactorType_COMPONENT, 282, 577, 282},
		{"Other", egapb.FactorType_COMPONENT, 558, 1078, 558},
		// Overhead groups: Bazel = parse + outputs + discover inputs;
		// Remote = fetch + setup + upload + retry.
		{"Bazel overhead", egapb.FactorType_OVERHEAD, 650, 1313, 650},
		{"Remote overhead", egapb.FactorType_OVERHEAD, 1394, 2489, 1394},
		// Runner factors cover the full duration of the runner's steps; all
		// six genrules ran remotely (the workspace status action has no
		// runner).
		{"remote", egapb.FactorType_RUNNER, 18396, 34499, 18965},
	} {
		fd := factorByName(t, analysis, tc.factor)
		require.NotNil(t, fd, "factor %q missing", tc.factor)
		assert.Equal(t, tc.ftype, fd.GetType(), "factor %q type", tc.factor)
		assert.Equal(t, tc.drag, fd.GetDragMillis(), "factor %q drag", tc.factor)
		assert.Equal(t, tc.total, fd.GetTotalMillis(), "factor %q total", tc.factor)
		assert.Equal(t, tc.cpTime, fd.GetCriticalPathMillis(), "factor %q critical path time", tc.factor)
		require.NotNil(t, fd.GetNewCriticalPath(), "factor %q new critical path", tc.factor)
		assert.Equal(t, int64(18965)-tc.drag, fd.GetNewCriticalPath().GetDurationMillis(), "factor %q new critical path length", tc.factor)
	}

	// Zero-drag per-value factors are omitted.
	assert.Nil(t, factorByName(t, analysis, "BazelWorkspaceStatusAction"))
	assert.Nil(t, factorByName(t, analysis, "empty target kind"))

	// Factors are sorted by descending drag.
	drags := analysis.GetFactorDrags()
	for i := 1; i < len(drags); i++ {
		assert.GreaterOrEqual(t, drags[i-1].GetDragMillis(), drags[i].GetDragMillis())
	}

	// Zeroing Genrule process time flips the critical path to the
	// overhead-heavy branch through fast_b and mid_c.
	genrule := factorByName(t, analysis, "Genrule")
	assert.Equal(t, []int32{1, 3, 4, 6}, genrule.GetNewCriticalPath().GetNodeIndex())
}

func TestAnalyze_SyntheticPhaseNodes(t *testing.T) {
	nodes := parseFixture(t)
	// The first genrule (base) starts at epoch 1785361825846 ms. Pick a build
	// start 5846 ms earlier so actions_execution_start lines up exactly, and
	// an analysis phase of 3000 ms.
	buildStart := int64(1785361820000)
	analysis, err := execution_graph.Analyze(nodes, &execution_graph.Options{
		InvocationID:                "test-invocation",
		BuildStartTimestampMillis:   buildStart,
		ActionsExecutionStartMillis: 5846,
		AnalysisPhaseMillis:         3000,
		WallTimeMillis:              26000,
	})
	require.NoError(t, err)

	// 7 real nodes + startup + analysis + finalization.
	require.Equal(t, int32(10), analysis.GetNumNodes())

	// Synthetic node indexes continue after the highest log index (6).
	var startup, analysisPhase, finalization *egapb.Node
	for _, n := range analysis.GetNodes() {
		switch n.GetDescription() {
		case "Bazel startup":
			startup = n
		case "Analysis phase":
			analysisPhase = n
		case "Finalization":
			finalization = n
		}
	}
	require.NotNil(t, startup)
	require.NotNil(t, analysisPhase)
	require.NotNil(t, finalization)
	assert.True(t, startup.GetSynthetic())
	assert.Equal(t, int64(2846), startup.GetDurationMillis())
	assert.Equal(t, int64(3000), analysisPhase.GetDurationMillis())
	assert.Equal(t, []int32{startup.GetIndex()}, analysisPhase.GetDepIndex())
	// Finalization spans from the last action end (final ends at
	// 1785361844816) to build start + wall time (1785361846000).
	assert.Equal(t, int64(1184), finalization.GetDurationMillis())

	// The critical path now runs through the synthetic phases:
	// startup -> analysis -> base -> slow_a -> final -> finalization.
	assert.Equal(t, int64(18965+2846+3000+1184), analysis.GetCriticalPath().GetDurationMillis())
	assert.Equal(t, []int32{
		startup.GetIndex(), analysisPhase.GetIndex(), 1, 5, 6, finalization.GetIndex(),
	}, analysis.GetCriticalPath().GetNodeIndex())

	// Each synthetic phase is a factor; every path passes through them, so
	// their drag equals their full duration.
	for _, tc := range []struct {
		factor string
		drag   int64
	}{
		{"Bazel startup", 2846},
		{"Analysis phase", 3000},
		{"Finalization", 1184},
	} {
		fd := factorByName(t, analysis, tc.factor)
		require.NotNil(t, fd, "factor %q missing", tc.factor)
		assert.Equal(t, egapb.FactorType_PHASE, fd.GetType())
		assert.Equal(t, tc.drag, fd.GetDragMillis(), "factor %q drag", tc.factor)
	}
}

func TestAnalyze_EmptyLog(t *testing.T) {
	_, err := execution_graph.Analyze(nil, &execution_graph.Options{})
	require.Error(t, err)
}

// Real logs from parallel builds are NOT written in dependency order, and
// with change-pruned actions a dependency's index can even be numerically
// greater than its dependent's. The analyzer must sort topologically itself.
func TestAnalyze_OutOfOrderLog(t *testing.T) {
	node := func(index int32, durMillis int32, deps ...int32) *egpb.Node {
		return &egpb.Node{
			Index:          index,
			TargetLabel:    fmt.Sprintf("//t:%d", index),
			Mnemonic:       "Genrule",
			DependentIndex: deps,
			Metrics: &egpb.Metrics{
				StartTimestampMillis: 1000,
				DurationMillis:       durMillis,
				ProcessMillis:        durMillis,
			},
		}
	}
	nodes := []*egpb.Node{
		node(4, 1000, 2),
		node(2, 2000, 3), // Depends on a numerically higher index.
		node(0, 4000),
		node(3, 500, 0),
		node(1, 100, 0),
	}
	analysis, err := execution_graph.Analyze(nodes, &execution_graph.Options{InvocationID: "test-invocation"})
	require.NoError(t, err)

	// 0 (4000) -> 3 (500) -> 2 (2000) -> 4 (1000).
	assert.Equal(t, int64(7500), analysis.GetCriticalPath().GetDurationMillis())
	assert.Equal(t, []int32{0, 3, 2, 4}, analysis.GetCriticalPath().GetNodeIndex())

	// The output node list is topologically ordered: deps come earlier.
	seen := map[int32]bool{}
	for _, n := range analysis.GetNodes() {
		for _, d := range n.GetDepIndex() {
			assert.True(t, seen[d], "dep %d of node %d must come earlier in the list", d, n.GetIndex())
		}
		seen[n.GetIndex()] = true
	}
}

func TestAnalyze_CycleError(t *testing.T) {
	nodes := []*egpb.Node{
		{Index: 0, DependentIndex: []int32{1}, Metrics: &egpb.Metrics{DurationMillis: 1}},
		{Index: 1, DependentIndex: []int32{0}, Metrics: &egpb.Metrics{DurationMillis: 1}},
	}
	_, err := execution_graph.Analyze(nodes, &execution_graph.Options{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cycle")
}

func TestAnalyze_RetriesAndFlakyTests(t *testing.T) {
	testNode := func(index int32, durMillis int32, subtype string, identifier string, retryOf *int32) *egpb.Node {
		return &egpb.Node{
			Index:         index,
			TargetLabel:   "//pkg:flaky_test",
			Mnemonic:      "TestRunner",
			Runner:        "remote",
			RunnerSubtype: subtype,
			Identifier:    identifier,
			RetryOf:       retryOf,
			Metrics: &egpb.Metrics{
				StartTimestampMillis: 1000 + int64(index)*10_000,
				DurationMillis:       durMillis,
				ProcessMillis:        durMillis,
			},
		}
	}
	retryOf0 := int32(0)
	retryOf1 := int32(1)
	nodes := []*egpb.Node{
		// Two attempts sharing an action digest (the first failed and was
		// re-executed), plus a follow-up spawn with its own digest (like
		// test.xml generation) that is NOT a wasted attempt.
		testNode(0, 5000, "pool-a", "run-digest", nil),
		testNode(1, 3000, "pool-b", "run-digest", &retryOf0),
		testNode(2, 100, "pool-b", "xml-digest", &retryOf1),
	}
	analysis, err := execution_graph.Analyze(nodes, &execution_graph.Options{InvocationID: "test-invocation"})
	require.NoError(t, err)

	// Retries are treated as dependency edges, so the critical path chains
	// all spawns of the action.
	assert.Equal(t, []int32{0, 1, 2}, analysis.GetCriticalPath().GetNodeIndex())
	assert.Equal(t, int64(8100), analysis.GetCriticalPath().GetDurationMillis())
	retryNode := analysis.GetNodes()[1]
	assert.Equal(t, []int32{0}, retryNode.GetDepIndex())
	require.NotNil(t, retryNode.RetryOfIndex)
	assert.Equal(t, int32(0), retryNode.GetRetryOfIndex())
	assert.Equal(t, "remote", retryNode.GetRunner())
	assert.Equal(t, "pool-b", retryNode.GetRunnerSubtype())

	// Flaky test factors cover the failed attempt (5000ms) only: the final
	// attempt and the unique-digest follow-up spawn are productive.
	for _, tc := range []struct {
		factor string
		ftype  egapb.FactorType
		drag   int64
		total  int64
	}{
		{"Flaky Test", egapb.FactorType_FLAKY_TEST, 5000, 5000},
		{"Flaky Test///pkg:flaky_test", egapb.FactorType_FLAKY_TEST, 5000, 5000},
		{"remote", egapb.FactorType_RUNNER, 8100, 8100},
		{"remote/pool-a", egapb.FactorType_RUNNER_SUBTYPE, 5000, 5000},
		{"remote/pool-b", egapb.FactorType_RUNNER_SUBTYPE, 3100, 3100},
	} {
		fd := factorByName(t, analysis, tc.factor)
		require.NotNil(t, fd, "factor %q missing", tc.factor)
		assert.Equal(t, tc.ftype, fd.GetType(), "factor %q type", tc.factor)
		assert.Equal(t, tc.drag, fd.GetDragMillis(), "factor %q drag", tc.factor)
		assert.Equal(t, tc.total, fd.GetTotalMillis(), "factor %q total", tc.factor)
	}
}

// A real execution graph log from `bazel test //flaky_demo:all
// --config=remote-dev --flaky_test_attempts=3`: flaky_test always fails (3
// attempts), passing_test passes. Each test attempt produces a run spawn plus
// a test.xml generation spawn, all chained via retry_of — only re-executed
// digests count as flaky waste.
func TestAnalyze_FlakyTestFixture(t *testing.T) {
	f, err := os.Open("testdata/flaky_test_dump.proto.zst")
	require.NoError(t, err)
	defer f.Close()
	nodes, err := execution_graph.ParseCompressedLog(f, 0)
	require.NoError(t, err)
	analysis, err := execution_graph.Analyze(nodes, &execution_graph.Options{InvocationID: "test-invocation"})
	require.NoError(t, err)

	assert.Equal(t, int64(18721), analysis.GetCriticalPath().GetDurationMillis())

	// Waste = the two failed runs (5746 + 5573) plus their re-generated
	// test.xml spawns (1602 + 188); the third run and its follow-ups are
	// productive. The flaky chain is this build's critical path, so drag
	// equals the full wasted time.
	flaky := factorByName(t, analysis, "Flaky Test")
	require.NotNil(t, flaky)
	assert.Equal(t, egapb.FactorType_FLAKY_TEST, flaky.GetType())
	assert.Equal(t, int64(13109), flaky.GetTotalMillis())
	assert.Equal(t, int64(13109), flaky.GetDragMillis())

	perTarget := factorByName(t, analysis, "Flaky Test///flaky_demo:flaky_test")
	require.NotNil(t, perTarget)
	assert.Equal(t, int64(13109), perTarget.GetTotalMillis())

	// The passing test also has retry_of-chained spawns (test.xml
	// generation), but no re-executed digests — it is not flaky.
	assert.Nil(t, factorByName(t, analysis, "Flaky Test///flaky_demo:passing_test"))

	// Both runners show up as factors.
	assert.NotNil(t, factorByName(t, analysis, "remote"))
	assert.NotNil(t, factorByName(t, analysis, "remote cache hit"))
}
