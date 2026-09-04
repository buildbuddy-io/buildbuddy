package analyze

import (
	"testing"

	bqpb "github.com/buildbuddy-io/buildbuddy/proto/bazel_query"
	"github.com/stretchr/testify/require"
)

func TestLongestPath_TiesChooseLexicographicallyFirstPath(t *testing.T) {
	rulesByName := map[string]*bqpb.Rule{
		"//:a_root":   rule("//:a_root", "//:a_left", "//:a_right"),
		"//:a_left":   rule("//:a_left", "//:a_leaf"),
		"//:a_right":  rule("//:a_right", "//:a_leaf"),
		"//:a_leaf":   rule("//:a_leaf"),
		"//:z_root":   rule("//:z_root", "//:z_middle"),
		"//:z_middle": rule("//:z_middle", "//:z_leaf"),
		"//:z_leaf":   rule("//:z_leaf"),
	}
	want := []string{"//:a_root", "//:a_left", "//:a_leaf"}

	graph := dependencyGraph(rulesByName)
	require.Equal(t, want, graph.LongestPath())
}

func TestLongestPath_TieBreakComparesPathsFromStart(t *testing.T) {
	rulesByName := map[string]*bqpb.Rule{
		"//:a_root":   rule("//:a_root", "//:z_parent"),
		"//:z_parent": rule("//:z_parent", "//:leaf"),
		"//:b_root":   rule("//:b_root", "//:a_parent"),
		"//:a_parent": rule("//:a_parent", "//:leaf"),
		"//:leaf":     rule("//:leaf"),
	}
	graph := dependencyGraph(rulesByName)
	want := []string{"//:a_root", "//:z_parent", "//:leaf"}

	require.Equal(t, want, graph.LongestPath())
}

func rule(name string, inputs ...string) *bqpb.Rule {
	return &bqpb.Rule{Name: &name, RuleInput: inputs}
}

func dependencyGraph(rulesByName map[string]*bqpb.Rule) *DependencyGraph {
	g := &DependencyGraph{
		Rules:  make(map[string]*bqpb.Rule, len(rulesByName)),
		DepsOn: make(map[string][]*bqpb.Rule),
	}
	for name, r := range rulesByName {
		g.Rules[name] = r
		for _, input := range r.RuleInput {
			g.DepsOn[input] = append(g.DepsOn[input], r)
		}
	}
	return g
}
