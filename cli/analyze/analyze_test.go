package analyze

import (
	"reflect"
	"testing"

	bqpb "github.com/buildbuddy-io/buildbuddy/proto/bazel_query"
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

	// Construct equivalent graphs in different orders to ensure that neither
	// proto result order nor Go map iteration affects which tied path wins.
	ruleOrders := [][]string{
		{"//:a_root", "//:a_left", "//:a_right", "//:a_leaf", "//:z_root", "//:z_middle", "//:z_leaf"},
		{"//:z_leaf", "//:z_middle", "//:z_root", "//:a_leaf", "//:a_right", "//:a_left", "//:a_root"},
		{"//:a_right", "//:z_root", "//:a_leaf", "//:z_leaf", "//:a_root", "//:z_middle", "//:a_left"},
	}
	for _, order := range ruleOrders {
		graph := dependencyGraph(rulesByName, order)
		if got := graph.LongestPath(); !reflect.DeepEqual(got, want) {
			t.Fatalf("LongestPath() = %v, want %v", got, want)
		}
	}
}

func TestLongestPath_TieBreakComparesPathsFromStart(t *testing.T) {
	rulesByName := map[string]*bqpb.Rule{
		"//:a_root":   rule("//:a_root", "//:z_parent"),
		"//:z_parent": rule("//:z_parent", "//:leaf"),
		"//:b_root":   rule("//:b_root", "//:a_parent"),
		"//:a_parent": rule("//:a_parent", "//:leaf"),
		"//:leaf":     rule("//:leaf"),
	}
	graph := dependencyGraph(rulesByName, []string{
		"//:a_root", "//:z_parent", "//:b_root", "//:a_parent", "//:leaf",
	})
	want := []string{"//:a_root", "//:z_parent", "//:leaf"}

	if got := graph.LongestPath(); !reflect.DeepEqual(got, want) {
		t.Fatalf("LongestPath() = %v, want %v", got, want)
	}
}

func rule(name string, inputs ...string) *bqpb.Rule {
	return &bqpb.Rule{Name: &name, RuleInput: inputs}
}

func dependencyGraph(rulesByName map[string]*bqpb.Rule, ruleOrder []string) *DependencyGraph {
	g := &DependencyGraph{
		Rules:  make(map[string]*bqpb.Rule, len(rulesByName)),
		DepsOn: make(map[string][]*bqpb.Rule),
	}
	for _, name := range ruleOrder {
		r := rulesByName[name]
		g.Rules[name] = r
		for _, input := range r.RuleInput {
			g.DepsOn[input] = append(g.DepsOn[input], r)
		}
	}
	return g
}
