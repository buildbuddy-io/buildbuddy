package wrand_test

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/wrand"
	"github.com/stretchr/testify/require"
)

func TestWeightedShuffle(t *testing.T) {
	nodes := []string{"A", "B", "C", "D", "E"}
	weights := map[string]int64{
		// A should always be at the start.
		"A": 1_000_000_000_000_000_000,
		// E should always be at the end.
		"E": 0,
		// B, C, and D should appear shuffled in the middle, with B appearing
		// towards the front most often, then C, then D.
		// Note: B, C, and D's weights add up to 100 for easier math.
		"B": 90,
		"C": 9,
		"D": 1,
	}

	const n = 100_000
	nodeIndexFrequency := make(map[string][]int, len(nodes))
	for _, node := range nodes {
		nodeIndexFrequency[node] = make([]int, len(nodes))
	}
	for range n {
		// Randomly shuffle the nodes before doing a weighted shuffle to make
		// sure we aren't inadvertently depending on the original order somehow.
		rand.Shuffle(len(nodes), func(i, j int) {
			nodes[i], nodes[j] = nodes[j], nodes[i]
		})

		shuffled := wrand.Shuffle(nodes, func(node string) int64 {
			return weights[node]
		})
		for i, node := range shuffled {
			nodeIndexFrequency[node][i]++
		}
	}

	for _, test := range []struct {
		node        string
		index       int
		probability float64
	}{
		// Weight of A is *practically* infinite; should always be at the start.
		{"A", 0, 1.00},

		// B, C, and D should appear at index 1 with probabilities matching
		// their weights.
		{"B", 1, 0.90},
		{"C", 1, 0.09},
		{"D", 1, 0.01},

		// Probabilities for indexes 2 and 3 are more involved.
		// Let P(X@i) be the probability of node X appearing at index i.
		// Let W_X be the weight of node X.
		//
		// Let's start with the probability of B appearing at index 2.
		// If B appears at index 2, then either C or D must have appeared at
		// index 1. So we can use the law of total probability:
		// P(B@2) = P(B@2 | C@1)*P(C@1) + P(B@2 | D@1)*P(D@1)
		//
		// P(C@1) and P(D@1) are equal to their relative weights.
		// The conditional probabilities are:
		// P(B@2 | C@1) is W_B  / (W_B + W_D)
		// P(B@2 | D@1) is W_B  / (W_B + W_C)
		{"B", 2, 90./(90.+1.)*0.09 + 90./(90.+9.)*0.01},
		// Continuing the pattern for C and D:
		{"C", 2, 9./(9.+1.)*0.90 + 9./(9.+90.)*0.01},
		{"D", 2, 1./(1.+90.)*0.09 + 1./(1.+9.)*0.90},

		// The probabilities for index 3 can be computed similarly but we don't
		// bother since by symmetry these assertions should be guaranteed to
		// pass (since we already asserted that all frequencies add up to 100%).

		// Weight of E is 0; should always be at the end.
		{"E", 4, 1.00},
	} {
		tolerance := 0.005
		if test.probability == 0 || test.probability == 1 {
			tolerance = 0
		}
		relativeFrequency := float64(nodeIndexFrequency[test.node][test.index]) / n
		require.InDelta(
			t,
			relativeFrequency,
			test.probability,
			tolerance,
			"relative frequency of %s at index %d is %.5f%% but theoretical probability is %.5f%%",
			test.node,
			test.index,
			relativeFrequency,
			test.probability,
		)
	}
}

func TestShuffle_RandomInputs(t *testing.T) {
	weightChoices := []int64{
		// Assorted positive weights.
		1, 10, 100,
		// Edge cases: 0 and ~infinite weights.
		0, 1_000_000_000_000_000_000,
		// More edge cases: negative weights. Negative weights are treated
		// as 0.
		-1, -10, -100,
		// TODO: handle integer overflow. The test currently fails if these
		// are included:
		// math.MaxInt64, math.MinInt64,
	}

	for range 100 {
		sliceLength := rand.Intn(5)
		nodes := make([]string, sliceLength)
		for i := range nodes {
			nodes[i] = fmt.Sprintf("X%d", i)
		}
		weights := make(map[string]int64, sliceLength)
		for i := range nodes {
			weights[nodes[i]] = weightChoices[rand.Intn(len(weightChoices))]
		}
		nodeIndexFrequency := make(map[string][]int, len(nodes))
		for _, node := range nodes {
			nodeIndexFrequency[node] = make([]int, len(nodes))
		}
		referenceNodeIndexFrequency := make(map[string][]int, len(nodes))
		for _, node := range nodes {
			referenceNodeIndexFrequency[node] = make([]int, len(nodes))
		}
		const n = 100_000
		for range n {
			referenceShuffle(nodes, weights)
			for i, node := range nodes {
				referenceNodeIndexFrequency[node][i]++
			}

			shuffled := wrand.Shuffle(nodes, func(node string) int64 {
				return weights[node]
			})
			for i, node := range shuffled {
				nodeIndexFrequency[node][i]++
			}
		}
		// Make sure the reference shuffle and wrand shuffle produce similar
		// results (frequencies within a small tolerance of each other).
		for _, node := range nodes {
			for i := range nodes {
				got := float64(nodeIndexFrequency[node][i]) / n
				want := float64(referenceNodeIndexFrequency[node][i]) / n
				require.InDelta(
					t, got, want, 0.05,
					"nodes: %+#v, weights: %+#v, actualFrequencies: %+#v, referenceFrequencies: %+#v",
					nodes, weights, nodeIndexFrequency, referenceNodeIndexFrequency)
			}
		}
	}
}

// Weighted shuffle algorithm used to compare against the wrand implementation.
// This implementation has poor time complexity but it is much simpler to verify
// that it is correct.
func referenceShuffle(items []string, weights map[string]int64) {
	// Each iteration of the loop picks a node from the remaining items and
	// swaps it with the item at index i.
	for i := range items {
		// Compute the total weight of remaining items.
		var totalWeight int64
		for j := i; j < len(items); j++ {
			weight := weights[items[j]]
			if weight > 0 {
				totalWeight += weight
			}
		}

		// If there is no weight remaining, perform a standard shuffle on the
		// remainder.
		if totalWeight <= 0 {
			remaining := items[i:]
			rand.Shuffle(len(remaining), func(a, b int) {
				remaining[a], remaining[b] = remaining[b], remaining[a]
			})
			return
		}

		// Pick a random number and find which node it corresponds to.
		randWeight := rand.Int63n(totalWeight)
		selectedIndex := i
		var cumulativeWeight int64
		for j := i; j < len(items); j++ {
			weight := weights[items[j]]
			if weight > 0 {
				cumulativeWeight += weight
			}
			if randWeight < cumulativeWeight {
				selectedIndex = j
				break
			}
		}

		// Swap the chosen node into the current position.
		items[i], items[selectedIndex] = items[selectedIndex], items[i]
	}
}
