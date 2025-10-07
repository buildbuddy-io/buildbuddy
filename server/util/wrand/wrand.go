// Package wrand provides utilities for weighted random selection.
package wrand

import (
	"math/rand"
)

// Shuffle performs a time-efficient, weighted random shuffle. It accepts a
// slice of any type `T` and a function `weightFn` that returns the weight of an
// element. It returns a new slice with the elements shuffled according to
// their weights.
//
// As a simple example, consider the items "A" and "B" with weights 9 and 1,
// respectively. A weighted random shuffle of these items will produce a list
// containing these 2 items, "A" and "B", with "A" appearing as the first list
// entry with probability (9/(9+1)) = 9/10 = 90%.
//
// Weighted random selection works as follows: the weights are evaluated once
// for each item initially, with negative weights being treated as 0. Then,
// items are placed into the output slice one by one, with probability equal to
// the item's weight divided by the sum of the remaining unpicked items'
// weights. This process is repeated until all items have been placed into the
// output slice.
//
// This function does not modify the original slice and has a time complexity of
// O(n log n).
func Shuffle[T any](nodes []T, weightFn func(T) int64) []T {
	n := len(nodes)
	if n == 0 {
		// Short-circuit to avoid BinaryIndex tree allocation.
		return nil
	}

	// Create a binary indexed tree to efficiently query and update prefix sums
	// of the weights. Building the tree is O(n log n).
	tree := make(binaryIndexedTree, n)
	for i, node := range nodes {
		tree.Add(i, max(0, weightFn(node)))
	}

	shuffled := make([]T, n)
	// Track which candidates have been picked. This is needed for the case
	// where all remaining weights become zero.
	picked := make([]bool, n)

	// In each iteration of the loop, select one node from the remaining
	// candidates and place it in the shuffled slice.
	for i := range n {
		currentTotalWeight := tree.PrefixSum(n)

		// If all remaining nodes have zero weight, their selection is
		// unweighted. Copy these to the remaining slots and shuffle them.
		if currentTotalWeight <= 0 {
			unpicked := shuffled[i:i:n]
			for j := range n {
				if !picked[j] {
					unpicked = append(unpicked, nodes[j])
				}
			}
			rand.Shuffle(len(unpicked), func(a, b int) {
				unpicked[a], unpicked[b] = unpicked[b], unpicked[a]
			})
			return shuffled
		}

		randWeight := rand.Int63n(currentTotalWeight)
		selectedIndex := tree.Find(randWeight)
		if selectedIndex == -1 {
			panic("integer overflow occurred when summing weights")
		}

		picked[selectedIndex] = true
		selectedNode := nodes[selectedIndex]
		shuffled[i] = selectedNode

		// "Remove" the selected node's weight from the tree by adding its negative
		// weight. This ensures it won't be picked again in a weighted selection.
		weight := max(0, weightFn(selectedNode))
		tree.Add(selectedIndex, -weight)
	}

	return shuffled
}

// binaryIndexedTree is a data structure that supports efficient prefix sum
// operations while also allowing for efficient updates.
type binaryIndexedTree []int64

// Add adds a value to the element at a given index. The value can be negative.
// Complexity: O(log n).
func (t binaryIndexedTree) Add(i int, value int64) {
	for ; i < len(t); i += (i + 1) & -(i + 1) {
		t[i] += value
	}
}

// PrefixSum returns the sum of the first n values.
// Complexity: O(log n).
func (t binaryIndexedTree) PrefixSum(n int) int64 {
	var sum int64
	for i := n - 1; i >= 0; i -= (i + 1) & -(i + 1) {
		sum += t[i]
	}
	return sum
}

// Find returns the largest index i such that prefixSum(i) <= value and
// i < len(t).
// Complexity: O(log n).
//
// Example: for the array [7, 11]:
// Find(6)  returns index 0 since prefixSum(0) = 0  (<= 6)  but prefixSum(1) =  7 (> 6)
// Find(7)  returns index 1 since prefixSum(1) = 7  (<= 7)  but prefixSum(2) = 18 (> 7)
// Find(8)  returns index 1 since prefixSum(1) = 7  (<= 8)  but prefixSum(2) = 18 (> 8)
// Find(18) returns index 2 since prefixSum(2) = 18 (<= 18) but 3 >= len(t)
func (t binaryIndexedTree) Find(value int64) int {
	i := 0
	mask := 1
	for (mask << 1) <= len(t) {
		mask <<= 1
	}
	for ; mask > 0; mask >>= 1 {
		if i+mask > len(t) {
			continue
		}
		tIdx := i + mask - 1
		if value >= t[tIdx] {
			i = tIdx + 1
			value -= t[tIdx]
		}
	}
	if i >= len(t) {
		return -1
	}
	return i
}
