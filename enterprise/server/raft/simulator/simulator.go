package main

import (
	"fmt"
	"math"
	"math/rand"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/driver"
)

type Transfer struct {
	fromNhid    string
	toNhid      string
	completesAt int //tick
}

type Snapshot struct {
	tick       int
	numLeases  map[string]int // numLeases[nhid] = the number of leases node holds
	deviation  float64
	numPending int
}

type World struct {
	numLeases map[string]int            // numLeases[nhid] = lease count
	views     map[string]map[string]int // views[nhid][otherNhid] = belief about other's lease count

	gossipIntervalMin int
	gossipIntervalMax int
	transferTimeMin   int
	transferTimeMax   int

	// per-node next gossip time
	nextGossipTick map[string]int

	// In-flight transfers (not yet completed)
	pending []Transfer

	// Current simulation time
	currentTick int

	// Metrics
	numTransfers int
	maxDev       float64
	history      []Snapshot
}

// NewWorld creates a new World with the given initial lease counts.
func NewWorld(c Config) *World {
	w := &World{
		numLeases:         make(map[string]int),
		views:             make(map[string]map[string]int),
		nextGossipTick:    make(map[string]int),
		pending:           []Transfer{},
		currentTick:       0,
		history:           []Snapshot{},
		gossipIntervalMin: c.gossipIntervalMin,
		gossipIntervalMax: c.gossipIntervalMax,
		transferTimeMin:   c.transferTimeMin,
		transferTimeMax:   c.transferTimeMax,
	}

	for nhid, count := range c.initialLeases {
		w.numLeases[nhid] = count
		w.views[nhid] = make(map[string]int)
		// Copy initial state to view
		for otherNhid, otherCount := range c.initialLeases {
			w.views[nhid][otherNhid] = otherCount
		}
		// initial stagger
		w.nextGossipTick[nhid] = rand.Intn(c.gossipIntervalMax)
	}

	return w
}

func (w *World) randomGossipInterval() int {
	return w.gossipIntervalMin + rand.Intn(w.gossipIntervalMax-w.gossipIntervalMin+1)
}

func (w *World) randomTransferTime() int {
	return w.transferTimeMin + rand.Intn(w.transferTimeMax-w.transferTimeMin+1)
}

// Mean calculates the mean number of leases across all nodes.
func (w *World) mean() float64 {
	sum := 0
	for _, count := range w.numLeases {
		sum += count
	}
	return float64(sum) / float64(len(w.numLeases))
}

// Deviation calculates the standard deviation of lease counts.
func (w *World) Deviation() float64 {
	mean := w.mean()
	sumSquares := 0.0
	for _, count := range w.numLeases {
		diff := float64(count) - mean
		sumSquares += diff * diff
	}
	return math.Sqrt(sumSquares / float64(len(w.numLeases)))
}

// ConvergenceTime calculates when the system converged (deviation stayed below threshold).
func (w *World) ConvergenceTime(threshold float64) int {
	convergedAt := -1
	for i := len(w.history) - 1; i >= 0; i-- {
		if w.history[i].deviation > threshold {
			break
		}
		convergedAt = w.history[i].tick
	}
	return convergedAt
}

// Oscillations counts direction changes in lease counts across all nodes.
func (w *World) Oscillations() int {
	if len(w.history) == 0 {
		return 0
	}
	count := 0
	mean := w.mean()
	for nhid := range w.numLeases {
		if _, exists := w.history[0].numLeases[nhid]; !exists {
			continue
		}
		above := float64(w.history[0].numLeases[nhid]) > mean
		for _, snap := range w.history[1:] {
			if leaseCount, exists := snap.numLeases[nhid]; exists {
				nowAbove := float64(leaseCount) > mean
				if nowAbove != above {
					count++
					above = nowAbove
				}
			}
		}
	}
	return count
}

// Step advances the simulation by one tick using the given strategy.
func (w *World) Step(strategy Strategy) {
	w.currentTick++

	// Process gossip updates
	for nhid := range w.views {
		if w.currentTick >= w.nextGossipTick[nhid] {
			// Update view to current reality
			for otherNhid, count := range w.numLeases {
				w.views[nhid][otherNhid] = count
			}
			w.nextGossipTick[nhid] = w.currentTick + w.randomGossipInterval()
		}
	}

	// Process completed transfers
	remaining := []Transfer{}
	for _, t := range w.pending {
		if t.completesAt <= w.currentTick {
			w.numLeases[t.fromNhid]--
			w.numLeases[t.toNhid]++
		} else {
			remaining = append(remaining, t)
		}
	}
	w.pending = remaining

	// Each node decides based on its view
	for nhid := range w.numLeases {
		targetNhid := strategy.Decide(nhid, w.views[nhid], w.currentTick)
		if targetNhid != "" && targetNhid != nhid && w.numLeases[nhid] > 0 {
			w.pending = append(w.pending, Transfer{
				fromNhid:    nhid,
				toNhid:      targetNhid,
				completesAt: w.currentTick + w.randomTransferTime(),
			})
			w.numTransfers++
		}
	}

	// Record snapshot
	dev := w.Deviation()
	if dev > w.maxDev {
		w.maxDev = dev
	}
	// Copy map for snapshot
	snapshotLeases := make(map[string]int)
	for nhid, count := range w.numLeases {
		snapshotLeases[nhid] = count
	}
	w.history = append(w.history, Snapshot{
		tick:       w.currentTick,
		numLeases:  snapshotLeases,
		deviation:  dev,
		numPending: len(w.pending),
	})
}

type Strategy interface {
	Decide(myNhid string, view map[string]int, currentTick int) string
}

// ProductionRebalanceStrategy uses the actual production lease rebalancing algorithm.
type ProductionRebalanceStrategy struct{}

func NewProductionRebalanceStrategy() *ProductionRebalanceStrategy {
	return &ProductionRebalanceStrategy{}
}

func (s *ProductionRebalanceStrategy) Decide(myNhid string, view map[string]int, currentTick int) string {
	// Convert int to int64
	leaseCounts := make(map[string]int64, len(view))
	for nhid, count := range view {
		leaseCounts[nhid] = int64(count)
	}

	// Call production algorithm via wrapper
	return driver.FindRebalanceLeaseOpForSimulation(myNhid, leaseCounts)
}

type Config struct {
	name              string
	initialLeases     map[string]int
	gossipIntervalMin int
	gossipIntervalMax int
	transferTimeMin   int
	transferTimeMax   int
	duration          int
	strategy          Strategy
}

type Result struct {
	name            string
	numTransfers    int
	maxDeviation    float64
	convergenceTime int
	numOscillations int
	finalState      map[string]int
}

func (r Result) String() string {
	return fmt.Sprintf("%-20s transfers=%-4d maxDev=%-6.1f%% converge=%-4d oscillations=%-3d final=%v\n", r.name, r.numTransfers, r.maxDeviation*100, r.convergenceTime, r.numOscillations, r.finalState)
}

func (c Config) Run() Result {
	w := NewWorld(c)
	for t := 0; t < c.duration; t++ {
		w.Step(c.strategy)
	}
	return Result{
		name:            c.name,
		numTransfers:    w.numTransfers,
		maxDeviation:    w.maxDev,
		convergenceTime: w.ConvergenceTime(0.1),
		numOscillations: w.Oscillations(),
		finalState:      w.numLeases,
	}

}

func main() {
	// Test: Balanced cluster (4 nodes with 25 leases each) + new node (0 leases)
	// Expected: Production algorithm should rebalance to ~20 leases per node
	initialLeases := map[string]int{
		"nhid-1": 25, // Balanced
		"nhid-2": 25, // Balanced
		"nhid-3": 25, // Balanced
		"nhid-4": 25, // Balanced
		"nhid-5": 0,  // New node joining
	}

	config := Config{
		name:              "ProductionStrategy",
		initialLeases:     initialLeases,
		gossipIntervalMin: 5,
		gossipIntervalMax: 10,
		transferTimeMin:   2,
		transferTimeMax:   5,
		duration:          300,
		strategy:          NewProductionRebalanceStrategy(),
	}

	result := config.Run()
	fmt.Println(result)
}
