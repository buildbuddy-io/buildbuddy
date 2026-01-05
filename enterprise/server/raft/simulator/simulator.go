package main

import (
	"container/heap"
	"fmt"
	"math"
	"math/rand"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/driver"
)

// Event represents a scheduled action in the simulation
type Event interface {
	Time() int
	Execute(w *World, strategy Strategy)
}

// EventQueue is a priority queue for events
type EventQueue []*eventItem

type eventItem struct {
	event Event
	time  int
	index int
}

func (pq EventQueue) Len() int { return len(pq) }

func (pq EventQueue) Less(i, j int) bool {
	return pq[i].time < pq[j].time
}

func (pq EventQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *EventQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*eventItem)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *EventQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*pq = old[0 : n-1]
	return item
}

// GossipEvent - node updates its view
type GossipEvent struct {
	nhid string
	time int
}

func (e *GossipEvent) Time() int { return e.time }

func (e *GossipEvent) Execute(w *World, strategy Strategy) {
	// Update view to current reality
	for otherNhid, count := range w.numLeases {
		w.views[e.nhid][otherNhid] = count
	}
	// Schedule next gossip
	nextTime := w.currentTick + w.randomGossipInterval()
	w.scheduleEvent(&GossipEvent{nhid: e.nhid, time: nextTime})
}

// RebalanceEvent - node evaluates and potentially initiates a transfer
type RebalanceEvent struct {
	nhid string
	time int
}

func (e *RebalanceEvent) Time() int { return e.time }

func (e *RebalanceEvent) Execute(w *World, strategy Strategy) {
	targetNhid := strategy.Decide(e.nhid, w.views[e.nhid], w.currentTick)
	transferInitiated := false
	if targetNhid != "" && targetNhid != e.nhid && w.numLeases[e.nhid] > 0 {
		completionTime := w.currentTick + w.randomTransferTime()
		w.scheduleEvent(&TransferCompleteEvent{
			fromNhid: e.nhid,
			toNhid:   targetNhid,
			time:     completionTime,
		})
		w.numTransfers++
		w.numPendingTransfers++
		transferInitiated = true
	}

	// Record snapshot if no transfer was initiated (system is stable)
	// This helps detect convergence when the system stops moving
	if !transferInitiated && w.numPendingTransfers == 0 {
		w.recordSnapshot()
	}

	// Schedule next rebalance evaluation
	nextTime := w.currentTick + w.rebalanceInterval
	w.scheduleEvent(&RebalanceEvent{nhid: e.nhid, time: nextTime})
}

// TransferCompleteEvent - lease moves from source to destination
type TransferCompleteEvent struct {
	fromNhid string
	toNhid   string
	time     int
}

func (e *TransferCompleteEvent) Time() int { return e.time }

func (e *TransferCompleteEvent) Execute(w *World, strategy Strategy) {
	w.numLeases[e.fromNhid]--
	w.numLeases[e.toNhid]++
	w.numPendingTransfers--
	w.numCompletedTransfers++

	// Record snapshot after transfer completes
	w.recordSnapshot()
}

type Snapshot struct {
	tick                  int
	numLeases             map[string]int // numLeases[nhid] = the number of leases node holds
	deviation             float64
	numPending            int
	numCompletedTransfers int // number of transfers completed at this snapshot
}

type World struct {
	numLeases map[string]int            // numLeases[nhid] = lease count
	views     map[string]map[string]int // views[nhid][otherNhid] = belief about other's lease count

	// All intervals in ticks (1 tick = 1ms)
	gossipIntervalMin int
	gossipIntervalMax int
	transferTimeMin   int
	transferTimeMax   int
	rebalanceInterval int

	// Event queue for efficient event-driven simulation
	eventQueue EventQueue

	// Current simulation time in ticks
	currentTick int

	// Metrics
	numTransfers          int
	numPendingTransfers   int
	numCompletedTransfers int
	maxDev                float64
	history               []Snapshot
}

// NewWorld creates a new World with the given initial lease counts.
func NewWorld(c Config) *World {
	w := &World{
		numLeases:         make(map[string]int),
		views:             make(map[string]map[string]int),
		eventQueue:        make(EventQueue, 0),
		currentTick:       0,
		history:           []Snapshot{},
		gossipIntervalMin: c.gossipIntervalMin,
		gossipIntervalMax: c.gossipIntervalMax,
		transferTimeMin:   c.transferTimeMin,
		transferTimeMax:   c.transferTimeMax,
		rebalanceInterval: c.rebalanceInterval,
	}

	heap.Init(&w.eventQueue)

	for nhid, count := range c.initialLeases {
		w.numLeases[nhid] = count
		w.views[nhid] = make(map[string]int)
		// Copy initial state to view
		for otherNhid, otherCount := range c.initialLeases {
			w.views[nhid][otherNhid] = otherCount
		}
		// Schedule initial gossip and rebalance events with random stagger
		w.scheduleEvent(&GossipEvent{
			nhid: nhid,
			time: rand.Intn(c.gossipIntervalMax),
		})
		w.scheduleEvent(&RebalanceEvent{
			nhid: nhid,
			time: rand.Intn(c.rebalanceInterval),
		})
	}

	return w
}

// scheduleEvent adds an event to the priority queue
func (w *World) scheduleEvent(event Event) {
	heap.Push(&w.eventQueue, &eventItem{
		event: event,
		time:  event.Time(),
	})
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

// ConvergenceTime calculates when the system converged (stopped making changes).
// A system is considered converged when lease counts haven't changed for consecutive snapshots.
// Returns (tick, numTransfers) where tick is the time and numTransfers is how many transfers had completed, or (-1, -1) if not converged.
func (w *World) ConvergenceTime(threshold float64) (int, int) {
	if len(w.history) < 2 {
		return -1, -1
	}

	// Look backwards to find when the system stopped changing
	convergedTick := -1
	convergedNumTransfers := -1
	for i := len(w.history) - 1; i >= 1; i-- {
		// Check if lease counts changed between consecutive snapshots
		changed := false
		for nhid, count := range w.history[i].numLeases {
			prevCount, exists := w.history[i-1].numLeases[nhid]
			if !exists || count != prevCount {
				changed = true
				break
			}
		}

		if changed {
			// Found the last change, so convergence is at i+1 (if it exists)
			if i+1 < len(w.history) {
				convergedTick = w.history[i+1].tick
				convergedNumTransfers = w.history[i+1].numCompletedTransfers
			}
			break
		}

		// Keep going backwards, update to the earliest stable point
		convergedTick = w.history[i].tick
		convergedNumTransfers = w.history[i].numCompletedTransfers
	}

	return convergedTick, convergedNumTransfers
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

// recordSnapshot records the current state as a snapshot
func (w *World) recordSnapshot() {
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
		tick:                  w.currentTick,
		numLeases:             snapshotLeases,
		deviation:             dev,
		numPending:            w.numPendingTransfers,
		numCompletedTransfers: w.numCompletedTransfers,
	})
}

// runUntil runs the simulation until the specified time
func (w *World) runUntil(endTime int, strategy Strategy) {
	for w.eventQueue.Len() > 0 {
		item := heap.Pop(&w.eventQueue).(*eventItem)

		// Stop if we've exceeded the duration
		if item.time > endTime {
			break
		}

		// Advance time to this event
		w.currentTick = item.time

		// Execute the event (events record their own snapshots as needed)
		item.event.Execute(w, strategy)
	}
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
	gossipIntervalMin int // in ticks (1 tick = 1ms)
	gossipIntervalMax int // in ticks
	transferTimeMin   int // in ticks
	transferTimeMax   int // in ticks
	rebalanceInterval int // in ticks (typically 1000 = 1 second)
	duration          int // in ticks
	strategy          Strategy
}

type Result struct {
	name             string
	numTransfers     int
	maxDeviation     float64
	convergenceTime  int
	convergenceIndex int
	numOscillations  int
	finalState       map[string]int
}

func (r Result) String() string {
	return fmt.Sprintf("%-20s transfers=%-4d maxDev=%-6.1f%% convergesAt=%-4d (op #%-3d) oscillations=%-3d final=%v\n", r.name, r.numTransfers, r.maxDeviation*100, r.convergenceTime, r.convergenceIndex, r.numOscillations, r.finalState)
}

func (c Config) Run() Result {
	w := NewWorld(c)
	w.runUntil(c.duration, c.strategy)
	convTime, convIndex := w.ConvergenceTime(0.1)
	return Result{
		name:             c.name,
		numTransfers:     w.numTransfers,
		maxDeviation:     w.maxDev,
		convergenceTime:  convTime,
		convergenceIndex: convIndex,
		numOscillations:  w.Oscillations(),
		finalState:       w.numLeases,
	}
}

func main() {
	// Test: Balanced cluster (4 nodes with 25 leases each) + new node (0 leases)
	// Expected: Production algorithm should rebalance to ~20 leases per node
	testCases := []struct {
		name          string
		initialLeases map[string]int
	}{
		{
			name: "ProductionStrategy - 5 machines",
			initialLeases: map[string]int{
				"nhid-1": 25, // Balanced
				"nhid-2": 25, // Balanced
				"nhid-3": 25, // Balanced
				"nhid-4": 25, // Balanced
				"nhid-5": 0,  // New node joining
			},
		},
		{
			name: "ProductionStrategy - 10 machines",
			initialLeases: map[string]int{
				"nhid-1": 11, // Balanced
				"nhid-2": 11, // Balanced
				"nhid-3": 11, // Balanced
				"nhid-4": 11, // Balanced
				"nhid-5": 11, // Balanced
				"nhid-6": 11, // Balanced
				"nhid-7": 11, // Balanced
				"nhid-8": 11, // Balanced
				"nhid-9": 12, // Balanced
				"nhid-0": 0,  // New node joining
			},
		},
	}

	for _, tc := range testCases {
		config := Config{
			name:              tc.name,
			initialLeases:     tc.initialLeases,
			gossipIntervalMin: 5,       // 5ms
			gossipIntervalMax: 10,      // 10ms
			transferTimeMin:   500,     // 500ms
			transferTimeMax:   5000,    // 2s
			rebalanceInterval: 1000,    // 1000ms = 1 second (matches production)
			duration:          500_000, // 5 minutes
			strategy:          NewProductionRebalanceStrategy(),
		}
		result := config.Run()
		fmt.Println(result)
	}

}
