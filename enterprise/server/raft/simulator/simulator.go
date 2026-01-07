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
	myLeases := w.nodeLeases[e.nhid]
	transferInitiated := false

	// If this node has leases, pick one to evaluate
	if len(myLeases) > 0 {
		// Pick a random shard this node holds a lease for
		shardID := myLeases[rand.Intn(len(myLeases))]
		shard := w.shards[shardID]

		// Ask strategy where to move this shard's lease
		targetNhid := strategy.Decide(e.nhid, shardID, shard.replicas, w.views[e.nhid], w.currentTick)

		if targetNhid != "" && targetNhid != e.nhid {
			// Verify target is in the replica set
			validTarget := false
			for _, replica := range shard.replicas {
				if replica == targetNhid {
					validTarget = true
					break
				}
			}

			if validTarget {
				completionTime := w.currentTick + w.randomTransferTime()
				w.scheduleEvent(&TransferCompleteEvent{
					shardID:  shardID,
					fromNhid: e.nhid,
					toNhid:   targetNhid,
					time:     completionTime,
				})
				w.numTransfers++
				w.numPendingTransfers++
				transferInitiated = true
			}
		}
	}

	// Record snapshot if no transfer was initiated (system is stable)
	// This helps detect convergence when the system stops moving
	if !transferInitiated && w.numPendingTransfers == 0 {
		w.recordSnapshot()
	}

	// Schedule next rebalance evaluation (with jitter if configured)
	nextTime := w.currentTick + w.randomRebalanceInterval()
	w.scheduleEvent(&RebalanceEvent{nhid: e.nhid, time: nextTime})
}

// TransferCompleteEvent - lease moves from source to destination
type TransferCompleteEvent struct {
	shardID  int
	fromNhid string
	toNhid   string
	time     int
}

func (e *TransferCompleteEvent) Time() int { return e.time }

func (e *TransferCompleteEvent) Execute(w *World, strategy Strategy) {
	// Transfer the lease for this specific shard
	shard := w.shards[e.shardID]
	shard.leaseHolder = e.toNhid

	// Update node lease tracking
	// Remove from source
	sourceLeases := w.nodeLeases[e.fromNhid]
	for i, sid := range sourceLeases {
		if sid == e.shardID {
			w.nodeLeases[e.fromNhid] = append(sourceLeases[:i], sourceLeases[i+1:]...)
			break
		}
	}
	// Add to target
	w.nodeLeases[e.toNhid] = append(w.nodeLeases[e.toNhid], e.shardID)

	// Update lease counts
	w.numLeases[e.fromNhid]--
	w.numLeases[e.toNhid]++
	w.numPendingTransfers--
	w.numCompletedTransfers++

	// Record snapshot after transfer completes
	w.recordSnapshot()
}

type Shard struct {
	id          int      // Shard ID (starts from 2)
	replicas    []string // 3 node IDs that have replicas of this shard
	leaseHolder string   // Node currently holding the lease (must be in replicas)
}

type Snapshot struct {
	tick                  int
	numLeases             map[string]int // numLeases[nhid] = the number of leases node holds
	numPending            int
	numCompletedTransfers int // number of transfers completed at this snapshot
}

type World struct {
	shards       map[int]*Shard            // shardID -> Shard
	nodeReplicas map[string][]int          // nhid -> list of shard IDs this node has replicas for
	nodeLeases   map[string][]int          // nhid -> list of shard IDs this node holds leases for
	numLeases    map[string]int            // nhid -> lease count (derived from nodeLeases)
	views        map[string]map[string]int // views[nhid][otherNhid] = belief about other's lease count

	// All intervals in ticks (1 tick = 1ms)
	gossipIntervalMin       int
	gossipIntervalMax       int
	transferTimeMin         int
	transferTimeMax         int
	rebalanceInterval       int
	rebalanceIntervalJitter int // ±jitter applied to rebalanceInterval (e.g., 100 = ±100ms)

	// Event queue for efficient event-driven simulation
	eventQueue EventQueue

	// Current simulation time in ticks
	currentTick int

	// Metrics
	numTransfers          int
	numPendingTransfers   int
	numCompletedTransfers int
	history               []Snapshot
}

// pickRandomNodes picks n random distinct nodes from the list
func pickRandomNodes(nodes []string, n int) []string {
	if n > len(nodes) {
		n = len(nodes)
	}
	// Shuffle and take first n
	shuffled := make([]string, len(nodes))
	copy(shuffled, nodes)
	rand.Shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})
	return shuffled[:n]
}

// NewWorld creates a new World with the given configuration.
func NewWorld(c Config) *World {
	w := &World{
		shards:            make(map[int]*Shard),
		nodeReplicas:      make(map[string][]int),
		nodeLeases:        make(map[string][]int),
		numLeases:         make(map[string]int),
		views:             make(map[string]map[string]int),
		eventQueue:        make(EventQueue, 0),
		currentTick:       0,
		history:           []Snapshot{},
		gossipIntervalMin:       c.gossipIntervalMin,
		gossipIntervalMax:       c.gossipIntervalMax,
		transferTimeMin:         c.transferTimeMin,
		transferTimeMax:         c.transferTimeMax,
		rebalanceInterval:       c.rebalanceInterval,
		rebalanceIntervalJitter: c.rebalanceIntervalJitter,
	}

	heap.Init(&w.eventQueue)

	// Initialize node maps
	for _, nhid := range c.nodes {
		w.nodeReplicas[nhid] = []int{}
		w.nodeLeases[nhid] = []int{}
		w.numLeases[nhid] = 0
		w.views[nhid] = make(map[string]int)
		// Initialize view with zero counts
		for _, otherNhid := range c.nodes {
			w.views[nhid][otherNhid] = 0
		}
	}

	// Build target lease counts
	targetLeases := make(map[string]int)
	if c.initialLeases != nil {
		// Use provided initial distribution
		for nhid, count := range c.initialLeases {
			targetLeases[nhid] = count
		}
	} else {
		// Default: evenly distribute leases
		leasesPerNode := c.numShards / len(c.nodes)
		remainder := c.numShards % len(c.nodes)
		for i, nhid := range c.nodes {
			targetLeases[nhid] = leasesPerNode
			if i < remainder {
				targetLeases[nhid]++
			}
		}
	}

	// Create shards with random replica placement (shard IDs start from 2)
	for shardID := 2; shardID < c.numShards+2; shardID++ {
		// Pick 3 random nodes for replicas
		replicas := pickRandomNodes(c.nodes, 3)

		// Pick lease holder from replicas to match target distribution
		// Choose the replica that is furthest below its target
		leaseHolder := replicas[0]
		maxDeficit := targetLeases[replicas[0]] - w.numLeases[replicas[0]]
		for _, replica := range replicas[1:] {
			deficit := targetLeases[replica] - w.numLeases[replica]
			if deficit > maxDeficit {
				maxDeficit = deficit
				leaseHolder = replica
			}
		}

		shard := &Shard{
			id:          shardID,
			replicas:    replicas,
			leaseHolder: leaseHolder,
		}
		w.shards[shardID] = shard

		// Track replicas for each node
		for _, nhid := range replicas {
			w.nodeReplicas[nhid] = append(w.nodeReplicas[nhid], shardID)
		}

		// Track lease for the lease holder
		w.nodeLeases[leaseHolder] = append(w.nodeLeases[leaseHolder], shardID)
		w.numLeases[leaseHolder]++
	}

	// Update views with initial lease counts
	for _, nhid := range c.nodes {
		for otherNhid, count := range w.numLeases {
			w.views[nhid][otherNhid] = count
		}
	}

	// Schedule initial gossip and rebalance events for each node
	for _, nhid := range c.nodes {
		w.scheduleEvent(&GossipEvent{
			nhid: nhid,
			time: rand.Intn(c.gossipIntervalMax),
		})
		w.scheduleEvent(&RebalanceEvent{
			nhid: nhid,
			time: rand.Intn(c.rebalanceInterval), // Initial stagger uses full interval range
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

func (w *World) randomRebalanceInterval() int {
	if w.rebalanceIntervalJitter == 0 {
		return w.rebalanceInterval
	}
	// Add random jitter: ±rebalanceIntervalJitter
	jitter := rand.Intn(2*w.rebalanceIntervalJitter+1) - w.rebalanceIntervalJitter
	return w.rebalanceInterval + jitter
}

// Mean calculates the mean number of leases across all nodes.
func (w *World) mean() float64 {
	sum := 0
	for _, count := range w.numLeases {
		sum += count
	}
	return float64(sum) / float64(len(w.numLeases))
}

// ConvergenceTime calculates when the system converged (stopped making changes).
// A system is considered converged when lease counts haven't changed for consecutive snapshots AND there are no pending transfers.
// Returns (tick, numTransfers) where tick is the time and numTransfers is how many transfers had completed, or (-1, -1) if not converged.
func (w *World) ConvergenceTime(threshold float64) (int, int) {
	if len(w.history) < 2 {
		return -1, -1
	}

	// Look backwards to find when the system stopped changing
	convergedTick := -1
	convergedNumTransfers := -1
	for i := len(w.history) - 1; i >= 1; i-- {
		// Cannot be converged if there are pending transfers
		if w.history[i].numPending > 0 {
			continue
		}

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
			if i+1 < len(w.history) && w.history[i+1].numPending == 0 {
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
// A direction change occurs when a node's lease count switches from increasing
// to decreasing or vice versa.
func (w *World) Oscillations() int {
	if len(w.history) < 3 {
		return 0
	}
	count := 0
	for nhid := range w.numLeases {
		// Need at least 3 points to detect a direction change
		if _, exists := w.history[0].numLeases[nhid]; !exists {
			continue
		}

		var lastDirection int // -1 = decreasing, 0 = stable, 1 = increasing
		prevCount := w.history[0].numLeases[nhid]

		for i := 1; i < len(w.history); i++ {
			currentCount, exists := w.history[i].numLeases[nhid]
			if !exists {
				continue
			}

			var currentDirection int
			if currentCount > prevCount {
				currentDirection = 1 // increasing
			} else if currentCount < prevCount {
				currentDirection = -1 // decreasing
			} else {
				currentDirection = 0 // stable (no change)
			}

			// Count a direction change if we switched from increasing to decreasing or vice versa
			// Ignore transitions involving stable (0)
			if lastDirection != 0 && currentDirection != 0 && lastDirection != currentDirection {
				count++
			}

			// Update lastDirection only if we actually moved
			if currentDirection != 0 {
				lastDirection = currentDirection
			}

			prevCount = currentCount
		}
	}
	return count
}

// recordSnapshot records the current state as a snapshot
func (w *World) recordSnapshot() {
	// Copy map for snapshot
	snapshotLeases := make(map[string]int)
	for nhid, count := range w.numLeases {
		snapshotLeases[nhid] = count
	}
	w.history = append(w.history, Snapshot{
		tick:                  w.currentTick,
		numLeases:             snapshotLeases,
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

	// Always record final snapshot at end time to capture final state
	// The snapshot's numPending field will show if there are still pending transfers
	w.currentTick = endTime
	w.recordSnapshot()
}

type Strategy interface {
	Decide(myNhid string, shardID int, replicas []string, view map[string]int, currentTick int) string
}

// ProductionRebalanceStrategy uses the actual production lease rebalancing algorithm.
type ProductionRebalanceStrategy struct{}

func NewProductionRebalanceStrategy() *ProductionRebalanceStrategy {
	return &ProductionRebalanceStrategy{}
}

func (s *ProductionRebalanceStrategy) Decide(myNhid string, shardID int, replicas []string, view map[string]int, currentTick int) string {
	// Convert int to int64
	leaseCounts := make(map[string]int64, len(view))
	for nhid, count := range view {
		leaseCounts[nhid] = int64(count)
	}

	// Call production algorithm via wrapper with shard's replicas
	return driver.FindRebalanceLeaseOpForSimulation(myNhid, int64(shardID), replicas, leaseCounts)
}

// ProbabilisticStrategy wraps a base strategy and only executes transfers with a fixed probability
type ProbabilisticStrategy struct {
	base        Strategy
	probability float64 // e.g., 0.3 means 30% chance of transferring
}

func NewProbabilisticStrategy(base Strategy, probability float64) *ProbabilisticStrategy {
	return &ProbabilisticStrategy{
		base:        base,
		probability: probability,
	}
}

func (s *ProbabilisticStrategy) Decide(myNhid string, shardID int, replicas []string, view map[string]int, currentTick int) string {
	target := s.base.Decide(myNhid, shardID, replicas, view, currentTick)
	if target != "" && rand.Float64() > s.probability {
		return "" // Cancel transfer with (1-p) probability
	}
	return target
}

// AdaptiveProbabilisticStrategy applies probability based on how overfull the node is
type AdaptiveProbabilisticStrategy struct {
	base                 Strategy
	minAbsoluteThreshold int     // Ignore deviations smaller than this (in lease count)
	relativeThreshold    float64 // Relative deviation threshold (e.g., 0.2 = 20%)
	maxProbability       float64 // Cap maximum probability (e.g., 0.5 = 50%)
}

func NewAdaptiveProbabilisticStrategy(base Strategy, minAbsolute int, relativeThreshold float64, maxProb float64) *AdaptiveProbabilisticStrategy {
	return &AdaptiveProbabilisticStrategy{
		base:                 base,
		minAbsoluteThreshold: minAbsolute,
		relativeThreshold:    relativeThreshold,
		maxProbability:       maxProb,
	}
}

func (s *AdaptiveProbabilisticStrategy) Decide(myNhid string, shardID int, replicas []string, view map[string]int, currentTick int) string {
	target := s.base.Decide(myNhid, shardID, replicas, view, currentTick)
	if target == "" {
		return ""
	}

	// Calculate mean
	sum := 0
	for _, count := range view {
		sum += count
	}
	mean := float64(sum) / float64(len(view))

	myCount := float64(view[myNhid])
	targetCount := float64(view[target])

	myDeviation := myCount - mean
	targetDeviation := targetCount - mean

	// Check if EITHER source is overfull enough OR target is underfull enough
	// This respects the production algorithm's logic to help underfull nodes
	myAbsDeviation := math.Abs(myDeviation)
	targetAbsDeviation := math.Abs(targetDeviation)

	// Ignore if both deviations are small
	if myAbsDeviation < float64(s.minAbsoluteThreshold) && targetAbsDeviation < float64(s.minAbsoluteThreshold) {
		return ""
	}

	// Calculate probability based on the larger relative deviation
	// (either source overfullness or target underfullness)
	maxAbsDeviation := math.Max(myAbsDeviation, targetAbsDeviation)
	relativeDeviation := maxAbsDeviation / mean
	probability := math.Min(s.maxProbability, relativeDeviation/s.relativeThreshold)

	// Apply probability
	if rand.Float64() > probability {
		return "" // Cancel transfer
	}
	return target
}

// CooldownStrategy prevents a node from transferring again until cooldown expires
type CooldownStrategy struct {
	base             Strategy
	cooldownTicks    int
	lastTransferTick map[string]int // Per-node last transfer time
}

func NewCooldownStrategy(base Strategy, cooldownTicks int) *CooldownStrategy {
	return &CooldownStrategy{
		base:             base,
		cooldownTicks:    cooldownTicks,
		lastTransferTick: make(map[string]int),
	}
}

func (s *CooldownStrategy) Decide(myNhid string, shardID int, replicas []string, view map[string]int, currentTick int) string {
	// Check if still in cooldown
	if lastTick, exists := s.lastTransferTick[myNhid]; exists {
		if currentTick < lastTick+s.cooldownTicks {
			return "" // Still in cooldown
		}
	}

	target := s.base.Decide(myNhid, shardID, replicas, view, currentTick)
	if target != "" {
		// Record transfer time to start cooldown
		s.lastTransferTick[myNhid] = currentTick
	}
	return target
}

// CooldownJitterStrategy prevents a node from transferring again until cooldown expires, with jitter
type CooldownJitterStrategy struct {
	base             Strategy
	cooldownTicks    int
	cooldownJitter   int // ±jitter applied to cooldown
	lastTransferTick map[string]int
}

func NewCooldownJitterStrategy(base Strategy, cooldownTicks int, cooldownJitter int) *CooldownJitterStrategy {
	return &CooldownJitterStrategy{
		base:             base,
		cooldownTicks:    cooldownTicks,
		cooldownJitter:   cooldownJitter,
		lastTransferTick: make(map[string]int),
	}
}

func (s *CooldownJitterStrategy) randomCooldown() int {
	if s.cooldownJitter == 0 {
		return s.cooldownTicks
	}
	jitter := rand.Intn(2*s.cooldownJitter+1) - s.cooldownJitter
	return s.cooldownTicks + jitter
}

func (s *CooldownJitterStrategy) Decide(myNhid string, shardID int, replicas []string, view map[string]int, currentTick int) string {
	// Check if still in cooldown
	if nextAllowedTick, exists := s.lastTransferTick[myNhid]; exists {
		if currentTick < nextAllowedTick {
			return "" // Still in cooldown
		}
	}

	target := s.base.Decide(myNhid, shardID, replicas, view, currentTick)
	if target != "" {
		// Record when this node can transfer again (with jitter)
		cooldown := s.randomCooldown()
		s.lastTransferTick[myNhid] = currentTick + cooldown
	}
	return target
}

type Config struct {
	name                    string
	numShards               int            // total number of shards (each has 3 replicas)
	nodes                   []string       // list of node IDs
	initialLeases           map[string]int // target initial lease distribution (e.g., {"nhid-1": 25, "nhid-5": 0})
	gossipIntervalMin       int            // in ticks (1 tick = 1ms)
	gossipIntervalMax       int            // in ticks
	transferTimeMin         int            // in ticks
	transferTimeMax         int            // in ticks
	rebalanceInterval       int            // in ticks (typically 1000 = 1 second)
	rebalanceIntervalJitter int            // ±jitter for rebalanceInterval (e.g., 100 = ±100ms)
	duration                int            // in ticks
	strategy                Strategy
}

type Result struct {
	name             string
	numTransfers     int
	convergenceTime  int
	convergenceIndex int
	numOscillations  int
	finalState       map[string]int
}

func (r Result) String() string {
	return fmt.Sprintf("%-40s transfers=%-4d convergesAt=%-6d (op #%-3d) oscillations=%-3d final=%v\n", r.name, r.numTransfers, r.convergenceTime, r.convergenceIndex, r.numOscillations, r.finalState)
}

func (c Config) Run() Result {
	w := NewWorld(c)
	w.runUntil(c.duration, c.strategy)
	convTime, convIndex := w.ConvergenceTime(0.1)
	return Result{
		name:             c.name,
		numTransfers:     w.numTransfers,
		convergenceTime:  convTime,
		convergenceIndex: convIndex,
		numOscillations:  w.Oscillations(),
		finalState:       w.numLeases,
	}
}

func main() {
	// Test cases with different cluster sizes
	// Each shard has 3 replicas distributed randomly
	// Initial lease distribution simulates a new node joining
	testCases := []struct {
		name          string
		numShards     int
		nodes         []string
		initialLeases map[string]int
	}{
		{
			name:      "5 machines, 100 shards",
			numShards: 100,
			nodes:     []string{"nhid-1", "nhid-2", "nhid-3", "nhid-4", "nhid-5"},
			initialLeases: map[string]int{
				"nhid-1": 25,
				"nhid-2": 25,
				"nhid-3": 25,
				"nhid-4": 25,
				"nhid-5": 0, // New node with no leases
			},
		},
		{
			name:      "10 machines, 100 shards",
			numShards: 100,
			nodes:     []string{"nhid-1", "nhid-2", "nhid-3", "nhid-4", "nhid-5", "nhid-6", "nhid-7", "nhid-8", "nhid-9", "nhid-10"},
			initialLeases: map[string]int{
				"nhid-1":  11,
				"nhid-2":  11,
				"nhid-3":  11,
				"nhid-4":  11,
				"nhid-5":  11,
				"nhid-6":  11,
				"nhid-7":  11,
				"nhid-8":  11,
				"nhid-9":  12,
				"nhid-10": 0, // New node with no leases
			},
		},
	}

	// Test each scenario with different strategies
	for _, tc := range testCases {
		baseConfig := Config{
			numShards:         tc.numShards,
			nodes:             tc.nodes,
			initialLeases:     tc.initialLeases,
			gossipIntervalMin: 5,       // 5ms
			gossipIntervalMax: 10,      // 10ms
			transferTimeMin:   500,     // 500ms
			transferTimeMax:   5000,    // 5s
			rebalanceInterval: 1000,    // 1000ms = 1 second (matches production)
			duration:          500_000, // 500 seconds = ~8 minutes
		}

		fmt.Printf("\n=== %s ===\n", tc.name)

		// Baseline: Production strategy
		config := baseConfig
		config.name = "Production (baseline)"
		config.strategy = NewProductionRebalanceStrategy()
		fmt.Println(config.Run())

		// Fixed Probabilistic strategies
		for _, prob := range []float64{0.1, 0.3, 0.5, 0.7} {
			config := baseConfig
			config.name = fmt.Sprintf("Probabilistic (p=%.1f)", prob)
			config.strategy = NewProbabilisticStrategy(NewProductionRebalanceStrategy(), prob)
			fmt.Println(config.Run())
		}

		// Adaptive Probabilistic strategies
		adaptiveConfigs := []struct {
			name        string
			minAbsolute int
			relThresh   float64
			maxProb     float64
		}{
			{"Adaptive (conservative)", 5, 0.20, 0.5},
			{"Adaptive (moderate)", 3, 0.15, 0.7},
			{"Adaptive (aggressive)", 2, 0.10, 0.8},
		}
		for _, ac := range adaptiveConfigs {
			config := baseConfig
			config.name = ac.name
			config.strategy = NewAdaptiveProbabilisticStrategy(
				NewProductionRebalanceStrategy(),
				ac.minAbsolute,
				ac.relThresh,
				ac.maxProb,
			)
			fmt.Println(config.Run())
		}

		// Cooldown strategies
		for _, cooldown := range []int{2000, 5000, 10000} {
			config := baseConfig
			config.name = fmt.Sprintf("Cooldown (%dms)", cooldown)
			config.strategy = NewCooldownStrategy(NewProductionRebalanceStrategy(), cooldown)
			fmt.Println(config.Run())
		}

		// Combinations: Probabilistic + Cooldown
		for _, cooldown := range []int{5000, 10000} {
			for _, prob := range []float64{0.1, 0.3, 0.5} {
				config = baseConfig
				config.name = fmt.Sprintf("Prob(%.1f) + Cooldown(%ds)", prob, cooldown/1000)
				config.strategy = NewCooldownStrategy(
					NewProbabilisticStrategy(NewProductionRebalanceStrategy(), prob),
					cooldown,
				)
				fmt.Println(config.Run())
			}
		}

		// Combinations: Adaptive + Cooldown
		for _, cooldown := range []int{5000, 10000} {
			adaptiveCooldownConfigs := []struct {
				name        string
				minAbsolute int
				relThresh   float64
				maxProb     float64
			}{
				{"conservative", 5, 0.20, 0.5},
				{"moderate", 3, 0.15, 0.7},
				{"aggressive", 2, 0.10, 0.8},
			}
			for _, ac := range adaptiveCooldownConfigs {
				config = baseConfig
				config.name = fmt.Sprintf("Adaptive(%s) + Cooldown(%ds)", ac.name, cooldown/1000)
				config.strategy = NewCooldownStrategy(
					NewAdaptiveProbabilisticStrategy(NewProductionRebalanceStrategy(), ac.minAbsolute, ac.relThresh, ac.maxProb),
					cooldown,
				)
				fmt.Println(config.Run())
			}
		}

		// Jitter experiments
		fmt.Printf("\n--- Jitter Experiments ---\n")

		// (1) Jitter on rebalance decisions only
		for _, jitter := range []int{50, 100, 200, 500, 1000} {
			config = baseConfig
			config.name = fmt.Sprintf("Jitter: Rebalance ±%dms", jitter)
			config.rebalanceIntervalJitter = jitter
			config.strategy = NewProductionRebalanceStrategy()
			fmt.Println(config.Run())
		}

		// (2) Jitter on cooldown only
		for _, jitter := range []int{250, 500, 1000, 2000} {
			config = baseConfig
			config.name = fmt.Sprintf("Jitter: Cooldown(5s±%dms)", jitter)
			config.strategy = NewCooldownJitterStrategy(NewProductionRebalanceStrategy(), 5000, jitter)
			fmt.Println(config.Run())
		}

		// (2b) 10s cooldown with jitter
		for _, jitter := range []int{500, 1000, 2000, 4000} {
			config = baseConfig
			config.name = fmt.Sprintf("Jitter: Cooldown(10s±%dms)", jitter)
			config.strategy = NewCooldownJitterStrategy(NewProductionRebalanceStrategy(), 10000, jitter)
			fmt.Println(config.Run())
		}

		// (3) Jitter on both rebalance + cooldown
		jitterCombos := []struct {
			rebalanceJitter int
			cooldownJitter  int
		}{
			{100, 500},
			{200, 1000},
			{500, 2000},
			{1000, 2000},
		}
		for _, jc := range jitterCombos {
			config = baseConfig
			config.name = fmt.Sprintf("Jitter: Rebalance±%dms + Cooldown(5s±%dms)", jc.rebalanceJitter, jc.cooldownJitter)
			config.rebalanceIntervalJitter = jc.rebalanceJitter
			config.strategy = NewCooldownJitterStrategy(NewProductionRebalanceStrategy(), 5000, jc.cooldownJitter)
			fmt.Println(config.Run())
		}

		// Jitter + existing strategies (test with best performing ones)
		config = baseConfig
		config.name = "Jitter(±100ms) + Prob(0.5)"
		config.rebalanceIntervalJitter = 100
		config.strategy = NewProbabilisticStrategy(NewProductionRebalanceStrategy(), 0.5)
		fmt.Println(config.Run())

		config = baseConfig
		config.name = "Jitter(±500ms) + Prob(0.5)"
		config.rebalanceIntervalJitter = 500
		config.strategy = NewProbabilisticStrategy(NewProductionRebalanceStrategy(), 0.5)
		fmt.Println(config.Run())

		config = baseConfig
		config.name = "Jitter(±100ms) + Adaptive(moderate)"
		config.rebalanceIntervalJitter = 100
		config.strategy = NewAdaptiveProbabilisticStrategy(NewProductionRebalanceStrategy(), 3, 0.15, 0.7)
		fmt.Println(config.Run())

		config = baseConfig
		config.name = "Jitter(±500ms) + Adaptive(moderate)"
		config.rebalanceIntervalJitter = 500
		config.strategy = NewAdaptiveProbabilisticStrategy(NewProductionRebalanceStrategy(), 3, 0.15, 0.7)
		fmt.Println(config.Run())

		// Jitter + Cooldown Jitter + Prob(0.1)
		config = baseConfig
		config.name = "Jitter(±100ms) + CooldownJitter(5s±500ms) + Prob(0.1)"
		config.rebalanceIntervalJitter = 100
		config.strategy = NewCooldownJitterStrategy(
			NewProbabilisticStrategy(NewProductionRebalanceStrategy(), 0.1),
			5000,
			500,
		)
		fmt.Println(config.Run())

		config = baseConfig
		config.name = "Jitter(±500ms) + CooldownJitter(5s±2s) + Prob(0.1)"
		config.rebalanceIntervalJitter = 500
		config.strategy = NewCooldownJitterStrategy(
			NewProbabilisticStrategy(NewProductionRebalanceStrategy(), 0.1),
			5000,
			2000,
		)
		fmt.Println(config.Run())

		// 10s cooldown jitter combos
		config = baseConfig
		config.name = "Jitter(±100ms) + CooldownJitter(10s±500ms) + Prob(0.1)"
		config.rebalanceIntervalJitter = 100
		config.strategy = NewCooldownJitterStrategy(
			NewProbabilisticStrategy(NewProductionRebalanceStrategy(), 0.1),
			10000,
			500,
		)
		fmt.Println(config.Run())

		config = baseConfig
		config.name = "Jitter(±500ms) + CooldownJitter(10s±1000ms) + Prob(0.1)"
		config.rebalanceIntervalJitter = 500
		config.strategy = NewCooldownJitterStrategy(
			NewProbabilisticStrategy(NewProductionRebalanceStrategy(), 0.1),
			10000,
			1000,
		)
		fmt.Println(config.Run())

		config = baseConfig
		config.name = "Jitter(±500ms) + CooldownJitter(10s±2000ms) + Prob(0.1)"
		config.rebalanceIntervalJitter = 500
		config.strategy = NewCooldownJitterStrategy(
			NewProbabilisticStrategy(NewProductionRebalanceStrategy(), 0.1),
			10000,
			2000,
		)
		fmt.Println(config.Run())
	}

}
