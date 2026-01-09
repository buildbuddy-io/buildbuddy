package main

import (
	"container/heap"
	"flag"
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

	// Check if system is balanced for early stopping
	if w.numPendingTransfers == 0 && w.isBalanced() {
		if w.firstBalancedTime < 0 {
			w.firstBalancedTime = w.currentTick
		}
	} else {
		// Reset if no longer balanced or there are pending transfers
		w.firstBalancedTime = -1
	}

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

	// Early stopping
	firstBalancedTime int // First time system became balanced (-1 if not balanced)
	gracePeriod       int // Ticks to wait after balanced before stopping (e.g., 60000 = 60s)
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
		shards:                  make(map[int]*Shard),
		nodeReplicas:            make(map[string][]int),
		nodeLeases:              make(map[string][]int),
		numLeases:               make(map[string]int),
		views:                   make(map[string]map[string]int),
		eventQueue:              make(EventQueue, 0),
		currentTick:             0,
		history:                 []Snapshot{},
		gossipIntervalMin:       c.gossipIntervalMin,
		gossipIntervalMax:       c.gossipIntervalMax,
		transferTimeMin:         c.transferTimeMin,
		transferTimeMax:         c.transferTimeMax,
		rebalanceInterval:       c.rebalanceInterval,
		rebalanceIntervalJitter: c.rebalanceIntervalJitter,
		firstBalancedTime:       -1,
		gracePeriod:             60000, // 60 seconds grace period
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

// isBalanced checks if the current state is balanced using production's canConverge logic
func (w *World) isBalanced() bool {
	const leaseCountMeanRatioThreshold = 0.05
	const minLeaseCountThreshold = 2.0

	mean := w.mean()
	overfullThreshold := int(math.Ceil(mean + math.Max(mean*leaseCountMeanRatioThreshold, minLeaseCountThreshold)))
	underfullThreshold := int(math.Floor(mean - math.Max(mean*leaseCountMeanRatioThreshold, minLeaseCountThreshold)))

	for _, count := range w.numLeases {
		if count > overfullThreshold || count < underfullThreshold {
			return false
		}
	}
	return true
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

		// Early stop if system has been balanced for the grace period
		if w.firstBalancedTime >= 0 && item.time >= w.firstBalancedTime+w.gracePeriod {
			w.currentTick = item.time
			w.recordSnapshot()
			return
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

// calculateDistanceFromDeadZone returns how far a count is from the dead zone boundaries.
// Returns 0 if inside the dead zone, otherwise returns the distance beyond the boundary.
func calculateDistanceFromDeadZone(count float64, lowerBound, upperBound float64) float64 {
	if count >= lowerBound && count <= upperBound {
		return 0 // Inside dead zone
	}
	if count > upperBound {
		return count - upperBound // Distance above upper boundary
	}
	return lowerBound - count // Distance below lower boundary
}

// AdaptiveProbabilisticStrategy applies probability based on how overfull the node is
type AdaptiveProbabilisticStrategy struct {
	base                 Strategy
	minAbsoluteThreshold int     // Absolute dead zone threshold (e.g., 2 leases)
	percentThreshold     float64 // Percent dead zone threshold (e.g., 0.05 = 5%)
	relativeThreshold    float64 // Distance/mean ratio to reach maxProbability (e.g., 0.2 = 20%)
	maxProbability       float64 // Cap maximum probability (e.g., 0.5 = 50%)
}

func NewAdaptiveProbabilisticStrategy(base Strategy, minAbsolute int, percentThreshold float64, relativeThreshold float64, maxProb float64) *AdaptiveProbabilisticStrategy {
	return &AdaptiveProbabilisticStrategy{
		base:                 base,
		minAbsoluteThreshold: minAbsolute,
		percentThreshold:     percentThreshold,
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

	// Define dead zone using BOTH percent and absolute (like driver.go)
	// deadZone = max(mean * percentThreshold, minAbsoluteThreshold)
	deadZoneWidth := math.Max(mean*s.percentThreshold, float64(s.minAbsoluteThreshold))
	upperBound := mean + deadZoneWidth
	lowerBound := mean - deadZoneWidth

	myCount := float64(view[myNhid])
	targetCount := float64(view[target])

	// Calculate distance FROM dead zone for source and target
	myDistance := calculateDistanceFromDeadZone(myCount, lowerBound, upperBound)
	targetDistance := calculateDistanceFromDeadZone(targetCount, lowerBound, upperBound)

	// If both are in dead zone, no transfer
	maxDistance := math.Max(myDistance, targetDistance)
	if maxDistance == 0 {
		return ""
	}

	// Express distance as fraction of mean (scale-independent)
	// relativeThreshold = "at this fraction of mean, reach maxProbability"
	relativeDistance := maxDistance / mean
	probability := math.Min(s.maxProbability, relativeDistance/s.relativeThreshold)

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
	isBalanced       bool
	maxDeviation     int
}

func (r Result) String() string {
	balanced := "✓"
	if !r.isBalanced {
		balanced = fmt.Sprintf("✗UNBALANCED(maxDev=%d)", r.maxDeviation)
	}

	convergence := fmt.Sprintf("convergesAt=%-6d (op #%-3d)", r.convergenceTime, r.convergenceIndex)
	if r.convergenceTime < 0 || r.convergenceIndex < 0 {
		convergence = "✗FAILED_TO_CONVERGE            "
	}

	return fmt.Sprintf("%-40s transfers=%-4d %s oscillations=%-3d %s final=%v\n",
		r.name, r.numTransfers, convergence, r.numOscillations, balanced, r.finalState)
}

type AggregatedResult struct {
	name                   string
	runs                   int
	avgTransfers           float64
	avgConvergenceTime     float64
	avgOscillations        float64
	minOscillations        int
	maxOscillations        int
	stddevOscillations     float64
	failedConvergenceCount int
	unbalancedCount        int
	allResults             []Result
}

func (ar AggregatedResult) String() string {
	return fmt.Sprintf("%-40s runs=%d avgOsc=%.1f (min=%d max=%d std=%.1f) avgTransfers=%.1f avgConverge=%.0fms failedConv=%d unbalanced=%d\n",
		ar.name, ar.runs, ar.avgOscillations, ar.minOscillations, ar.maxOscillations, ar.stddevOscillations, ar.avgTransfers, ar.avgConvergenceTime, ar.failedConvergenceCount, ar.unbalancedCount)
}

func (c Config) Run() Result {
	w := NewWorld(c)
	w.runUntil(c.duration, c.strategy)
	convTime, convIndex := w.ConvergenceTime(0.1)

	// Calculate max deviation for reporting
	mean := w.mean()
	maxDeviation := 0
	for _, count := range w.numLeases {
		deviation := count - int(mean)
		if deviation < 0 {
			deviation = -deviation
		}
		if deviation > maxDeviation {
			maxDeviation = deviation
		}
	}

	return Result{
		name:             c.name,
		numTransfers:     w.numTransfers,
		convergenceTime:  convTime,
		convergenceIndex: convIndex,
		numOscillations:  w.Oscillations(),
		finalState:       w.numLeases,
		isBalanced:       w.isBalanced(),
		maxDeviation:     maxDeviation,
	}
}

func aggregateResults(name string, results []Result) AggregatedResult {
	iterations := len(results)
	if iterations == 0 {
		return AggregatedResult{name: name}
	}

	// Calculate statistics
	var totalTransfers, totalConvergenceTime, totalOscillations int
	minOsc := results[0].numOscillations
	maxOsc := results[0].numOscillations
	failedCount := 0
	unbalancedCount := 0

	for _, r := range results {
		totalTransfers += r.numTransfers
		// Check if converged (both time and index must be non-negative)
		if r.convergenceTime >= 0 && r.convergenceIndex >= 0 {
			totalConvergenceTime += r.convergenceTime
		} else {
			failedCount++
		}
		if !r.isBalanced {
			unbalancedCount++
		}
		totalOscillations += r.numOscillations
		if r.numOscillations < minOsc {
			minOsc = r.numOscillations
		}
		if r.numOscillations > maxOsc {
			maxOsc = r.numOscillations
		}
	}

	avgTransfers := float64(totalTransfers) / float64(iterations)
	avgOscillations := float64(totalOscillations) / float64(iterations)
	avgConvergenceTime := float64(totalConvergenceTime) / float64(iterations-failedCount)
	if failedCount == iterations {
		avgConvergenceTime = -1
	}

	// Calculate standard deviation for oscillations
	var sumSquaredDiff float64
	for _, r := range results {
		diff := float64(r.numOscillations) - avgOscillations
		sumSquaredDiff += diff * diff
	}
	stddev := math.Sqrt(sumSquaredDiff / float64(iterations))

	return AggregatedResult{
		name:                   name,
		runs:                   iterations,
		avgTransfers:           avgTransfers,
		avgConvergenceTime:     avgConvergenceTime,
		avgOscillations:        avgOscillations,
		minOscillations:        minOsc,
		maxOscillations:        maxOsc,
		stddevOscillations:     stddev,
		failedConvergenceCount: failedCount,
		unbalancedCount:        unbalancedCount,
		allResults:             results,
	}
}

func (c Config) RunMultiple(iterations int) AggregatedResult {
	results := make([]Result, iterations)
	for i := 0; i < iterations; i++ {
		results[i] = c.Run()
	}
	return aggregateResults(c.name, results)
}

func generateTestCase(numMachines int, numShards int) (string, []string, map[string]int) {
	// Generate node IDs
	nodes := make([]string, numMachines)
	for i := 0; i < numMachines; i++ {
		nodes[i] = fmt.Sprintf("nhid-%d", i+1)
	}

	// Distribute N shards among first M-1 machines (last machine is new node with 0)
	initialLeases := make(map[string]int)
	leasesPerNode := numShards / (numMachines - 1)
	remainder := numShards % (numMachines - 1)

	for i := 0; i < numMachines-1; i++ {
		initialLeases[nodes[i]] = leasesPerNode
		// Distribute remainder to first few nodes
		if i < remainder {
			initialLeases[nodes[i]]++
		}
	}
	// New node with no leases
	initialLeases[nodes[numMachines-1]] = 0

	name := fmt.Sprintf("%d machines, %d shards", numMachines, numShards)
	return name, nodes, initialLeases
}

func main() {
	iterations := flag.Int("iterations", 1, "Number of iterations to run for each strategy (default 1 = single run)")
	flag.Parse()

	// Test cases with different cluster sizes
	// Each shard has 3 replicas distributed randomly
	// Initial lease distribution simulates a new node joining
	testConfigs := []struct {
		numMachines int
		numShards   int
	}{
		{5, 100},
		{5, 300},
		{5, 600},
		{10, 100},
		{10, 300},
		{10, 600},
		{20, 100},
		{20, 300},
		{20, 600},
	}

	for _, tc := range testConfigs {
		name, nodes, initialLeases := generateTestCase(tc.numMachines, tc.numShards)
		runTestCase(name, tc.numShards, nodes, initialLeases, *iterations)
	}
}

func runTestCase(name string, numShards int, nodes []string, initialLeases map[string]int, iterations int) {
	baseConfig := Config{
		numShards:         numShards,
		nodes:             nodes,
		initialLeases:     initialLeases,
		gossipIntervalMin: 5,
		gossipIntervalMax: 10,
		transferTimeMin:   500,
		transferTimeMax:   5000,
		rebalanceInterval: 1000,
		duration:          1200_000,
	}

	fmt.Printf("\n=== %s ===\n", name)

	// Define test strategies with factory functions to create fresh instances
	testStrategies := []struct {
		name            string
		strategyFactory func() Strategy
	}{
		{"Production (baseline)", func() Strategy { return NewProductionRebalanceStrategy() }},
		{"Cooldown(5s)", func() Strategy { return NewCooldownStrategy(NewProductionRebalanceStrategy(), 5000) }},
		{"Cooldown(10s)", func() Strategy { return NewCooldownStrategy(NewProductionRebalanceStrategy(), 10000) }},
		{"Cooldown(5s±500ms)", func() Strategy { return NewCooldownJitterStrategy(NewProductionRebalanceStrategy(), 5000, 500) }},
		{"Cooldown(10s±500ms)", func() Strategy { return NewCooldownJitterStrategy(NewProductionRebalanceStrategy(), 10000, 500) }},
		{"Prob(0.1)", func() Strategy {
			return NewProbabilisticStrategy(NewProductionRebalanceStrategy(), 0.1)
		}},
		{"Prob(0.3)", func() Strategy {
			return NewProbabilisticStrategy(NewProductionRebalanceStrategy(), 0.1)
		}},
		{"Prob(0.5)", func() Strategy {
			return NewProbabilisticStrategy(NewProductionRebalanceStrategy(), 0.1)
		}},
		{"Adaptive(conservative)", func() Strategy {
			return NewAdaptiveProbabilisticStrategy(NewProductionRebalanceStrategy(), 2, 0.05, 0.20, 0.5)
		}},
		{"Adaptive(moderate)", func() Strategy {
			return NewAdaptiveProbabilisticStrategy(NewProductionRebalanceStrategy(), 2, 0.05, 0.15, 0.7)
		}},
		{"Adaptive(aggresive)", func() Strategy {
			return NewAdaptiveProbabilisticStrategy(NewProductionRebalanceStrategy(), 2, 0.05, 0.10, 0.8)
		}},
		{"Prob(0.1) + Cooldown(10s)", func() Strategy {
			return NewCooldownStrategy(NewProbabilisticStrategy(NewProductionRebalanceStrategy(), 0.1), 10000)
		}},
		{"Prob(0.3) + Cooldown(10s)", func() Strategy {
			return NewCooldownStrategy(NewProbabilisticStrategy(NewProductionRebalanceStrategy(), 0.3), 10000)
		}},
		{"Prob(0.5) + Cooldown(10s)", func() Strategy {
			return NewCooldownStrategy(NewProbabilisticStrategy(NewProductionRebalanceStrategy(), 0.5), 10000)
		}},
		{"Adaptive(conservative) + Cooldown(10s)", func() Strategy {
			return NewCooldownStrategy(NewAdaptiveProbabilisticStrategy(NewProductionRebalanceStrategy(), 2, 0.05, 0.20, 0.5), 10000)
		}},
		{"Adaptive(moderate) + Cooldown(10s)", func() Strategy {
			return NewCooldownStrategy(NewAdaptiveProbabilisticStrategy(NewProductionRebalanceStrategy(), 2, 0.05, 0.15, 0.7), 10000)
		}},
		{"Adaptive(aggresive) + Cooldown(10s)", func() Strategy {
			return NewCooldownStrategy(NewAdaptiveProbabilisticStrategy(NewProductionRebalanceStrategy(), 2, 0.05, 0.10, 0.8), 10000)
		}},
	}

	var aggregatedResults []AggregatedResult

	for _, strat := range testStrategies {
		// Run multiple iterations with fresh strategy instances
		results := make([]Result, iterations)
		for i := 0; i < iterations; i++ {
			config := baseConfig
			config.name = strat.name
			config.strategy = strat.strategyFactory() // Create fresh strategy for each iteration
			results[i] = config.Run()
		}

		agg := aggregateResults(strat.name, results)
		aggregatedResults = append(aggregatedResults, agg)

		if iterations == 1 {
			// Single run: just show the result directly
			fmt.Printf("%s", agg.allResults[0].String())
		} else {
			// Multiple runs: show detailed results and summary
			fmt.Printf("\n%s:\n", strat.name)
			for i, r := range agg.allResults {
				fmt.Printf("  Run %2d: %s", i+1, r.String())
			}
			fmt.Printf("  Summary: %s\n", agg.String())
		}
	}

	// Show recommendations for multiple runs
	if iterations > 1 {
		showRecommendations(name, aggregatedResults)
	}
}

func showRecommendations(testCaseName string, results []AggregatedResult) {
	// Sort by average oscillations
	sorted := make([]AggregatedResult, len(results))
	copy(sorted, results)
	// Use a simple sort with stable ordering
	for i := 0; i < len(sorted)-1; i++ {
		for j := i + 1; j < len(sorted); j++ {
			if sorted[j].avgOscillations < sorted[i].avgOscillations {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}

	fmt.Printf("\n--- RECOMMENDATION for %s ---\n", testCaseName)
	fmt.Printf("  Best strategy (lowest avg oscillations): %s\n", sorted[0].name)
	fmt.Printf("    Avg Oscillations: %.1f (min=%d, max=%d, std=%.1f)\n",
		sorted[0].avgOscillations, sorted[0].minOscillations, sorted[0].maxOscillations, sorted[0].stddevOscillations)
	fmt.Printf("    Avg Convergence Time: %.0fms\n", sorted[0].avgConvergenceTime)
	fmt.Printf("    Avg Transfers: %.1f\n", sorted[0].avgTransfers)

	fmt.Printf("\n  Top 3 strategies by oscillations:\n")
	for i := 0; i < 3 && i < len(sorted); i++ {
		fmt.Printf("    %d. %s - avgOsc=%.1f std=%.1f\n",
			i+1, sorted[i].name, sorted[i].avgOscillations, sorted[i].stddevOscillations)
	}
}
