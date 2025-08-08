---
slug: self-healing-distributed-systems-raft-driver
title: Building Self-Healing Distributed Systems with Intelligent Rebalancing
description: How BuildBuddy's Raft Driver uses multi-dimensional scoring algorithms and statistical analysis to automatically maintain cluster health, balance load, and prevent failures before they happen.
authors: siggi
date: 2025-01-07:16:00:00
image: /img/blog/raft-driver-rebalancing.webp
tags: [engineering, distributed-systems, raft, consensus]
---

Most Raft implementations handle consensus well but require manual intervention for operational tasks like rebalancing replicas or replacing failed nodes. At BuildBuddy, we've built an intelligent driver layer on top of Raft that creates truly self-healing distributed systems. Our driver uses multi-dimensional scoring algorithms, statistical analysis, and predictive intelligence to automatically maintain optimal cluster health. Here's how we turned a consensus algorithm into an autonomous operations engineer.

<!-- truncate -->

## The Problem: Raft Doesn't Manage Itself

Raft is elegant for achieving consensus, but it doesn't address operational challenges:

- **Uneven load distribution**: Some nodes handle more traffic than others
- **Disk space imbalance**: Nodes fill up at different rates
- **Failed node recovery**: Dead replicas need manual replacement
- **Range splitting**: Large data ranges need subdivision
- **Lease management**: Leader distribution affects performance

Traditional solutions require human operators or simplistic scripts. We wanted something smarter - a system that manages itself with the sophistication of an experienced SRE.

## The Architecture: A Priority-Driven Operations Queue

Our driver implements a sophisticated decision engine that continuously monitors cluster state and takes corrective actions:

```go
type DriverAction int

const (
    DriverReplaceDeadReplica  // Priority: 700 - Critical for maintaining quorum
    DriverAddReplica          // Priority: 600 - Important for replication factor
    DriverFinishReplicaRemoval // Priority: 500 - Complete in-progress operations
    DriverRemoveDeadReplica   // Priority: 400 - Clean up dead nodes
    DriverRemoveReplica       // Priority: 300 - Reduce over-replication
    DriverSplitRange          // Priority: 200 - Split large ranges
    DriverRebalanceReplica    // Priority: 0   - Optimize distribution
    DriverRebalanceLease      // Priority: 0   - Balance leader load
)
```

Actions are processed by priority, ensuring critical operations (like maintaining quorum) always take precedence over optimizations.

## Multi-Dimensional Scoring: Beyond Simple Metrics

The heart of our system is a sophisticated scoring algorithm that evaluates nodes across multiple dimensions:

### 1. Disk Capacity Scoring

We use graduated thresholds to prevent disk exhaustion:

```go
const (
    maximumDiskCapacity          = 0.95  // Evacuate replicas immediately
    maxDiskCapacityForRebalance  = 0.925 // Don't accept new replicas
)

func isDiskFull(usage *StoreUsage) bool {
    if usage.TotalCapacityBytes == 0 {
        return false
    }
    return float64(usage.UsedCapacityBytes) /
           float64(usage.TotalCapacityBytes) > maximumDiskCapacity
}
```

Nodes approaching capacity are proactively evacuated before they become critical.

### 2. Statistical Mean Deviation Analysis

We determine if a node is "above", "below", or "around" the cluster mean using statistical thresholds:

```go
func replicaCountMeanLevel(stores *StoresWithStats, usage *StoreUsage) MeanLevel {
    mean := stores.ReplicaCount.Mean
    count := usage.ReplicaCount

    // Calculate deviation thresholds
    threshold := math.Max(
        mean * replicaCountMeanRatioThreshold,  // 5% of mean
        minReplicaCountThreshold,                // Minimum 2 replicas
    )

    if float64(count) > mean + threshold {
        return AboveMean
    } else if float64(count) < mean - threshold {
        return BelowMean
    }
    return AroundMean
}
```

This prevents thrashing - we only rebalance when there's meaningful deviation.

### 3. Convergence Detection

Before rebalancing, we verify the operation will actually improve cluster balance:

```go
func canConvergeByRebalanceReplica(choice *rebalanceChoice,
                                   allStores *StoresWithStats) bool {
    overfullThreshold := math.Ceil(aboveMeanThreshold(allStores.ReplicaCount.Mean))

    // Case 1: Source store is way above mean
    if choice.existing.usage.ReplicaCount > overfullThreshold {
        return true
    }

    // Case 2: Source is slightly above, but target is way below
    if float64(choice.existing.usage.ReplicaCount) > allStores.ReplicaCount.Mean {
        underfullThreshold := math.Floor(belowMeanThreshold(allStores.ReplicaCount.Mean))
        for _, candidate := range choice.candidates {
            if candidate.usage.ReplicaCount < underfullThreshold {
                return true
            }
        }
    }
    return false
}
```

This ensures we're moving toward equilibrium, not just shuffling replicas.

## The Candidate Comparison Algorithm

When choosing between potential targets, we use a sophisticated multi-factor comparison:

```go
func compareByScore(a, b *candidate) int {
    // Priority 1: Disk capacity (critical)
    if a.fullDisk != b.fullDisk {
        if a.fullDisk {
            return 1  // a is worse
        }
        return -1     // b is worse
    }

    // Priority 2: Replica count distribution
    replicaComparison := compareMeanLevels(
        a.replicaCountMeanLevel,
        b.replicaCountMeanLevel,
        a.usage.ReplicaCount,
        b.usage.ReplicaCount,
    )
    if replicaComparison != 0 {
        return replicaComparison
    }

    // Priority 3: Lease count distribution
    leaseComparison := compareMeanLevels(
        a.leaseCountMeanLevel,
        b.leaseCountMeanLevel,
        a.usage.LeaseCount,
        b.usage.LeaseCount,
    )
    if leaseComparison != 0 {
        return leaseComparison
    }

    // Priority 4: Disk usage percentage
    return cmp.Compare(
        diskUsagePercentage(a.usage),
        diskUsagePercentage(b.usage),
    )
}
```

This creates a nuanced ranking that considers both critical constraints and optimization goals.

## Intelligent Range Splitting with Jitter

To prevent synchronized splits across the cluster (thundering herd), we use jittered thresholds:

```go
func shouldSplitRange(rd *RangeDescriptor, sizeBytes int64) bool {
    targetSize := config.TargetRangeSizeBytes()
    jitterFactor := config.RangeSizeJitterFactor() // 0.2 = ±20%

    // Add random jitter to prevent synchronized splits
    jitter := (rand.Float64()*2 - 1) * jitterFactor * float64(targetSize)
    threshold := float64(targetSize) + jitter

    return float64(sizeBytes) > threshold
}
```

This simple technique prevents cluster-wide split storms that could overwhelm the system.

## Handling Complex Failure Scenarios

The driver handles intricate failure cases with atomic operations:

### Dead Replica Replacement

When a node dies, we atomically add a replacement before removing the dead replica:

```go
func replaceDeadReplica(rd *RangeDescriptor, deadReplicaID uint64) *change {
    // Find suitable replacement target
    target := findBestTarget(rd, excludeDeadNodes())

    // Atomic add-then-remove operation
    return &change{
        addOp: &AddReplicaRequest{
            Range: rd,
            Node:  target,
        },
        removeOp: &RemoveReplicaRequest{
            Range:     rd,
            ReplicaId: deadReplicaID,
        },
        // Execute add before remove to maintain quorum
        atomic: true,
    }
}
```

This ensures we never drop below the replication factor, even temporarily.

### Graceful Degradation

Different behaviors based on quorum state:

```go
func getPriorityAdjustment(rd *RangeDescriptor) float64 {
    liveReplicas := countLiveReplicas(rd)
    targetReplicas := getTargetReplicas(rd)

    if liveReplicas < quorumSize(targetReplicas) {
        // Critical: At risk of losing quorum
        return 1000.0
    } else if liveReplicas < targetReplicas {
        // Important: Under-replicated but have quorum
        return 500.0
    } else if liveReplicas == targetReplicas {
        // Normal: Optimization only
        return 0.0
    }
    // Over-replicated: Lower priority
    return -100.0
}
```

## Production Intelligence Features

### 1. New Replica Grace Period

Fresh replicas need time to catch up before participating in rebalancing decisions:

```go
const newReplicaGracePeriod = 5 * time.Minute

func isReplicaReady(replica *Replica) bool {
    age := time.Since(replica.CreatedAt)
    if age < newReplicaGracePeriod {
        return false // Still catching up
    }
    return replica.RaftState == StateFollower ||
           replica.RaftState == StateLeader
}
```

### 2. Retry Logic with Backoff

Failed operations are retried with exponential backoff:

```go
func (rq *Queue) processWithRetry(item *queueItem) {
    if item.retryCount >= maxRetry {
        alert.UnexpectedEvent("driver-max-retry",
            "Operation failed after %d retries: %v", maxRetry, item)
        return
    }

    result := rq.processItem(item)
    if result.requeueType == RequeueRetry {
        item.retryCount++
        delay := time.Duration(math.Pow(2, float64(item.retryCount))) * time.Second
        rq.requeueAfter(item, delay)
    }
}
```

### 3. Comprehensive Metrics

Every decision is instrumented for observability:

```go
var (
    driverActionsTotal = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name: "raft_driver_actions_total",
        },
        []string{"action", "result"},
    )

    rebalanceScoreHistogram = prometheus.NewHistogram(
        prometheus.HistogramOpts{
            Name:    "raft_driver_rebalance_score",
            Buckets: prometheus.LinearBuckets(-1, 0.1, 20),
        },
    )
)
```

## Real-World Impact

This intelligent driver has transformed our operations:

### Before (Manual Management)

- **Incident response time**: 15-30 minutes for dead node recovery
- **Disk full incidents**: 2-3 per month requiring manual intervention
- **Load imbalance**: Some nodes at 90% CPU while others at 10%
- **On-call burden**: Regular pages for rebalancing tasks

### After (Self-Healing Driver)

- **Incident response time**: <30 seconds automatic recovery
- **Disk full incidents**: Zero in the past year
- **Load imbalance**: Maximum 15% deviation from mean
- **On-call burden**: 90% reduction in operational pages

## Lessons Learned

Building this system taught us crucial lessons about distributed systems operations:

### 1. Statistical Thresholds Prevent Thrashing

Using mean deviation with minimum thresholds prevents constant rebalancing:

- Percentage thresholds handle different cluster sizes
- Minimum absolute thresholds prevent noise in small clusters
- Hysteresis prevents oscillation

### 2. Multi-Dimensional Scoring Is Essential

Single metrics lie. Our scoring considers:

- Hard constraints (disk space)
- Soft goals (load balance)
- Temporal factors (node age)
- Network topology (connection health)

### 3. Priorities Must Be Dynamic

Static priorities don't work. We adjust based on:

- Cluster health (quorum risk)
- Time of day (defer optimizations during peak)
- Recent failure rate (more conservative after incidents)

### 4. Convergence Checking Saves Resources

Before any operation, verify it improves the situation:

- Prevents unnecessary operations
- Reduces network traffic
- Improves overall stability

## The Broader Pattern: Autonomous Operations

This driver exemplifies a broader pattern in modern distributed systems - embedding operational intelligence directly into the system:

```
Traditional Approach:          Modern Approach:
System + Human Operator   →    Self-Managing System

- Manual intervention          - Automatic healing
- Reactive responses          - Predictive actions
- Point-in-time decisions     - Continuous optimization
- Tribal knowledge            - Encoded expertise
```

## Implementation Tips

If you're building similar systems, consider:

1. **Start with priorities**: Define what matters most (availability > performance > efficiency)
2. **Use statistical analysis**: Simple thresholds cause thrashing
3. **Add jitter everywhere**: Prevent synchronized operations
4. **Instrument everything**: You can't improve what you can't measure
5. **Build in grace periods**: New components need time to stabilize
6. **Plan for partial failures**: Graceful degradation over perfect operation

## Conclusion

By building intelligence into our Raft driver, we've created a distributed system that manages itself better than human operators could. The combination of multi-dimensional scoring, statistical analysis, and priority-driven operations creates a robust, self-healing system that maintains optimal performance without manual intervention.

This approach - encoding operational expertise directly into the system - represents the future of distributed systems. Instead of building systems that require careful operation, we're building systems that operate themselves carefully.

The code is complex, but the result is simple: a distributed system that just works, healing itself faster than operators can even notice problems. That's the power of intelligent, autonomous operations.

If you're interested in trying BuildBuddy's self-healing distributed cache, check out our [documentation](https://www.buildbuddy.io/docs/cache/). And if building autonomous distributed systems excites you, [we're hiring](https://www.buildbuddy.io/careers/)!
