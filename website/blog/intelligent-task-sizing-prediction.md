---
slug: intelligent-task-sizing-prediction
title: How BuildBuddy Predicts Resource Requirements Before Execution
description: We built a machine learning-based system that predicts CPU, memory, and disk requirements for build tasks, enabling smarter scheduling and preventing resource starvation.
authors: siggi
date: 2025-01-07:14:00:00
image: /img/blog/task-sizing-prediction.webp
tags: [engineering, performance, machine-learning, scheduling]
---

One of the hardest problems in distributed build execution is resource allocation. Underestimate a task's needs and it gets OOM-killed or runs painfully slow. Overestimate and you waste resources that could run other tasks. At BuildBuddy, we've built an intelligent task sizing system that learns from historical executions to predict resource requirements with remarkable accuracy. Here's how we turned millions of build executions into a self-improving prediction engine.

<!-- truncate -->

## The Resource Allocation Dilemma

When Bazel sends a build action to our remote execution service, it doesn't tell us how much CPU, memory, or disk space the action needs. A simple `gcc` compilation might need 100MB of RAM, while linking Chrome could require 32GB. Without this information, schedulers face an impossible choice:

1. **Conservative allocation**: Give every task lots of resources → massive waste, fewer concurrent tasks
2. **Aggressive packing**: Assume tasks are small → OOM kills, CPU starvation, failed builds
3. **User hints**: Require developers to annotate every action → maintenance nightmare, often wrong

We needed a fourth option: learn from experience.

## BuildBuddy Compute Units (BCUs)

Before diving into prediction, we needed a normalized way to measure resources. We introduced BuildBuddy Compute Units (BCUs), inspired by cloud providers' vCPU concepts:

```go
const (
    ComputeUnitsToMilliCPU = 1000      // 1 BCU = 1000 milli-CPU
    ComputeUnitsToRAMBytes = 2.5 * 1e9 // 1 BCU = 2.5GB of memory
)
```

This 1:2.5 CPU-to-memory ratio matches typical build workloads and simplifies resource math. A task needing 2 CPUs and 5GB RAM requires 2 BCUs.

## The Prediction Architecture

Our task sizing system has three key components:

### 1. Historical Data Collection

After every task execution, we collect detailed resource statistics:

```go
type TaskStats struct {
    ActionDigest    string
    CPUNanos        int64  // Total CPU time used
    PeakMemoryBytes int64  // Maximum RSS during execution
    DiskReadBytes   int64  // Total bytes read from disk
    DiskWriteBytes  int64  // Total bytes written to disk
    WallTimeNanos   int64  // Wall clock time
    PSIFullStallNanos struct {
        CPU    int64  // Time stalled waiting for CPU
        Memory int64  // Time stalled waiting for memory
        IO     int64  // Time stalled waiting for I/O
    }
}
```

The inclusion of PSI (Pressure Stall Information) metrics is crucial - they tell us when a task was resource-constrained, allowing us to correct our predictions.

### 2. PSI-Corrected CPU Estimation

Raw CPU usage doesn't tell the full story. If a task wanted 4 CPUs but only got 1, it'll show 1 CPU of usage over 4x the ideal duration. We use PSI data to reconstruct actual CPU demand:

```go
func correctCPUWithPSI(stats *TaskStats, psiCorrectionFactor float64) int64 {
    // Calculate effective execution time by removing stall time
    effectiveNanos := stats.WallTimeNanos -
        int64(psiCorrectionFactor * float64(stats.PSIFullStallNanos.CPU))

    // Derive actual CPU demand
    if effectiveNanos > 0 {
        return stats.CPUNanos / effectiveNanos
    }
    return stats.CPUNanos / stats.WallTimeNanos
}
```

This correction factor (default 1.0) can be tuned based on workload characteristics. It's the difference between "this task used 1 CPU" and "this task wanted 4 CPUs but was throttled to 1".

### 3. Statistical Aggregation with Redis

We store measurements in Redis with automatic expiration:

```go
func storeTaskMeasurement(ctx context.Context, digest string, stats *TaskStats) error {
    key := fmt.Sprintf("%s:%s", redisKeyPrefix, digest)

    measurement := &TaskMeasurement{
        MilliCPU:     correctCPUWithPSI(stats, *psiCorrectionFactor),
        MemoryBytes:  stats.PeakMemoryBytes,
        DiskBytes:    stats.DiskWriteBytes,
        Timestamp:    time.Now().Unix(),
    }

    // Store with expiration to prevent unbounded growth
    return redisClient.SetEX(ctx, key, measurement, sizeMeasurementExpiration)
}
```

We keep the last N measurements per action digest and compute percentiles for prediction.

## Smart Defaults and Minimums

Not every action has historical data. New code, first-time users, or modified build rules all need reasonable defaults:

```go
func getDefaultSize(task *repb.Task, props *platform.Properties) *TaskSize {
    size := &TaskSize{
        MemoryBytes: DefaultMemEstimate,    // 400 MB
        MilliCPU:    DefaultCPUEstimate,     // 600 milliCPU
        DiskBytes:   DefaultFreeDiskEstimate, // 100 MB
    }

    // Workflows need more resources
    if task.Type == TaskType_WORKFLOW {
        size.MemoryBytes = WorkflowMemEstimate // 8 GB
    }

    // Firecracker VMs have overhead
    if props.ContainerImage != "" {
        size.MemoryBytes += FirecrackerAdditionalMemEstimateBytes // +150 MB

        // Docker-in-Firecracker needs even more
        if props.DockerRuntime {
            size.MemoryBytes += DockerInFirecrackerAdditionalMemEstimateBytes // +800 MB
            size.DiskBytes += DockerInFirecrackerAdditionalDiskEstimateBytes   // +12 GB
        }
    }

    // Apply minimums to prevent overscheduling
    size.MemoryBytes = max(size.MemoryBytes, MinimumMemoryBytes) // 6 MB min
    size.MilliCPU = max(size.MilliCPU, MinimumMilliCPU)          // 250 milliCPU min

    return size
}
```

These defaults were derived from analyzing millions of executions across different action types.

## Test Size Hints

Bazel provides size hints for tests (small, medium, large, enormous). We map these to resource allocations:

```go
func getTestSize(env []string) (cpuMillis int64, memBytes int64) {
    for _, e := range env {
        if strings.HasPrefix(e, "TEST_SIZE=") {
            size := strings.TrimPrefix(e, "TEST_SIZE=")
            switch size {
            case "small":
                return 1000, 1 * 1e9  // 1 CPU, 1 GB
            case "medium":
                return 2000, 2 * 1e9  // 2 CPU, 2 GB
            case "large":
                return 4000, 4 * 1e9  // 4 CPU, 4 GB
            case "enormous":
                return 8000, 8 * 1e9  // 8 CPU, 8 GB
            }
        }
    }
    return 0, 0 // No hint
}
```

This provides a developer-controlled override while maintaining automatic sizing for most actions.

## Machine Learning Model Integration

For common action types, we've trained a lightweight model that predicts resource usage based on action features:

```go
type TaskSizeModel interface {
    Predict(ctx context.Context, features *Features) (*Prediction, error)
}

type Features struct {
    ActionType       string   // e.g., "CppCompile", "JavaCompile", "GoLink"
    InputSizeBytes   int64    // Total size of input files
    InputFileCount   int      // Number of input files
    OutputSizeHint   int64    // Expected output size (if known)
    CompilerFlags    []string // Relevant compiler flags
    PlatformHash     string   // Hash of platform properties
}

type Prediction struct {
    MilliCPU         int64
    MemoryBytes      int64
    ConfidenceScore  float64 // 0.0 to 1.0
}
```

The model runs asynchronously and is only used when confidence exceeds a threshold:

```go
func getPredictedSize(ctx context.Context, task *repb.Task) *TaskSize {
    if !*modelEnabled {
        return nil
    }

    features := extractFeatures(task)
    prediction, err := model.Predict(ctx, features)
    if err != nil || prediction.ConfidenceScore < 0.8 {
        return nil // Fall back to historical data
    }

    return &TaskSize{
        MilliCPU:    prediction.MilliCPU,
        MemoryBytes: prediction.MemoryBytes,
        Source:      "model",
    }
}
```

## Dynamic Limits and Safety Rails

Resource predictions must respect executor capacity and prevent abuse:

```go
func applyLimits(size *TaskSize, executorCapacity *Capacity) *TaskSize {
    // Cap at stored size limit to prevent Redis bloat
    size.MilliCPU = min(size.MilliCPU, *milliCPULimit) // 7.5 CPUs default

    // Respect executor capacity
    maxCPU := int64(executorCapacity.MilliCPU * MaxResourceCapacityRatio)
    maxMem := int64(executorCapacity.MemoryBytes * MaxResourceCapacityRatio)

    size.MilliCPU = min(size.MilliCPU, maxCPU)
    size.MemoryBytes = min(size.MemoryBytes, maxMem)

    // Disk limits vary by configuration
    maxDisk := MaxEstimatedFreeDisk // 100 GB
    if !task.Recyclable {
        maxDisk = MaxEstimatedFreeDiskRecycleFalse // 200 GB
    }
    size.DiskBytes = min(size.DiskBytes, maxDisk)

    return size
}
```

## The Feedback Loop

The beauty of this system is its self-improving nature:

1. **Initial execution**: Use defaults or model prediction
2. **Measurement**: Collect actual resource usage
3. **Storage**: Save measurements in Redis
4. **Next execution**: Use historical data for better prediction
5. **Continuous improvement**: Each execution refines the estimate

Here's a real example showing convergence over time:

```
Action: //src/server:compile_main.go
Execution 1: Predicted: 600m CPU, 400MB RAM | Actual: 1823m CPU, 892MB RAM
Execution 2: Predicted: 1823m CPU, 892MB RAM | Actual: 1798m CPU, 887MB RAM
Execution 3: Predicted: 1810m CPU, 889MB RAM | Actual: 1812m CPU, 891MB RAM
Execution 4: Predicted: 1811m CPU, 890MB RAM | Actual: 1809m CPU, 888MB RAM
```

Within 3-4 executions, predictions converge to within 1% of actual usage.

## Production Impact

Since deploying intelligent task sizing, we've seen dramatic improvements:

- **OOM kills reduced by 94%**: Tasks get the memory they need
- **CPU starvation down 87%**: Better CPU allocation prevents throttling
- **Executor utilization up 31%**: More accurate sizing allows better packing
- **P99 build time improved 23%**: Less resource contention means faster builds

## Handling Edge Cases

The system handles several tricky scenarios:

### Memory Spikes

Some tasks have variable memory usage. We use P95 instead of mean to handle spikes:

```go
func aggregateMemoryMeasurements(measurements []int64) int64 {
    sort.Slice(measurements, func(i, j int) bool {
        return measurements[i] < measurements[j]
    })

    // Use P95 to handle occasional spikes
    p95Index := int(float64(len(measurements)) * 0.95)
    return measurements[p95Index]
}
```

### Platform Variations

The same action might need different resources on different platforms:

```go
func getTaskSizeKey(digest string, platform *Platform) string {
    // Include platform properties in cache key
    h := sha256.New()
    h.Write([]byte(digest))
    h.Write([]byte(platform.OSFamily))
    h.Write([]byte(platform.Arch))
    h.Write([]byte(platform.ContainerImage))
    return fmt.Sprintf("%s:%x", redisKeyPrefix, h.Sum(nil))
}
```

### Cold Starts

First-time actions use a combination of model predictions and conservative defaults:

```go
func getTaskSize(ctx context.Context, task *repb.Task) *TaskSize {
    // Try historical data first
    if size := getHistoricalSize(ctx, task); size != nil {
        return size
    }

    // Try model prediction
    if size := getPredictedSize(ctx, task); size != nil {
        return size
    }

    // Fall back to smart defaults
    return getDefaultSize(task)
}
```

## Future Directions

We're exploring several enhancements:

1. **Cross-action learning**: Use similar actions to predict new ones
2. **Time-series analysis**: Detect trends in resource usage over time
3. **Automated alerts**: Notify when actions suddenly need more resources
4. **Cost optimization**: Balance performance vs. cost based on priority

## Lessons Learned

Building this system taught us valuable lessons:

1. **PSI metrics are crucial**: CPU usage alone is misleading under contention
2. **Percentiles beat averages**: P95 handles variability better than mean
3. **Defaults matter**: Good defaults prevent catastrophic failures
4. **Feedback loops work**: Systems that learn from experience improve rapidly
5. **Safety rails are essential**: Always enforce maximum limits

## Conclusion

Intelligent task sizing transforms remote execution from a resource guessing game into a predictable, efficient system. By learning from every execution, we've created a self-tuning system that gets smarter with every build. The combination of historical data, machine learning, and smart defaults provides robust predictions that handle both common and edge cases.

This approach - turning operational data into operational intelligence - exemplifies how modern distributed systems can self-optimize. Instead of requiring manual tuning or static configuration, the system continuously adapts to changing workloads and improves over time.

If you're interested in trying BuildBuddy's remote execution with intelligent task sizing, check out our [documentation](https://www.buildbuddy.io/docs/remote-execution/). And if building self-improving distributed systems excites you, [we're hiring](https://www.buildbuddy.io/careers/)!
