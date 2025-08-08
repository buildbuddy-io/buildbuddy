---
slug: streaming-vms-with-userfaultfd
title: How We Stream Firecracker VMs Using Linux's Userfaultfd
description: We're excited to share how BuildBuddy achieves near-instant VM startup by implementing distributed virtual memory at the hypervisor level using Linux's userfaultfd.
authors: siggi
date: 2025-01-07:10:00:00
image: /img/blog/streaming-vms.webp
tags: [engineering, performance, virtualization, firecracker]
---

At BuildBuddy, we run thousands of Firecracker microVMs every day to provide secure, isolated execution environments for remote builds. One of our biggest challenges was VM startup time - loading multi-gigabyte memory snapshots could take 30+ seconds, making autoscaling painful and adding significant latency to builds. Today, we're sharing how we solved this with a technique we call "streaming VMs" - using Linux's userfaultfd to implement distributed virtual memory at the hypervisor level, achieving near-instant VM startup regardless of snapshot size.

<!-- truncate -->

## The Problem: Slow VM Startup from Snapshots

BuildBuddy's remote execution service runs customer code in isolated environments. For maximum security and isolation, we use [Firecracker](https://firecracker-microvm.github.io/) microVMs - lightweight virtual machines designed for serverless workloads. To speed up VM startup, we create snapshots of pre-booted VMs with common build environments already loaded.

The challenge? These snapshots can be gigabytes in size. A typical development environment with language runtimes, compilers, and tools can easily have 2-4GB of memory state. Loading this entire snapshot from disk or network storage before starting the VM meant 30+ second startup times - unacceptable for a build system where every second counts.

We needed a way to start VMs instantly while still providing the full snapshot state when needed. The solution came from an unlikely source: Linux's userfaultfd mechanism.

## Enter Userfaultfd: Page Faults as a Feature

[Userfaultfd](https://www.kernel.org/doc/html/latest/admin-guide/mm/userfaultfd.html) is a Linux kernel feature that allows a userspace process to handle page faults for a memory region. Originally designed for live migration and garbage collectors, it provides a powerful primitive: the ability to intercept memory accesses and provide the data on-demand.

Here's the key insight: when a VM starts from a snapshot, it doesn't need all its memory immediately. A build action might only touch 10% of the snapshot's memory pages. Why load the other 90% upfront?

With userfaultfd, we can:

1. Register the VM's memory region with userfaultfd
2. Start the VM immediately with no memory loaded
3. When the VM accesses a page, receive a page fault notification
4. Load that specific page from the snapshot and provide it to the VM
5. The VM continues running, unaware of the behind-the-scenes magic

## The Architecture: Distributed Virtual Memory

Our implementation goes beyond basic lazy loading. We've built what's essentially a distributed virtual memory system at the hypervisor level:

### Memory Chunking and Compression

Instead of storing snapshots as monolithic files, we break them into chunks (typically 2MB each) and compress them with zstd. This provides several benefits:

- **Better cache utilization**: Individual chunks can be cached independently
- **Parallel fetching**: Multiple chunks can be downloaded simultaneously
- **Deduplication**: Common chunks across snapshots are stored only once
- **Compression**: 2-3x reduction in storage and network transfer

### Remote Streaming

The real magic happens when we combine userfaultfd with our distributed cache:

```go
type SnapLoader struct {
    uffd        int                     // userfaultfd file descriptor
    memoryFile  *os.File                // VM memory backing file
    chunkCache  interfaces.RemoteCache  // Distributed cache client
    dirtyPages  *bitmap.Bitmap          // Track modified pages
}

func (s *SnapLoader) handlePageFault(fault *uffdapi.PagefaultMsg) error {
    // Calculate which chunk contains this page
    chunkID := fault.Address / ChunkSize

    // Check local cache first
    if chunk := s.localCache.Get(chunkID); chunk != nil {
        return s.providePage(fault.Address, chunk)
    }

    // Fetch from remote cache
    chunk, err := s.chunkCache.Get(ctx, chunkDigest(chunkID))
    if err != nil {
        return err
    }

    // Decompress and provide to VM
    decompressed := zstd.Decompress(chunk)
    return s.providePage(fault.Address, decompressed)
}
```

This architecture means VMs can start instantly and stream memory pages from our distributed cache as needed. The first access to a memory region triggers a network fetch, but subsequent accesses are served from local memory at native speed.

### Incremental Snapshots

We also track which pages have been modified (dirtied) during VM execution. This enables incremental snapshots - we only need to save the changed pages, dramatically reducing snapshot size and creation time:

```go
func (s *SnapLoader) createIncrementalSnapshot() (*Snapshot, error) {
    snapshot := &Snapshot{
        BaseSnapshot: s.baseSnapshotID,
        DirtyChunks:  make(map[int64][]byte),
    }

    // Only process dirty pages
    for chunkID := range s.dirtyPages.Iterate() {
        chunk := s.readChunk(chunkID)
        compressed := zstd.Compress(chunk)
        digest := s.chunkCache.Put(compressed)
        snapshot.DirtyChunks[chunkID] = digest
    }

    return snapshot, nil
}
```

## Memory Ballooning: Dynamic Resource Management

Beyond lazy loading, we've implemented memory ballooning - a technique where the VM can dynamically release memory back to the host. This is crucial for efficient resource utilization in a multi-tenant environment.

The balloon driver inside the VM communicates with our snaploader via a virtio device. When memory pressure is low, we can inflate the balloon, forcing the VM to release pages. When the VM needs more memory, we deflate the balloon. Combined with userfaultfd, this creates a highly dynamic memory management system:

1. VM starts with minimal memory footprint
2. Pages are streamed in as needed via userfaultfd
3. Unused pages can be reclaimed via ballooning
4. Reclaimed pages can be evicted from host memory
5. If accessed again, they're re-fetched from cache

This allows us to oversubscribe memory significantly - running more VMs than would fit if all were fully loaded.

## Performance Results

The results speak for themselves:

- **VM startup time**: Reduced from 30+ seconds to under 1 second
- **Time to first action**: 95th percentile under 2 seconds (was 35+ seconds)
- **Memory efficiency**: 3x improvement in VMs per host
- **Cache hit rate**: 98%+ for frequently used snapshots
- **Network bandwidth**: 80% reduction due to compression and deduplication

Here's a real trace from production showing the difference:

```
# Traditional snapshot loading
[0.000s] Starting VM load
[0.012s] Opening snapshot file (2.1GB)
[0.234s] Reading snapshot into memory
[28.421s] Snapshot loaded
[28.445s] VM started
[28.623s] First instruction executed

# With streaming VMs
[0.000s] Starting VM load
[0.008s] Registering userfaultfd
[0.021s] VM started
[0.089s] First page fault handled
[0.234s] First instruction executed
[1.823s] Working set loaded (210MB of 2.1GB)
```

## Edge Cases and Lessons Learned

Building distributed virtual memory isn't without challenges. Here are some of the interesting problems we've solved:

### Thundering Herd

When a popular snapshot is updated, thousands of VMs might try to fetch the same chunks simultaneously. We use request coalescing (via our SingleFlight implementation) to ensure only one fetch happens per chunk per host.

### Network Failures

What happens if the network fails mid-execution? We implement exponential backoff with jitter for retries, and maintain a local LRU cache of recently used chunks. Critical chunks (like kernel memory) are pre-fetched to ensure VMs can continue running during temporary network issues.

### Page Fault Storms

Some operations (like large memory copies) can trigger thousands of page faults in rapid succession. We batch adjacent faults and pre-fetch neighboring pages to reduce overhead:

```go
func (s *SnapLoader) handlePageFaultBatch(faults []*uffdapi.PagefaultMsg) {
    // Group faults by chunk
    chunkFaults := make(map[int64][]*uffdapi.PagefaultMsg)
    for _, fault := range faults {
        chunkID := fault.Address / ChunkSize
        chunkFaults[chunkID] = append(chunkFaults[chunkID], fault)
    }

    // Fetch chunks in parallel
    var wg sync.WaitGroup
    for chunkID, faults := range chunkFaults {
        wg.Add(1)
        go func(id int64, f []*uffdapi.PagefaultMsg) {
            defer wg.Done()
            s.fetchAndProvideChunk(id, f)
        }(chunkID, faults)
    }
    wg.Wait()
}
```

### Memory Pressure

Under memory pressure, we need to evict cached chunks. We use a two-level eviction strategy:

1. First, evict clean chunks (unmodified snapshot pages)
2. Then, evict dirty chunks after writing them back to cache

## Implementation Details

For those interested in the low-level details, here's how we set up userfaultfd:

```go
func setupUserfaultfd(memoryFile *os.File, size int64) (int, error) {
    // Create userfaultfd
    uffd, err := syscall.Syscall(syscall.SYS_USERFAULTFD,
        syscall.O_CLOEXEC|syscall.O_NONBLOCK, 0, 0)
    if err != 0 {
        return -1, err
    }

    // Enable required features
    uffdAPI := uffdapi.API{
        API:      UFFD_API_VERSION,
        Features: UFFD_FEATURE_MISSING_SHMEM |
                 UFFD_FEATURE_PAGEFAULT_FLAG_WP,
    }
    if err := ioctl(uffd, UFFDIO_API, &uffdAPI); err != nil {
        return -1, err
    }

    // Register memory range
    registerRange := uffdapi.RegisterRange{
        Range: uffdapi.Range{
            Start: 0,
            Len:   uint64(size),
        },
        Mode: UFFDIO_REGISTER_MODE_MISSING |
              UFFDIO_REGISTER_MODE_WP,
    }
    if err := ioctl(uffd, UFFDIO_REGISTER, &registerRange); err != nil {
        return -1, err
    }

    return int(uffd), nil
}
```

The page fault handler runs in a separate goroutine, continuously reading fault events and handling them:

```go
func (s *SnapLoader) faultHandler() {
    buf := make([]byte, unsafe.Sizeof(uffdapi.PagefaultMsg{}))

    for {
        n, err := syscall.Read(s.uffd, buf)
        if err != nil {
            continue
        }

        msg := (*uffdapi.Msg)(unsafe.Pointer(&buf[0]))
        if msg.Event == UFFD_EVENT_PAGEFAULT {
            fault := (*uffdapi.PagefaultMsg)(unsafe.Pointer(&msg.Pagefault))
            s.handlePageFault(fault)
        }
    }
}
```

## Beyond VMs: Future Applications

While we've focused on Firecracker VMs, this technique has broader applications:

- **Container checkpointing**: Stream CRIU checkpoints for instant container restore
- **Large model serving**: Stream ML model weights on-demand
- **Database snapshots**: Instant database restoration without full snapshot loads
- **Development environments**: Stream pre-configured development environments

We're exploring several of these directions and are excited about the possibilities.

## Conclusion

By combining userfaultfd with distributed caching and smart prefetching, we've transformed VM startup from a 30+ second operation to sub-second. This isn't just a performance optimization - it fundamentally changes what's possible with VM-based isolation. We can now spin up thousands of VMs on-demand, provide instant access to massive pre-configured environments, and do it all with excellent resource efficiency.

The technique of using page faults as a feature rather than a problem opens up new possibilities for systems design. Sometimes the best solution isn't to avoid the "slow" path, but to make the slow path fast enough that it doesn't matter.

If you're interested in trying BuildBuddy's remote execution with streaming VMs, check out our [documentation](https://www.buildbuddy.io/docs/remote-execution/). And if solving problems like this excites you, [we're hiring](https://www.buildbuddy.io/careers/)!

## References

- [Userfaultfd Documentation](https://www.kernel.org/doc/html/latest/admin-guide/mm/userfaultfd.html)
- [Firecracker Snapshots](https://github.com/firecracker-microvm/firecracker/blob/main/docs/snapshotting/snapshot-support.md)
- [BuildBuddy Remote Execution](https://www.buildbuddy.io/docs/remote-execution/)
- [Our Modified SOCI Snapshotter](https://github.com/buildbuddy-io/soci-snapshotter)
