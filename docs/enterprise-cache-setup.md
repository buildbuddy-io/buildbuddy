---
id: enterprise-cache-setup
title: Enterprise cache setup
sidebar_label: Enterprise Cache
---

This guide covers the cache backend and related UI features for a self-hosted BuildBuddy Enterprise deployment. Start with the [Enterprise setup guide](enterprise-setup.md) if you have not installed BuildBuddy yet.

The examples below are additions to your existing configuration. For the [Enterprise Helm chart](enterprise-helm.md), place server settings under `config:` in your Helm values. For other deployments, place them directly in `config.yaml`. Merge the examples into the existing sections rather than adding duplicate YAML keys.

## Use Pebble on persistent SSD storage

We recommend Pebble for the cache backend. It stores cache metadata and small artifacts in a local key-value database, with larger artifacts stored as files. (The Enterprise Helm chart enables Pebble by default as of chart version [0.0.301](https://github.com/buildbuddy-io/buildbuddy-helm/releases/tag/buildbuddy-enterprise-0.0.301).)

For a deployment configured directly through `config.yaml`, use:

```yaml title="config.yaml"
cache:
  max_size_bytes: 100000000000 # Example capacity of 100 GB per app.
  pebble:
    root_directory: /data/buildbuddy/pebble-cache/
```

For best performance, mount persistent SSD storage at the configured path. Give
each app replica its own cache directory and volume. Start with
`cache.max_size_bytes` set to about **75% of the disk capacity**, leaving roughly
25% for database compaction, metadata, and temporary disk usage. Use a lower
percentage if build event storage or other data shares the volume, and adjust
based on observed peak disk usage. The size is per replica, so account for
replication when estimating the cluster's usable cache capacity.

Changing the backend or root directory does not migrate existing cached artifacts. For an existing installation, plan the transition with [BuildBuddy support](mailto:setup@buildbuddy.io).

## Configure distributed caching

Use distributed caching when running multiple app replicas so requests can reach the replica holding an artifact. Each app runs its own Pebble cache, and the distributed cache routes requests between them.

### Helm example

The following values configure a new cache with three app replicas, two copies of each artifact, and Kubernetes peer discovery. They also allocate a persistent volume for each app. Replace `your-ssd-storage-class` with an SSD StorageClass available in your cluster. For an existing cache, review the [consistent hashing guidance](#consistent-hashing) before changing its hash settings.

```yaml title="values.yaml"
replicas: 3
distributed:
  enabled: true
  size: 200Gi
  storageClass: your-ssd-storage-class
rbac:
  create: true
serviceAccount:
  create: true

config:
  cache:
    max_size_bytes: 100000000000
    distributed_cache:
      listen_addr: "0.0.0.0:5151"
      kubernetes_discovery: true
      cluster_size: 3
      replication_factor: 2
      consistent_hash_function: SHA256
      consistent_hash_vnodes: 10000
```

This uses the chart's existing Pebble configuration and requires a chart version with `rbac.create` and `serviceAccount.create` support. Allow app pods to reach each other on the distributed cache port, and configure shared SQL, Redis, and build event storage for the app replicas as described in [Enterprise configuration](enterprise-config.md).

`replicas` controls the number of app pods; `replication_factor` controls how many copies of each artifact are stored. Keep `cluster_size` consistent with your app replica count and at least as large as the replication factor. More copies consume additional disk space and write bandwidth.

### Consistent hashing

<!-- TODO(buildbuddy): find a non-disruptive strategy to make these flags the default. -->

We recommend setting `cache.distributed_cache.consistent_hash_function` to `SHA256` and `cache.distributed_cache.consistent_hash_vnodes` to `10000`, as shown in the Helm example. These settings distribute keys more evenly across replicas.

**Choose these settings before populating a new cache.** Changing either setting on an existing cache changes which replicas own its keys. This is a disruptive change that can cause cache misses, and replicas using different settings can disagree about where to read and write data. Coordinate an existing cluster's migration with BuildBuddy support.

## Reduce repeated cache lookups

The following settings trade memory and less frequent access-time updates for fewer database operations. Apply them with enough memory headroom, and compare cache latency, disk activity, and hit rates before and after the change.

### Access times and the presence cache

Pebble records when an artifact was last accessed so it can evict older artifacts first. Increasing `cache.pebble.atime_update_threshold` from its default of `10m` to `3h` reduces metadata writes for frequently accessed artifacts. Keep this threshold well below the shortest expected retention time for artifacts in the Pebble cache. A longer threshold can leave recently accessed artifacts looking old enough to evict.

The presence cache remembers which digests exist, avoiding repeated Pebble lookups for `FindMissingBlobs` requests. We recommend enabling it when these requests contribute significant cache load. For a cache with enough memory, start with the following configuration:

```yaml title="config.yaml"
cache:
  pebble:
    atime_update_threshold: 3h
    presence_cache:
      max_entries: 2000000
      ttl: 1h
```

The presence cache is disabled by default. At approximately 190 bytes per entry, two million entries need roughly 380 MB of memory per app. Size it alongside the Pebble block cache, other in-memory caches, and the app's remaining memory needs.

Keep the presence-cache TTL positive and well below the access-time update threshold. A presence-cache hit skips the underlying lookup and its access-time update; expiring the presence entry periodically allows the next request to refresh that information. If you use a shorter access-time threshold, shorten the presence-cache TTL accordingly.

### Small-object lookaside cache

For a distributed cache, a lookaside cache keeps recently read small objects in memory on the app handling the request. This can avoid both a peer request and a Pebble lookup for a repeated read.

We recommend trying a 1 GB cache with a 15-minute TTL, then measuring its hit rate:

```yaml title="config.yaml"
cache:
  distributed_cache:
    lookaside_cache_size_bytes: 1000000000
    lookaside_cache_ttl: 15m
```

The lookaside cache is disabled by default. This example adds up to approximately 1 GB per app, plus bookkeeping overhead, and assumes the `3h` Pebble access-time threshold above. Keep the lookaside TTL well below that threshold so repeated reads eventually reach the backing cache and refresh access times. Use `buildbuddy_remote_cache_lookaside_cache_lookup_count`, grouped by its `status` label, to compare hits and misses before increasing the size.

## Tune Pebble using measurements

If the cache is performing well, additional tuning may not help. Use [Prometheus metrics](prometheus-metrics-on-prem.mdx) to identify pressure on disk reads, memory, and file handles before changing these settings.

### Block cache size

`cache.pebble.block_cache_size_bytes` controls how much memory Pebble can use to retain database blocks and avoid disk reads. Its default is 1 GB. Increase it when block-cache misses contribute to disk load and the app has memory available.

Track hits and misses with `buildbuddy_remote_cache_pebble_cache_pebble_block_cache_requests_count`, using the `cache_status` label. For example, an app with sufficient memory could try:

```yaml title="config.yaml"
cache:
  pebble:
    block_cache_size_bytes: 4000000000 # 4 GB per app.
```

Measure the resulting hit rate and disk reads. Account for this allocation together with the presence cache, lookaside cache, app memory, and filesystem page cache when sizing the pod or machine.

## Memory usage

Each app replica uses memory to serve requests and retain frequently accessed cache data. The Pebble block cache, presence cache, and lookaside cache each have their own size settings, but all share the app's available memory. Tune these values together, leaving room for the rest of the app and filesystem page cache. The `cache.max_size_bytes` setting controls Pebble's disk capacity and does not limit memory use.

The percentages below are approximate starting points for dividing each app replica's memory budget. Adjust them based on cache hit rates and memory use under representative load.

### Memory constraints

- **`resources.limits.memory` (k8s)** sets the container memory limit. Size it to accommodate the combined cache allocations and measured peak memory use of the rest of the app, with headroom for traffic spikes. This limit covers Go memory, native allocations, and filesystem page cache. Exceeding it can cause the container to be killed.
- **`resources.requests.memory` (k8s)** reserves memory for scheduling. Set it to the planned memory budget per app replica so the scheduler accounts for that capacity. It does not set a limit on memory use.
- **Memory headroom** is the portion of the app's memory budget left available after accounting for the configured caches. Reserve it for bookkeeping, request and compression buffers, Pebble write buffers and compactions, filesystem page cache, and other app activity. Go also needs space for allocations awaiting garbage collection. Measure container memory under representative load, including upload and download bursts, to determine how much headroom your workload needs. Go heap metrics alone omit native allocations and filesystem cache.
- **Filesystem page cache** is memory Linux uses to retain file contents and avoid repeated disk reads. When larger artifacts are stored as separate files on local disk, their contents benefit from this cache independently of the Pebble block cache. This benefit does not apply to artifact contents read directly from GCS or another remote blobstore. Linux manages the page cache automatically. Leave memory available for it after accounting for app allocations, and adjust that headroom using disk-read and download-latency measurements under representative load. See the [Linux page-cache documentation](https://docs.kernel.org/admin-guide/mm/concepts.html#page-cache).

### Memory settings

- **`cache.pebble.block_cache_size_bytes`** budgets memory for cached database blocks. Start with around **25% of the app memory budget**. Official BuildBuddy Enterprise images allocate these blocks outside Go's managed heap. Allow additional memory for bookkeeping and other Pebble allocations.
- **`cache.pebble.presence_cache.max_entries`** limits the number of digests remembered in Go memory. Start by budgeting around **2% of the app memory budget**. Divide that byte budget by approximately **190 bytes per entry** to estimate the entry limit. `cache.pebble.presence_cache.ttl` controls how long entries remain, but enough distinct requests can still fill the cache to its configured capacity.
- **`cache.distributed_cache.lookaside_cache_size_bytes`** budgets Go memory for small objects. Start with around **5% of the app memory budget**, plus overhead for keys, maps, and other bookkeeping. `cache.distributed_cache.lookaside_cache_ttl` controls how long objects remain, but enough distinct requests can still fill the cache to its configured capacity.

## Additional tuning options

Consider these settings only when measurements identify a specific performance or resource problem.

### Open SST files

Pebble stores its tables in sorted string table (SST) files. `cache.pebble.max_open_files` limits how many SST files it keeps open, with a default of `4000`. If the database has close to or more than that many files, increasing the limit can reduce repeated file opens.

Sum the file counts across all levels for each app and cache:

```promql
sum by (pod_name, cache_name) (
  buildbuddy_remote_cache_pebble_cache_pebble_level_num_files
)
```

Use your scrape configuration's pod or instance label if it differs from `pod_name`. Choose a limit above the observed file count with room for growth, and keep it below the process's open-file limit, leaving room for sockets and other files. For example, a database with around 10,000 SST files could use:

```yaml title="config.yaml"
cache:
  pebble:
    max_open_files: 32768
```

### Go memory controls

- **`GOMEMLIMIT`** sets a soft limit for memory managed by Go. Most deployments should leave it unchanged and tune cache sizes and the app's memory budget first. It includes the lookaside and presence caches, but excludes Pebble's native block allocations in the official Enterprise app images. If profiling shows a need to configure it, leave room below the container limit for those allocations, filesystem cache, and traffic spikes. Set it through `extraEnvVars` in Helm. It can be exceeded, so it does not guarantee that the container stays below its limit. See the [Go memory-limit guide](https://go.dev/doc/gc-guide#Memory_limit).
- **`GOGC`** controls how much the Go heap can grow between garbage collections. Most deployments should leave it unchanged. Raising it generally trades more memory for less garbage-collection CPU; lowering it does the reverse. Adjust it only when profiling identifies a garbage-collection problem and after measuring memory headroom. Set it through `extraEnvVars` in Helm.

## Enable cache-related UI features

### Cache request details and directory sizes

We recommend enabling both of these features to help users investigate cache behavior:

```yaml title="config.yaml"
cache:
  detailed_stats_enabled: true
  directory_sizes_enabled: true
```

Both settings default to `false`.

- `detailed_stats_enabled` populates the invocation's Cache tab with individual hits, misses, uploads, transfer sizes, and durations. Bazel must send its build events to this BuildBuddy deployment so requests can be associated with an invocation. For multiple app replicas, configure shared Redis via `app.default_redis_target` so requests handled by different replicas contribute to the same invocation. Detailed stats add metrics-collection and storage overhead.
- `directory_sizes_enabled` shows cumulative directory sizes in the action's input tree, helping users find large inputs. The action and input tree must be available in the cache; enabling the feature does not upload them. Computing sizes reads the directory tree from the cache when the action is inspected.

## Verify the setup

Run a representative build that populates the cache, then repeat it from a clean output base or another machine so the local build state does not hide remote cache activity. Confirm that the second build gets remote cache hits and that the Cache tab displays request details.

Monitor request latency, cache hit rate, disk utilization, eviction age, memory, and peer traffic under normal load. Compare block-cache and lookaside-cache hit rates after sizing changes.

## More configuration

For more configuration options beyond caching, such as authentication and storage, see our [configuration docs](config.md) and our [enterprise configuration guide](enterprise-config.md). See [all configuration options](config-all-options.mdx) for the complete flag reference.
