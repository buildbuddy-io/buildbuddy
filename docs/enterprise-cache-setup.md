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

- For best performance, mount persistent SSD storage at the configured path.
- Give each app replica its own cache directory and volume.
- Start with `cache.max_size_bytes` set to about **75% of the disk capacity**, leaving roughly 25% for database compaction, metadata, and temporary disk usage. Use a lower percentage if build event storage or other data shares the volume, and adjust based on observed peak disk usage.
- `cache.max_size_bytes` applies to each replica, so account for replication when estimating the cluster's usable cache capacity.

Changing the backend or root directory does not migrate existing cached artifacts. For an existing installation, plan the transition with [BuildBuddy support](mailto:setup@buildbuddy.io).

## Configure distributed caching

Use distributed caching when running multiple app replicas so requests can reach the replica holding an artifact. Each app runs its own Pebble cache, and the distributed cache routes requests between them.

### Helm example

The following values configure a new cache with three app replicas, two copies of each artifact, and Kubernetes peer discovery. They also allocate a persistent volume for each app. Replace `your-ssd-storage-class` with an SSD StorageClass available in your cluster.

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
    max_size_bytes: 160000000000 # About 75% of the 200Gi volume.
    distributed_cache:
      listen_addr: "0.0.0.0:5151"
      kubernetes_discovery: true
      cluster_size: 3
      replication_factor: 2
```

This uses the chart's existing Pebble configuration and requires a chart version with `rbac.create` and `serviceAccount.create` support. Allow app pods to reach each other on the distributed cache port, and configure shared SQL, Redis, and build event storage for the app replicas as described in [Enterprise configuration](enterprise-config.md).

`replicas` controls the number of app pods; `replication_factor` controls how many copies of each artifact are stored. Keep `cluster_size` consistent with your app replica count and at least as large as the replication factor. More copies consume additional disk space and write bandwidth.

Use a replication factor of at least 2 so that artifacts stay available while an app restarts, such as during a rolling update, since another replica still holds a copy of each artifact. With a replication factor of 1, artifacts stored on a restarting app are unavailable until it returns, which causes cache misses. More generally, restart fewer apps at a time than the replication factor.

## Tune memory usage

Each app replica uses memory to serve requests and retain frequently accessed cache data. The Pebble block cache, presence cache, and lookaside cache each have their own size settings, but all share the app's available memory. Tune these values together, leaving room for the rest of the app and filesystem page cache. The `cache.max_size_bytes` setting controls Pebble's disk capacity and does not limit memory use.

### Memory constraints

- **`resources.limits.memory` (k8s)** sets the container memory limit. Size it to accommodate the combined cache allocations and measured peak memory use of the rest of the app, with headroom for traffic spikes. This limit covers Go memory, native allocations, and filesystem page cache. Exceeding it can cause the container to be killed.
- **`resources.requests.memory` (k8s)** reserves memory for scheduling. Set it to the planned memory budget per app replica so the scheduler accounts for that capacity. It does not set a limit on memory use.
- **Memory headroom** is the portion of the app's memory budget left available after accounting for the configured caches. Reserve it for bookkeeping, request and compression buffers, Pebble write buffers and compactions, filesystem page cache, and other app activity. Go also needs space for allocations awaiting garbage collection. Measure container memory under representative load, including upload and download bursts, to determine how much headroom your workload needs. Go heap metrics alone omit native allocations and filesystem cache.
- **Filesystem page cache** is memory Linux uses to retain file contents and avoid repeated disk reads. When larger artifacts are stored as separate files on local disk, their contents benefit from this cache independently of the Pebble block cache. This benefit does not apply to artifact contents read directly from GCS or another remote blobstore. Linux manages the page cache automatically. Leave memory available for it after accounting for app allocations, and adjust that headroom using disk-read and download-latency measurements under representative load. See the [Linux page-cache documentation](https://docs.kernel.org/admin-guide/mm/concepts.html#page-cache).

### Memory settings

The percentages below are approximate starting points for dividing each app replica's memory budget. Adjust them based on cache hit rates and memory use under representative load.

- **`cache.pebble.block_cache_size_bytes`** budgets memory for cached database blocks. Start with around **25% of the app memory budget**. Official BuildBuddy Enterprise images allocate these blocks outside Go's managed heap. Allow additional memory for bookkeeping and other Pebble allocations.
- **`cache.pebble.presence_cache.max_entries`** limits the number of digests remembered in Go memory. Start by budgeting around **2% of the app memory budget**. Divide that byte budget by approximately **190 bytes per entry** to estimate the entry limit. `cache.pebble.presence_cache.ttl` controls how long entries remain, but enough distinct requests can still fill the cache to its configured capacity.
- **`cache.distributed_cache.lookaside_cache_size_bytes`** budgets Go memory for small objects. Start with around **5% of the app memory budget**, plus overhead for keys, maps, and other bookkeeping. `cache.distributed_cache.lookaside_cache_ttl` controls how long objects remain, but enough distinct requests can still fill the cache to its configured capacity.

## Reduce repeated cache lookups

The following mechanisms trade memory and less frequent access-time updates for fewer database operations. They are enabled by default. If you adjust them, compare cache latency, disk activity, and hit rates before and after the change.

### Access times and the presence cache

Pebble records when an artifact was last accessed so it can evict older artifacts first, and it does not evict artifacts accessed within `cache.pebble.min_eviction_age` (default `6h`). To reduce metadata writes, Pebble skips an access-time update if the recorded access time is newer than `cache.pebble.atime_update_threshold`. The threshold defaults to half of the minimum eviction age, which keeps artifacts read at least once per minimum eviction age from being evicted. If you set the threshold explicitly, keep it at or below half of the minimum eviction age.

The presence cache remembers which digests exist, avoiding repeated Pebble lookups for `FindMissingBlobs` requests. By default, it holds up to 1,000,000 entries (`cache.pebble.presence_cache.max_entries`) for 1 minute each (`cache.pebble.presence_cache.ttl`). At approximately 190 bytes per entry, the default size needs roughly 190 MB of memory per app.

Keep the presence-cache TTL positive and well below the access-time update threshold. A presence-cache hit skips the underlying lookup and its access-time update; expiring the presence entry periodically allows the next request to refresh that information. If you use a shorter access-time threshold, shorten the presence-cache TTL accordingly.

### Small-object lookaside cache

For a distributed cache, a lookaside cache keeps recently read small objects in memory on the app handling the request. This can avoid both a peer request and a Pebble lookup for a repeated read.

By default, the lookaside cache holds up to 1 GB per app (`cache.distributed_cache.lookaside_cache_size_bytes`) and serves each object for up to 1 minute (`cache.distributed_cache.lookaside_cache_ttl`). Keep the lookaside TTL well below the Pebble access-time threshold so repeated reads eventually reach the backing cache and refresh access times. Use `buildbuddy_remote_cache_lookaside_cache_lookup_count`, grouped by its `status` label, to compare hits and misses before increasing the size.

## Tune Pebble

### Block cache size

`cache.pebble.block_cache_size_bytes` controls how much memory Pebble can use to retain database blocks and avoid disk reads. Its default is 1 GB. Increase it when block-cache misses contribute to disk load and the app has memory available.

Track hits and misses with `buildbuddy_remote_cache_pebble_cache_pebble_block_cache_requests_count`, using the `cache_status` label. A hit rate of around 90% is reasonable. If the hit rate is well below that and the app has memory to spare, try increasing the block cache size:

```yaml title="config.yaml"
cache:
  pebble:
    block_cache_size_bytes: 4000000000 # 4 GB per app.
```

Measure the resulting hit rate and disk reads. Account for this allocation together with the presence cache, lookaside cache, app memory, and filesystem page cache when sizing the pod or machine.

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

## Enable detailed cache stats

By default, BuildBuddy records action cache misses for each invocation and lists up to 1,000 of them in the invocation's Cache tab. To capture more detailed metadata, including hits, misses, uploads, transfer sizes, and durations, enable `cache.detailed_stats_enabled`. Detailed stats add metrics-collection and storage overhead.

Both the default miss list and detailed stats require Bazel to send its build events to this BuildBuddy deployment so requests can be associated with an invocation. For multiple app replicas, configure shared Redis via `app.default_redis_target` so requests handled by different replicas contribute to the same invocation.

To enable detailed stats, add the following configuration:

```yaml title="config.yaml"
cache:
  detailed_stats_enabled: true
```

## Verify the setup

Run a representative build that populates the cache, then repeat it from a clean output base or another machine so the local build state does not hide remote cache activity. Confirm that the second build gets remote cache hits. If you enabled detailed stats, also confirm that the Cache tab displays individual cache requests.

Monitor request latency, cache hit rate, disk utilization, eviction age, memory, and peer traffic under normal load. Compare block-cache and lookaside-cache hit rates after sizing changes.

## More configuration

For more configuration options beyond caching, such as authentication and storage, see our [configuration docs](config.md) and our [enterprise configuration guide](enterprise-config.md). See [all configuration options](config-all-options.mdx) for the complete flag reference.

## Legacy cache backends

The GCS, S3, Redis, and Memcache cache backends are deprecated. Please [contact us](https://www.buildbuddy.io/contact) for help with migrating to Pebble.
