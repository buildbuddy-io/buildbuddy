---
id: config-cache
title: Cache Configuration
sidebar_label: Cache
---

## Section

`cache:` The cache section enables the BuildBuddy cache and configures how and where it will store data. **Optional**

## Options

**Optional**

- `max_size_bytes:` How big to allow the cache to be (in bytes).

- `in_memory:` Whether or not to use the in_memory cache.

- `zstd_transcoding_enabled`: Whether or not to enable cache compression capabilities. You need to use `--experimental_remote_cache_compression` to activate it on your build.

- `disk:` The Disk section configures a disk-based cache.

  - `root_directory` The root directory to store cache data in, if using the disk cache. This directory must be readable and writable by the BuildBuddy process. The directory will be created if it does not exist.

**Enterprise only**

Legacy GCS, S3, Redis, and Memcache cache backends are deprecated for new deployments.
Prefer `cache.disk.root_directory` for cache storage, and configure `storage.gcs` or
`storage.aws_s3` for durable blob and build event storage. The legacy cache options
remain listed in the [all options](config-all-options.mdx) docs and are marked deprecated
for existing deployments.

## Example section

### Disk

```yaml title="config.yaml"
cache:
  max_size_bytes: 10000000000 # 10 GB
  disk:
    root_directory: /tmp/buildbuddy-cache
```
