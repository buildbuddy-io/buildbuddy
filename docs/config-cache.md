---
id: config-cache
title: Cache Configuration
sidebar_label: Cache
---

This document describes how to configure **BuildBuddy's open-source (OSS) cache**, which can act as a remote disk cache on a single node only. For Enterprise configuration, see [Enterprise cache setup](enterprise-cache-setup.md).

## Section

`cache:` The cache section enables the BuildBuddy cache and configures how and where it will store data. **Optional**

## Options

**Optional**

- `max_size_bytes:` How big to allow the cache to be (in bytes).

- `in_memory:` Whether or not to use the in_memory cache.

- `zstd_transcoding_enabled`: Whether or not to enable cache compression capabilities. You need to use `--experimental_remote_cache_compression` to activate it on your build.

- `disk:` The Disk section configures a disk-based cache.
  - `root_directory` The root directory to store cache data in, if using the disk cache. This directory must be readable and writable by the BuildBuddy process. The directory will be created if it does not exist.

## Example section

### Disk

```yaml title="config.yaml"
cache:
  max_size_bytes: 10000000000 # 10 GB
  disk:
    root_directory: /tmp/buildbuddy-cache
```
