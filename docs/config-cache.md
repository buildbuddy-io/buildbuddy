<!--
{
  "name": "Cache",
  "category": "5eed3e2ace045b343fc0a328"
}
-->
# Cache Configuration

## Section

```cache:``` The cache section enables the BuildBuddy cache and configures how and where it will store data. **Optional**

## Options

**Optional**

* ```max_size_bytes:``` How big to allow the cache to be (in bytes).

* ```in_memory:``` Whether or not to use the in_memory cache.

* ```disk:``` The Disk section configures a disk-based cache.

  - ```root_directory``` The root directory to store cache data in, if using the disk cache. This directory must be readable and writable by the BuildBuddy process. The directory will be created if it does not exist.

## Example section
```
cache:
  max_size_bytes: 10000000000  # 10 GB
  disk:
    root_directory: /tmp/buildbuddy-cache
```