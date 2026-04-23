---
title: "End-to-end CDC support"
date: 2026-04-23T10:00:00
authors: tyler-french
tags: [bazel, featured]
---

We're excited to announce end-to-end content-defined chunking (CDC) support for Bazel remote caching in BuildBuddy.

With Bazel 9.1's new `--experimental_remote_cache_chunking` support, large outputs like linker artifacts can be uploaded and downloaded in content-defined chunks instead of as monolithic blobs. That lets BuildBuddy deduplicate similar artifacts across builds, reducing upload bandwidth and storage usage. In one benchmark on the BuildBuddy repo, this showed roughly 40% less uploaded data and a roughly 40% smaller disk cache. Breaking large blobs into smaller reusable pieces also means fewer long-running RPCs and more granular retries.

To enable it, add these flags to your `.bazelrc`:

```text
common --experimental_remote_cache_chunking
common --remote_header=x-buildbuddy-cdc-enabled=true
```

To see the download-side savings, you should also set `--disk_cache`, since the downloaded chunks need to be stored somewhere in order to be reused locally. We also recommend setting `--experimental_disk_cache_gc_max_age` to a value below your remote cache TTL—for example, `3h`, or `1d` if your remote TTL is longer.

Bazel 9.1 is required today, and this support will also be backported to Bazel 8.7.

For more background, see [bazelbuild/bazel#28437](https://github.com/bazelbuild/bazel/pull/28437).

We'll share a longer blog post soon with more details on the technical journey behind this work.
