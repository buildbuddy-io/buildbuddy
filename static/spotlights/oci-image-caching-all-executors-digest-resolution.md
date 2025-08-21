---
title: OCI Image Caching Now Available for All Executors with Digest Resolution
description: OCI image caching is now enabled across 100% of executors with enhanced digest resolution for improved cache hit rates and reliability
date: 2025-08-18
tags: ['Performance', 'Remote Runners']
author: Dan
---

## OCI Image Caching Now Available for All Executors with Digest Resolution

We've successfully rolled out OCI image caching to 100% of our executors, delivering significant performance improvements for container-based builds. The feature is showing exceptionally high cache hit rates in production, dramatically reducing image pull times for remote execution workloads.

As part of this rollout, we've implemented automatic image name resolution to include content digests. This enhancement ensures cache entries are keyed by immutable image digests rather than potentially mutable tags, improving cache consistency and eliminating issues where the same tag could reference different image content over time.

This change particularly benefits workflows using popular base images or frequently updated container images, as subsequent builds can leverage cached layers instead of re-downloading identical content. Engineers should see noticeable improvements in remote execution performance, especially for builds with heavy container dependencies.