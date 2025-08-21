---
title: OCI Image Caching Now Live Across All Remote Executors
description: Container image caching with digest resolution is now enabled at 100% rollout, delivering exceptional cache hit rates and faster build execution times
date: 2025-01-27
tags: ['Remote Runners', 'Performance']
author: Dan
---

## OCI Image Caching Now Live Across All Remote Executors

We've successfully rolled out OCI image caching to 100% of remote executors, significantly improving container-based build performance. The feature is delivering exceptionally high cache hit rates, dramatically reducing the time spent pulling container images during remote execution.

**Key improvements:**
- **Universal availability**: All remote executors now benefit from intelligent image caching
- **Digest-based resolution**: Container image names are automatically resolved to include cryptographic digests, ensuring cache consistency and eliminating tag-based cache misses
- **High hit rates**: Early metrics show outstanding cache effectiveness across diverse workloads

This enhancement is particularly beneficial for workflows that frequently use the same base images across builds, as subsequent executions can skip expensive image pulls entirely. The digest resolution ensures that cached images remain valid even when tags are updated, providing both performance gains and build reproducibility.