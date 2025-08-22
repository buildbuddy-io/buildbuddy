---
title: OCI image caching hits 100% rollout with insanely high hit rates
description: Container image caching is now fully deployed across all executors with digest-based resolution for optimal cache performance
date: 2025-01-27
tags: ['platform', 'performance']
author: Dan
---

Our OCI image caching feature has reached 100% rollout across all executors and is delivering exceptional performance with incredibly high cache hit rates. This optimization significantly reduces build times by eliminating redundant container image downloads during remote execution.

The latest improvement includes automatic resolution of image names to include digests, ensuring deterministic caching based on the actual image content rather than potentially mutable tags. This change guarantees that cached images are correctly identified and reused across builds, even when image tags are updated to point to different content.

While rolling out this enhancement, we identified and resolved authentication challenges with specific customer configurations. We're continuing to refine the implementation to ensure seamless operation across diverse container registry setups and authentication methods.