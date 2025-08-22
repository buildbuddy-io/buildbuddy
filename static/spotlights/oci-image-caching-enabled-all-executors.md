---
title: Oci image caching now enabled across all executors
description: OCI image caching is now available on 100% of executors with consistently high hit rates and improved image digest resolution
date: 2025-08-18
tags: ['performance', 'platform']
author: Dan
---

OCI image caching has been fully rolled out to all executors, delivering significant performance improvements for containerized builds. The feature is showing exceptionally high cache hit rates in production, dramatically reducing image pull times during build execution.

A recent enhancement ensures that image names are always resolved to include their cryptographic digests before caching. This change improves cache consistency and reliability by guaranteeing that cached images are referenced by their immutable digest rather than potentially mutable tags. The digest resolution happens transparently during the caching process, providing both performance benefits and stronger guarantees about image content integrity.

This optimization is particularly beneficial for builds that frequently use the same base images or have multiple steps requiring identical container environments, as subsequent executions can skip expensive network operations and use locally cached image layers.