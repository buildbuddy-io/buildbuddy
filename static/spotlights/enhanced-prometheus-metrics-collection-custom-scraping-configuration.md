---
title: Enhanced Prometheus Metrics Collection with Custom Scraping Configuration
description: Added support for custom Prometheus scraping configurations to enable metrics collection for clients unable to use remote write approach
date: 2025-08-18
tags: ['Platform', 'Performance']
author: Brandon
---

We've introduced enhanced Prometheus metrics collection capabilities to accommodate diverse client infrastructure requirements. For organizations that cannot implement the standard remote write approach for metrics collection, we now support deploying a lightweight Prometheus instance that retains approximately one day of data, specifically configured for external scraping.

This change addresses the common scenario where security policies or network configurations prevent direct remote write access, while still enabling comprehensive observability. The dedicated scraping endpoint provides a minimal footprint solution that maintains essential metrics without requiring long-term storage on our infrastructure.

The implementation includes configurable retention policies and optimized resource allocation for the scraping instance, ensuring reliable metrics collection without impacting primary system performance. This approach provides flexibility for enterprise environments with strict data egress requirements while maintaining full compatibility with existing Prometheus-based monitoring stacks.