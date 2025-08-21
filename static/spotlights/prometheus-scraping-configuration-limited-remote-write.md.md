---
title: Prometheus Scraping Configuration for Limited Remote Write Environments
description: Added support for Prometheus scraping configuration via a lightweight local Prometheus instance with short-term data retention for environments that cannot use remote write approaches.
date: 2025-08-18
tags: [monitoring, prometheus, metrics, infrastructure]
---

## Prometheus Scraping Configuration Support

We've introduced a new metrics collection pattern for environments with restricted remote write capabilities. This enhancement enables organizations to integrate BuildBuddy metrics into their Prometheus monitoring stack through a scraping-based approach.

### Implementation Details

The solution deploys a minimal Prometheus instance that:
- Maintains only 1 day of local data retention to minimize storage overhead
- Exposes metrics endpoints optimized for external scraping
- Operates as a lightweight proxy between BuildBuddy's internal metrics and external monitoring infrastructure

This pattern is particularly valuable for enterprise environments where security policies or network configurations prevent direct remote write access to external monitoring systems. The short retention window ensures the local instance remains resource-efficient while still providing real-time metrics availability for downstream Prometheus servers.

The scraping configuration allows for seamless integration with existing Prometheus deployments without requiring changes to network policies or authentication mechanisms that typically restrict remote write operations.