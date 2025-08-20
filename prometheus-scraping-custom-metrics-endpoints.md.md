---
title: Prometheus Scraping Support for Custom Metrics Endpoints
description: Added configurable Prometheus scraping endpoints to support customers who cannot use remote write protocols, enabling metrics collection through lightweight scraping targets with configurable retention periods.
date: 2025-01-16
tags: [monitoring, prometheus, metrics, infrastructure]
---

## Prometheus Scraping Configuration Support

We've added support for configurable Prometheus scraping endpoints to accommodate enterprise customers with restrictive network policies that prevent remote write access. This enhancement allows you to deploy lightweight Prometheus instances that expose metrics via scraping endpoints with minimal resource overhead.

The implementation includes:
- **Configurable retention periods**: Deploy scraping targets with customizable data retention (e.g., 1 day) to minimize storage requirements
- **Lightweight deployment model**: Run minimal Prometheus instances dedicated solely to metrics exposure rather than long-term storage
- **Enterprise-friendly architecture**: Designed for environments where outbound remote write connections are blocked by security policies

This approach provides a bridge for organizations that need to integrate BuildBuddy metrics into their existing Prometheus-based monitoring infrastructure while working within network security constraints.