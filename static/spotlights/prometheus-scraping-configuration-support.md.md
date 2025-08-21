---
title: Prometheus Scraping Configuration Support
description: Added support for Prometheus-compatible metrics scraping with configurable retention periods for environments that cannot use remote write
date: 2025-08-18
tags: [monitoring, prometheus, metrics, observability]
---

We've introduced a new Prometheus scraping configuration that allows external monitoring systems to collect metrics directly from BuildBuddy instances. This feature is particularly valuable for organizations with existing Prometheus infrastructure that prefer scraping over remote write protocols.

The implementation includes:

- **Lightweight metrics endpoint**: A dedicated Prometheus-compatible metrics server that exposes essential BuildBuddy telemetry
- **Configurable retention**: Adjustable data retention periods (e.g., 1-day windows) to minimize storage overhead while maintaining operational visibility
- **Pull-based architecture**: Native support for Prometheus scraping patterns, eliminating the need for push-based remote write configurations

This enhancement enables better integration with existing observability stacks and provides more flexibility for metrics collection strategies in enterprise environments where network policies or architectural constraints make remote write approaches impractical.