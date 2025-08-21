---
title: Prometheus Scraping Support for Custom Configurations
description: Added support for Prometheus scraping configurations to accommodate environments that cannot use remote write approaches
date: 2025-01-08
tags: [monitoring, prometheus, metrics, infrastructure]
---

## Prometheus Scraping Configuration Support

We've added support for custom Prometheus scraping configurations to better accommodate diverse monitoring setups. This enhancement allows organizations with restricted networking or specific monitoring policies to integrate BuildBuddy metrics into their existing Prometheus infrastructure.

The implementation deploys a lightweight Prometheus instance that maintains a minimal data retention window (approximately 1 day) and exposes metrics endpoints optimized for external scraping. This approach is particularly valuable for environments where the standard remote write protocol cannot be used due to security policies, network restrictions, or existing monitoring architecture constraints.

This change enables seamless metrics collection for organizations that require pull-based monitoring patterns while maintaining the performance characteristics needed for high-throughput build environments.