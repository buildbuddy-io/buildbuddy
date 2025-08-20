---
title: Prometheus Scraping Configuration for Custom Client Metrics
description: Enhanced monitoring capabilities with dedicated Prometheus endpoint for clients requiring scrape-based metrics collection
date: 2025-08-18
tags: [monitoring, prometheus, metrics, infrastructure]
---

## Prometheus Scraping Configuration Support

We've added support for clients who need to scrape metrics from a dedicated Prometheus endpoint rather than using remote write approaches. This implementation creates a lightweight Prometheus instance that maintains a 1-day retention window specifically for scraping scenarios.

This enhancement enables organizations with existing scrape-based monitoring infrastructure to integrate BuildBuddy metrics without requiring changes to their remote write configurations. The dedicated endpoint provides standard Prometheus metrics format while maintaining minimal resource overhead through the short retention policy.

The scraping configuration is particularly useful for enterprise environments where metrics collection policies require pull-based approaches or where network configurations don't support direct remote write connectivity.