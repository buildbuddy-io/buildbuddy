---
title: Flexible Prometheus Metrics Scraping Configuration
description: Added support for alternative Prometheus metrics collection via dedicated scraping endpoints for environments that cannot use remote write protocols
date: 2025-01-08
tags: [prometheus, metrics, monitoring, scraping]
---

## Flexible Prometheus Metrics Scraping Configuration

We've implemented an alternative metrics collection approach for environments with specific infrastructure constraints. This new configuration allows external Prometheus instances to scrape metrics from a lightweight, dedicated Prometheus endpoint that maintains a rolling 1-day data retention window.

This solution is particularly valuable for organizations that cannot implement remote write protocols due to network policies or security requirements, providing a pull-based metrics collection pattern while maintaining minimal resource overhead through the compact data retention policy.