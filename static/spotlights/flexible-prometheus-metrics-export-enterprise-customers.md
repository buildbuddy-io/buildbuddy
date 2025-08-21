---
title: Flexible Prometheus Metrics Export for Enterprise Customers
description: Support for custom Prometheus scraping configurations when remote write isn't feasible
date: 2025-01-08
tags: ['Platform', 'Performance']
author: Brandon
---

We've enhanced our metrics infrastructure to support customers who need to scrape Prometheus metrics directly rather than using the standard remote write approach. This change enables organizations with strict networking policies or existing monitoring setups to integrate BuildBuddy metrics into their observability stack by configuring a lightweight Prometheus instance that retains short-term data (approximately 1 day) specifically for scraping purposes.

This solution provides the flexibility needed for enterprise environments where remote write protocols may be restricted while maintaining the performance characteristics of our metrics collection system.