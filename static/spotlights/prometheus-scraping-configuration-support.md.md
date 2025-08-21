---
title: Prometheus Scraping Configuration Support
description: Enhanced metrics collection flexibility with configurable Prometheus scraping endpoints for customers with restricted remote write capabilities
date: 2025-01-27
tags: [monitoring, prometheus, metrics, configuration]
---

## Prometheus Scraping Configuration Support

We've added support for Prometheus scraping configurations to accommodate customers who cannot use the standard remote write approach for metrics collection. This enhancement allows you to deploy a lightweight Prometheus instance that maintains short-term data retention (typically 1 day) while exposing metrics through a scrapeable endpoint.

This solution is particularly useful for organizations with network restrictions or security policies that prevent outbound remote write connections, enabling them to pull metrics data using their existing Prometheus infrastructure while maintaining minimal resource overhead on the BuildBuddy side.