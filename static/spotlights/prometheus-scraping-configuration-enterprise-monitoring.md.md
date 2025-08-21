---
title: Prometheus Scraping Configuration for Enterprise Monitoring
description: Configure BuildBuddy to expose metrics via a lightweight Prometheus instance for customers who require pull-based monitoring instead of remote write capabilities.
date: 2025-01-27
tags: [monitoring, prometheus, metrics, infrastructure]
---

## Prometheus Scraping Configuration for Enterprise Customers

We've added support for customers who need to scrape metrics from BuildBuddy using their existing Prometheus infrastructure, rather than pushing metrics via remote write. This is particularly useful for organizations with strict network policies or existing monitoring architectures that require pull-based metric collection.

### Key Features

- **Lightweight Prometheus Instance**: BuildBuddy now runs a minimal Prometheus instance that retains only 1 day of data, optimized for scraping rather than long-term storage
- **Scrape-Compatible Endpoint**: Exposes all BuildBuddy metrics in a format compatible with external Prometheus scrapers
- **Network-Friendly**: Eliminates the need for outbound remote write connections, working within restrictive network environments
- **Resource Efficient**: The scraping instance uses minimal resources since it only maintains short-term data for collection

This enhancement allows enterprise customers to integrate BuildBuddy metrics into their existing observability stack without requiring changes to their network configuration or monitoring architecture. The scraping endpoint provides the same comprehensive metrics as our standard monitoring, but in a format that fits naturally into pull-based monitoring workflows.