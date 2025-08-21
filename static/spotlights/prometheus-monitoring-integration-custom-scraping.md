---
title: Enhanced Prometheus Monitoring Integration for Custom Scraping Configurations
description: Added support for custom Prometheus scraping configurations with lightweight local data retention for organizations that cannot use remote write approaches
date: 2025-08-18
tags: ['Platform', 'Performance']
author: Brandon
---

We've implemented a flexible Prometheus monitoring solution designed for organizations with restrictive network policies that prevent using remote write approaches. The new setup provisions a lightweight Prometheus instance that maintains only short-term data retention (approximately 1 day) while exposing metrics endpoints that can be scraped by your existing monitoring infrastructure.

This architecture allows you to integrate BuildBuddy metrics into your observability stack without requiring outbound network access from your environment. The local Prometheus instance acts as a metrics buffer, collecting and temporarily storing telemetry data before it's consumed by your primary monitoring systems through standard scraping protocols.

The implementation includes configurable retention policies and scraping intervals, making it suitable for high-security environments while maintaining full visibility into build performance and system health metrics.