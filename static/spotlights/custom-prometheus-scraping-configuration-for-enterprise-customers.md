---
title: Custom Prometheus Scraping Configuration for Enterprise Customers
description: Added support for Prometheus scraping configuration to accommodate customers with restricted remote write capabilities
date: 2025-08-18
tags: ['Platform', 'Performance']
author: Grace
---

We've introduced a flexible Prometheus metrics collection solution for enterprise customers who cannot use the standard remote write approach due to security or infrastructure constraints. This new configuration allows customers to scrape metrics from a lightweight, dedicated Prometheus instance that we deploy and maintain.

The solution provisions a minimal Prometheus server that retains only essential metrics with a short retention period (approximately 1 day), optimizing for both performance and storage efficiency. This approach enables customers with strict network policies or compliance requirements to still access BuildBuddy's telemetry data through their existing Prometheus infrastructure, while maintaining the security boundaries required by their organizations.

This configuration is particularly beneficial for customers operating in air-gapped environments or those with policies that prevent outbound metric shipping, ensuring they can still leverage BuildBuddy's observability features without compromising their security posture.