---
title: Prometheus Metrics Scraping Support for Enterprise Customers
description: Added support for Prometheus scraping configuration to accommodate customers who cannot use remote write approach
date: 2025-08-18
tags: ['Platform', 'Performance']
author: Brandon
---

We've introduced support for Prometheus metrics scraping to better accommodate enterprise customers with strict network policies. This new configuration allows customers to scrape metrics from a lightweight Prometheus instance that we run on their behalf, which maintains only 1 day of data retention for optimal performance.

This approach is particularly beneficial for organizations that cannot implement the standard remote write pattern due to security constraints or network topology requirements. The scraping configuration provides the same observability capabilities while working within existing infrastructure limitations and compliance requirements.