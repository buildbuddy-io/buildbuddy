---
title: Enhanced usage data with category breakdowns
description: Usage reports now break down cache and execution metrics by internal, external, and workflow traffic
date: 2024-08-22
author: Jim
---

Our Usages page now provides detailed breakdowns of your BuildBuddy consumption
across different categories.

![usage card](/spotlight_images/usage.png)
![usage chart](/spotlight_images/usage-chart.png)

Previously, cache download/upload metrics and
execution duration were reported as aggregate totals. Now you can see usage segmented by:

- **External traffic**: Builds originating from outside BuildBuddy-managed infrastructure
- **Internal traffic**: Standard Remote Build Execution (RBE) usage on BuildBuddy executors
- **Workflow traffic**: Usage from BuildBuddy's managed Bazel workflows

The enhanced reporting includes new CPU time metrics that distinguish between wall-clock execution duration and actual CPU utilization across RBE and workflow environments. Charts now display stacked visualizations showing the contribution of each category to your total usage.

This granular visibility helps teams understand their usage patterns, optimize resource allocation, and track the impact of different build strategies on their overall BuildBuddy consumption.
