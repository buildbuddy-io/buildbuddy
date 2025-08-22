---
title: Enhanced usage reporting
description: More detailed usage breakdowns and visualization charts
date: 2024-08-22
author: Jim
---

We've updated BuildBuddy's usage page with detailed breakdowns and charts.

![usage card](/spotlight_images/usage.png)
![usage chart](/spotlight_images/usage-chart.png)

## New Detailed Usage Metrics

Our enhanced usage reporting now tracks and displays:

### Data Transfer Breakdown

- **External uploads/downloads** - Bytes transferred outside BuildBuddy servers
- **Internal uploads/downloads** - Bytes transferred between the BuildBuddy servers and executors (excluding workflows)
- **Workflow uploads/downloads** - Bytes transferred specifically for firecracker Workflow snapshots

### Compute Usage Tracking

- **Total cloud CPU time** - Raw CPU usage across all BuildBuddy cloud services
- **Remote execution CPU time** - CPU usage specifically for BuildBuddy executors
- **Workflow CPU time** - CPU usage specifically for BuildBuddy workflow runners
- **Remote execution duration** - Wall time for tasks on BuildBuddy executors
- **Workflow execution duration** - Wall time for tasks on BuildBuddy workflow runners

## Why These Detailed Metrics Matter

This granular visibility helps you:

- **Understand data flow patterns** - See exactly where your data transfers are happening
- **Optimize workflow efficiency** - Compare CPU vs wall time to identify performance bottlenecks
- **Allocate costs accurately** - Break down usage by service type for precise cost attribution
- **Plan capacity needs** - Use historical patterns to predict future resource requirements
- **Budget forecasting** - Predict future costs based on usage patterns

## Getting Started

The new usage reporting features are available in your BuildBuddy dashboard. Navigate to the usage section to explore:

- Detailed breakdown charts showing consumption by category
- Historical usage trends and patterns
- Downloadable usage reports for further analysis
