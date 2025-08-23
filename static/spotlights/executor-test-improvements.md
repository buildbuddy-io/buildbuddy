---
title: Enhanced self-hosted executor visibility
description: New insights into executor queue length and in-flight actions
date: 2024-08-22
tags: ["platform"]
author: Siggi
---

We've enhanced the executor card with real-time monitoring capabilities, giving you better visibility into your self-hosted executor performance and health.

![executor card](/spotlight_images/executor-card.png)
![executor drilldown](/spotlight_images/executor-drilldown.png)

## New Executor Card Features

Each executor card now displays additional critical information to help you monitor and manage your infrastructure:

- **Live queue length** - See the current number of actions waiting to be processed by each executor
- **Last check-in time** - Monitor when each executor last communicated with BuildBuddy to ensure connectivity
- **In-flight action count** - View how many actions are currently being executed on each executor

## Why This Matters

These real-time metrics provide essential visibility for:

- **Load balancing** - Identify which executors are handling the most work
- **Health monitoring** - Quickly spot executors that may have connectivity issues
- **Capacity planning** - Understand current utilization across your executor fleet
- **Troubleshooting** - Diagnose bottlenecks and performance issues in real-time

## Diving Deeper with "View executions"

Each executor card includes a "View executions" button that takes you to a detailed drilldown page. This is particularly useful for:

- **Performance analysis** - See historical execution patterns and identify trends
- **Debugging slow builds** - Trace specific actions that may be causing bottlenecks
- **Resource optimization** - Understand which types of workloads perform best on each executor
- **Troubleshooting failures** - Quickly access logs and error details for failed executions

Use this feature when you notice unusual queue lengths or execution times to get the full context behind your executor's performance.
