---
title: Enhanced UI Metrics for Test Execution and Remote Runner Monitoring
description: Added comprehensive duration statistics for test targets and real-time monitoring capabilities for remote executors
date: 2025-08-18
tags: ['Testing', 'Remote Runners', 'Performance']
author: Siggi
---

## Enhanced Test Target and Executor Monitoring

We've significantly improved visibility into test execution performance and remote runner health with several key UI enhancements:

**Test Target Duration Analytics**: Test target pages now display comprehensive duration statistics (average, minimum, and maximum) when `runs_per_test > 0`, providing engineers with detailed performance insights for flaky test analysis and execution time optimization.

**Executor Capabilities Visibility**: The executor list view now includes supported isolation types, making it easier to understand which executors support specific containerization or sandboxing requirements for your builds.

**Real-time Executor Health Monitoring**: Executor cards have been enhanced with live operational metrics including:
- Current queue length for pending work visibility
- Last check-in timestamp for connection health monitoring  
- In-flight action count for active workload tracking

These improvements provide better observability into both test execution patterns and remote runner infrastructure health, enabling more effective debugging and capacity planning.