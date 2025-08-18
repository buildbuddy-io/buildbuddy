---
title: Remote Execution Performance Improvements
description: Significant performance improvements to remote execution with new scheduling algorithms
category: Performance
date: 2024-01-15
image: /static/img/rbe-performance.png
tags: [remote-execution, performance, bazel]
author: @maggie
---

We've made significant improvements to our remote execution infrastructure that deliver faster build times and better resource utilization.

## Key Improvements

- **40% faster build times** for large codebases
- **Improved scheduling algorithms** that better match workloads to available resources
- **Enhanced caching strategies** reducing redundant work across builds
- **Better error handling** with automatic retry mechanisms

## Technical Details

Our new scheduling system uses machine learning to predict optimal resource allocation, resulting in:

- Reduced queue times for high-priority builds
- Better utilization of executor resources
- Improved fault tolerance and recovery

## Getting Started

To take advantage of these improvements, update your `.bazelrc` configuration:

```bash
# Enable improved remote execution
build --remote_executor=grpcs://remote.buildbuddy.io
build --remote_cache=grpcs://remote.buildbuddy.io
build --experimental_remote_execution_keepalive=true
```

## What's Next

We're continuing to invest in remote execution performance with upcoming features:

- **Smart workload balancing** across multiple regions
- **Predictive caching** based on historical build patterns
- **Enhanced monitoring** and debugging tools

These performance improvements are available now for all Enterprise customers. Contact your account manager for assistance with configuration and optimization.