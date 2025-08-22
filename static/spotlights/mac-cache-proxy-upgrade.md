---
title: Improved Mac executor performance with edge caches
description: All Mac executors now point to edge cache proxies for faster builds
date: 2024-08-22
tags: ["performance", "platform"]
author: Iain
---

We've updated all Mac executors to route cache traffic through our San Jose edge cache proxies,
delivering significantly faster build performance for Mac workloads and reducing network
egress costs.

![Mac egress](/spotlight_images/mac-egress-with-proxy.png)

_Mac egress to the apps before and after enabling the SJC cache proxies._

## Performance Improvements

All US-based Mac executors now benefit from:

- **Reduced latency** - Cache requests now go through geographically closer SJC proxies
- **Faster artifact downloads** - Improved download speeds for cached build outputs
- **Better upload performance** - Faster cache uploads during build execution
- **Enhanced reliability** - More robust cache connectivity with regional proximity

## Technical Details

The upgrade includes:

- **Automatic routing** - All Mac executors seamlessly use SJC cache proxies
- **Zero configuration** - No changes needed to existing build configurations
- **Transparent operation** - Cache behavior remains the same, just faster
- **Regional optimization** - Cache traffic stays within the US West Coast region

## Usage

This improvement is live now for all US Mac executor workloads. No action is required - your Mac builds will automatically benefit from the enhanced cache performance.
