---
title: Memory pool optimization for remote cache uploads
description: Reduced garbage collection overhead by implementing buffer pooling for remote cache upload operations
date: 2024-12-19
tags: ['performance', 'bazel']
author: Vanja
---

We've optimized memory allocation in the remote cache upload path by replacing direct buffer allocation with a shared buffer pool. Previously, the `uploadFromReader` function created new byte buffers for each upload operation, including separate buffers for compression operations when using ZSTD compression.

The change introduces buffer pooling through `uploadBufPool.Get()` and `uploadBufPool.Put()` calls, allowing buffers to be reused across multiple upload operations. This reduces garbage collection pressure and memory allocation overhead, particularly beneficial for workloads with frequent remote cache uploads.

The optimization applies to both the main upload buffer and the read/compression buffers used during ZSTD compression, ensuring consistent memory management throughout the upload pipeline.