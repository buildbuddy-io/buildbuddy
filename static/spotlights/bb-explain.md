---
title: Debug cache misses with bb explain
description: Debug non-determinism between builds with execution log diffing
date: 2025-08-21
tags: [featured, bazel]
author: Fabian
---

_bb explain_ can help you debug the root cause of cache misses between builds and identify sources
of non-determinism. This CLI command compares the compact execution logs of two
builds to identify what has changed - such as modified inputs or transitive
invalidations.

![bb explain](/spotlight_images/bb-explain.png)

## Perfect for debugging:

- Unexpected build slowdowns ("Why did 500 targets rebuild?")
- Flaky test failures ("Did the environment or inputs change?")
- Cache misses ("Which action inputs are non-deterministic?")
- Build hermiticity issues ("Are outputs changing between runs?")

## Usage:

1. Run two builds with the _--execution_log_compact_file_ flag.
2. The _bb_ CLI takes either two compact execution logs or invocation IDs.

`bb explain --old bdd9db47-c099-4901-953f-92fa0f8534d1 --new 5e7c2203-6500-47b9-bbe3-6cc3223c2b01`
