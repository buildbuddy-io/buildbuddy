---
title: "Improved GPU and device support for self-hosted executors"
date: 2026-03-31T10:00:00
authors: brandon
---

GPU support for self-hosted Linux executors is now easier to set up.

BuildBuddy's executor image now ships with GPU supporting tools
(`nvidia-cdi-hook`), and the executor now supports configuring CDI devices
so executors can pass through vendor-provided GPU device configuration to
child containers.

See the new setup docs for details:
[GPU support for self-hosted executors](https://buildbuddy.io/docs/config-rbe#gpu-support).
