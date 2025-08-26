---
title: Fixed custom resource starvation with backfilling scheduler
description: Prevents large GPU tasks from being indefinitely blocked by smaller tasks skipping ahead in the queue
date: 2025-01-14
tags: ['performance', 'remote runners']
author: Brandon
---

We've implemented a backfilling scheduling policy that prevents resource starvation when using custom resources like GPUs. Previously, tasks requiring large amounts of custom resources could be indefinitely blocked if smaller tasks continuously skipped ahead in the queue.

The scheduler now tracks "reserved" resources when evaluating which tasks can run, ensuring that tasks skipping ahead don't delay the start time of tasks that have been waiting longer. When a task is skipped, its resource requirements are reserved for future scheduling decisions, preventing scenarios where a task needing exclusive GPU access gets starved by a stream of smaller GPU tasks.

This change also generalizes the queue skipping logic to work with CPU and RAM resources, not just custom resources, providing more consistent scheduling behavior across all resource types. The improvement is particularly beneficial for workloads mixing lightweight compilation tasks with resource-intensive GPU or simulator tests.