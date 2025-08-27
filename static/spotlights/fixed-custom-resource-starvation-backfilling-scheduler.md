---
title: Fixed custom resource starvation with backfilling scheduler
description: Prevents large GPU tasks from being indefinitely delayed by smaller tasks skipping ahead in the queue
date: 2025-01-16
tags: ['platform', 'performance']
author: Brandon
---

We've implemented a backfilling scheduling policy to fix a critical issue where tasks requiring large amounts of custom resources (like GPU memory) could be starved indefinitely by smaller tasks jumping ahead in the queue.

Previously, when a task was blocked only on custom resources, the scheduler would skip it and attempt to schedule the next task in line. This could lead to scenarios where a task requiring exclusive GPU access would never get scheduled if the executor was busy with many smaller GPU tasks that could "cut in line."

The new backfilling algorithm maintains resource reservations as it iterates through the queue, ensuring that tasks can only skip ahead if they don't delay the start time of tasks that have been waiting longer. When evaluating whether a task can skip ahead, the scheduler now accounts for the resources needed by all previously skipped tasks, preventing resource starvation.

This change improves fairness in task scheduling and ensures that high-resource tasks like ML training jobs or comprehensive test suites requiring dedicated hardware resources will eventually get their turn to execute, even in busy environments with many concurrent smaller tasks.