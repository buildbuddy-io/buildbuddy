---
title: "Auto-cancel duplicate CI runs on the same branch"
date: 2026-05-04T10:00:00
authors: maggie
tags: [CI]
---

Starting June 1, 2026, BuildBuddy will automatically cancel in-progress Workflow runs when a newer run is triggered for the same action on a non-default branch. This helps avoid wasting resources on outdated runs when, for example, several commits are pushed in quick succession to a pull request branch.

This behavior only applies to non-default branches. Runs on your repo's default branch are not affected.

If you'd like to disable this behavior and allow concurrent runs for an action, set `allow_concurrent_runs: true` in the action's configuration.

See the [Workflows configuration docs](/docs/workflows-config#concurrent-workflow-runs) for more details.
