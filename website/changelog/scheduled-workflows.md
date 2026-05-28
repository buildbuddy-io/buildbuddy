---
title: "Schedule Workflows with cron expressions"
date: 2026-05-28T10:00:00
authors: maggie
tags: [CI]
---

You can now run BuildBuddy Workflows on a recurring schedule using standard cron expressions. This is useful for nightly builds, periodic integration tests, or any job that should run independently of code pushes or pull requests.

To schedule an action, add a `schedule` trigger with one or more cron expressions under `triggers`:

```yaml title="buildbuddy.yaml"
actions:
  - name: "Nightly tests"
    triggers:
      schedule:
        crons:
          - "0 2 * * *" # 2:00 AM UTC every day
    steps:
      - run: "bazel test //..."
```

Scheduled runs always execute against the latest commit on your repo's default branch. All cron times are interpreted as UTC, and the minimum supported interval between triggers is 15 minutes.

See the [Workflows configuration docs](/docs/workflows-config#scheduling-workflows-with-cron-expressions) for format details and more examples.
