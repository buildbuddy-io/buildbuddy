---
title: "bb detect nondeterminism"
date: 2026-07-01T10:00:00
authors: maggie
tags: [bazel, debugging]
---

You can now use the `bb` CLI to detect non-deterministic builds using the `bb detect nondeterminism` command.

The command runs a Bazel command twice with caching disabled, then compares the two compact execution logs with [bb explain](/changelog/bb-explain). Spans whose outputs differ between the two runs are reported as non-deterministic.

```bash
# By default, the command runs "build //...".
bb detect nondeterminism

# You can pass any Bazel command to run.
bb detect nondeterminism --bazel_command='build //foo:bar --config=linux'
```

## Sending notifications

To email all BuildBuddy org admins when non-determinism is detected, add `--notify_email`.
To post a notification to a Slack channel, add `--notify_slack=<SECRET_NAME>`, where `<SECRET_NAME>` is the name of a [BuildBuddy secret](/docs/secrets) holding a Slack webhook URL.

Sending notifications requires an API key with the **notification** capability. Set it via the `BB_NOTIFY_API_KEY` environment variable:

```bash
BB_NOTIFY_API_KEY=<API_KEY> bb detect nondeterminism --notify_email
```

## Running on a schedule

To schedule a nightly nondeterminism check to catch regressions, you can configure a scheduled Workflow in your buildbuddy.yaml:

```yaml title="buildbuddy.yaml"
actions:
  - name: Nondeterminism check
    triggers:
      schedule:
        crons:
          - "0 8 * * *" # 8:00 AM UTC every day
    steps:
      - run: bb detect nondeterminism --notify_email --notify_slack=SLACK_WEBHOOK_URL
    platform_properties:
      # Caching is disabled for this check anyway, so recycling adds little.
      recycle-runner: false
```
