---
id: cli-commands
title: CLI Commands
sidebar_label: CLI Commands
---

In addition to functioning as a Bazel wrapper, the [BuildBuddy CLI](/cli) ships with a set of subcommands that help you debug and improve your builds.

If a command is not documented here, you can always see all available commands and their flags using `bb help`:

```bash
# See all available commands.
bb help

# See help for a specific command.
bb help <command>
```

## bb detect

### bb detect nondeterminism

Non-deterministic builds can cause wasted computation and degraded performance. `bb detect nondeterminism` detects non-determinism
by running the same Bazel command twice with all caching disabled, then comparing the two compact execution logs with `bb explain`. Spans whose outputs differ between the two runs are reported as non-deterministic.

#### Usage

```bash
# By default, the command runs "build //...".
bb detect nondeterminism

# You can pass any Bazel command to run.
bb detect nondeterminism --bazel_command='build //foo:bar --config=linux'
```

If non-determinism is detected, the command exits with exit code `10`.

#### Sending notifications

When non-determinism is detected, the CLI can automatically notify your team.

To email all BuildBuddy org admins, add `--notify_email`.
To post a notification to a Slack channel, add `--notify_slack=<SECRET_NAME>`, where `<SECRET_NAME>` is the name of a [BuildBuddy secret](/docs/secrets) holding a Slack webhook URL.

Sending notifications requires an API key with the **notification** capability. Set it via the `BB_NOTIFY_API_KEY` environment variable:

```bash
BB_NOTIFY_API_KEY=<API_KEY> bb detect nondeterminism --notify_email
```

#### Running on a schedule

To schedule a nightly nondeterminism check to catch regressions, you can configure a scheduled Workflow in your buildbuddy.yaml:

```yaml title="buildbuddy.yaml"
actions:
  - name: Nondeterminism check
    triggers:
      schedule:
        crons:
          - "0 8 * * *" # 8:00 AM UTC every day
    steps:
      - run: bb detect nondeterminism --notify_email --notify_slack=SLACK_WEBHOOK_URL_SECRET_NAME
    platform_properties:
      # Caching is disabled for this check anyway, so recycling adds little.
      recycle-runner: false
```

##### Tips

If sending notifications from the Workflow, remember to set the required secrets in the BuildBuddy UI. These secrets will be
automatically injected into the Workflow environment:

- `BB_NOTIFY_API_KEY`: An API key with the **notification** capability.
- `SLACK_WEBHOOK_URL_SECRET_NAME`: A Slack webhook URL. This should be set via a secret because anyone with
  the URL can post to the channel.

Even though the builds are run with caching disabled, we still recommend enabling remote execution for the builds. This will
make the builds faster, and make the Workflow runner less likely to run out of local resources.

```bash
bb detect nondeterminism --bazel_command='build //foo:bar --remote_executor=grpcs://remote.buildbuddy.io'
```
