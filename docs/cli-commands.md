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

## bb fix

### bb fix test

`bb fix test` first runs the same remote runner reconstruction and escalating
strategies as `bb detect flake`. Once the flake is reproduced, it fetches the
matching test output from the newly created child Bazel invocation using the
same target and test filter accepted by `bb view`. It gives that output and the
successful reproduction scope to the selected agent. The agent edits the local
workspace, and the command then mirrors that patch into a second remote
detector run. Verification retries only the strategy that reproduced the
flake. For example, if the target and filter reproduced it, verification reruns
only that filtered target command with the patch applied.

```bash
bb fix test <invocation-id-or-url> -n=100

bb fix test <invocation-id-or-url> //server/foo:foo_test \
  --test_filter=TestName -n=100
```

Use `--agent=claude` (the default) or `--agent=codex`; `--model` and `--effort`
are forwarded to the selected agent.

## bb detect

### bb detect flake

`bb detect flake` replays the explicit Bazel command from an existing BuildBuddy
invocation and progressively broadens the test scope until it reproduces a
flaky test. Bazel reloads the checked-in rc files and expands the same explicit
`--config` flags, rather than replaying canonical/internal flags. The detector
recreates the original CI runner type by cloning the
outer runner action's platform properties. Runner recycling is disabled for the
detector so it cannot overwrite the original runner snapshot. The replayed
Bazel commands are dispatched directly as steps on that runner. The outer
runner log, including the full output of each Bazel command, is streamed
locally. The runner automatically links each nested Bazel invocation to the
outer invocation. The repository is checked out at the exact commit recorded in
the original outer runner command.

1. The specified target and `--test_filter` with `--runs_per_test=n`.
2. The specified target without the filter with `--runs_per_test=n`.
3. The entire original command in up to `n` separate Bazel invocations, without
   adding `--runs_per_test`.

The first two policies run once with `--runs_per_test=n`. The filtered policy
also uses `--notest_keep_going`, so Bazel stops after the first matching failure.
The whole-target policy does not stop early, since a different test in the
target could fail and its output needs to remain visible. The final policy
preserves the original command shape and stops as soon as one of its repeated
invocations fails. Every policy disables test-result caching and Bazel's
flaky-test retries.

```bash
bb detect flake <invocation-id> \
  --target=//server/foo:foo_test \
  --test_filter=TestName \
  --n=100
```

The command exits with code 10 when it reproduces the flake, 0 when it does not,
and another nonzero code for usage or infrastructure errors.

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
