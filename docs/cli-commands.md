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

## bb agent

`bb agent` subcommands use an AI coding agent to analyze BuildBuddy invocations.

Relevant invocation data is sent to the selected AI provider. This data can contain target names, file paths, build and test output, and other details about the build.

#### Prerequisites

The selected agent's CLI must be installed and available in `PATH`: `claude` or `codex`.

#### Authentication

On remote runners, only authorization via an API key is supported. It should be set as a [BuildBuddy secret](/docs/secrets).

Agent usage is billed according to the selected provider and authentication method.

```bash
ANTHROPIC_API_KEY=<API_KEY> bb agent <SUBCOMMAND> --agent=claude <INVOCATION_ID>

CODEX_API_KEY=<API_KEY> bb agent <SUBCOMMAND> --agent=codex <INVOCATION_ID>
```

Local runs can use a locally authenticated Claude Code or Codex subscription if available, or can authenticate with an API key.

#### Choosing an agent and model

Use `--agent` to select Claude or Codex.
If `--model` and `--effort` are omitted, the selected agent's defaults are used.

```bash
bb agent fix \
  --agent=codex \
  --model=gpt-5.4 \
  --effort=high \
  <INVOCATION_ID>

bb agent fix \
  --agent=claude \
  --model=claude-opus-5 \
  --effort=low \
  <INVOCATION_ID>
```

### bb agent analyze-profile

`bb agent analyze-profile` analyzes a Bazel timing profile uploaded by a BuildBuddy invocation. It produces a detailed report with recommendations for improving build performance.

The command accepts an invocation ID or invocation URL:

```bash
bb agent analyze-profile <INVOCATION_ID>
bb agent analyze-profile https://app.buildbuddy.io/invocation/<INVOCATION_ID>
```

#### Prerequisites

- The build must have uploaded a timing profile to the remote cache.
- Only `darwin-arm64` and `linux-amd64` are currently supported.

### bb agent fix

`bb agent fix` fixes a failure from a previous invocation by editing the current working tree, then verifies the fix.

The agent inspects the failure output and relevant source code, applies a minimal fix, and reruns the invocation's original command to verify it.
Pass `--verify=false` to skip reproduction and verification for a faster fix.

When run locally, the changes are applied to the current working tree.
When run remotely, the diffset is uploaded to the 'Artifacts' tab of the remote `agent fix` run. It can be [downloaded from the invocation](/docs/remote-runner-features#downloading-artifacts) and applied to a local workspace using `git apply <DIFF_FILE>`.

#### Usage

```bash
# Fix the error log from the invocation.
bb agent fix <INVOCATION_ID>

# Fix only a single failing target.
bb agent fix <INVOCATION_ID> //foo:bar_test

# Fix only the failing test cases that match a test filter.
bb agent fix <INVOCATION_ID> //foo:bar_test --test_filter=TestBaz

# Skip reproduction and verification for a faster fix.
bb agent fix <INVOCATION_ID> --verify=false

# Run the command remotely.
bb remote --script='bb agent fix <INVOCATION_ID>'
```

#### Prerequisites

- Run the command from the workspace that produced the failure, ideally checked out at the same commit.

### bb agent review

`bb agent review` reviews the open GitHub pull request for the checked-out branch, then posts the findings as PR review comments.

The review reads the source from the local checkout, so to review another PR, check it out first with `gh pr checkout <PR>`.

Draft PRs and PRs that already have a bot review are skipped. Pass `--force` (or set `AGENT_REVIEW_FORCE=1`) to review them anyway.
PRs from forks are refused by default, because reviewing one runs the agent over untrusted code. Pass `--allow_fork` to review one anyway.

#### Usage

```bash
# Review the PR and post comments.
bb agent review

# Print the review without posting it.
bb agent review --dry_run
```

#### Prerequisites

- The `gh` CLI must be installed and available in `PATH`.
- A GitHub token must be set in `REPO_TOKEN`, `GH_TOKEN`, or `GITHUB_TOKEN`.

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

## bb remote

`bb remote` runs Bazel commands or scripts on a remote runner, automatically mirroring your local git state.

```bash
bb remote build //foo:bar
bb remote --script='bb agent fix <INVOCATION_ID>'
```

See [Remote Bazel](/docs/remote-bazel) for full documentation.

## bb view

`bb view` fetches and prints the build logs for an invocation. It accepts an invocation ID or invocation URL.

#### Usage

```bash
# Print the full build logs.
bb view <INVOCATION_ID>
bb view https://app.buildbuddy.io/invocation/<INVOCATION_ID>

# Print only the first build error.
bb view <INVOCATION_ID> --errors

# Print the output of failed test cases for a target.
bb view <INVOCATION_ID> //foo:bar_test

# Print the output of failed test cases matching a test filter.
bb view <INVOCATION_ID> //foo:bar_test --test_filter=TestBaz
```

`--errors` cannot be combined with targets or `--test_filter`.

Use `--lines` to set the minimum number of log lines to fetch (default `100000`).
