---
id: workflows-config
title: Workflows configuration
sidebar_label: Workflows configuration
---

Once you've linked your repo to BuildBuddy via
[BuildBuddy workflows](workflows-setup.md), there are two ways to start running Workflows.

The default workflow config runs `bazel test //...` whenever a commit is pushed to your repo's default branch or a pull request branch is updated. In order to enable this, click "Enable default workflow config"
in the three-dot dropdown for your repository on the Workflows page.

You may wish to configure multiple test commands with different test
tag filters, or run the same tests on multiple different platform
configurations (running some tests on Linux, and some on macOS, for
example).

This page describes how to configure your workflows beyond the default
configuration.

## Configuring workflow actions and triggers

BuildBuddy workflows can be configured using a file called
`buildbuddy.yaml`, which can be placed at the root of your git repo.

`buildbuddy.yaml` consists of multiple **actions**. Each action describes
a list of commands to be run in order, as well as the set of git
events that should trigger these commands.

:::note

The configuration in `buildbuddy.yaml` only takes effect after you
[enable workflows for the repo](workflows-setup#enable-workflows-for-a-repo).

:::

### Example config

You can copy this example config as a starting point for your own `buildbuddy.yaml`:

```yaml title="buildbuddy.yaml"
actions:
  - name: "Test all targets"
    triggers:
      push:
        branches:
          - "main" # <-- replace "main" with your main branch name
      pull_request:
        branches:
          - "*"
    steps:
      - run: "bazel test //..."
```

This config is equivalent to the default config that we use if you
do not have a `buildbuddy.yaml` file at the root of your repo.

### Running bash commands

Each step can run arbitrary bash code, which may be useful for running Bazel commands
conditionally, or for installing system dependencies
that aren't available in BuildBuddy's available workflow images.

Because workflows are run in [snapshotted microVMs](./rbe-microvms), system
dependencies will be persisted across workflow runs. However, we recommend
fetching dependencies with Bazel whenever possible, rather than relying
on system dependencies.

To specify multiple bash commands, you can either specify a block of bash code within a single step:

```yaml title="buildbuddy.yaml"
# ...
steps:
  - run: |
      sudo apt-get update && sudo apt-get install -y my-lib
      bazel test //...
```

Or you can specify one command per step. Note that each step is run in a separate
bash process, so locally initialized variables will not persist across steps:

```yaml title="buildbuddy.yaml"
# ...
steps:
  - run: sudo apt-get update && sudo apt-get install -y my-lib
  - run: bazel test //...
```

## Concurrent Workflow runs

Starting June 1, 2026, BuildBuddy will automatically cancel in-progress
Workflow runs when a newer run is triggered for the same action on a
non-default branch. This helps avoid wasting resources on outdated runs
when, for example, several commits are pushed in quick succession to a
pull request branch.

This behavior only applies to non-default branches. Runs on your repo's
default branch are not affected.

If you'd like to disable this behavior and allow concurrent runs for an
action, set `allow_concurrent_runs: true` in the action's configuration:

```yaml title="buildbuddy.yaml"
actions:
  - name: "Test all targets"
    allow_concurrent_runs: true # <-- disables auto-cancellation of concurrent runs
    ...
```

## Bazel configuration

### Bazel version

BuildBuddy runs each bazel command in your workflow with a
[bazelisk](https://github.com/bazelbuild/bazelisk)-compatible wrapper so
that your `.bazelversion` file is respected.

If `.bazelversion` is missing, the latest version of Bazel is used. We
always recommend including a `.bazelversion` in your repo to prevent
problems caused by using conflicting versions of Bazel in different build
environments.

### bazelrc

BuildBuddy runs each bazel command directly in your workspace, which means
that your `.bazelrc` is respected. If you have lots of flags, we recommend
adding them to your `.bazelrc` instead of adding them to your `buildbuddy.yaml`.

BuildBuddy also provides a [`bazelrc`](https://bazel.build/docs/bazelrc)
file which passes these default options to each bazel invocation listed in
`steps`:

- `--bes_backend` and `--bes_results_url`, so that the results from each
  Bazel command are viewable with BuildBuddy
- `--remote_header=x-buildbuddy-api-key=YOUR_API_KEY`, so that invocations
  are authenticated by default
- `--build_metadata=ROLE=CI`, so that workflow invocations are tagged as
  CI invocations, and so that workflow tests are viewable in the test grid

BuildBuddy's `bazelrc` takes lower precedence than your workspace
`.bazelrc`. You can view the exact flags provided by this bazelrc by
inspecting the command line details in the invocation page (look for
`buildbuddy.bazelrc`).

:::note

BuildBuddy remote cache and remote execution (RBE) are not enabled by
default for workflows, and require additional configuration. The
configuration steps are the same as when running Bazel locally. See the
**Quickstart** page in the BuildBuddy UI.

:::

### Secrets

Trusted workflow executions can access [secrets](secrets) using
environment variables.

For example, if we have a secret named `REGISTRY_TOKEN` and we want to set
the remote header `x-buildbuddy-platform.container-registry-password` to
the value of that secret, we can get the secret value using
`$REGISTRY_TOKEN`, as in the following example:

```yaml title="buildbuddy.yaml"
# ...
steps:
  - run: "bazel test ... --remote_exec_header=x-buildbuddy-platform.container-registry-password=$REGISTRY_TOKEN"
```

To access the environment variables within `build` or `test` actions, you
may need to explicitly expose the environment variable to the actions by
using a bazel flag like
[`--action_env`](https://bazel.build/reference/command-line-reference#flag--action_env)
or
[`--test_env`](https://bazel.build/reference/command-line-reference#flag--test_env):

```yaml title="buildbuddy.yaml"
# ...
steps:
  - run: "bazel test ... --test_env=REGISTRY_TOKEN"
```

## Scheduling workflows with cron expressions

BuildBuddy supports running Workflows on a recurring schedule using
standard cron expressions. This is useful for nightly builds, periodic
integration tests, or any job that should run independently of code pushes
or pull requests.

To schedule an action, add a `schedule` trigger with one or more cron
expressions under `triggers`:

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

You can specify multiple cron expressions to run an action at different
intervals:

```yaml title="buildbuddy.yaml"
actions:
  - name: "Frequent integration tests"
    triggers:
      schedule:
        crons:
          - "0 * * * *" # top of every hour
          - "30 * * * *" # middle of every hour
    steps:
      - run: "bazel test //integration/..."
```

Scheduled runs always execute against the latest commit on your repo's
default branch.

:::note

The minimum supported interval between cron triggers is 15 minutes.

:::

### Cron expression format

BuildBuddy uses standard 5-field cron syntax:

```
┌─────────── minute (0–59)
│ ┌───────── hour (0–23)
│ │ ┌─────── day of month (1–31)
│ │ │ ┌───── month (1–12)
│ │ │ │ ┌─── day of week (0–6, Sunday = 0)
│ │ │ │ │
* * * * *
```

Common examples:

| Expression     | Meaning                               |
| -------------- | ------------------------------------- |
| `0 * * * *`    | Every hour, on the hour               |
| `0 2 * * *`    | Daily at 2:00 AM UTC                  |
| `30 6 * * 1-5` | 6:30 AM UTC, Monday–Friday            |
| `0 0 1 * *`    | Midnight UTC on the 1st of each month |
| `0 */6 * * *`  | Every 6 hours                         |

All times are interpreted as UTC.

## Merge queue support

BuildBuddy workflows are compatible with GitHub's [merge queues](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/configuring-pull-request-merges/managing-a-merge-queue).

To ensure that workflows are run as part of merge queue CI, configure
a push trigger that runs whenever GitHub pushes its temporary merge queue
branch, as described in [Triggering merge group checks with third-party CI providers](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/configuring-pull-request-merges/managing-a-merge-queue).

Example `buildbuddy.yaml` file:

```yaml title="buildbuddy.yaml"
actions:
  - name: Test
    triggers:
      push:
        # Run when a merge queue branch is pushed or the main branch is
        # pushed.
        branches: ["main", "gh-readonly-queue/*"]
    # ...
```

## Merge with base

By default, when workflows are triggered by `pull_request` events, the CI runner will merge
the PR branch with the PR's base branch (e.g. main). This is controlled by
`merge_with_base`, which defaults to `true`.

This may help catch integration problems before merge,
such as merge conflicts or tests that only fail when the PR is combined
with recent changes on the base branch.

```yaml title="buildbuddy.yaml"
actions:
  - name: "Test"
    triggers:
      pull_request:
        branches: ["main"]
        merge_with_base: true # default
    steps:
      - run: "bazel test //..."
```

By default, the PR is merged with the current base branch tip. This may hurt CI
performance if the base branch tip advances frequently. Merging in
unrelated base branch changes may invalidate the Bazel cache and require
rebuilds, even when the PR branch itself is unchanged. This behavior can be
disabled by setting `merge_with_base: false`, or made less frequent by setting
`merge_with_base_interval`.

### Merge with base interval

When `merge_with_base_interval` is set, the CI runner merges with the oldest base branch commit within the interval,
rather than the base branch tip.

The intention is that multiple CI runs within an interval will all merge with the same commit, even
if the base branch tip has advanced. Repeated runs of an unchanged PR will produce the same merged result and hit a warm Bazel cache.

The runner will still merge with the base branch tip once per interval, keeping the PR reasonably up to date with the base branch to catch integration issues.

If the PR's merge base is already newer than the compute commit, the merge is skipped.
And if the interval contains no base branch commits, it falls back to the tip.

For example, with `merge_with_base_interval: "3h"`, the base advances at 00:00, 03:00,
06:00, ... UTC, and all runs within the same 3h window merge with the first base
branch commit after the window boundary:

- The interval boundary is [09:00 UTC, 12:00 UTC].
- Main is pushed at 08:00 UTC (commit `a`). This commit is outside the current interval.
- Main is pushed at 09:05 UTC (commit `b`). This commit is the oldest commit in the current interval.
- Main is pushed at 10:00 UTC (commit `c`). This commit is in the current interval, but is not the oldest commit in the interval.
- Main is pushed at 12:05 UTC (commit `d`). This commit is in the next interval.
- A PR Workflow runs at 09:30 UTC. It merges with the oldest base branch commit in the current interval (commit `b`).
- A PR Workflow runs at 10:45 UTC. Even though Main has advanced to commit `c`, it merges with the oldest base branch commit in the current interval (commit `c`).
  This lets it reuse the cache from the earlier PR run.
- A run at 12:15 UTC is in the next interval, so it merges with commit `d`.
  This helps keep the PR reasonably up to date with the base branch.

With a shorter interval like `1h`, the base advances at the start of each hour.

`merge_with_base_interval` is capped at `3h` so that PRs are not merged with an overly
stale base, which would also have the negative side effect of keeping stale
artifacts in the cache longer. Use a shorter interval when merge confidence is
more important, and a longer interval when cache stability and reduced CI churn
are more important.

### Interaction with merge queues

Merge with base is still useful when your repo uses merge queues. Merging with base on
`pull_request` workflows can catch PR-specific issues earlier, before the PR
reaches the merge queue.

Workflows that run on merge queue branches themselves
do not need `merge_with_base`, since the merge queue commit should already be
merged with the base branch.

Also, merge queue workflows can only be triggered on `push` events, where `merge_with_base` is not supported.

## Ref pattern matching

In `buildbuddy.yaml`, workflow triggers such as `push` and `pull_request`
are configured using a list of patterns that are matched against
the branch or tag name from the repository event.

Ref patterns are evaluated using the following rules:

- Patterns may contain a single wildcard character (`*`) which matches any
  sequence of characters. For example, the branch pattern
  `"release-*-linux"` results in a positive match for the branch
  `"release-v1.2.3-linux"`. Note: if there is more than one wildcard, only
  the first one is expanded, and subsequent wildcards are treated
  literally.
- Patterns starting with an exclamation mark (`!`) are _negated_ patterns
  and result in a negative match if the rest of the pattern after the
  exclamation mark is matched. For example, the branch pattern `"!main"`
  results in a negative match for the branch `"main"`.
- All characters other than negation flags or wildcards are matched
  exactly. For example, the branch pattern `"main"` results in a positive
  match for the branch `"main"`.
- If multiple patterns are specified, then the last matching pattern
  (positive or negative) determines whether the branch is matched. For
  example, given the list of branch patterns
  `["*", "!release-*", "release-special"]`, matching against the branch
  name `"release-20210101"` results in a positive match for `"*"`, then a
  negative match for `"!release-*"`, then a non-match for
  `"release-special"`. The last matching pattern is `"!release-*"`, which
  is a negative match, so the workflow is not triggered.

## Linux image configuration

By default, workflows run on an Ubuntu 18.04-based image. You can
customize the image using the `container_image` action setting:

```yaml title="buildbuddy.yaml"
actions:
  - name: "Test all targets"
    container_image: "ubuntu-24.04" # <-- add this line
    steps:
      - run: "bazel test //..."
```

The supported values for `container_image` are:

- `"ubuntu-18.04"` (the default)
- `"ubuntu-20.04"`
- `"ubuntu-22.04"`
- `"ubuntu-24.04"`

These images are aliases for BuildBuddy's official Ubuntu-based CI images.

### Installing custom software

If BuildBuddy's official Ubuntu images do not contain the software that
you need, you can install custom software using `apt-get` at runtime.

Because workflow VMs are snapshotted and reused between runs, you can
speed up workflows by skipping `apt-get install` if the package is already
installed.

Example:

```yaml title="buildbuddy.yaml"
actions:
  - name: Test
    steps:
      # Ensure apt packages are installed ("libexample0" in this example)
      # Note: workflow VMs are snapshotted and reused, so normally,
      # the apt-get install step only needs to run once.
      - run: |
          if ! [ -e /usr/lib/libexample.so.0 ] ; do
            # libexample0 is not installed; install it:
            sudo apt-get update && sudo apt-get install -y libexample0
          fi
      - run: |
          bazel test //some/target/that/needs:libexample
```

If you have requirements that prevent you from using one of the official
Ubuntu images, please [contact us](https://buildbuddy.io/contact).

## Linux resource configuration

By default, Linux workflow VMs have the following resources available:

- 3 CPU
- 8 GB of RAM
- 20 GB of disk space

These values are configurable using [resource requests](#resourcerequests).

## Mac configuration

By default, workflows will execute on BuildBuddy's Linux executors,
but it is also possible to run workflows on macOS by using self-hosted
executors.

1. Set up one or more Mac executors that will be dedicated to running
   workflows, following the steps in the [Enterprise
   Mac RBE Setup](/docs/enterprise-mac-rbe) guide.

   Then, in your `buildbuddy-executor.plist` file, find the
   `EnvironmentVariables` section and set `MY_POOL` to `workflows`. You'll
   also need to set `SYS_MEMORY_BYTES` to allow enough memory to be
   used for workflows (a minimum of 8GB is required).

```xml title="buildbuddy-executor.plist"
        ...
        <key>EnvironmentVariables</key>
        <dict>
            ...
            <!-- Set the required executor pool name for workflows -->
            <key>MY_POOL</key>
            <string>workflows</string>
            <!-- Allocate 16GB of memory to workflows (8GB minimum) -->
            <key>SYS_MEMORY_BYTES</key>
            <string>16000000000</string>
        </dict>
        ...
```

2. If you haven't already, [enable workflows for your
   repo](/docs/workflows-setup#enable-workflows-for-a-repo), then create a
   file called `buildbuddy.yaml` at the root of your repo. See the
   [Example config](#example-config) for a starting point.

3. Set `os: "darwin"` on the workflow action that you would like to build
   on macOS. For Apple silicon (ARM-based) Macs, add `arch: "arm64"` as
   well. Note: if you copy another action as a starting point, be sure to
   give the new action a unique name:

```yaml title="buildbuddy.yaml"
actions:
  - name: "Test all targets (Mac)"
    os: "darwin" # <-- add this line
    arch: "arm64" # <-- add this line for Apple silicon (ARM-based) Macs only
    triggers:
      push:
        branches:
          - "main"
      pull_request:
        branches:
          - "*"
    steps:
      - run: "bazel test //..."
```

That's it! Whenever any of the configured triggers are matched, one of
the Mac executors in the `workflows` pool should execute the
workflow, and BuildBuddy will publish the results to your branch.

## Troubleshooting

### Unexpected cache misses

If seeing unexpected cache misses from multiple runs of the same Workflow,
check whether [merge with base](#merge-with-base) is pulling in frequent changes from the
base branch. Even when the PR branch is unchanged, a new base branch tip can
produce a different merged base, which can invalidate the Bazel cache
and require rebuilds. See the [merge with base section](#merge-with-base) for more detail.

If so, consider setting `merge_with_base_interval`.

## buildbuddy.yaml schema

### `BuildBuddyConfig`

The top-level BuildBuddy workflow config, which specifies bazel commands
that can be run on a repo, as well as the events that trigger those commands.

**Fields:**

- **`actions`** ([`Action`](#action) list): List of actions that can be triggered by BuildBuddy.
  Each action corresponds to a separate check on GitHub.
  If multiple actions are matched for a given event, the actions are run in
  order. If an action fails, subsequent actions will still be executed.

### `Action`

A named group of Bazel commands that run when triggered.

**Fields:**

- **`name`** (`string`): A name unique to this config, which shows up as the name of the check
  in GitHub.
- **`triggers`** ([`Triggers`](#triggers)): The triggers that should cause this action to be run.
- **`os`** (`string`): The operating system on which to run the workflow.
  Defaults to `"linux"`. `"darwin"` (macOS) is also supported, but
  requires using self-hosted Mac executors running on a dedicated
  `workflows` pool.
- **`arch`** (`string`): The CPU architecture of the workflow runner.
  Defaults to `"amd64"`. `"arm64"` is also supported when running under
  `os: "darwin"`, but requires using self-hosted Apple silicon (ARM-based)
  Mac executors running on a dedicated `workflows` pool.
- **`pool`** (`string`): The executor pool name for running workflows.
  This option has no effect unless `self_hosted: true` is also specified.
- **`self_hosted`** (`boolean`): Whether to run the workflow on
  self-hosted executors. The executor's default isolation type will be
  used to run workflows. Unless `pool` is also specified, the configured
  pool name for the self-hosted workflow executors must be `"workflows"`.
  This option is ignored for macOS workflows, since macOS workflows are
  always required to be self-hosted.
- **`container_image`** (`string`): The Linux container image to use
  (has no effect for Mac workflows). Supported values are `"ubuntu-18.04"`
  and `"ubuntu-20.04"`. Defaults to `"ubuntu-18.04"`.
- **`resource_requests`** ([`ResourceRequests`](#resourcerequests)):
  the requested resources for this action.
- **`user`** (`string`): User to run the workflow as. This can be set to
  `"root"` to run the workflow as root, but it is recommended to keep the
  default value, which is a non-root user provisioned in the CI
  environment (usually named `"buildbuddy"`). Note: some legacy workflows
  might still have `"root"` as the default user, but we are in the process
  of migrating all users to non-root by default.
- **`env`** (`map` with string values): Map of static environment variables and their values.
- **`git_fetch_filters`** (`string` list): list of [`--filter` option](https://git-scm.com/docs/git-clone#Documentation/git-clone.txt-code--filtercodeemltfilter-specgtem)
  values to the `git fetch` command used when fetching the git commits
  to build. Defaults to `["blob:none"]`.
- **`git_fetch_depth`** (`int`): [`--depth` option](https://git-scm.com/docs/git-fetch#Documentation/git-fetch.txt---depthltdepthgt) value used when
  fetching the git commits to build. When using this option in combination
  with a `pull_request` trigger, it's recommended to set
  `merge_with_base: false` in the `pull_request` trigger, since the
  limited fetch depth might prevent the merge-base commit from being
  fetched. Defaults to `0` (unset).
- **`git_clean_exclude`** (`string` list): List of directories within the
  workspace that are excluded when running `git clean` across actions that
  are executed in the same runner instance. This is an advanced option and
  is not recommended for most users.
- **`bazel_workspace_dir`** (`string`): A subdirectory within the repo
  containing the bazel workspace for this action. By default, this is
  assumed to be the repo root directory.
- **`steps`** (list): Bash commands to be run in order.
  If a command fails, subsequent ones are not run, and the action is
  reported as failed. Otherwise, the action is reported as succeeded.
  Environment variables are expanded, which means that the commands
  can reference [secrets](secrets.md) if the workflow execution
  is trusted.
- **`timeout`** (`duration` string, e.g. '30m', '1h'): If set, workflow actions that have been
  running for longer than this duration will be canceled automatically. This
  only applies to a single invocation, and does not include multiple retry attempts.
- **`allow_concurrent_runs`** (`boolean`, default: `false`): If set to `true`,
  multiple runs of the same action on the same branch will be allowed to run concurrently.
  By default or if set to `false`, concurrent runs will be automatically cancelled.
  See [Concurrent Workflow runs](#concurrent-workflow-runs).

### `Triggers`

Defines whether an action should run when a branch is pushed to the repo.

**Fields:**

- **`push`** ([`PushTrigger`](#pushtrigger)): Configuration for push events associated with the repo.
  This is mostly useful for reporting commit statuses that show up on the
  home page of the repo.
- **`pull_request`** ([`PullRequestTrigger`](#pullrequesttrigger)):
  Configuration for pull request events associated with the repo.
  This is required if you want to use BuildBuddy to report the status of
  this action on pull requests, and optionally prevent pull requests from
  being merged if the action fails.
- **`schedule`** ([`ScheduleTrigger`](#scheduletrigger)):
  Configuration for running the action on a recurring schedule. The
  action runs against the latest commit on the repo's default branch.

### `PushTrigger`

Defines whether an action should execute when a branch is pushed.

**Fields:**

- **`branches`** (`string` list): The branch patterns that determine
  whether a push to the branch will trigger the workflow. Patterns are
  matched using the rules described in
  [Ref pattern matching](#ref-pattern-matching)
- **`tags`** (`string` list): The tag patterns that determine
  whether a push to the tag will trigger the workflow. Patterns are
  matched using the rules described in
  [Ref pattern matching](#ref-pattern-matching)

### `PullRequestTrigger`

Defines whether an action should execute when a pull request (PR) branch is
pushed.

**Fields:**

- **`branches`** (`string` list): The branch patterns that determine whether an
  update to the pull request will trigger the workflow. The _base_ branch of the
  PR is matched against this list. For example, if this is set to
  `[ "main", "release/*" ]`, then the associated action is only run when a PR
  wants to merge a branch _into_ the `main` branch or any branch prefixed with
  `release/`. Branch patterns are matched using the rules described in
  [Ref pattern matching](#ref-pattern-matching)
- **`types`** (`string` list): The pull request actions that trigger the
  workflow. The supported values are `opened`, `synchronize`, `reopened`,
  `edited` (base-branch changes only), and `ready_for_review`. If unset, the
  trigger fires on `opened`, `synchronize`, `reopened`, and base-branch edits.
  Set this to scope the workflow to specific pull_request actions — for example
  `[ "ready_for_review" ]` to run only when a draft PR is marked ready for
  review.
- **`merge_with_base`** (`boolean`, default: `true`): Whether to merge the
  base branch into the PR branch before running the workflow action. This
  can help ensure that the changes in the PR branch do not conflict with
  the main branch. However, the action will not be continuously re-run as
  changes are pushed to the base branch. For stronger protection against
  breaking the main branch, you may wish to use
  [merge queues](#merge-queue-support). See
  [Merge with base behavior](#merge-with-base).
- **`merge_with_base_interval`** (`duration`, optional): If set with
  `merge_with_base: true`, merge with the oldest base branch commit in the current interval (UTC)
  instead of the base branch tip, improving cache stability across runs.
  Capped at `3h`. See [Merge with base interval](#merge-with-base-interval) for more details.

### `ScheduleTrigger`

Defines a recurring schedule for an action using cron expressions.

**Fields:**

- **`crons`** (`string` list): One or more 5-field cron expressions
  that define when the action should run. All times are UTC. The minimum
  supported interval between triggers is 15 minutes. See
  [Scheduling workflows with cron expressions](#scheduling-workflows-with-cron-expressions)
  for format details and examples.

### `ResourceRequests`

Defines the requested resources for a workflow action.

**Fields:**

- **`memory`** (`string` or `number`): Requested amount of memory for the
  workflow action. Can be specified as an exact number of bytes, or a
  numeric string containing an IEC unit abbreviation. For example: `8GB`
  represents `8 * (1024)^3` bytes of memory.
- **`cpu`** (`string` or `number`): Requested amount of CPU for the
  workflow action. Can be specified as a number of CPU cores, or a numeric
  string containing an `m` suffix to represent thousandths of a CPU core.
  For example: `8000m` represents 8 CPU cores.
- **`disk`** (`string` or `number`): Requested amount of disk space for the
  workflow action. Can be specified as a number of bytes, or a numeric
  string containing an IEC unit abbreviation. For example: `8GB` represents
  `8 * (1024)^3` bytes of disk space.
