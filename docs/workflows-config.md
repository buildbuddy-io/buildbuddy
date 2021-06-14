---
id: workflows-config
title: Workflows configuration
sidebar_label: Workflows configuration
---

This page provides documentation for `buildbuddy.yaml`, which can be placed
at the root of your git repo to configure BuildBuddy workflow execution.

:::note

The configuration in `buildbuddy.yaml` only takes effect after you
[enable workflows for the repo](workflows-setup#enable-workflows-for-a-repo).

:::

## Example config

You can copy this example config as a starting point for your own `buildbuddy.yaml`:

```yaml
actions:
  - name: "Test all targets"
    triggers:
      push:
        branches:
          - "main"
      pull_request:
        branches:
          - "main"
    bazel_commands:
      - "test //... --build_metadata=ROLE=CI --bes_backend=grpcs://cloud.buildbuddy.io --bes_results_url=https://app.buildbuddy.io/invocation/"
```

This config is equivalent to the default config that we use if you do
not have a `buildbuddy.yaml`, with some exceptions:

- This example uses `"main"` for the branch name -- if you copy this config,
  be sure to replace that with the name of your main branch. By default, we
  run the above bazel command when any branch is pushed.
- By default, we also pass `--remote_header=x-buildbuddy-api-key=<YOUR_API_KEY>`,
  so that workflow builds show up in your BuildBuddy org. For security reasons,
  we only do this if your repo is private.

Other points to note:

- Remote cache and remote execution (RBE) require additional configuration.
  The configuration steps are the same as when running Bazel locally.
  See the **Setup** page in the BuildBuddy UI.
- Bazel commands are run directly in your workspace, which means that your
  `.bazelrc` is respected. If you have lots of flags, we recommend adding
  them to your `.bazelrc` instead of adding them in this YAML config.
- We run your commands with a [bazelisk](https://github.com/bazelbuild/bazelisk)-compatible
  wrapper so that your `.bazelversion` file is respected. If
  `.bazelversion` is missing, the latest version of Bazel is used. We
  always recommend including a `.bazelversion` in your repo to prevent
  problems caused by using conflicting versions of Bazel in different
  build environments.

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
- **`bazel_commands`** (`string` list): Bazel commands to be run in order.
  If a command fails, subsequent ones are not run, and the action is
  reported as failed. Otherwise, the action is reported as succeeded.

### `Triggers`

Defines whether an action should run when a branch is pushed to the repo.

**Fields:**

- **`push`** ([`PushTrigger`](#push-trigger)): Configuration for push events associated with the repo.
  This is mostly useful for reporting commit statuses that show up on the
  home page of the repo.
- **`pull_request`** ([`PullRequestTrigger`](#pull-request-trigger)):
  Configuration for pull request events associated with the repo.
  This is required if you want to use BuildBuddy to report the status of
  this action on pull requests, and optionally prevent pull requests from
  being merged if the action fails.

### `PushTrigger`

Defines whether an action should execute when a branch is pushed.

**Fields:**

- **`branches`** (`string` list): The branches that, when pushed to, will trigger the action.

### `PullRequestTrigger`

Defines whether an action should execute when a pull request (PR) branch is
pushed.

**Fields:**

- **`branches`** (`string` list): The _target_ branches of a pull request.
  For example, if this is set to `[ "v1", "v2" ]`, then the
  associated action is only run when a PR wants to merge a branch _into_
  the `v1` branch or the `v2` branch.
