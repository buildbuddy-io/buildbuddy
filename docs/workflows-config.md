---
id: workflows-config
title: Workflows configuration
sidebar_label: Workflows configuration
---

<!--

GENERATED FILE; DO NOT EDIT.

Edit workflows-config-header.md to edit the header contents
or update workflow/config/generate_docs.py to change how the
schema documentation is displayed.

Re-generate by running:

python3 workflow/config/generate_docs.py

-->

This page provides documentation for `buildbuddy.yaml`, which can be placed
at the root of your git repo to configure BuildBuddy [workflow](workflows) execution.

_NOTE: This file only takes effect after you
[enable workflows for the repo](workflows#enable-workflows-for-a-repo)._

## Default / example config

The following config is roughly equivalent to the one we use if you don't
have a `buildbuddy.yaml` in your repo.

You can copy this as a starting point for your own `buildbuddy.yaml`:

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
      - "bazel test //... --build_metadata=ROLE=CI --bes_backend=grpcs://cloud.buildbuddy.io --bes_results_url=https://app.buildbuddy.io/invocation/"
```

A few points to note:

- This example uses `"main"` for the branch name -- if you copy this config,
  be sure to replace that with the name of your main branch. By default, we
  run the above bazel command when any branch is pushed.
- By default, we also pass `--remote_header=x-buildbuddy-api-key=<YOUR_API_KEY>`,
  so that workflow builds show up in your BuildBuddy org. For security reasons,
  we only do this if your repo is private.
- Remote cache and remote execution (RBE) require additional configuration.
  See the **Setup** page in the BuildBuddy UI.
- Bazel commands are run directly in your workspace, which means that your
  `.bazelrc` is respected. If you have lots of flags, we recommend adding
  them to your `.bazelrc` instead of adding them in this YAML config.

## `BuildBuddyConfig` {#buildbuddy-config}

The top-level BuildBuddy workflow config, which specifies bazel commands
that can be run on a repo, as well as the events that trigger those commands.


| Field | Type | Description |
| ------|------|-------------|
| `actions` | [`Action`](#action) list | List of actions that can be triggered by BuildBuddy.<br /><br />Each action corresponds to a separate check on GitHub.<br /><br />If multiple actions are matched for a given event, the actions are run in order. If an action fails, subsequent actions will still be executed.  |

## `Action` {#action}

A named group of Bazel commands that run when triggered.


| Field | Type | Description |
| ------|------|-------------|
| `name` | `string` | A name unique to this config, which shows up as the name of the check in GitHub.  |
| `triggers` | [`Triggers`](#triggers) | The triggers that should cause this action to be run.  |
| `bazel_commands` | `string` list | Bazel commands to be run in order.<br /><br />If a command fails, subsequent ones are not run, and the action is reported as failed. Otherwise, the action is reported as succeeded.  |

## `Triggers` {#triggers}

Defines whether an action should run when a branch is pushed to the repo.


| Field | Type | Description |
| ------|------|-------------|
| `push` | [`PushTrigger`](#push-trigger) | Configuration for push events associated with the repo.<br /><br />This is mostly useful for reporting commit statuses that show up on the home page of the repo.  |
| `pull_request` | [`PullRequestTrigger`](#pull-request-trigger) | Configuration for pull request events associated with the repo.<br /><br />This is required if you want to use BuildBuddy to report the status of this action on pull requests, and optionally prevent pull requests from being merged if the action fails.  |

## `PushTrigger` {#push-trigger}

Defines whether an action should execute when a branch is pushed.


| Field | Type | Description |
| ------|------|-------------|
| `branches` | `string` list | The branches that, when pushed to, will trigger the action.  |

## `PullRequestTrigger` {#pull-request-trigger}

Defines whether an action should execute when a pull request (PR) branch is
pushed.


| Field | Type | Description |
| ------|------|-------------|
| `branches` | `string` list | The _target_ branches of a pull request.<br /><br />For example, if this is set to `[ "v1", "v2" ]`, then the associated action is only run when a PR wants to merge a branch _into_ the `v1` branch or the `v2` branch.  |

