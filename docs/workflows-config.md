---
id: workflows-config
title: Workflows configuration
sidebar_label: Workflows configuration
---

Once you've linked your repo to BuildBuddy via
[BuildBuddy workflows](workflows-setup.md), BuildBuddy will automatically
run `bazel test //...` on each push to your repo, reporting results to the
BuildBuddy UI.

But you may wish to configure multiple test commands with different test
tag filters, or run the same tests on multiple different platform
configurations (running some tests on Linux, and some on macOS, for
example).

This page describes how to configure your workflows beyond the default
configuration.

## Configuring workflow actions and triggers

BuildBuddy workflows can be configured using a file called
`buildbuddy.yaml`, which can be placed at the root of your git repo.

`buildbuddy.yaml` consists of multiple **actions**. Each action describes
a list of bazel commands to be run in order, as well as the set of git
events that should trigger these commands.

:::note

The configuration in `buildbuddy.yaml` only takes effect after you
[enable workflows for the repo](workflows-setup#enable-workflows-for-a-repo).

:::

### Example config

You can copy this example config as a starting point for your own `buildbuddy.yaml`:

```yaml
actions:
  - name: "Test all targets"
    triggers:
      push:
        branches:
          - "main" # <-- replace "main" with your main branch name
      pull_request:
        branches:
          - "*"
    bazel_commands:
      - "test //..."
```

This config is roughly equivalent to the default config that we use if you
do not have a `buildbuddy.yaml`.

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
`bazel_commands`:

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

Environment variables are expanded inline in the `bazel_commands` list.
For example, if we have a secret named `REGISTRY_TOKEN` and we want to set
the remote header `x-buildbuddy-platform.container-registry-password` to
the value of that secret, we can get the secret value using
`$REGISTRY_TOKEN`, as in the following example:

```yaml
# ...
bazel_commands:
  - "test ... --remote_header=x-buildbuddy-platform.container-registry-password=$REGISTRY_TOKEN"
```

To access the environment variables within `build` or `test` actions, you
may need to explicitly expose the environment variable to the actions by
using a bazel flag like
[`--action_env`](https://bazel.build/reference/command-line-reference#flag--action_env)
or
[`--test_env`](https://bazel.build/reference/command-line-reference#flag--test_env):

```yaml
# ...
bazel_commands:
  - "test ... --test_env=REGISTRY_TOKEN"
```

### Dynamic bazel flags

Sometimes, you may wish to set a bazel flag using a shell command. For
example, you might want to set image pull credentials using a command like
`aws` that requests an image pull token on the fly.

To do this, we recommend using a setup script that generates a `bazelrc`
file.

For example, in `/buildbuddy.yaml`, you would write:

```yaml
# ...
bazel_commands:
  - bazel run :generate_ci_bazelrc
  - bazel --bazelrc=ci.bazelrc test //...
```

In `/BUILD`, you'd declare an `sh_binary` target for your setup script:

```python
sh_binary(name = "generate_ci_bazelrc", srcs = ["generate_ci_bazelrc.sh"])
```

Then in `/generate_ci_bazelrc.sh`, you'd generate the `ci.bazelrc` file in
the workspace root (make sure to make this file executable with `chmod +x`):

```shell
#!/usr/bin/env bash
set -e
# Change to the WORKSPACE directory
cd "$BUILD_WORKSPACE_DIRECTORY"
# Run a command to request image pull credentials:
REGISTRY_PASSWORD=$(some-command)
# Write the credentials to ci.bazelrc in the workspace root directory:
echo >ci.bazelrc "
build --remote_header=x-buildbuddy-platform.container-registry-password=${REGISTRY_PASSWORD}
"
```

:::tip

This `generate_ci_bazelrc.sh` script can access workflow secrets using
environment variables.

For Linux workflows, the script can also install system dependencies using
`apt-get` if needed, although if possible we recommend using Bazel to
build or fetch these dependencies.

:::

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

```xml
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

```yaml
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
    bazel_commands:
      - "test //... --bes_backend=remote.buildbuddy.io --bes_results_url=https://app.buildbuddy.io/invocation/"
```

That's it! Whenever any of the configured triggers are matched, one of
the Mac executors in the `workflows` pool should execute the
workflow, and BuildBuddy will publish the results to your branch.

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
- **`resource_requests`** ([`ResourceRequests`](#resourcerequests)):
  the requested resources for this action.
- **`user`** (`string`): User to run the workflow as. For Linux workflows,
  the user `buildbuddy` can be specified here to ensure that the action
  runs as a non-root user, to accomodate certain Bazel actions that refuse
  to run as root (like `rules_hermetic_python`).
- **`git_clean_exclude`** (`string` list): List of directories within the
  workspace that are excluded when running `git clean` across actions that
  are executed in the same runner instance. This is an advanced option and
  is not recommended for most users.
- **`bazel_workspace_dir`** (`string`): A subdirectory within the repo
  containing the bazel workspace for this action. By default, this is
  assumed to be the repo root directory.
- **`bazel_commands`** (`string` list): Bazel commands to be run in order.
  If a command fails, subsequent ones are not run, and the action is
  reported as failed. Otherwise, the action is reported as succeeded.
  Environment variables are expanded, which means that the bazel command
  line can reference [secrets](secrets.md) if the workflow execution
  is trusted.

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

- **`branches`** (`string` list): The branches that, when pushed to, will
  trigger the action. This field accepts a simple wildcard character
  (`"*"`) as a possible value, which will match any branch.

### `PullRequestTrigger`

Defines whether an action should execute when a pull request (PR) branch is
pushed.

**Fields:**

- **`branches`** (`string` list): The _target_ branches of a pull request.
  For example, if this is set to `[ "v1", "v2" ]`, then the associated
  action is only run when a PR wants to merge a branch _into_ the `v1`
  branch or the `v2` branch. This field accepts a simple wildcard
  character (`"*"`) as a possible value, which will match any branch.

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
