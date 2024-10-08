---
id: remote-bazel-introduction
title: Introduction to Remote Bazel
sidebar_label: Remote bazel introduction
---

Remote Bazel is an easily configurable way to run commands on a remote runner. You
can think of it as dynamically spinning up a VM to execute a single command (or multiple
commands, if you'd like!).

This means you don't even need Bazel installed
on your local machine to initiate a Bazel build! Plus, our remote
runners support any bash commands, not just Bazel commands.

## Benefits of Remote Bazel

Remote Bazel makes it easy to configure the OS, architecture, and container image
of the remote runner. This makes it easy to run builds on a specific
platform.

Remote Bazel also has the following performance benefits:

1. Colocation with BuildBuddy servers, ensuring a **fast network
   connection between Bazel and BuildBuddy's RBE & caching servers**.
2. Bazel workspaces are recycled between runs, allowing subsequent runs to
   take advantage of **warm Bazel instances**.

Remote Bazel uses the same backend technology as our CI product, BuildBuddy
Workflows. See [our docs on BuildBuddy Workflows](https://www.buildbuddy.io/docs/workflows-introduction/)
for a more in-depth description of the performance optimizations and mechanism
for workspace recycling.

## Uses for Remote Bazel

### CI

Remote Bazel is powered by the same backend as our
CI product, BuildBuddy Workflows. However, Remote Bazel is a much more flexible
entrypoint to our CI platform.

Workflows must be configured with a config YAML that is checked in to GitHub.
Despite the performance benefits of running on Workflows, many companies have
legacy CI workflows that would be challenging and error-prone
to migrate to a new CI platform.

Remote Bazel is a drop-in solution that can be more easily integrated into pre-existing CI pipelines.
It can be triggered via CURL request or by replacing
`bazel` commands with `bb remote` commands (Ex. `bazel test //...` => `bb remote test //...`).

As Remote Bazel commands are dynamically constructed, it is also easier to pass
in short-lived credentials or to run a wider range of commands, because they don't
have to be codified in a YAML.

### Developer day-to-day

Remote Bazel also has a wide range of applications in the day-to-day of developers.
Some ideas are:

**Cross-platform development**

```bash Sample Command
bb remote --os=linux --arch=amd64 test //...
```

Given the ease of configuring a Remote Bazel command, targeting a specific platform
is very easy.

Say you have a Mac laptop, but you need to run some tests that only run on Linux.
Rather than bravely trying to add support for cross-platform builds via Bazel, you
can use Remote Bazel to run the build on a remote Linux runner.

Because the remote logs are streamed back to
your local machine, it will feel like you're directly running the build locally.

Our CLI automatically syncs your local git workspace with the remote runner's, so
this is easy even if you are quickly iterating on code between each build. The CLI
will upload and apply any local diffs to the remote workspace, so any local changes
are reflected in each new build.

Given that the remote runner is only running for the exact duration of the command,
this is much more economical than spinning up a VM that is running 24/7. If you
use an IDE or have custom VIM bindings etc. the automatic git sync also lets you
write code in your optimized local development setup, without having to constantly
push and pull changes from a VM.

**Accessing powerful remote machines**

```bash Sample Command
 bb remote \
      --runner_exec_properties=EstimatedCPU=24 \
      --runner_exec_properties=EstimatedFreeDiskBytes=50GB \
      test //...
```

For developers with a slow network connection or limited resources (CPU, memory, disk)
on their machine, Remote Bazel is a convenient way to run faster builds.

**Running multiple Bazel builds in parallel**

Remote Bazel lets you spin up multiple simultaneous builds without conflict,
and without utilizing all the resources on your machine.

For example, when debugging a flaky test, you might run the test with `--runs_per_test=100` to
root out the flake. While that slow build is running, you might want to work on
something else. Remote Bazel lets you run these workloads simultaneously.

**Running builds from our UI (in Alpha)**

Now that we can initiate a Bazel build via CURL request, even a web browser can
run builds. We've added several useful Remote Bazel backed UI features to solve common
customer pain points.

_Why did this seemingly unrelated target build?_

From an invocation link, you can run a `bazel query` to visualize the dependency
graph between two targets.

_What invalidated the cache between these builds?_

From an invocation link, you can initiate a Remote Bazel run that compares
the compact execution logs of two invocations, to determine the root cause of what
changed and what it invalidated.

_What is the code coverage of this test?_

From an invocation link for a test run, you can run a `bazel coverage` to see
stats on code coverage and a highlighted view of which code paths are untested.

**Debug a flaky test that only fails on CI, or a specific platform**

**Ensure a consistent execution environment between multiple developers**

While these are some ideas we've had,
the magic of Remote Bazel is its flexibility! We're sure many of our customers will
discover creative ways to use it.

## Getting started

You can invoke Remote Bazel with the BuildBuddy CLI or by CURL request.

### Using the CLI

1. Download the bb CLI: https://www.buildbuddy.io/cli/
2. If you have already installed it, make sure it's up-to-date with `bb update`
3. From a local git repo, trigger a remote run with `bb remote <bazel command>`
   - Ex. `bb remote build //...` `bb remote test //...`
4. You can configure the remote run with flags between `remote` and the bazel command
   - See `Configuring the remote runner` below for more details

#### Automatic git state mirroring

In order to facilitate convenient local development, the CLI will automatically
upload any local git diffs to the remote runner. This ensures that the remote git
workspace matches your local one. This is helpful if you are quickly iterating on code changes, and
want the changes to be reflected on the remote runner without having to push and
pull changes from GitHub.

If you wish to disable git mirroring and want the remote runner to run from a specific
git ref, you can use `--run_from_branch` or `--run_from_commit`.

```bash
bb remote --run_from_branch=my_remote_branch build //...
```

#### Configuring the remote runner

In order to configure the remote runner, you can add the following flags between
`remote` and the bazel command.

```bash
bb remote --os=linux --arch=amd64 build //...
```

The following configuration options are supported:

- `--os`: The operating system of the remote runner. `linux` is supported by default.
  `darwin` is supported with self-hosted Mac executors.
- `--arch`: The CPU architecture of the remote runner. `amd64` is supported by default.
  `arm64` is supported with self-hosted executors.
- `--container_image`: The Linux container image to use. Has no effect on Mac runners.
- `--env`: Environment variables to set on the remote runner.
  - Ex. `--env=K1=V1 --env=K2=V2`
- `--runner_exec_properties`: Platform properties to configure the remote runner.
  - Ex. To run on a self-hosted executor pool, you could use
    `--runner_exec_properties=use-self-hosted-executors=true --runner_exec_properties=Pool=custom-pool`
- `--timeout` (Ex. '30m', '1h'): If set, remote runs that have been running for longer
  than this duration will be canceled automatically. This only applies to a single attempt,
  and does not include multiple retry attempts.
- `--run_from_branch` `--run_from_commit`: If either of these is set, the remote runner
  will run off the specified GitHub ref. By default if neither is set, the remote GitHub workspace
  will mirror the local state (including any non-committed local diffs).

In order to run the CLI with debug logs enabled, you can add `--verbose=1` between
`bb` and `remote`. Note that this is a different syntax from the rest of the
Remote Bazel flags, which go after `remote`.

```bash
bb --verbose=1 remote build //...
```

### CURL request

See the API definition [here](enterprise-api.md).

Sample CURL request:

```bash
curl -d '{
    "repo”: "git@github.com:buildbuddy-io/buildbuddy.git",
    "branch":"main",
    "steps": [{"run": "bazel test //..."}]
}' \
-H "x-buildbuddy-api-key: YOUR_BUILDBUDDY_API_KEY" \
-H 'Content-Type: application/json' \
https://app.buildbuddy.io/api/v1/Run
```

### Private GitHub repos

If your GitHub repo is private, you must first link it at https://app.buildbuddy.io/workflows/
to authorize the remote runner to access it.

### GitHub Enterprise

In order to use Remote Bazel with GitHub Enterprise, you must set `--use_system_git_credentials`
and use self-hosted executors that are configured with SSH credentials to access your repo.

```bash
bb remote \
  --runner_exec_properties=use-self-hosted-executors=true \
  --runner_exec_properties=Pool=custom-pool \
  --use_system_git_credentials=1 \
  build //...
```

As the remote runners will use this SSH configuration to access your repo,
containerization is not supported.
