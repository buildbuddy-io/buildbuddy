---
id: remote-runner-introduction
title: Introduction to Remote Runners
sidebar_label: Remote Runner Introduction
---

A BuildBuddy remote runner is an execution environment that runs on a BuildBuddy executor.
Our remote runners are optimized to run Bazel commands, and allow users to maintain
a warm bazel instance in a secure execution environment managed by BuildBuddy.
For example, this might look like a Firecracker microVM or an OCI container where
you can run Bazel commands.

BuildBuddy remote runners have the following unique advantages:

1. Colocation with BuildBuddy servers, ensuring a **fast network
   connection** between Bazel and BuildBuddy's cache & RBE servers.
2. Running workloads in **persistent execution environments** using microVM
   snapshotting (on Linux) and persistent runners (on macOS). This allows reusing
   Bazel's in-memory analysis cache and local disk cache, achieving higher performance
   compared to remote caching alone.

There are two ways to use remote runners:

1. **BuildBuddy Workflows:** Our continuous integration (CI) solution that runs Bazel builds and tests in response to git events (pull requests or pushes).
2. **Remote Bazel:** a CLI tool that works exactly like the Bazel command, but runs Bazel on a remote workspace and streams the output back to the local machine.

See [our blog post](https://www.buildbuddy.io/blog/meet-buildbuddy-workflows)
for more details on the motivation behind remote runners as well as some
real-world results.

We currently support two products built on top of remote runners: Workflows and
Remote Bazel.

## Differences between Workflows and Remote Bazel

In many ways, Remote Bazel and Workflows are the same product and share much of
the same backend code. Both are mechanisms to run code on remote runners. The
primary difference is the entrypoint.

#### Workflows

Workflows are configured with a config YAML that is checked in to GitHub.
Remote runs can be automatically triggered by GitHub events, like push and
pull events. Workflows are commonly used as a Continuous Integration (CI) solution.

Workflows are a good fit if you:

- Have a static list of commands to run
- Want your commands checked in to your codebase for review
- Are exclusively using BuildBuddy to run CI and do not have another CI provider
  that can initiate commands

#### Remote Bazel

Remote Bazel can be configured by CURL request or by using the BuildBuddy CLI.
Remote Bazel can be used as a Continuous Integration (CI) solution, or by developers
in their daily workflows for use cases that are more dynamic than CI.

Remote Bazel is a good fit if you:

- Have a dynamic or frequently changing list of commands to run, that you do not
  want to check into your codebase
- You are continuing to use a legacy CI platform and want to integrate BuildBuddy
  into it

## Benefits of remote runners

### Colocation with BuildBuddy servers

Network latency is often the biggest bottleneck in many Bazel Remote Build
Execution and Remote Caching setups. This is because Bazel's remote APIs
require several chained RPCs due to dependencies between actions.

To address this bottleneck, BuildBuddy remote runners are executed in the same
datacenters where BuildBuddy RBE and Cache nodes are deployed. This
results in sub-millisecond round trip times to BuildBuddy's servers,
minimizing the overhead incurred by Bazel's remote APIs.

### Hosted, warm, Bazel instances

Running Bazel on most CI solutions is typically expensive and slow.
There are several sources of overhead:

- When using Bazelisk, Bazel itself is re-downloaded and extracted on each
  CI run.
- The Bazel server starts from a cold JVM, meaning that it will be running
  unoptimized code until the JIT compiler kicks in.
- Bazel's analysis cache starts empty, which often means the entire
  workspace has to be re-scanned on each CI run.
- Any remote repositories referenced by the Bazel workspace all have to be
  re-fetched on each run.
- Bazel's on-disk cache starts completely empty, causing action
  re-execution or excess remote cache usage.

A common solution is to use something like
[actions/cache](https://github.com/actions/cache) to store Bazel's cache
for reuse between runs, but this solution is extremely data-intensive, as
Bazel's cache can be several GB in size and consist of many individual
files which are expensive to unpack from an archive. It also does not
solve the problems associated with the Bazel server having starting from
scratch.

By contrast, BuildBuddy uses a Bazel workspace reuse approach, similar to
how [Google's Build Dequeuing Service](https://dl.acm.org/doi/pdf/10.1145/3395363.3397371) performs
workspace selection:

> A well-chosen workspace can increase the build speed by an
> order of magnitude by reusing the various cached results from the
> previous execution. [...] We have observed that builds that execute the same targets as a previous
> build are effectively no-ops using this technique

## Runner recycling

To match workloads to warm runners, BuildBuddy uses VM
snapshotting powered by
[Firecracker](https://github.com/firecracker-microvm/firecracker) on
Linux, and a simpler runner-recycling based approach on macOS.

### Firecracker VMs (Linux only)

On Linux, remote runs are executed inside Firecracker VMs, which
have a low startup time (hundreds of milliseconds). VM snapshots include
the full disk and memory contents of the machine, meaning that the Bazel
server is effectively kept warm between runs.

Remote runners use a sophisticated snapshotting mechanism that minimizes the
work that Bazel has to do on each CI run.

First, VM snapshots can be stored both locally on the machine that ran the
remote run, as well as remotely in BuildBuddy's cache. This way, if the
original machine that ran the remote run is fully occupied with other
workloads, subsequent runs can be executed on another machine,
but still be able to resume from a warm VM snapshot. BuildBuddy stores VM
snapshots in granular chunks that are downloaded lazily, so that unneeded
disk and memory chunks are not re-downloaded.

Second, snapshots are stored using a branching model that closely mirrors
the branching structure of the git repository itself, allowing remote runs
to be matched optimally to VM snapshots.

After a remote run executes on a particular git branch, BuildBuddy snapshots the
VM and saves it under a cache key which includes the git branch.

When starting a remote run on a particular git branch, BuildBuddy
attempts to locate an optimal snapshot to run it. It considers
the following snapshot keys in order:

1. The latest snapshot matching the git branch associated with the
   run.
1. The latest snapshot matching the base branch of the PR associated with
   the run.
1. The latest snapshot matching the default branch of the repo associated
   with the run.

For example, consider a remote run that runs on pull requests
(PRs). Given a PR that is attempting to merge the branch `users-ui` into a
PR base branch `users-api`, BuildBuddy will first try to resume the latest
snapshot associated with the `users-ui` branch. If that doesn't exist,
we'll try to resume from the snapshot associated with the `users-api`
branch. If that doesn't exist, we'll look for a snapshot for the `main`
branch (the repo's default branch). If all of that fails, only then do we
boot a new VM from scratch. When the remote run finishes and we save a
snapshot, we only overwrite the snapshot for the `users-ui` branch,
meaning that the `users-api` and `main` branch snapshots will not be
affected.

One common issue is not running any remote runs on your default branch.
Every time there is a new PR branch, for example, the remote run will start from
scratch because there is not a shared default snapshot for them to start from. One
solution is to trigger a remote run on every push to the default branch, to make sure
the default snapshot stays up to date.

For more technical details on our VM implementation, see our BazelCon
talk [Reusing Bazel's Analysis Cache by Cloning Micro-VMs](https://www.youtube.com/watch?v=YycEXBlv7ZA).

#### Recommended configuration

Snapshot matching is complex and different strategies come with performance
or cost trade-offs. More details can be found below at [Snapshot save policy](#snapshot-save-policy)
and [Snapshot read policy](#snapshot-read-policy).

Here are the recommended platform properties for most CI use cases. They balance
cost considerations and maintain good performance:

- `remote-snapshot-save-policy=none-available`
- `snapshot-read-policy=local-first`
- `max-stale-fallback-snapshot-age=1d`

For performance-sensitive or interactive workloads (like if you're using the Remote
Bazel CLI to build interactively), we recommend the following platform properties.
They will result in higher cache upload and download and associated costs:

- `remote-snapshot-save-policy=always`
- `snapshot-read-policy=newest`

For Workflows, you can set platform properties using the `platform_properties` field.

```yaml title="buildbuddy.yaml"
actions:
  - name: "Test all targets"
    platform_properties:
      remote-snapshot-save-policy: none-available
    env:
      GIT_REPO_DEFAULT_BRANCH: main
    # ...
```

For Remote Bazel, you can set platform properties this using the
`--runner_exec_properties=remote-snapshot-save-policy=` flag.

```bash Sample Command
bb remote --runner_exec_properties=remote-snapshot-save-policy=none-available test //...
```

NOTE: Setting platform properties will change the snapshot key. Immediately after
you update a platform property, you will see snapshot misses until a new default
snapshot with the new properties is saved.

#### Snapshot save policy

In order to reuse a warm VM, a snapshot of the machine must be taken and cached. While
this ensures good performance for future runs, caching snapshots can be expensive.

Snapshots can be very large because they include the entirety of
the disk and memory of the machine. For example, a VM with 16GB memory and a 50GB disk results in a 66GB
snapshot file. Storing them in the remote cache can cause significant network transfer, and
because cache uploads and downloads are billed, caching lots of snapshots may result in a high bill.

We support configuring the snapshot save policy with the `remote-snapshot-save-policy`
platform property. See [Recommended configuration](#recommended-configuration) for our recommendations.
Valid values are:

- `always`: Always save a snapshot.
  - For performance sensitive or interactive workloads, this will ensure the latest snapshot is
    always saved to the cache and will always be accessible.
- `first-non-default-ref`: Only the first run on a non-default ref will save a snapshot.
  All runs on default refs will save a snapshot.
  - This policy is applied by default.
  - Every run on the default branch (Ex. `main` or `master`) will save a snapshot.
  - The first run on your feature branch `my-feature` can resume from the default
    snapshot, and will save a snapshot for the `my-feature` ref.
  - The second run on the `my-feature` branch will resume from the original `my-feature`
    snapshot. However it will not save a snapshot.
  - The third run on the `my-feature` branch will resume from the original `my-feature`
    snapshot. It will not resume from the second run of the `my-feature` branch, and
    it will not save a snapshot.
- `none-available`: A snapshot on a non-default ref will only be saved if
  there are no snapshots available. If there is any fallback snapshot,
  a snapshot will not be saved. All runs on default refs will still save a snapshot.
  - Every run on the default branch (Ex. `main` or `master`) will save a snapshot.
  - For the first run on your feature branch `my-feature`, if there is snapshot
    for the default branch available, you will resume from it. Because you resumed
    from a snapshot, a snapshot will not be saved.
  - Subsequent runs on the `my-feature` branch will resume from the default snapshot,
    because no snapshots were saved for the `my-feature` branch.
  - If there is no default snapshot available, then a snapshot will be saved
    for the `my-feature` branch on its first run.

See [Recommended configuration](#recommended-configuration) for details on how to
apply these properties.

NOTE: This property relies on the `GIT_REPO_DEFAULT_BRANCH` environment variable
to be set (it should be set to something like `main` or `master` depending on your
repo's default branch).

If your workflow was triggered by a GitHub webhook event or the Remote Bazel CLI,
this environment variable will be set automatically. If you plan to manually
dispatch a Workflow with the ExecuteWorkflow API or our UI, or a Remote Bazel
command with the Run API, you must manually set this environment variable.

```yaml title="buildbuddy.yaml"
actions:
  - name: "Test all targets"
    platform_properties:
      remote-snapshot-save-policy: none-available
    env:
      GIT_REPO_DEFAULT_BRANCH: main
    # ...
```

For Remote Bazel, you can configure this using the
`--runner_exec_properties=remote-snapshot-save-policy=` flag.

NOTE: If your run is triggered by the BB CLI, the `GIT_REPO_DEFAULT_BRANCH`
environment variable will be set automatically. We use this to determine whether
the Workflow is running on a default ref. If you plan to use the `Run` API directly,
you must manually set this environment variable in the API request for this to work as
expected.

```bash Sample Command
bb remote --runner_exec_properties=remote-snapshot-save-policy=none-available test //...
```

##### Overriding the snapshot save policy

If you want to override the snapshot save policy for a specific run, you
can use a remote header. For example, lets say you typically
set a snapshot save policy where a snapshot would not be saved for a given workload, but you'd like to force a snapshot save for debug purposes.

By default, platform properties are hashed in the
snapshot key, so changing the snapshot save policy would invalidate your snapshot.
However platform properties set in remote headers are not included in the snapshot
key.

For example, lets say you use the `first-non-default-ref` policy, but wish
to force save a specific remote snapshot run to do some additional debugging. You
could apply that with a command like:

```bash Sample Command
bb remote --remote_run_header=x-buildbuddy-platform.remote-snapshot-save-policy=always test //...
```

Workflows do not support setting remote headers, but you can use Remote Bazel to
force save a snapshot that is used by Workflows. From the invocation page for your
Workflow, on the Executions tab next to the label "Saved to snapshot ID", there
is a button named `Copy Remote Bazel command to run commands in snapshot`. You
can add `--remote_run_header=x-buildbuddy-platform.remote-snapshot-save-policy=X`
to this command to force save a remote snapshot.

```bash Sample Command
bb remote --remote_run_header=x-buildbuddy-platform.remote-snapshot-save-policy=always --run_from_snapshot='{"snapshotId":"XXX","instanceName":""}' --script='echo "Just running this to force save a remote snapshot."'
```

#### Snapshot read policy

VM snapshots can be stored both locally on the machine that ran the
remote run, as well as remotely in BuildBuddy's cache.

There are pros and cons to local vs remote snapshots.

Resuming from a local snapshot is often faster, because the large snapshots don't have to be fetched over the network. They are also more economical, because remote cache reads are billed at a higher rate. Due to snapshots' size, reading remote snapshots can result in high cache download.

However local snapshots may be inaccessible by future workloads if the original machine is fully occupied and can't accept additional
work, or if the executor is restarting during a release. Remote snapshots can be
fetched from any machine, and guarantee you'll always be able to resume from a warm VM.

When looking for a snapshot, we look in the following order:

1. Search for a local snapshot on the same branch of the current run.
2. Search for a remote snapshot on the same branch.
3. Search for a local snapshot on a fallback branch (typically the default branch, like
   `main` or `master`).
4. Search for a remote snapshot on a fallback branch.

We support configuring the snapshot read policy with the `snapshot-read-policy`
platform property. Valid values are:

- `newest`: Always read the newest snapshot available.
  - For performance-sensitve or interactive workloads, this will ensure the
    most recently updated snapshot is used.
  - This could be a local or remote snapshot.
- `local-first`: Use a local snapshot if available.
  - This is more economical, because remote cache reads are billed at a higher rate.
  - This may mean that even if a newer remote snapshot exists, a more stale local
    snapshot may be used.
  - This can be used in conjunction with the platform property `max-stale-fallback-snapshot-age`,
    which is described in more detail below.
- `local-only`: Only use local snapshots.
  - This may result in more snapshot misses, and more runs will start from clean runners.
  - Local snapshots may become unusable if the executor that has it cached locally
    is fully occupied with other workloads or is restarting during a release.
    Whereas remote snapshots are usable no matter the underlying executor.

Snapshot reads are also configurable with the `max-stale-fallback-snapshot-age`
platform property. Valid values are durations like `12h` or `1d`.

This platform sets a limit on the max age of a fallback snapshot to prevent using very stale local snapshots that may cause performance degradation. The system will then fetch a newer remote snapshot.

For example, let's say you set `snapshot-read-policy=local-first` and
`max-stale-fallback-snapshot-age=1d`.

- The first run on your feature branch `my-feature` runs on Machine A and resumes from a fallback
  snapshot on the default branch `main`. There is no local snapshot for `main`
  available, so it will fetch a remote snapshot. It will cache this `main` snapshot
  locally (`Main A`).
- A `main` workflow runs on another machine, and saves a newly updated `main`
  snapshot to the remote cache (`Main B`).
- 20 minutes later, the first run of another feature branch `feature-2` runs on Machine A and
  resumes from a fallback snapshot on `main`. It will resume from the local snapshot
  `Main A` due to the snapshot read policy `local-first`, even though a more recent
  snapshot on `main` exists in the remote cache (`Main B`).
- 1 day 20 minutes later, the first run of another feature branch `feature-3`
  runs on Machine A and resumes from a fallback snapshot on `main`. This time,
  the local snapshot `Main A` is too stale, because the max fallback age was set to `1d`.
  It will fetch and resume from the remote snapshot `Main B`.

### Runner recycling (macOS only)

On macOS, remote runs are matched to workspaces using a simpler
runner-recycling based approach. Remote runs are associated with Git
repositories, and matched to any runner associated with the repository.
Each runner keeps a separate Bazel workspace directory and on-disk cache,
as well as its own Bazel server instance, which is kept alive between
runs. Runners are evicted from the machine only if the number of runners
exceeds a configured limit or if the disk resource usage exceeds a
configured amount.

macOS remote runners are only available for self-hosted Macs. See our
[configuration docs](workflows-config#mac-configuration) for more details,
or [contact us](https://www.buildbuddy.io/contact) for more info about
BuildBuddy-managed Macs.

## Optimal usage of remote runners

While our remote runners support running arbitrary bash code, they were specifically
designed and optimized to run the Bazel client server and power Bazel commands with
remote execution (RBE).

Because we snapshot the entire memory and disk of each runner, the product is slower
when we must serialize and deserialize larger runners. Thus we have a 100GB limit
on disks for remote runners.

It's more effective when a smaller remote runner is used to orchestrate farming out most computation to
traditional Bazel remote executors.

## Getting started

You can get started with [BuildBuddy Workflows(https://docs.buildbuddy.io/docs/workflows-setup/)
or [Remote Bazel](https://docs.buildbuddy.io/docs/remote-bazel/) by checking out the
corresponding docs.

If you've already linked your GitHub account to BuildBuddy, it'll only take
about 30 seconds to enable remote runners for your repo &mdash; just select a repo
to link, and we'll take care of the rest!
