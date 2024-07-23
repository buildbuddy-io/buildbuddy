---
id: workflows-introduction
title: Introduction to Workflows
sidebar_label: Workflows introduction
---

BuildBuddy Workflows is a Continuous Integration (CI) solution for Bazel
repositories hosted on GitHub.

BuildBuddy Workflows are specifically designed for Bazel builds, and are
tightly integrated with BuildBuddy RBE and Remote Caching, making them
significantly faster than other CI options.

BuildBuddy Workflows provides the following unique advantages:

1. Colocation with BuildBuddy servers, ensuring a **fast network
   connection between Bazel and BuildBuddy's RBE & caching servers**.
2. Running workflows against **hosted, warm, Bazel instances** using VM
   snapshotting (on Linux) and persistent runners (on macOS).

See [our blog post](https://www.buildbuddy.io/blog/meet-buildbuddy-workflows)
for more details on the motivation behind workflows as well as some
real-world results.

## Colocation with BuildBuddy servers

Network latency is often the biggest bottleneck in many Bazel Remote Build
Execution and Remote Caching setups. This is because Bazel's remote APIs
require several chained RPCs due to dependencies between actions.

To address this bottleneck, BuildBuddy Workflows are executed in the same
datacenters where BuildBuddy RBE and Cache nodes are deployed. This
results in sub-millisecond round trip times to BuildBuddy's servers,
minimizing the overhead incurred by Bazel's remote APIs.

## Hosted, warm, Bazel instances

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

### Bazel instance matching

To match workflow attempts to warm Bazel instances, BuildBuddy uses VM
snapshotting powered by
[Firecracker](https://github.com/firecracker-microvm/firecracker) on
Linux, and a simpler runner-recycling based approach on macOS.

#### Firecracker VMs (Linux only)

On Linux, BuildBuddy Workflows are executed inside Firecracker VMs, which
have a low startup time (hundreds of milliseconds). VM snapshots include
the full disk and memory contents of the machine, meaning that the Bazel
server is effectively kept warm between workflow runs.

Workflows use a sophisticated snapshotting mechanism that minimizes the
work that Bazel has to do on each CI run.

First, VM snapshots are stored both locally on the machine that ran the
workflow, as well as remotely in BuildBuddy's cache. This way, if the
original machine that ran a workflow is fully occupied with other
workloads, subsequent workflow runs can be executed on another machine,
but still be able to resume from a warm VM snapshot. BuildBuddy stores VM
snapshots in granular chunks that are downloaded lazily, so that unneeded
disk and memory chunks are not re-downloaded.

Second, snapshots are stored using a branching model that closely mirrors
the branching structure of the git repository itself, allowing CI
workloads to be matched optimally to VM snapshots.

After a workflow runs on a particular git branch, BuildBuddy snapshots the
workflow VM and saves it under a cache key which includes the git
branch.

When starting a workflow execution on a particular git branch, BuildBuddy
attempts to locate an optimal snapshot to run the workflow. It considers
the following snapshot keys in order:

1. The latest snapshot matching the git branch associated with the
   workflow run.
1. The latest snapshot matching the base branch of the PR associated with
   the workflow run.
1. The latest snapshot matching the default branch of the repo associated
   with the workflow run.

For example, consider a BuildBuddy workflow that runs on pull requests
(PRs). Given a PR that is attempting to merge the branch `users-ui` into a
PR base branch `users-api`, BuildBuddy will first try to resume the latest
snapshot associated with the `users-ui` branch. If that doesn't exist,
we'll try to resume from the snapshot associated with the `users-api`
branch. If that doesn't exist, we'll look for a snapshot for the `main`
branch (the repo's default branch). If all of that fails, only then do we
boot a new VM from scratch. When the workflow finishes and we save a
snapshot, we only overwrite the snapshot for the `users-ui` branch,
meaning that the `users-api` and `main` branch snapshots will not be
affected.

For more technical details on our VM implementation, see our BazelCon
talk [Reusing Bazel's Analysis Cache by Cloning Micro-VMs](https://www.youtube.com/watch?v=YycEXBlv7ZA).

#### Runner recycling (macOS only)

On macOS, workflows are matched to workspaces using a simpler
runner-recycling based approach. Workflow runs are associated with Git
repositories, and matched to any runner associated with the repository.
Each runner keeps a separate Bazel workspace directory and on-disk cache,
as well as its own Bazel server instance, which is kept alive between
runs. Runners are evicted from the machine only if the number of runners
exceeds a configured limit or if the disk resource usage exceeds a
configured amount.

macOS workflows are only available for self-hosted Macs. See our
[configuration docs](workflows-config#mac-configuration) for more details,
or [contact us](https://www.buildbuddy.io/contact) for more info about
BuildBuddy-managed Macs.

## Getting started

You can get started with BuildBuddy Workflows by checking out our
[setup guide](https://docs.buildbuddy.io/docs/workflows-setup/).

If you've already linked your GitHub account to BuildBuddy, it'll only take
about 30 seconds to enable Workflows for your repo &mdash; just select a repo
to link, and we'll take care of the rest!
