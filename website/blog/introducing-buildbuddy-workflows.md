---
slug: introducing-buildbuddy-workflows
title: "Introducing BuildBuddy workflows: convenient, fast, and secure CI for bazel"
author: Brandon Duffany
author_title: Engineer @ BuildBuddy
date: 2021-06-24:12:00:00 # DO NOT MERGE: NEEDS UPDATE
author_url: https://www.linkedin.com/in/brandon-duffany-39b7217a
author_image_url: https://avatars.githubusercontent.com/u/2414826?v=4
tags: [product]
---

In our [latest release](TODO/LINK_TO_RELEASE_POST), we launched BuildBuddy
Workflows, a **convenient**, **fast**, and **secure** CI solution for
GitHub repositories using Bazel.

Workflows automatically run Bazel on powerful machines deployed close
to BuildBuddy's servers whenever you push a commit to your repo.
This gives you the confidence that your code builds successfully and
passes all tests before you merge pull requests or deploy a new release.

We've used workflows on our own source repositories for the past few
months, and have found them to speed up our existing CI solution by TODO
on average. If you have a Bazel repository hosted on GitHub, try out
workflows and let us know what you think!

<!-- TODO: Chart comparing GitHub CI times to BuildBuddy CI -->

## How workflows work

Workflows work like this:

- First, you link your repository to BuildBuddy via the workflows
  page. This gives us the go-ahead and permissions (if needed) to start
  listening for new commits and to build your code.
- Now let's say you open a PR on GitHub. BuildBuddy gets a notification,
  checks out your repo, and runs `bazel test //...` (by default) on your PR
  branch.
- Once the build is complete, BuildBuddy reports a status to the pull
  request (succeeded or failed).

The full power of workflows is unlocked when you use them in combination
with branch protection rules. This means that the PR will not be allowed
to merge until it passes on BuildBuddy.

## How we made workflows fast

In addition to convenience and security, one of our main goals for workflows
was to maximize performance, even for very large source repositories.

We did this in two main ways:

1. Ensure that workflows can communicate with BuildBuddy's remote cache and
   remote execution system with a fast network connection (high throughput
   and low latency).
2. Run workflows in a persistent environment (as opposed to creating a
   fresh environment each time).

### Running workflows close to BuildBuddy's servers

For users taking advantage of BuildBuddy's remote cache and remote
execution system, it's crucial to ensure that we run workflows very close
to BuildBuddy's servers.

This means that if we need to fetch something from BuildBuddy's cache, or
we need to build a particular target remotely, the network will not be
a major bottleneck.

The solution here is simple: run workflows on a dedicated executor
deployment in the same datacenter where BuildBuddy is deployed.

Because of our recent improvements to our caching infrastructure
in [BuildBuddy v2](introducing-buildbuddy-v2), the workflow executor
pool gets <!-- TODO --> latency and <!-- TODO --> throughput to
the remote cache.

### Persistent containers

In [BuildBuddy v2](introducing-buildbuddy-v2#sandboxing), we announced
improvements to our remote build execution system that allow us to re-use
sandbox environments across build actions.

For workflows, this new functionality is especially useful. Instead of creating
a new Docker container each time we build a repository, we
try to reuse a container that previously built the repository. This has
several advantages:

- Whenever a new commit is pushed, we only need to fetch the diffs,
  and not re-clone the whole repo.
- We don't need to start a new bazel server each time, meaning that
  we can re-use the analysis cache from the previous build. Roughly,
  this means that bazel doesn't have to construct the full build graph by
  parsing all the BUILD files in your repo, because it already has them in
  memory. This also means that if you specify a `.bazelversion`, we don't
  need to download and install your requested bazel version each time.
- The development environment keeps the local bazel cache around, meaning
  that we don't need to make as many calls to the remote cache. Our
  remote caching infrastructure is fast, but the on-disk bazel cache
  is faster.

## Next steps

Get started with workflows by checking out our [setup guide](/docs/workflows-setup/).
If you've already linked your GitHub account to BuildBuddy, it'll only take
about a minute to enable workflows for a repo.

<!-- TODO: webm video of one-click setup process -->

As always, message us on [Slack](https://buildbuddy.slack.com) or
[file an issue](https://github.com/buildbuddy-io/buildbuddy/issues/new)
if you need help, run into any issues, or have feature requests!
