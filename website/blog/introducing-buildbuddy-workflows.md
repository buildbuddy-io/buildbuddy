---
slug: introducing-buildbuddy-workflows
title: "Introducing BuildBuddy Workflows"
subtitle: "Fast and secure CI for Bazel"
description: "Test subtitle"
author: Brandon Duffany
author_title: Engineer @ BuildBuddy
date: 2021-06-24:12:00:00 # DO NOT MERGE: NEEDS UPDATE
author_url: https://www.linkedin.com/in/brandon-duffany-39b7217a
author_image_url: https://avatars.githubusercontent.com/u/2414826?v=4
tags: [product]
---

In our [latest release](TODO/LINK_TO_RELEASE_POST), we launched BuildBuddy
Workflows, a Continuous Integration (CI) solution for Bazel repositories
hosted on GitHub.

Like other CI solutions, workflows give you the confidence that your code
builds successfully and passes all tests before you merge pull requests or
deploy a new release.

But because BuildBuddy workflows are optimized for Bazel repos and closely
integrated with BuildBuddy services, they are _really fast_.

## How fast are BuildBuddy workflows?

We've used BuildBuddy workflows on our own source repositories for the past few
months, comparing them side-by-side with our existing CI solution.

By leveraging Bazel's local caching as well as BuildBuddy's
remote caching and execution, workflows dramatically sped up our CI runs.
Compared to our previous solution, we reduced the median duration by nearly
**8X**.

This overlapping histogram chart shows the complete picture. Note that
the majority of BuildBuddy workflow runs took 30 seconds or less, while
nearly all runs on GitHub Actions took at least 2 minutes and 15 seconds:

![overlapping histogram comparing BuildBuddy and GitHub actions](images/workflows.png)

## How did we make workflows fast?

In addition to convenience and security, one of our main goals for workflows
was to maximize performance, even for very large source repositories.

We did this in two main ways:

1. Ensure a **fast network connection to BuildBuddy's servers**.
2. Run workflow actions in a **persistent sandbox environment**.

### Fast connection to BuildBuddy

For users taking advantage of BuildBuddy's remote cache and remote
execution system, it's crucial to ensure that we run workflows very close
to BuildBuddy's servers.

This means that if we need to fetch something from BuildBuddy's cache, or
we need to build a particular target remotely, network latency and
bandwidth will not be a major bottleneck.

The solution here was simple: run workflows on a dedicated executor
deployment in the same datacenter where BuildBuddy is deployed.

With GitHub actions or other CI solutions, the network connection might
be fast (especially after the recent network optimizations we made in
[BuildBuddy v2](introducing-buildbuddy-v2)) &mdash; but not nearly as fast
as having the workflow runner on the same local network as BuildBuddy
itself.

### Sandbox persistence

In [BuildBuddy v2](introducing-buildbuddy-v2#sandboxing), we announced
improvements to our remote build execution system that allow us to re-use
sandbox environments across build actions.

For workflows, this new functionality is especially useful. Instead of creating
a new Docker container each time we build a repository, we
try to reuse a container that previously built the repository. This has
several advantages:

- Whenever a new commit is pushed, we only need to fetch the diffs,
  and not re-clone the whole repo.
- We don't need to start a new Bazel server each time, meaning that
  we can re-use the analysis cache from the previous build. Roughly,
  this means that Bazel doesn't have to construct the full build graph by
  parsing all the BUILD files in your repo, because it has the build graph
  in memory. This also means that if you specify a `.bazelversion`, we don't
  need to download and install your requested Bazel version each time.
- The development environment keeps the local Bazel cache around, meaning
  that we don't need to make as many calls to the remote cache. Our
  remote caching infrastructure is fast, but the on-disk Bazel cache
  is faster.

## How do I use workflows?

Get started with BuildBuddy workflows by checking out our [setup guide](/docs/workflows-setup/).
If you've already linked your GitHub account to BuildBuddy, it'll only take
about 30 seconds to enable workflows for your repo &mdash; just select a repo
to link, and we take care of the rest!

As always, message us on [Slack](https://buildbuddy.slack.com) or
[file an issue](https://github.com/buildbuddy-io/buildbuddy/issues/new)
if you need help, run into any issues, or have feature requests!
