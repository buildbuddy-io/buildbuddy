---
slug: meet-buildbuddy-workflows
title: "Meet BuildBuddy Workflows"
author: Brandon Duffany
author_title: Engineer @ BuildBuddy
date: 2021-06-30:12:00:00
author_url: https://www.linkedin.com/in/brandon-duffany-39b7217a
author_image_url: https://avatars.githubusercontent.com/u/2414826?v=4
image: /img/workflows.png
tags: [product]
---

In today's [BuildBuddy v2.3 release](https://github.com/buildbuddy-io/buildbuddy/releases/tag/v2.3.0), which is now live on BuildBuddy Cloud, we're launching **BuildBuddy Workflows**. BuildBuddy Workflows is a Continuous Integration (CI) solution for Bazel repositories hosted on GitHub (with support for other providers coming soon).

Like other CI solutions, Workflows give you the confidence that your code
builds successfully and passes all tests before you merge pull requests or
deploy a new release.

But because BuildBuddy Workflows were built for Bazel repos and tightly
integrated with BuildBuddy RBE and Remote Caching, they are **_really fast_**.

<!-- truncate -->

# Why a Bazel-focused CI solution?

Traditional [CI systems](https://en.wikipedia.org/wiki/Continuous_integration), like Jenkins, Travis, CircleCI, and BuildKite, are built around the concept of a pipeline. Pipelines allow you to specify a list of build/test steps to run for each commit or pull request to your repo. Pipelines are great because you can run many in parallel across multiple machines. Unfortunately, there are often dependencies between these pipelines, for example a build step that must be completed before a test step can begin.

Some tools, like [GitLab Pipelines](https://docs.gitlab.com/ee/ci/pipelines/), attempt to solve this problem by allowing you to specify dependencies between pipelines. This approach is better, but forces you to manually maintain the relationships between pipelines in a pile of YAML configuration files. As the number of dependencies grow, any sufficiently complex CI system [starts to resemble a build system](https://gregoryszorc.com/blog/2021/04/07/modern-ci-is-too-complex-and-misdirected/).

None of these pipeline-based approaches are well suited for Bazel's approach to dependency management and remote build execution, which involves generating a [Directed Acyclic Graph](https://en.wikipedia.org/wiki/Directed_acyclic_graph) of all build and test actions. Bazel's approach allows for optimal parallelization and caching of these actions. It also enables rebuilding and retesting only affected targets, saving both engineering time and compute resources.

## How fast are BuildBuddy Workflows?

We've used BuildBuddy Workflows on our own repos for the past few
months, comparing them side-by-side with our existing CI solution built on GitHub Actions with BuildBuddy RBE and Remote Caching enabled.

By leveraging warm, hosted, Bazel processes, as well as BuildBuddy's
remote caching and execution, Workflows dramatically sped up our CI runs.
Compared to our previous solution (which used BuildBuddy RBE and Remote Caching on GitHub Runners), we reduced the median duration by nearly **8X** &mdash; with most CI runs completing in just a few seconds.

This overlapping histogram chart shows the complete picture. Note that
the majority of BuildBuddy workflow runs took 30 seconds or less, while
nearly all runs on GitHub Actions took at least 2 minutes and 15 seconds:

![overlapping histogram comparing BuildBuddy and GitHub actions](../static/img/blog/workflows.png)

## How did we make BuildBuddy Workflows so fast?

In addition to convenience and security, one of our main goals for Workflows
was to maximize performance, even for very large source repositories.

We did this in two main ways:

1. Ensuring a **fast network connection between Bazel and BuildBuddy's RBE & caching servers**.
2. Running workflows against **hosted, warm, Bazel instances**.

### Fast connection to BuildBuddy RBE

In our experience, network latency is often the biggest bottleneck in many Bazel Remote Build Execution and Remote Caching setups.

The solution here was simple: run Workflows on executors in the same datacenters where BuildBuddy RBE and Cache nodes are deployed.

With GitHub actions or other CI solutions, the network connection might
be fast (particularly after the recent network optimizations we made in
[BuildBuddy v2](/blog/introducing-buildbuddy-v2)) &mdash; but not nearly as fast
as having workflow runners on the same local network as BuildBuddy
itself.

### Hosted, Warm, Bazel instances

Once you have a sufficiently fast RBE and Remote Caching setup, and have removed network bottlenecks &mdash; the CI bottleneck often becomes Bazel's [analysis phase](https://docs.bazel.build/versions/main/glossary.html#analysis-phase).

By re-using warm Bazel processes when possible, we're able to re-use Bazel's analysis cache across CI runs of the same repo. This can save several minutes per build, depending on the size of your repository and the number of external dependencies being pulled in.

This is similar to how [Google's Build Dequeuing Service](https://dl.acm.org/doi/pdf/10.1145/3395363.3397371) performs workspace selection:

> A well-chosen workspace can increase the build speed by an
> order of magnitude by reusing the various cached results from the
> previous execution. [...] We have observed that builds that execute the same targets as a previous
> build are effectively no-ops using this technique

## How do I use BuildBuddy Workflows?

BuildBuddy Workflows are launching today, in Beta, for all GitHub users. You can get started with BuildBuddy Workflows by checking out our [setup guide](https://docs.buildbuddy.io/docs/workflows-setup/).
If you've already linked your GitHub account to BuildBuddy, it'll only take
about 30 seconds to enable Workflows for your repo &mdash; just select a repo
to link, and we'll take care of the rest!

## Other changes in BuildBuddy v2.3

While the main focus of BuildBuddy v2.3 has been on launching BuildBuddy Workflows, the release also contains several other features, in addition to lots of bug fixes and performance improvements.

### Dependency graph visualization

We added dependency graph visualizations for `bazel query` commands that use the `--output graph` parameter. This visualization is zoom-able and pan-able, and can render graphs with thousands of edges.

Here's an example of a command you can run to generate a graph:

```shell
bazel query '//...' --output graph --bes_backend=remote.buildbuddy.io --bes_results_url=https://app.buildbuddy.io/invocation/
```

And the resulting output:

![Bazel query dependency graph visualization](../static/img/blog/query_graph.png)

### Clickable RBE Actions

For actions executed with BuildBuddy Remote Build Execution, you can now click on individual actions to get the full set of command arguments, environment variables, execution metadata, output files, and more:

![RBE actions view](../static/img/blog/clickable_rbe_actions.png)

That's it for this release! As always, message us on [Slack](https://buildbuddy.slack.com) or
[file an issue](https://github.com/buildbuddy-io/buildbuddy/issues/new)
if you need help, run into any issues, or have feature requests!
