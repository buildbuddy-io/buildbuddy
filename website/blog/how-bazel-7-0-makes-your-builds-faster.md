---
slug: how-bazel-7-0-makes-your-builds-faster
title: How Bazel 7.0 Makes Your Builds Faster
description: "Highlighting changes in Bazel 7.0 that help BuildBuddy users build even faster!"
author: Brentley Jones
author_title: "Developer Evangelist @ BuildBuddy"
date: 2024-01-08:10:00:00
author_url: https://brentleyjones.com
author_image_url: https://avatars.githubusercontent.com/u/158658?v=4
image: /img/bazel_7_0_faster.png
tags: [bazel]
---

In our [last post][bazel_7_0],
we summarized the changes that were in the Bazel 7.0 release.
There were a lot of changes though,
so it can be hard to determine which ones are impactful to you and why.

Don't worry, we've got your back.
In this post we highlight the changes that help BuildBuddy users build even faster!

[bazel_7_0]: whats-new-in-bazel-7-0.mdx

<!-- truncate -->

## Analysis phase

Bazel 7.0 includes numerous optimizations to the Starlark interpreter.
These optimizations result in faster loading and analysis phases,
allowing execution to start sooner.
They also result in lower peak and retained Bazel server memory usage,
which can indirectly speed up your build
(e.g. because of fewer JVM garbage collections).

## Execution phase

In Bazel 6.x and 7.0 file checksumming was optimized.
This means that actions that output symlinks of large objects or large tree artifacts run much faster.

The [`--reuse_sandbox_directories`][reuse_sandbox_directories] feature received some bug fixes.
Using this flag on macOS can be a sizable speedup if you use sandboxing,
which is the the default for most actions.

Bazel's local CPU resource counting on Linux is now container aware.
This should result in better default utilization of CPUs in containers.

The Android rules added persistent worker support to more actions.
The persistent workers can be enabled with the
[`--experimental_persistent_aar_extractor`][experimental_persistent_aar_extractor],
[`--persistent_android_resource_processor`][persistent_android_resource_processor],
and [`--persistent_android_dex_desugar`][persistent_android_dex_desugar] flags.

[experimental_persistent_aar_extractor]: https://bazel.build/versions/7.0.0/reference/command-line-reference#flag--experimental_persistent_aar_extractor
[persistent_android_resource_processor]: https://bazel.build/versions/7.0.0/reference/command-line-reference#flag--persistent_android_resource_processor
[persistent_android_dex_desugar]: https://bazel.build/versions/7.0.0/reference/command-line-reference#flag--persistent_android_dex_desugar
[reuse_sandbox_directories]: https://bazel.build/versions/7.0.0/reference/command-line-reference#flag--experimental_circuit_breaker_strategy

## Skymeld

In Bazel 7.0 the analysis and execution phases are now merged
(i.e. [project Skymeld][skymeld]).
Depending on the shape of your build,
and the number of top-level targets you are building,
this can result in a decent speedup.

You can disable this feature
(e.g. to work around bugs or to benchmark)
with [`--noexperimental_merged_skyframe_analysis_execution`][experimental_merged_skyframe_analysis_execution].

[experimental_merged_skyframe_analysis_execution]: https://github.com/bazelbuild/bazel/blob/7.0.0/src/main/java/com/google/devtools/build/lib/buildtool/BuildRequestOptions.java#L376-L383
[skymeld]: https://github.com/bazelbuild/bazel/issues/14057

## BLAKE3

In Bazel 6.4 the `blake3` option was added to the [`--digest_function`][digest_function] startup flag.
When using this option the [BLAKE3][blake3] hash function is used to compute file digests.
For large files this can be significantly faster than the default SHA-256 hash function.

There is a small caveat to using this new option.
If you use a remote cache,
it needs to also support BLAKE3 digests.
If you are not using a cache,
or only using `--disk_cache`,
you can safely use this option.
In case you were wondering,
all of BuildBuddy's products
(i.e. Build and Test UI, Remote Build Cache, and Remote Build Execution)
support BLAKE3 digests 😊.

[blake3]: https://github.com/BLAKE3-team/BLAKE3
[digest_function]: https://github.com/bazelbuild/bazel/blob/7.0.0/src/main/java/com/google/devtools/build/lib/runtime/BlazeServerStartupOptions.java#L407-L418

## Remote caching and remote execution

Remote server capabilities are now fetched asynchronously,
allowing analysis and non-remote actions to start sooner.

Merkle trees,
which are created by Bazel for use with remote execution,
are now built faster while using less memory.

When the remote cache or executors are unreachable,
Bazel now automatically falls back to local execution,
instead of erroring.
Similarly, in Bazel 6.3 the
[`--experimental_circuit_breaker_strategy`][experimental_circuit_breaker_strategy],
[`--experimental_remote_failure_window_interval`][experimental_remote_failure_window_interval],
and [`--experimental_remote_failure_rate_threshold`][experimental_remote_failure_rate_threshold]
flags were added,
which allows configuring Bazel to switch to local execution if a certain rate of remote errors are encountered during a build.

The [`--experimental_remote_scrubbing_config`][experimental_remote_scrubbing_config] flag was added,
which allows you to increase cache hits for platform independent targets when building from multiple host platforms.

All of these changes result in faster and more reliable builds when you use remote strategies.

[experimental_circuit_breaker_strategy]: https://bazel.build/versions/7.0.0/reference/command-line-reference#flag--experimental_circuit_breaker_strategy
[experimental_remote_failure_rate_threshold]: https://bazel.build/versions/7.0.0/reference/command-line-reference#flag--experimental_remote_failure_rate_threshold
[experimental_remote_failure_window_interval]: https://bazel.build/versions/7.0.0/reference/command-line-reference#flag--experimental_remote_failure_window_interval
[experimental_remote_scrubbing_config]: https://bazel.build/versions/7.0.0/reference/command-line-reference#flag--experimental_remote_scrubbing_config

## Build without the Bytes

There were quite a few changes to the [Build without the Bytes feature][bwtb],
in the form of both performance enhancements and bug fixes.
Because of those changes,
the Bazel team is finally confident enough in this feature that `toplevel` is now the the default value for the [`--remote_download_outputs`][remote_download_outputs] flag.

Using Build without the Bytes can speed up your build by allowing Bazel to not fetch unneeded intermediate build artifacts from your cache.
The benefit of this is even more pronounced if you are using Remote Build Execution,
since any actions that need to be rerun are run remotely,
saving you from having to download action inputs.

If you don't need the outputs
(e.g. on CI or when running tests with Remote Build Execution),
you can use the `minimal` option with the `--remote_download_outputs` flag,
which can speed up your builds even more.
And as of Bazel 7.0,
changing the value of the `--remote_download_outputs` flag won't invalidate the analysis cache.

[bwtb]: https://blog.bazel.build/2023/10/06/bwob-in-bazel-7.html
[remote_download_outputs]: https://bazel.build/versions/7.0.0/reference/command-line-reference#flag--remote_download_outputs

## Bzlmod

The Bzlmod lockfile,
which received bug fixes and breaking changes through the Bazel 6.x and 7.0 releases,
is now stable as of Bazel 7.0.
Using the lockfile can speed up fresh Bazel server launches,
by preventing unnecessary rerunning of dependency resolution.
This has an added benefit of allowing your workspace to build offline,
even if the Bazel server is restarted.

Additionally,
Bazel 6.4 and 7.0 includes dependency resolution performance optimizations,
resulting in reduced CPU and memory usage.

Both Bzlmod and the Bzlmod lockfile are enabled by default in Bazel 7.0,
and can be adjusted with the [`--enable_bzlmod`][enable_bzlmod] and [`--lockfile_mode`][lockfile_mode] flags.

[enable_bzlmod]: https://bazel.build/versions/7.0.0/reference/command-line-reference#flag--enable_bzlmod
[lockfile_mode]: https://bazel.build/versions/7.0.0/reference/command-line-reference#flag--lockfile_mode

## And more...

Bazel 7.0 includes many additional changes that improve its reliability and performance.
To dig a little deeper,
be sure to check out our [What's New in Bazel 7.0][bazel_7_0] post.
