---
slug: how-bazel-5-0-makes-your-builds-faster
title: How Bazel 5.0 Makes Your Builds Faster
description: Highlighting changes in Bazel 5.0 that help BuildBuddy users build even faster!
author: Brentley Jones
author_title: Developer Evangelist @ BuildBuddy
date: 2022-01-25:12:00:00
author_url: https://brentleyjones.com
author_image_url: https://avatars.githubusercontent.com/u/158658?v=4
image: /img/bazel_5_0_faster.png
tags: [bazel]
---

In our [last post][bazel_5_0],
we summarized the changes that were in the Bazel 5.0 release.
There were a lot of changes though,
so it can be hard to determine which ones are impactful to you and why.

Don't worry, we've got your back.
In this post we highlight the changes that help BuildBuddy users build even faster!

[bazel_5_0]: whats-new-in-bazel-5-0.md

<!-- truncate -->

## Build Event Service (BES) improvements

Bazel 5.0 includes many changes that make uploading BES events and artifacts to the BuildBuddy UI more reliable and performant.

Before Bazel 5.0,
if you wanted to upload a timing profile to BuildBuddy,
you had to be fine with Bazel uploading all outputs referenced in the BEP,
even if they were set to not be cached
(e.g. the `--noremote_upload_local_results` flag or the `no-remote-cache` tag).
Now you can set the `--incompatible_remote_build_event_upload_respect_no_cache` flag,
which causes Bazel to respect your wishes in regards to output caching.

Also,
if you've had [warnings][bes_upload_mode_warnings] or [crashes][bes_upload_mode_crashes] when using `--bes_upload_mode=fully_async` in the past,
those should now be fixed as well.

[bes_upload_mode_crashes]: https://github.com/bazelbuild/bazel/issues/11408
[bes_upload_mode_warnings]: https://github.com/bazelbuild/bazel/issues/11392

## Deduplicated cache calls and uploads

Before Bazel 5.0,
when using Remote Build Execution (RBE),
Bazel would ask the cache which blobs it needed to upload for every input to an action that was going to be executed remotely.[^1]
It did this even if it had previously uploaded a specific input for a previous action.
Similarly,
if two actions with similar inputs were executed concurrently,
then Bazel didn't deduplicate the input existence checking or uploading.

For projects with a large number of shared inputs,
which is common in C-family programming languages and Node.js,
this can cause a significant amount of network traffic,
resulting in long build times on low-bandwidth, high-latency connections.

On the output side of things,
if multiple actions produced the same output,
then Bazel would upload the output multiple times.
If these outputs were large,
then this caused significant network overhead.

Bazel 5.0 addresses these issues.
If you have a project that shares a lot of inputs between actions
(e.g. C, C++, Objective-C, and Node.js),
and you use BuildBuddy's Remote Execution,
then Bazel 5.0 should improve your build times.

[^1]:
    Specifically,
    Bazel would make a `FindMissingBlobs` that contained every input for an action,
    every time.
    Now Bazel will only include the unique inputs it hasn't already uploaded before.

## Faster action cache checking

In a similar vein as the above mentioned issues,
Bazel can be unnecessarily slow when checking if an action with a large number of inputs is cached.
This is because Bazel needs to build a [Merkle tree][merkle_tree] from an action's inputs,
which can be expensive if there are many.

Starting in Bazel 5.0,
you can use the `--experimental_remote_merkle_tree_cache` and `--experimental_remote_merkle_tree_cache_size` flags to cache the nodes of these merkle trees.
If you have a project that shares a lot of it's inputs between actions
(e.g. C, C++, Objective-C, and Node.js),
and you use BuildBuddy's Remote Cache or Remote Execution,
then these flags should improve your build times.

[merkle_tree]: https://en.wikipedia.org/wiki/Merkle_tree

## Asynchronous cache uploading

When an action is run locally,
and Bazel uploads the action's outputs to a remote build cache,
it waits for that upload to complete before it executes dependent actions.
This is needed for dependent actions that are executed remotely,
but it's unnecessary if they are executed locally.
These delays can cause builds that use BuildBuddy's Remote Cache to sometimes be slower than if they didn't use the cache.

Starting in Bazel 5.0,
you can use the `--experimental_remote_cache_async` flag to have Bazel perform these uploads asynchronously.
Uploads still need to complete before a build finishes,
but now Bazel can speed ahead with local execution regardless of upload speed.
Network utilization is also improved,
as uploads can queue up quicker.
If you have a project that uploads to BuildBuddy's Remote Cache,
then this flag should improve your build times.

## Compressed uploads and downloads

Bazel 5.0 added the `--experimental_remote_cache_compression` flag,
which causes Bazel to compresses and decompress artifact uploads and downloads with the [zstd algorithm][zstd].
In our testing this reduces the average number of bytes transferred by 60-70%.

There is some overhead involved in this,
both on the Bazel side and the cache side,
so it won't make every build for every project faster.[^2]
That said,
if network bandwidth is a bottleneck in your build times,
this flag could make those builds significantly faster.

[zstd]: https://en.wikipedia.org/wiki/Zstandard

[^2]:
    Eventually we might store artifacts in their compressed state,
    removing most overhead on the cache side.

## And more...

Bazel 5.0 includes many additional changes that improve it's reliability and performance.
To dig a little deeper,
be sure to check out our [What's New in Bazel 5.0][bazel_5_0] post.
