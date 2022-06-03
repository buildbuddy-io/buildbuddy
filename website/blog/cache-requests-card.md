---
slug: cache-requests-card
title: Unlocking More Build Insights with the Cache Requests Card
description: All about the new cache requests card and how to use it.
author: Brandon Duffany
author_title: Engineer @ BuildBuddy
date: 2022-05-25:12:00:00
author_url: https://github.com/bduffany
author_image_url: https://avatars.githubusercontent.com/u/2414826?v=4
image: /img/blog/cache-requests-card.png
tags: [ui, bes, cache]
---

When using BuildBuddy's remote cache, Bazel can spend most of its time
uploading and downloading artifacts from cache, rather than building
artifacts from scratch.

When a build is making heavy use of the remote cache (either uploading or
downloading artifacts), the overall build can be bottlenecked by the
performance of the network, especially if your build is
bandwidth-restricted.

And when you want to address performance bottlenecks, it's helpful to have
as much visibility as possible, in a format that is quickly and easily
digestable. That's why we build the new cache requests card.

In this post we'll explore what insights this card can give you into your
builds, as well as some fun details about how the card works under the
hood.

<!-- truncate -->

![](/img/blog/cache-requests-card.png)

## Some history: the cache misses card

Before the cache requests card existed, we had a "cache misses" card. It
was our first attempt at giving per-request visibility into a build's
remote cache activity, and it looked like this:

![](/img/blog/cache-misses-card.png)

This card contained a list of Bazel targets, along with their actions that
were not found in BuildBuddy's remote cache, meaning that Bazel needed to
execute the action in order to get its outputs.

More specifically, it showed misses from the **Action Cache (AC)**, which
just holds **action results**. Action results basically just contain some
pointers to the outputs for a particular build step.

When using remote execution, you could even click individual actions to
explore the action as well as the action result:

![](/img/blog/action-details-card.png)

The cache misses card used only a small subset of the request metadata
that Bazel sends to us, and had limited interactivity. There was more we
could do to let you answer more questions about your build's cache
usage.

## The cache requests card

Bazel sends up lots of useful metadata for every cache request, not just
for action cache (AC) requests, but also for content-addressable storage
(CAS) requests, which holds build artifacts such as action input and
output files, among other things.

CAS requests are where the bulk of the time is being spent transferring
data over the network, and the payload size can be much larger and much
more variable than action cache requests. So in the new cache requests
card, we made CAS requests visible, and we made the payload size visible.

When Bazel sends us build info via the build event stream (BES), it sends
us some other interesting cache request metadata as well. For example, it
sends us file names for some files that it uploads which aren't
necessarily associated with actions, such as the timing profile (which is
displayed in the "Timing" tab). We can use this metadata to show proper
names for these types of requests.

Further, there is some data that we compute for each request on our side,
such as timing (start time / duration), compressed size (when using
compression [added in Bazel 5.0](whats-new-in-bazel-5.0)), and RPC
response code (such as `OK` or `NotFound`). All of this data is useful, so
in the new card, we made all of this data visible as well.

All of this data is sortable and searchable, so that you can answer
specific questions and find bottlenecks more easily.

## New insights

The cache requests card lets you answer some interesting questions about
your build which were not easily answerable before. Here are a few:

_**My build seems to have uploaded a lot of data &mdash; which targets
uploaded the largest artifacts?**_

To answer this question, select "Sort by **Size**" and "Show **All**".
Then, select "Group by **None**". This will show the largest artifacts
first, with target names displayed in the leftmost column.

![](/img/blog/cache-requests-card.png)

We can see in this screenshot that the largest cache transfer was a
download of an artifact from the external repository
`com_github_tuist_xcodeproj`. To see the full target and action name,
hover over the row.

_**I see some file references in the build event stream which aren't
associated with an action. What are these files?**_

To answer this question, select "Show **All**" and search for
**bes-upload**. You'll see all the files which were uploaded by Bazel
and not associated with an action, including the timing profile.

![](/img/blog/cache-requests-card-bes-upload.png)

The build in this screenshot shows that a large artifact (285.9 MB) was
uploaded at the very end of the build, so it most likely was blocking the
build's completion. We can see the full artifact path by hovering over the
row.

_**I expected my build to be fully-cached, but it was not. Was there a
single action whose inputs or environment variables changed unexpectedly,
thus triggering all its dependent targets to be rebuilt?**_

To answer this question, make sure you are sorted by **start time** in
ascending order, and take a look at the actions with the earliest
timestamps. The earliest action is most likely the one that changed from
the previous build.

![](/img/blog/cache-requests-card-incremental-rebuild.png)

Before the build in this screenshot, a file in the
`priority_task_scheduler` target was edited, which we can see triggered a
cascade of action executions which transitively depended on
`priority_task_scheduler`.

## How it works

Implementing the cache requests card required solving a few interesting
problems.

The total size of the request metadata stored for each build is not
extremely large &mdash; just tens of megabytes for builds with millions of
cache requests &mdash; but we serve a high volume of requests, and we
don't want to negatively impact cache performance just to store this
metadata for each request.

The simplest solution to implement would be to do a blocking write to a
MySQL table for each cache request. This would also be pretty convenient
for querying the data however we like. However, this would place far too
much load on the database and add way too much latency to each cache
request.

So, instead of using MySQL, we used Redis as an intermediate storage
medium while the invocation is in progress. Redis can handle a much higher
volume of writes than MySQL because it only stores values in memory and it
has a much simpler key-value storage model.

We can't just store this data in Redis and call it day, though. Firstly,
Redis does not give us long-term persistence, and it would be nice to be
able to keep this data around even for older invocations. To get long-term
persistence, we read all of the data from Redis and then serialize it into
a [proto](https://developers.google.com/protocol-buffers). We store this
proto into a "blobstore," which is just a generic storage interface backed
by a local disk, Google Cloud Storage, Amazon S3, etc.

Secondly, even with the amazing performance of Redis, we can't just issue
a single write request for every cache request. Doing a separate Redis
write for every request places a large amount of CPU load on Redis, since
it needs to do a `read()` and `write()` system call for each write. To
address this, we used Redis
[**pipelining**](https://redis.io/docs/manual/pipelining/). Instead of
issuing Redis commands directly, we add each command to a pipeline, and
have a separate background job that periodically flushes the pipeline.
Adding the command to the pipeline is just a matter of appending to an
in-memory buffer, which takes just nanoseconds, so it doesn't impact cache
performance to a significant degree.

![](/img/blog/cache-requests-design-1.png)

Once an invocation is complete, we kick off a job to read all of the
requests from Redis and then store it as a single blob in blobstore.

![](/img/blog/cache-requests-design-2.png)

To read back this data for the UI, all we have to do is load this whole
blob into memory and then apply any client-side sorting and filtering.
These blobs are small enough that we easily load the full blob into memory
on the server &mdash; the blob is too big to be loaded in a browser,
though, so we do use a relatively small page size.

![](/img/blog/cache-requests-design-3.png)

## What's next

We hope that you find the new cache requests card useful and that you
enjoyed reading about how it works! We would love to hear your feedback,
which will help inform how we design the next iteration of our cache
debugging tools to help make your builds even faster and more scalable.
Join our [Slack channel](https://slack.buildbuddy.io) or email us at
<hello@buildbuddy.io> with any questions, comments, or thoughts.
