---
slug: action-merging
title: Action Merging
description: Learn all the details about BuildBuddy's action deduplication mechanism
authors: iain
date: 2025-12-02:12:00:00
image: /img/blog/action-merging.webp
tags: [product, engineering, performance]
---

BuildBuddy's remote caching and remote execution engine merges in-flight executions of identical actions to save our users time and resources. While simple in theory, in practice this poses some interesting engineering challenges. In this blog post, we'll explain why action merging is important, how it works, and some fun stuff we've learned over the years running our implementation in production.

<!-- truncate -->

## Overview

Sharing the results of unchanged compile, link, test, or other "actions" is one of the performance optimizations at the heart of Bazel. One of the main purposes of Bazel's local and remote caches is to distribute these shareable build and test artifacts across machines and environments. Unfortunately this falls short for in-flight executions because the cache doesn't offer an affordance for sharing the result of a pending execution across invocations.

Concretely, that means that if Alice submits a backend change that requires running BigSlowTest, then twenty minutes later Bob submits a small javascript change while Alice's BigSlowTest invocation is still running, Bazel won't know about Alice's BigSlowTest run (because it's not yet in the cache) so it will kick off a fresh BigSlowTest for Bob. Obviously this is slower than it could be because the test result should be the same and Alice has a twenty minute head start. But if BigSlowTest runs on a simulator that requires a license, or uses quota from some external service, then Bob's invocation will unnecessarily consume these resources. This problem compounds as BigSlowTest gets slower because the window during which new runs are needed grows. And if BigSlowTest is flaky? Well, Alice and Bob might as well head out for an early lunch.

![](/img/blog/action-merging-timeline.webp)
_A timeline of Alice and Bob's unmerged BigSlowTest executions._

While this may sound a bit contrived, we have heard it from many customers over the years. So, we wanted to share some details about what BuildBuddy does to help our users avoid unnecessary long-running executions.

## Background

### Actions and Executions

Before going into the details of BuildBuddy's action merging implementation, we need to cover some basics. Let's start with the concepts in play. "Actions" are the units of work Bazel uses to compose invocations. Examples of actions include genrules, individual compilations or links, or test rules in your favorite language. Actions are identified by an "Action ID" which is a hash of the action's definition and its inputs. An "execution" is a run of a single action that is uniquely identified by an "Execution ID." There will always be exactly one action per execution, but there could be zero, one, or many executions of an action.

![](/img/blog/action-merging-definitions.webp)
_An example of a simple genrule action and an execution of that action._

### BuildBuddy

BuildBuddy's remote cache and remote execution is primarily implemented by two services: stateful "apps" that store and serve the cache, interact with databases, coordinate remote execution workers, and serve the web frontend; and stateless "executors" that perform remote execution. BuildBuddy uses the [Sparrow scheduling algorithm](/blog/distributed-scheduling-for-faster-builds) to schedule remote executions. Each executor maintains its own priority queue of tasks to execute and, when a task reaches the head of the queue, the executor tries to lease it from the app. When an app receives new remote execution requests, it enqueues the pending execution on multiple executors and whichever executor dequeues the task first gets to run it. Once execution tasks are leased, they are recorded in the executions table in the invocations database so the frontend can show details about the pending execution. When the execution finishes, that's recorded along with statistics and other finalization data in the database.

![](/img/blog/action-merging-architecture.webp)
_A simple overview of BuildBuddy's app-and-executor architecture and how scheduling works._

There are a few key details that are important to call out for later: first, queued executions are not stored in the invocations database, only in-flight and finished executions are stored. This helps us reduce load on MySQL a bit because we don't need information about queued executions. Second, BuildBuddy executors are intentionally stateless and don't coordinate directly with one another. This is a design decision for scalability. Finally, we have to coordinate across app servers so that we can correctly serve LeaseTask requests in our distributed environment (among other things). We do this using a sharded Redis deployment to store temporary state for communication between apps.

## Details

Let's go back to Alice and Bob as we get into the nitty gritty details. With our new understanding of actions and executions, you might realize that both Alice and Bob's runs of BigSlowTest will have the same Action ID but different Execution IDs. As a simple improvement, we can re-use Alice's in-flight execution for Bob's BigSlowTest action -- it's already in the executions database after all! At the simplest level, that's how action merging works. The devil is in the details, though.

As we dig into the edge cases, let's call Alice's execution the "canonical" execution and Bob's the "merged" execution.

### Queued Executions

Remember how queued executions aren't stored in the executions database?
That means that if executions are queuing before executing and Alice's and Bob's executions are in the queue at the same time, they can't be merged just using state from the executions database. This wouldn't be such a big deal if we only cared about the time it takes to run BigSlowTest. Queue times should generally be short enough that Bob's BigSlowTest should finish shortly after Alice's. But Bob's unnecessary run could use simulators or external service quota, and that matters a lot for some customers.

To address this, we store all action merging state in Redis. This consists of a bidirectional mapping between the Action ID and the canonical Execution ID for each pending execution, as well as a few other things. Because executions can disappear from the scheduler (for example, if the invocation is cancelled), we keep the TTLs on this state short to prevent merging against executions that don't exist.

When an executor claims an execution, the scheduler overwrites the action merging state in Redis to indicate the task has been claimed. It also overwrites the TTL and then periodically updates it as part of the app / executor healthcheck process. This action merging state is deleted when the canonical execution finishes and the action result is stored in the cache. At this point, subsequent invocations can use that action result, no merging required.

### Cancelled Invocations

Here's another hypothetical: if a unit test fails as part of Alice's presubmit and she cancels the invocation to fix it, what happens to Bob's action, which was merged against Alice's now-cancelled invocation? Sure, Bob's BigSlowTest is going to fail, but he might want it to at least run so he knows about the failure.

We address this by keeping a count of actions that have merged against a given execution and only cancelling executions that haven't been merged against. Our implementation cuts a corner here in that we never decrement the action merging reference count. That would require tracking state about all of the merged executions (like Bob's), not just the canonical one (Alice's) and this isn't quite common enough to warrant that complexity. So, if both Alice and Bob cancel their invocation, the canonical execution will run to completion even though no one is waiting for it.

### Stalled Executions

After running action merging in production for a few years, we heard occasional complaints from customers about stuck executions. It turns out not all Bazel rules are hermetic and reproducible, so sometimes actions can encounter deadlock, hang on external dependencies, or get stuck for other mysterious reasons. If this happens to an execution that has been merged against, all subsequent invocations of that action will be stuck for as long as the execution is allowed to run (hours, in the worst case). Not good!

The first few times we observed this, we manually fixed the issue by deleting the action merging state from Redis to unblock subsequent invocations. Obviously this wasn't a long-term solution, so we added support for "hedging" merged executions to alleviate the impact of stuck executions. The idea is simple: if a canonical execution runs for "too long" or is merged against "too many times," we dispatch a hedged execution that races against the canonical execution. The finish line of this race is writing the action result to the action cache. That means hedging doesn't perfectly address the issue, specifically in-flight invocations that are merged against the canonical execution remain stuck, but it helps a lot by unblocking subsequent invocations after the hedged execution finishes.

![](/img/blog/action-merging-hedging.webp)
_How hedging unblocks merges against stuck executions._

### Shards

When we added support for hedging, we also added a few metrics tracking action merging performance. We found that in some cases, hundreds of invocations merged against a single canonical execution, and action merging saved O(hours) for some invocations. This was really encouraging to see! But over time, as our deployment grew, we saw these gains decline. Initially, we chalked this up to changes in traffic patterns, but after taking a closer look, Brandon found that our action merging client was configured incorrectly. Specifically, it was only reading and writing action merging state from a single Redis shard instead of the entire Redis deployment. This meant that the effectiveness of action merging decreased proportionally to the size of our Redis deployment. Whoops!

Fortunately this was an [easy fix](https://github.com/buildbuddy-io/buildbuddy/pull/10257). We switched to use the correct client library and saw the effectiveness of action merging jumped to the highest levels ever. Nowadays, we see hundreds of actions merged per second at peak.

## Future Improvements

I mentioned some fixes and optimizations above, like reference counting merged invocations to improve the correctness of cancellation. But there are other enhancements we'd like to make to action merging too. First, one downside of action merging is that it can amplify the negative impacts of flaky tests. If a canonical execution fails due to a flake, all merged executions will fail too. We already have information about flaky tests, so we would like to use this to be a bit smarter about merging and hedging. Second, we would like to be able to use the result of hedged executions for merged actions, when the hedged execution finishes before the canonical execution. Finally, we'd like to show more information about action merging in the BuildBuddy UI.

## Conclusion

Action merging saves our users time and resources by deduplicating identical in-flight executions of an action. But, like most distributed system designs, it's tricky to get right and there are some interesting corner cases. We hope this blog post helps understand why this feature is important and how it works. If you want to learn more, we recommend checking out [Christian Scott's talk on this topic at Bazelcon, 2024](https://www.youtube.com/watch?v=zZB_Q-BKJ04&list=PLbzoR-pLrL6ptKfAQNZ5RS4HMdmeilBcw&index=6), or reaching out via [Slack](https://buildbuddy.slack.com/) or [Email](mailto:hello@buildbuddy.io). And if you made it this far and found this interesting, [we're hiring](https://www.buildbuddy.io/careers)!
