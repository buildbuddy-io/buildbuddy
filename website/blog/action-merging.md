---
slug: action-merging
title: Action Merging
description: Learn all the details about BuildBuddy's action deduplication mechanism
authors: iain
date: 2025-12-11:12:00:00
image: /img/blog/action-merging.webp
tags: [product, engineering, performance]
---

BuildBuddy's remote execution engine merges in-flight executions of identical actions to save our users time and resources. While simple in theory, this poses some interesting engineering challenges. In this blog post, we'll explain why action merging is important, how it works, and some fun stuff we've learned over the years running our implementation in production.

<!-- truncate -->

## Overview

Caching action results is one of the performance optimizations at the heart of Bazel. Unfortunately, none of Bazel's caching mechanisms provide a mechanism for sharing the results of pending executions. Concretely, that means that if Alice submits a backend change that requires running BigSlowTest, then Bob submits a small frontend change while Alice's BigSlowTest is still running, Bob will have to run BigSlowTest too. Obviously this makes Bob's invocation slower, but if BigSlowTest runs on a simulator that requires a license, or uses quota from some external service, then Bob's invocation will unnecessarily consume these resources as well as his time.

![](/img/blog/action-merging-timeline.webp)
_A timeline of Alice and Bob's unmerged BigSlowTest executions._

While this may sound a bit contrived, we've heard it from many customers over the years. So, we wanted to share some details about how BuildBuddy helps users avoid these unnecessary long-running executions.

## Background

### Actions and Executions

Before going into the details, let's cover some basics. "Actions" are the units of work Bazel uses to compose invocations. They are identified by an "Action ID" which is a hash of the action's definition and its inputs. An example action is a go_test. An "execution" is a run of a single action that is uniquely identified by an "Execution ID." There will always be exactly one action per execution, but there could be zero, one, or many executions of an action.

![](/img/blog/action-merging-definitions.webp)
_An example of a simple genrule action and an execution of that action._

### BuildBuddy

BuildBuddy's remote cache and remote execution is primarily implemented by two services: stateful "apps" that store and serve the cache, interact with databases, and perform scheduling; and stateless "executors" that run remote executions. BuildBuddy uses the [Sparrow scheduling algorithm](/blog/distributed-scheduling-for-faster-builds) to schedule remote executions. Each executor maintains a queue of tasks to execute and, when a task reaches the head of the queue, the executor tries to claim and runs it if the claim was successful. Leased execution tasks are recorded in the invocations database so they're visible in the UI. When the execution finishes, that's recorded along with statistics and other finalization data in the database.

![](/img/blog/action-merging-architecture.webp)
_A simple overview of BuildBuddy's app-and-executor architecture and how scheduling works._

## Details

Going back to Alice and Bob, their two BigSlowTests have the same Action ID but different Execution IDs. Bob could reuse Alice's BigSlowTest by just waiting on her Execution ID. At the simplest level, that's how action merging works. The devil is in the details, though.

As we dig into the edge cases, let's call Alice's execution the "canonical" execution and Bob's the "merged" execution.

### Queued Executions

The simple implementation above doesn't allow merging against queued executions becuase they're not stored in the invocations database. This wouldn't be a problem deal if we only cared about the time it takes to run BigSlowTest. Queue times should generally be short. But Bob's unnecessary run could use simulators or external service quota, and that matters a lot for some customers.

To address this, we store all action merging state in Redis. This consists of a bidirectional mapping between the Action ID and the canonical Execution ID for each pending execution, as well as a few other things. Because executions can disappear from the scheduler (for example, if the invocation is cancelled), we keep the TTLs on this state short to prevent merging against nonexistant executions.

When an executor claims an execution, the scheduler updates the Redis action merging state to indicate the task has been claimed. It also increases the TTL and then periodically updates it as part of the app / executor healthcheck process. This action merging state is deleted when the canonical execution finishes and the action result is stored in the cache. At this point, subsequent invocations can use that action result, no merging required.

### Cancelled Invocations

Here's another hypothetical: if a unit test fails as part of Alice's invocation and she cancels the entire run, what happens to Bob's action, which was merged against Alice's now-cancelled invocation? We address this by reference-counting actions that have merged against a given execution and only cancelling executions with no waiting actions. Our implementation cuts a corner here in that we never decrement the action merging reference count. That would require tracking state about all of the merged executions, not just the canonical one and this isn't quite common enough to warrant that complexity. So, if Alice and Bob both cancel their invocations, the canonical execution will run to completion even though no one is waiting for it.

### Stalled Executions

After running action merging in production for a few years, we heard occasional complaints from customers about stuck executions. It turns out not all Bazel rules are hermetic and reproducible, so sometimes actions can encounter deadlock, hang on external dependencies, or get stuck for other mysterious reasons. If this happens to an execution that has been merged against, all subsequent invocations of that action will be stuck for as long as the execution is allowed to run (hours, in the worst case). Not good!

The first few times we observed this, we manually fixed the issue by deleting the action merging state from Redis to unblock subsequent invocations. Obviously this wasn't a long-term solution, so we added support for "hedging" merged executions to alleviate the impact of stuck executions. The idea is simple: if a canonical execution runs for "too long" or is merged against "too many times," we dispatch a hedged execution that races against the canonical execution. The finish line of this race is writing the action result to the action cache. That means hedging doesn't perfectly address the issue, specifically in-flight invocations that are merged against the canonical execution remain stuck, but it helps a lot by unblocking subsequent invocations after the hedged execution finishes.

![](/img/blog/action-merging-hedging.webp)
_How hedging unblocks merges against stuck executions._

### Shards

When we added support for hedging, we also added a few metrics tracking action merging performance. We found that in some cases, hundreds of invocations merged against a single canonical execution, and action merging saved hours for some invocations. This was really encouraging to see! But over time, as our deployment grew, we saw these gains decline. Initially, we chalked this up to changes in traffic patterns, but after taking a closer look, Brandon found that our action merging Redis client was configured incorrectly. Specifically, it was only reading and writing action merging state from a single Redis shard instead of the entire Redis deployment. This meant that the effectiveness of action merging decreased proportionally to the size of our Redis deployment. Whoops!

Fortunately this was an [easy fix](https://github.com/buildbuddy-io/buildbuddy/pull/10257). We switched to use the correct client library and saw the effectiveness of action merging jumped to the highest levels ever. Nowadays, we see hundreds of actions merged per second at peak.

## Future Improvements

I mentioned some fixes and optimizations above, like correctly reference counting merged invocations to improve the correctness of cancellation. But there are other enhancements we'd like to make to action merging too. First, one downside of action merging is that it can amplify the negative impacts of flaky tests. If a canonical execution fails due to a flake, all merged executions will fail too. We already have information about flaky tests, so we would like to use this to be a bit smarter about merging and hedging. Second, we would like to be able to use the result of hedged executions for merged actions, to unblock all of those pesky stuck executions. Finally, we'd like to show a bit more information about action merging in the BuildBuddy UI.

## Conclusion

Action merging saves our users time and resources by deduplicating identical in-flight executions of an action. But, like most distributed system designs, it's tricky to get right and there are some interesting corner cases. We hope this blog post helps understand why this feature is important and how it works. If you want to learn more, we recommend checking out [Christian Scott's talk on this topic at Bazelcon, 2024](https://www.youtube.com/watch?v=zZB_Q-BKJ04&list=PLbzoR-pLrL6ptKfAQNZ5RS4HMdmeilBcw&index=6), or reaching out via [Slack](https://buildbuddy.slack.com/) or [Email](mailto:hello@buildbuddy.io). And if you made it this far and found this interesting, [we're hiring](https://www.buildbuddy.io/careers)!
