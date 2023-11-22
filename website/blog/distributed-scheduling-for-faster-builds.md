---
slug: distributed-scheduling-for-faster-builds
title: Distributed Scheduling for Faster Builds
description: The distributed scheduler that powers BuildBuddy remote execution
author: Tyler Williams
author_title: "Co-founder @ BuildBuddy"
date: 2022-04-07:08:00:00
author_url: https://www.linkedin.com/in/tyler-williams-80480519b/
author_image_url: https://avatars.githubusercontent.com/u/141737?v=4
image: /img/distributed-scheduling.png
tags: [engineering, go, sparrow, scheduler]
---

Let's start with "what's BuildBuddy" for the kids in back. In short, we provide a UI, distributed cache, and remote execution platform for your Bazel builds. That means we securely compile your code, cache the artifacts, and help you visualize the results. We make it possible to build projects like Tensorflow from your laptop in under 5 minutes instead of 90 minutes.

Obviously to do all this, we have to handle some thorny engineering challenges, one of which is scheduling remote executions. For that, we have a scheduler. The scheduler just matches actions (basically jobs) received by our API to remote workers that actually do the work. If you think of a full build of something like Tensorflow as a 10 course meal, a single action is like a recipe for a tiny part of that meal. To make it easier to visualize, here's a real action from building BuildBuddy:

<!-- truncate -->

```bash
# this action just uses gcc to compile zlib/infback.c into an object file, zlib/infback.o
/usr/bin/gcc -U_FORTIFY_SOURCE -fstack-protector -Wall -Wunused-but-set-parameter \
  -Wno-free-nonheap-object -fno-omit-frame-pointer -g0 -O2 -D_FORTIFY_SOURCE=1 \
  -DNDEBUG -ffunction-sections -fdata-sections -MD -MF \
  bazel-out/k8-opt-exec-34F00540/bin/external/zlib/_objs/zlib/infback.d \
  -frandom-seed=bazel-out/k8-opt-exec-34F00540/bin/external/zlib/_objs/zlib/infback.o \
  -iquote external/zlib -iquote bazel-out/k8-opt-exec-34F00540/bin/external/zlib \
  -isystem external/zlib/zlib/include \
  -isystem bazel-out/k8-opt-exec-34F00540/bin/external/zlib/zlib/include -g0 \
  -Wno-unused-variable -Wno-implicit-function-declaration -fno-canonical-system-headers \
  -Wno-builtin-macro-redefined -D__DATE__="redacted" -D__TIMESTAMP__="redacted" \
  -D__TIME__="redacted" -c external/zlib/infback.c -o \
  bazel-out/k8-opt-exec-34F00540/bin/external/zlib/_objs/zlib/infback.o
```

And here's another:

```bash
# this action uses protobufjs to generate typescript bindings for a protobuf file
bazel-out/host/bin/external/npm/protobufjs/bin/pbts.sh
--out=bazel-out/k8-fastbuild/bin/proto/buildbuddy_service_ts_proto.d.ts
bazel-out/k8-fastbuild/bin/proto/buildbuddy_service_ts_proto.js
--bazel_node_modules_manifest=bazel-out/k8-fastbuild/bin/proto/__buildbuddy_service_ts_proto_pbts.module_mappings.json
```

So you get the idea, building a binary involves compiling and linking many different libraries etc and a single action is usually one of those commands. Great.

So let's say we have hundreds or thousands of these actions hitting our API at a time, and a pool of remote workers ready to run them. Let's get to it. If you're like me, your first thought here is _load balancer_. I mean, why not? These are just requests that need to get to a pool of machines, and load balancers are a really common, well understood way to do this.

In the very early days, this is what we did. We ran nginx in front of a pool of executors. And it kind of worked! But the results were... lumpy. In a typical web application, most requests served by load balancers are pretty homogeneous. They are static file lookups or simple page actions that all usually take under a second to serve. But our requests were very heterogenous: the fastest ones were simple gcc compile commands that generated a single object file and could finish in 10s of milliseconds. The slowest ones were gigantic slow link statements that required every single object file and took 30+ seconds to link. Or even worse, test actions that ran a unit test binary and took 30+ minutes to run.

What happened is that the load balancer would assign tasks in a round robin fashion and put two large tasks on the same worker, while other workers were sitting there idle, and the build would take much longer than it should.

I should mention that I really wanted this to work, because I didn't want to write a scheduler. It seemed complex and risky. I would much rather use something simple and well tested than build a custom critical piece of infrastructure like a scheduler. In fact I went through all kinds of machinations to avoid doing this, from using different load balancing techniques like least loaded or exponential weighted moving average (EWMA) to trying to split the work into multiple load balancer targets, to using the Kubernetes scheduler.

And in the end, nothing worked well. The core reason a load balancer was such a poor fit here is that it didn't know the size of the requests it was routing. It seemed possible to give the load balancer some hints about this, but it would have meant writing our own load balancing algorithm, or relying on a very complex routing configuration. Neither option sounded appealing, and both negated my whole rationale for using a load balancer in the first place, namely that they are fast and easy.

So I went and did some reading about schedulers, and found this really lovely paper [https://cs.stanford.edu/~matei/papers/2013/sosp_sparrow.pdf](https://cs.stanford.edu/~matei/papers/2013/sosp_sparrow.pdf) about a distributed scheduler called Sparrow. This paper is great because it's short and clearly written, and it talks about some cool ideas. The biggest idea is the power of two choices.

:::note

[The Power of Two Choices in Randomized Load Balancing](https://www.eecs.harvard.edu/~michaelm/postscripts/mythesis.pdf)

In a typical load balancer, some metrics are kept about how loaded a worker is based on how long it takes to serve a request, and then the load balancer uses those metrics to decide where to assign new requests. The metrics are stale though, being based on past requests, which leads to some non-optimal behavior. A better way to assign requests is just to pick two random workers and assign to the least loaded of the two. This leads to an exponential improvement in the maximum load.

:::

Sparrow modifies two-choices slightly, and also introduces the idea of Late-Binding. In many schedulers, each worker maintains a queue and the scheduler tries to assign work to the worker with the shortest queue. The problem with this, and one of the core reasons it doesn't give good performance on our workloads, is that queue length is not a good indicator of how long a task will take. Late-binding solves this by enqueuing a task on multiple workers, and then the first worker to get to the task takes it. This effectively avoids the problems of huge single tasks blocking other work.

Another reason this paper is so great is because the authors actually implemented Sparrow, ported Apache Spark to use it, and then analyzed the results. Side note: I wish more papers were like this! So fun to read.

Sparrow is a _distributed_ scheduling algorithm, so no single node holds the entire state of the world. This is really important for us at BuildBuddy because we want our infrastructure to be resilient. When an API server or backend worker restarts or goes down, for whatever reason, we don't want it to impact customer builds.

So with this paper as a basis, I went and implemented Sparrow in Go, which was not nearly as hard as I'd made it out to be. It maybe took a few days, which was far less time than I'd spent trying to find alternative solutions. The lead author of the paper was even happy to help clarify things over email. And when I replaced our load balancer with it, the results were pretty much instantly far better than anything we'd had before. No longer were builds timing out because of unequal load distribution. Even better, due to the distributed nature of the algorithm, we could run our workers on cheap preemptible compute machines that are often restarted and not worry about losing tasks. (These machines are less than half the cost!)

We still use this scheduler in production today, and it's working well for us. We've made some modifications to the way requests are routed, but fundamentally this algorithm is the same one in the paper, the same one powering all remote execution on BuildBuddy today.

If you want to learn more about BuildBuddy or try it, check out [our docs](https://www.buildbuddy.io/docs/introduction/). And if you enjoy this kind of problem solving and engineering work, [we’re hiring](https://www.buildbuddy.io/careers/)!
