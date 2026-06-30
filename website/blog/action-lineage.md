---
slug: action-lineage
title: Bazel Action Lineage
description: Track the origin of action cache hits with BuildBuddy.
authors: son
date: 2025-12-17:12:00:00
image: /img/blog/action-lineage.png
tags: [bazel, remote-cache, engineering]
---

When you use BuildBuddy's remote cache, we record the requests Bazel sends to our server and display them in the cache
requests card. This includes Action Cache (AC) and content-addressable storage (CAS) requests, along with their status:
download miss, download hit, and upload.

When Bazel gets an Action Cache hit, it skips executing the action and instead downloads the outputs referenced in the
cached Action Result.

<!-- truncate -->

But have you ever wondered where that cached result came from? Which invocation produced the Action Cache entry you just
hit?

## Introducing action lineage

BuildBuddy now records the origin invocation for Action Cache entries written to the cache. When you get an AC hit, you
can see which invocation created that cache entry directly in the cache requests card.

![](/img/blog/action-lineage.png)

Because we attach lots of metadata to each invocation, you can now answer questions like:

- Which git commit previously built the `//my:server_binary` target?
- Which user triggered that build, and at what time?
- Which Bazel flags were used to build this action, and which Bazel version was it?

In other words, you can trace the parent invocations your build is inheriting Action Results from. We call this feature
Action Lineage.

## How does it work?

This is enabled by the tight integration between our remote cache and the invocation UI.

On the write path, when Bazel finishes running an action, it calls the `UpdateActionResult` gRPC endpoint to upload the
Action Result to our cache.

```protobuf
rpc UpdateActionResult(UpdateActionResultRequest) returns (ActionResult) {...}

message UpdateActionResultRequest {
  ...
  // Action Cache key
  Digest action_digest = 2;

  // Action Cache value
  ActionResult action_result = 3;
  ...
}
```

Typically, we store this key-value pair in our remote cache as-is.
With Action Lineage enabled, before writing the Action Result, we add additional metadata to indicate the origin invocation.
Currently, that's a small proto message that contains the invocation ID that uploaded the Action Result.

```protobuf
message OriginMetadata {
  string invocation_id = 1;
}
```

We insert this proto message into the `ActionResult.execution_metadata.auxiliary_metadata` field, which can store
arbitrary metadata via the protobuf `Any` type.

```protobuf
message ActionResult {
  ...
  ExecutedActionMetadata execution_metadata = 9;
}

message ExecutedActionMetadata {
  ...
  repeated google.protobuf.Any auxiliary_metadata = 11;
}
```

When we serve an Action Result, we look for the `OriginMetadata` we previously inserted. If found, we add this
information to our ScoreCard data structure, which we use to keep track of cache requests for each invocation.

```protobuf
message ScoreCard {
  message Result {
    string action_mnemonic = 1;
    string target_id = 2;
    string action_id = 3;

    RequestType request_type = 5;
    google.rpc.Status status = 6;

    ...

    // The invocation that originally uploaded this result to the action cache.
    string origin_invocation_id = 17;
  }

  repeated Result misses = 1;
  repeated Result results = 2;
}
```

Our UI then reads the ScoreCard containing multiple cache requests.
If an AC hit includes an origin invocation ID, we show it in the UI.

## What's next

As tempting as it might be, it's worth noting that this is a troubleshooting / build telemetry feature, not a security
feature. The origin invocation data can be faked by a malicious client, so you should keep Action Cache write permission
restricted to trusted clients.

That said, this demonstrates a clear path to improving supply-chain security in the Bazel ecosystem: Action Cache
provenance is possible. With a similar pattern, Action Cache signing and verification could be possible too.

Imagine being able to cryptographically verify the origin of all the actions within a fully cached Bazel build.
You can be fast, correct, _and_ **secure**!

If you're interested in such a feature, join our [Slack channel](https://community.buildbuddy.io) or email us at
[hello@buildbuddy.io](mailto:hello@buildbuddy.io).

Happy building!

Son Luong
