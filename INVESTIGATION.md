# Execution ID Compression Investigation

Status: in progress

## Goal

Trace the full `Execute` RPC lifecycle end-to-end and verify whether a normal execution can ever acquire an `execution_id` containing `compressed-blobs`.

More specifically:

- Prove where `execution_id` is minted.
- Prove which components merely copy it.
- Identify any current-code path that could still cause `compressed-blobs/zstd` to appear in `execution_id`.

## Working hypothesis

Initial expectation:

- A normal `Execute` request should mint an identity CAS resource name.
- That CAS resource should be serialized into the execution ID via `NewUploadString()`.
- Scheduler, executor, and `PublishOperation` should treat the execution ID as an opaque string and pass it through unchanged.
- Therefore, if `compressed-blobs` appears in `execution_id`, the important question is not "did the executor invent it?" but "where did the chosen execution ID string come from?"

## Findings

### 1. Fresh `Execute` requests start from an identity CAS resource name

`ExecutionServer.execute()` constructs:

```go
adInstanceDigest := digest.NewCASResourceName(
    req.GetActionDigest(),
    req.GetInstanceName(),
    req.GetDigestFunction(),
)
```

`NewCASResourceName(...)` delegates to `NewResourceName(...)`, and `NewResourceName(...)` initializes `Compressor` to `IDENTITY`.

Evidence:

- `enterprise/server/remote_execution/execution_server/execution_server.go:978`
- `server/remote_cache/digest/digest.go:124-145`

Conclusion:

- A normal `ExecuteRequest` does not bring a compressor field for the action digest.
- The first action CAS resource name created in the `Execute` path is identity, not compressed.

### 2. `NewUploadString()` only emits `compressed-blobs` if the resource is already ZSTD

`CASResourceName.NewUploadString()` emits:

- `.../blobs/...` for identity
- `.../compressed-blobs/zstd/...` for `ZSTD`

This is controlled by `blobTypeSegment(r.GetCompressor())`.

Evidence:

- `server/remote_cache/digest/digest.go:258-266`
- `server/remote_cache/digest/digest.go:639-643`

Conclusion:

- `NewUploadString()` does not infer or inject compression on its own.
- If an execution ID contains `compressed-blobs/zstd`, then the source `CASResourceName` already had `Compressor_ZSTD` at serialization time.

### 3. Fresh execution IDs in the normal `Execute` path are minted from identity CAS resource names

Fresh execution ID minting sites in `execution_server.go`:

- Cached-result path:
  - `executionID := adInstanceDigest.NewUploadString()`
- Main scheduling path:
  - `action_merger.GetOrCreateExecutionID(..., adInstanceDigest, ...)`
- Hedged path:
  - `hedgedExecutionID := adInstanceDigest.NewUploadString()`
- Tee path:
  - `r := digest.NewCASResourceName(...)`
  - `newExecutionID := r.NewUploadString()`

Evidence:

- `enterprise/server/remote_execution/execution_server/execution_server.go:988-993`
- `enterprise/server/remote_execution/execution_server/execution_server.go:1006`
- `enterprise/server/remote_execution/execution_server/execution_server.go:1043-1048`
- `enterprise/server/remote_execution/execution_server/execution_server.go:688-691`
- `enterprise/server/remote_execution/action_merger/action_merger.go:260-263`

Conclusion:

- Fresh execution IDs created by the normal current-code `Execute` path should serialize as `.../blobs/...`, not `.../compressed-blobs/zstd/...`.

### 4. I do not see in-place compressor mutation in the execution ID minting path

I checked the execution-ID minting packages for explicit compressor mutation:

- `enterprise/server/remote_execution/execution_server`
- `enterprise/server/remote_execution/action_merger`

There are no `SetCompressor(...)` call sites there.

Additional sanity checks:

- `getActionResultFromCache(...)` validates cached results but does not mutate the passed CAS resource.
- `fetchAction(...)` uses the resource for reads only.

Evidence:

- No `SetCompressor(...)` matches in:
  - `enterprise/server/remote_execution/execution_server/execution_server.go`
  - `enterprise/server/remote_execution/action_merger/action_merger.go`
- `enterprise/server/remote_execution/execution_server/execution_server.go:588-597`
- `enterprise/server/remote_execution/execution_server/execution_server.go:1589-1604`

Conclusion:

- I do not currently see a code path in the normal `Execute` minting flow that flips the action CAS resource from identity to ZSTD before `NewUploadString()` is called.

### 5. There appears to be only one production scheduler-task creation path in repo

A repo-wide search for `ScheduleTaskRequest{` found one production constructor:

- `execution_server.dispatch()`

The other matches are tests.

Evidence:

- `enterprise/server/remote_execution/execution_server/execution_server.go:954-958`
- `enterprise/server/scheduling/scheduler_server/scheduler_server_test.go:678`

Conclusion:

- In the current repo, production scheduler tasks appear to be created centrally in `execution_server.dispatch()`.
- I do not see a second production scheduling path minting hidden compressed execution IDs.

### 6. `dispatch(...)` copies the already-chosen execution ID into the task unchanged

When dispatching, `execution_server` writes the same `executionID` string into:

- `ExecutionTask.ExecutionId`
- `ScheduleTaskRequest.TaskId`

Evidence:

- `enterprise/server/remote_execution/execution_server/execution_server.go:759-766`
- `enterprise/server/remote_execution/execution_server/execution_server.go:949-958`

Conclusion:

- `dispatch(...)` is a pass-through for the chosen execution ID.
- If the string is clean before `dispatch(...)`, it stays clean here.

### 7. The scheduler stores and returns the task ID as an opaque string

The scheduler:

- accepts `ScheduleTaskRequest.TaskId`
- stores the serialized task bytes
- stores the Redis task keyed by `taskID`
- later unmarshals and leases that same serialized task

The experiment hook `modifyTaskForExperiments(...)` changes task metadata / platform properties but does not touch `ExecutionId`.

Evidence:

- `enterprise/server/scheduling/scheduler_server/scheduler_server.go:2340-2375`
- `enterprise/server/scheduling/scheduler_server/scheduler_server.go:1528-1557`
- `enterprise/server/scheduling/scheduler_server/scheduler_server.go:2044-2100`
- `enterprise/server/scheduling/scheduler_server/scheduler_server.go:95`

Conclusion:

- Scheduler enqueue / lease logic is not inventing compressed execution IDs.
- It can preserve a bad execution ID for a while: scheduler task TTL is 24 hours.

### 8. Executor and `operation.Publisher` do not rewrite the name

On the executor side:

- `priority_task_scheduler.runTask(...)` opens the stream with `execTask.GetExecutionId()`
- `Executor.ExecuteTaskAndStreamResults(...)` reads `taskID := task.GetExecutionId()`
- `operation.Publish(...)` parses the task ID but stores the original string
- `operation.Assemble(...)` writes `Name: name` directly
- state updates and completion both use `Assemble(taskID, ...)`

Evidence:

- `enterprise/server/scheduling/priority_task_scheduler/priority_task_scheduler.go:606-618`
- `enterprise/server/remote_execution/executor/executor.go:218-266`
- `enterprise/server/remote_execution/operation/operation.go:122-137`
- `enterprise/server/remote_execution/operation/operation.go:271-345`

Conclusion:

- The executor does not mint or normalize `execution_id`.
- `operation.Publisher` is an echo path: `op.Name` equals the scheduled task's `ExecutionId`.

### 9. `PublishOperation(...)` trusts `op.Name` and reparses it

On the server side, `PublishOperation(...)`:

- reads `taskID = op.GetName()`
- puts it into log context as `execution_id`
- on completion, reparses it with `ParseUploadResourceName(taskID)`
- calls `fetchActionAndCommand(ctx, actionCASRN)`

`ParseUploadResourceName(...)` preserves the compressor encoded in the string.

Evidence:

- `enterprise/server/remote_execution/execution_server/execution_server.go:1381-1413`
- `server/remote_cache/digest/digest.go:596-620`

Conclusion:

- `PublishOperation(...)` is not a minting site for `compressed-blobs`.
- But it is where a pre-existing bad execution ID becomes dangerous, because it turns the string back into a compressor-aware CAS resource name.

### 10. Why a compressed task ID breaks action fetch

`fetchAction(...)` does:

```go
actionBytes, err := s.cache.Get(ctx, actionResourceName.ToProto())
```

With the distributed + pebble + lookaside stack:

- `distributed.Cache.Get(...)` drains a reader for the exact requested resource name
- `PebbleCache.Reader(...)` decompresses only when the request is `IDENTITY`
- if the request compressor is `ZSTD`, Pebble returns ZSTD bytes, either by passthrough or by compressing on read

Evidence:

- `enterprise/server/remote_execution/execution_server/execution_server.go:1589-1602`
- `enterprise/server/backends/distributed/distributed.go:818-840`
- `enterprise/server/backends/distributed/distributed.go:1280-1286`
- `enterprise/server/backends/pebble_cache/pebble_cache.go:3327-3377`

Conclusion:

- If `PublishOperation(...)` receives a compressed execution ID, `fetchAction(...)` can read compressed bytes and then fail to unmarshal the action proto.

### 11. Only the action fetch is exposed to this compressor leakage

`fetchCommand(...)` rebuilds a fresh CAS resource name for the command digest:

```go
cmdInstanceNameDigest := digest.NewCASResourceName(
    cmdDigest,
    actionResourceName.GetInstanceName(),
    actionResourceName.GetDigestFunction(),
)
```

That fresh resource starts as `IDENTITY`.

Evidence:

- `enterprise/server/remote_execution/execution_server/execution_server.go:1607-1618`
- `server/remote_cache/digest/digest.go:139-140`

Conclusion:

- A compressed `taskID` can make the action read compressed.
- The command read is rebuilt from a fresh identity CAS resource name, so it is not exposed to the same compressor leakage.

### 12. The first real current-code gap is action-merger reuse

`action_merger.GetOrCreateExecutionID(...)` computes a fresh identity-based:

```go
newExecutionID := adResource.NewUploadString()
```

But if there is already an execution recorded for that action digest and it still exists in scheduler state, action-merger returns the existing `executionID` string instead of the freshly minted one.

Important details:

- the action-merging forward key is based on:
  - user prefix
  - digest function
  - digest hash
- it does not include:
  - instance name
  - compressor
- the stored `executionID` value is treated as an opaque string

Evidence:

- `enterprise/server/remote_execution/action_merger/action_merger.go:68-82`
- `enterprise/server/remote_execution/action_merger/action_merger.go:260-320`
- `enterprise/server/remote_execution/action_merger/action_merger.go:331-366`

Conclusion:

- Fresh action-merger IDs should still be identity.
- But action-merger can reuse any existing execution ID string already stored for that digest, including a bad compressed one.
- This is a current-code propagation path, not just an "old executors" theory.

### 13. Current code also refreshes the stored merged execution ID during leasing

When the scheduler successfully claims a task, it calls:

```go
action_merger.RecordClaimedExecution(ctx, s.rdb, taskID, s.actionMergingLeaseTTL)
```

That function writes the exact `taskID` string back into the action-merging forward record.

Evidence:

- `enterprise/server/scheduling/scheduler_server/scheduler_server.go:2024-2028`
- `enterprise/server/remote_execution/action_merger/action_merger.go:111-133`

Conclusion:

- If a bad compressed execution ID ever gets into the in-flight scheduler/action-merger state, current code can keep refreshing and reusing it.
- So reuse does not require version skew. It can self-sustain within the current version as long as the bad ID is already present in live scheduler / Redis state.

### 14. Once a bad execution ID exists, later paths keep reparsing and persisting it

These paths consume an existing execution ID rather than minting a new one:

- `waitExecution(...)`
- `WaitExecution(...)`
- `MarkExecutionFailed(...)`
- `metadataForClickhouse(...)`
- `cacheExecuteResponse(...)`
- `updateExecution(...)`

Evidence:

- `enterprise/server/remote_execution/execution_server/execution_server.go:1068-1136`
- `enterprise/server/remote_execution/execution_server/execution_server.go:1240-1299`
- `enterprise/server/remote_execution/execution_server/execution_server.go:1477-1499`
- `enterprise/server/remote_execution/execution_server/execution_server.go:368-373`

Conclusion:

- These are trust / persistence / replay sites, not minting sites.
- Once a compressed execution ID exists anywhere in the system, several later paths will preserve and reuse that compressor information.

### 15. Redis storage mostly wraps task IDs into Redis keys; it does not rewrite the values

I looked at the Redis-backed pieces that store or index task / execution IDs:

- Scheduler task storage
- Scheduler task-reservation storage
- Execution status pubsub streams
- Redis execution collector
- Action-merger state

What the code does:

- Scheduler task keys use either `task/<taskID>` or `task/{<taskID>}` depending on Redis availability monitoring.
- Scheduler reservation keys use either `taskReservations/<taskID>` or `taskReservations/{<taskID>}`.
- Execution status stream channels use either `taskStatusStream/<taskID>` or `taskStatusStream/{<taskID>}`.
- Redis execution collector uses keys like `executionUpdates/<executionID>` and `invocationLink/<executionID>`.
- In all of those cases, the execution/task ID is embedded into the Redis key, but the stored execution/task proto still contains the original string unchanged.

Evidence:

- `enterprise/server/scheduling/scheduler_server/scheduler_server.go:1456-1470`
- `enterprise/server/remote_execution/execution_server/execution_server.go:157-165`
- `enterprise/server/backends/redis_execution_collector/redis_execution_collector.go:76-94`
- `enterprise/server/backends/redis_execution_collector/redis_execution_collector.go:120-145`
- `enterprise/server/backends/redis_execution_collector/redis_execution_collector.go:201-243`

Conclusion:

- I do not see Redis task/execution storage mutating an identity task ID into a compressed one.
- The main Redis-specific transformations here are sharding / namespacing wrappers around the key name, not changes to the stored execution ID value.

### 16. The one Redis helper that does transform an execution ID is action-merger sharding

`action_merger.executionIDWithDigestSharding(executionID)`:

- parses the execution ID as an upload resource name
- splits the string on `/`
- wraps the digest-hash path segment with `{...}`
- rejoins the string

This transformed string is used only inside the reverse Redis key name for action-merging state.

Evidence:

- `enterprise/server/remote_execution/action_merger/action_merger.go:84-108`

Why it matters:

- This is a real transformation of the execution ID string, but it is for Redis key construction only.
- The original execution ID value stored in the forward action-merging record remains unchanged.
- So this helper does not itself explain how `execution_id` would become `.../compressed-blobs/zstd/...`.

### 17. The most important Redis-related risk is reuse, not mutation

From the Redis code alone, the clearest risk is:

- action-merger stores an opaque execution ID string in Redis
- action-merger later reads that exact string back
- scheduler lease handling refreshes that exact string again during task claim

Evidence:

- `enterprise/server/remote_execution/action_merger/action_merger.go:128-133`
- `enterprise/server/remote_execution/action_merger/action_merger.go:270-320`
- `enterprise/server/scheduling/scheduler_server/scheduler_server.go:2024-2028`

Conclusion:

- Redis appears to be a persistence and reuse mechanism for execution IDs, not a place where the ID is recomputed from scratch.
- So the Redis angle looks theoretically capable of preserving and re-serving a bad compressed task ID, but not of spontaneously converting a clean identity task ID into a compressed one.

## Current conclusion

What I can prove from the current code:

- Fresh execution IDs minted from the normal `Execute` RPC path should be identity-based `.../blobs/...`.
- Scheduler, executor, `operation.Publisher`, and `PublishOperation` all preserve the chosen execution ID string; they do not invent `compressed-blobs`.
- I do not currently see a current-code minting step in the mainline `Execute` path that should produce `.../compressed-blobs/zstd/...`.

What I cannot prove:

- I cannot prove that `compressed-blobs` is impossible in the overall end-to-end `Execute` flow, because `action_merger.GetOrCreateExecutionID(...)` can reuse a previously stored execution ID string from live Redis / scheduler state.

Most likely interpretation of current code:

- If a modern log line shows `execution_id=.../compressed-blobs/zstd/...`, that string was almost certainly already present before the executor published status updates.
- The strongest current-code candidate for how it re-enters a normal `Execute` flow is action-merging reuse of an already-existing execution ID.
- Once present in live scheduler/action-merger state, the current code can preserve and propagate it without any version skew.

## Remaining question

I still do not see where a compressed execution ID would be freshly minted in the current codebase.

The remaining hypotheses are:

1. Some current-code path outside the normal `ExecutionServer.execute()` minting flow is creating a compressed execution ID, and I have not found it yet.
2. A bad compressed execution ID entered live scheduler / action-merger state somehow, and the current code is now preserving and reusing it.
3. There is an environmental or request-shape detail I have not yet modeled, such as an execution ID string arriving from outside the normal `Execute` minting path and then being persisted.
