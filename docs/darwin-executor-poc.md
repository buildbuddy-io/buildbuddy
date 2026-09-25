# Darwin executor workflow POC

The Darwin workflows now build the executor binary and run an explicit core suite,
`//enterprise/server/remote_execution:darwin_tests`, instead of `//...`. The suite
covers execution lifecycle, bare commands, container accounting, platform
selection, operation updates, persistent workers, workspace cleanup and input
downloads, file caching, disk utilities, and port leasing. The broader server and
CLI suites continue to run in Linux CI. This deliberately reduces native Darwin
coverage; add targets back individually after measuring them on the Intel host.

`mac-workflows` selects Linux amd64 execution platforms for hermetic compilation
(including Darwin C/C++ and Go), while `TestRunner` uses `darwin-sandbox` on the
Mac running Bazel. It allows 32 outstanding build actions, two local tests, and
two CPUs of local action capacity. These are per-invocation limits, not a
machine-wide admission limit. The existing `allow_concurrent_runs: false` remains
in place. Bazel analysis, downloads, and sandbox setup still run on the Mac.

Two test changes accompany the workflow:

- Port locks retain their `*os.File` objects. Previously, omitting `Close()` did
  not retain the lock: Go's file finalizer released it after garbage collection.
  `TestPortLockSurvivesGC` reproduces this on the old code and passes with the fix.
  This fixes premature lease release, not every possible port collision (for
  example, unrelated processes do not participate in the locking protocol).
- `TestScanWithConcurrentAdd` uses ten trials of 100 files instead of 100 trials.
  Each trial is a subtest, so its cache and temporary files are released before
  the next trial. Both executable and non-executable files and concurrent scan/add
  behavior are still covered. Use repeated runs for stress testing rather than
  making every ordinary invocation pay the full stress-test cost. Production
  file-cache durability behavior is unchanged.

## Reproduce

From the repository, with `BUILDBUDDY_API_KEY` set:

```sh
bb remote --os=darwin --arch=amd64 \
  --runner_exec_properties=Pool=workflows \
  --runner_exec_properties=use-self-hosted-executors=true \
  --timeout=30m \
  test //enterprise/server/remote_execution:darwin_tests \
  //enterprise/server/cmd/executor:executor \
  --config=mac-workflows --nocache_test_results
```

Use `--arch=arm64` for prod ARM. For dev ARM, also set
`BUILDBUDDY_API_KEY="$DEV_BUILDBUDDY_API_KEY"` and add
`--remote_runner=grpcs://remote.buildbuddy.dev`. `bb remote` mirrors local changes,
so publishing the branch is unnecessary. Add `--runs_per_test=3` to exercise all
tests repeatedly; the two-test concurrency limit still applies.

The workflow-generated endpoint config keeps compilation and cache traffic in
the same environment as the workflow. This config is intended for workflow or
`bb remote` runners, which supply those endpoint definitions.

## Validation

Runs on September 25, 2026, using the local patch mirrored by `bb remote`:

| Environment | Validation | Result | Bazel elapsed |
| --- | --- | --- | --- |
| Prod Intel | Final sandboxed config, three uncached runs per target | [30/30 passed](https://app.buildbuddy.io/invocation/53d87a63-f3b0-45e6-9402-d0b5d1ee33c7) | 23.575s |
| Dev ARM64 | Final sandboxed config, three uncached runs per target | [30/30 passed](https://app.buildbuddy.dev/invocation/40aff224-9187-4110-b429-16a576f2479e) | 14.236s |
| Prod ARM64 | Final sandboxed config, uncached tests and cold cross-build | [10/10 passed](https://app.buildbuddy.io/invocation/f0187247-08c7-4112-95be-43307aa3823d) | 210.802s |
| Linux | Port-lock regression and modified file-cache suite, uncached | [2/2 passed](https://app.buildbuddy.io/invocation/de1b80f4-ba69-4141-b9b7-bf3e47548109) | 89.791s |

The repeated Mac runs reused compiled artifacts but did not cache test results.
Both also successfully built the executor binary. The Intel profile records 30
`darwin-sandbox` executions and a maximum of **two simultaneous local actions**
(measured from the `action count (local)` / `Resources acquired` intervals).
Remote action metadata from the initial cross-compilation probe identifies a
Linux amd64 Go SDK with `-installsuffix darwin_amd64`.

The first full-suite experiments, before restoring the Darwin sandbox, passed
10/10 targets on both [Intel](https://app.buildbuddy.io/invocation/aff038bd-918e-4b3d-b9ea-730ec7e9ba41)
and [dev ARM64](https://app.buildbuddy.dev/invocation/b30e5a2f-0e25-47b3-9baa-f03551bbf097).
Those builds took 160.946s and 194.089s respectively; the file-cache target took
3.673s and 2.120s. These are observations, not a controlled speedup comparison
with the old full-repository workflow.

The port-lock regression was additionally run against the original source: it
failed immediately after GC released the lease. With the fix, ten repeated
race-enabled runs passed (`GO111MODULE=off go test -race
./server/testutil/testport -count=10`). YAML parsing, Go formatting, BUILD
formatting, and `git diff --check` passed.

One exploratory Linux run failed before Bazel started because a recycled VM had
no disk space. Retrying with
`--runner_exec_properties=EstimatedFreeDiskBytes=27000000000` passed. This was
unrelated to the Darwin/test changes.

Before broadening the suite, measure each added target uncached on the Intel
host. The port-lock fix is independently useful, but does not establish that all
the historical distributed-cache or ClickHouse failures share that root cause.
Machine-wide contention and long-running durability syncs remain possible;
per-workflow throttling does not impose a global host limit.
