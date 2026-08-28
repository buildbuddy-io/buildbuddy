# fcbench — Firecracker action-preparation harness

`fcbench` drives `FirecrackerContainer` through the same calls the executor
makes for one action (`NewContainer` → `PullImageIfNecessary` →
`DownloadInputs` → `Create`/`Unpause` → `Exec` → `Pause` → `Remove`), so each
stage of action preparation can be timed in isolation and under concurrency,
without the scheduler, app, or Bazel in the way.

It brings its own CAS (a `disk_cache` behind CAS/ByteStream/AC gRPC servers on
a local port) unless `--cache_target` is given, generates synthetic input
trees, uploads them, and runs N iterations per VM slot at concurrency K. Per
iteration it records wall time per stage **and every tracing span emitted by
the BuildBuddy code** (e.g. `ext4.DirectoryToImage`, `vmexec.Exec/MountWorkspace`,
`StartMachine`), so no instrumentation changes are needed to see where time
goes. Results are a JSON file; `~/fcperf/analyze.py` style scripts can render
percentiles and span trees (see `analyze.py` in the experiment repo).

Requirements: Linux, root (FUSE mounts + jailer), `firecracker`/`jailer` in
`$PATH` (see `tools/enable_local_firecracker.sh`), a kernel with
`CONFIG_USERFAULTFD` (snapshot restore uses UFFD).

```sh
bazel build //enterprise/tools/fcbench
sudo env RUNFILES_DIR=$PWD/bazel-bin/enterprise/tools/fcbench/fcbench_/fcbench.runfiles \
  bazel-bin/enterprise/tools/fcbench/fcbench_/fcbench \
  --root=/fcb --data_dir=/fcb-data \
  --workload=small=50:8192,medium=2000:32768,large=10000:32768 \
  --iterations=5 --concurrency=2 --mode=recycle --out=/tmp/results.json \
  --executor.enable_local_snapshot_sharing=true --executor.enable_remote_snapshot_sharing=true
```

Useful flags:

| flag | meaning |
|---|---|
| `--workload=name=files:avgBytes[:dirs][:maxBytes]` | synthetic input tree(s); exponential size distribution |
| `--unique_inputs` | new tree per iteration (cold filecache); `--seed` to make it reproducible |
| `--mode=recycle\|fresh\|keep` | pause/unpause between iterations (like runner recycling), boot fresh each time, or keep one VM |
| `--concurrency=K` | K VM slots in flight; `--shared_snapshot_key=false` gives each slot its own snapshot |
| `--overlap_unpause` | restore the snapshot concurrently with the input fetch |
| `--tree_image` | build the workspace image from the input Tree + filecache (no host hardlinks) |
| `--touch_inputs`, `--output_files=N`, `--cmd` | what the guest does |
| `--network`, `--init_dockerd`, `--mem_mb`, `--cpus`, `--image` | VM config |
| any executor flag | e.g. `--executor.firecracker_workspace_image_writer=native` |

Output JSON: `results[]` (per iteration: stage wall times, transfer stats,
trace id), `spans[]` (all spans with parent ids, attributes), `flags`, `host`.
