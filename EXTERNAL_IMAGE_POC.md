# POC: serving Bazel's external tree from a read-only image

Overnight experiment, 2026-09-25, on this VM (8 vCPU, 31 GB, virtio SSD), Bazel 9.3.0rc2,
`bazel build --nobuild --config=cache //...` as the analysis workload. No repo changes;
everything lives in the session scratchpad
(`/tmp/claude-1000/-home-exedev-squash-buildbuddy/*/scratchpad`: scripts, images, profiles, logs).

## TL;DR

| Goal | Verdict | Evidence |
|---|---|---|
| Fewer / no external fetches | **Proven.** A fresh output base analysed all of `//...` with zero network fetches off the image. | `freshob.log`: 0 "Fetching" lines, 35.5s |
| Smaller disk footprint | **Proven.** 9.2 GB (contents cache + archives + external) becomes one 2.1 GB squashfs (zstd) or 3.0 GB erofs (lz4hc). | `img-zstd.sqfs`, `img-lz4hc.erofs` |
| Lazily streamable, analysis files first | **Proven.** With files sorted so the analysis set leads, analysis of `//...` completed with only the first 30 MB + last 5 MB of the 2087 MB image present. | truncated sparse image, `trunc-coldpage.profile.gz` |
| Faster analysis | **Disproven** (for this idea). Analysis is CPU-bound; page-cache-cold vs warm differs by ~1.5s. Image adds ~10% (erofs) to ~20% (squashfuse). | profiles below |
| Real builds work off the image | **Proven.** 2311 sandboxed actions (libc++ compile, go toolchain builder, protoc, Go compile, tests) ran with toolchains + sources on the image; both tests pass. | `sqfs-p3.profile.gz`, `erofs-p3.log` |

## Baseline numbers

| Run (`--nobuild //...`) | Wall |
|---|---|
| Cold: fetch everything + load + analyze | 90.4s (60s of that is fetching) |
| Cold JVM, warm disk | 29.5s |
| Cold JVM, cold page cache (`drop_caches`) | 31.0s |
| Warm server, no-op | 3.6s |

Cold-JVM profile: 25.5s of 29.5s is `skyframeExecutor.configureTargets` (680k configured targets,
5.8k aspect applications for 3273 top-level targets). Repo marker re-validation on a cold JVM is
~13.5s CPU spread across threads (3046 "Fetching repository" events, each just a marker check).

Disk after one full fetch:

| Location | Size | Files |
|---|---|---|
| `cache/repos/v1/contents` (Bazel 9 repo contents cache, extracted repos) | 6.0 GB | ~214k |
| `cache/repos/v1/content_addressable` (downloaded archives; redundant once extracted) | 2.0 GB | 998 |
| `<output_base>/external` real dirs (non-cacheable repos: chromium 505M, node 205M, gazelle repo cache 155M, oci bases…) | 1.2 GB | ~8k |
| **Total** | **9.2 GB** | ~225k |

The big items are execution-time toolchains and binaries, not sources: llvm 628M, clickhouse 564M,
npm_typescript 542M, chromium 505M, go sdk 282M, glibc 249M, six copies of the bazel binary (~360M).

## What analysis actually reads

`fatrace` on the Bazel server during a cold-JVM `--nobuild //...`:

- **6463 files, 52.8 MB** out of ~225k files / 7.2 GB (0.7% by bytes).
- 1310 `.bzl` (12.4 MB), 3825 `BUILD.bazel` (4.0 MB), 814 `.recorded_inputs`, 113 `.marker`, 128 `.go`
  (gazelle sources watched by the `go_repository_tools` repo rule), 31 patches.
- 34 MB of the 53 MB is three binaries digested during marker validation: `go` (17 MB), `gazelle` (9 MB),
  `fetch_repo` (7 MB). Those are recorded inputs of `bazel_gazelle_go_repository_tools`.

Bazel 9 layout detail that shapes the design: cached repos are symlinks
`external/<repo> -> cache/repos/v1/contents/<predeclared-hash>/<uuid>`, and their marker is
`<uuid>.recorded_inputs` next to that dir. So the image must carry **both** `external/`
(markers + the 115 non-cacheable repos, symlinks kept as symlinks) and `contents/` at their real paths.

## Images

Staging tree: hardlinked copy of `external/` (symlinks preserved) + `contents/` (7.2 GB, 255k inodes).

| Image | Size | Ratio | Build time | Mount |
|---|---|---|---|---|
| squashfs, zstd-15, 256K blocks | 2087 MB | 3.5x | 141s (8 threads) | squashfuse_ll (no kernel squashfs module on this VM) |
| squashfs, same, `-sort` analysis set first | 2087 MB | 3.5x | 114s | squashfuse_ll |
| erofs, lz4hc-9, 64K clusters | 2971 MB | 2.4x | 678s (erofs-utils 1.7.1 is single-threaded; kernel here lacks erofs zstd/deflate) | kernel, loop |

Mounted with overlayfs on top (`lowerdir=image, upperdir=scratch`) at
`cache/repos/v1/contents` and `<output_base>/external`, `content_addressable` replaced with an empty dir.
Bazel writes small things into the upper: it re-touches every `.recorded_inputs` for contents-cache GC
(815 files, 10 MB after copy-up), re-generates 46 `local=True` config repos (rules_java toolchain
config repos, local_config_cc, host_platform, bazel_features…) on every server start (it does this
on the plain tree too), and re-downloaded 389 tiny BCR registry JSON files (3.5 MB) on the first
`bazel test` because `content_addressable` was gone. Include those registry files in the image.

## Results on the image

| Run (`--nobuild //...`, cold JVM) | Wall | Image bytes read |
|---|---|---|
| native tree, warm page cache | 29.5s | – |
| native tree, cold page cache | 31.0s | – |
| erofs (kernel), cold page cache | 31.9s | 54.9 MB (loop device stats) |
| erofs (kernel), second run | 34.2s | 54.9 MB |
| sorted squashfs via squashfuse + strace, cold page cache | 36.8s | 82.8 MB of preads, 18.2 MB distinct |
| sorted squashfs, **truncated** to bytes [0,30MB) + [2081MB,2087MB) | 37.1s | analysis succeeded; reading any other file fails |
| erofs, **fresh output base** (`--output_base` never used before) | 35.5s | 0 fetches; only the 46 local config repos regenerated |

Real execution (`bazel test --config=cache --noremote_accept_cached //server/util/status:all //server/util/lru:all //proto:acl_go_proto`):
on squashfs it executed 2311 actions locally (libc++/libc++abi compiles from `@llvm`, Go toolchain
builder, protoc, Go compiles); on erofs 731 more sandboxed actions + both tests pass.

Squashfuse's read pattern: all data reads fell in the first 64 MB (ended at byte 29.6 MB) and all
metadata reads (inode/directory tables) in the last 5 MB. So a lazy fetcher needs the prefix plus the
tail, ~34 MB (1.6% of the image), to complete analysis of the entire repo.

## Interpretation

- **Fetch blips**: an image built once per lockfile change (CI, keyed by hash of `MODULE.bazel.lock`
  + `go.mod`), stored in the CAS or a bucket, mounted read-only, removes fetches from the dev/CI critical
  path. Bazel's own marker logic accepts it unchanged; the overlay absorbs its writes.
- **Disk**: 9.2 GB -> 2.1 GB (squashfs) per machine, shared by every output base on the machine
  (the contents cache is already shared; the `external/` part is per output base but the image's
  `external/` overlaid on a fresh output base worked).
- **Lazy streaming**: sort order + range fetch is enough. The firecracker snapshot chunk store
  (`copy_on_write`, UFFD lazy loading) is exactly this shape; a FUSE/NBD reader over CAS chunks
  would fetch ~34 MB to analyse and then only the inputs of actions that actually execute locally.
  With remote execution and `--unix_digest_hash_attribute_name` (xattr-provided digests), Bazel would
  not need to read most source bytes at all; not tested tonight.
- **Analysis time**: not helped by any of this. It is Skyframe/Starlark CPU (configureTargets 25s).
  Reducing it means fewer configured targets/aspects (e.g. the validation aspect, exec transitions
  that produce 20+ `k8-opt-exec-ST-*` configurations), or Skymeld/analysis caching, not I/O.

## Caveats

- One VM, one repo state, n=1 timings (±1-2s noise).
- erofs was single-threaded lz4hc; real erofs-utils 1.8+ has `--workers` and zstd (needs kernel support).
- squashfuse is FUSE; kernel squashfs would be closer to erofs numbers.
- The image is per (machine user, cache path): `external/` symlinks embed the absolute contents-cache
  path. Fine for a fleet with a fixed layout; otherwise rewrite on build or use `--repo_contents_cache`.
- Not tried: `bazel vendor --vendor_dir` as the image source (documented, path-independent, bzlmod
  only), which would be the cleaner productisation.

## Reproduce

Scripts in the scratchpad: `switch-on.sh <image>` / `switch-off.sh` (mount/unmount overlays),
`p1-test.sh` (timed analysis + loop stats), `p2-test.sh` (squashfuse pread trace),
`p3-test.sh` (real build/test), `erofs-run.sh` (whole erofs sequence incl. fresh output base),
`fatrace_an.py` / `preads_an.py` / `profsum.py` (analysis of traces and profiles).
