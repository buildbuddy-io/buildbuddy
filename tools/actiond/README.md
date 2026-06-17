# Building BuildBuddy with actiond

[actiond](https://github.com/hermeticbuild/actiond) is a local Remote Execution
API worker that runs each Bazel action in an empty chroot inside a Linux microVM,
for hermetic local builds. This directory wires BuildBuddy up to build through it.

## Usage

1. Start an actiond worker listening on `127.0.0.1:8980`. It must serve a runtime
   that provides coreutils (BuildBuddy genrules assume a POSIX userspace; the
   stock actiond runtime ships only glibc + bash, so a busybox-augmented runtime
   is needed — see the actiond notes).

2. Build:

   ```bash
   # pure-Go targets (works today):
   tools/actiond/build.sh --config=actiond-pure //server/version:version

   # or directly (note the SHA256 startup flag — actiond does not support BLAKE3):
   bazel --digest_function=SHA256 build --config=actiond-pure //server/util/...
   ```

`--config=actiond` targets the hermetic musl toolchains (so no host gcc is
needed in the empty chroot) and points remote exec/cache at the local worker.
`--config=actiond-pure` additionally disables cgo + nogo (see status below).

## Status (2026-06-17)

Works:
- Pure-Go targets build hermetically through actiond. A single pass over
  `//proto/... //server/util/... //server/metrics/... //cli/parser/... //cli/log/...`
  built ~3,800 actions; a pure-Go test batch passed 12/13 (the one failure is an
  actiond sandbox limitation, not a test bug — see below).

Blocked (actiond bugs, reported upstream):
1. **cgo targets** — including the `bb` CLI and the enterprise app/executor/
   cache-proxy. rules_go emits an empty `<pkg>_/<pkg>.a.cgo` tree artifact for
   cgo=True-but-pure-on-linux/amd64 packages (e.g. `golang.org/x/sys/unix`),
   and actiond drops **empty output directories**, so the build fails with
   `mandatory output ... was not created`. These targets also pull in
   real-cgo deps (go-tree-sitter, sqlite) that would build once the empty-dir
   bug is fixed.
2. **runtime symlink creation** returns `EPERM` inside the actiond sandbox, so
   actions/tests that create symlinks at runtime fail.

The `--digest_function=SHA256` startup flag is required because actiond only
supports SHA256 while BuildBuddy defaults to BLAKE3; `build.sh` sets it for you.
