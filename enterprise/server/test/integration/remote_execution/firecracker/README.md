# Executor-level Firecracker integration tests

One **real executor binary**, started by `testexecutor.RunWithOptions`, serves
many concurrent RE API test cases against one `rbetest` app/scheduler/cache.
The suite does not construct Firecracker containers or call their methods.
App/executor startup is serial; independent leaf cases run in parallel and the
executor scheduler controls guest resource allocation. Global network-pool
observations run after the parallel group.

## Running

Use a dedicated Linux worker as root, outside the Bazel sandbox, with working
`/dev/kvm`, `/dev/fuse`, writable cgroups, network namespace/mount privileges,
host IPv4 forwarding enabled (`net.ipv4.ip_forward=1`),
and `firecracker`, `jailer`, `ip`, `iptables`, `mke2fs`, and `debugfs` installed.
Use this repository's pinned Firecracker/jailer versions as regular executable
files (not symlinks). Kernel and initrd come from the executor's Bazel dependencies.

The worker needs network access to the pinned OCI registries. Images are pulled
and converted by the executor, not injected as preconverted container images.
Docker tests run Docker inside the guest, without a host Docker socket.

```sh
bazel test --config=remote --config=tests-bare \
  //enterprise/server/test/integration/remote_execution/firecracker:firecracker_test \
  --test_output=all
```

For a local root run after building with the usual remote toolchain:

```sh
bazel build --config=remote \
  //enterprise/server/test/integration/remote_execution/firecracker:firecracker_test
sudo ./bazel-bin/enterprise/server/test/integration/remote_execution/firecracker/firecracker_test_/firecracker_test \
  -test.v -test.timeout=20m
```

Use `-test.run=TestFirecracker/Parallel/CaseName` to select a case. The optional
`-firecracker_test_legacy` run uses one separate executor configuration with
chunked sharing disabled and the older vmexec readiness path. It covers storage
variants, not snapshot persistence: disabling both sharing flags prevents the
current Firecracker implementation from saving snapshots at all.

Do not run this suite on production executors or inside unprivileged containers.
Missing root/KVM prerequisites fail rather than silently skipping coverage.

## Adding cases

Register a `func testExample(t *testing.T, env *firecrackerEnv)` in the suite.
The suite passes `env.forTest(t)` to each case. Scope again when creating nested
leaf subtests:

```go
t.Run("example", func(t *testing.T) {
    t.Parallel()
    rbe := env.forTest(t)
    result := rbe.Execute(firecrackerCommand("echo hello"), &rbetest.ExecuteOpts{
        APIKey: rbe.APIKey1,
        ActionTimeout: time.Minute,
    }).Wait()
    require.Equal(t, 0, result.ExitCode, "stderr: %s", result.Stderr)
})
```

The wrapper embeds `*rbetest.Env` without copying any fixture locks or atomics.
Its `Execute` clones the command/options, sets `ExecuteOpts.TestingT` to the
leaf test (preserving an explicitly supplied `TestingT` for nested cases), and
adds stable `test-case=t.Name()` platform metadata from the wrapper owner. This isolates
runner/snapshot keys and action digests across cases while allowing a case's
successive actions to restore the same VM. Input-upload, execution and result
assertions, plus output-directory cleanup, belong to the leaf test. The shared
fixture's startup/shutdown assertions remain owned by the suite test.
Unless explicitly supplied, `ExecuteOpts.Context` is the effective leaf test's
context, cancelling unfinished uploads and execution RPCs when that test ends.
Existing rbetest callers still default to `context.Background()`.

`DownloadOutputsToNewTempDir` reuses existing app clients, so downloading outputs
in parallel does not create new fixtures or mutate global app flags. Pass
`rbe.Env` only to helpers specifically requiring `*rbetest.Env`; executing directly
through that pointer bypasses the wrapper's case isolation.

`firecrackerCommand` defaults to Linux, the host architecture, Firecracker,
digest-pinned BusyBox, one CPU, 512 MiB RAM and 100 MB estimated scratch disk.
Properties replace defaults case-insensitively, then are sorted. Scripts run as
`sh -eu -c` so failed assertions cannot be hidden by later successful commands.
Commands default to `network=off` to avoid unnecessary network-namespace and
iptables contention. Network cases explicitly override it; `init-dockerd=true`
with `network=off` still creates the local guest network needed by Docker.
The shared guest resolver file contains
`firecrackerTestResolvConf` from the network cases.

## Snapshot synchronization and authentication

Local chunked sharing is enabled by default; remote sharing is disabled.
Snapshot cases authenticate with `APIKey: rbe.APIKey1`, set `recycle-runner=true`,
and keep their platform/runner-recycling key stable across the sequence. After
the first action completes, call:

```go
waitForFirecrackerSnapshot(t, rbe, firstResult)
```

This waits for the **specific execution's** post-completion statistics, including
successful snapshot-save status. This is a limited, read-only observation of the
app's stored metadata originating from the real executor, not a container call.
The app enables post-completion publication and retains execution records in its
fixture Redis until teardown. A global pool
gauge is unsuitable: chunked snapshots bypass that pool, and global counters can
be advanced by other cases. The following action must still verify guest state
(a marker or boot ID), so fresh-boot fallback cannot pass as a restore.

The app and external executor share an explicit JWT signing key. The executor
also configures an unused, lazily initialized OIDC provider to enable its real
JWT authenticator. Without it, executor startup falls back to NullAuthenticator:
runner grouping can still read trusted JWTs, while snapshot/image-cache grouping
loses the group ID. The rbetest authenticator already issues real signed JWTs;
no fake JWT implementation is needed. Anonymous requests remain explicitly
enabled for the authorization-negative cases, and an empty `ExecuteOpts.APIKey`
is never replaced with a default. The fixture group's executor pool is configured
as shared before any parallel cases begin.

The fixture sets `EnvOptions.CommandTimeout` to five minutes for scheduling,
cold image conversion and execution. This client-side result deadline is
independent of `ExecuteOpts.ActionTimeout`; existing rbetest suites retain their
60-second default.

## Lifecycle

Executor-wide flags belong in the suite's serial `newFirecrackerEnv` call, not
inside parallel cases. `flags.Set` affects the app/test process, not the external
executor. Additional executor arguments override harness defaults. Cases must
not change process-global flags/environment or host-global networking policy.

The executor has a short `/tmp/fc-e2e-*` build root and shared-per-batch image
cache/metadata directories. `/tmp/buildbuddy-fc-e2e-network-locks` is retained to
coordinate IP allocation across processes. Existing network namespaces are
preserved on startup.

At teardown, SIGTERM gives the executor a 45-second graceful shutdown window for
runner, VM, mount and network cleanup. The harness waits for process exit before
app teardown and root deletion. After one minute it kills/reaps an unresponsive
executor and fails the test. Existing `testexecutor.Run` callers remain unchanged.
Disposable workers are still required for crash/forced-shutdown failure cases.

## Coverage and limits

The default target uses **one executor and one Bazel shard**. `-test.parallel=N`
(or `--test_arg=-test.parallel=N`) controls concurrent Go leaf tests; the real
executor scheduler still enforces task resource limits. `ExecutionsOverlap`
uses a two-action HTTP barrier to require actual concurrent guest execution on
that executor, not just concurrent client requests.

Workload coverage includes:

- Exit codes, stdout/stderr, environment, working directory, named/numeric
  non-root users, and a small scratch-disk request.
- CAS inputs, executable scripts, symlinks, nested output files/directories,
  omitted undeclared outputs, and the same file-transfer cases with VFS enabled.
- Large stdout, timed-out commands retaining debug output/files, orphan reaping,
  and concurrent verified disk IO.
- Network enabled/disabled, guest IPv6, a configured resolver, and mixed network
  modes with pooling enabled. A dedicated routed network namespace makes
  EXTERNAL/LOCAL checks traverse the host FORWARD chain, not merely host-local
  INPUT. Successful controls bracket every denied request.
- Docker 20/28/29, UDS/TCP, disabled TCP/daemon initialization, native storage,
  nested bind-mounted inputs/outputs, published ports, and a controlled registry
  mirror. Nested images come from CAS, except the explicit mirror-pull case.
- A cold registry/image reference followed by a warm run without more registry
  blob downloads, and cached private-image authorization across groups.
- Workspace replacement and Docker storage/port publishing after snapshot
  restore. Guest markers and boot IDs make fresh-boot fallback fail these cases.
- A frozen guest vmexec server producing a health-check failure, then a healthy
  execution on the same executor.

The optional legacy run covers Docker's `vfs` storage path in the non-chunked
configuration. It does not assert unsupported non-chunked snapshot persistence.
The full Bazel-build benchmark/workload is not ported here.

These are functional equivalents, not replacements for every original
assertion. Retain the container-level suite for guest API/image consistency,
snapshot policy/versioning/fallback details, diff merging, balloon effectiveness,
live/container stats, exact disk accounting, internal recycling decisions, and
fault-injection cases. Warm blob-download avoidance alone does not prove that
image reconversion never happened. This new target is additive; the existing
container-level target and its shards are unchanged pending further migration.

The fixture deliberately uses MMIO (`executor.firecracker_enable_pci=false`),
matching the direct tests. PCI is not covered: currently fresh `getConfig` omits
`--enable-pci`, while `LoadSnapshot` adds it when the VM configuration requests
PCI. Boot/restore transport consistency needs a separate production fix.

The short fixture root is a dedicated self-bind **shared mount**. The jailer
creates a slave mount namespace, and memory-export FUSE mounts created after
boot must propagate into it. A private backing `/tmp` otherwise produces empty
memory COW snapshots even though Firecracker reports successful export. Only
the fixture root's propagation changes, never `/` or `/tmp`; normal teardown
unmounts it before deleting it and fails rather than masking a busy mount.

For diagnosis, use `--test_arg=-firecracker_test_debug_vm_logs=true` to stream VM
logs. This is intentionally off by default to avoid large logs and unnecessary
logging contention.

The routed probe is set up before the executor starts: a locked free `/30` in
`198.18.0.0/15`, a private namespace/veth pair, and two interface/address-specific
FORWARD accepts. The executor inserts its per-VM restrictions ahead of those
accepts, so the probe cannot bypass LOCAL isolation. Its child HTTP process is
killed/reaped before rules, link and namespace are removed. Setup/cleanup is
outside parallel cases; the allocation lock inode is intentionally retained.
Ordered mixed-mode checks verify behavior with pooling enabled, not the identity
of a particular internal pool object.
