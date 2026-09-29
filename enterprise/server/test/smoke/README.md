# Executor smoke tests

These tests take an executor binary (for example a release artifact), start it
on the local host, register it with a BuildBuddy app in a unique pool, and run
a set of actions on it through the remote execution API. The suite lives in
`executorsmoke/` and checks:

- **Binary**: the binary's object file headers match the expected OS/arch.
  Optionally, the binary must also be statically linked
  (`--expect_static_binary`).
- **Health**: `/healthz` and `/readyz` report OK.
- **Version**: the `buildbuddy_version` metric reports `--expected_version`.
- **Registration**: the executor registers with the app and accepts work in
  its pool. If `--group_id` is set, the suite also checks the registered
  OS, arch, pool, version, and isolation types.
- **Actions**: stdout/stderr, exit codes, nested input trees, output files and
  directories (contents downloaded and verified), 64MB inputs and outputs,
  env vars, working directory, timeouts, action cache writes and hits,
  runner recycling, concurrent actions, and the host's native shell.
  Optionally, the suite also runs a shell action with each extra isolation
  type passed as `--isolation_type`.
- **Shutdown**: the executor exits cleanly on SIGTERM and unregisters.
  Windows skips this check.

Most actions run the test binary itself as a helper tool, so the suite has
no dependency on the host's shell or coreutils.

## Targets

- `:executor_smoke_local_test` starts a test-scoped app with executor auth
  enabled and tests the executor built from source. Pass
  `--test_arg=--executor_binary=<path or URL>` to test another binary. This
  target runs only on Linux.
- `:executor_smoke_test` (manual) tests an executor against an existing app.
  Use it for platforms that can't run the app, such as macOS and Windows. The
  test must run on a host whose OS/arch matches the executor.

## Testing a release artifact

On Linux, against a local app:

```sh
bazel test //enterprise/server/test/smoke:executor_smoke_local_test \
  --test_output=all \
  --test_arg=--executor_binary=https://github.com/buildbuddy-io/buildbuddy/releases/download/v2.310.0/executor-enterprise-linux-amd64-static \
  --test_arg=--expected_version=v2.310.0 \
  --test_arg=--expect_static_binary
```

Against an existing app, on any platform:

```sh
bazel test //enterprise/server/test/smoke:executor_smoke_test \
  --test_output=all \
  --test_env=EXECUTOR_SMOKE_API_KEY \
  --test_arg=--app_target=grpcs://remote.buildbuddy.dev \
  --test_arg=--executor_binary=/path/to/executor-enterprise-darwin-arm64 \
  --test_arg=--expected_version=v2.310.0
```

The API key needs the CACHE_WRITE capability, plus REGISTER_EXECUTOR unless
you set a separate executor key (`--executor_api_key` or
`$EXECUTOR_SMOKE_EXECUTOR_API_KEY`). The key's org must allow self-hosted
executors: actions are sent with `use-self-hosted-executors=true` and a unique
`Pool`. To also verify the registration details, pass `--group_id` and an
ORG_ADMIN key (`--admin_api_key` or `$EXECUTOR_SMOKE_ADMIN_API_KEY`).

### Running without Bazel on the test host

The test binary has no runfiles, so you can cross-compile it and copy it to
the machine under test along with the executor:

```sh
bazel build //enterprise/server/test/smoke:executor_smoke_test \
  --platforms=@io_bazel_rules_go//go/toolchain:windows_amd64
# Copy bazel-bin/enterprise/server/test/smoke/executor_smoke_test_/executor_smoke_test.exe
# to the Windows host, then:
executor_smoke_test.exe -test.v -app_target=grpcs://remote.buildbuddy.dev -executor_binary=executor-enterprise-windows-amd64-beta.exe
```

The test sends actions with the OS/arch of the test binary by default. If
those differ from the executor (for example, a `windows-386` executor on an
amd64 host), pass `--expected_os` and `--expected_arch`.

### Isolation types

The suite always tests the bare runner. To also test container isolation on
Linux, enable it in the executor and name it:

```sh
  --test_arg=--executor_arg=--executor.enable_podman=true \
  --test_arg=--isolation_type=podman \
  --test_arg=--container_image=docker://mirror.gcr.io/library/busybox
```

Executor logs are written to `executor.log` in the test's undeclared outputs.
The last lines are also printed when a test fails.
