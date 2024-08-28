---
id: rbe-platforms
title: RBE Platforms
sidebar_label: RBE Platforms
---

This documentation describes how to configure the execution
environment when building with remote execution on BuildBuddy.

For example, you can configure a container image in which actions are run,
customize the resources available to each action, or adjust action
timeouts.

For basic remote execution setup, see [RBE Setup](rbe-setup).

## Extending the BuildBuddy platform

To extend BuildBuddy's remote execution platform, you can
define a custom platform referencing the BuildBuddy RBE platform
in the `parents` list:

```python title="BUILD"
platform(
    name = "buildbuddy_linux_amd64",
    parents = [
        # Extend the platform from buildbuddy-toolchain
        "@buildbuddy_toolchain//:platform",
    ],
    exec_properties = {
        # Override or add new exec properties here:
        "example-property": "...",
        # Use "test." prefix to only apply the property to test actions:
        "test.example-property": "...",
    },
)
```

Any `exec_properties` defined in this platform will override properties
from the BuildBuddy platform.

You can use this platform with the following Bazel flag:

```bash
--host_platform=//:buildbuddy_linux_amd64
```

This assumes you've placed this rule in your root BUILD file.
If you place it elsewhere, make sure to update the path accordingly.

## Defining a new platform

If you don't want to extend BuildBuddy's recommended platform (for
example, when executing builds on macOS executors), you can
define a custom platform.

The following example may be useful as a starting point:

```python title="BUILD"
platform(
    name = "buildbuddy_linux_amd64",
    constraint_values = [
        "@platforms//cpu:x86_64",
        "@platforms//os:linux",
        "@bazel_tools//tools/cpp:clang",
    ],
    exec_properties = {
        "OSFamily": "linux",
        "Arch": "amd64",
        "container-image": "docker://gcr.io/flame-public/rbe-ubuntu20-04:latest",
        # RECOMMENDED: disable networking to reduce execution setup time.
        # You can set `test.dockerNetwork: bridge` on individual targets
        # that require networking.
        "dockerNetwork": "off",
    },
)
```

You can use this platform with the following Bazel flag:

```bash
--host_platform=//:buildbuddy_linux_amd64
```

This assumes you've placed this rule in your root BUILD file.
If you place it elsewhere, make sure to update the path accordingly.

## Using a custom Docker image

To override the Docker image for one or more targets, use the
`container-image` execution property:

```python title="BUILD"
    exec_properties = {
      # Set a container image for build and test actions:
      "container-image": "docker://YOUR:IMAGE",
      # Set a container image for test actions only:
      "test.container-image": "docker://YOUR:IMAGE",
    }
```

Execution properties can be set at various levels of granularity within
a build. See [Execution Properties](#execution-properties) for more information.

### ENTRYPOINT and CMD

Remote build actions will be run in your container via `CMD`, so note that any `CMD` instructions in your Dockerfile will be ignored.
`ENTRYPOINT`, on the other hand, is not ignored, so make sure that the container image's `ENTRYPOINT` is either unset,
or is a wrapper that is compatible with your build actions' commands.

For more information, see [Understand how CMD and ENTRYPOINT interact](https://docs.docker.com/engine/reference/builder/#understand-how-cmd-and-entrypoint-interact).

### Passing credentials for Docker images

You can use images from private container registries by adding the following
flags to your `bazel` command (replace `USERNAME` and `ACCESS_TOKEN` with
the appropriate credentials for the container registry):

```bash
--remote_exec_header=x-buildbuddy-platform.container-registry-username=USERNAME
--remote_exec_header=x-buildbuddy-platform.container-registry-password=ACCESS_TOKEN
```

For the value of `ACCESS_TOKEN`, we recommend generating a short-lived
token using the command-line tool for your cloud provider.

To generate a short-lived token for GCR (Google Container Registry),
the username must be `_dcgcloud_token` and the token can be generated with
`gcloud auth print-access-token`:

```bash
--remote_exec_header=x-buildbuddy-platform.container-registry-username=_dcgcloud_token
--remote_exec_header=x-buildbuddy-platform.container-registry-password="$(gcloud auth print-access-token)"
```

For Amazon ECR (Elastic Container Registry), the username must be `AWS`
and a short-lived token can be generated with `aws ecr get-login-password --region REGION`
(replace `REGION` with the region matching the ECR image URL):

```bash
--remote_exec_header=x-buildbuddy-platform.container-registry-username=AWS
--remote_exec_header=x-buildbuddy-platform.container-registry-password="$(aws ecr get-login-password --region REGION)"
```

Some cloud providers may also allow the use of long-lived tokens, which
can also be used in remote headers. For example, GCR allows setting a
username of `_json_key` and then using a service account's
[JSON-format private key](https://cloud.google.com/iam/docs/creating-managing-service-account-keys)
as the password. Note that remote headers cannot have newlines;
the command `tr '\n' ' '` is used in this example to remove them:

```bash
--remote_exec_header=x-buildbuddy-platform.container-registry-username=_json_key
--remote_exec_header=x-buildbuddy-platform.container-registry-password="$(cat service-account-keyfile.json | tr '\n' ' ')"
```

## Specifying a custom executor pool

You can configure BuildBuddy RBE to use a custom
[executor pool](rbe-pools.md) using the `Pool` property:

```python title="BUILD"
platform(
    name = "gpu_platform",
    # ...
    exec_properties = {
        # ...
        "Pool": "my-gpu-pool",
    },
)
```

Make sure to replace `my-gpu-pool` with your pool name.

Execution properties can be set at various levels of granularity within
a build. See [Execution Properties](#execution-properties) for more information.

For instructions on how to deploy custom executor pools, see the [RBE Executor Pools docs](rbe-pools.md).

## Execution properties

BuildBuddy RBE supports various `exec_properties` that modify remote action execution.

These properties can be used in different ways:

- Set `exec_properties` in the execution platform definition.
- Set `exec_properties` in each BUILD target.
- Set `--remote_default_exec_properties=KEY=VALUE` in `.bazelrc` or at the Bazel command line.
  Note that these properties are not applied when using a `platform`, even if
  the platform does not specify any conflicting exec properties.
- Set `--remote_header=x-buildbuddy-platform.KEY=VALUE`. This is
  a BuildBuddy-specific feature and is not generally recommended except
  for certain properties, described in
  [Setting properties via remote headers](#setting-properties-via-remote-headers).

### Target-level execution properties

If you want different targets to run in different RBE environments, you can specify `exec_properties` at the target level.

For example if you want to run one set of tests in a high-memory pool, or another set of targets on executors with GPUs.

```python title="BUILD"
go_test(
    name = "memory_hogging_test",
    srcs = ["memory_hogging_test.go"],
    embed = [":go_default_library"],
    exec_properties = {
        "Pool": "high-memory-pool",
    },
)
```

### Setting execution properties only for tests

To set `exec_properties` only for tests, you can prefix the execution
property with `"test."` within the `exec_properties` map:

```python title="BUILD"
example_test(
    name = "my_example_test",
    # ...
    exec_properties = {
        # Only enable networking for the test action, not the action
        # that builds the test.
        "test.dockerNetwork": "bridge",
    },
)
```

This `"test."` prefix works when setting properties in `platform()` or
when setting properties on individual `_test` targets.

::: info

[Execution groups](https://bazel.build/extending/exec-groups) allow more control over which execution properties can be used for each group of actions in each BUILD target.

Execution groups are typically implemented by individual rules,
but Bazel includes a built-in execution group called `"test"`
that allows applying execution properties only to tests.

:::

### Setting properties via remote headers

BuildBuddy supports setting execution properties via remote headers.
This can be done by setting `--remote_header=x-buildbuddy-platform.KEY=VALUE`
at the Bazel command line.

This feature is useful as a more secure option for passing secret property
values, such as `container-registry-password` or
`container-registry-username`, since other methods of setting exec
properties result in the properties being stored in the remote cache,
while header values are not stored.

Properties which are set via remote headers take precedence over any
exec properties set via any other method.

The following is the complete list of properties which are officially
supported for use in `remote_header`. Other properties _may_ work but
are not officially supported and can break at any time.

- `container-registry-username`
- `container-registry-password`

## Supported execution properties

This section contains documentation for execution properties that
BuildBuddy supports.

### Action scheduling properties

These execution properties affect how BuildBuddy's scheduler selects an executor for action execution:

- `Pool`: selects which [executor pool](rbe-pools) to use.
- `OSFamily`: selects which operating system the executor must be running. Available options are `linux` (default), `darwin`, and `windows` (`darwin` and `windows` are currently only available for self-hosted executors).
- `Arch`: selects which CPU architecture the executor must be running on. Available options are `amd64` (default) and `arm64`.
- `use-self-hosted-executors`: use [self-hosted executors](enterprise-rbe) instead of BuildBuddy's managed executor pool. Available options are `true` and `false`. The default value is configurable from [organization settings](https://app.buildbuddy.io/settings/).

### Action isolation and hermeticity properties

When executing actions, each BuildBuddy executor can spin up multiple
action **runners**. Each runner executes one action at a time. Each runner
has a **workspace** which represents the working directory of the action
and contains the action's input tree. Each runner also has an
**isolation** strategy which decides which technology is used to isolate
the action from others running on the same machine. Isolation strategies
may also be loosely referred to as **containers** or **sandboxes**.

The following properties allow customizing the behavior of the runner:

- `workload-isolation-type`: selects which isolation technology is the runner should use.
  When using BuildBuddy Cloud executors, `podman` (the default) and `firecracker` are supported.
  For self-hosted executors, the available options are `docker`, `podman`, `firecracker`, `sandbox`, and `none`. The executor must have relevant flags enabled.
- `recycle-runner`: whether to retain the runner after action execution
  and reuse it to execute subsequent actions. The runner's container is
  paused between actions, and the workspace is cleaned between actions by
  default. This option may be useful to improve performance in some
  situations, but is not generally recommended for most actions as it
  reduces action hermeticity. Available options are `true` and `false`.
- `preserve-workspace`: only applicable when `"recycle-runner": "true"` is set. Whether to re-use the Workspace directory from the previous action. Available options are `true` and `false`.
- `clean-workspace-inputs`: a comma-separated list of glob values that
  decides which files in the action's input tree to clean up before the
  action starts. Has no effect unless `preserve-workspace` is `true`. Glob
  patterns should follow the specification in
  [gobwas/glob](https://pkg.go.dev/github.com/gobwas/glob#Compile)
  library.
- `nonroot-workspace`: If set to `true`, the workspace directory will be
  writable by non-root users (permission `0o777`). Otherwise, it will be
  read-only to non-root users (permission `0o755`).

### Runner resource allocation

BuildBuddy's scheduler intelligently allocates resources to actions,
so it's generally not needed to manually configure resources for actions.
However, some `exec_properties` are provided as manual overrides:

- `EstimatedCPU`: the CPU time allocated for the action. Example values:
  - `2`: 2 CPU cores
  - `0.5`: 500 MilliCPU
  - `4000m`: 4000 MilliCPU
- `EstimatedMemory`: the memory allocated to the action. Example values:
  - `1M`: 1 MB
  - `2GB`: 2 GB
  - `4.5GB`: 4.5 GB
- `EstimatedComputeUnits`: a convenience unit that specifies both CPU
  and memory. One compute unit is defined as 1 CPU and 2.5GB of
  memory. Accepts numerical values, e.g. `1` or `9`.
- `EstimatedFreeDiskBytes`: the amount of disk space allocated to the action.
  Example values:
  - `1M`: 1 MB
  - `2GB`: 2 GB
  - `4.5GB`: 4.5 GB

### Remote persistent worker properties

Similar to the local execution environment, the remote execution environment may also retain a long-running process acting as a [persistent worker](https://bazel.build/remote/persistent) to help reduce the total cost of cold-starts for build actions with high startup cost.

To use remote persistent workers, the action must have `"recycle-runner": "true"`
in `exec_properties`.

The Bazel's flag ["--experimental_remote_mark_tool_inputs"](https://bazel.build/reference/command-line-reference#flag--experimental_remote_mark_tool_inputs) should help set these automatically so you don't have to set them manually. However, we provide these `exec_properties` for rules authors to experiment with.

- `persistentWorkerKey`: unique key for the persistent worker. This should be automatically set by Bazel.
- `persistentWorkerProtocol`: the serialization protocol used by the persistent worker. Available options are `proto` (default) and `json`.

### Runner container support

For `docker`, `podman`, and `firecracker` isolation, the executor supports
running actions in user-provided container images.

Some of these have a `docker` prefix. This is just a historical artifact;
all properties with this prefix also apply to `podman`. Our goal is to
support all of these for `firecracker` as well, but we are not there yet.

The following execution properties provide more customization.

- `container-image`: the container image to use, in the format `docker://<container-image>:<tag>`. The default value is `docker://gcr.io/flame-public/executor-docker-default:enterprise-v1.6.0`.
  For Firecracker, the image will be converted to a VM root image automatically.
- `container-registry-username` and `container-registry-password`:
  credentials to be used to pull private container images.
  These are not needed if the image is public. We recommend setting these
  via remote headers, to avoid storing them in the cache.
- `dockerUser`: determines which user the action should be run with inside
  the container. The default is the user set on the image.
  If setting to a non-root user, you may also need to set
  `nonroot-workspace` to `true`.

The following properties apply to `podman` and `docker` isolation,
and are currently unsupported by `firecracker`. (The `docker` prefix is
just a historical artifact.)

- `dockerInit`: determines whether `--init` should be used when starting a
  container. Available options are `true` and `false`. Defaults to `false`.
- `dockerRunAsRoot`: when set to `true`, forces the container to run as
  root, even the image specification specifies a non-root `USER`.
  Available options are `true` and `false`. Defaults to `false`.
- `dockerNetwork`: determines which network mode should be used. For
  `sandbox` isolation, this determines whether the network is enabled or
  not. Available options are `off` and `bridge`. The default is `bridge`,
  but we strongly recommend setting this to `off` for faster runner
  startup time. The latest version of the BuildBuddy toolchain does this
  for you automatically.

### Runner secrets

Please consult [RBE secrets](secrets) for more information on the related properties.

### Docker daemon support

For `firecracker` isolation, we support starting a [Docker daemon](https://docs.docker.com/config/daemon/)
(`dockerd`) which allows actions to run Docker containers.
Check out our [RBE with Firecracker MicroVMs](rbe-microvms) doc for examples.

The following `exec_properties` are supported:

- `init-dockerd`: whether to start the `dockerd` process inside the VM
  before execution. Available options are `true` and `false`. Defaults to
  `false`.
- `enable-dockerd-tcp`: whether `dockerd` should listen on TCP port 2375
  in addition to the default Unix domain socket. Available options are
  `true` and `false`. Defaults to `false`.
