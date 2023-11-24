---
id: rbe-platforms
title: RBE Platforms
sidebar_label: RBE Platforms
---

## BuildBuddy default

BuildBuddy's default platform is Ubuntu 16.04 with Java 8 installed. Building on our basic command can specify this platform with the `--host_platform` flag:

```
--host_platform=@buildbuddy_toolchain//:platform
```

## Using a custom Docker image

You can configure BuildBuddy RBE to use a custom docker image, by adding the following rule to a BUILD file:

```python
platform(
    name = "docker_image_platform",
    constraint_values = [
        "@bazel_tools//platforms:x86_64",
        "@bazel_tools//platforms:linux",
        "@bazel_tools//tools/cpp:clang",
    ],
    exec_properties = {
        "OSFamily": "Linux",
        "container-image": "docker://gcr.io/YOUR:IMAGE",
    },
)
```

Make sure to replace `gcr.io/YOUR:IMAGE` with your docker image url.

You can then pass this configuration to BuildBuddy RBE with the following flag:

```
--host_platform=//:docker_image_platform
```

This assumes you've placed this rule in your root BUILD file. If you place it elsewhere, make sure to update the path accordingly.

### ENTRYPOINT and CMD

Remote build actions will be run in your container via `CMD`, so note that any `CMD` instructions in your Dockerfile will be ignored.
`ENTRYPOINT`, on the other hand, is not ignored, so make sure that the container image's `ENTRYPOINT` is either unset,
or is a wrapper that is compatible with your build actions' commands.

For more information, see [Understand how CMD and ENTRYPOINT interact](https://docs.docker.com/engine/reference/builder/#understand-how-cmd-and-entrypoint-interact).

### Passing credentials for Docker images

You can use images from private container registries by adding the following
flags to your `bazel` command (replace `USERNAME` and `ACCESS_TOKEN` with
the appropriate credentials for the container registry):

```
--remote_exec_header=x-buildbuddy-platform.container-registry-username=USERNAME
--remote_exec_header=x-buildbuddy-platform.container-registry-password=ACCESS_TOKEN
```

For the value of `ACCESS_TOKEN`, we recommend generating a short-lived
token using the command-line tool for your cloud provider.

To generate a short-lived token for GCR (Google Container Registry),
the username must be `_dcgcloud_token` and the token can be generated with
`gcloud auth print-access-token`:

```
--remote_exec_header=x-buildbuddy-platform.container-registry-username=_dcgcloud_token
--remote_exec_header=x-buildbuddy-platform.container-registry-password="$(gcloud auth print-access-token)"
```

For Amazon ECR (Elastic Container Registry), the username must be `AWS`
and a short-lived token can be generated with `aws ecr get-login-password --region REGION`
(replace `REGION` with the region matching the ECR image URL):

```
--remote_exec_header=x-buildbuddy-platform.container-registry-username=AWS
--remote_exec_header=x-buildbuddy-platform.container-registry-password="$(aws ecr get-login-password --region REGION)"
```

Some cloud providers may also allow the use of long-lived tokens, which
can also be used in remote headers. For example, GCR allows setting a
username of `_json_key` and then using a service account's
[JSON-format private key](https://cloud.google.com/iam/docs/creating-managing-service-account-keys)
as the password. Note that remote headers cannot have newlines;
the command `tr '\n' ' '` is used in this example to remove them:

```
--remote_exec_header=x-buildbuddy-platform.container-registry-username=_json_key
--remote_exec_header=x-buildbuddy-platform.container-registry-password="$(cat service-account-keyfile.json | tr '\n' ' ')"
```

## Specifying a custom executor pool

You can configure BuildBuddy RBE to use a custom executor pool, by adding the following rule to a BUILD file:

```python
platform(
    name = "gpu_platform",
    constraint_values = [
        "@bazel_tools//platforms:x86_64",
        "@bazel_tools//platforms:linux",
        "@bazel_tools//tools/cpp:clang",
    ],
    exec_properties = {
        "OSFamily": "Linux",
        "Pool": "my-gpu-pool",
    },
)
```

Make sure to replace `my-gpu-pool` with your pool name.

You can then pass this configuration to BuildBuddy RBE with the following flag:

```
--host_platform=//:gpu_platform
```

This assumes you've placed this rule in your root BUILD file. If you place it elsewhere, make sure to update the path accordingly.

For instructions on how to deploy custom executor pools, we the [RBE Executor Pools docs](rbe-pools.md).

## Target level execution properties

If you want different targets to run in different RBE environments, you can specify `exec_properties` at the target level. For example if you want to run one set of tests in a high-memory pool, or another set of targets on executors with GPUs.

```python
go_test(
    name = "memory_hogging_test",
    srcs = ["memory_hogging_test.go"],
    embed = [":go_default_library"],
    exec_properties = {
        "Pool": "high-memory-pool",
    },
)
```

## Execution Properties

BuildBuddy RBE comes with some `exec_properties` that you could use to modify how the actions
are executed in the remote environment.

These properties could be used in different ways:

- Setting `--remote_default_exec_properties=KEY=VALUE` in `.bazelrc` or in Bazel command line.
- Setting `exec_properties` in Execution Platform definition.
- Setting `exec_properties` in each BUILD target.

Rules authors could also leverage [Execution Group](https://bazel.build/extending/exec-groups) to give users
more control over which Execution Properties could be used for each group of actions in each BUILD target.

### Executor Selection properties

These execution properties affect how our Action Scheduler select which BuildBuddy Executor to run the Action on:

- `Pool`: select which [Executor Pool](./rbe-pools) to use. If unset, we will use the `default` executor pool.
- `OSFamily`: select which Operating System the Executor should be running on. Available options are `linux` (default), `darwin` and `windows`.
- `Arch`: select which CPU architecture the Executor should be running on. Available options are `amd64` (default) and `arm64`.
- `use-self-hosted-executors`: select from [self-hosted Executors](./enterprise-rbe) instead of BuildBuddy's managed executor pool. Available options are "true" and "false". Default value is configurable on the [Organization's Setting page](https://app.buildbuddy.io/settings/).

### Runner Isolation properties

Each BuildBuddy Executor could spin up multiple "Runners", each is in charge of executing one Action at the time.
Inside each Runner is a "Workspace", which represents the working directory of the Action and the Action's Input Tree.
These are some way you could customize the Runner and Workspace that help execute your actions.

- `workload-isolation-type`: select which isolation technology the Runners use. Available options are `docker`, `podman`, `firecracker`, `sandbox`, `none`.The Executor must have relevant sanddbox options enabled. If unset, use the default isolation configured in Executor. On BuildBuddy Cloud (SaaS), `podman` (default) and `firecracker` isolation are available.
- `recycle-runner`: Whether to retain the runner after action have executed and reuse to execute subsequent actions. Available options are `true` and `false`. Recycled runner's workspace are subjected to clean up operations between Actions.
- `preserve-workspace`: Only applicable when `"recycle-runner": "true"` is set. Whether to re-use the Workspace directory from the previous action. Available options are `true` and `false`.
- `clean-workspace-inputs`: A glob value that let user's selectively pick which files in Action's Input Tree to clean up before action starts. The string value should follow the specification in [gobwas/glob](https://pkg.go.dev/github.com/gobwas/glob#Compile) library.
- `nonroot-workspace`: If set to `true`, the workspace directory will be writeable by non-root users (permission `0o777`). Otherwise, it will be read-only to non-root users (permission `Oo755`).

### Runner Resource properties

To help aid our Scheduler in bin-packing Actions into Executor more effectively,
user could choose to set the following properties and hint on how much compute resources an action would require.

- `EstimatedComputeUnits`: Numerical values (i.e. `1`, `9`). A BuildBuddy's Compute Unit is defined as 1 cpu and 2.5GB of memory.
- `EstimatedCPU`: The amount of CPU an Action would consume. Example valid values:
  - `2`: 2000 MiliCPU
  - `0.5`: 500 MiliCPU
  - `+0.1e+1`: 1000 MiliCPU
  - `4000m`: 4000 MiliCPU
- `EstimatedMemory`: The amount of Memory an Action would consume. Example valid values:
  - `1M`: 1 MB
  - `2GB`: 2 GB
  - `4.5GB`: 4.5 GB
- `EstimatedFreeDiskBytes`: The amount of Disk Space an Action would consume. Example valid values:
  - `1M`: 1 MB
  - `2GB`: 2 GB
  - `4.5GB`: 4.5 GB

### Remote Persistent Worker properties

Similar to local execution environment, the remote execution environment could also retains a long running process
acting as a [Persistent Worker](https://bazel.build/remote/persistent) to help reducing cold-start for build Actions with high overhead startup cost.
These settings came with the assumption that `"recycle-runner": "true"` is set to enable re-use of action runners.

The Bazel's flag ["--experimental_remote_mark_tool_inputs"](https://bazel.build/reference/command-line-reference#flag--experimental_remote_mark_tool_inputs) should help set these automatically so you don't have to set them manually.
However, we do provide these `exec_properties` for rules authors to experiment with.

- `persistentWorkerKey`: Unique key for the Persistent Worker. This should be automatically set by Bazel.
- `persistentWorkerProtocol`: The protocol used by the Persistent Worker. Available options are `proto` (default) and `json`.

### Runner Container support

For `docker`, `podman` and `firecracker` isolation, our runner support running actions in user-provided container images.
Here are a few Execution Properties that provide users more customizations.

- `container-image`: the container image do use in the format `docker://<container-image>:<tag>`. Default image is `docker://gcr.io/flame-public/executor-docker-default:enterprise-v1.6.0`.
- `container-registry-username` and `container-registry-password`: credentials to be used to pull private container images. Not needed if the image is public.

- `dockerInit`: Specific to `podman` and `docker` isolation. Determines whether `--init` should be used when starting a container. Available options are "true" and "false".
- `dockerUser`: Determines which user the action should be run with inside the container image. Default is to the user set on the image.
- `dockerRunAsRoot`: Determines which user the action should be run with inside the container image. Available options are "true" and "false" (default).
- `dockerNetwork`: Determine what network mode should be used with `podman` and `docker` isolation. For `sandbox` isolation, this determines whether network is enabled or not. Available options are `off` and `bridge`. Although the default is unset, we strongly recommend setting this to `off` for faster runner startup time.

### Runner Secret support

Please consult our [RBE Secrets](./secrets) doc for more information on the related Properties.

### DockerD support

For `firecracker` isolation, we support starting a `dockerd` process for actions requiring access to
an isolated [Docker daemon](https://docs.docker.com/config/daemon/).
Checkout our [RBE with Firecracker MicroVMs](./rbe-microvms) doc for example usages.

- `init-dockerd`: Whether to start the `dockerd` process inside Firecracker MicroVM. Available options are `true` and `false` (default).
- `enable-dockerd-tcp`: Whether to expose `dockerd` host via TCP connection in additional to the default Unix Domain Socket. Available options are `true` and `false` (default).
