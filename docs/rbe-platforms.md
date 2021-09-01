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

### Passing credentials for Docker images

You can pull images from private container registries by adding the following
flags to your `bazel` command:

```
--remote_default_exec_properties=container-registry-username=USERNAME
--remote_default_exec_properties=container-registry-password=ACCESS_TOKEN
```

For the values of `USERNAME` and `ACCESS_TOKEN`, we recommend generating a
short lived token using the appropriate utility for your cloud provider.

For GCR (Google Container Registry), the username must be `_dcgcloud_token`
and a token can be generated with `gcloud auth print-access-token`:

```
--remote_default_exec_properties=container-registry-username=_dcgcloud_token
--remote_default_exec_properties=container-registry-password="$(gcloud auth print-access-token)"
```

For Amazon ECR (Elastic Container Registry), the username must be `AWS`
and a token can be generated with `aws ecr get-login-password --region REGION`
(replace `REGION` with the region matching the ECR image URL):

```
--remote_default_exec_properties=container-registry-username=AWS
--remote_default_exec_properties=container-registry-password="$(aws ecr get-login-password --region REGION)"
```

Some cloud providers may also allow the use of long-lived tokens, which
can be set directly in your `platform` definition, rather than at the
command line. For example, GCR allows setting a username of `_json_key`
and then using a service account's [JSON-format private key](https://cloud.google.com/iam/docs/creating-managing-service-account-keys)
as the password:

```python
SERVICE_ACCOUNT_KEY = """
{
    "type": "service_account",
    "project_id": my-project-id",
    "private_key_id": "...",
    "private_key": "...",
    // More fields omitted
    ...
}
"""

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
        "container-registry-username": "_json_key",
        "container-registry-password": SERVICE_ACCOUNT_KEY,
    },
)
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
