---
id: rbe-pools
title: RBE Executor Pools
sidebar_label: RBE Executor Pools
---

By default, all BuildBuddy executors are placed in a single pool - and any task can run on any executor (running the same operating system and cpu architecture).

[Platforms](rbe-platforms.md) can be used to specify custom Docker images in which to run your actions, but sometimes you want control over more properties of the executor machine - like available memory, access to GPUs, or physical location.

To support these use cases, BuildBuddy allows executors to be registered in different pools - and for Bazel to select from these pools at either the Platform level or the target level, depending on your needs.

## Deploying executors in a pool

When creating an executor deployment, you can specify the name of the pool its executors should be registered to with the `MY_POOL` environment variable. This can be set to any string value.

If using the `buildbuddy/buildbuddy-executor` [Helm charts](https://github.com/buildbuddy-io/buildbuddy-helm/tree/master/charts/buildbuddy-executor), you can set this using the [poolName value](https://github.com/buildbuddy-io/buildbuddy-helm/blob/master/charts/buildbuddy-executor/values.yaml#L15).

## Setting the app's default pool name

By default, both executors and the BuildBuddy app do not set a pool name and any RBE request that comes in without a `Pool` property set will be sent to the default pool. If you'd like requests without a `Pool` property to be sent to a different default pool, you can set the app's `default_pool_name` in the `remote_execution` block of its `config.yaml`.

```
remote_execution:
    enable_remote_exec: true
    default_pool_name: my-default-pool
```

## Selecting a pool to run your builds on

Now that you've deployed multiple executor pools, you can select which pool you'd like your builds to run on - either at the platform level or the target level.

### Platform level

You can configure BuildBuddy RBE to use a custom executor pool at the platform level, by adding the following rule to a BUILD file:

```
platform(
    name = "gpu_platform",
    exec_properties = {
        "OSFamily": "Linux",
        "Pool": "my-gpu-pool",
    },
)
```

Make sure to replace `my-gpu-pool` with your docker image url.

You can then pass this configuration to BuildBuddy RBE with the following flag:

```
--host_platform=//:gpu_platform
```

This assumes you've placed this rule in your root BUILD file. If you place it elsewhere, make sure to update the path accordingly.

### Target level

If you want different targets to run in different RBE environments, you can specify `exec_properties` at the target level. For example if you want to run one set of tests in a high-memory pool, or another set of targets on executors with GPUs.

```
go_test(
    name = "memory_hogging_test",
    srcs = ["memory_hogging_test.go"],
    embed = [":go_default_library"],
    exec_properties = {
        "Pool": "high-memory-pool",
    },
)
```
