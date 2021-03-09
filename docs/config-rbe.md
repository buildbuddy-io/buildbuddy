---
id: config-rbe
title: RBE Configuration
sidebar_label: RBE
---

Remote Build Execution is only configurable in the [Enterprise version](enterprise.md) of BuildBuddy.

RBE configuration must be enabled in your `config.yaml` file, but most configuration is done via [toolchains](rbe-setup.md), [platforms](rbe-platforms.md), or the [enterprise Helm chart](enterprise-helm).

## Section

`remote_execution:` The remote_execution section allows you to configure BuildBuddy's remote build execution. **Optional**

## Options

**Optional**

- `enable_remote_exec:` True if remote execution should be enabled.
- `default_pool_name:` The default executor pool to use if one is not specified.


## Example section

```
remote_execution:
  enable_remote_exec: true
```

## Executor config

BuildBuddy RBE executors take their own configuration file that is pulled from `/config.yaml` on the executor docker image. Using BuildBuddy's [Enterprise Helm chart](enterprise-helm.md) will take care of most of this configuration for you.

Here is a minimal example (recommended):

```
executor:
  app_target: "grpcs://your.buildbuddy.install:443"
  root_directory: "/buildbuddy/remotebuilds/"
  local_cache_directory: "/buildbuddy/filecache/"
  local_cache_size_bytes: 5000000000 # 5GB
```

And a fully loaded example:

```
executor:
  app_target: "grpcs://your.buildbuddy.install:443"
  root_directory: "/buildbuddy/remotebuilds/"
  local_cache_directory: "/buildbuddy/filecache/"
  local_cache_size_bytes: 5000000000 # 5GB
  docker_sock: /var/run/docker.sock
auth:
  enable_anonymous_usage: true
  oauth_providers:
    - issuer_url: "https://accounts.google.com"
      client_id: "myclient.apps.googleusercontent.com"
      client_secret: "mysecret"
cache:
  redis_target: "my-release-redis-master:6379"
  gcs:
    bucket: "buildbuddy_cache_bucket"
    project_id: "myprojectid"
    credentials_file: "mycredentials.json"
    ttl_days: 30
```

## Executor environment variables.

In addition to the config.yaml, there are also environment variables that executors consume. To get more information about their environment. All of these are optional, but can be useful for more complex configurations.

- `SYS_MEMORY_BYTES`: The amount of memory (in bytes) that this executor is allowed to consume. Defaults to free system memory. 
- `SYS_MILLICPU`: The amount of CPU (in millicpus) that this executor is allowed to consume. Defaults to system CPU.
- `MY_NODENAME`: The name of the machine/node that the executor is running on. Defaults to empty string.
- `MY_HOSTNAME`: The hostname by which the app can communicate to this executor. Defaults to machine hostname.
- `MY_PORT`: The port over which the app can communicate with this executor. Defaults to the executor's gRPC port.
- `MY_POOL`: The executor pool that this executor should be placed in. Defaults to empty string.

Many of these environment variables are typically set based on Kubernetes FieldRefs like so:

```
  env:
    - name: SYS_MEMORY_BYTES
      valueFrom:
        resourceFieldRef:
          resource: limits.memory
    - name: SYS_MILLICPU
      valueFrom:
        resourceFieldRef:
          resource: limits.cpu
    - name: MY_HOSTNAME
      valueFrom:
        fieldRef:
          fieldPath: status.podIP
    - name: MY_NODENAME
      valueFrom:
        fieldRef:
          fieldPath: spec.nodeName
```