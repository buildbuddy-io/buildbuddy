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

```yaml
remote_execution:
  enable_remote_exec: true
```

## Executor config

BuildBuddy RBE executors take their own configuration file that is pulled from `/config.yaml` on the executor docker image. Using BuildBuddy's [Enterprise Helm chart](enterprise-helm.md) will take care of most of this configuration for you.

Here is an example:

```yaml
executor:
  app_target: "grpcs://your.buildbuddy.install:443"
  root_directory: "/buildbuddy/remotebuilds/"
  local_cache_directory: "/buildbuddy/filecache/"
  local_cache_size_bytes: 5000000000 # 5GB
  docker_socket: /var/run/docker.sock
```

### Container registry authentication

By default, executors will respect the container registry configuration in
`~/.docker/config.json`. The format of this file is described [here](https://docs.docker.com/engine/reference/commandline/login/).
Any credential helpers configured there will be respected.

For convenience, per-registry credentials can also be statically configured
in the executor config YAML. These credentials will take priority over the
configuration in `~/.docker/config.json`.

Here is an example:

```yaml
executor:
  container_registries:
    - hostnames:
        - "my-private-registry.io"
        - "subdomain.my-private-registry.io"
      username: "registry-username"
      password: "registry-password-or-long-lived-token"
```

This is especially useful for registries that allow using static tokens
for authentication, which avoids the need to set up credential helpers.

For example, Google Container Registry allows setting a username of
"\_json_key" and then passing the service account key directly:

```yaml
executor:
  container_registries:
    - hostnames:
        - "gcr.io"
        - "marketplace.gcr.io"
      username: "_json_key"
      # Note: the YAML multiline string syntax ">" is used to embed the
      # key JSON as a raw string. Be sure to indent as shown below:
      password: >
        {
          "type": "service_account",
          "project_id": my-project-id",
          "private_key_id": "...",
          "private_key": "...",
          // More fields omitted
          ...
        }
```

## Executor environment variables

In addition to the config.yaml, there are also environment variables that executors consume. To get more information about their environment. All of these are optional, but can be useful for more complex configurations.

- `SYS_MEMORY_BYTES`: The amount of memory (in bytes) that this executor is allowed to consume. Defaults to free system memory.
- `SYS_MILLICPU`: The amount of CPU (in millicpus) that this executor is allowed to consume. Defaults to system CPU.
- `MY_NODENAME`: The name of the machine/node that the executor is running on. Defaults to empty string.
- `MY_HOSTNAME`: The hostname by which the app can communicate to this executor. Defaults to machine hostname.
- `MY_PORT`: The port over which the app can communicate with this executor. Defaults to the executor's gRPC port.
- `MY_POOL`: The executor pool that this executor should be placed in. Defaults to empty string.

Many of these environment variables are typically set based on Kubernetes FieldRefs like so:

```yaml
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
