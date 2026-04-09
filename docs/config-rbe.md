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

```yaml title="config.yaml"
remote_execution:
  enable_remote_exec: true
```

## Executor config

BuildBuddy RBE executors take their own configuration file that is pulled from `/config.yaml` on the executor docker image. Using BuildBuddy's [Enterprise Helm chart](enterprise-helm.md) will take care of most of this configuration for you.

Here is an example:

```yaml title="config.yaml"
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

```yaml title="config.yaml"
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

```yaml title="config.yaml"
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

### GPU support

Self-hosted executors can expose GPUs to remotely executed actions. Setup
requires the following steps.

#### 1. Install GPU drivers and device plugin

Follow the GPU vendor's instructions to install GPU drivers on the
Kubernetes nodes and deploy the corresponding Kubernetes device plugin so
GPU resources can be requested by executor Pods:

- [AMD](https://github.com/ROCm/k8s-device-plugin#deployment)
- [Intel](https://intel.github.io/intel-device-plugins-for-kubernetes/cmd/gpu_plugin/README.html)
- [NVIDIA](https://github.com/NVIDIA/k8s-device-plugin#quick-start)

After completing these steps, the executor deployment YAML should resemble
the following:

```yaml
spec:
  containers:
    - name: buildbuddy-executor
      # ...
      resources:
        limits:
          gpu-vendor.example/gpu: 1 # requesting 1 GPU from gpu-vendor.example
```

To verify whether the device is visible to the executor, run one of these
vendor-specific commands:

```bash
# AMD
kubectl exec deployment/buildbuddy-executor -- sh -xc 'stat /dev/kfd /dev/dri/renderD128'

# Intel
kubectl exec deployment/buildbuddy-executor -- sh -xc 'ls /dev/dri/renderD*'

# NVIDIA
kubectl exec deployment/buildbuddy-executor -- nvidia-smi
```

#### 2. Mount host paths for container runtime support

:::note

This step is not required when using `docker` isolation with
`/var/run/docker.sock` mounted from the host, which is the default only
for Helm chart versions below v0.0.242.

:::

Because actions run inside child containers, the executor needs additional
host mounts to pass through GPU-related configuration.

Example configuration:

```yaml
spec:
  template:
    spec:
      containers:
        - name: executor
          volumeMounts:
            # Pass through CDI device specifications from the host so that
            # they can be used for child containers.
            # These specifications tell the executor which device-specific
            # files need to be mounted into child containers.
            - name: host-cdi-etc
              mountPath: /etc/cdi
              readOnly: true
            - name: host-cdi-run
              mountPath: /var/run/cdi
              readOnly: true
            # NVIDIA only: vulkan has some container support files which
            # are used to provision the executor container but which need
            # to be manually forwarded in order to provision child
            # containers.
            - name: host-vulkan
              mountPath: /usr/share/vulkan
              readOnly: true
      volumes:
        - name: host-cdi-etc
          hostPath:
            path: /etc/cdi
            type: DirectoryOrCreate
        - name: host-cdi-run
          hostPath:
            path: /var/run/cdi
            type: DirectoryOrCreate
        - name: host-vulkan
          hostPath:
            path: /usr/share/vulkan
            type: DirectoryOrCreate
```

#### 3. Enable GPU devices in child containers

Once GPU devices are available to the executor container, the remaining
configuration depends on the isolation type:

```yaml title="config.yaml"
executor:
  # If using OCI isolation (the default for buildbuddy-executor Helm chart v0.0.306+)
  oci:
    # Fully-qualified CDI device names.
    cdi_devices:
      # NVIDIA example:
      - "nvidia.com/gpu=all"
      # AMD example:
      - "amd.com/gpu=all"
    # Optional override of CDI spec directories:
    cdi_spec_dirs:
      - "/etc/cdi"
      - "/var/run/cdi"

  # If using Podman isolation (the default for buildbuddy-executor Helm chart v0.0.242 - v0.0.305)
  podman:
    gpus: "all" # or "0", "1", etc.

  # If using Docker isolation (the default for buildbuddy-executor Helm chart versions below v0.0.242)
  docker_gpus: "all" # or "0", "1", etc.
```

#### Forwarding container tools

In some custom setups, the CDI spec may reference additional host paths
needed for container setup that are not included as mounts within the spec
itself.

If the GPU device is visible within actions but issues arise such as
missing libraries or other resources, check the CDI spec (typically at
`/etc/cdi/<vendor>.yaml` or `/var/run/cdi/<vendor>.yaml` on the host
node) to see whether there are files used during container setup (e.g. in
`hooks`) that are not mounted into the child container itself (in
`mounts`).

For example, `/usr/bin/nvidia-cdi-hook` is required for provisioning child
containers when using NVIDIA GPUs, but this tool is not listed as a mount
in the CDI spec, so the executor container cannot pass it through to child
containers by default. For `nvidia-cdi-hook` specifically, BuildBuddy
handles this by distributing NVIDIA's container tools within the executor
image. If the CDI specs reference other custom tools, it may be
necessary to extend the executor image or mount them from the host.

## Executor environment variables

In addition to the config.yaml, there are also environment variables that executors consume. To get more information about their environment. All of these are optional, but can be useful for more complex configurations.

- `SYS_MEMORY_BYTES`: The amount of memory (in bytes) that this executor is allowed to consume. Defaults to free system memory.
- `SYS_CPU`: The amount of CPU that this executor is allowed to consume. Can be a core count such as `1.5` or a milli-CPU count such as `1500m`. Defaults to system CPU.
- `MY_NODENAME`: The name of the machine/node that the executor is running on. Defaults to empty string.
- `MY_HOSTNAME`: The hostname by which the app can communicate to this executor. Defaults to machine hostname.
- `MY_PORT`: The port over which the app can communicate with this executor. Defaults to the executor's gRPC port.
- `MY_POOL`: The executor pool that this executor should be placed in. Defaults to empty string.

Many of these environment variables are typically set based on Kubernetes FieldRefs like so:

```yaml title="config.yaml"
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
