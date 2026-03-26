# Firecracker Guest Kernel Build + Upload

This directory contains the configs and scripts for building guest kernels used
by Firecracker.

Base configs are derived from Firecracker guest configs:
https://github.com/firecracker-microvm/firecracker/tree/main/resources/guest_configs

## Building and uploading the guest kernel

When updating guest kernel configs, you need to build them and manually
upload them to GCS.

There are two ways to do this, listed below. For both methods, it is
relatively safe to let the tools upload their outputs to GCS
automatically, since the GCS URLs contain a sha256 hash of the artifacts
that were built.

### Method 1: Invoke `rebuild.sh` from the host

This method may be useful in cases where you're doing lots of iterations
(e.g. kernel debugging) and you don't mind installing all the kernel's
system dependencies to your host. If you're just making a small config
tweak, method 2 is likely easier.

Run `rebuild.sh` to build the guest kernel images and upload them to GCS.

When run from an x86_64 host, it will build and upload the x86_64 guest
kernel. When run from an arm64 host, it will build and upload the arm64
guest kernel.

```bash
cd enterprise/vmsupport/kernel
./rebuild.sh
```

To skip GCS upload, set `SKIP_UPLOAD=1`.

### Method 2 (recommended): build and upload one kernel with bazel

Run the `_upload` target for the kernel you want to publish. Each target
builds exactly one kernel, then runs the upload tool with that kernel's
arch, version, and runfile path:

```bash
# x86_64 v5.15
bb run //enterprise/vmsupport/kernel:guest_kernel_x86_64_v5_15_upload --config=remote

# x86_64 v6.1
bb run //enterprise/vmsupport/kernel:guest_kernel_x86_64_v6_1_upload --config=remote

# aarch64 v5.10
# --config=target-linux-x86-exec-multiarch enables the BB arm64 platform in the build graph
bb run //enterprise/vmsupport/kernel:guest_kernel_aarch64_v5_10_upload --config=remote --config=target-linux-x86-exec-multiarch
```

## Using the guest kernel

After building and uploading the guest kernel, update the
`org_kernel_git_linux_kernel-vmlinux*` targets in `deps.bzl` with the new
URLs and sha256 for the target(s) you intended to rebuild. This
information should get printed out by the build tools from the above
steps.

## Updating the guest kernel builder image

The kernel requires various system dependencies in order to build.
These are all conveniently packaged up into the guest kernel builder
image under `//enterprise/dockerfiles/guest_kernel_builder`.

If you need to update the docker image (e.g. adding deps):

1. Update the Dockerfile
2. Build and push the image for both amd64 and arm64:

```bash
./enterprise/tools/build_images/build_image.py \
  --registry=gcr.io \
  --repository=flame-public/guest-kernel-builder \
  --tag=latest \
  --no-suffix \
  --dockerfile=enterprise/dockerfiles/guest_kernel_builder/Dockerfile \
  --platform=amd64 \
  --platform=arm64
```

3. Update the docker image SHA256 hash in `./enterprise/vmsupport/kernel/defs.bzl`
