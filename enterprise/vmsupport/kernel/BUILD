load(":defs.bzl", "microvm_kernel")

package(default_visibility = ["//enterprise:__subpackages__"])

# x86_64 (v5.15)
#
# Build and upload with:
#   bazel run //enterprise/vmsupport/kernel:guest_kernel_x86_64_v5_15_upload --config=remote
microvm_kernel(
    name = "guest_kernel_x86_64_v5_15",
    out = "vmlinux-x86_64-v5.15",
    arch = "x86_64",
    config = "microvm-kernel-x86_64-v5.15.config",
    version = "v5.15",
)

# x86_64 (v6.1) - EXPERIMENTAL (not used in prod yet)
#
# Build and upload with:
#   bazel run //enterprise/vmsupport/kernel:guest_kernel_x86_64_v6_1_upload --config=remote
microvm_kernel(
    name = "guest_kernel_x86_64_v6_1",
    out = "vmlinux-x86_64-v6.1",
    arch = "x86_64",
    config = "microvm-kernel-x86_64-v6.1.config",
    version = "v6.1",
)

# arm64 (v5.10)
#
# Build and upload with:
#   bazel run //enterprise/vmsupport/kernel:guest_kernel_aarch64_v5_10_upload --config=remote --config=target-linux-x86-exec-multiarch
microvm_kernel(
    name = "guest_kernel_aarch64_v5_10",
    out = "vmlinux-aarch64-v5.10",
    arch = "arm64",
    config = "microvm-kernel-aarch64-v5.10.config",
    version = "v5.10",
)
