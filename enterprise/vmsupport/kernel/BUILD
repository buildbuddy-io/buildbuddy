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

# x86_64 (v6.18) - EXPERIMENTAL (used by the upgrade-fc-guest-kernel experiment)
#
# Build and upload with:
#   bazel run //enterprise/vmsupport/kernel:guest_kernel_x86_64_v6_18_upload --config=remote
microvm_kernel(
    name = "guest_kernel_x86_64_v6_18",
    out = "vmlinux-x86_64-v6.18",
    arch = "x86_64",
    config = "microvm-kernel-x86_64-v6.18.config",
    git_tag = "v6.18.57",
    version = "v6.18",
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
