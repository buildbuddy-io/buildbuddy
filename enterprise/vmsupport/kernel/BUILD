load(":defs.bzl", "microvm_kernel")

package(default_visibility = ["//enterprise:__subpackages__"])

# x86_64 (v5.15)
microvm_kernel(
    name = "guest_kernel_x86_64_v5_15",
    out = "vmlinux-x86_64-v5.15",
    arch = "x86_64",
    config_src = "microvm-kernel-x86_64-v5.15.config",
    kernel_version = "v5.15",
)

# x86_64 (v6.1)
# EXPERIMENTAL. Not used in prod yet.
microvm_kernel(
    name = "guest_kernel_x86_64_v6_1",
    out = "vmlinux-x86_64-v6.1",
    arch = "x86_64",
    config_src = "microvm-kernel-x86_64-v6.1.config",
    kernel_version = "v6.1",
)

# arm64 (v5.10)
microvm_kernel(
    name = "guest_kernel_aarch64_v5_10",
    out = "vmlinux-aarch64-v5.10",
    arch = "arm64",
    config_src = "microvm-kernel-aarch64-v5.10.config",
    kernel_version = "v5.10",
)
