# TODO(bduffany): The sh_toolchain config here is a workaround for
# https://github.com/aspect-build/rules_swc/issues/20
# We should probably either move these to the buildbuddy-toolchain repo
# or add a symlink from /usr/bin/bash -> /bin/bash to remove the need for these.
load("@bazel_tools//tools/sh:sh_toolchain.bzl", "sh_toolchain")

sh_toolchain(
    name = "bash_rbe",
    path = "/bin/bash",
)

toolchain(
    name = "sh_toolchain",
    toolchain = ":bash_rbe",
    toolchain_type = "@bazel_tools//tools/sh:toolchain_type",
)

# A duplicate of @buildbuddy_toolchain//:ubuntu_cc_toolchain
# with correct cc_compiler constraint value.
# TODO(sluongng): upstream this to buildbuddy-toolchain
toolchain(
    name = "ubuntu_cc_toolchain",
    exec_compatible_with = [
        "@platforms//cpu:x86_64",
        "@platforms//os:linux",
        # This is not needed for the toolchain itself,
        # but it helps us differentiate between the local platform and the remote RBE platform.
        #
        # For local builds, we do NOT want to use this toolchain as the gcc and glibc versions might differ.
        "@bazel_tools//tools/cpp:gcc",
    ],
    target_compatible_with = [
        "@platforms//os:linux",
        "@platforms//cpu:x86_64",
    ],
    toolchain = "@buildbuddy_toolchain//:ubuntu_local_cc_toolchain",
    toolchain_type = "@bazel_tools//tools/cpp:toolchain_type",
)
