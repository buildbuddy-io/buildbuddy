# TODO(bduffany): The sh_toolchain config here is a workaround for
# https://github.com/aspect-build/rules_swc/issues/20
# We should probably either move these to the buildbuddy-toolchain repo
# or add a symlink from /usr/bin/bash -> /bin/bash to remove the need for these.
load("@bazel_tools//tools/sh:sh_toolchain.bzl", "sh_toolchain")

sh_toolchain(
    name = "bash_rbe_ubuntu1604",
    path = "/bin/bash",
)

toolchain(
    name = "sh_toolchain",
    exec_compatible_with = [
        "@platforms//os:linux",
    ],
    toolchain = ":bash_rbe_ubuntu1604",
    toolchain_type = "@bazel_tools//tools/sh:toolchain_type",
)
