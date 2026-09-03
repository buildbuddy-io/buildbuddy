package(default_visibility = ["//visibility:public"])

# Target this platform with --platforms for a static build linked against musl libc.
# Only meant to be used as a target platform.
#
# The musl platforms also set the "static" Go build tag, so Go sources can opt
# out of dependencies that cannot be linked statically (e.g. libraries that
# dlopen shared objects at runtime). Setting the tag on the platform itself
# rather than in a bazelrc config means it applies however the platform is
# selected, whether via --config=static, --platforms, or the
# linux_x86_64_musl_alias transition in //rules:platform_transitions.bzl.
platform(
    name = "linux_x86_64_musl",
    flags = ["--@io_bazel_rules_go//go/config:tags=static"],
    parents = ["@llvm//platforms:linux_x86_64_musl"],
)

# Target this platform with --platforms for a static build linked against musl libc.
# Only meant to be used as a target platform.
platform(
    name = "linux_arm64_musl",
    flags = ["--@io_bazel_rules_go//go/config:tags=static"],
    parents = ["@llvm//platforms:linux_arm64_musl"],
)

alias(
    name = "linux_x86_64",
    actual = "@llvm//platforms:linux_x86_64_gnu.2.28",
)

alias(
    name = "linux_arm64",
    actual = "@llvm//platforms:linux_arm64_gnu.2.28",
)
