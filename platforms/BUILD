package(default_visibility = ["//visibility:public"])

# Target this platforms with --platforms for a static build linked against musl libc.
# Only meant to be used as a target platform.
platform(
    name = "linux_x86_64_musl",
    constraint_values = [
        "@platforms//cpu:x86_64",
        "@platforms//os:linux",
        "//toolchains:musl_on",
    ],
)

# Target this platforms with --platforms for a static build linked against musl libc.
# Only meant to be used as a target platform.
platform(
    name = "linux_arm64_musl",
    constraint_values = [
        "@platforms//cpu:arm64",
        "@platforms//os:linux",
        "//toolchains:musl_on",
    ],
)

platform(
    name = "linux_x86_64",
    constraint_values = [
        "@platforms//os:linux",
        "@platforms//cpu:x86_64",
    ],
)

platform(
    name = "linux_arm64",
    constraint_values = [
        "@platforms//os:linux",
        "@platforms//cpu:arm64",
    ],
)

platform(
    name = "windows_x86_64",
    constraint_values = [
        "@platforms//os:windows",
        "@platforms//cpu:x86_64",
    ],
)

platform(
    name = "darwin_arm64",
    constraint_values = [
        "@platforms//os:osx",
        "@platforms//cpu:arm64",
    ],
)

# define a platform for the container runtime on your host.
# It uses the same CPU architecture as Bazel itself (i.e. arm64 on an Apple Silicon Mac), but always target Linux.
platform(
    name = "host_docker_platform",
    constraint_values = ["@platforms//os:linux"],
    parents = ["@platforms//host"],
)
