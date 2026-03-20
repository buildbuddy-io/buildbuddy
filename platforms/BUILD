package(default_visibility = ["//visibility:public"])

# Target this platform for Linux x86_64 builds linked against the default libc.
platform(
    name = "linux_x86_64",
    constraint_values = [
        "@platforms//cpu:x86_64",
        "@platforms//os:linux",
    ],
)

# Target this platform for Linux arm64 builds linked against the default libc.
platform(
    name = "linux_arm64",
    constraint_values = [
        "@platforms//cpu:arm64",
        "@platforms//os:linux",
    ],
)

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
