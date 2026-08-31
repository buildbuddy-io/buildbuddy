package(default_visibility = ["//visibility:public"])

# Target this platforms with --platforms for a static build linked against musl libc.
# Only meant to be used as a target platform.
alias(
    name = "linux_x86_64_musl",
    actual = "@llvm//platforms:linux_x86_64_musl",
)

# Target this platforms with --platforms for a static build linked against musl libc.
# Only meant to be used as a target platform.
alias(
    name = "linux_arm64_musl",
    actual = "@llvm//platforms:linux_arm64_musl",
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
