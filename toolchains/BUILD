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

constraint_setting(
    name = "use_musl",
    default_constraint_value = ":musl_off",
    visibility = [
        "//visibility:public",
    ],
)

constraint_value(
    name = "musl_on",
    constraint_setting = ":use_musl",
    visibility = [
        "//visibility:public",
    ],
)

constraint_value(
    name = "musl_off",
    constraint_setting = ":use_musl",
    visibility = [
        "//visibility:public",
    ],
)
