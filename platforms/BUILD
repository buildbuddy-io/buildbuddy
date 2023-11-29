load("@io_buildbuddy_buildbuddy_toolchain//:rules.bzl", "UBUNTU20_04_IMAGE")
load("@local_config_platform//:constraints.bzl", "HOST_CONSTRAINTS")
load(":platforms.bzl", "get_parent_from_constraints")

package(default_visibility = ["//visibility:public"])

platform(
    name = "linux",
    constraint_values = [
        "@platforms//os:linux",
        # This helps us differentiate between the default local platform, which does not have this,
        # and the remote platform, which does have this.
        # For more information, see //toolchains:BUILD comments.
        #
        # We can remove this constraint value once we migrated to a hermetic toolchain.
        "@bazel_tools//tools/cpp:gcc",
    ],
    exec_properties = {
        "OSFamily": "Linux",
        "dockerNetwork": "off",
    },
)

platform(
    name = "linux_x86_64",
    constraint_values = ["@platforms//cpu:x86_64"],
    exec_properties = {"container-image": "docker://" + UBUNTU20_04_IMAGE},
    parents = [":linux"],
)

platform(
    name = "macos",
    constraint_values = ["@platforms//os:macos"],
    exec_properties = {
        "OSFamily": "darwin",
    },
)

platform(
    name = "macos_x86_64",
    constraint_values = ["@platforms//cpu:x86_64"],
    exec_properties = {
        "Arch": "amd64",
    },
    parents = [":macos"],
)

platform(
    name = "macos_arm64",
    constraint_values = ["@platforms//cpu:arm64"],
    exec_properties = {
        "Arch": "arm64",
    },
    parents = [":macos"],
)

platform(
    name = "local_config_platform",
    constraint_values = HOST_CONSTRAINTS,
    parents = [get_parent_from_constraints(HOST_CONSTRAINTS)],
)
