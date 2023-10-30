package(default_visibility = ["//visibility:public"])

platform(
    name = "linux_x86_64",
    constraint_values = [
        "@platforms//cpu:x86_64",
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
        "container-image": "docker://gcr.io/flame-public/rbe-ubuntu20-04:latest",
        "dockerNetwork": "off",
    },
)

platform(
    name = "macos_x86_64",
    constraint_values = [
        "@platforms//cpu:x86_64",
        "@platforms//os:macos",
    ],
    exec_properties = {
        "Arch": "amd64",
        "OSFamily": "darwin",
    },
)

platform(
    name = "macos_arm64",
    constraint_values = [
        "@platforms//cpu:arm64",
        "@platforms//os:macos",
    ],
    exec_properties = {
        "Arch": "arm64",
        "OSFamily": "darwin",
    },
)

load("@io_buildbuddy_buildbuddy_toolchain//:rules.bzl", "UBUNTU20_04_IMAGE")

platform(
    name = "exec_linux",
    exec_properties = {
        "OSFamily": "Linux",
        "container-image": "docker://" + UBUNTU20_04_IMAGE,
        "dockerNetwork": "off",
    },
    constraint_values = ["@platforms//os:linux"]
)

platform(
    name = "exec_darwin",
    exec_properties = {
        "OSFamily": "Darwin",
        "container-image": "none",
        "dockerNetwork": "off",
    },
    constraint_values = ["@platforms//os:macos"]
)

load("@local_config_platform//:constraints.bzl", "HOST_CONSTRAINTS")

platform(
    name = "buildbuddy_rbe_default",
    constraint_values = HOST_CONSTRAINTS,
    parents = [":exec_linux"] if "@platforms//os:linux" in HOST_CONSTRAINTS else [":exec_darwin"] if "@platforms//os:osx" in HOST_CONSTRAINTS else [],
)

platform(
    name = "buildbuddy_rbe_default_with_clang",
    constraint_values = ["@bazel_tools//tools/cpp:clang"],
    parents = [":buildbuddy_rbe_default"],
)

platform(
    name = "buildbuddy_rbe_linux_x86_64",
    constraint_values = ["@platforms//cpu:x86_64"],
    parents = [":exec_linux"],
)

platform(
    name = "buildbuddy_rbe_linux_x86_64_with_clang",
    constraint_values = ["@bazel_tools//tools/cpp:clang"],
    parents = [":buildbuddy_rbe_linux_x86_64"],
)

platform(
    name = "buildbuddy_rbe_darwin_x86_64",
    constraint_values = ["@platforms//cpu:x86_64"],
    parents = [":exec_darwin"],
)

platform(
    name = "buildbuddy_rbe_darwin_x86_64_with_clang",
    constraint_values = ["@bazel_tools//tools/cpp:clang"],
    parents = [":buildbuddy_rbe_darwin_x86_64"],
)

platform(
    name = "buildbuddy_rbe_darwin_arm64",
    constraint_values = ["@platforms//cpu:aarch64"],
    parents = [":exec_darwin"],
)

platform(
    name = "buildbuddy_rbe_darwin_arm64_with_clang",
    constraint_values = ["@bazel_tools//tools/cpp:clang"],
    parents = [":buildbuddy_rbe_darwin_arm64"],
)
