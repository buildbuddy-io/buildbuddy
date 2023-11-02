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
