package(default_visibility = ["//visibility:public"])

platform(
    name = "linux_x86_64",
    constraint_values = [
        "@platforms//cpu:x86_64",
        "@platforms//os:linux",
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
