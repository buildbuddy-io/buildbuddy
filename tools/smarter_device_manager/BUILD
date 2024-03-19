load("@io_bazel_rules_docker//go:image.bzl", "go_image")
load("@io_bazel_rules_docker//container:container.bzl", "container_push")

# We use this internally so create an alias here to
# ensure our changes don't break the internal build.
alias(
    name = "smarter-device-manager",
    actual = "@com_gitlab_arm_research_smarter_smarter_device_manager//:smarter-device-manager",
)

go_image(
    name = "smarter_device_manager_image",
    base = "@buildbuddy_go_image_base//image",
    binary = ":smarter-device-manager",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)

container_push(
    name = "push_image",
    format = "Docker",
    image = ":smarter_device_manager_image",
    registry = "gcr.io",
    repository = "flame-build/smarter-device-manager",
    tag = "server_image",
    tags = [
        "manual",
    ],
)
