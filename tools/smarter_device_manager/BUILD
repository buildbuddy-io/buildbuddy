load("@io_bazel_rules_docker//go:image.bzl", "go_image")

go_image(
    name = "smarter_device_manager_image",
    binary = "@com_gitlab_arm_research_smarter_device_manager//:smarter-device-manager",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)
