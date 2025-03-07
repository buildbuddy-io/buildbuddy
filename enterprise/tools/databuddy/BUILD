load("@io_bazel_rules_docker//go:image.bzl", "go_image")

package(
    default_visibility = ["//enterprise:__subpackages__"],
)

alias(
    name = "databuddy",
    actual = "//enterprise/tools/databuddy/cmd/databuddy",
)

go_image(
    name = "databuddy_image",
    # TODO: use our own image here or rename the buildbuddy one to be more generic
    base = "@buildbuddy_go_image_base//image",
    binary = "//enterprise/tools/databuddy/cmd/databuddy",
    tags = ["manual"],
    visibility = [
        "//enterprise:__subpackages__",
        "@buildbuddy_internal//:__subpackages__",
    ],
)
