load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "generate_ext4_image_lib",
    srcs = ["main.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/firecracker/testdata/generate_ext4_image",
    visibility = ["//visibility:private"],
    deps = [
        "//enterprise/server/oci/ociconv",
        "//enterprise/server/util/oci",
        "//server/real_environment",
    ],
)

go_binary(
    name = "generate_ext4_image",
    embed = [":generate_ext4_image_lib"],
    visibility = ["//visibility:public"],
)
