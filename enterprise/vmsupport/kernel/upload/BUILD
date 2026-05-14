load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_binary(
    name = "upload",
    embed = [":upload_lib"],
    tags = ["manual"],
    visibility = ["//visibility:public"],
)

go_library(
    name = "upload_lib",
    srcs = ["upload.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/vmsupport/kernel/upload",
    tags = ["manual"],
    deps = [
        "//server/util/flag",
        "//server/util/log",
        "@io_bazel_rules_go//go/runfiles",
    ],
)
