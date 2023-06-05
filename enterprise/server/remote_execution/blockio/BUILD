load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "blockio",
    srcs = ["blockio.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/blockio",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/status",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])
