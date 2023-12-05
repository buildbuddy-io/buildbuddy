load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "bisect",
    srcs = ["bisect.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/bisect",
    visibility = ["//visibility:public"],
    deps = [
        "//cli/arg",
        "//server/util/flag",
    ],
)

package(default_visibility = ["//cli:__subpackages__"])
