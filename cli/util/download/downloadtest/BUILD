load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "downloadtest",
    srcs = ["downloadtest.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/util/download/downloadtest",
    visibility = ["//visibility:public"],
    deps = ["//cli/util/download"],
)
