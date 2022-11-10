load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "workspace",
    srcs = ["workspace.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/workspace",
    visibility = ["//visibility:public"],
    deps = ["//server/util/disk"],
)
