load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "config",
    srcs = ["config.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/executor/config",
    visibility = ["//visibility:public"],
    deps = [
        "//server/config",
        "//server/util/flagutil",
    ],
)
