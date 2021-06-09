load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "container",
    srcs = ["container.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/interfaces",
    ],
)
