load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "bare",
    srcs = ["bare.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/bare",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/remote_execution/commandutil",
        "//enterprise/server/remote_execution/container",
        "//proto:remote_execution_go_proto",
        "//server/interfaces",
    ],
)
