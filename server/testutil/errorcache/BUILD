load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "errorcache",
    srcs = ["error_cache.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/errorcache",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/interfaces",
    ],
)
