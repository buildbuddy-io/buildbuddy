load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "directory_size",
    srcs = ["directory_size.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/remote_cache/directory_size",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:cache_go_proto",
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/remote_cache/digest",
        "//server/util/log",
        "//server/util/status",
    ],
)
