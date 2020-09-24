load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["hit_tracker.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/remote_cache/hit_tracker",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:cache_go_proto",
        "//proto:remote_execution_go_proto",
        "//server/environment:go_default_library",
        "//server/interfaces:go_default_library",
    ],
)
