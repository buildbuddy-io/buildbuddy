load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "capabilities_server",
    srcs = ["capabilities_server.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/remote_cache/capabilities_server",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//proto:semver_go_proto",
    ],
)
