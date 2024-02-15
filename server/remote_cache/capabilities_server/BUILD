load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "capabilities_server",
    srcs = ["capabilities_server.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/remote_cache/capabilities_server",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:api_key_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:semver_go_proto",
        "//server/environment",
        "//server/real_environment",
        "//server/remote_cache/config",
        "//server/remote_cache/digest",
        "//server/util/bazel_request",
    ],
)
