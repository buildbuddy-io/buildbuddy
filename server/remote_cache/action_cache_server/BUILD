load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "action_cache_server",
    srcs = ["action_cache_server.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/remote_cache/action_cache_server",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:api_key_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/environment",
        "//server/hostid",
        "//server/interfaces",
        "//server/real_environment",
        "//server/remote_cache/digest",
        "//server/remote_cache/hit_tracker",
        "//server/util/capabilities",
        "//server/util/log",
        "//server/util/prefix",
        "//server/util/proto",
        "//server/util/status",
        "@org_golang_x_sync//errgroup",
    ],
)
