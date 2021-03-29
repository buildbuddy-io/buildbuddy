load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "action_cache_server",
    srcs = ["action_cache_server.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/remote_cache/action_cache_server",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:api_key_go_proto",
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/remote_cache/digest",
        "//server/remote_cache/hit_tracker",
        "//server/remote_cache/namespace",
        "//server/util/capabilities",
        "//server/util/prefix",
        "//server/util/status",
        "@com_github_golang_protobuf//proto:go_default_library",
        "@com_github_google_uuid//:uuid",
        "@org_golang_x_sync//errgroup",
    ],
)
