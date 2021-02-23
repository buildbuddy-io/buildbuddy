load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["distributed.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/distributed",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/util/cacheproxy:go_default_library",
        "//enterprise/server/util/heartbeat:go_default_library",
        "//proto:remote_execution_go_proto",
        "//server/environment:go_default_library",
        "//server/interfaces:go_default_library",
        "//server/util/consistent_hash:go_default_library",
        "//server/util/status:go_default_library",
        "@org_golang_x_sync//errgroup:go_default_library",
    ],
)
