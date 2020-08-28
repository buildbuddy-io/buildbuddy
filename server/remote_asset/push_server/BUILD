load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["push_server.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/remote_asset/push_server",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_asset_go_proto",
        "//server/environment:go_default_library",
        "//server/util/status:go_default_library",
    ],
)
