load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "namespace",
    srcs = ["namespace.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/remote_cache/namespace",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:resource_go_proto",
        "//server/interfaces",
    ],
)
