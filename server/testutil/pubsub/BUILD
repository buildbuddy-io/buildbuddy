load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["pubsub.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/pubsub",
    visibility = ["//visibility:public"],
    deps = ["//server/interfaces:go_default_library"],
)
