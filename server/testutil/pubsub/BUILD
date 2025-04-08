load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "pubsub",
    srcs = ["pubsub.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/v2/server/testutil/pubsub",
    visibility = ["//visibility:public"],
    deps = [
        "//server/interfaces",
        "//server/util/log",
    ],
)
