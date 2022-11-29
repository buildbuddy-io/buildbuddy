load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "testcompression",
    testonly = 1,
    srcs = ["testcompression.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/testcompression",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/interfaces",
        "//server/util/compression",
        "//server/util/status",
    ],
)
