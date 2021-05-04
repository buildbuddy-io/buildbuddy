load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "chunkstore",
    srcs = ["chunkstore.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/backends/chunkstore",
    visibility = ["//visibility:public"],
    deps = [
        "//server/interfaces",
    ],
)

go_test(
    name = "chunkstore_test",
    srcs = ["chunkstore_test.go"],
    deps = [
        ":chunkstore",
        "//server/interfaces",
    ],
)
