load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "mockinvocationdb",
    srcs = ["mockinvocationdb.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/mockinvocationdb",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:acl_go_proto",
        "//proto:telemetry_go_proto",
        "//server/interfaces",
        "//server/tables",
        "//server/util/status",
    ],
)
