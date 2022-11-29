load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "testhealthcheck",
    srcs = ["testhealthcheck.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/testhealthcheck",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:health_go_proto",
        "//server/interfaces",
        "//server/util/status",
    ],
)
