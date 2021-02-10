load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    testonly = 1,
    srcs = ["healthcheck.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/healthcheck",
    visibility = ["//visibility:public"],
    deps = ["//server/interfaces:go_default_library"],
)
