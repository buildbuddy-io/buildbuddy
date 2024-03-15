load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "testserver",
    srcs = ["testserver.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/testserver",
    visibility = ["//visibility:public"],
    deps = ["@io_bazel_rules_go//go/runfiles:go_default_library"],
)
