load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["healthcheck.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/healthcheck",
    visibility = ["//visibility:public"],
    deps = ["@org_golang_x_sync//errgroup:go_default_library"],
)
