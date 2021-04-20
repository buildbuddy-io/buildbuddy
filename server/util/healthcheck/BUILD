load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "healthcheck",
    srcs = ["healthcheck.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/healthcheck",
    visibility = ["//visibility:public"],
    deps = [
        "//server/interfaces",
        "//server/util/log",
        "//server/util/status",
        "@org_golang_x_sync//errgroup",
    ],
)
