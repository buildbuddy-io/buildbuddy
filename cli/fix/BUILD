load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "fix",
    srcs = ["fix.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/fix",
    visibility = ["//visibility:public"],
    # keep
    deps = [
        "//cli/arg",
        "//cli/log",
        "@bazel_gazelle//cmd/gazelle:gazelle_lib",
    ],
)
