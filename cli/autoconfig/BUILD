load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "autoconfig",
    srcs = ["autoconfig.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/autoconfig",
    visibility = ["//visibility:public"],
    deps = [
        "//cli/commandline",
        "//server/util/bazel",
        "//server/util/networking",
    ],
)
