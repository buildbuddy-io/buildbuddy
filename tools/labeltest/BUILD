load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "labeltest_lib",
    srcs = ["main.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/labeltest",
    visibility = ["//visibility:private"],
    deps = [
        "@org_golang_x_exp//trace",
        "@org_golang_x_sync//errgroup",
    ],
)

go_binary(
    name = "labeltest",
    embed = [":labeltest_lib"],
    visibility = ["//visibility:public"],
)
