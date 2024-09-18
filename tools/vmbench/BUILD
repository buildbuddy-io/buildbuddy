load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "vmbench_lib",
    srcs = ["vmbench.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/vmbench",
    visibility = ["//visibility:private"],
    deps = [
        "//server/util/log",
        "@org_golang_x_sync//errgroup",
    ],
)

go_binary(
    name = "vmbench",
    embed = [":vmbench_lib"],
    visibility = ["//visibility:public"],
)
