load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_binary(
    name = "vmstart",
    embed = [":vmstart_lib"],
    visibility = ["//visibility:public"],
)

go_library(
    name = "vmstart_lib",
    srcs = ["vmstart.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/vmstart",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/remote_execution/containers/firecracker",
        "//server/config",
        "//server/real_environment",
        "//server/util/healthcheck",
        "//server/util/log",
    ],
)
