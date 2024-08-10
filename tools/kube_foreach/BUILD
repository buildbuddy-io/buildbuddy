load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "kube_foreach_lib",
    srcs = ["kube_foreach.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/kube_foreach",
    visibility = ["//visibility:private"],
    deps = ["@org_golang_x_sync//errgroup"],
)

go_binary(
    name = "kube_foreach",
    embed = [":kube_foreach_lib"],
    pure = "on",
    static = "on",
    visibility = ["//visibility:public"],
)
