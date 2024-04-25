load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "podman_stress_lib",
    srcs = ["podman_stress.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/podman_stress",
    visibility = ["//visibility:private"],
    deps = [
        "//server/util/log",
        "//server/util/qps",
        "//server/util/uuid",
        "@org_golang_x_sync//errgroup",
    ],
)

go_binary(
    name = "podman_stress",
    embed = [":podman_stress_lib"],
    pure = "on",
    static = "on",
    visibility = ["//visibility:public"],
)
