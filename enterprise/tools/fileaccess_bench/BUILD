load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "fileaccess_bench_lib",
    srcs = ["fileaccess_bench.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/tools/fileaccess_bench",
    target_compatible_with = ["@platforms//os:linux"],
    visibility = ["//visibility:private"],
    deps = [
        "//enterprise/server/remote_execution/containers/ociruntime/fileaccess",
        "@org_golang_x_sys//unix",
    ],
)

go_binary(
    name = "fileaccess_bench",
    embed = [":fileaccess_bench_lib"],
    target_compatible_with = ["@platforms//os:linux"],
    visibility = ["//visibility:public"],
)
