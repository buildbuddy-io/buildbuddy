load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "chunk_benchmark_lib",
    srcs = ["main.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/chunk_benchmark",
    visibility = ["//visibility:private"],
    deps = [
        "@com_github_buildbarn_go_cdc//:go-cdc",
        "@com_github_cespare_xxhash_v2//:xxhash",
        "@com_github_jotfs_fastcdc_go//:fastcdc-go",
        "@com_github_jotfs_fastcdc_go//v2020:fastcdc-go",
    ],
)

go_binary(
    name = "chunk_benchmark",
    embed = [":chunk_benchmark_lib"],
    visibility = ["//visibility:public"],
)
