load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "fsbench_lib",
    srcs = ["fsbench.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/tools/fsbench",
    visibility = ["//visibility:private"],
    deps = [
        "//server/util/log",
        "@com_github_cespare_xxhash_v2//:xxhash",
        "@org_golang_x_sync//errgroup",
        "@org_golang_x_sys//unix",
    ],
)

go_binary(
    name = "fsbench",
    embed = [":fsbench_lib"],
    visibility = ["//visibility:public"],
)

package(default_visibility = ["//enterprise:__subpackages__"])
