load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "explain",
    srcs = ["explain.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/explain",
    deps = [
        "//cli/arg",
        "//cli/explain/compactgraph",
        "//cli/log",
        "//cli/storage",
        "//proto:spawn_diff_go_proto",
        "//proto:spawn_go_proto",
        "@com_github_google_go_cmp//cmp",
        "@org_golang_x_exp//maps",
        "@org_golang_x_sync//errgroup",
    ],
)
