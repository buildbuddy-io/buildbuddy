load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "why",
    srcs = ["why.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/why",
    visibility = ["//visibility:public"],
    deps = [
        "//cli/log",
        "//cli/why/compactgraph",
        "@org_golang_x_sync//errgroup",
    ],
)

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "explain",
    srcs = ["explain.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/explain",
    deps = [
        "//cli/explain/compactgraph",
        "//cli/log",
        "//proto:spawn_diff_go_proto",
        "@com_github_google_go_cmp//cmp",
        "@org_golang_x_sync//errgroup",
    ],
)
