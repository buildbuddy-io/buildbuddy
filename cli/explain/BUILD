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
        "@org_golang_x_sync//errgroup",
    ],
)
