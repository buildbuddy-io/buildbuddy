load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "searcher",
    srcs = ["searcher.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/codesearch/searcher",
    visibility = ["//visibility:public"],
    deps = [
        "//codesearch/types",
        "//server/util/log",
        "@org_golang_x_sync//errgroup",
    ],
)
