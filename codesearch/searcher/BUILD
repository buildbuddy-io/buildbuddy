load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "searcher",
    srcs = ["searcher.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/codesearch/searcher",
    visibility = ["//visibility:public"],
    deps = [
        "//codesearch/query",
        "//codesearch/result",
        "//codesearch/types",
        "//server/util/log",
        "//server/util/status",
        "@com_github_go_enry_go_enry_v2//:go-enry",
        "@org_golang_x_sync//errgroup",
    ],
)
