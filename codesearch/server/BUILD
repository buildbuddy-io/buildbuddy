load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "server",
    srcs = ["server.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/codesearch/server",
    visibility = ["//visibility:public"],
    deps = [
        "//codesearch/index",
        "//codesearch/performance",
        "//codesearch/query",
        "//codesearch/schema",
        "//codesearch/searcher",
        "//codesearch/types",
        "//proto:index_go_proto",
        "//proto:search_go_proto",
        "//server/util/disk",
        "//server/util/git",
        "//server/util/log",
        "@com_github_cockroachdb_pebble//:pebble",
        "@org_golang_x_sync//errgroup",
    ],
)
