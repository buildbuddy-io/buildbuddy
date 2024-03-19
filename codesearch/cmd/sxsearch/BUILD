load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "sxsearch_lib",
    srcs = ["sxsearch.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/codesearch/cmd/sxsearch",
    visibility = ["//visibility:private"],
    deps = [
        "//codesearch/index",
        "//codesearch/searcher",
        "//codesearch/types",
        "//server/util/log",
        "@com_github_cockroachdb_pebble//:pebble",
    ],
)

go_binary(
    name = "sxsearch",
    embed = [":sxsearch_lib"],
    visibility = ["//visibility:public"],
)
