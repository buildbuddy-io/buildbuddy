load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "cli_lib",
    srcs = ["cli.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/codesearch/cmd/cli",
    visibility = ["//visibility:private"],
    deps = [
        "//codesearch/index",
        "//codesearch/performance",
        "//codesearch/query",
        "//codesearch/schema",
        "//codesearch/searcher",
        "//codesearch/types",
        "//server/util/git",
        "//server/util/log",
        "@com_github_cockroachdb_pebble//:pebble",
    ],
)

go_binary(
    name = "cli",
    embed = [":cli_lib"],
    visibility = ["//visibility:public"],
)
