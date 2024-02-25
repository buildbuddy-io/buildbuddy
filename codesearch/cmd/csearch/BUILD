load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "csearch_lib",
    srcs = ["csearch.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/codesearch/cmd/csearch",
    visibility = ["//visibility:private"],
    deps = [
        "//codesearch/index",
        "//codesearch/query",
        "//codesearch/regexp",
        "//server/util/log",
        "@com_github_cockroachdb_pebble//:pebble",
    ],
)

go_binary(
    name = "csearch",
    embed = [":csearch_lib"],
    visibility = ["//visibility:public"],
)
