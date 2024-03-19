load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "cindex_lib",
    srcs = ["cindex.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/codesearch/cmd/cindex",
    visibility = ["//visibility:private"],
    deps = [
        "//codesearch/index",
        "//codesearch/types",
        "//proto:remote_execution_go_proto",
        "//server/util/log",
        "@com_github_cockroachdb_pebble//:pebble",
        "@com_github_gabriel_vasile_mimetype//:mimetype",
    ],
)

go_binary(
    name = "cindex",
    embed = [":cindex_lib"],
    visibility = ["//visibility:public"],
)
