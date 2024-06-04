load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "cli_lib",
    srcs = ["cli.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/codesearch/cmd/cli",
    visibility = ["//visibility:private"],
    deps = [
        "//codesearch/index",
        "//codesearch/query",
        "//codesearch/searcher",
        "//codesearch/types",
        "//server/util/log",
        "@com_github_cespare_xxhash_v2//:xxhash",
        "@com_github_cockroachdb_pebble//:pebble",
        "@com_github_gabriel_vasile_mimetype//:mimetype",
        "@com_github_go_enry_go_enry_v2//:go-enry",
    ],
)

go_binary(
    name = "cli",
    embed = [":cli_lib"],
    visibility = ["//visibility:public"],
)
