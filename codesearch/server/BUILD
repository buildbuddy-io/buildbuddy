load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "server",
    srcs = ["server.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/codesearch/server",
    visibility = ["//visibility:public"],
    deps = [
        "//codesearch/index",
        "//codesearch/query",
        "//codesearch/searcher",
        "//codesearch/types",
        "//proto:index_go_proto",
        "//proto:search_go_proto",
        "//server/util/disk",
        "//server/util/log",
        "@com_github_cespare_xxhash_v2//:xxhash",
        "@com_github_cockroachdb_pebble//:pebble",
        "@com_github_gabriel_vasile_mimetype//:mimetype",
        "@com_github_go_enry_go_enry_v2//:go-enry",
    ],
)
