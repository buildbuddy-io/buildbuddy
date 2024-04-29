load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "query",
    srcs = [
        "regexp.go",
        "regexp_query.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/codesearch/query",
    visibility = ["//visibility:public"],
    deps = [
        "//codesearch/types",
        "//server/util/log",
        "//server/util/status",
        "@com_github_go_enry_go_enry_v2//:go-enry",
    ],
)

go_test(
    name = "query_test",
    srcs = ["regexp_test.go"],
    embed = [":query"],
)
