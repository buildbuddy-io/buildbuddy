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
        "//codesearch/dfa",
        "//codesearch/filters",
        "//codesearch/token",
        "//codesearch/types",
        "//server/util/log",
    ],
)

go_test(
    name = "query_test",
    srcs = [
        "regexp_query_test.go",
        "regexp_test.go",
    ],
    embed = [":query"],
    deps = [
        "//codesearch/token",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
