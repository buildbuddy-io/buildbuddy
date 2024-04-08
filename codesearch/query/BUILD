load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "query",
    srcs = ["regexp.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/codesearch/query",
    visibility = ["//visibility:public"],
)

go_test(
    name = "query_test",
    srcs = ["regexp_test.go"],
    embed = [":query"],
)
