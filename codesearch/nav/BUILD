load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "nav",
    srcs = [
        "handlers.go",
        "lookup.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/codesearch/nav",
    visibility = ["//visibility:public"],
    deps = [
        "//codesearch/annotations",
        "//codesearch/index",
        "//codesearch/schema",
        "//proto:search_go_proto",
        "//server/util/log",
        "@io_kythe//kythe/proto:common_go_proto",
        "@io_kythe//kythe/proto:xref_go_proto",
    ],
)
