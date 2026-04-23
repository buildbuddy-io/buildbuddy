load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "gitclient",
    srcs = ["gitclient.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/codesearch/github/gitclient",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:index_go_proto",
        "//server/util/log",
        "//server/util/status",
        "@com_github_gabriel_vasile_mimetype//:mimetype",
    ],
)
