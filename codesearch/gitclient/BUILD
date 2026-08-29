load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "gitclient",
    srcs = ["gitclient.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/codesearch/gitclient",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:index_go_proto",
        "//server/util/log",
        "//server/util/status",
        "@com_github_gabriel_vasile_mimetype//:mimetype",
    ],
)

go_test(
    name = "gitclient_test",
    srcs = ["gitclient_test.go"],
    embed = [":gitclient"],
    deps = [
        "//proto:index_go_proto",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
