load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "testclickhouse",
    srcs = ["testclickhouse.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/testclickhouse",
    visibility = ["//visibility:public"],
    deps = [
        "//server/testutil/dockerutil",
        "//server/testutil/testport",
        "//server/util/log",
        "@com_github_clickhouse_clickhouse_go_v2//:clickhouse-go",
        "@com_github_stretchr_testify//require",
    ],
)
