load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "testclickhouse",
    testonly = 1,
    srcs = ["testclickhouse.go"],
    data = ["//tools/clickhouse_bin"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/testclickhouse",
    visibility = ["//visibility:public"],
    x_defs = {
        "clickhouseRlocationpath": "$(rlocationpath //tools/clickhouse_bin)",
    },
    deps = [
        "//server/real_environment",
        "//server/testutil/testport",
        "//server/util/clickhouse",
        "//server/util/log",
        "//server/util/retry",
        "//server/util/testing/flags",
        "@com_github_clickhouse_clickhouse_go_v2//:clickhouse-go",
        "@com_github_stretchr_testify//require",
        "@io_bazel_rules_go//go/runfiles",
    ],
)
