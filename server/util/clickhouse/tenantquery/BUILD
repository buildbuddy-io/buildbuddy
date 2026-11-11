load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "tenantquery",
    srcs = ["tenantquery.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/clickhouse/tenantquery",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/clickhouse/schema",
        "//server/util/flag",
        "@io_gorm_gorm//:gorm",
    ],
)

go_test(
    name = "tenantquery_test",
    srcs = ["tenantquery_test.go"],
    deps = [
        ":tenantquery",
        "//server/real_environment",
        "//server/testutil/testclickhouse",
        "//server/testutil/testenv",
        "//server/util/testing/flags",
        "@com_github_clickhouse_ch_go//proto",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
