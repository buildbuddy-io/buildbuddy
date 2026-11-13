load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "tenantquery",
    srcs = ["tenantquery.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/clickhouse/tenantquery",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:tenant_query_go_proto",
        "//server/environment",
        "//server/real_environment",
        "//server/util/clickhouse/schema",
        "//server/util/flag",
        "//server/util/status",
        "@com_github_clickhouse_ch_go//proto",
        "@io_gorm_gorm//:gorm",
    ],
)

go_test(
    name = "tenantquery_test",
    srcs = ["tenantquery_test.go"],
    deps = [
        ":tenantquery",
        "//proto:tenant_query_go_proto",
        "//server/real_environment",
        "//server/testutil/testauth",
        "//server/testutil/testclickhouse",
        "//server/testutil/testenv",
        "//server/util/status",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
