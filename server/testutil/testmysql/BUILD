load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "testmysql",
    testonly = 1,
    srcs = ["testmysql.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/testmysql",
    visibility = ["//visibility:public"],
    deps = [
        "//server/testutil/testport",
        "//server/util/log",
        "@com_github_go_sql_driver_mysql//:mysql",
        "@com_github_stretchr_testify//require",
    ],
)
