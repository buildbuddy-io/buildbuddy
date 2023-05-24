load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "testpostgres",
    testonly = 1,
    srcs = ["testpostgres.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/testpostgres",
    visibility = ["//visibility:public"],
    deps = [
        "//server/testutil/testport",
        "//server/util/log",
        "@com_github_jackc_pgx_v5//:pgx",
        "@com_github_stretchr_testify//require",
    ],
)
