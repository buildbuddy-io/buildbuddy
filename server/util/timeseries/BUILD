load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "timeseries",
    srcs = ["timeseries.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/timeseries",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:timeseries_go_proto",
        "@com_github_bmkessler_streamvbyte//:streamvbyte",
    ],
)

go_test(
    name = "timeseries_test",
    srcs = ["timeseries_test.go"],
    deps = [
        ":timeseries",
        "//proto:timeseries_go_proto",
        "//server/util/proto",
        "@com_github_stretchr_testify//require",
    ],
)
