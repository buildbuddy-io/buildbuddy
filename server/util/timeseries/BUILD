load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "timeseries",
    srcs = ["timeseries.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/timeseries",
    visibility = ["//visibility:public"],
)

go_test(
    name = "timeseries_test",
    srcs = ["timeseries_test.go"],
    deps = [
        ":timeseries",
        "@com_github_stretchr_testify//require",
    ],
)
