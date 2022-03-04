load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "testmetrics",
    testonly = 1,
    srcs = ["testmetrics.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/testmetrics",
    visibility = ["//visibility:public"],
    deps = [
        "@com_github_prometheus_client_golang//prometheus",
        "@com_github_prometheus_client_model//go",
        "@com_github_stretchr_testify//require",
    ],
)
