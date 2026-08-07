load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "cpusampler",
    srcs = ["cpusampler.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/cpusampler",
    visibility = ["//visibility:public"],
    deps = [
        "//server/environment",
        "//server/interfaces",
        "//server/metrics",
        "//server/util/claims",
        "//server/util/flag",
        "//server/util/log",
        "//server/util/status",
        "@com_github_google_pprof//profile",
        "@org_golang_google_grpc//:grpc",
    ],
)

go_test(
    name = "cpusampler_test",
    size = "small",
    srcs = ["cpusampler_test.go"],
    embed = [":cpusampler"],
    deps = [
        "//server/interfaces",
        "//server/util/claims",
        "//server/util/testing/flags",
        "@com_github_google_pprof//profile",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_grpc//:grpc",
    ],
)
