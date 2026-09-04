load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "channelz_metrics",
    srcs = ["channelz_metrics.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/channelz_metrics",
    visibility = ["//visibility:public"],
    deps = [
        "//server/environment",
        "//server/metrics",
        "//server/util/flag",
        "//server/util/grpc_client",
        "//server/util/grpc_server",
        "//server/util/log",
        "//server/util/status",
        "@org_golang_google_grpc//channelz/grpc_channelz_v1",
    ],
)

go_test(
    name = "channelz_metrics_test",
    srcs = ["channelz_metrics_test.go"],
    embed = [":channelz_metrics"],
    deps = [
        "//server/metrics",
        "//server/util/status",
        "@com_github_prometheus_client_golang//prometheus/testutil",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_grpc//channelz/grpc_channelz_v1",
        "@org_golang_google_protobuf//types/known/wrapperspb",
    ],
)
