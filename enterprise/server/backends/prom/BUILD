load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "prom",
    srcs = ["prom.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/prom",
    deps = [
        "//proto:metrics_go_proto",
        "//server/environment",
        "//server/metrics",
        "//server/real_environment",
        "//server/util/alert",
        "//server/util/log",
        "//server/util/proto",
        "//server/util/status",
        "@com_github_go_redis_redis_v8//:redis",
        "@com_github_prometheus_client_golang//api",
        "@com_github_prometheus_client_golang//api/prometheus/v1:prometheus",
        "@com_github_prometheus_client_golang//prometheus",
        "@com_github_prometheus_client_model//io/prometheus/client:go",
        "@com_github_prometheus_common//model",
        "@org_golang_x_sync//errgroup",
    ],
)

go_test(
    name = "prom_test",
    size = "small",
    srcs = ["prom_test.go"],
    embed = [":prom"],
    deps = [
        "//server/metrics",
        "@com_github_prometheus_client_golang//prometheus/testutil",
        "@com_github_prometheus_client_model//io/prometheus/client:go",
        "@com_github_prometheus_common//model",
        "@com_github_stretchr_testify//require",
    ],
)
