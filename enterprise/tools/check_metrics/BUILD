load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "check_metrics_lib",
    srcs = [
        "check_metrics.go",
        "config.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/tools/check_metrics",
    visibility = ["//visibility:private"],
    deps = [
        "//server/config",
        "//server/util/flagutil",
        "//server/util/log",
        "//server/util/status",
        "@com_github_prometheus_client_golang//api",
        "@com_github_prometheus_client_golang//api/prometheus/v1:prometheus",
        "@com_github_prometheus_common//model",
        "@org_golang_x_sync//errgroup",
    ],
)

go_binary(
    name = "check_metrics",
    embed = [":check_metrics_lib"],
    visibility = ["//enterprise:__subpackages__"],
)

package(default_visibility = ["//enterprise:__subpackages__"])
