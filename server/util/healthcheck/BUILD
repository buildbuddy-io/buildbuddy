load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "healthcheck",
    srcs = ["healthcheck.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/healthcheck",
    visibility = ["//visibility:public"],
    deps = [
        "//server/interfaces",
        "//server/metrics",
        "//server/util/log",
        "//server/util/status",
        "//server/util/statusz",
        "//server/util/watchdog",
        "@com_github_mattn_go_isatty//:go-isatty",
        "@com_github_prometheus_client_golang//prometheus",
        "@org_golang_google_grpc//health/grpc_health_v1",
        "@org_golang_x_sync//errgroup",
    ],
)
