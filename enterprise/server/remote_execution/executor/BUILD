load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "executor",
    srcs = ["executor.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/executor",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/remote_execution/operation",
        "//enterprise/server/remote_execution/runner",
        "//proto:execution_stats_go_proto",
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/metrics",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/util/background",
        "//server/util/disk",
        "//server/util/log",
        "//server/util/status",
        "//server/util/tracing",
        "@com_github_golang_protobuf//ptypes:go_default_library_gen",
        "@com_github_google_uuid//:uuid",
        "@com_github_prometheus_client_golang//prometheus",
        "@io_bazel_rules_go//proto/wkt:duration_go_proto",
        "@io_bazel_rules_go//proto/wkt:timestamp_go_proto",
        "@org_golang_google_grpc//codes",
        "@org_golang_google_grpc//status",
    ],
)

go_test(
    name = "executor_test",
    size = "small",
    srcs = ["executor_test.go"],
    embed = [":executor"],
    tags = ["manual"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/testutil/testenv",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//assert",
        "@org_golang_google_grpc//codes",
        "@org_golang_google_grpc//status",
    ],
)
