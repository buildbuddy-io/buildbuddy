load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "hit_tracker",
    srcs = ["hit_tracker.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/remote_cache/hit_tracker",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:cache_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/metrics",
        "//server/remote_cache/digest",
        "//server/tables",
        "//server/util/bazel_request",
        "//server/util/log",
        "@com_github_prometheus_client_golang//prometheus",
        "@go_googleapis//google/rpc:status_go_proto",
        "@org_golang_google_grpc//codes",
        "@org_golang_google_protobuf//proto",
        "@org_golang_google_protobuf//types/known/durationpb",
        "@org_golang_google_protobuf//types/known/timestamppb",
    ],
)

go_test(
    name = "hit_tracker_test",
    size = "small",
    srcs = ["hit_tracker_test.go"],
    deps = [
        ":hit_tracker",
        "//proto:cache_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/backends/memory_metrics_collector",
        "//server/interfaces",
        "//server/tables",
        "//server/testutil/testenv",
        "//server/util/bazel_request",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_protobuf//proto",
    ],
)
