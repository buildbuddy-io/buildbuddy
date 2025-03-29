load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "hit_tracker_service",
    srcs = ["hit_tracker_service.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/hit_tracker_service",
    deps = [
        "//proto:hit_tracker_go_proto",
        "//server/interfaces",
        "//server/real_environment",
        "//server/util/log",
        "//server/util/status",
    ],
)

go_test(
    name = "hit_tracker_service_test",
    size = "small",
    srcs = ["hit_tracker_service_test.go"],
    deps = [
        ":hit_tracker_service",
        "//proto:cache_go_proto",
        "//proto:hit_tracker_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/backends/memory_metrics_collector",
        "//server/interfaces",
        "//server/remote_cache/digest",
        "//server/remote_cache/hit_tracker",
        "//server/tables",
        "//server/testutil/testenv",
        "//server/util/bazel_request",
        "//server/util/proto",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_protobuf//types/known/durationpb",
    ],
)
