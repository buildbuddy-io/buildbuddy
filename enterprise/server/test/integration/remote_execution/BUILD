load("@io_bazel_rules_go//go:def.bzl", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_test(
    name = "remote_execution_test",
    size = "small",
    # TODO(https://github.com/buildbuddy-io/buildbuddy-internal/issues/2379):
    # make these tests run faster
    timeout = "long",
    srcs = ["remote_execution_test.go"],
    args = ["--test.v"],
    data = [
        "//enterprise/server/test/integration/remote_execution/command:testcommand",
    ],
    shard_count = 11,
    deps = [
        "//enterprise/server/build_event_publisher",
        "//enterprise/server/remote_execution/commandutil",
        "//enterprise/server/scheduling/scheduler_server",
        "//enterprise/server/tasksize",
        "//enterprise/server/test/integration/remote_execution/rbetest",
        "//enterprise/server/testutil/buildbuddy_enterprise",
        "//enterprise/server/testutil/testexecutor",
        "//enterprise/server/testutil/testredis",
        "//proto:build_event_stream_go_proto",
        "//proto:remote_execution_go_proto",
        "//server/build_event_protocol/build_event_handler",
        "//server/interfaces",
        "//server/metrics",
        "//server/remote_cache/cachetools",
        "//server/testutil/testbazel",
        "//server/testutil/testenv",
        "//server/testutil/testfs",
        "//server/testutil/testmetrics",
        "//server/util/bazel",
        "//server/util/grpc_client",
        "//server/util/log",
        "//server/util/proto",
        "//server/util/status",
        "//server/util/testing/flags",
        "@com_github_google_uuid//:uuid",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_grpc//:go_default_library",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_protobuf//types/known/timestamppb",
        "@org_golang_x_sync//errgroup",
    ],
)
