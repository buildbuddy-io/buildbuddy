load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "scheduler_server",
    srcs = ["scheduler_server.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/scheduling/scheduler_server",
    deps = [
        "//enterprise/server/remote_execution/config",
        "//enterprise/server/remote_execution/platform",
        "//enterprise/server/scheduling/scheduler_server/config",
        "//enterprise/server/tasksize",
        "//proto:api_key_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:scheduler_go_proto",
        "//proto:trace_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/resources",
        "//server/util/background",
        "//server/util/grpc_client",
        "//server/util/log",
        "//server/util/perms",
        "//server/util/status",
        "//server/util/tracing",
        "@com_github_go_redis_redis_v8//:redis",
        "@com_github_prometheus_client_golang//prometheus",
        "@org_golang_google_grpc//:go_default_library",
        "@org_golang_google_grpc//peer",
        "@org_golang_google_protobuf//encoding/prototext",
        "@org_golang_google_protobuf//proto",
        "@org_golang_google_protobuf//types/known/timestamppb",
    ],
)

go_test(
    name = "scheduler_server_test",
    size = "small",
    srcs = ["scheduler_server_test.go"],
    embed = [":scheduler_server"],
    deps = [
        "//enterprise/server/testutil/enterprise_testenv",
        "//server/interfaces",
        "//server/testutil/testauth",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
    ],
)
