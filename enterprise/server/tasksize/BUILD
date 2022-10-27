load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//visibility:public"])

go_library(
    name = "tasksize",
    srcs = ["tasksize.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/tasksize",
    deps = [
        "//enterprise/server/remote_execution/platform",
        "//enterprise/server/tasksize_model",
        "//proto:remote_execution_go_proto",
        "//proto:scheduler_go_proto",
        "//server/environment",
        "//server/metrics",
        "//server/util/log",
        "//server/util/perms",
        "//server/util/status",
        "@com_github_go_redis_redis_v8//:redis",
        "@com_github_prometheus_client_golang//prometheus",
        "@org_golang_google_protobuf//proto",
    ],
)

go_test(
    name = "tasksize_test",
    size = "small",
    srcs = ["tasksize_test.go"],
    deps = [
        ":tasksize",
        "//enterprise/server/remote_execution/platform",
        "//enterprise/server/testutil/testredis",
        "//proto:remote_execution_go_proto",
        "//server/testutil/testauth",
        "//server/testutil/testenv",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_protobuf//types/known/timestamppb",
    ],
)
