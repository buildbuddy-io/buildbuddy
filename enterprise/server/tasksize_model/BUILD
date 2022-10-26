load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "tasksize_model",
    srcs = ["tasksize_model.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/tasksize_model",
    deps = [
        "//enterprise/server/remote_execution/platform",
        "//proto:remote_execution_go_proto",
        "//proto:scheduler_go_proto",
        "//server/environment",
        "//server/metrics",
        "//server/util/grpc_client",
        "//server/util/log",
        "//server/util/status",
        "@com_github_buildbuddy_io_tensorflow_proto//tensorflow/core/framework",
        "@com_github_buildbuddy_io_tensorflow_proto//tensorflow_serving/apis",
        "@com_github_prometheus_client_golang//prometheus",
        "@org_golang_x_sync//errgroup",
    ],
)

go_test(
    name = "tasksize_model_test",
    srcs = ["tasksize_model_test.go"],
    embed = [":tasksize_model"],
    tags = [
        "manual",
        "no-sandbox",
    ],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/testutil/testenv",
        "//server/util/log",
        "@com_github_stretchr_testify//require",
    ],
)
