load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "tasksize_model",
    srcs = ["tasksize_model.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/tasksize_model",
    visibility = [
        "//enterprise:__subpackages__",
        "@buildbuddy_internal//enterprise:__subpackages__",
    ],
    deps = [
        "//enterprise/server/remote_execution/platform",
        "//proto:remote_execution_go_proto",
        "//proto:scheduler_go_proto",
        "//server/environment",
        "//server/util/grpc_client",
        "//server/util/log",
        "//server/util/status",
        "@com_github_buildbuddy_io_tensorflow_proto//tensorflow/core/framework",
        "@com_github_buildbuddy_io_tensorflow_proto//tensorflow_serving/apis",
        "@org_golang_x_sync//errgroup",
    ],
)
