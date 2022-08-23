load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

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
        "//server/util/status",
    ],
)

go_test(
    name = "tasksize_model_test",
    srcs = ["tasksize_model_test.go"],
    deps = [
        ":tasksize_model",
        "//proto:remote_execution_go_proto",
        "@com_github_stretchr_testify//require",
    ],
)
