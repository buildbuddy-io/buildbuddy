load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "tasksize",
    srcs = ["tasksize.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/tasksize",
    visibility = [
        "//enterprise:__subpackages__",
        "@buildbuddy_internal//enterprise:__subpackages__",
    ],
    deps = [
        "//enterprise/server/remote_execution/platform",
        "//proto:execution_stats_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:scheduler_go_proto",
        "//server/environment",
        "//server/util/log",
        "//server/util/status",
    ],
)

go_test(
    name = "tasksize_test",
    srcs = ["tasksize_test.go"],
    deps = [
        ":tasksize",
        "//enterprise/server/remote_execution/platform",
        "//proto:remote_execution_go_proto",
        "@com_github_stretchr_testify//assert",
    ],
)
