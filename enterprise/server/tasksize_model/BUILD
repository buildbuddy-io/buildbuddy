load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "tasksize_model",
    srcs = [
        "tasksize_model.go",
        "tasksize_model_darwin.go",
        "tasksize_model_linux.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/tasksize_model",
    visibility = [
        "//enterprise:__subpackages__",
        "@buildbuddy_internal//enterprise:__subpackages__",
    ],
    deps = [
        "//enterprise/server/remote_execution/platform",
        "//proto:remote_execution_go_proto",
        "//proto:scheduler_go_proto",
    ] + select({
        "@io_bazel_rules_go//go/platform:linux": [
            "//server/util/log",
            "//server/util/status",
            "@com_github_tensorflow_tensorflow//tensorflow/go",
        ],
        "//conditions:default": [],
    }),
)
