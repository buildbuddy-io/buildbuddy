load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "task_leaser",
    srcs = ["task_leaser.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/scheduling/task_leaser",
    deps = [
        "//proto:remote_execution_go_proto",
        "//proto:scheduler_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/util/authutil",
        "//server/util/flag",
        "//server/util/log",
        "//server/util/proto",
        "//server/util/retry",
        "//server/util/status",
        "@org_golang_google_grpc//status",
    ],
)
