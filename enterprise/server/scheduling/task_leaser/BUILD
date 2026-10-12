load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

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

go_test(
    name = "task_leaser_test",
    srcs = ["task_leaser_test.go"],
    deps = [
        ":task_leaser",
        "//proto:remote_execution_go_proto",
        "//proto:scheduler_go_proto",
        "//server/real_environment",
        "//server/util/proto",
        "//server/util/status",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_grpc//:grpc",
    ],
)
