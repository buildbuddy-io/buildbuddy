load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "priority_task_scheduler",
    srcs = ["priority_task_scheduler.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/scheduling/priority_task_scheduler",
    deps = [
        "//enterprise/server/auth",
        "//enterprise/server/remote_execution/executor",
        "//enterprise/server/remote_execution/operation",
        "//enterprise/server/scheduling/priority_queue",
        "//enterprise/server/tasksize",
        "//proto:remote_execution_go_proto",
        "//proto:scheduler_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/metrics",
        "//server/resources",
        "//server/util/alert",
        "//server/util/bazel_request",
        "//server/util/log",
        "//server/util/proto",
        "//server/util/status",
        "//server/util/tracing",
        "//server/util/usageutil",
        "@com_github_jonboulle_clockwork//:clockwork",
        "@com_github_prometheus_client_golang//prometheus",
        "@org_golang_google_grpc//metadata",
        "@org_golang_x_text//language",
        "@org_golang_x_text//message",
    ],
)

go_test(
    name = "priority_task_scheduler_test",
    size = "small",
    srcs = ["priority_task_scheduler_test.go"],
    embed = [":priority_task_scheduler"],
    deps = [
        "//proto:scheduler_go_proto",
        "@com_github_stretchr_testify//require",
    ],
)
