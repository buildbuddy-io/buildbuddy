load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "priority_task_scheduler",
    srcs = ["priority_task_scheduler.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/scheduling/priority_task_scheduler",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/remote_execution/executor",
        "//enterprise/server/scheduling/priority_queue",
        "//enterprise/server/scheduling/task_leaser",
        "//enterprise/server/tasksize",
        "//proto:remote_execution_go_proto",
        "//proto:scheduler_go_proto",
        "//server/environment",
        "//server/metrics",
        "//server/resources",
        "//server/util/alert",
        "//server/util/bazel_request",
        "//server/util/log",
        "//server/util/status",
        "//server/util/tracing",
        "@com_github_prometheus_client_golang//prometheus",
        "@org_golang_google_grpc//codes",
        "@org_golang_google_grpc//status",
        "@org_golang_google_protobuf//proto",
    ],
)

go_test(
    name = "priority_task_scheduler_test",
    srcs = ["priority_task_scheduler_test.go"],
    embed = [":priority_task_scheduler"],
    deps = [
        "//proto:scheduler_go_proto",
        "@com_github_stretchr_testify//require",
    ],
)
