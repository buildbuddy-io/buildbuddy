load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "scheduler_client",
    srcs = ["scheduler_client.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/scheduling/scheduler_client",
    deps = [
        "//enterprise/server/auth",
        "//enterprise/server/scheduling/priority_task_scheduler",
        "//enterprise/server/scheduling/task_leaser",
        "//proto:scheduler_go_proto",
        "//server/environment",
        "//server/resources",
        "//server/util/log",
        "//server/util/status",
        "//server/version",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_protobuf//encoding/prototext",
    ],
)
