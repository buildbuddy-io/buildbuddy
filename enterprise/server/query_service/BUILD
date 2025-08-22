load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//visibility:public"])

go_library(
    name = "query_service",
    srcs = ["query_service.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/query_service",
    deps = [
        "//enterprise/server/hostedrunner",
        "//proto:query_go_proto",
        "//proto:bazel_query_go_proto",
        "//proto:build_event_stream_go_proto",
        "//proto:git_go_proto",
        "//proto:invocation_go_proto",
        "//proto:invocation_status_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:runner_go_proto",
        "//server/build_event_protocol/build_event_handler",
        "//server/environment",
        "//server/interfaces",
        "//server/real_environment",
        "//server/tables",
        "//server/util/db",
        "//server/util/grpc_client",
        "//server/util/log",
        "//server/util/proto",
        "//server/util/query_builder",
        "//server/util/status",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
    ],
)