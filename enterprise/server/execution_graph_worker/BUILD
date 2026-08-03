load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "execution_graph_worker",
    srcs = ["execution_graph_worker.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/execution_graph_worker",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:build_event_stream_go_proto",
        "//proto:capability_go_proto",
        "//proto:execution_graph_analysis_go_proto",
        "//proto:invocation_go_proto",
        "//proto:invocation_status_go_proto",
        "//server/build_event_protocol/build_event_handler",
        "//server/environment",
        "//server/execution_graph",
        "//server/interfaces",
        "//server/metrics",
        "//server/util/authutil",
        "//server/util/claims",
        "//server/util/db",
        "//server/util/log",
        "//server/util/proto",
        "//server/util/redact",
        "//server/util/status",
        "@com_github_golang_jwt_jwt_v4//:jwt",
        "@org_golang_google_grpc//metadata",
    ],
)

go_test(
    name = "execution_graph_worker_test",
    size = "small",
    srcs = ["execution_graph_worker_test.go"],
    embed = [":execution_graph_worker"],
    deps = [
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
