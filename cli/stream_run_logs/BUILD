load("@io_bazel_rules_go//go:def.bzl", "go_library")

# gazelle:default_visibility //cli:__subpackages__,//enterprise:__subpackages__
package(default_visibility = [
    "//cli:__subpackages__",
    "//enterprise:__subpackages__",
])

go_library(
    name = "stream_run_logs",
    srcs = ["stream_run_logs.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/stream_run_logs",
    deps = [
        "//cli/log",
        "//cli/parser",
        "//cli/parser/parsed",
        "//cli/stream_run_logs/option_definitions",
        "//cli/terminal",
        "//proto:buildbuddy_service_go_proto",
        "//proto:eventlog_go_proto",
        "//proto:invocation_status_go_proto",
        "//server/util/grpc_client",
        "//server/util/status",
        "@com_github_creack_pty//:pty",
        "@com_github_google_uuid//:uuid",
        "@org_golang_google_grpc//metadata",
    ],
)
