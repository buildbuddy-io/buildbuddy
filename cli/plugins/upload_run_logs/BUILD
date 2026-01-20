load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

# gazelle:default_visibility //cli:__subpackages__,//enterprise:__subpackages__
package(default_visibility = [
    "//cli:__subpackages__",
    "//enterprise:__subpackages__",
])

go_binary(
    name = "upload_run_logs",
    embed = [":upload_run_logs_lib"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/plugins/upload_run_logs",
)

go_library(
    name = "upload_run_logs_lib",
    srcs = ["upload_run_logs.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/plugins/upload_run_logs",
    deps = [
        "//cli/log",
        "//cli/login",
        "//proto:buildbuddy_service_go_proto",
        "//proto:eventlog_go_proto",
        "//proto:invocation_status_go_proto",
        "//server/util/grpc_client",
        "//server/util/status",
        "@com_github_creack_pty//:pty",
        "@org_golang_google_grpc//metadata",
    ],
)
