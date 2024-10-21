load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "get_executions_lib",
    srcs = ["get_executions.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/tools/get_executions",
    visibility = ["//visibility:private"],
    deps = [
        "//proto:buildbuddy_service_go_proto",
        "//proto:execution_stats_go_proto",
        "//server/util/flag",
        "//server/util/grpc_client",
        "//server/util/log",
        "@com_github_mattn_go_isatty//:go-isatty",
        "@org_golang_google_protobuf//encoding/protojson",
    ],
)

go_binary(
    name = "get_executions",
    embed = [":get_executions_lib"],
    visibility = ["//visibility:public"],
)

package(default_visibility = ["//enterprise:__subpackages__"])
