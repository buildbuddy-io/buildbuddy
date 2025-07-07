load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "server_lib",
    srcs = ["server.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/experimental/mcp/cmd/server",
    visibility = ["//visibility:private"],
    deps = [
        "//enterprise/server/backends/configsecrets",
        "//experimental/mcp/resources",
        "//experimental/mcp/templates",
        "//experimental/mcp/tools",
        "//proto:buildbuddy_service_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto/api/v1:api_v1_go_proto",
        "//server/config",
        "//server/real_environment",
        "//server/util/authutil",
        "//server/util/flag",
        "//server/util/grpc_client",
        "//server/util/healthcheck",
        "//server/util/log",
        "//server/util/monitoring",
        "@com_github_modelcontextprotocol_go_sdk//mcp",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//metadata",
    ],
)

go_binary(
    name = "server",
    embed = [":server_lib"],
    visibility = ["//visibility:public"],
)

package(default_visibility = ["//experimental:__subpackages__"])
