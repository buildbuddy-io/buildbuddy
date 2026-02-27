load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "executions",
    srcs = ["executions.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/executions",
    deps = [
        "//cli/arg",
        "//cli/log",
        "//cli/login",
        "//cli/terminal",
        "//proto:execution_stats_go_proto",
        "//proto:remote_execution_go_proto",
        "//server/util/grpc_client",
        "//server/util/rexec",
        "@org_golang_google_grpc//codes",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_protobuf//encoding/protojson",
        "@org_golang_google_protobuf//types/known/timestamppb",
    ],
)
