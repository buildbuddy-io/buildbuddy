load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "execution",
    srcs = ["execution.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/execution",
    deps = [
        "//cli/arg",
        "//cli/log",
        "//cli/login",
        "//cli/markdown",
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
