load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "execution_service",
    srcs = ["execution_service.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/execution_service",
    deps = [
        "//proto:execution_stats_go_proto",
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/remote_cache/digest",
        "//server/tables",
        "//server/util/perms",
        "//server/util/query_builder",
        "//server/util/status",
        "@go_googleapis//google/rpc:status_go_proto",
        "@org_golang_google_grpc//codes",
        "@org_golang_google_protobuf//proto",
        "@org_golang_google_protobuf//types/known/timestamppb",
    ],
)
