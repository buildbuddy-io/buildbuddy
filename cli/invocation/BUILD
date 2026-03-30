load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "invocation",
    srcs = ["invocation.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/invocation",
    deps = [
        "//cli/arg",
        "//cli/log",
        "//cli/login",
        "//proto:buildbuddy_service_go_proto",
        "//proto:execution_stats_go_proto",
        "//proto:invocation_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:target_go_proto",
        "//proto/api/v1:common_go_proto",
        "//server/util/grpc_client",
        "//server/util/rexec",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//metadata",
    ],
)
