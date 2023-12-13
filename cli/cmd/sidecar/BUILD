load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "sidecar",
    srcs = ["sidecar.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/cmd/sidecar",
    deps = [
        "//cli/arg",
        "//cli/devnull",
        "//proto:publish_build_event_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:sidecar_go_proto",
        "//server/backends/disk_cache",
        "//server/build_event_protocol/build_event_proxy",
        "//server/build_event_protocol/build_event_server",
        "//server/cache_proxy",
        "//server/nullauth",
        "//server/real_environment",
        "//server/rpc/interceptors",
        "//server/util/grpc_client",
        "//server/util/grpc_server",
        "//server/util/healthcheck",
        "//server/util/log",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//:go_default_library",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_grpc//reflection",
    ],
)

package(default_visibility = ["//cli:__subpackages__"])
