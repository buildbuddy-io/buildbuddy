load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "cacheproxy_lib",
    srcs = ["cacheproxy.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/cmd/cacheproxy",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/backends/pebble_cache",
        "//proto:remote_execution_go_proto",
        "//server/cache_proxy",
        "//server/config",
        "//server/nullauth",
        "//server/real_environment",
        "//server/rpc/interceptors",
        "//server/util/grpc_client",
        "//server/util/grpc_server",
        "//server/util/healthcheck",
        "//server/util/log",
        "//server/util/monitoring",
        "//server/util/tracing",
        "//server/version",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//:go_default_library",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_grpc//reflection",
    ],
)

go_binary(
    name = "cacheproxy",
    embed = [":cacheproxy_lib"],
    visibility = ["//visibility:public"],
)

package(default_visibility = ["//enterprise:__subpackages__"])
