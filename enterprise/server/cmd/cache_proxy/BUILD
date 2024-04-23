load("@io_bazel_rules_docker//container:container.bzl", "container_image")
load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "cache_proxy_lib",
    srcs = ["cache_proxy.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/cmd/cache_proxy",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/action_cache_server_proxy",
        "//enterprise/server/backends/configsecrets",
        "//enterprise/server/backends/pebble_cache",
        "//enterprise/server/byte_stream_server_proxy",
        "//enterprise/server/capabilities_server_proxy",
        "//enterprise/server/content_addressable_storage_server_proxy",
        "//proto:remote_execution_go_proto",
        "//server/config",
        "//server/http/interceptors",
        "//server/nullauth",
        "//server/real_environment",
        "//server/rpc/interceptors",
        "//server/ssl",
        "//server/util/grpc_client",
        "//server/util/grpc_server",
        "//server/util/healthcheck",
        "//server/util/log",
        "//server/util/monitoring",
        "//server/util/tracing",
        "//server/version",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//:go_default_library",
    ],
)

go_binary(
    name = "cache_proxy",
    embed = [":cache_proxy_lib"],
)

container_image(
    name = "base_image",
    base = "@buildbuddy_go_image_base//image",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)
