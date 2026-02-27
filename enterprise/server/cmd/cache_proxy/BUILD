load("@io_bazel_rules_docker//container:container.bzl", "container_image")
load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")
load("@rules_oci//oci:defs.bzl", "oci_image")
load("@rules_pkg//pkg:tar.bzl", "pkg_tar")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "cache_proxy_lib",
    srcs = ["cache_proxy.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/cmd/cache_proxy",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/action_cache_server_proxy",
        "//enterprise/server/atime_updater",
        "//enterprise/server/backends/configsecrets",
        "//enterprise/server/backends/distributed",
        "//enterprise/server/backends/pebble_cache",
        "//enterprise/server/byte_stream_server_proxy",
        "//enterprise/server/capabilities_server_proxy",
        "//enterprise/server/content_addressable_storage_server_proxy",
        "//enterprise/server/experiments",
        "//enterprise/server/hit_tracker_client",
        "//enterprise/server/ocifetcher_server_proxy",
        "//enterprise/server/remote_crypter",
        "//enterprise/server/remoteauth",
        "//enterprise/server/routing/routing_action_cache_client",
        "//enterprise/server/routing/routing_byte_stream_client",
        "//enterprise/server/routing/routing_capabilities_client",
        "//enterprise/server/routing/routing_content_addressable_storage_client",
        "//enterprise/server/routing/routing_service",
        "//enterprise/server/util/proxy_util",
        "//proto:cache_service_go_proto",
        "//proto:oci_fetcher_go_proto",
        "//proto:remote_asset_go_proto",
        "//proto:remote_execution_go_proto",
        "//server/cache_server",
        "//server/config",
        "//server/http/interceptors",
        "//server/real_environment",
        "//server/remote_asset/fetch_server",
        "//server/remote_cache/action_cache_server",
        "//server/remote_cache/byte_stream_server",
        "//server/remote_cache/content_addressable_storage_server",
        "//server/rpc/interceptors",
        "//server/ssl",
        "//server/util/grpc_client",
        "//server/util/grpc_server",
        "//server/util/healthcheck",
        "//server/util/log",
        "//server/util/monitoring",
        "//server/util/scratchspace",
        "//server/util/status",
        "//server/util/tracing",
        "//server/util/usageutil",
        "//server/version",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_grpc//channelz/service",
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

pkg_tar(
    name = "tar",
    srcs = [":cache_proxy"],
)

oci_image(
    name = "oci_image",
    base = "@buildbuddy_go_oci_image_base",
    entrypoint = ["/cache_proxy"],
    target_compatible_with = ["@platforms//os:linux"],
    tars = [":tar"],
    visibility = ["//visibility:public"],
)
