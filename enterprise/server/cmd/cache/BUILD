load("@io_bazel_rules_docker//container:container.bzl", "container_image")
load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")
load("@rules_oci//oci:defs.bzl", "oci_image")
load("@rules_pkg//pkg:tar.bzl", "pkg_tar")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "cache_lib",
    srcs = ["cache.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/cmd/cache",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/backends/configsecrets",
        "//enterprise/server/backends/distributed",
        "//enterprise/server/backends/gcs_cache",
        "//enterprise/server/backends/migration_cache",
        "//enterprise/server/backends/pebble_cache",
        "//enterprise/server/backends/s3_cache",
        "//enterprise/server/hit_tracker_client",
        "//enterprise/server/remoteauth",
        "//proto:remote_execution_go_proto",
        "//server/config",
        "//server/http/interceptors",
        "//server/real_environment",
        "//server/remote_asset/fetch_server",
        "//server/remote_asset/push_server",
        "//server/remote_cache/action_cache_server",
        "//server/remote_cache/byte_stream_client",
        "//server/remote_cache/byte_stream_server",
        "//server/remote_cache/capabilities_server",
        "//server/remote_cache/content_addressable_storage_server",
        "//server/rpc/interceptors",
        "//server/ssl",
        "//server/util/authutil",
        "//server/util/bazel_request",
        "//server/util/grpc_client",
        "//server/util/grpc_server",
        "//server/util/healthcheck",
        "//server/util/log",
        "//server/util/monitoring",
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
    name = "cache",
    embed = [":cache_lib"],
)

container_image(
    name = "base_image",
    base = "@buildbuddy_go_image_base//image",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)

pkg_tar(
    name = "tar",
    srcs = [":cache"],
)

oci_image(
    name = "oci_image",
    base = "@buildbuddy_go_oci_image_base",
    entrypoint = ["/cache"],
    target_compatible_with = ["@platforms//os:linux"],
    tars = [":tar"],
    visibility = ["//visibility:public"],
)
