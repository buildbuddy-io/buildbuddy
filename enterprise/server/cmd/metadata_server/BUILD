load("@io_bazel_rules_docker//container:container.bzl", "container_image")
load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")
load("@rules_oci//oci:defs.bzl", "oci_image", "oci_push")
load("@rules_pkg//pkg:tar.bzl", "pkg_tar")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "metadata_server_lib",
    srcs = ["main.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/cmd/metadata_server",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/backends/configsecrets",
        "//enterprise/server/raft/metadata",
        "//enterprise/server/remoteauth",
        "//proto:metadata_service_go_proto",
        "//server/config",
        "//server/gossip",
        "//server/http/interceptors",
        "//server/real_environment",
        "//server/ssl",
        "//server/util/grpc_server",
        "//server/util/healthcheck",
        "//server/util/log",
        "//server/util/monitoring",
        "//server/util/statusz",
        "//server/util/tracing",
        "//server/version",
    ],
)

go_binary(
    name = "metadata_server",
    data = [
        "//enterprise:licenses",
        "//enterprise/config:config_files",
    ],
    embed = [":metadata_server_lib"],
    visibility = ["//visibility:public"],
)

container_image(
    name = "base_image",
    base = "@buildbuddy_go_image_base//image",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)

pkg_tar(
    name = "tar",
    srcs = [":metadata_server"],
)

oci_image(
    name = "oci_image",
    base = "@buildbuddy_go_oci_image_base",
    entrypoint = ["/metadata_server"],
    target_compatible_with = ["@platforms//os:linux"],
    tars = [":tar"],
    visibility = ["//visibility:public"],
)
