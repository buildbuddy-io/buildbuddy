load("@io_bazel_rules_docker//container:container.bzl", "container_push")
load("@io_bazel_rules_docker//go:image.bzl", "go_image")
load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")
load("@rules_oci//oci:defs.bzl", "oci_image", "oci_push")
load("@rules_pkg//pkg:tar.bzl", "pkg_tar")

go_library(
    name = "server_lib",
    srcs = ["server.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/codesearch/cmd/server",
    visibility = ["//visibility:private"],
    deps = [
        "//codesearch/server",
        "//enterprise/server/backends/configsecrets",
        "//enterprise/server/remoteauth",
        "//proto:codesearch_service_go_proto",
        "//server/config",
        "//server/real_environment",
        "//server/rpc/interceptors",
        "//server/util/flag",
        "//server/util/grpc_client",
        "//server/util/grpc_server",
        "//server/util/healthcheck",
        "//server/util/log",
        "//server/util/monitoring",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//:grpc",
    ],
)

go_binary(
    name = "server",
    embed = [":server_lib"],
    visibility = ["//visibility:public"],
)

go_image(
    name = "server_image",
    base = "@buildbuddy_go_image_base//image",
    binary = ":server",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)

# Definitions for uploading new default base docker image.
container_push(
    name = "push_server_image",
    format = "Docker",
    image = ":server_image",
    registry = "gcr.io",
    repository = "flame-build/codesearch",  # Note flame-build, not flame-public.
    # Set the image tag with the bazel run flag "--//deployment:image_tag=TAG"
    tag_file = "//deployment:image_tag_file",
    tags = ["manual"],  # Don't include this target in wildcard patterns
)

pkg_tar(
    name = "tar",
    srcs = [":server"],
)

oci_image(
    name = "oci_image",
    base = "@buildbuddy_go_oci_image_base",
    entrypoint = ["/server"],
    target_compatible_with = ["@platforms//os:linux"],
    tars = [":tar"],
    visibility = ["//visibility:public"],
)

oci_push(
    name = "push",
    image = ":oci_image",
    # Set the image tag with the bazel run flag "--//deployment:image_tag=TAG"
    remote_tags = "//deployment:oci_tag_file",
    repository = "gcr.io/flame-build/codesearch-oci",
)
