load("@io_bazel_rules_docker//container:container.bzl", "container_push")
load("@io_bazel_rules_docker//go:image.bzl", "go_image")
load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")
load("@rules_oci//oci:defs.bzl", "oci_image", "oci_push")
load("@rules_pkg//pkg:tar.bzl", "pkg_tar")

go_library(
    name = "cacheload_lib",
    srcs = ["cacheload.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/cacheload",
    visibility = ["//visibility:private"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/metrics",
        "//server/real_environment",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/util/bazel_request",
        "//server/util/grpc_client",
        "//server/util/log",
        "//server/util/monitoring",
        "//server/util/qps",
        "//server/util/retry",
        "//server/util/status",
        "@com_github_prometheus_client_golang//prometheus",
        "@com_github_prometheus_client_golang//prometheus/promauto",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_grpc//metadata",
        "@org_golang_x_sync//errgroup",
    ],
)

go_binary(
    name = "cacheload",
    embed = [":cacheload_lib"],
    visibility = ["//visibility:public"],
)

go_image(
    name = "cacheload_image",
    base = "@buildbuddy_go_image_base//image",
    binary = ":cacheload",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)

# Definitions for uploading new default base docker image.
container_push(
    name = "push_cacheload_image",
    format = "Docker",
    image = ":cacheload_image",
    registry = "gcr.io",
    repository = "flame-build/cacheload",  # Note flame-build, not flame-public.
    # Set the image tag with the bazel run flag "--//deployment:image_tag=TAG"
    tag_file = "//deployment:image_tag_file",
    tags = ["manual"],  # Don't include this target in wildcard patterns
)

pkg_tar(
    name = "tar",
    srcs = [":cacheload"],
)

oci_image(
    name = "oci_image",
    base = "@buildbuddy_go_oci_image_base",
    entrypoint = ["/cacheload"],
    target_compatible_with = ["@platforms//os:linux"],
    tars = [":tar"],
)

oci_push(
    name = "push",
    image = ":oci_image",
    # Set the image tag with the bazel run flag "--//deployment:image_tag=TAG"
    remote_tags = "//deployment:oci_tag_file",
    repository = "gcr.io/flame-build/cacheload-oci",
)
