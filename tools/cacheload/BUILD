load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")
load("@io_bazel_rules_docker//go:image.bzl", "go_image")
load("@io_bazel_rules_docker//container:container.bzl", "container_push")

go_library(
    name = "cacheload_lib",
    srcs = ["cacheload.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/cacheload",
    visibility = ["//visibility:private"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/metrics",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/util/grpc_client",
        "//server/util/log",
        "//server/util/monitoring",
        "//server/util/qps",
        "//server/util/retry",
        "//server/util/status",
        "@com_github_prometheus_client_golang//prometheus",
        "@com_github_prometheus_client_golang//prometheus/promauto",
        "@go_googleapis//google/bytestream:bytestream_go_proto",
        "@org_golang_google_grpc//:go_default_library",
        "@org_golang_google_grpc//metadata",
        "@org_golang_x_sync//errgroup",
        "@org_golang_x_time//rate",
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
