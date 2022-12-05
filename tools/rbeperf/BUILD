load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")
load("@io_bazel_rules_docker//go:image.bzl", "go_image")
load("@io_bazel_rules_docker//container:container.bzl", "container_push")

go_library(
    name = "rbeperf_lib",
    srcs = ["rbeperf.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/rbeperf",
    visibility = ["//visibility:private"],
    deps = [
        "//enterprise/server/test/integration/remote_execution/rbeclient",
        "//proto:remote_execution_go_proto",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/util/bazel_request",
        "//server/util/grpc_client",
        "//server/util/histogram",
        "//server/util/log",
        "//server/util/random",
        "//server/util/status",
        "@com_github_google_uuid//:uuid",
        "@com_github_mattn_go_shellwords//:go-shellwords",
        "@com_github_prometheus_client_golang//prometheus",
        "@com_github_prometheus_client_golang//prometheus/promauto",
        "@com_github_prometheus_client_golang//prometheus/push",
        "@go_googleapis//google/bytestream:bytestream_go_proto",
        "@org_golang_google_grpc//:go_default_library",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_protobuf//proto",
        "@org_golang_google_protobuf//types/known/timestamppb",
    ],
)

go_binary(
    name = "rbeperf",
    embed = [":rbeperf_lib"],
    visibility = ["//visibility:public"],
)

go_image(
    name = "rbeperf_image",
    base = "@buildbuddy_go_image_base//image",
    binary = ":rbeperf",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)

# Definitions for uploading new default base docker image.
container_push(
    name = "push_rbeperf_image",
    format = "Docker",
    image = ":rbeperf_image",
    registry = "gcr.io",
    repository = "flame-build/rbeperf",  # Note flame-build, not flame-public.
    # Set the image tag with the bazel run flag "--//deployment:image_tag=TAG"
    tag_file = "//deployment:image_tag_file",
    tags = ["manual"],  # Don't include this target in wildcard patterns
)
