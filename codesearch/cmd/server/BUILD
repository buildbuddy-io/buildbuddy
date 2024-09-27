load("@io_bazel_rules_docker//container:container.bzl", "container_push")
load("@io_bazel_rules_docker//go:image.bzl", "go_image")
load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "server_lib",
    srcs = ["server.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/codesearch/cmd/server",
    visibility = ["//visibility:private"],
    deps = [
        "//codesearch/server",
        "//proto:codesearch_service_go_proto",
        "//proto:kythe_service_go_proto",
        "//server/config",
        "//server/nullauth",
        "//server/real_environment",
        "//server/util/flag",
        "//server/util/grpc_server",
        "//server/util/healthcheck",
        "//server/util/log",
        "//server/util/monitoring",
        "@com_github_grpc_ecosystem_go_grpc_prometheus//:go-grpc-prometheus",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_grpc//reflection",
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
