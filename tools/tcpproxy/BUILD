load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")
load("@io_bazel_rules_docker//go:image.bzl", "go_image")
load("@io_bazel_rules_docker//container:container.bzl", "container_push")

go_library(
    name = "tcpproxy_lib",
    srcs = ["tcpproxy.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/tcpproxy",
    visibility = ["//visibility:private"],
    deps = ["//server/util/log"],
)

go_binary(
    name = "tcpproxy",
    embed = [":tcpproxy_lib"],
    visibility = ["//visibility:public"],
)

go_image(
    name = "tcpproxy_image",
    base = "@buildbuddy_go_image_base//image",
    binary = ":tcpproxy",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)

# Definitions for uploading new default base docker image.
container_push(
    name = "push_tcpproxy_image",
    format = "Docker",
    image = ":tcpproxy_image",
    registry = "gcr.io",
    repository = "flame-build/tcpproxy",  # Note flame-build, not flame-public.
    # Set the image tag with the bazel run flag "--//deployment:image_tag=TAG"
    tag_file = "//deployment:image_tag_file",
    tags = ["manual"],  # Don't include this target in wildcard patterns
)
