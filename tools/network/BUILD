load("@io_bazel_rules_docker//container:container.bzl", "container_push")
load("@io_bazel_rules_docker//go:image.bzl", "go_image")
load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library", "go_test")

go_test(
    name = "network_test",
    srcs = ["network_test.go"],
    embed = [":network_lib"],
    exec_properties = {
        "test.dockerNetwork": "bridge",
        "workload-isolation-type": "podman",
    },
    deps = ["@com_github_stretchr_testify//require"],
)

go_library(
    name = "network_lib",
    srcs = ["network.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/network",
    visibility = ["//visibility:private"],
    deps = ["//server/util/log"],
)

go_binary(
    name = "network",
    embed = [":network_lib"],
    pure = "on",
    static = "on",
    visibility = ["//visibility:public"],
)

go_image(
    name = "network_image",
    base = "@buildbuddy_go_image_base//image",
    binary = ":network",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)

# Definitions for uploading new default base docker image.
container_push(
    name = "push_network_image",
    format = "Docker",
    image = ":network_image",
    registry = "gcr.io",
    repository = "flame-public/vadim-network-test",
    # Set the image tag with the bazel run flag "--//deployment:image_tag=TAG"
    tag_file = "//deployment:image_tag_file",
    tags = ["manual"],  # Don't include this target in wildcard patterns
)
