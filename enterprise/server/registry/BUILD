load("@io_bazel_rules_go//go:def.bzl", "go_library")
load("@io_bazel_rules_docker//go:image.bzl", "go_image")
load("@io_bazel_rules_docker//container:container.bzl", "container_image")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "registry_lib",
    srcs = ["registry.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/registry",
    deps = [
        "//proto:registry_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/metrics",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/util/grpc_client",
        "//server/util/log",
        "//server/util/prefix",
        "//server/util/status",
        "@com_github_google_go_containerregistry//pkg/authn",
        "@com_github_google_go_containerregistry//pkg/name",
        "@com_github_google_go_containerregistry//pkg/v1:pkg",
        "@com_github_google_go_containerregistry//pkg/v1/match",
        "@com_github_google_go_containerregistry//pkg/v1/partial",
        "@com_github_google_go_containerregistry//pkg/v1/remote",
        "@com_github_google_go_containerregistry//pkg/v1/remote/transport",
        "@com_github_google_go_containerregistry//pkg/v1/types",
        "@org_golang_google_protobuf//proto",
        "@org_golang_x_sync//errgroup",
    ],
)

container_image(
    name = "base_image",
    base = "@buildbuddy_go_image_base//image",
    tags = ["manual"],
)

# Build a docker image similar to the go_binary above, but use the "go_image"
# rule from @io_bazel_rules_docker instead, which creates a docker image.
go_image(
    name = "registry_image",
    base = ":base_image",
    binary = ":registry",
    tags = ["manual"],
)
