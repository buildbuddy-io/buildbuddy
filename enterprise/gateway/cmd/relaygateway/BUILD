load("@io_bazel_rules_docker//container:container.bzl", "container_image")
load("@io_bazel_rules_docker//go:image.bzl", "go_image")
load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "relaygateway_lib",
    srcs = ["relaygateway.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/gateway/cmd/relaygateway",
    visibility = ["@buildbuddy_internal//:__subpackages__"],
    deps = [
        "//enterprise/gateway/certauth",
        "//enterprise/gateway/relay",
        "//enterprise/gateway/server",
        "//enterprise/server/backends/configsecrets",
        "//proto:gateway_service_go_proto",
        "//server/config",
        "//server/http/interceptors",
        "//server/nullauth",
        "//server/real_environment",
        "//server/ssl",
        "//server/util/flag",
        "//server/util/grpc_server",
        "//server/util/healthcheck",
        "//server/util/log",
        "//server/util/monitoring",
        "//server/util/tracing",
        "//server/version",
    ],
)

go_binary(
    name = "relaygateway",
    embed = [":relaygateway_lib"],
    pure = "on",
    static = "on",
    visibility = ["//visibility:public"],
)

go_image(
    name = "relaygateway_go_image",
    base = "@buildbuddy_go_image_base//image",
    binary = ":relaygateway",
    tags = ["manual"],
)

container_image(
    name = "relaygateway_image",
    base = ":relaygateway_go_image",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)
