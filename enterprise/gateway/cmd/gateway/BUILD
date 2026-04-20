load("@io_bazel_rules_docker//container:container.bzl", "container_image")
load("@io_bazel_rules_docker//go:image.bzl", "go_image")
load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "gateway_lib",
    srcs = ["gateway.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/gateway/cmd/gateway",
    visibility = ["//visibility:private"],
    deps = [
        "//enterprise/gateway/server",
        "//enterprise/server/backends/configsecrets",
        "//enterprise/server/remoteauth",
        "//proto:gateway_service_go_proto",
        "//server/config",
        "//server/http/interceptors",
        "//server/real_environment",
        "//server/rpc/interceptors",
        "//server/ssl",
        "//server/util/authutil",
        "//server/util/flag",
        "//server/util/grpc_server",
        "//server/util/healthcheck",
        "//server/util/log",
        "//server/util/monitoring",
        "//server/util/tracing",
        "//server/version",
        "@org_golang_google_grpc//:grpc",
    ],
)

go_binary(
    name = "gateway",
    embed = [":gateway_lib"],
    visibility = ["//visibility:public"],
)

go_image(
    name = "gateway_go_image",
    base = "@buildbuddy_go_image_base//image",
    binary = ":gateway",
    tags = ["manual"],
)

container_image(
    name = "gateway_image",
    base = ":gateway_go_image",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)
