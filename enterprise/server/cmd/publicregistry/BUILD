load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "publicregistry_lib",
    srcs = ["publicregistry.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/cmd/publicregistry",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/ociregistry",
        "//proto:remote_execution_go_proto",
        "//server/real_environment",
        "//server/util/grpc_client",
        "//server/util/healthcheck",
        "//server/util/log",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
    ],
)

go_binary(
    name = "publicregistry",
    embed = [":publicregistry_lib"],
)
