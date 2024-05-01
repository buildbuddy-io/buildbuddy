load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "certgenerator_lib",
    srcs = ["certgenerator.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/certgenerator",
    visibility = ["//visibility:private"],
    deps = [
        "//enterprise/server/auth",
        "//enterprise/server/backends/configsecrets",
        "//proto:certgenerator_go_proto",
        "//server/config",
        "//server/real_environment",
        "//server/ssl",
        "//server/util/flag",
        "//server/util/grpc_server",
        "//server/util/healthcheck",
        "//server/util/log",
        "//server/util/status",
        "//server/util/tracing",
        "@com_github_coreos_go_oidc//:go-oidc",
        "@org_golang_x_crypto//ssh",
    ],
)

go_binary(
    name = "certgenerator",
    embed = [":certgenerator_lib"],
    visibility = ["//visibility:public"],
)

package(default_visibility = ["//enterprise:__subpackages__"])
