load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "certgenerator_lib",
    srcs = ["certgenerator.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/certgenerator",
    visibility = [
        "@buildbuddy_internal//:__subpackages__",
    ],
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
        "//server/util/monitoring",
        "//server/util/status",
        "//server/util/tracing",
        "@com_github_coreos_go_oidc//:go-oidc",
        "@org_golang_x_crypto//ssh",
    ],
)

go_binary(
    name = "certgenerator",
    embed = [":certgenerator_lib"],
)
