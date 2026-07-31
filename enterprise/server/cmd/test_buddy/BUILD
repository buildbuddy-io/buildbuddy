load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "test_buddy_lib",
    srcs = ["main.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/cmd/test_buddy",
    visibility = ["//visibility:private"],
    deps = [
        "//enterprise/server/backends/configsecrets",
        "//enterprise/server/clientidentity",
        "//enterprise/server/remoteauth",
        "//server/config",
        "//server/nullauth",
        "//server/real_environment",
        "//server/ssl",
        "//server/test_health",
        "//server/util/db",
        "//server/util/grpc_server",
        "//server/util/healthcheck",
        "//server/util/log",
        "//server/util/monitoring",
        "//server/util/tracing",
        "//server/version",
    ],
)

go_binary(
    name = "test_buddy",
    embed = [":test_buddy_lib"],
    visibility = ["//visibility:public"],
)
