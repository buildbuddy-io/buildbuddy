load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "execution_service",
    srcs = ["execution_service.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/execution_service",
    deps = [
        "//enterprise/server/util/execution",
        "//proto:execution_stats_go_proto",
        "//server/environment",
        "//server/tables",
        "//server/util/db",
        "//server/util/log",
        "//server/util/perms",
        "//server/util/query_builder",
        "//server/util/status",
        "@org_golang_x_sync//errgroup",
    ],
)
