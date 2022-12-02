load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "invocation_stat_service",
    srcs = ["invocation_stat_service.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/invocation_stat_service",
    deps = [
        "//proto:context_go_proto",
        "//proto:invocation_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/util/blocklist",
        "//server/util/db",
        "//server/util/log",
        "//server/util/perms",
        "//server/util/query_builder",
        "//server/util/status",
        "@org_golang_x_sync//errgroup",
    ],
)
