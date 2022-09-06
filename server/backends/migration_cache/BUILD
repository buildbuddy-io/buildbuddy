load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "migration_cache",
    srcs = [
        "config.go",
        "migration_cache.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/backends/migration_cache",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/backends/pebble_cache",
        "//proto:remote_execution_go_proto",
        "//server/backends/disk_cache",
        "//server/cache/config",
        "//server/environment",
        "//server/interfaces",
        "//server/util/disk",
        "//server/util/flagutil",
        "//server/util/log",
        "//server/util/status",
    ],
)
