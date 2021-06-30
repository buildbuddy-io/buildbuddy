load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "disk_cache",
    srcs = ["disk_cache.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/backends/disk_cache",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/interfaces",
        "//server/remote_cache/digest",
        "//server/util/disk",
        "//server/util/log",
        "//server/util/lru",
        "//server/util/prefix",
        "//server/util/status",
        "//server/util/statusz",
        "@org_golang_x_sync//errgroup",
    ],
)

go_test(
    name = "disk_cache_test",
    srcs = ["disk_cache_test.go"],
    deps = [
        ":disk_cache",
        "//proto:remote_execution_go_proto",
        "//server/interfaces",
        "//server/remote_cache/digest",
        "//server/testutil/testauth",
        "//server/testutil/testdigest",
        "//server/testutil/testenv",
        "//server/util/disk",
        "//server/util/log",
        "//server/util/prefix",
        "//server/util/testing/flags",
        "@org_golang_x_sync//errgroup",
    ],
)
