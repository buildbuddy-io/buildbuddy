load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = ["disk_cache.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/backends/disk_cache",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/interfaces:go_default_library",
        "//server/remote_cache/digest:go_default_library",
        "//server/util/disk:go_default_library",
        "//server/util/lru:go_default_library",
        "//server/util/prefix:go_default_library",
        "//server/util/status:go_default_library",
    ],
)

go_test(
    name = "go_default_test",
    srcs = ["disk_cache_test.go"],
    embed = [":go_default_library"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/interfaces:go_default_library",
        "//server/remote_cache/digest:go_default_library",
        "//server/testutil/auth:go_default_library",
        "//server/testutil/digest:go_default_library",
        "//server/testutil/environment:go_default_library",
        "//server/util/disk:go_default_library",
        "//server/util/prefix:go_default_library",
        "//server/util/testing/flags:go_default_library",
        "@org_golang_x_sync//errgroup:go_default_library",
    ],
)
