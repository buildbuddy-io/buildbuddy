load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = ["distributed.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/distributed",
    visibility = [
        "//enterprise:__subpackages__",
        "@buildbuddy_internal//enterprise:__subpackages__",
    ],
    deps = [
        "//enterprise/server/util/cacheproxy:go_default_library",
        "//enterprise/server/util/heartbeat:go_default_library",
        "//proto:remote_execution_go_proto",
        "//server/environment:go_default_library",
        "//server/interfaces:go_default_library",
        "//server/util/consistent_hash:go_default_library",
        "//server/util/status:go_default_library",
        "@org_golang_x_sync//errgroup:go_default_library",
    ],
)

go_test(
    name = "go_default_test",
    srcs = ["distributed_test.go"],
    embed = [":go_default_library"],
    visibility = [
        "//enterprise:__subpackages__",
        "@buildbuddy_internal//enterprise:__subpackages__",
    ],
    deps = [
        "//enterprise/server/util/heartbeat:go_default_library",
        "//proto:remote_execution_go_proto",
        "//server/backends/memory_cache:go_default_library",
        "//server/environment:go_default_library",
        "//server/interfaces:go_default_library",
        "//server/remote_cache/digest:go_default_library",
        "//server/testutil/app:go_default_library",
        "//server/testutil/pubsub:go_default_library",
        "//server/testutil/testauth:go_default_library",
        "//server/testutil/testdigest:go_default_library",
        "//server/testutil/testenv:go_default_library",
        "//server/util/prefix:go_default_library",
        "//server/util/testing/flags:go_default_library",
    ],
)
