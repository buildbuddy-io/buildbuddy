load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "distributed",
    srcs = ["distributed.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/distributed",
    visibility = [
        "//enterprise:__subpackages__",
        "@buildbuddy_internal//enterprise:__subpackages__",
    ],
    deps = [
        "//enterprise/server/util/cacheproxy",
        "//enterprise/server/util/heartbeat",
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/util/consistent_hash",
        "//server/util/log",
        "//server/util/status",
        "@org_golang_x_sync//errgroup",
    ],
)

go_test(
    name = "distributed_test",
    srcs = ["distributed_test.go"],
    embed = [":distributed"],
    tags = ["manual"],
    visibility = [
        "//enterprise:__subpackages__",
        "@buildbuddy_internal//enterprise:__subpackages__",
    ],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/backends/memory_cache",
        "//server/environment",
        "//server/interfaces",
        "//server/testutil/app",
        "//server/testutil/testauth",
        "//server/testutil/testdigest",
        "//server/testutil/testenv",
        "//server/util/prefix",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//assert",
    ],
)
