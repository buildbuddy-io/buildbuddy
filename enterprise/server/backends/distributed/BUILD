load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "distributed",
    srcs = ["distributed.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/distributed",
    deps = [
        "//enterprise/server/backends/pubsub",
        "//enterprise/server/util/cacheproxy",
        "//enterprise/server/util/heartbeat",
        "//enterprise/server/util/redisutil",
        "//proto:distributed_cache_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/resources",
        "//server/util/background",
        "//server/util/consistent_hash",
        "//server/util/flagutil",
        "//server/util/log",
        "//server/util/peerset",
        "//server/util/status",
        "@org_golang_google_grpc//metadata",
        "@org_golang_x_sync//errgroup",
    ],
)

go_test(
    name = "distributed_test",
    size = "small",
    srcs = ["distributed_test.go"],
    embed = [":distributed"],
    shard_count = 10,
    deps = [
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/backends/memory_cache",
        "//server/environment",
        "//server/interfaces",
        "//server/testutil/testauth",
        "//server/testutil/testcompression",
        "//server/testutil/testdigest",
        "//server/testutil/testenv",
        "//server/testutil/testport",
        "//server/util/compression",
        "//server/util/grpc_client",
        "//server/util/prefix",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_grpc//:go_default_library",
    ],
)
