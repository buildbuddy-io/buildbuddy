load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "cache",
    srcs = ["cache.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/cache",
    deps = [
        "//enterprise/server/raft/bringup",
        "//enterprise/server/raft/driver",
        "//enterprise/server/raft/filestore",
        "//enterprise/server/raft/logger",
        "//enterprise/server/raft/rbuilder",
        "//enterprise/server/raft/registry",
        "//enterprise/server/raft/sender",
        "//enterprise/server/raft/store",
        "//proto:raft_go_proto",
        "//proto:raft_service_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/cache/config",
        "//server/environment",
        "//server/gossip",
        "//server/interfaces",
        "//server/real_environment",
        "//server/remote_cache/digest",
        "//server/util/disk",
        "//server/util/flag",
        "//server/util/flagutil/types",
        "//server/util/ioutil",
        "//server/util/log",
        "//server/util/status",
        "//server/util/statusz",
    ],
)

go_test(
    name = "cache_test",
    srcs = ["cache_test.go"],
    shard_count = 5,
    deps = [
        ":cache",
        "//proto:resource_go_proto",
        "//server/gossip",
        "//server/interfaces",
        "//server/testutil/testauth",
        "//server/testutil/testdigest",
        "//server/testutil/testenv",
        "//server/testutil/testfs",
        "//server/testutil/testport",
        "//server/util/log",
        "//server/util/prefix",
        "@com_github_stretchr_testify//require",
        "@org_golang_x_sync//errgroup",
    ],
)
